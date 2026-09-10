#!/usr/bin/env bash
# Run the CRI-only kubernetes_logs_fs test against disposable Minikube.
# Set VECTOR_IMAGE to an image already built from distribution/docker/debian/Dockerfile,
# or VECTOR_DEB to build that image locally before loading it into Minikube.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CODEX_TOOLS="${HOME}/.tools"
export PATH="${CODEX_TOOLS}/bin:${PATH}"
PROFILE="${MINIKUBE_PROFILE:-vector-logs-fs}"
TIMEOUT="${E2E_TIMEOUT:-180}"
IMAGE="${VECTOR_IMAGE:-}"
NAMESPACE="vector-logs-fs-e2e"
TMP_DIR="$(mktemp -d)"
cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != 1 ]]; then minikube delete -p "${PROFILE}" >/dev/null 2>&1 || true; fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT
for tool in minikube kubectl docker; do command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 2; }; done

if [[ -z "$IMAGE" ]]; then
  : "${VECTOR_DEB:?Set VECTOR_IMAGE or VECTOR_DEB}"
  [[ -f "$VECTOR_DEB" ]] || { echo "VECTOR_DEB does not exist: $VECTOR_DEB" >&2; exit 2; }
  cp "$VECTOR_DEB" "$TMP_DIR/vector_$(dpkg-deb -f "$VECTOR_DEB" Version)_$(dpkg-deb -f "$VECTOR_DEB" Architecture).deb"
  IMAGE="localhost/vector-kubernetes-logs-fs:e2e-${RANDOM}"
  docker build --pull=false --provenance=false --sbom=false \
    -f "$ROOT_DIR/distribution/docker/debian/Dockerfile" -t "$IMAGE" "$TMP_DIR"
fi

# The node uses containerd while the Docker driver only hosts the Minikube node.
minikube delete -p "$PROFILE" >/dev/null 2>&1 || true
minikube start -p "$PROFILE" --driver=docker --container-runtime=containerd --cni=calico --cpus=2 --memory=4096 --wait=all
kubectl config use-context "$PROFILE" >/dev/null
docker save "$IMAGE" -o "$TMP_DIR/vector-image.tar"
minikube image load -p "$PROFILE" "$TMP_DIR/vector-image.tar"

cat > "$TMP_DIR/vector.yaml" <<'YAML'
data_dir: /var/lib/vector
sources:
  container_logs:
    type: kubernetes_logs_fs
    include: [/var/log/pods/vector-logs-fs-e2e_cri-*/*/*.log*]
    exclude: [/var/log/pods/*/*/excluded-*.log]
    auto_partial_merge: true
    glob_minimum_cooldown_secs: 1
    data_dir: /var/lib/vector
transforms:
  add_metadata:
    type: remap
    inputs: [container_logs]
    source: |
      .kubernetes.pod_name = "web-7d9c"
      .kubernetes.pod_namespace = "app"
      .kubernetes.container_name = "nginx"
      .kubernetes.node_name = "worker-1"
      .kubernetes.node_kernel_version = "6.1.0"
      .log_collector.name = "app-logs"
      .source_type = "container"
sinks:
  console:
    type: console
    inputs: [add_metadata]
    encoding: {codec: json}
YAML

kubectl create namespace "$NAMESPACE" >/dev/null
{
  cat <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: vector-config, namespace: $NAMESPACE}
data:
  vector.yaml: |
YAML
  sed 's/^/    /' "$TMP_DIR/vector.yaml"
  cat <<YAML
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: deny-vector-api-egress, namespace: $NAMESPACE}
spec:
  podSelector: {matchLabels: {app: vector-logs-fs}}
  policyTypes: [Egress]
  egress: []
---
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: vector-logs-fs, namespace: $NAMESPACE}
spec:
  selector: {matchLabels: {app: vector-logs-fs}}
  template:
    metadata: {labels: {app: vector-logs-fs}}
    spec:
      automountServiceAccountToken: false
      serviceAccountName: default
      containers:
      - name: vector
        image: $IMAGE
        imagePullPolicy: IfNotPresent
        args: [--config, /etc/vector/vector.yaml]
        securityContext: {readOnlyRootFilesystem: true}
        volumeMounts:
        - {name: config, mountPath: /etc/vector, readOnly: true}
        - {name: logs, mountPath: /var/log, readOnly: true}
        - {name: checkpoints, mountPath: /var/lib/vector}
      volumes:
      - {name: config, configMap: {name: vector-config}}
      - {name: logs, hostPath: {path: /var/log, type: Directory}}
      - {name: checkpoints, hostPath: {path: /var/lib/vector-logs-fs-e2e, type: DirectoryOrCreate}}
YAML
} | kubectl apply -f -
kubectl -n "$NAMESPACE" rollout status daemonset/vector-logs-fs --timeout="${TIMEOUT}s"
VECTOR_POD="$(kubectl -n "$NAMESPACE" get pod -l app=vector-logs-fs -o jsonpath='{.items[0].metadata.name}')"

# This proves the DaemonSet did not receive a projected ServiceAccount token.
if kubectl -n "$NAMESPACE" exec "$VECTOR_POD" -- test -e /var/run/secrets/kubernetes.io/serviceaccount/token 2>/dev/null; then
  echo "service-account token unexpectedly mounted" >&2; exit 1
fi
kubectl -n "$NAMESPACE" run cri-fixture --image=busybox:1.36 --restart=Never -- sh -c 'printf "STDOUT-CRI-OK\\n"; printf "STDERR-CRI-OK\\n" >&2; printf "LONG-START-"; head -c 262144 /dev/zero | tr "\\000" L; printf -- "-LONG-END\\n"; sleep 300' >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/cri-fixture --timeout="${TIMEOUT}s"
wait_for() {
  local pattern="$1" deadline=$((SECONDS + TIMEOUT)) output
  while ((SECONDS < deadline)); do
    output="$(kubectl -n "$NAMESPACE" logs "$VECTOR_POD" --since=2m 2>/dev/null || true)"
    if grep -Fq -- "$pattern" <<<"$output"; then printf '%s\n' "$output"; return 0; fi
    sleep 2
  done
  echo "Timed out waiting for Vector output: $pattern" >&2
  kubectl -n "$NAMESPACE" logs "$VECTOR_POD" --tail=100 >&2 || true
  return 1
}
output="$(wait_for -LONG-END)"
grep -F STDOUT-CRI-OK <<<"$output" | grep -Fq '"stream":"stdout"'
grep -F STDERR-CRI-OK <<<"$output" | grep -Fq '"stream":"stderr"'
grep -Fq STDERR-CRI-OK <<<"$output"
grep -Eq 'LONG-START-.*-LONG-END' <<<"$output"
for field in '"pod_name":"web-7d9c"' '"pod_namespace":"app"' '"container_name":"nginx"' '"node_name":"worker-1"' '"node_kernel_version":"6.1.0"' '"name":"app-logs"' '"source_type":"container"'; do grep -Fq "$field" <<<"$output" || { echo "missing static metadata: $field" >&2; exit 1; }; done
echo "CRI stdout/stderr, partial merge, and static VRL metadata verified"

kubectl -n "$NAMESPACE" rollout restart daemonset/vector-logs-fs >/dev/null
kubectl -n "$NAMESPACE" rollout status daemonset/vector-logs-fs --timeout="${TIMEOUT}s"
VECTOR_POD="$(kubectl -n "$NAMESPACE" get pod -l app=vector-logs-fs -o jsonpath='{.items[0].metadata.name}')"
if kubectl -n "$NAMESPACE" logs "$VECTOR_POD" --since=15s 2>/dev/null | grep -Fq STDOUT-CRI-OK; then
  echo "checkpoint replayed an already-read record after restart" >&2
  exit 1
fi
kubectl -n "$NAMESPACE" exec cri-fixture -- sh -c 'printf "AFTER-RESTART-CHECKPOINT\\n" > /proc/1/fd/1' >/dev/null
wait_for AFTER-RESTART-CHECKPOINT >/dev/null
echo "checkpoint recovery after Vector restart verified"

kubectl -n "$NAMESPACE" delete pod cri-fixture --ignore-not-found >/dev/null
kubectl -n "$NAMESPACE" run cri-rotated --image=busybox:1.36 --restart=Never -- sh -c 'printf "ROTATED-CRI-FILE\\n"; sleep 3' >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/cri-rotated --timeout="${TIMEOUT}s"
wait_for ROTATED-CRI-FILE >/dev/null
rotated_uid="$(kubectl -n "$NAMESPACE" get pod cri-rotated -o jsonpath='{.metadata.uid}')"
rotated_dir="/var/log/pods/${NAMESPACE}_cri-rotated_${rotated_uid}/busybox"
rotation_command="mkdir -p ${rotated_dir}; printf '2026-09-10T00:00:00.000000000Z stdout F ROTATED-FILE-1\\n' > ${rotated_dir}/rotation.log; mv ${rotated_dir}/rotation.log ${rotated_dir}/rotation.log.1; printf '2026-09-10T00:00:01.000000000Z stdout F ROTATED-FILE-2\\n' > ${rotated_dir}/rotation.log"
minikube ssh -p "$PROFILE" -- "sudo sh -c $(printf '%q' "$rotation_command")"
wait_for ROTATED-FILE-1 >/dev/null
wait_for ROTATED-FILE-2 >/dev/null
echo "CRI file discovery and rotation verified"
echo "kubernetes_logs_fs E2E passed (containerd, no token, deny-all API egress)"
