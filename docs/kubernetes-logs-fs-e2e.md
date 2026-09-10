# `kubernetes_logs_fs` Kubernetes E2E

`scripts/test-e2e-kubernetes-logs-fs.sh` exercises the source against a real,
single-node Minikube cluster. The Minikube Docker driver hosts the node, while
the node's Kubernetes runtime is explicitly set to containerd. Calico provides
NetworkPolicy enforcement for the API egress check. No Docker runtime test is
used.

The script accepts either `VECTOR_IMAGE`, an image built from one of the
retained distribution Dockerfiles, or `VECTOR_DEB`, a Debian package from which
it builds the Debian image. It loads the image into Minikube, so a registry is
not required:

```sh
VECTOR_DEB=target/artifacts/vector_0.58.0-x86_64-unknown-linux-gnu.deb \
  scripts/test-e2e-kubernetes-logs-fs.sh
```

The Vector DaemonSet mounts `/var/log` read-only and a host checkpoint directory
at `/var/lib/vector`. `automountServiceAccountToken` is disabled, no RBAC
objects are created, and a default-deny egress policy prevents API-server
access from the Vector pod. The source configuration contains only filesystem
paths and `auto_partial_merge`; static Kubernetes and collector metadata is
added by a generated VRL transform.

The test waits for stdout and stderr records, a 256 KiB CRI partial sequence,
and every static metadata field (including the kernel version). It then
restarts Vector and checks that the checkpoint allows a subsequent record to be
read, creates a second pod to exercise discovery, then renames and replaces a
CRI file on the node to exercise rotation handling. Set `KEEP_CLUSTER=1` to
retain the cluster for diagnostics. `E2E_TIMEOUT`
controls each polling operation (180 seconds by default).
