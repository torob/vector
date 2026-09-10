//! CRI-only Kubernetes pod log source reading directly from the node filesystem.
#![deny(missing_docs)]
use crate::{
    SourceSender,
    config::{DataType, GenerateConfig, SourceConfig, SourceContext, SourceOutput, log_schema},
    internal_events::{FileInternalMetricsConfig, FileSourceInternalEventsEmitter},
    shutdown::ShutdownSignal,
    transforms::{FunctionTransform, OutputBuffer},
};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use serde_with::serde_as;
use std::{path::PathBuf, time::Duration};
use vector_lib::{
    codecs::{BytesDeserializer, BytesDeserializerConfig, OversizedAction},
    config::{LegacyKey, LogNamespace},
    configurable::configurable_component,
    file_source::{
        file_server::{FileServer, Line, Shutdown as FileServerShutdown, calculate_ignore_before},
        paths_provider::{Glob, MatchOptions},
    },
    file_source_common::{
        Checkpointer, FingerprintStrategy, Fingerprinter, ReadFrom, ReadFromConfig,
    },
    internal_event::{ByteSize, BytesReceived, InternalEventHandle, Protocol},
    lookup::{owned_value_path, path},
};
use vrl::value::Kind;

#[path = "kubernetes_logs/parser/cri.rs"]
mod cri;
#[path = "kubernetes_logs/partial_events_merger.rs"]
mod partial_events_merger;
#[cfg(test)]
#[path = "kubernetes_logs/parser/test_util.rs"]
mod test_util;
use cri::Cri;

const NAME: &str = "kubernetes_logs_fs";
const fn default_read_from() -> ReadFromConfig {
    ReadFromConfig::Beginning
}
const fn default_max_line_bytes() -> usize {
    32 * 1024
}
const fn default_max_read_bytes() -> usize {
    2048
}
const fn default_fingerprint_lines() -> usize {
    1
}
const fn default_glob_cooldown() -> Duration {
    Duration::from_secs(60)
}
const fn default_rotate_wait() -> Duration {
    Duration::from_secs(u64::MAX / 2)
}

#[serde_as]
#[configurable_component(source(
    "kubernetes_logs_fs",
    "Collect CRI Kubernetes Pod logs from the filesystem."
))]
#[derive(Clone, Debug)]
#[configurable(description = "Collect CRI Kubernetes Pod logs from the filesystem.")]
#[serde(deny_unknown_fields, default)]
/// Configuration for the filesystem Kubernetes logs source.
pub struct Config {
    /// File glob patterns to include.
    #[configurable(derived)]
    pub include: Vec<PathBuf>,
    /// File glob patterns to exclude.
    #[configurable(derived)]
    pub exclude: Vec<PathBuf>,
    /// Merge CRI partial records.
    #[configurable(derived)]
    pub auto_partial_merge: bool,
    /// Checkpoint directory.
    #[configurable(derived)]
    pub data_dir: Option<PathBuf>,
    #[serde(default = "default_read_from")]
    #[configurable(derived)]
    /// Source option.
    pub read_from: ReadFromConfig,
    #[serde(default)]
    #[configurable(derived)]
    /// Source option.
    pub ignore_older_secs: Option<u64>,
    #[serde(default = "default_max_read_bytes")]
    #[configurable(derived)]
    /// Source option.
    pub max_read_bytes: usize,
    #[serde(default)]
    #[configurable(derived)]
    /// Source option.
    pub oldest_first: bool,
    #[serde(default = "default_max_line_bytes")]
    #[configurable(derived)]
    /// Source option.
    pub max_line_bytes: usize,
    #[configurable(derived)]
    /// Source option.
    pub max_merged_line_bytes: Option<usize>,
    #[serde(default)]
    #[configurable(derived)]
    /// Source option.
    pub max_merged_line_action: OversizedAction,
    #[serde(default = "default_fingerprint_lines")]
    #[configurable(derived)]
    /// Source option.
    pub fingerprint_lines: usize,
    #[serde(default = "default_glob_cooldown")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(derived)]
    /// Source option.
    pub glob_minimum_cooldown_secs: Duration,
    #[serde(default)]
    #[configurable(derived)]
    /// Source option.
    pub log_namespace: Option<bool>,
    #[serde(default)]
    #[configurable(derived)]
    /// Source option.
    pub internal_metrics: FileInternalMetricsConfig,
    #[serde(default = "default_rotate_wait")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(derived)]
    /// Source option.
    pub rotate_wait: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            include: vec!["/var/log/pods/*/*/*.log*".into()],
            exclude: vec!["**/*.gz".into(), "**/*.tmp".into()],
            auto_partial_merge: true,
            data_dir: None,
            read_from: default_read_from(),
            ignore_older_secs: None,
            max_read_bytes: default_max_read_bytes(),
            oldest_first: false,
            max_line_bytes: default_max_line_bytes(),
            max_merged_line_bytes: None,
            max_merged_line_action: OversizedAction::Drop,
            fingerprint_lines: default_fingerprint_lines(),
            glob_minimum_cooldown_secs: default_glob_cooldown(),
            log_namespace: None,
            internal_metrics: Default::default(),
            rotate_wait: default_rotate_wait(),
        }
    }
}
impl GenerateConfig for Config {
    fn generate_config() -> serde_json::Value {
        serde_json::to_value(Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "kubernetes_logs_fs")]
impl SourceConfig for Config {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;
        let log_namespace = cx.log_namespace(self.log_namespace);
        Ok(Box::pin(
            run(self.clone(), data_dir, cx.out, cx.shutdown, log_namespace).map(|r| {
                r.map_err(|e| {
                    error!(message = "kubernetes_logs_fs failed", error = %e);
                    ()
                })
            }),
        ))
    }
    fn outputs(&self, global: LogNamespace) -> Vec<SourceOutput> {
        let ns = global.merge(self.log_namespace);
        let schema = BytesDeserializerConfig
            .schema_definition(ns)
            .with_source_metadata(
                NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("file"))),
                &owned_value_path!("file"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("stream"))),
                &owned_value_path!("stream"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                NAME,
                log_schema()
                    .timestamp_key()
                    .cloned()
                    .map(LegacyKey::Overwrite),
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
            .with_standard_vector_source_metadata();
        vec![SourceOutput::new_maybe_logs(DataType::Log, schema)]
    }
    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn run(
    config: Config,
    data_dir: PathBuf,
    mut out: SourceSender,
    shutdown: ShutdownSignal,
    log_namespace: LogNamespace,
) -> crate::Result<()> {
    if config.include.is_empty() {
        return Err("`include` must contain at least one path".into());
    }
    let emitter = FileSourceInternalEventsEmitter {
        include_file_metric_tag: config.internal_metrics.include_file_tag,
    };
    let paths_provider = Glob::new(
        &config.include,
        &config.exclude,
        MatchOptions::default(),
        emitter.clone(),
    )
    .ok_or("invalid glob")?;
    let checkpointer = Checkpointer::new(&data_dir);
    let file_server = FileServer {
        paths_provider,
        max_read_bytes: config.max_read_bytes,
        ignore_checkpoints: false,
        read_from: ReadFrom::from(config.read_from),
        ignore_before: calculate_ignore_before(config.ignore_older_secs),
        max_line_bytes: config.max_line_bytes,
        line_delimiter: Bytes::from("\n"),
        data_dir,
        glob_minimum_cooldown: config.glob_minimum_cooldown_secs,
        fingerprinter: Fingerprinter::new(
            FingerprintStrategy::FirstLinesChecksum {
                ignored_header_bytes: 0,
                lines: config.fingerprint_lines,
            },
            config.max_line_bytes,
            true,
        ),
        oldest_first: config.oldest_first,
        remove_after: None,
        emitter,
        rotate_wait: config.rotate_wait,
    };
    let (tx, rx) = futures::channel::mpsc::channel::<Vec<Line>>(2);
    let checkpoints = checkpointer.view();
    let mut parser = Cri::with_source_name(log_namespace, NAME);
    let events = rx
        .flat_map(futures::stream::iter)
        .map(move |line| {
            let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
            bytes_received.emit(ByteSize(line.text.len()));
            let mut log = BytesDeserializer.parse_single(line.text, log_namespace);
            log_namespace.insert_source_metadata(
                NAME,
                &mut log,
                Some(LegacyKey::Overwrite(path!("file"))),
                path!("file"),
                line.filename.as_str(),
            );
            log_namespace.insert_vector_metadata(
                &mut log,
                log_schema().source_type_key(),
                path!("source_type"),
                NAME,
            );
            checkpoints.update(line.file_id, line.end_offset);
            let mut buf = OutputBuffer::with_capacity(1);
            parser.transform(&mut buf, log.into());
            futures::stream::iter(buf.into_events())
        })
        .flatten();
    let mut stream = if config.auto_partial_merge {
        partial_events_merger::merge_partial_events_with_source_name(
            events,
            log_namespace,
            NAME,
            config.max_merged_line_bytes,
            config.max_merged_line_action,
        )
        .left_stream()
    } else {
        events.right_stream()
    };
    let send = out.send_event_stream(&mut stream);
    let fs = crate::sources::kubernetes_logs_fs::run_file_server(
        file_server,
        tx,
        shutdown.clone(),
        checkpointer,
    );
    futures::pin_mut!(send);
    futures::pin_mut!(fs);
    let _ = futures::future::select(send, fs).await;
    Ok(())
}

async fn run_file_server<PP, E, C, S>(
    file_server: FileServer<PP, E>,
    chans: C,
    shutdown: S,
    checkpointer: Checkpointer,
) -> Result<FileServerShutdown, tokio::task::JoinError>
where
    PP: vector_lib::file_source::paths_provider::PathsProvider + Send + Sync + 'static,
    E: vector_lib::file_source_common::FileSourceInternalEvents,
    C: futures::Sink<Vec<Line>> + Unpin + Send + 'static,
    C::Error: std::error::Error + Send,
    S: futures::Future + Unpin + Send + Clone + 'static,
    S::Output: Clone + Send + Sync,
    PP::IntoIter: IntoIterator + Send,
    <PP::IntoIter as IntoIterator>::IntoIter: Send,
{
    let shutdown2 = shutdown.clone();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(file_server.run(
            chans,
            shutdown,
            shutdown2,
            checkpointer,
        ))
    })
    .await
    .map(|r| r.unwrap())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::StreamExt;
    use vector_lib::{config::LogNamespace, lookup::event_path};
    use vrl::{metadata_path, value};

    use super::*;
    use crate::event::{self, Event, LogEvent};

    #[test]
    fn default_config_is_filesystem_only() {
        let config = Config::default();
        assert_eq!(
            config.include,
            vec![PathBuf::from("/var/log/pods/*/*/*.log*")]
        );
        assert!(config.auto_partial_merge);
        assert!(config.exclude.iter().any(|path| path == "**/*.gz"));
    }

    #[test]
    fn cri_parser_uses_filesystem_metadata_namespace_and_stream() {
        let mut parser = Cri::with_source_name(LogNamespace::Vector, NAME);
        let mut event = LogEvent::from(value!(Bytes::from(
            "2026-09-10T00:00:00.000000000Z stderr F filesystem-error\n",
        )));
        let mut output = OutputBuffer::with_capacity(1);
        parser.transform(&mut output, Event::Log(std::mem::take(&mut event)));
        let Event::Log(log) = output.into_events().next().expect("CRI event") else {
            panic!("expected log event")
        };
        assert_eq!(
            log.get(event_path!()).cloned(),
            Some(value!("filesystem-error"))
        );
        assert_eq!(
            log.get(metadata_path!("kubernetes_logs_fs", "stream"))
                .cloned(),
            Some(value!("stderr"))
        );
    }

    #[tokio::test]
    async fn cri_partial_records_merge_in_filesystem_namespace() {
        let mut first = LogEvent::from(value!("first"));
        first.insert(metadata_path!(NAME, "file"), "/var/log/pods/app/pod/c.log");
        first.insert(metadata_path!(NAME, event::PARTIAL), true);
        let mut final_record = LogEvent::from(value!("second"));
        final_record.insert(metadata_path!(NAME, "file"), "/var/log/pods/app/pod/c.log");

        let events = futures::stream::iter([first.into(), final_record.into()]);
        let mut output = partial_events_merger::merge_partial_events_with_source_name(
            events,
            LogNamespace::Vector,
            NAME,
            None,
            OversizedAction::Drop,
        )
        .collect::<Vec<_>>()
        .await;
        let Event::Log(log) = output.pop().expect("merged event") else {
            panic!("expected log event")
        };
        assert_eq!(log.get(event_path!()).cloned(), Some(value!("firstsecond")));
    }
}
