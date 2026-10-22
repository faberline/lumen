//! `lumen serve`'s arguments: the write log, the log format, the persistence
//! mode and the serving node's flags.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub(crate) enum WalBackend {
    /// Auto-detect (default, k8s-native): a StatefulSet with
    /// `REPLICAS_PER_SHARD > 1` runs raft (replica/HA mode); a single replica —
    /// or no cluster context (local dev) — runs embedded. An explicit
    /// `--wal <backend>` overrides this.
    Auto,
    /// In-process log. Single-node / dev. No external dependency.
    Embedded,
    /// Lumen-owned raft_core replication (#515). HA without an external broker.
    #[cfg(feature = "raft-wal")]
    Raft,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum LogFormat {
    Pretty,
    Json,
}

/// Cold-start / snapshot persistence mode for `--data-dir` (Stage 2 Phase 2f-2).
/// Selected at runtime via `--persistence`; defaults to the CBOR RDB, so the
/// default `serve` path is byte-identical to today unless `segment` is passed.
#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub(crate) enum Persistence {
    /// CBOR RDB blob (`rdb-<seq>.lrb`) — the default, byte-identical to today.
    Cbor,
    /// Columnar segment checkpoint (`gen-<seq>/<collection>/...`) — the disk
    /// engine as persistence. Cold start reopens segments WITHOUT a whole-
    /// collection load; the periodic snapshotter re-seals (re-seal-capable).
    Segment,
}

#[derive(Parser)]
pub(crate) struct ServeArgs {
    /// Bind address. K8s passes 0.0.0.0.
    #[arg(long, env = "LUMEN_HOST", default_value = "127.0.0.1")]
    pub(crate) host: String,
    /// Client API port. 7373 avoids the usual collisions (8080/9200/9000).
    #[arg(long, env = "LUMEN_PORT", default_value_t = 7373)]
    pub(crate) port: u16,
    /// `trace|debug|info|warn|error` (overrides via RUST_LOG still apply).
    #[arg(long, env = "LUMEN_LOG_LEVEL", default_value = "info")]
    pub(crate) log_level: String,
    /// Log output format.
    #[arg(long, env = "LUMEN_LOG_FORMAT", value_enum, default_value_t = LogFormat::Pretty)]
    pub(crate) log_format: LogFormat,
    /// Write-log backend.
    #[arg(long = "wal", env = "LUMEN_WAL", value_enum, default_value_t = WalBackend::Auto)]
    pub(crate) wal: WalBackend,
    /// Data dir for raft hard state (used when `--wal raft`). A PVC in k8s.
    #[cfg(feature = "raft-wal")]
    #[arg(
        long,
        env = "LUMEN_RAFT_DATA_DIR",
        default_value = "/var/lib/lumen/raft"
    )]
    pub(crate) raft_data_dir: String,
    /// Peer port for raft RPCs (used when `--wal raft`; multi-pod, Slice 2).
    #[cfg(feature = "raft-wal")]
    #[arg(long, env = "LUMEN_RAFT_PORT", default_value_t = 7374)]
    pub(crate) raft_port: u16,
    /// Physical storage shard count. Data ownership uses the versioned
    /// virtual-bucket map, not permanent `hash % shardCount` routing.
    /// Deliberately `Option<u32>` with no `default_value_t`: this is the
    /// only clap-native way to tell "the operator/user actually set
    /// `--shard-count`/`SHARD_COUNT`" (`Some`) apart from "nobody set it"
    /// (`None`) — the segment-dirs fan-in path (below) needs that
    /// distinction to default to the loaded-dir count instead of silently
    /// assuming 1 (#1398 R4). Non-fan-in call sites treat `None` as 1.
    #[arg(long, env = "SHARD_COUNT")]
    pub(crate) shard_count: Option<u32>,
    /// Directory for RDB snapshots (cold-start baseline). When unset,
    /// no snapshots are taken and a node rebuilds from the full log.
    #[arg(long, env = "LUMEN_DATA_DIR")]
    pub(crate) data_dir: Option<String>,
    /// Persistence mode for `--data-dir`: `cbor` (the CBOR RDB, default) or
    /// `segment` (the columnar disk-engine checkpoint). Defaults to `cbor`; pass
    /// `--persistence=segment` to opt into the disk tier.
    #[arg(long = "persistence", env = "LUMEN_PERSISTENCE", value_enum, default_value_t = Persistence::Cbor)]
    pub(crate) persistence: Persistence,
    /// Comma-separated segment-checkpoint roots to serve as read shards. Each
    /// root must contain a committed `gen-<seq>/` checkpoint. When set, search
    /// requests fan in across these roots through the API SearchBackend seam;
    /// writes still apply to the node's local engine/log.
    #[arg(long, env = "LUMEN_SEARCH_SHARD_SEGMENT_DIRS", value_delimiter = ',')]
    pub(crate) search_shard_segment_dirs: Vec<PathBuf>,
    /// Optional SnapshotV1 JSON seed URI for empty-PVC bootstrap. Supports
    /// exact `file://` paths and, in backup-enabled builds, exact
    /// `s3://bucket/key` objects.
    #[arg(long, env = "LUMEN_BOOTSTRAP_SEED_URI")]
    pub(crate) bootstrap_seed_uri: Option<String>,
    /// Optional seed fetch throttle advertised in CR/env. Exact object fetch is
    /// a one-shot read; streaming throttle belongs in the source adapter.
    #[arg(long, env = "LUMEN_BOOTSTRAP_MAX_BYTES_PER_SEC")]
    pub(crate) bootstrap_max_bytes_per_sec: Option<u64>,
    /// Seconds between RDB snapshots when `--data-dir` is set.
    #[arg(long, env = "LUMEN_SNAPSHOT_SECS", default_value_t = 300)]
    pub(crate) snapshot_secs: u64,
    /// Graceful drain window on SIGTERM.
    #[arg(long, env = "LUMEN_GRACE_SECS", default_value_t = 30)]
    pub(crate) grace_secs: u64,
    /// OTLP gRPC endpoint for trace export, e.g. `http://otel-collector:4317`.
    /// Opt-in: traces export only when this is set (unset = plain logs, no OTLP,
    /// no collector connection). Requires the `otel` build feature (on in release
    /// builds); a plain dev build ignores it with a warning.
    #[arg(long, env = "LUMEN_OTLP_ENDPOINT")]
    pub(crate) otlp_endpoint: Option<String>,
}
