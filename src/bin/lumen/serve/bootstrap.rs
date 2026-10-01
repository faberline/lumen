//! A serving node's startup: the write log it opens, the bootstrap seed it
//! restores, and where its segments and cold start come from.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use lumen::rdb::{LocalFsRdbStore, RdbStore};
use lumen::storage::Engine;

use crate::cli::serve::{Persistence, ServeArgs, WalBackend};

pub(super) fn recovery_phase_start<T>(phase: &'static str, work: impl FnOnce() -> T) -> T {
    if std::env::var_os("LUMEN_RECOVERY_PROFILE").is_some_and(|value| value == "1") {
        tracing::info!(phase, state = "start", "recovery phase");
    }
    work()
}

/// Resolve `--wal auto` to a concrete backend, k8s-native: a StatefulSet with
/// `REPLICAS_PER_SHARD > 1` (the downward-API value) runs raft; one replica — or
/// no cluster context (the env unset, e.g. local dev) — runs embedded. An
/// explicit `--wal <backend>` passes through unchanged.
pub(super) fn resolve_wal_backend(requested: WalBackend) -> WalBackend {
    if requested != WalBackend::Auto {
        return requested;
    }
    #[cfg(feature = "raft-wal")]
    if raft_runtime::cluster::replica_mode() {
        tracing::info!("wal=auto → raft (StatefulSet REPLICAS_PER_SHARD > 1)");
        return WalBackend::Raft;
    }
    tracing::info!("wal=auto → embedded (single replica / no cluster context)");
    WalBackend::Embedded
}

pub(super) fn apply_bootstrap_seed(engine: &Engine, seed_uri: Option<&str>) -> Result<bool> {
    let Some(seed_uri) = seed_uri else {
        return Ok(false);
    };
    let bytes = service_backup::fetch_backup_object(seed_uri)
        .with_context(|| format!("read bootstrap seed {seed_uri}"))?;
    let snap: lumen::storage::SnapshotV1 =
        serde_json::from_slice(&bytes).context("decode bootstrap SnapshotV1 JSON")?;
    engine.restore(snap).context("apply bootstrap seed")?;
    tracing::info!(
        seed_uri,
        bytes = bytes.len(),
        "bootstrap seed restored before WAL/raft catch-up"
    );
    Ok(true)
}

/// Whether segment persistence is selected. Driven purely by `--persistence`:
/// `false` for the default `cbor` mode (the binary's cold-start + snapshotter are
/// byte-identical to today), `true` only when `--persistence=segment` is passed.
pub(super) fn use_segment_persistence(args: &ServeArgs) -> bool {
    args.persistence == Persistence::Segment
}

pub(super) fn load_search_shard_segment_roots(dirs: &[PathBuf]) -> Result<Vec<Arc<Engine>>> {
    let mut shards = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let store = lumen::segment_rdb::SegmentRdbStore::new(dir)
            .with_context(|| format!("open search shard segment root {}", dir.display()))?;
        let Some((engine, seq)) = store
            .load_latest()
            .with_context(|| format!("load search shard segment root {}", dir.display()))?
        else {
            anyhow::bail!(
                "search shard segment root {} has no committed gen-<seq> checkpoint",
                dir.display()
            );
        };
        tracing::info!(
            root = %dir.display(),
            up_to_seq = seq,
            "loaded search shard segment root"
        );
        shards.push(engine);
    }
    Ok(shards)
}

/// The CBOR-RDB cold start: load the latest `rdb-<seq>.lrb` (if any) into
/// `engine` and return its sequence so the apply loop tails from there. This is
/// the exact restore the binary has always done; factored out so the segment
/// branch can sit beside it without duplicating it.
pub(super) async fn cbor_cold_start(
    rdb_store: &Option<Arc<LocalFsRdbStore>>,
    engine: &Arc<Engine>,
) -> Result<u64> {
    if let Some(store) = rdb_store {
        match store.load_latest().await? {
            Some(rdb) => {
                let seq = rdb.up_to_seq;
                rdb.restore_into(engine).context("restore RDB")?;
                tracing::info!(up_to_seq = seq, "restored RDB baseline");
                Ok(seq)
            }
            None => Ok(0),
        }
    } else {
        Ok(0)
    }
}

#[cfg(test)]
mod tests;
