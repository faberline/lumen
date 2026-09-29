//! Cold start: every collection reopened from its segment checkpoint into this
//! Engine, and the probe that reports whether a field's reads have moved to its
//! segment.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{anyhow, Result};

use crate::index::application::engine::Engine;
use crate::index::application::recovery_profile::RecoveryProfile;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::storage::{collection_name_from_dir, CheckpointSchema, CHECKPOINT_SCHEMA_FILE};

impl Engine {
    /// PRODUCTION cold-start (Phase 2f-2): reopen EVERY collection's checkpoint
    /// under `dir` into THIS engine and return the max applied_seq across them (0
    /// if `dir` has no collections). Each `dir/<collection>/` is reopened via the
    /// re-seal-capable `Collection::open_from_segments` (no CBOR snapshot, no
    /// whole-collection load — the forward payload stays demand-paged on the
    /// mmaps). The returned seq is the WAL position the binary tails from.
    pub fn reopen_from_segment_dir(&self, dir: &std::path::Path) -> Result<u64> {
        self.reopen_from_segment_dir_with_vectors(dir, false)
    }

    pub(crate) fn reopen_from_segment_dir_with_vectors(
        &self,
        dir: &std::path::Path,
        defer_hnsw: bool,
    ) -> Result<u64> {
        self.reopen_from_segment_dir_with_base_rows(dir, defer_hnsw, &BTreeMap::new(), None)
    }

    pub(crate) fn reopen_from_segment_dir_with_base_rows(
        &self,
        dir: &std::path::Path,
        defer_hnsw: bool,
        mapped_rows: &BTreeMap<String, BTreeMap<String, Vec<String>>>,
        recovery_profile: Option<&RecoveryProfile>,
    ) -> Result<u64> {
        let _apply = self.capture_barrier.apply();
        if !dir.exists() {
            return Ok(0);
        }
        let mut max_seq = 0u64;
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for entry in std::fs::read_dir(dir)
            .map_err(|e| anyhow!("read checkpoint dir {}: {e}", dir.display()))?
        {
            let entry = entry.map_err(|e| anyhow!("read checkpoint entry: {e}"))?;
            let coll_dir = entry.path();
            if !coll_dir.is_dir() {
                continue;
            }
            let schema_path = coll_dir.join(CHECKPOINT_SCHEMA_FILE);
            if !schema_path.exists() {
                continue; // not a collection checkpoint subdir
            }
            let json = std::fs::read(&schema_path)
                .map_err(|e| anyhow!("read checkpoint schema {}: {e}", schema_path.display()))?;
            let sidecar: CheckpointSchema = serde_json::from_slice(&json)
                .map_err(|e| anyhow!("decode checkpoint schema {}: {e}", schema_path.display()))?;
            let name = collection_name_from_dir(&coll_dir)
                .ok_or_else(|| anyhow!("undecodable checkpoint subdir {}", coll_dir.display()))?;
            let collection_started = recovery_profile.map(|_| Instant::now());
            let mut coll = Collection::open_from_segments_with_vectors(
                &coll_dir,
                sidecar.fields,
                sidecar.version,
                defer_hnsw,
                sidecar.segment_layout,
                mapped_rows.get(&name),
                recovery_profile,
            )?;
            if let (Some(profile), Some(started)) = (recovery_profile, collection_started) {
                profile.collection_opened(started.elapsed());
            }
            coll.collection_generation = state.allocate_collection_generation()?;
            max_seq = max_seq.max(sidecar.applied_seq);
            state.collections.insert(name, coll);
        }
        drop(state);
        self.report_reindex_needed("segment checkpoint");
        Ok(max_seq)
    }

    /// PUBLIC PROBE (Stage 2 disk-tier validation): report whether a field's
    /// in-RAM forward/inverted driver has been DROPPED to disk and a segment is
    /// attached, so an out-of-crate consumer (the disk perf gate integration
    /// test) can assert the query path is GENUINELY segment-driven rather than
    /// silently still in RAM. Returns `(forward_or_tokens_len, has_segment)` for
    /// the named field: after a `flush_to_segments`, a sealed field reads
    /// `forward_or_tokens_len == 0` (driver dropped) AND `has_segment == true`
    /// (mmap attached). Mirrors the in-crate `__field_forward_probe`, but is a
    /// real `pub` API (not `#[cfg(test)]`) so `tests/perf_gate_vs_db.rs` — a
    /// separate crate — can read it. Experimental-gated alongside the rest of
    /// the disk tier.
    pub fn segment_field_probe(&self, collection_id: &str, field: &str) -> Result<(usize, bool)> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let fi = coll
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        Ok(match fi {
            FieldIndex::Number(n) => (n.forward_len(), n.segment.is_some()),
            FieldIndex::Hash(h) => (h.forward.len(), h.segment.is_some()),
            FieldIndex::Keyword(k) => (k.forward_len(), k.segment.is_some()),
            FieldIndex::Set(s) => (s.forward.len(), s.segment.is_some()),
            FieldIndex::Text { idx, .. } => (idx.tokens.len(), idx.segment.is_some()),
            FieldIndex::Vector { idx, .. } => (
                idx.resident_vector_payload_rows(),
                idx.has_checkpoint_mapping(),
            ),
        })
    }
}
