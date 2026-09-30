//! A hash field's index: each doc's 64-bit hash for the Hamming scan, over a
//! sealed segment base plus the live tail, and the parse of a hash field value.

use anyhow::{anyhow, Result};
use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

/// A `hash` field: docid → 64-bit hash, answered by a brute-force Hamming scan.
/// No LSH bucketing yet (linear over the forward map) — correct, not yet
/// sub-linear; perceptual-hash corpora are typically small relative to text.
#[derive(Debug, Default)]
pub(in crate::index) struct HashIndex {
    pub(in crate::index) forward: FastHashMap<u32, u64>,
    pub(in crate::index) bytes: u64,
    pub(in crate::index) tombstones: RoaringBitmap,
    /// Stage 2 disk-tier (Phase 2d): a sealed columnar mmap segment covering
    /// doc ids `[0..n_docs)`. When present, the per-doc hash read in the Hamming
    /// scan (`hash_at`) reads the segment for sealed ids and the in-RAM
    /// `forward` tail for ids `>= n_docs`. DEFAULTS to `None`; while it is
    /// `None` (nothing sealed) the read
    /// path is byte-for-byte the in-RAM path. Purely additive.
    pub(in crate::index) segment: Option<std::sync::Arc<ComposedSegmentReader>>,
}

impl HashIndex {
    /// The doc's 64-bit hash for the per-doc Hamming read. When a sealed segment
    /// is attached it serves ids in its covered range `[0..n_docs)` (the live
    /// tail keeps ids `>= n_docs`); otherwise — and always when no segment
    /// is attached — this is exactly `self.forward.get(&id)`. The segment stores the
    /// raw `u64`, so a hit is bit-equal to the live entry.
    #[inline]
    pub(in crate::index) fn hash_at(&self, id: u32) -> Option<u64> {
        if let Some(value) = self.forward.get(&id) {
            return Some(*value);
        }
        if self.tombstones.contains(id) {
            return None;
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() {
                return seg.hash_at(id);
            }
        }
        None
    }

    /// `true` once the sealed forward payload has been dropped to disk
    /// (Phase 2f-1) — the segment is attached. See `KeywordIndex::forward_dropped`.
    #[inline]
    fn forward_dropped(&self) -> bool {
        self.segment.is_some()
    }
}

/// Parse a `hash` field value: a 64-bit hex string, optionally `0x`-prefixed.
pub(in crate::index) fn parse_hash(s: &str) -> Result<u64> {
    parse_hash_number(s)
        .map_err(|e| anyhow!("hash field expects a 64-bit hex string (got `{s}`): {e}"))
}

/// Allocation-free numeric parse for retained WAL values. The ordinary error
/// response keeps its existing diagnostic through `parse_hash` above.
pub(in crate::index) fn parse_hash_number(
    s: &str,
) -> std::result::Result<u64, std::num::ParseIntError> {
    let t = s.trim();
    let hex = t
        .strip_prefix("0x")
        .or_else(|| t.strip_prefix("0X"))
        .unwrap_or(t);
    u64::from_str_radix(hex, 16)
}
