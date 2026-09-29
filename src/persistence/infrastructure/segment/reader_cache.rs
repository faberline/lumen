//! The reader's bounded caches: decoded var blocks and decoded postings,
//! weighted by their retained bytes.

use std::sync::Arc;

// ---------------------------------------------------------------------------
// Reader (zero-copy)
// ---------------------------------------------------------------------------

/// A decoded var-column block: the reconstructed full byte strings, in dict-id
/// order. Cached behind an `Arc` so the moka cache hands out cheap clones.
pub(super) type DecodedBlock = Arc<Vec<Vec<u8>>>;

/// Approximate retained-byte weight of a decoded var block, for the moka
/// byte-budget: the string bytes plus a small per-entry overhead.
pub(super) fn decoded_block_weight(block: &DecodedBlock) -> u32 {
    let mut w: usize = 32;
    for s in block.iter() {
        w = w.saturating_add(s.len() + 24);
    }
    w.min(u32::MAX as usize) as u32
}

/// Default decompressed-var-block cache budget. The fixed columns are served
/// zero-copy off the mmap and never touch this cache; only the prefix-delta
/// LZ4 dictionary blocks are decoded and cached here. 16 MiB comfortably holds
/// the dictionaries of a typical sealed segment.
pub(super) const DEFAULT_VAR_CACHE_BYTES: u64 = 16 * 1024 * 1024;

// ---------------------------------------------------------------------------
// BOUNDED DECODED-POSTING CACHE (Phase 2m)
// ---------------------------------------------------------------------------
//
// The decoded-posting accessors (`keyword_postings` / `set_postings` /
// `number_value_postings` / `number_range` / `text_postings`) re-`decode_*`-d a
// FRESH `RoaringBitmap` (or `(docids, tfs)`) per call off the mmap. The forward
// payload is already demand-paged + warmed by the OS page cache, but the
// INVERTED postings had no such hot-zone, so a repeated low-cardinality
// number/range/sort/keyword query paid the whole delta-varint decode every time
// (5x-525x slower than the in-RAM driver that held the bitmaps resident).
//
// This is a BOUNDED, weight-capped cache of DECODED postings — the inverted-index
// analogue of the OS page cache for the forward payload. It holds the RAW
// immutable posting a fresh decode would produce; the `storage.rs` accessors keep
// subtracting the per-field tombstone AFTER the cache fetch, so RESULTS are
// byte-identical. Bounded by a serialized-size weigher + a configurable cap
// (`LUMEN_SEG_POSTING_CACHE_MB`, default 64 MiB), so the 2i scale-proof RSS bound
// still holds (the cap prevents O(cardinality) resident growth); warm => repeated
// queries hit the resident `Arc<RoaringBitmap>` => competitive.
//
// Keyed by a packed `(role, id) -> u64`: `role` distinguishes the column
// (`ROLE_KEYWORD_POSTINGS` / `ROLE_SET_POSTINGS` / `ROLE_NUMBER_POSTINGS`), and
// `id` is the dict-id (Keyword/Set) or the sorted-value index (Number). Both id
// spaces are dense and segment-local, so the pack is collision-free within one
// reader. Text postings carry a parallel `tf` stream, so they use a separate
// `(docids, tfs)` cache keyed by dict-id.

/// A resident decoded docid-only posting (Keyword / Set / Number value). Behind
/// an `Arc` so the moka cache hands out cheap clones — a hit is a refcount bump,
/// not a re-decode.
pub(super) type CachedPosting = Arc<roaring::RoaringBitmap>;

/// A resident decoded Text posting: the `(docids, tfs)` SoA streams the live
/// `Postings` held. `Arc`-shared like [`CachedPosting`].
pub(crate) type CachedTextPosting = Arc<(Vec<u32>, Vec<u32>)>;

/// Pack a `(role, id)` pair into a collision-free `u64` cache key. `role` is a
/// `ROLE_*` discriminant (small); `id` is a dense segment-local dict-id or
/// sorted-value index (`< 2^32`), so the high byte carries the role and the low
/// 32 bits carry the id with room to spare.
#[inline]
pub(super) fn posting_cache_key(role: u8, id: u32) -> u64 {
    ((role as u64) << 32) | (id as u64)
}

/// Approximate retained-byte weight of a cached docid posting, for the moka
/// byte-budget. A `RoaringBitmap`'s serialized size is the honest on-heap proxy
/// for its container payload (array/bitset/run blocks); a small constant covers
/// the `Arc` + map-entry overhead so a cache of many tiny postings is still
/// bounded.
pub(super) fn cached_posting_weight(p: &CachedPosting) -> u32 {
    let bytes = p.serialized_size().saturating_add(64);
    bytes.min(u32::MAX as usize) as u32
}

/// Approximate retained-byte weight of a cached Text posting (`docids` + `tfs`
/// `u32` vectors plus a small constant).
pub(crate) fn cached_text_posting_weight(p: &CachedTextPosting) -> u32 {
    let (docids, tfs) = p.as_ref();
    let bytes = docids
        .len()
        .saturating_add(tfs.len())
        .saturating_mul(4)
        .saturating_add(64);
    bytes.min(u32::MAX as usize) as u32
}

/// Default decoded-posting cache budget (per reader): 64 MiB. Big enough that a
/// warm working set of low-cardinality number/range/sort/keyword postings stays
/// resident (so repeated queries are RAM-speed), small enough that the 2i
/// scale-proof RSS bound still holds (the cap prevents O(cardinality) growth).
/// Overridable via `LUMEN_SEG_POSTING_CACHE_MB`.
const DEFAULT_POSTING_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// The decoded-posting cache byte budget — `LUMEN_SEG_POSTING_CACHE_MB` (MiB) if
/// set and parseable, else [`DEFAULT_POSTING_CACHE_BYTES`]. A value of `0`
/// disables the cache (max_capacity 0 ⇒ every insert is immediately evicted, so
/// the accessors fall back to a fresh decode — the pre-cache behaviour).
pub(crate) fn posting_cache_bytes() -> u64 {
    match std::env::var("LUMEN_SEG_POSTING_CACHE_MB") {
        Ok(s) => match s.trim().parse::<u64>() {
            Ok(mb) => mb.saturating_mul(1024 * 1024),
            Err(_) => DEFAULT_POSTING_CACHE_BYTES,
        },
        Err(_) => DEFAULT_POSTING_CACHE_BYTES,
    }
}
