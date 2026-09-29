//! Columnar mmap disk segment — Stage 2 disk-tier (Phase 0 + 2a + 2b).
//!
//! One file = one Number column for `n_docs` rows at a single `applied_seq`.
//! The layout is little-endian and read **tail-first**: the 24-byte footer at
//! the very end points back at a CBOR directory, which in turn locates the
//! page-aligned fixed-width columns. Reads are zero-copy (`mmap` +
//! `bytemuck::try_cast_slice`); a torn / truncated / endian-mismatched file is
//! always reported as an error and **never panics** — the caller discards it
//! and replays from the log, there is no in-place recovery.
//!
//! This module is compiled by default; the disk tier is selected at runtime
//! (`--persistence=segment`), while the in-memory serving engine keeps the CBOR
//! RDB as its default.
//!
//! Phase 2c wired the READER's per-doc lookups (`SegmentReader::number_at` /
//! `n_docs`) into the Number PREDICATE path in `storage.rs`, so those are always
//! live. Phase 2h-3 (WS2 BKD) added the Number RANGE
//! index — a fixed-width ascending `u64[distinct]` SORTED-VALUE column
//! (`ROLE_NUMBER_SORTED`) binary-searched by `number_range` + a parallel
//! per-value docid posting column (`ROLE_NUMBER_POSTINGS`) — so range / exact /
//! boolean queries drive straight off the mmap and the in-RAM `values` BTreeMap
//! is dropped at seal. The WRITER (`write_number_segment`) and
//! `SegmentReader::open` / `applied_seq` are driven by the test seal seam and by
//! the runtime segment-persistence path; in the default (CBOR) configuration they
//! are reachable only from that runtime path, so the module silences dead-code
//! there rather than carry unused `pub` plumbing. The read path that serves live
//! queries is fully exercised and is NOT covered by this allow.
#![cfg_attr(not(test), allow(dead_code))]

pub(crate) mod codecs;
pub(crate) mod eid_writer;
pub(crate) mod format;
pub(crate) mod hash_writer;
pub(crate) mod keyword_writer;
pub(crate) mod number_writer;
pub(crate) mod reader_cache;
pub(crate) mod segment_reader;
pub(crate) mod set_writer;
pub(crate) mod sparse_rows;
pub(crate) mod stream;
pub(crate) mod text_row_stage;
pub(crate) mod text_writer;
pub(crate) mod var_column;
pub(crate) mod vector_writer;

use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;

use serde::{Deserialize, Serialize};

use crate::persistence::infrastructure::segment::reader_cache::{
    CachedPosting, CachedTextPosting, DecodedBlock,
};
use crate::persistence::infrastructure::segment::var_column::SparseVarIndex;

#[cfg(test)]
thread_local! {
    pub(crate) static DICTIONARY_SEARCHES: Cell<usize> = const { Cell::new(0) };
    static VAR_SKIP_INDEX_DECODES: Cell<usize> = const { Cell::new(0) };
    static OWNED_STAGE_OPEN_CALLS: Cell<usize> = const { Cell::new(0) };
}

/// Header magic, "LSEG" little-endian.
const MAGIC1: u32 = 0x4C53_4547;
/// Footer magic, "GESL" little-endian.
const MAGIC2: u32 = 0x4753_454C;
/// On-disk format version.
const FORMAT_VER: u32 = 1;
/// Endianness stamp written into the header. On read, a mismatch means the
/// file was produced on a host with a different byte order and must be
/// discarded (we do not byte-swap — discard-and-replay is cheaper than a
/// portable codec for a hot column).
const HOST_ENDIAN_MARKER: u32 = 0x0102_0304;
/// Page size we align fixed-width columns to. mmap hands back a page-aligned
/// base pointer, so a page-aligned column offset yields a `u64`-aligned slice.
const PAGE: usize = 4096;
/// Header is a fixed, zero-padded 4096-byte block at the file start.
const HEADER_LEN: usize = 4096;
/// Footer is a fixed 24-byte block at the file end: dir_offset(8) + dir_len(8)
/// + crc32(4) + magic2(4).
const FOOTER_LEN: usize = 24;

/// Column role discriminant, persisted in [`ColumnRef::role`].
const ROLE_NUMBER: u8 = 0;
const ROLE_PRESENT: u8 = 1;
/// Hash forward column (raw `u64` perceptual hash per doc). Phase 2d.
const ROLE_HASH: u8 = 2;
/// Vector forward column (contiguous `f32[n_docs * dim]`). Phase 2d.
const ROLE_VECTOR: u8 = 3;
/// A prefix-compressed, LZ4-blocked, variable-width string dictionary. Stored
/// in the VAR region after the fixed region; located via the per-column
/// skip-index carried in [`ColumnRef::skip_index`]. Phase 2e-A. Shared by the
/// Keyword and Set sealed columns.
const ROLE_DICT: u8 = 4;
/// Keyword forward column (`u32[n_docs]` dict-id per doc). Phase 2e-A. FIXED
/// width — stays zero-copy via `try_cast_slice`. A sentinel of [`DICT_ABSENT`]
/// marks a doc with no keyword (also gated by the present bitset).
const ROLE_KEYWORD_DICTID: u8 = 5;
/// Set CSR offsets column (`u32[n_docs + 1]`). Phase 2e-A. FIXED width. Doc
/// `i`'s member dict-ids are `packed[offsets[i]..offsets[i+1]]`.
const ROLE_SET_OFFSETS: u8 = 6;
/// Set packed member dict-ids column (`u32[total_members]`). Phase 2e-A. FIXED
/// width, indexed through the CSR offsets column.
const ROLE_SET_PACKED: u8 = 7;
/// Text per-token POSTING-BLOCK column (var-width, LZ4-blocked). Phase 2e-B.
/// Parallel to the [`ROLE_DICT`] token dictionary: the entry at dict-id `t` is
/// token `t`'s posting blob (a delta-varint `docid`-gap + `tf` stream — see
/// [`encode_posting_block`](codecs::encode_posting_block)). Text term-frequency
/// is NOT rebuildable, so unlike the Keyword/Set inverted indexes the postings
/// ARE stored on disk.
const ROLE_TEXT_POSTINGS: u8 = 8;
/// Text per-doc length column (`u32[n_docs]`). Phase 2e-B. FIXED width. Doc
/// `i`'s BM25 length is `doclen[i]` (zero may be an explicit empty value).
/// Read zero-copy; the separate present bitset distinguishes empty from absent.
const ROLE_TEXT_DOCLEN: u8 = 9;
/// Collection-level external-id DICTIONARY-by-position column (var-width,
/// LZ4-blocked). Phase 2f-1. UNLIKE [`ROLE_DICT`] (which is the SORTED distinct
/// dictionary of a Keyword/Set field), this column stores the external_id string
/// of docid `i` at entry position `i` — i.e. it is the interner's `to_eid` Vec
/// laid out densely in docid order `[0..n_docs)`. It makes a sealed collection
/// SELF-DESCRIBING: reopening the collection rebuilds the whole `Interner` by
/// scanning this column, with no CBOR whole-collection snapshot. Written once
/// per collection seal into `<collection>.lmeta.lseg`. The entries are NOT
/// sorted (docid order, not lexical), so the var-column prefix-delta finds
/// `shared == 0` for unrelated eids and LZ4 does the real compression.
const ROLE_EID: u8 = 10;
/// Keyword per-term INVERTED posting-block column (var-width, LZ4-blocked).
/// Phase 2h-1. Parallel to the Keyword [`ROLE_DICT`] string dictionary: the
/// entry at dict-id `t` is term `t`'s sorted-docid posting blob (a delta-varint
/// `docid`-gap stream with NO tf — a Keyword term holds a doc at most once, so
/// the only fact is membership; see
/// [`encode_docid_block`](codecs::encode_docid_block)). Stored ON DISK so a
/// reopen drives Term/Terms/boolean queries straight off the mmap WITHOUT
/// rebuilding the in-RAM `terms: BTreeMap<String, RoaringBitmap>` inverted index.
/// The per-term doc-count (`df`) is the cheap LEB128 count prefix of the blob
/// (mirrors [`SegmentReader::text_token_df`]), keeping the boolean planner's
/// rarest-first clause ordering cheap. Located by the term's dict index via
/// [`SegmentReader::dict_block_at`].
const ROLE_KEYWORD_POSTINGS: u8 = 11;
/// Set per-element INVERTED posting-block column (var-width, LZ4-blocked).
/// Phase 2h-2. The Set analogue of [`ROLE_KEYWORD_POSTINGS`]: parallel to the
/// Set [`ROLE_DICT`] string dictionary, the entry at dict-id `t` is element
/// `t`'s sorted-docid posting blob — the docids whose set CONTAINS element `t`
/// (a delta-varint `docid`-gap stream with NO tf; a Set holds an element at
/// most once per doc, so the only fact is membership; see
/// [`encode_docid_block`](codecs::encode_docid_block)). Stored ON DISK so a
/// reopen drives membership /
/// Terms / boolean queries straight off the mmap WITHOUT rebuilding the in-RAM
/// `elements: BTreeMap<String, RoaringBitmap>` inverted index (which grows
/// O(distinct set elements)). The per-element doc-count (`df`) is the cheap
/// LEB128 count prefix of the blob, keeping the boolean planner's rarest-first
/// clause ordering cheap. Located by the element's dict index via
/// [`SegmentReader::dict_block_at`].
const ROLE_SET_POSTINGS: u8 = 12;
/// Number SORTED-VALUE range index — the ascending DISTINCT `SortableF64` bit
/// keys (Phase 2h-3). FIXED-WIDTH `u64[distinct]` column: entry `i` is the raw
/// `SortableF64.0` of the `i`-th smallest distinct numeric value in the field.
/// The `SortableF64` transform makes UNSIGNED `u64` order == numeric order, so
/// this column is monotonically ascending and zero-copy binary-searchable on the
/// mmap (`try_cast_slice::<u8, u64>`). The bit keys are EXACTLY the in-RAM
/// `NumberIndex.values` BTreeMap keys, so a binary-search over this column with
/// the identical inclusive/exclusive bounds reproduces `values.range(..)`
/// byte-for-byte. Parallel to [`ROLE_NUMBER_POSTINGS`] (value `i`'s docids live
/// at posting index `i`). Stored ON DISK so a reopen drives range / exact /
/// boolean queries straight off the mmap WITHOUT rebuilding the in-RAM `values:
/// BTreeMap<SortableF64, RoaringBitmap>` index (which grows O(distinct numeric
/// values)).
const ROLE_NUMBER_SORTED: u8 = 13;
/// Number per-distinct-value INVERTED posting-block column (var-width,
/// LZ4-blocked). Phase 2h-3. Parallel to the [`ROLE_NUMBER_SORTED`] sorted-value
/// column: the entry at sorted index `i` is that value's sorted-docid posting
/// blob — the docids whose number EQUALS the `i`-th distinct value (a
/// delta-varint `docid`-gap stream with NO tf; see
/// [`encode_docid_block`](codecs::encode_docid_block)). The
/// per-value doc-count (`df`) is the cheap LEB128 count prefix of the blob,
/// keeping the boolean planner's selectivity input off a full posting decode.
/// Located by the value's index in the sorted column (the binary-search result),
/// NOT by a string dict-id — Number has no [`ROLE_DICT`].
const ROLE_NUMBER_POSTINGS: u8 = 14;
/// Byte offsets for a [`CODEC_RAW_VAR`] [`ROLE_DICT`].  This is deliberately a
/// separate fixed mmap column: `elem_count` is the number of dictionary terms,
/// while this column has `elem_count + 1` `u64` byte offsets.
const ROLE_DICT_OFFSETS: u8 = 15;

/// Codec discriminant for [`ColumnRef::codec`]. A fixed-width column is stored
/// raw (zero-copy `try_cast_slice` on read); a var-width column is LZ4-blocked.
const CODEC_FIXED: u8 = 0;
/// LZ4-framed variable-width blocks (the var-column codec). Phase 2e-A.
const CODEC_LZ4_VAR: u8 = 1;
/// Raw concatenated UTF-8 dictionary bytes.  Only [`ROLE_DICT`] may use this
/// codec; [`ROLE_DICT_OFFSETS`] supplies its mmap-resident entry boundaries.
const CODEC_RAW_VAR: u8 = 2;

/// Sentinel dict-id meaning "no keyword for this doc" in the Keyword forward
/// column. `u32::MAX` can never be a real dict-id (a dict that large would
/// overflow the file long before reaching it), so it is an unambiguous absent
/// marker independent of the present bitset.
const DICT_ABSENT: u32 = u32::MAX;

/// Soft target for a var-column LZ4 block's *uncompressed* size: 64 KB. A block
/// is flushed once the accumulated raw bytes reach this cap; a single oversized
/// entry may exceed it.
const VAR_BLOCK_BYTES: usize = 64 * 1024;

/// Round `n` up to the next multiple of [`PAGE`].
#[inline]
fn page_align(n: usize) -> usize {
    (n + PAGE - 1) & !(PAGE - 1)
}

/// A directory entry locating one fixed-width column inside the file. CBOR is
/// only read once on open (cold path), so its overhead is irrelevant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRef {
    pub name: String,
    /// `ROLE_*` discriminant identifying what the column holds.
    pub role: u8,
    /// `byte_offset` of the column's bytes. For a FIXED column this is the
    /// page-aligned start of the raw element array; for a VAR (LZ4) column it
    /// is the start of the column's first block in the VAR region.
    pub byte_offset: u64,
    pub byte_len: u64,
    /// FIXED: element count of the raw array. VAR: number of logical entries
    /// (dictionary cardinality) across all blocks.
    pub elem_count: u64,
    /// FIXED: element width in bytes. VAR: 0 (entries are variable width).
    pub width: u8,
    /// `CODEC_FIXED` (default — raw, zero-copy) or `CODEC_LZ4_VAR` (the
    /// var-width blocked codec). Defaults to `CODEC_FIXED` so a segment written
    /// before Phase 2e-A (no `codec` in its CBOR) decodes its fixed columns
    /// unchanged.
    #[serde(default)]
    pub codec: u8,
    /// VAR only: the per-column skip-index bytes (CBOR of [`SparseVarIndex`]),
    /// carried inline in the directory so a var read needs no second seek. A
    /// FIXED column leaves this empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_index: Vec<u8>,
}

/// The fixed-size header fields (the on-disk header is this prefix followed by
/// zero padding out to [`HEADER_LEN`]). Not a bytemuck `Pod` because a 4072-byte
/// pad array exceeds bytemuck's derived-trait array support; the few scalar
/// fields are written/read explicitly little-endian via `byteorder` instead.
struct Header {
    magic1: u32,
    format_ver: u32,
    applied_seq: u64,
    host_endian_marker: u32,
    n_docs: u32,
    /// Phase 2e-B: BM25 corpus scalars for a Text segment (the `N` and the
    /// `total_doc_len` that derive `avgdl`). Zero for every non-text segment,
    /// and zero in any segment written before 2e-B — the header block is always
    /// the zero-padded [`HEADER_LEN`], so an older file reads these back as 0
    /// and stays decodable.
    doc_count: u64,
    total_doc_len: u64,
}

/// The fixed 24-byte footer at the file tail. Read field-by-field little-endian
/// rather than via a zero-copy cast: the footer starts at `file_len - 24`,
/// which is not 8-byte aligned for an arbitrary file length, so a `bytemuck`
/// cast to an alignment-8 struct would fail. A 24-byte cold read is cheap.
#[derive(Clone, Copy)]
struct Footer {
    dir_offset: u64,
    dir_len: u64,
    crc32: u32,
    magic2: u32,
}

/// A zero-copy, read-only view over a segment file. `mmap` is the page-aligned
/// kernel mapping; `dir` and the scalar fields are owned (decoded once on
/// open). FIXED columns (Number/Hash/Vector forward, Keyword dict-id, Set CSR
/// offsets + packed) are read zero-copy via `try_cast_slice`. VAR columns (the
/// Keyword/Set string dictionaries) are LZ4-blocked: a block is decompressed on
/// first touch and cached in `block_cache` (a moka byte-weighted cache), keyed
/// by the block's file offset.
///
/// `Send + Sync`: `Arc<Mmap>` + owned fields + `moka::sync::Cache` are all
/// `Send + Sync`; there is no self-referential borrow or `'static` transmute.
pub struct SegmentReader {
    mmap: Arc<memmap2::Mmap>,
    dir: Vec<ColumnRef>,
    /// Parsed once at open, in the same order as `dir`. A malformed VAR skip
    /// index remains `None`, preserving the prior no-panic lookup refusal.
    var_skip_indices: Vec<Option<SparseVarIndex>>,
    applied_seq: u64,
    n_docs: u32,
    /// Phase 2e-B BM25 corpus scalars (0 for a non-text segment): the document
    /// count `N` and the summed document length `Σ|d|`. `avgdl` is derived from
    /// these so the sealed BM25 path uses the identical `n` / `avgdl` the live
    /// `TextIndex` held.
    doc_count: u64,
    total_doc_len: u64,
    /// Decompressed var-block cache, keyed by the block's byte offset in the
    /// file. Byte-weighted to `DEFAULT_VAR_CACHE_BYTES`.
    block_cache: moka::sync::Cache<u64, DecodedBlock>,
    /// BOUNDED decoded-posting cache (Phase 2m): RAW immutable docid-only
    /// postings (Keyword / Set / Number value), keyed by a packed `(role, id)`
    /// (see [`posting_cache_key`](reader_cache::posting_cache_key)). A hit returns the resident
    /// `Arc<RoaringBitmap>` (refcount bump); a miss decodes the posting block and
    /// inserts. Byte-weighted to
    /// [`posting_cache_bytes`](reader_cache::posting_cache_bytes) so the 2i RSS bound
    /// holds. The `storage.rs` accessors subtract the per-field tombstone AFTER
    /// the fetch, so cached results stay byte-identical.
    posting_cache: moka::sync::Cache<u64, CachedPosting>,
    /// BOUNDED Text decoded-posting cache (Phase 2m): the `(docids, tfs)` SoA for
    /// a token, keyed by its dict-id. Separate from [`Self::posting_cache`]
    /// because Text carries a parallel `tf` stream the docid-only cache cannot
    /// hold. Same budget + tombstone-after-fetch discipline.
    text_posting_cache: moka::sync::Cache<u64, CachedTextPosting>,
    /// A locally authored, private staging directory.  Its final `Arc` drop
    /// removes only this exact directory after the mmap and every reader clone
    /// have released it.
    owned_stage: Option<Arc<OwnedStageDirectory>>,
}

struct OwnedStageDirectory {
    path: PathBuf,
}

impl Drop for OwnedStageDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl std::fmt::Debug for SegmentReader {
    /// A terse, allocation-free summary — the mmap bytes and CBOR directory are
    /// not interesting in a `{:?}` dump and would be huge. Lets containers that
    /// hold a `SegmentReader` keep their `#[derive(Debug)]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("applied_seq", &self.applied_seq)
            .field("n_docs", &self.n_docs)
            .field("columns", &self.dir.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarPayloadKind {
    Keyword,
    Number,
    Set,
}

#[cfg(test)]
mod tests;
