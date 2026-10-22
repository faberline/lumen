//! Column access shared by every value type: directory lookups, zero-copy fixed
//! columns, var-block decode through the cache, and presence.

use std::borrow::Cow;
use std::sync::Arc;

use crate::persistence::infrastructure::segment::codecs::decode_docid_block;
use crate::persistence::infrastructure::segment::reader_cache::{
    posting_cache_key, CachedPosting, DecodedBlock,
};
use crate::persistence::infrastructure::segment::var_column::{
    SparseVarIndex, VarBlockBody, VarBlockMeta,
};
use crate::persistence::infrastructure::segment::{
    ColumnRef, SegmentReader, CODEC_LZ4_VAR, CODEC_RAW_VAR, ROLE_DICT, ROLE_DICT_OFFSETS,
    ROLE_PRESENT,
};

impl SegmentReader {
    /// The WAL sequence this segment is current as of.
    pub fn applied_seq(&self) -> u64 {
        self.applied_seq
    }

    /// Number of doc rows in this segment.
    pub fn n_docs(&self) -> u32 {
        self.n_docs
    }

    /// Locate the first directory entry with the given role.
    fn column_index(&self, role: u8) -> Option<usize> {
        self.dir.iter().position(|column| column.role == role)
    }

    pub(super) fn column(&self, role: u8) -> Option<&ColumnRef> {
        self.column_index(role)
            .and_then(|index| self.dir.get(index))
    }

    /// Borrow a column's bytes, bounds-checked against the mmap. A column ref
    /// pointing past the (possibly truncated) file yields `None` rather than a
    /// panic.
    pub(super) fn column_bytes(&self, col: &ColumnRef) -> Option<&[u8]> {
        let off = usize::try_from(col.byte_offset).ok()?;
        let len = usize::try_from(col.byte_len).ok()?;
        let end = off.checked_add(len)?;
        self.mmap.get(off..end)
    }

    /// Borrow a FIXED `u32` column's elements zero-copy off the mmap. `None`
    /// for a torn/misaligned column ref — never panics. The Keyword dict-id and
    /// Set offsets/packed columns are all read through this.
    pub(super) fn u32_column(&self, role: u8) -> Option<&[u32]> {
        let col = self.column(role)?;
        let bytes = self.column_bytes(col)?;
        bytemuck::try_cast_slice::<u8, u32>(bytes).ok()
    }

    fn u64_column(&self, role: u8) -> Option<&[u64]> {
        let col = self.column(role)?;
        let bytes = self.column_bytes(col)?;
        bytemuck::try_cast_slice::<u8, u64>(bytes).ok()
    }

    /// Raw dictionaries are borrowed directly from the mapping.  Older LZ4
    /// dictionaries still decode through the bounded block cache and therefore
    /// return an owned fallback.
    pub(in crate::persistence::infrastructure::segment) fn dict_value(
        &self,
        id: u32,
    ) -> Option<Cow<'_, str>> {
        let dict = self.column(ROLE_DICT)?;
        match dict.codec {
            CODEC_RAW_VAR => {
                let offsets = self.u64_column(ROLE_DICT_OFFSETS)?;
                let start = usize::try_from(*offsets.get(id as usize)?).ok()?;
                let end = usize::try_from(*offsets.get(id as usize + 1)?).ok()?;
                let bytes = self.column_bytes(dict)?.get(start..end)?;
                std::str::from_utf8(bytes).ok().map(Cow::Borrowed)
            }
            CODEC_LZ4_VAR => {
                let (block, within) = self.dict_block_at(ROLE_DICT, id)?;
                String::from_utf8(block.get(within)?.clone())
                    .ok()
                    .map(Cow::Owned)
            }
            _ => None,
        }
    }

    /// Borrow the parsed per-column skip-index. A malformed index was recorded
    /// as `None` during open, matching the former lookup refusal behavior.
    fn var_skip_index(&self, column_index: usize) -> Option<&SparseVarIndex> {
        self.var_skip_indices.get(column_index)?.as_ref()
    }

    /// Decompress one var block (or return the cached copy). The block frame is
    /// `u32` compressed-length + LZ4(cbor([`VarBlockBody`])); a truncated /
    /// corrupt frame yields `None` (never panics — the discard-on-torn gate is
    /// the directory crc, so an in-range frame should always decode, but we
    /// honor the no-panic discipline regardless). Caches the reconstructed
    /// strings in the moka byte-weighted cache keyed by `meta.offset`.
    fn decode_var_block(&self, meta: &VarBlockMeta) -> Option<DecodedBlock> {
        if let Some(hit) = self.block_cache.get(&meta.offset) {
            return Some(hit);
        }
        let off = usize::try_from(meta.offset).ok()?;
        // The frame is [u32 len][compressed...]; the len prefix must match the
        // directory's recorded length.
        let len_end = off.checked_add(4)?;
        let len_bytes = self.mmap.get(off..len_end)?;
        let frame_len = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
        if frame_len != meta.length as usize {
            return None;
        }
        let comp_end = len_end.checked_add(frame_len)?;
        let compressed = self.mmap.get(len_end..comp_end)?;
        let raw = lz4_flex::decompress_size_prepended(compressed).ok()?;
        let body: VarBlockBody = ciborium::from_reader(&raw[..]).ok()?;
        let decoded: DecodedBlock = Arc::new(body.reconstruct());
        self.block_cache.insert(meta.offset, decoded.clone());
        Some(decoded)
    }

    /// Resolve a dict-id in a VAR dict column to its owning decoded block plus
    /// the in-block position. Binary-searches the skip-index to the rightmost
    /// block with `first_entry <= id`, decompresses it (cache-on-touch), and
    /// bounds-checks the position. `None` for an out-of-range id or a torn
    /// block — never panics.
    pub(in crate::persistence::infrastructure::segment) fn dict_block_at(
        &self,
        dict_role: u8,
        id: u32,
    ) -> Option<(DecodedBlock, usize)> {
        let column_index = self.column_index(dict_role)?;
        let col = self.dir.get(column_index)?;
        if id as u64 >= col.elem_count {
            return None;
        }
        let index = self.var_skip_index(column_index)?;
        let mut lo = 0usize;
        let mut hi = index.blocks.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if index.blocks[mid].first_entry <= id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return None;
        }
        let meta = &index.blocks[lo - 1];
        let within = id.checked_sub(meta.first_entry)? as usize;
        if within >= meta.entry_count as usize {
            return None;
        }
        let block = self.decode_var_block(meta)?;
        if within >= block.len() {
            return None;
        }
        Some((block, within))
    }

    /// Fetch a docid-only posting at `(role, id)` through the BOUNDED posting
    /// cache (Phase 2m). On a hit, return the resident `Arc<RoaringBitmap>` (a
    /// refcount bump — no re-decode). On a miss, decode the posting block at this
    /// id (`dict_block_at` + `decode_docid_block`), insert the `Arc`, and return
    /// it. `role` selects the posting column (`ROLE_KEYWORD_POSTINGS` /
    /// `ROLE_SET_POSTINGS` / `ROLE_NUMBER_POSTINGS`); `id` is the dict-id
    /// (Keyword/Set) or sorted-value index (Number). The cached bitmap is the RAW
    /// immutable posting a fresh decode would produce — the `storage.rs` accessor
    /// subtracts the tombstone AFTER this call, so the result stays
    /// byte-identical. `None` for an out-of-range id or a torn block (never
    /// panics; a torn block is never cached).
    pub(super) fn cached_docid_postings(&self, role: u8, id: u32) -> Option<CachedPosting> {
        let key = posting_cache_key(role, id);
        if let Some(hit) = self.posting_cache.get(&key) {
            return Some(hit);
        }
        let (block, within) = self.dict_block_at(role, id)?;
        let blob = block.get(within)?;
        let docids = decode_docid_block(blob)?;
        let bitmap: CachedPosting = Arc::new(docids.into_iter().collect());
        self.posting_cache.insert(key, bitmap.clone());
        Some(bitmap)
    }

    /// Resolve a dict-id to its decoded UTF-8 string via the [`ROLE_DICT`]
    /// column. `None` for an out-of-range id, a torn block, or non-UTF-8 bytes
    /// (a real dict always holds valid UTF-8 strings). The returned `String` is
    /// owned because the bytes live in the moka-cached decompressed block, not
    /// on the mmap page — see [`Self::keyword_at`].
    pub(in crate::persistence::infrastructure::segment) fn dict_string(
        &self,
        id: u32,
    ) -> Option<String> {
        self.dict_value(id).map(Cow::into_owned)
    }

    /// `true` if doc `id` is in range AND its present-bit is set. `false` for
    /// an out-of-range id, an absent doc, or a torn/misaligned present column —
    /// never panics. Every per-doc reader gates on this first.
    pub(super) fn is_present(&self, id: u32) -> bool {
        if id >= self.n_docs {
            return false;
        }
        let Some(present) = self.column(ROLE_PRESENT) else {
            return false;
        };
        let Some(present_bytes) = self.column_bytes(present) else {
            return false;
        };
        // try_cast_slice (NOT cast_slice): a truncated/misaligned column is an
        // Err we turn into false, never a panic.
        let Ok(present_words) = bytemuck::try_cast_slice::<u8, u64>(present_bytes) else {
            return false;
        };
        let Some(word) = present_words.get((id as usize) / 64) else {
            return false;
        };
        (word >> (id % 64)) & 1 == 1
    }
}
