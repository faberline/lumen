//! Keyword lookups: per-doc terms, term postings, ordinals and counts.

use std::borrow::Cow;

use crate::persistence::infrastructure::segment::codecs::{decode_docid_block, read_varint};
use crate::persistence::infrastructure::segment::{
    SegmentReader, DICT_ABSENT, ROLE_DICT, ROLE_KEYWORD_DICTID, ROLE_KEYWORD_POSTINGS,
};

impl SegmentReader {
    /// The keyword string for doc `id`, or `None` if `id` is out of range, the
    /// doc has no keyword (present-bit clear or a [`DICT_ABSENT`] dict-id), or
    /// any column is torn — never panics. The dict-id forward column is read
    /// zero-copy off the mmap; the resolved string is materialized from the
    /// moka-cached decompressed dictionary block.
    ///
    /// DEVIATION FROM SPEC (Part A): the brief asks for `keyword_at(id) ->
    /// Option<&str>` "zero-copy off the decompressed dict block". A `&str`
    /// borrowing `&self` is impossible once the decompressed block lives in the
    /// moka cache (the cache, not a `&self` field, owns the bytes — moka hands
    /// out `Arc` clones). The fixed dict-id column IS read zero-copy; only the
    /// final dictionary string is owned. Returning `String` is the minimal,
    /// sound API; the equality / membership compares in `storage.rs` work
    /// identically against an owned `String`.
    pub fn keyword_at(&self, id: u32) -> Option<String> {
        self.keyword_at_cow(id).map(Cow::into_owned)
    }

    /// Internal raw-or-legacy dictionary view for staging and merge paths.
    /// Raw scalar dictionaries borrow the mmap; LZ4 dictionaries allocate only
    /// the selected legacy term.
    pub(crate) fn keyword_at_cow(&self, id: u32) -> Option<Cow<'_, str>> {
        if !self.is_present(id) {
            return None;
        }
        let ids = self.u32_column(ROLE_KEYWORD_DICTID)?;
        let dict_id = *ids.get(id as usize)?;
        if dict_id == DICT_ABSENT {
            return None;
        }
        self.dict_value(dict_id)
    }

    /// The dict-id of keyword `value` in the shared sorted [`ROLE_DICT`]
    /// dictionary, or `None` if absent. Binary-searches the dict-id space
    /// comparing decoded dictionary bytes against `value` (one var block per
    /// probe, cache-on-touch). The Keyword and Text dicts share [`ROLE_DICT`]
    /// and the identical sorted layout, so this delegates to [`Self::text_dict_id`].
    /// Phase 2h-1. Never panics.
    pub(in crate::persistence::infrastructure::segment) fn keyword_dict_id(
        &self,
        value: &str,
    ) -> Option<u32> {
        self.text_dict_id(value)
    }

    /// Keyword `value`'s INVERTED posting list as an ascending docid
    /// `RoaringBitmap`, decoded from the parallel [`ROLE_KEYWORD_POSTINGS`]
    /// column located by the value's dict-id. `None` if the value is not in this
    /// segment's dictionary or the posting block is torn — never panics. The
    /// docid stream is byte-identical to what the live `terms[value]` bitmap
    /// held at seal, so the segment-driven Term/Terms/boolean algebra matches
    /// the in-RAM path exactly. Phase 2h-1.
    pub fn keyword_postings(&self, value: &str) -> Option<roaring::RoaringBitmap> {
        let dict_id = self.keyword_dict_id(value)?;
        // Phase 2m: served through the BOUNDED posting cache (resident `Arc` on a
        // warm hit, fresh decode + insert on a miss). The cached bitmap is the RAW
        // immutable posting; `storage.rs::term_postings` subtracts the tombstone
        // AFTER this call, so the result is byte-identical.
        self.cached_docid_postings(ROLE_KEYWORD_POSTINGS, dict_id)
            .map(|p| (*p).clone())
    }

    /// Number of keyword dictionary ordinals in lexical order. This is the
    /// direct sort-planner seam: it avoids materializing every dictionary
    /// string when a page only needs the first few posting buckets. `None`
    /// means the dictionary column is absent or malformed.
    pub fn keyword_ordinal_count(&self) -> Option<u32> {
        self.column(ROLE_DICT)
            .and_then(|column| u32::try_from(column.elem_count).ok())
    }

    /// Keyword term at a lexical dictionary ordinal. This decodes only the
    /// requested dictionary entry (and its bounded compressed block), so the
    /// sort planner can merge a sealed dictionary with its live tail without
    /// materializing every term. `None` is fail-closed for an invalid ordinal
    /// or torn dictionary data.
    pub fn keyword_term_at_ordinal(&self, ordinal: u32) -> Option<String> {
        self.keyword_term_at_ordinal_cow(ordinal)
            .map(Cow::into_owned)
    }

    /// Internal ordinal dictionary view for scalar checkpoint streaming. A raw
    /// dictionary lends mmap bytes; a legacy compressed block owns one term.
    pub(crate) fn keyword_term_at_ordinal_cow(&self, ordinal: u32) -> Option<Cow<'_, str>> {
        if ordinal >= self.keyword_ordinal_count()? {
            return None;
        }
        self.dict_value(ordinal)
    }

    /// Keyword posting bucket at a lexical dictionary ordinal. Ordinal `0` is
    /// the smallest keyword. The returned bitmap is the immutable sealed
    /// posting, before `storage.rs` applies query-time tombstones. `None` is
    /// returned for an invalid ordinal or a malformed posting block.
    pub fn keyword_postings_at_ordinal(&self, ordinal: u32) -> Option<roaring::RoaringBitmap> {
        let count = self.keyword_ordinal_count()?;
        if ordinal >= count {
            return None;
        }
        self.cached_docid_postings(ROLE_KEYWORD_POSTINGS, ordinal)
            .map(|posting| (*posting).clone())
    }

    /// Materialize EVERY keyword term in the [`ROLE_DICT`] string dictionary of a
    /// Keyword segment paired with its stored INVERTED postings, in dict-id
    /// (ascending lexical) order. `None` if any dict entry or posting block is
    /// torn — never panics. Mirrors [`Self::text_tokens_all`] but decodes the
    /// docid-only posting codec ([`decode_docid_block`], no tf). Used by the
    /// segment-aware duplicate / unique-term enumeration (Phase 2h-1 FIX): a
    /// sealed Keyword field dropped its in-RAM `terms` driver, so the only way to
    /// walk distinct values is the on-disk dict. The docid stream is
    /// byte-identical to what the live `terms[value]` bitmap held at seal.
    pub fn keyword_terms_all(&self) -> Option<Vec<(String, roaring::RoaringBitmap)>> {
        let col = self.column(ROLE_DICT)?;
        let n = u32::try_from(col.elem_count).ok()?;
        let mut out = Vec::with_capacity(n as usize);
        for dict_id in 0..n {
            let term = self.dict_value(dict_id)?.into_owned();
            let (pblock, pwithin) = self.dict_block_at(ROLE_KEYWORD_POSTINGS, dict_id)?;
            let blob = pblock.get(pwithin)?;
            let docids = decode_docid_block(blob)?;
            out.push((term, docids.into_iter().collect()));
        }
        Some(out)
    }

    /// Keyword `value`'s document frequency (`df`) = the stored posting length,
    /// or `None` if the value is absent from the dictionary. Decodes ONLY the
    /// LEB128 `count` prefix of the posting blob (cheap — keeps the boolean
    /// planner's rarest-first clause ordering off the full decode). Mirrors
    /// [`Self::text_token_df`]. Phase 2h-1. Never panics.
    pub fn keyword_df(&self, value: &str) -> Option<u64> {
        let dict_id = self.keyword_dict_id(value)?;
        let (block, within) = self.dict_block_at(ROLE_KEYWORD_POSTINGS, dict_id)?;
        let blob = block.get(within)?;
        let mut pos = 0usize;
        read_varint(blob, &mut pos)
    }
}
