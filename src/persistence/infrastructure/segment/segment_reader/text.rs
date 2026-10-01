//! Text lookups: token postings, streamed posting scans, document lengths and
//! the BM25 corpus scalars.

use std::sync::Arc;

use crate::persistence::infrastructure::segment::codecs::{decode_posting_block, read_varint};
use crate::persistence::infrastructure::segment::reader_cache::{
    posting_cache_key, CachedTextPosting,
};
use crate::persistence::infrastructure::segment::{
    SegmentReader, CODEC_RAW_VAR, ROLE_DICT, ROLE_TEXT_DOCLEN, ROLE_TEXT_POSTINGS,
};

#[cfg(test)]
use crate::persistence::infrastructure::segment::DICTIONARY_SEARCHES;

impl SegmentReader {
    // -----------------------------------------------------------------------
    // Text segment reads (Phase 2e-B)
    // -----------------------------------------------------------------------

    /// The dict-id of `token` in the [`ROLE_DICT`] token dictionary, or `None`
    /// if the token is absent. The dict is sorted ascending (BTreeMap order at
    /// write), so this binary-searches the dict-id space comparing the decoded
    /// dictionary bytes against `token`. Each probe decodes one var block
    /// (cache-on-touch). Never panics.
    pub(crate) fn text_dict_id(&self, token: &str) -> Option<u32> {
        #[cfg(test)]
        DICTIONARY_SEARCHES.with(|count| count.set(count.get() + 1));
        let col = self.column(ROLE_DICT)?;
        let n = u32::try_from(col.elem_count).ok()?;
        let needle = token.as_bytes();
        let mut lo = 0u32;
        let mut hi = n; // exclusive
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = self.dict_value(mid)?;
            match entry.as_bytes().cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Whether this segment carries a Text posting column at all. A scalar
    /// segment shares [`ROLE_DICT`] with Text, so this is what separates a Text
    /// dictionary from a keyword one (#4246).
    pub(crate) fn has_text_postings(&self) -> bool {
        self.column(ROLE_TEXT_POSTINGS).is_some()
    }

    /// Whether `token` is in this segment's dictionary, without decoding its
    /// posting. One binary search over the dict-id space (#4246): a distinct
    /// term count folds each staged/tail token with this, never by decoding a
    /// posting per dictionary entry.
    pub(crate) fn text_has_term(&self, token: &str) -> bool {
        self.text_dict_id(token).is_some()
    }

    /// Token `token`'s stored postings as `(docids, tfs)`, docid-ascending — the
    /// exact streams the live `Postings` SoA holds. `None` if the token is not
    /// in this segment's dictionary, or the posting block is torn. The fixed
    /// docid/tf streams are decoded from the token's delta-varint blob, located
    /// by its dict-id in the parallel [`ROLE_TEXT_POSTINGS`] column. Never panics.
    pub fn text_postings(&self, token: &str) -> Option<(Vec<u32>, Vec<u32>)> {
        let cached = self.text_postings_arc(token)?;
        Some((cached.0.clone(), cached.1.clone()))
    }

    /// The cache-resident Text posting `Arc<(docids, tfs)>` for `token` (Phase
    /// 2m). A warm hit returns the shared `Arc` (a refcount bump — NO vector
    /// copy); a miss decodes the posting block, inserts, and returns the `Arc`.
    /// `storage.rs::tok_postings` holds this `Arc` directly so a per-candidate
    /// BM25 `match_doc_score` probe (`.tf(id)` binary-search) NEVER re-decodes or
    /// re-clones the whole posting — the cure for the `filtered_search` 25x disk
    /// blow-up where the previous owned-clone path copied the entire ~25k-docid
    /// posting on every candidate doc. The streams are the RAW immutable postings;
    /// `tok_postings` filters the tombstone into its own owned copy only when a
    /// delete is pending (the rare path). `text_postings` keeps its owned-tuple
    /// signature for callers that need to mutate (re-seal).
    pub fn text_postings_arc(&self, token: &str) -> Option<std::sync::Arc<(Vec<u32>, Vec<u32>)>> {
        let dict_id = self.text_dict_id(token)?;
        let key = posting_cache_key(ROLE_TEXT_POSTINGS, dict_id);
        if let Some(hit) = self.text_posting_cache.get(&key) {
            return Some(hit);
        }
        let (block, within) = self.dict_block_at(ROLE_TEXT_POSTINGS, dict_id)?;
        let blob = block.get(within)?;
        let (docids, tfs) = decode_posting_block(blob)?;
        let cached: CachedTextPosting = Arc::new((docids, tfs));
        self.text_posting_cache.insert(key, cached.clone());
        Some(cached)
    }

    /// The cache-resident posting for `token`, or `None` without decoding
    /// anything (#4246). A miss means [`Self::text_postings_arc`] would have
    /// to materialize the posting; callers that only need a few docids stream
    /// it with [`Self::text_posting_scan`] instead.
    pub(crate) fn text_posting_cached(&self, token: &str) -> Option<CachedTextPosting> {
        let dict_id = self.text_dict_id(token)?;
        self.text_posting_cache
            .get(&posting_cache_key(ROLE_TEXT_POSTINGS, dict_id))
    }

    /// Stream `token`'s stored posting — `visit(docid, tf)` for every entry in
    /// ascending docid order — decoding the delta-varint blob in place and
    /// materializing nothing: the posting cache is neither read nor filled
    /// (#4246: a 500k-row posting is 4 MiB decoded, so the bounded cache
    /// cannot hold a 90-token ngram query's working set and re-decodes every
    /// token on every cold request). Returns the stored entry count (the
    /// token's df), or `None` for a token absent from the dictionary or a
    /// torn blob — never panics. A torn blob may have visited a prefix;
    /// callers treat `None` as "no posting", exactly as
    /// [`Self::text_postings_arc`] does.
    pub(crate) fn text_posting_scan(
        &self,
        token: &str,
        mut visit: impl FnMut(u32, u32),
    ) -> Option<usize> {
        let dict_id = self.text_dict_id(token)?;
        let (block, within) = self.dict_block_at(ROLE_TEXT_POSTINGS, dict_id)?;
        let blob = block.get(within)?;
        let mut pos = 0usize;
        let count = usize::try_from(read_varint(blob, &mut pos)?).ok()?;
        let mut prev: u32 = 0;
        for _ in 0..count {
            let gap = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
            let tf = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
            let id = prev.checked_add(gap)?;
            visit(id, tf);
            prev = id;
        }
        Some(count)
    }

    /// Whether the posting stored at dictionary ordinal `dict_id` holds a docid
    /// `accept` returns true for, decoding the delta-varint blob only as far as
    /// that docid (#4246). Nothing is materialized and the posting cache is
    /// neither read nor filled: a distinct-term count visits every dictionary
    /// entry once per request and must neither allocate per term nor evict the
    /// query working set. `None` for an ordinal outside the posting column or
    /// a torn block — never panics.
    pub(crate) fn text_posting_any_at(
        &self,
        dict_id: u32,
        mut accept: impl FnMut(u32) -> bool,
    ) -> Option<bool> {
        let (block, within) = self.dict_block_at(ROLE_TEXT_POSTINGS, dict_id)?;
        let blob = block.get(within)?;
        let mut pos = 0usize;
        let count = read_varint(blob, &mut pos)?;
        let mut prev: u32 = 0;
        for _ in 0..count {
            let gap = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
            let _tf = read_varint(blob, &mut pos)?;
            let id = prev.checked_add(gap)?;
            if accept(id) {
                return Some(true);
            }
            prev = id;
        }
        Some(false)
    }

    /// Doc `id`'s BM25 length from the fixed [`ROLE_TEXT_DOCLEN`] column, read
    /// zero-copy off the mmap. Reproduces `TextIndex::doc_len(id)`: the stored
    /// `lens[id]`, or 0 when `id` is out of range or the column is torn — never
    /// panics. Does NOT gate on the present bitset (a 0-length present doc and an
    /// out-of-range doc are both length 0, matching the live `unwrap_or(0)`).
    pub fn text_doc_len(&self, id: u32) -> u32 {
        let Some(doclen) = self.u32_column(ROLE_TEXT_DOCLEN) else {
            return 0;
        };
        doclen.get(id as usize).copied().unwrap_or(0)
    }

    /// Whether text doc `id` has an explicit value according to the segment's
    /// presence bitset. Torn or out-of-range columns return false.
    pub fn text_is_present(&self, id: u32) -> bool {
        self.is_present(id)
    }

    /// Borrow the whole text doc-length column. Hot BM25 paths use this to avoid
    /// re-resolving the fixed column for every scored docid.
    pub fn text_doc_lens(&self) -> Option<&[u32]> {
        self.u32_column(ROLE_TEXT_DOCLEN)
    }

    /// Token `token`'s document frequency (`df`) = the stored posting length,
    /// or 0 if the token is absent. Decodes only the LEB128 `count` prefix of
    /// the posting blob (cheap). Never panics.
    pub fn text_token_df(&self, token: &str) -> usize {
        let Some(dict_id) = self.text_dict_id(token) else {
            return 0;
        };
        let Some((block, within)) = self.dict_block_at(ROLE_TEXT_POSTINGS, dict_id) else {
            return 0;
        };
        let Some(blob) = block.get(within) else {
            return 0;
        };
        let mut pos = 0usize;
        read_varint(blob, &mut pos).unwrap_or(0) as usize
    }

    /// The BM25 corpus document count `N` carried in the header (0 for a
    /// non-text segment).
    pub fn text_doc_count(&self) -> u64 {
        self.doc_count
    }

    /// The BM25 corpus summed document length `Σ|d|` carried in the header
    /// (0 for a non-text segment).
    pub fn text_total_doc_len(&self) -> u64 {
        self.total_doc_len
    }

    /// Count a Text dictionary without materializing its terms or postings.
    /// One decoded dictionary block is borrowed at a time; the bounded reader
    /// cache controls retained decompressed blocks.
    pub(crate) fn text_dictionary_stats(&self) -> Option<(u64, u64)> {
        let column = self.column(ROLE_DICT)?;
        let count = column.elem_count;
        let mut bytes = 0u64;
        for ordinal in 0..count {
            let id = u32::try_from(ordinal).ok()?;
            let len = if column.codec == CODEC_RAW_VAR {
                self.dict_value(id)?.len()
            } else {
                let (block, within) = self.dict_block_at(ROLE_DICT, id)?;
                block.get(within)?.len()
            };
            bytes = bytes.checked_add(u64::try_from(len).ok()?)?;
        }
        Some((count, bytes))
    }

    /// Materialize every token in the [`ROLE_DICT`] token dictionary of a Text
    /// segment paired with its stored postings, in dict-id (ascending lexical)
    /// order. `None` if any dict entry or posting block is torn. Used to
    /// reconstruct the live `tokens` BTreeMap on reopen (and for a CBOR snapshot
    /// taken after a Text seal, where the in-RAM `tokens` was dropped). The
    /// `(docids, tfs)` streams are byte-identical to what the live index held at
    /// seal.
    pub fn text_tokens_all(&self) -> Option<Vec<(String, Vec<u32>, Vec<u32>)>> {
        let col = self.column(ROLE_DICT)?;
        let n = u32::try_from(col.elem_count).ok()?;
        let mut out = Vec::with_capacity(n as usize);
        for dict_id in 0..n {
            let tok = self.dict_value(dict_id)?.into_owned();
            let (pblock, pwithin) = self.dict_block_at(ROLE_TEXT_POSTINGS, dict_id)?;
            let blob = pblock.get(pwithin)?;
            let (docids, tfs) = decode_posting_block(blob)?;
            out.push((tok, docids, tfs));
        }
        Some(out)
    }
}
