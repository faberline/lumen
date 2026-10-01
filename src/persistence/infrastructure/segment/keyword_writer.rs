//! Writes a Keyword segment: the term dictionary, the dict-id forward column
//! and the per-term postings.

use std::path::Path;

use anyhow::{Context, Result};

use crate::persistence::infrastructure::segment::codecs::{append_u32_column, encode_docid_block};
use crate::persistence::infrastructure::segment::format::{
    append_present_bitset, finalize_and_write, header_block, present_column_ref,
};
use crate::persistence::infrastructure::segment::var_column::{
    append_dict_column, append_var_blob_column,
};
use crate::persistence::infrastructure::segment::{
    page_align, ColumnRef, CODEC_FIXED, DICT_ABSENT, ROLE_KEYWORD_DICTID, ROLE_KEYWORD_POSTINGS,
};

/// Write a Keyword segment: a sorted prefix-compressed string DICT (var-width)
/// + a FIXED `u32[n_docs]` dict-id forward column ([`DICT_ABSENT`] for a doc
/// with no keyword) + a parallel per-term INVERTED posting-block column
/// ([`ROLE_KEYWORD_POSTINGS`]) + a present bitset. `values[i]` is doc `i`'s
/// keyword (`None` = absent).
///
/// Phase 2h-1: the inverted `terms` index now lives ON DISK — for each dict
/// term (in dict-id / sorted order) a docid-only delta-varint posting blob is
/// pushed into a var column parallel to the dictionary. `postings` is the live
/// `terms: BTreeMap<String, RoaringBitmap>`; its keys MUST equal `values`'s
/// distinct non-`None` strings (the seal builds both from the same forward
/// state). The blobs are written in DICT order (BTreeSet ascending), which is
/// exactly `postings`'s BTreeMap key order, so term `t`'s dict index locates
/// its posting block. On reopen the inverted index is NOT rebuilt in RAM — the
/// reader serves Term/Terms/df straight off this column.
pub fn write_keyword_segment(
    path: &Path,
    applied_seq: u64,
    values: &[Option<&str>],
    postings: &std::collections::BTreeMap<String, roaring::RoaringBitmap>,
) -> Result<()> {
    let n_docs: u32 = values
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;

    // Build the sorted distinct dictionary; map each distinct string -> dict-id.
    let mut distinct: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for v in values {
        if let Some(s) = v {
            distinct.insert(s);
        }
    }
    let dict: Vec<Vec<u8>> = distinct.iter().map(|s| s.as_bytes().to_vec()).collect();
    let dict_id: std::collections::HashMap<&str, u32> = distinct
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, i as u32))
        .collect();

    // Per-term posting blobs in DICT order. `distinct` is the sorted set of the
    // forward values; `postings` is keyed by the same strings (both derive from
    // the same live forward state at seal), so `postings[term]` always exists.
    // Each term's bitmap is ascending-docid by construction (RoaringBitmap
    // iterates sorted), matching `encode_docid_block`'s contract.
    let blobs: Vec<Vec<u8>> = distinct
        .iter()
        .map(|term| {
            let docids: Vec<u32> = postings
                .get(*term)
                .map(|bm| bm.iter().collect())
                .unwrap_or_default();
            encode_docid_block(&docids)
        })
        .collect();

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- FIXED REGION ---
    // Forward dict-id column: u32[n_docs], DICT_ABSENT for an absent doc.
    let dictid_start = page_align(buf.len());
    buf.resize(dictid_start, 0);
    let ids: Vec<u32> = values
        .iter()
        .map(|v| v.map(|s| dict_id[s]).unwrap_or(DICT_ABSENT))
        .collect();
    let (dictid_off, dictid_len) = append_u32_column(&mut buf, &ids)?;

    // Present bitset.
    let present: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, &present)?;

    // --- VAR REGION (after the page-padded fixed region) ---
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);
    let dict_ref = append_dict_column(&mut buf, &dict)?;
    let postings_ref =
        append_var_blob_column(&mut buf, &blobs, ROLE_KEYWORD_POSTINGS, "keyword_postings")?;

    let dir = vec![
        ColumnRef {
            name: "keyword_dictid".to_string(),
            role: ROLE_KEYWORD_DICTID,
            byte_offset: dictid_off,
            byte_len: dictid_len,
            elem_count: n_docs as u64,
            width: 4,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        present_column_ref(present_off, present_len, n_words),
        dict_ref,
        postings_ref,
    ];
    finalize_and_write(path, buf, dir)
}
