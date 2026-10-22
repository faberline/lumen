//! Writes a Set segment: the element dictionary, the CSR offsets and packed
//! dict-ids, and the per-element postings.

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
    page_align, ColumnRef, CODEC_FIXED, ROLE_SET_OFFSETS, ROLE_SET_PACKED, ROLE_SET_POSTINGS,
};

/// Write a Set segment: a shared sorted string DICT (var-width) + a FIXED
/// `u32[n_docs + 1]` CSR offsets column + a FIXED `u32[total_members]` packed
/// dict-id column + a parallel per-element INVERTED posting-block column
/// ([`ROLE_SET_POSTINGS`]) + a present bitset. Doc `i`'s members are
/// `packed[offsets[i]..offsets[i+1]]` (CSR). `values[i]` is doc `i`'s members
/// in ascending order (`None` = the doc has no set value at all; an empty
/// `Some(&[])` is a present-but-empty set).
///
/// Phase 2h-2: the inverted `elements` index now lives ON DISK — for each dict
/// element (in dict-id / sorted order) a docid-only delta-varint posting blob
/// (the docids whose set contains that element) is pushed into a var column
/// parallel to the dictionary. `postings` is the live
/// `elements: BTreeMap<String, RoaringBitmap>`; its keys MUST equal `values`'s
/// distinct members (the seal builds both from the same forward state). The
/// blobs are written in DICT order (BTreeSet ascending), which is exactly
/// `postings`'s BTreeMap key order, so element `t`'s dict index locates its
/// posting block. On reopen the inverted index is NOT rebuilt in RAM — the
/// reader serves membership / Terms / df straight off this column.
pub fn write_set_segment(
    path: &Path,
    applied_seq: u64,
    values: &[Option<&[String]>],
    postings: &std::collections::BTreeMap<String, roaring::RoaringBitmap>,
) -> Result<()> {
    let n_docs: u32 = values
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;

    // Shared distinct dictionary across all members of all docs.
    let mut distinct: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for v in values.iter().flatten() {
        for m in v.iter() {
            distinct.insert(m.as_str());
        }
    }
    let dict: Vec<Vec<u8>> = distinct.iter().map(|s| s.as_bytes().to_vec()).collect();
    let dict_id: std::collections::HashMap<&str, u32> = distinct
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, i as u32))
        .collect();

    // CSR offsets[n_docs + 1] + packed dict-ids. offsets[0] = 0; offsets[i+1] =
    // offsets[i] + |doc i's members|. An absent doc contributes 0 members (its
    // slice offsets[i]..offsets[i+1] is empty) and is also clear in `present`.
    let mut offsets: Vec<u32> = Vec::with_capacity(values.len() + 1);
    let mut packed: Vec<u32> = Vec::new();
    offsets.push(0);
    for v in values {
        if let Some(members) = v {
            for m in members.iter() {
                packed.push(dict_id[m.as_str()]);
            }
        }
        offsets.push(packed.len() as u32);
    }

    // Per-element INVERTED posting blobs in DICT order (Phase 2h-2). `distinct`
    // is the sorted set of all members; `postings` is keyed by the same strings
    // (both derive from the same live forward state at seal), so
    // `postings[element]` always exists. Each element's bitmap is ascending-docid
    // by construction (RoaringBitmap iterates sorted), matching
    // `encode_docid_block`'s contract.
    let blobs: Vec<Vec<u8>> = distinct
        .iter()
        .map(|el| {
            let docids: Vec<u32> = postings
                .get(*el)
                .map(|bm| bm.iter().collect())
                .unwrap_or_default();
            encode_docid_block(&docids)
        })
        .collect();

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- FIXED REGION ---
    let offsets_start = page_align(buf.len());
    buf.resize(offsets_start, 0);
    let (offsets_off, offsets_len) = append_u32_column(&mut buf, &offsets)?;
    let (packed_off, packed_len) = append_u32_column(&mut buf, &packed)?;

    let present: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, &present)?;

    // --- VAR REGION ---
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);
    let dict_ref = append_dict_column(&mut buf, &dict)?;
    let postings_ref = append_var_blob_column(&mut buf, &blobs, ROLE_SET_POSTINGS, "set_postings")?;

    let dir = vec![
        ColumnRef {
            name: "set_offsets".to_string(),
            role: ROLE_SET_OFFSETS,
            byte_offset: offsets_off,
            byte_len: offsets_len,
            elem_count: (n_docs as u64) + 1,
            width: 4,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        ColumnRef {
            name: "set_packed".to_string(),
            role: ROLE_SET_PACKED,
            byte_offset: packed_off,
            byte_len: packed_len,
            elem_count: packed.len() as u64,
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
