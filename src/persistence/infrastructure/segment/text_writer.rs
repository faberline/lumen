//! Writes a Text segment: the token dictionary, the stored `(docid, tf)`
//! postings, document lengths and the BM25 corpus scalars.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::persistence::infrastructure::segment::codecs::{
    append_u32_column, encode_posting_block,
};
use crate::persistence::infrastructure::segment::format::{
    append_present_bitset, finalize_and_write, header_block, present_column_ref,
};
use crate::persistence::infrastructure::segment::var_column::{
    append_dict_column, append_var_blob_column,
};
use crate::persistence::infrastructure::segment::{
    page_align, ColumnRef, CODEC_FIXED, ROLE_TEXT_DOCLEN, ROLE_TEXT_POSTINGS,
};

/// Write a Text segment (Phase 2e-B). Stores the WHOLE inverted text field for
/// `n_docs` docs at `applied_seq`:
///
/// - a sorted token DICT (var-width, [`ROLE_DICT`](super::ROLE_DICT)) — token `t` has dict-id `t`
///   (BTreeMap iteration is ascending, so the dict is already sorted);
/// - a parallel per-token POSTING-BLOCK column (var-width, [`ROLE_TEXT_POSTINGS`]):
///   the entry at dict-id `t` is token `t`'s delta-varint `(docid_gap, tf)` blob
///   (see [`encode_posting_block`]). Term frequency is NOT rebuildable, so the
///   postings are stored;
/// - a FIXED `u32[n_docs]` DocLen column ([`ROLE_TEXT_DOCLEN`]) = `lens` (0 for
///   a doc's length, including zero for an explicit empty value, read zero-copy;
/// - a present bitset (doc present == the explicit `present[i]` value);
/// - the BM25 corpus scalars `doc_count` / `total_doc_len` in the header, so
///   the reader derives the identical `n` / `avgdl`.
///
/// `tokens` is the live `BTreeMap<String, Postings>` (each `Postings` is
/// docid-sorted). `lens[i]` is doc `i`'s length. The reader+writer round-trip
/// the postings exactly, so the sealed BM25 path is bit-identical to the live
/// path.
pub fn write_text_segment(
    path: &Path,
    applied_seq: u64,
    tokens: &std::collections::BTreeMap<String, crate::storage::Postings>,
    lens: &[u32],
    present: &[bool],
    doc_count: u64,
    total_doc_len: u64,
) -> Result<()> {
    if present.len() != lens.len() {
        bail!(
            "text segment present/doclen length mismatch: {} != {}",
            present.len(),
            lens.len()
        );
    }
    let n_docs: u32 = lens
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;

    // Token dict (ascending — BTreeMap order) and the parallel posting blobs, in
    // the SAME dict-id order so a token's dict index locates its posting block.
    let mut dict: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
    let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
    for (tok, postings) in tokens {
        dict.push(tok.as_bytes().to_vec());
        blobs.push(encode_posting_block(postings.docids(), postings.tfs()));
    }

    let mut buf = header_block(applied_seq, n_docs, doc_count, total_doc_len);

    // --- FIXED REGION ---
    // DocLen forward column: u32[n_docs] = lens (zero may be explicit empty).
    let doclen_start = page_align(buf.len());
    buf.resize(doclen_start, 0);
    let (doclen_off, doclen_len) = append_u32_column(&mut buf, lens)?;

    // Present bitset: bit i set == doc i has an explicit value. This is
    // independent from DocLen so an explicit empty value remains covered.
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, present)?;

    // --- VAR REGION (after the page-padded fixed region) ---
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);
    let dict_ref = append_dict_column(&mut buf, &dict)?;
    let postings_ref =
        append_var_blob_column(&mut buf, &blobs, ROLE_TEXT_POSTINGS, "text_postings")?;

    let dir = vec![
        ColumnRef {
            name: "text_doclen".to_string(),
            role: ROLE_TEXT_DOCLEN,
            byte_offset: doclen_off,
            byte_len: doclen_len,
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
