//! Writes a Number segment: the forward column, the sorted distinct values and
//! their per-value postings.

use std::path::Path;

use anyhow::{anyhow, Context, Result};

use crate::persistence::infrastructure::segment::codecs::encode_docid_block;
use crate::persistence::infrastructure::segment::format::{
    append_present_bitset, finalize_and_write, header_block, present_column_ref, sortable_bits,
};
use crate::persistence::infrastructure::segment::var_column::append_var_blob_column;
use crate::persistence::infrastructure::segment::{
    page_align, ColumnRef, CODEC_FIXED, ROLE_NUMBER, ROLE_NUMBER_POSTINGS, ROLE_NUMBER_SORTED,
};

/// Write a Number column for `values` (one entry per doc id, `None` = absent)
/// to `path`, tagged with `applied_seq`. Emits the segment format:
/// 4096-byte header, page-aligned `u64[n_docs]` forward column, present
/// bitset, then (Phase 2h-3) a SORTED-VALUE range index — a fixed-width
/// ascending `u64[distinct]` column of the distinct `SortableF64` bit keys
/// ([`ROLE_NUMBER_SORTED`]) plus a parallel per-distinct-value docid posting
/// column ([`ROLE_NUMBER_POSTINGS`]) — page-padded region tail, VAR region,
/// CBOR directory, 24-byte footer. Writes to a temp file then atomically
/// renames into place (mirrors `rdb.rs`).
///
/// Phase 2h-3: the in-RAM inverted/range `values: BTreeMap<SortableF64,
/// RoaringBitmap>` now lives ON DISK. The sorted-value column + postings are
/// DERIVED from `values` here (folded over the same per-doc forward state the
/// in-RAM `values` map was built from), so a reopen drives range / exact /
/// boolean queries straight off the mmap WITHOUT rebuilding `values` in RAM.
/// `values[id]` is doc `id`'s live value (a deleted base doc is `None`, so it is
/// excluded from BOTH the forward column AND the sorted index), making the
/// on-disk index exactly the live in-RAM `values` snapshot.
pub fn write_number_segment(path: &Path, applied_seq: u64, values: &[Option<f64>]) -> Result<()> {
    let n_docs: u32 = values
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;

    // Fold the forward column into the SORTED distinct-value range index: each
    // distinct `SortableF64` bit key -> its ascending docid list. BTreeMap keyed
    // on the raw `u64` bit key gives ascending numeric order (the key is monotone
    // in value), exactly reproducing the in-RAM `values` BTreeMap's key order and
    // per-key ascending-docid posting (RoaringBitmap iterates sorted).
    let mut sorted: std::collections::BTreeMap<u64, Vec<u32>> = std::collections::BTreeMap::new();
    for (id, v) in values.iter().enumerate() {
        if let Some(x) = v {
            sorted.entry(sortable_bits(*x)).or_default().push(id as u32);
        }
    }
    // Distinct keys ascending; parallel per-value docid-only posting blobs.
    let sorted_keys: Vec<u64> = sorted.keys().copied().collect();
    let posting_blobs: Vec<Vec<u8>> = sorted
        .values()
        .map(|docids| encode_docid_block(docids))
        .collect();

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- FIXED-WIDTH REGION ---
    // Pad up to the page-aligned start of the Number forward column.
    let number_off = page_align(buf.len());
    buf.resize(number_off, 0);

    // Number forward column: u64[n_docs] = f64::to_bits of each value (0 for absent).
    let number_words: Vec<u64> = values
        .iter()
        .map(|v| v.map(f64::to_bits).unwrap_or(0))
        .collect();
    // try_cast_slice (never cast_slice): u64 -> u8 always succeeds, but we
    // honor the no-panic discipline everywhere.
    let number_col_bytes: &[u8] = bytemuck::try_cast_slice(&number_words)
        .map_err(|e| anyhow!("cast number column to bytes: {e:?}"))?;
    buf.extend_from_slice(number_col_bytes);
    let number_len = number_words.len() * std::mem::size_of::<u64>();

    // SORTED-VALUE range index column (Phase 2h-3): u64[distinct] ascending bit
    // keys. The forward column ends 8-aligned, so this u64 column is already
    // 8-aligned for a zero-copy `try_cast_slice::<u8, u64>` on read.
    let sorted_col_bytes: &[u8] = bytemuck::try_cast_slice(&sorted_keys)
        .map_err(|e| anyhow!("cast sorted-value column to bytes: {e:?}"))?;
    let sorted_off = buf.len() as u64;
    buf.extend_from_slice(sorted_col_bytes);
    let sorted_len = (sorted_keys.len() * std::mem::size_of::<u64>()) as u64;

    // Present bitset: u64[ceil(n_docs/64)], bit i set == doc i has a value.
    let present: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, &present)?;

    // --- VAR REGION (after the page-padded fixed region) ---
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);
    let postings_ref = append_var_blob_column(
        &mut buf,
        &posting_blobs,
        ROLE_NUMBER_POSTINGS,
        "number_postings",
    )?;

    let dir = vec![
        ColumnRef {
            name: "number".to_string(),
            role: ROLE_NUMBER,
            byte_offset: number_off as u64,
            byte_len: number_len as u64,
            elem_count: n_docs as u64,
            width: 8,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        ColumnRef {
            name: "number_sorted".to_string(),
            role: ROLE_NUMBER_SORTED,
            byte_offset: sorted_off,
            byte_len: sorted_len,
            elem_count: sorted_keys.len() as u64,
            width: 8,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        present_column_ref(present_off, present_len, n_words),
        postings_ref,
    ];
    finalize_and_write(path, buf, dir)
}
