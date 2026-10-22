//! Writes a Hash segment.

use std::path::Path;

use anyhow::{anyhow, Context, Result};

use crate::persistence::infrastructure::segment::format::{
    append_present_bitset, finalize_and_write, header_block, present_column_ref,
};
use crate::persistence::infrastructure::segment::{page_align, ColumnRef, CODEC_FIXED, ROLE_HASH};

/// Write a Hash column for `values` (one `u64` perceptual hash per doc id,
/// `None` = absent) to `path`. Same layout as the Number column but the
/// forward column stores the raw `u64` hash directly (no f64-bits transform) —
/// role [`ROLE_HASH`]. Mirrors
/// [`write_number_segment`](super::number_writer::write_number_segment) for everything else.
pub fn write_hash_segment(path: &Path, applied_seq: u64, values: &[Option<u64>]) -> Result<()> {
    let n_docs: u32 = values
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- FIXED-WIDTH REGION ---
    let hash_off = page_align(buf.len());
    buf.resize(hash_off, 0);

    // Hash forward column: u64[n_docs] = the raw hash (0 for absent).
    let hash_words: Vec<u64> = values.iter().map(|v| v.unwrap_or(0)).collect();
    let hash_col_bytes: &[u8] = bytemuck::try_cast_slice(&hash_words)
        .map_err(|e| anyhow!("cast hash column to bytes: {e:?}"))?;
    buf.extend_from_slice(hash_col_bytes);
    let hash_len = hash_words.len() * std::mem::size_of::<u64>();

    let present: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, &present)?;

    let dir = vec![
        ColumnRef {
            name: "hash".to_string(),
            role: ROLE_HASH,
            byte_offset: hash_off as u64,
            byte_len: hash_len as u64,
            elem_count: n_docs as u64,
            width: 8,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        present_column_ref(present_off, present_len, n_words),
    ];
    finalize_and_write(path, buf, dir)
}
