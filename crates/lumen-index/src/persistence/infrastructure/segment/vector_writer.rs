//! Writes a Vector segment.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::format::{
    append_present_bitset, finalize_and_write, header_block, present_column_ref,
};
use crate::persistence::infrastructure::segment::{
    page_align, ColumnRef, CODEC_FIXED, ROLE_VECTOR,
};

/// Write a Vector column: a contiguous, decoded `f32[n_docs * dim]` forward
/// column plus a present bitset. `vectors[i]` is doc `i`'s vector (`None` =
/// absent, written as `dim` zeros and bit clear in the present set). Every
/// present slice MUST be exactly `dim` long. Role [`ROLE_VECTOR`], width 4. No
/// scalar quantization — the bytes on disk are the exact `f32` bits, so a
/// zero-copy scan is bit-identical to the in-RAM corpus.
pub fn write_vector_segment(
    path: &Path,
    applied_seq: u64,
    dim: usize,
    vectors: &[Option<&[f32]>],
) -> Result<()> {
    let n_docs: u32 = vectors
        .len()
        .try_into()
        .context("segment exceeds u32 doc capacity")?;
    if dim == 0 {
        bail!("vector segment dim must be > 0");
    }

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- FIXED-WIDTH REGION ---
    let vector_off = page_align(buf.len());
    buf.resize(vector_off, 0);

    // Vector forward column: f32[n_docs * dim], dense in docid order. An absent
    // doc contributes `dim` zeros (and a clear present bit).
    let zeros = vec![0f32; dim];
    let mut data: Vec<f32> = Vec::with_capacity(vectors.len() * dim);
    for v in vectors {
        match v {
            Some(slice) => {
                if slice.len() != dim {
                    bail!("vector has dim {} but segment dim is {dim}", slice.len());
                }
                data.extend_from_slice(slice);
            }
            None => data.extend_from_slice(&zeros),
        }
    }
    let vector_col_bytes: &[u8] = bytemuck::try_cast_slice(&data)
        .map_err(|e| anyhow!("cast vector column to bytes: {e:?}"))?;
    buf.extend_from_slice(vector_col_bytes);
    let vector_len = data.len() * std::mem::size_of::<f32>();

    let present: Vec<bool> = vectors.iter().map(|v| v.is_some()).collect();
    let (present_off, present_len, n_words) = append_present_bitset(&mut buf, &present)?;

    let dir = vec![
        ColumnRef {
            name: "vector".to_string(),
            role: ROLE_VECTOR,
            byte_offset: vector_off as u64,
            byte_len: vector_len as u64,
            elem_count: (n_docs as u64) * (dim as u64),
            width: 4,
            codec: CODEC_FIXED,
            skip_index: Vec::new(),
        },
        present_column_ref(present_off, present_len, n_words),
    ];
    finalize_and_write(path, buf, dir)
}
