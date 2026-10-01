//! Vector seal from a composed view.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::stream::var_writer::{
    pad_to_page, write_counted, write_zeroes,
};
use crate::persistence::infrastructure::segment::stream::{stream_atomic, write_stream_directory};
use crate::persistence::infrastructure::segment::*;

/// Stream a dense Vector field without constructing `n_docs * dim` values in
/// memory. `row` is called exactly once for each dense target row and may hold
/// only that row's decoded vector. `None` writes `dim` zero components and a
/// clear present bit; `Some(vec![0.0; dim])` stays explicitly present.
pub(crate) fn write_vector_stream(
    path: &Path,
    seq: u64,
    n_docs: u32,
    dim: usize,
    mut row: impl FnMut(u32) -> Result<Option<Vec<f32>>>,
) -> Result<()> {
    if dim == 0 {
        bail!("vector segment dim must be > 0");
    }
    let dim_u64 = u64::try_from(dim).context("vector dimension exceeds u64")?;
    let elem_count = u64::from(n_docs)
        .checked_mul(dim_u64)
        .ok_or_else(|| anyhow!("vector segment element count overflow"))?;
    let vector_bytes = elem_count
        .checked_mul(std::mem::size_of::<f32>() as u64)
        .ok_or_else(|| anyhow!("vector segment byte count overflow"))?;
    let row_bytes = usize::try_from(
        dim_u64
            .checked_mul(4)
            .ok_or_else(|| anyhow!("vector row byte count overflow"))?,
    )
    .context("vector row byte count exceeds platform size")?;
    let word_count = usize::try_from((u64::from(n_docs) + 63) / 64)
        .context("vector present bitset exceeds platform size")?;

    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let vector_off = *at;
        let mut present = vec![0u64; word_count];
        for id in 0..n_docs {
            match row(id)? {
                None => write_zeroes(out, at, row_bytes)?,
                Some(values) => {
                    if values.len() != dim {
                        bail!("vector has dim {} but segment dim is {dim}", values.len());
                    }
                    for value in values {
                        if !value.is_finite() {
                            bail!("vector contains non-finite component");
                        }
                        write_counted(out, at, &value.to_le_bytes())?;
                    }
                    present[id as usize / 64] |= 1u64 << (id % 64);
                }
            }
        }
        if *at - vector_off != vector_bytes {
            bail!("streamed vector byte count mismatch");
        }
        let vector_len = *at - vector_off;
        let present_pad = (8 - (*at % 8)) % 8;
        write_zeroes(out, at, present_pad as usize)?;
        let present_off = *at;
        for word in present {
            write_counted(out, at, &word.to_le_bytes())?;
        }
        let present_len = *at - present_off;
        pad_to_page(out, at)?;
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "vector".to_owned(),
                    role: ROLE_VECTOR,
                    byte_offset: vector_off,
                    byte_len: vector_len,
                    elem_count,
                    width: 4,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                present_column_ref(present_off, present_len, word_count as u64),
            ],
        )
    })
}
