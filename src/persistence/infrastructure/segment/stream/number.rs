//! Number seal: from a scalar projection or from a composed view.

use std::path::Path;

use anyhow::{bail, Result};

use crate::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::{
    header_block, present_column_ref, sortable_bits,
};
use crate::persistence::infrastructure::segment::stream::projection_columns::{
    projection_posting, write_present_projection,
};
use crate::persistence::infrastructure::segment::stream::scalar_projection::{
    NumberStreamProjection, ScalarProjectionScratch,
};
use crate::persistence::infrastructure::segment::stream::var_writer::{
    pad_to_page, write_counted, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::stream::{
    encode_bitmap_posting, stream_atomic, write_present_stream, write_stream_directory,
};
use crate::persistence::infrastructure::segment::*;

fn inverse_sortable_bits(bits: u64) -> f64 {
    let raw = if bits >> 63 == 1 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    f64::from_bits(raw)
}

pub(crate) fn write_number_projection<V: NumberStreamProjection>(
    path: &Path,
    seq: u64,
    view: &V,
    scratch: ScalarProjectionScratch,
) -> Result<()> {
    let n_docs = view.n_docs();
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let number_off = *at;
        for id in 0..n_docs {
            let mut value = None;
            view.number_row(id, &mut |v| {
                value = v;
                Ok(())
            })?;
            write_counted(out, at, &value.map(f64::to_bits).unwrap_or(0).to_le_bytes())?;
        }
        let number_len = *at - number_off;
        let sorted_off = *at;
        let mut count = 0u64;
        let mut last = None;
        view.number_keys(&mut |key| {
            if last.is_some_and(|old| old >= key)
                || sortable_bits(inverse_sortable_bits(key)) != key
            {
                bail!("invalid scalar projection sortable number key");
            }
            let live = view.number_posting(key, &mut |_| Ok(()))?;
            if live {
                write_counted(out, at, &key.to_le_bytes())?;
                count += 1;
            }
            last = Some(key);
            Ok(())
        })?;
        let sorted_len = *at - sorted_off;
        let (present_off, present_len, words) = write_present_projection(out, at, n_docs, |id| {
            let mut yes = false;
            view.number_row(id, &mut |v| {
                yes = v.is_some();
                Ok(())
            })?;
            Ok(yes)
        })?;
        pad_to_page(out, at)?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::bounded_projection();
        view.number_keys(&mut |key| {
            if let Some(blob) = projection_posting(n_docs, scratch, "number posting", |emit| {
                view.number_posting(key, emit)
            })? {
                postings.push(&blob, out, at)?;
            }
            Ok(())
        })?;
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if count != posting_count {
            bail!("streamed number key/posting ordinal mismatch");
        }
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "number".to_owned(),
                    role: ROLE_NUMBER,
                    byte_offset: number_off,
                    byte_len: number_len,
                    elem_count: n_docs as u64,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                ColumnRef {
                    name: "number_sorted".to_owned(),
                    role: ROLE_NUMBER_SORTED,
                    byte_offset: sorted_off,
                    byte_len: sorted_len,
                    elem_count: count,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                present_column_ref(present_off, present_len, words),
                ColumnRef {
                    name: "number_postings".to_owned(),
                    role: ROLE_NUMBER_POSTINGS,
                    byte_offset: posting_off,
                    byte_len: posting_len,
                    elem_count: posting_count,
                    width: 0,
                    codec: CODEC_LZ4_VAR,
                    skip_index: posting_skip,
                },
            ],
        )
    })
}

impl NumberStreamProjection for ComposedSegmentReader {
    fn n_docs(&self) -> u32 {
        ComposedSegmentReader::n_docs(self)
    }
    fn number_row(&self, row: u32, emit: &mut dyn FnMut(Option<f64>) -> Result<()>) -> Result<()> {
        emit(self.number_at(row))
    }
    fn number_keys(&self, emit: &mut dyn FnMut(u64) -> Result<()>) -> Result<()> {
        let mut keys = self.number_keys(None, None, false)?;
        while let Some(key) = keys.next()? {
            if self.number_value_postings(key).is_some() {
                emit(key)?;
            }
        }
        Ok(())
    }
    fn number_posting(&self, key: u64, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        match self.number_value_postings(key) {
            Some(p) => {
                for row in p {
                    emit(row)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// Stream Number forward values, sorted keys, and one docid posting at a time.
pub(crate) fn write_number_stream(
    path: &Path,
    seq: u64,
    view: &ComposedSegmentReader,
) -> Result<()> {
    let n_docs = view.n_docs();
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let number_off = *at;
        for id in 0..n_docs {
            write_counted(
                out,
                at,
                &view
                    .number_at(id)
                    .map(f64::to_bits)
                    .unwrap_or(0)
                    .to_le_bytes(),
            )?;
        }
        let number_len = *at - number_off;
        let sorted_off = *at;
        let mut keys = view.number_keys(None, None, false)?;
        let mut sorted_count = 0u64;
        while let Some(key) = keys.next()? {
            if view.number_value_postings(key).is_some() {
                // Validate the frozen sortable encoding before persisting it.
                if sortable_bits(inverse_sortable_bits(key)) != key {
                    bail!("invalid composed sortable number key")
                }
                write_counted(out, at, &key.to_le_bytes())?;
                sorted_count += 1;
            }
        }
        let sorted_len = *at - sorted_off;
        let (present_off, present_len, words) =
            write_present_stream(out, at, n_docs, |id| view.number_at(id).is_some())?;
        pad_to_page(out, at)?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::new();
        let mut keys = view.number_keys(None, None, false)?;
        while let Some(key) = keys.next()? {
            let Some(posting) = view.number_value_postings(key) else {
                continue;
            };
            let blob = encode_bitmap_posting(&posting);
            postings.push(&blob, out, at)?;
        }
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if sorted_count != posting_count {
            bail!("streamed number key/posting ordinal mismatch")
        }
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "number".to_owned(),
                    role: ROLE_NUMBER,
                    byte_offset: number_off,
                    byte_len: number_len,
                    elem_count: n_docs as u64,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                ColumnRef {
                    name: "number_sorted".to_owned(),
                    role: ROLE_NUMBER_SORTED,
                    byte_offset: sorted_off,
                    byte_len: sorted_len,
                    elem_count: sorted_count,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                present_column_ref(present_off, present_len, words),
                ColumnRef {
                    name: "number_postings".to_owned(),
                    role: ROLE_NUMBER_POSTINGS,
                    byte_offset: posting_off,
                    byte_len: posting_len,
                    elem_count: posting_count,
                    width: 0,
                    codec: CODEC_LZ4_VAR,
                    skip_index: posting_skip,
                },
            ],
        )
    })
}
