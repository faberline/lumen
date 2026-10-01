//! Hash seal from a composed view.

use std::path::Path;

use anyhow::Result;

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::stream::var_writer::write_counted;
use crate::persistence::infrastructure::segment::stream::{
    stream_atomic, write_present_stream, write_stream_directory,
};
use crate::persistence::infrastructure::segment::*;

/// Stream the effective Hash forward column and its presence bits.
pub(crate) fn write_hash_stream(path: &Path, seq: u64, view: &ComposedSegmentReader) -> Result<()> {
    let n_docs = view.n_docs();
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let hash_off = *at;
        for id in 0..n_docs {
            write_counted(out, at, &view.hash_at(id).unwrap_or(0).to_le_bytes())?;
        }
        let hash_len = *at - hash_off;
        let (present_off, present_len, words) =
            write_present_stream(out, at, n_docs, |id| view.hash_at(id).is_some())?;
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "hash".to_owned(),
                    role: ROLE_HASH,
                    byte_offset: hash_off,
                    byte_len: hash_len,
                    elem_count: n_docs as u64,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                present_column_ref(present_off, present_len, words),
            ],
        )
    })
}
