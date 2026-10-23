//! Set seal: from a scalar projection or from a composed view.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::stream::dictionary_spool::DictionarySpool;
use crate::persistence::infrastructure::segment::stream::projection_columns::{
    projection_dictionary, projection_posting, write_present_projection,
    write_projection_dictionary,
};
use crate::persistence::infrastructure::segment::stream::scalar_projection::{
    ScalarProjectionScratch, SetStreamProjection,
};
use crate::persistence::infrastructure::segment::stream::var_writer::{
    pad_to_page, write_counted, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::stream::{
    encode_bitmap_posting, stream_atomic, write_present_stream, write_stream_directory,
};
use crate::persistence::infrastructure::segment::*;

pub(crate) fn write_set_projection<V: SetStreamProjection>(
    path: &Path,
    seq: u64,
    view: &V,
    scratch: ScalarProjectionScratch,
) -> Result<()> {
    let n_docs = view.n_docs();
    let spool = projection_dictionary(path, seq, scratch, |emit| view.set_terms(emit))?;
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let offsets_off = *at;
        let mut packed = 0u32;
        write_counted(out, at, &packed.to_le_bytes())?;
        for id in 0..n_docs {
            let mut members = 0u32;
            let mut previous = None;
            let present = view.set_row(id, &mut |member| {
                let ordinal = spool.dict_id(member)?;
                if previous.is_some_and(|old| old >= ordinal) {
                    bail!("set projection members must be sorted and unique");
                }
                previous = Some(ordinal);
                members = members
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("set member count exceeds u32"))?;
                Ok(())
            })?;
            if !present && members != 0 {
                bail!("set projection emitted members for an absent row");
            }
            if present {
                packed = packed
                    .checked_add(members)
                    .ok_or_else(|| anyhow!("set packed column exceeds u32"))?;
            }
            write_counted(out, at, &packed.to_le_bytes())?;
        }
        let offsets_len = *at - offsets_off;
        let packed_off = *at;
        for id in 0..n_docs {
            view.set_row(id, &mut |member| {
                write_counted(out, at, &spool.dict_id(member)?.to_le_bytes())
            })?;
        }
        let packed_len = *at - packed_off;
        if packed_len != u64::from(packed) * 4 {
            bail!("set projection changed its packed row count during replay");
        }
        let (present_off, present_len, words) =
            write_present_projection(out, at, n_docs, |id| view.set_row(id, &mut |_| Ok(())))?;
        pad_to_page(out, at)?;
        let (dict_columns, dict_count) = write_projection_dictionary(
            path,
            scratch,
            out,
            at,
            |emit| view.set_terms(emit),
            Some(&spool),
        )?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::bounded_projection();
        view.set_terms(&mut |term| {
            if let Some(blob) = projection_posting(n_docs, scratch, "set posting", |emit| {
                view.set_posting(term, emit)
            })? {
                postings.push(&blob, out, at)?;
            }
            Ok(())
        })?;
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if dict_count != posting_count || dict_count != spool.count {
            bail!("streamed set dictionary/posting ordinal mismatch");
        }
        let mut directory = vec![
            ColumnRef {
                name: "set_offsets".to_owned(),
                role: ROLE_SET_OFFSETS,
                byte_offset: offsets_off,
                byte_len: offsets_len,
                elem_count: n_docs as u64 + 1,
                width: 4,
                codec: CODEC_FIXED,
                skip_index: Vec::new(),
            },
            ColumnRef {
                name: "set_packed".to_owned(),
                role: ROLE_SET_PACKED,
                byte_offset: packed_off,
                byte_len: packed_len,
                elem_count: packed as u64,
                width: 4,
                codec: CODEC_FIXED,
                skip_index: Vec::new(),
            },
            present_column_ref(present_off, present_len, words),
        ];
        directory.extend(dict_columns);
        directory.push(ColumnRef {
            name: "set_postings".to_owned(),
            role: ROLE_SET_POSTINGS,
            byte_offset: posting_off,
            byte_len: posting_len,
            elem_count: posting_count,
            width: 0,
            codec: CODEC_LZ4_VAR,
            skip_index: posting_skip,
        });
        write_stream_directory(out, at, directory)
    })
}

impl SetStreamProjection for ComposedSegmentReader {
    fn n_docs(&self) -> u32 {
        ComposedSegmentReader::n_docs(self)
    }
    fn set_row(&self, row: u32, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<bool> {
        let Some((present, count)) = self.set_row_member_count(row) else {
            return Ok(false);
        };
        if !present {
            return Ok(false);
        }
        for member in 0..count {
            let value = self
                .set_member_at_cow(row, member)
                .ok_or_else(|| anyhow!("invalid set member in scalar projection"))?;
            emit(value.as_ref())?;
        }
        Ok(true)
    }
    fn set_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        let mut terms = self.string_terms(false)?;
        while let Some(term) = terms.next_cow()? {
            if self.set_postings(term.as_ref()).is_some() {
                emit(term.as_ref())?;
            }
        }
        Ok(())
    }
    fn set_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        match self.set_postings(term) {
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

/// Stream the effective Set field.  CSR offsets and packed IDs are emitted in
/// row order; an explicit empty set stays present while an absent row is clear.
pub(crate) fn write_set_stream(path: &Path, seq: u64, view: &ComposedSegmentReader) -> Result<()> {
    let n_docs = view.n_docs();
    let spool = DictionarySpool::build(path, seq, view, |term| view.set_postings(term).is_some())?;
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let offsets_off = *at;
        let mut packed_count = 0u32;
        write_counted(out, at, &packed_count.to_le_bytes())?;
        for id in 0..n_docs {
            if let Some(members) = view.set_at(id) {
                packed_count = packed_count
                    .checked_add(
                        u32::try_from(members.len()).context("set member count exceeds u32")?,
                    )
                    .ok_or_else(|| anyhow!("set packed column exceeds u32"))?;
            }
            write_counted(out, at, &packed_count.to_le_bytes())?;
        }
        let offsets_len = *at - offsets_off;
        let packed_off = *at;
        for id in 0..n_docs {
            if let Some(members) = view.set_at(id) {
                for member in members {
                    let ordinal = spool.dict_id(&member)?;
                    write_counted(out, at, &ordinal.to_le_bytes())?;
                }
            }
        }
        let packed_len = *at - packed_off;
        let (present_off, present_len, words) =
            write_present_stream(out, at, n_docs, |id| view.set_at(id).is_some())?;
        pad_to_page(out, at)?;
        let dict_start = *at;
        let mut dict = StreamingVarWriter::new();
        let mut terms = view.string_terms(false)?;
        while let Some(term) = terms.next()? {
            if view.set_postings(&term).is_some() {
                dict.push(term.as_bytes(), out, at)?;
            }
        }
        let (dict_skip, dict_off, dict_len, dict_count) = dict.finish(out, at, dict_start)?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::new();
        let mut terms = view.string_terms(false)?;
        while let Some(term) = terms.next()? {
            let Some(posting) = view.set_postings(&term) else {
                continue;
            };
            let blob = encode_bitmap_posting(&posting);
            postings.push(&blob, out, at)?;
        }
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if dict_count != posting_count || dict_count != spool.count {
            bail!("streamed set dictionary/posting ordinal mismatch")
        }
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "set_offsets".to_owned(),
                    role: ROLE_SET_OFFSETS,
                    byte_offset: offsets_off,
                    byte_len: offsets_len,
                    elem_count: n_docs as u64 + 1,
                    width: 4,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                ColumnRef {
                    name: "set_packed".to_owned(),
                    role: ROLE_SET_PACKED,
                    byte_offset: packed_off,
                    byte_len: packed_len,
                    elem_count: packed_count as u64,
                    width: 4,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
                present_column_ref(present_off, present_len, words),
                ColumnRef {
                    name: "dict".to_owned(),
                    role: ROLE_DICT,
                    byte_offset: dict_off,
                    byte_len: dict_len,
                    elem_count: dict_count,
                    width: 0,
                    codec: CODEC_LZ4_VAR,
                    skip_index: dict_skip,
                },
                ColumnRef {
                    name: "set_postings".to_owned(),
                    role: ROLE_SET_POSTINGS,
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
