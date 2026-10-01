//! Keyword seal: from a scalar projection or from a composed view.

use std::path::Path;

use anyhow::{anyhow, bail, Result};

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::stream::dictionary_spool::DictionarySpool;
use crate::persistence::infrastructure::segment::stream::projection_columns::{
    projection_dictionary, projection_posting, write_present_projection,
    write_projection_dictionary,
};
use crate::persistence::infrastructure::segment::stream::scalar_projection::{
    KeywordStreamProjection, ScalarProjectionScratch,
};
use crate::persistence::infrastructure::segment::stream::var_writer::{
    pad_to_page, write_counted, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::stream::{
    encode_bitmap_posting, stream_atomic, write_present_stream, write_stream_directory,
};
use crate::persistence::infrastructure::segment::*;

pub(crate) fn write_keyword_projection<V: KeywordStreamProjection>(
    path: &Path,
    seq: u64,
    view: &V,
    scratch: ScalarProjectionScratch,
) -> Result<()> {
    let n_docs = view.n_docs();
    let spool = projection_dictionary(path, seq, scratch, |emit| view.keyword_terms(emit))?;
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let dictid_off = *at;
        for id in 0..n_docs {
            let mut ordinal = None;
            view.keyword_row(id, &mut |value| {
                if ordinal.is_some() {
                    bail!("keyword projection emitted a row more than once");
                }
                ordinal = Some(match value {
                    Some(value) => {
                        if !scratch.raw_dictionary {
                            scratch.require("keyword row", value.len())?;
                        }
                        spool.dict_id(value)?
                    }
                    None => DICT_ABSENT,
                });
                Ok(())
            })?;
            let ordinal = ordinal.ok_or_else(|| anyhow!("keyword projection omitted a row"))?;
            write_counted(out, at, &ordinal.to_le_bytes())?;
        }
        let dictid_len = *at - dictid_off;
        let (present_off, present_len, words) = write_present_projection(out, at, n_docs, |id| {
            let mut yes = false;
            view.keyword_row(id, &mut |v| {
                yes = v.is_some();
                Ok(())
            })?;
            Ok(yes)
        })?;
        pad_to_page(out, at)?;
        let (dict_columns, dict_count) = write_projection_dictionary(
            path,
            scratch,
            out,
            at,
            |emit| view.keyword_terms(emit),
            Some(&spool),
        )?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::bounded_projection();
        view.keyword_terms(&mut |term| {
            if let Some(blob) = projection_posting(n_docs, scratch, "keyword posting", |emit| {
                view.keyword_posting(term, emit)
            })? {
                postings.push(&blob, out, at)?;
            }
            Ok(())
        })?;
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if dict_count != posting_count || dict_count != spool.count {
            bail!("streamed keyword dictionary/posting ordinal mismatch");
        }
        let mut directory = vec![
            ColumnRef {
                name: "keyword_dictid".to_owned(),
                role: ROLE_KEYWORD_DICTID,
                byte_offset: dictid_off,
                byte_len: dictid_len,
                elem_count: n_docs as u64,
                width: 4,
                codec: CODEC_FIXED,
                skip_index: Vec::new(),
            },
            present_column_ref(present_off, present_len, words),
        ];
        directory.extend(dict_columns);
        directory.push(ColumnRef {
            name: "keyword_postings".to_owned(),
            role: ROLE_KEYWORD_POSTINGS,
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

impl KeywordStreamProjection for ComposedSegmentReader {
    fn n_docs(&self) -> u32 {
        ComposedSegmentReader::n_docs(self)
    }
    fn keyword_row(
        &self,
        row: u32,
        emit: &mut dyn FnMut(Option<&str>) -> Result<()>,
    ) -> Result<()> {
        let value = self.keyword_at_cow(row);
        emit(value.as_deref())
    }
    fn keyword_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        let mut terms = self.string_terms(false)?;
        while let Some(term) = terms.next_cow()? {
            if self.keyword_postings(term.as_ref()).is_some() {
                emit(term.as_ref())?;
            }
        }
        Ok(())
    }
    fn keyword_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        match self.keyword_postings(term) {
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

/// Stream the effective Keyword field from `view`.  A disposable mmap spool
/// supplies each forward dict-id through the segment reader's O(log terms)
/// binary lookup without a resident whole-dictionary map.
pub(crate) fn write_keyword_stream(
    path: &Path,
    seq: u64,
    view: &ComposedSegmentReader,
) -> Result<()> {
    let n_docs = view.n_docs();
    let spool = DictionarySpool::build(path, seq, view, |term| {
        view.keyword_postings(term).is_some()
    })?;
    stream_atomic(path, |out, at| {
        write_counted(out, at, &header_block(seq, n_docs, 0, 0))?;
        let dictid_off = *at;
        for id in 0..n_docs {
            let dict_id = match view.keyword_at(id) {
                Some(value) => spool.dict_id(&value)?,
                None => DICT_ABSENT,
            };
            write_counted(out, at, &dict_id.to_le_bytes())?;
        }
        let dictid_len = *at - dictid_off;
        let (present_off, present_len, words) =
            write_present_stream(out, at, n_docs, |id| view.keyword_at(id).is_some())?;
        pad_to_page(out, at)?;

        let dict_start = *at;
        let mut dict = StreamingVarWriter::new();
        let mut terms = view.string_terms(false)?;
        while let Some(term) = terms.next()? {
            if view.keyword_postings(&term).is_some() {
                dict.push(term.as_bytes(), out, at)?;
            }
        }
        let (dict_skip, dict_off, dict_len, dict_count) = dict.finish(out, at, dict_start)?;
        let posting_start = *at;
        let mut postings = StreamingVarWriter::new();
        let mut terms = view.string_terms(false)?;
        while let Some(term) = terms.next()? {
            let Some(posting) = view.keyword_postings(&term) else {
                continue;
            };
            let blob = encode_bitmap_posting(&posting);
            postings.push(&blob, out, at)?;
        }
        let (posting_skip, posting_off, posting_len, posting_count) =
            postings.finish(out, at, posting_start)?;
        if dict_count != posting_count || dict_count != spool.count {
            bail!("streamed keyword dictionary/posting ordinal mismatch")
        }
        write_stream_directory(
            out,
            at,
            vec![
                ColumnRef {
                    name: "keyword_dictid".to_owned(),
                    role: ROLE_KEYWORD_DICTID,
                    byte_offset: dictid_off,
                    byte_len: dictid_len,
                    elem_count: n_docs as u64,
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
                    name: "keyword_postings".to_owned(),
                    role: ROLE_KEYWORD_POSTINGS,
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
