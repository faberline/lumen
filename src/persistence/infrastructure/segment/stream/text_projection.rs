//! Text seal: a sorted Text projection streamed to a segment one term and one
//! decoded posting at a time, never the whole dictionary.

use std::borrow::Cow;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::codecs::encode_posting_block;
use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::stream::projection_columns::write_raw_dictionary;
use crate::persistence::infrastructure::segment::stream::var_writer::{
    new_stream_temp, pad_to_page, write_counted, write_zeroes, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::*;

/// A sorted Text projection that can be sealed without rebuilding its
/// dictionary or postings.  `terms` may allocate one returned term at a time;
/// it must not retain the full dictionary.
pub(crate) trait TextStreamView {
    fn n_docs(&self) -> u32;
    fn text_is_present(&self, id: u32) -> bool;
    fn text_doc_len(&self, id: u32) -> u32;
    fn terms<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<Cow<'a, str>>> + 'a>>;
    fn text_postings(&self, term: &str) -> Result<Option<std::sync::Arc<(Vec<u32>, Vec<u32>)>>>;
}

struct ComposedTerms<'a> {
    cursor: crate::composed_segment::StringTermCursor<'a>,
}

impl<'a> Iterator for ComposedTerms<'a> {
    type Item = Result<Cow<'a, str>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next_cow() {
            Ok(Some(term)) => Some(Ok(term)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

impl TextStreamView for ComposedSegmentReader {
    fn n_docs(&self) -> u32 {
        ComposedSegmentReader::n_docs(self)
    }
    fn text_is_present(&self, id: u32) -> bool {
        ComposedSegmentReader::text_is_present(self, id)
    }
    fn text_doc_len(&self, id: u32) -> u32 {
        ComposedSegmentReader::text_doc_len(self, id)
    }
    fn terms<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<Cow<'a, str>>> + 'a>> {
        Ok(Box::new(ComposedTerms {
            cursor: self.string_terms(false)?,
        }))
    }
    fn text_postings(&self, term: &str) -> Result<Option<std::sync::Arc<(Vec<u32>, Vec<u32>)>>> {
        Ok(self.text_postings_arc(term))
    }
}

/// Seal a sorted Text projection without expanding its whole dictionary or
/// postings collection. `view` IDs are dense in `[0, view.n_docs())`.
pub(crate) fn write_text_projection(
    path: &Path,
    seq: u64,
    view: &impl TextStreamView,
) -> Result<()> {
    let n_docs = view.n_docs();
    let (temp, file) = new_stream_temp(path)?;
    let result = (|| {
        let mut out = BufWriter::new(file);
        let mut at = 0u64;
        let mut doc_count = 0u64;
        let mut total_doc_len = 0u64;
        for id in 0..n_docs {
            if view.text_is_present(id) {
                doc_count = doc_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("text doc count overflow"))?;
                total_doc_len = total_doc_len
                    .checked_add(view.text_doc_len(id) as u64)
                    .ok_or_else(|| anyhow!("text total doc length overflow"))?;
            }
        }
        write_counted(
            &mut out,
            &mut at,
            &header_block(seq, n_docs, doc_count, total_doc_len),
        )?;
        let doclen_off = at;
        for id in 0..n_docs {
            write_counted(&mut out, &mut at, &view.text_doc_len(id).to_le_bytes())?;
        }
        let doclen_len = at - doclen_off;
        let present_pad = (8 - (at % 8)) % 8;
        write_zeroes(&mut out, &mut at, present_pad as usize)?;
        let present_off = at;
        let mut words = 0u64;
        let mut word = 0u64;
        for id in 0..n_docs {
            if view.text_is_present(id) {
                word |= 1u64 << (id % 64);
            }
            if id % 64 == 63 {
                write_counted(&mut out, &mut at, &word.to_le_bytes())?;
                words += 1;
                word = 0;
            }
        }
        if n_docs % 64 != 0 {
            write_counted(&mut out, &mut at, &word.to_le_bytes())?;
            words += 1;
        }
        let present_len = at - present_off;
        pad_to_page(&mut out, &mut at)?;
        // Text terms may lend an arbitrarily large raw mmap slice. The raw
        // dictionary codec writes that slice directly and spools only u64
        // boundaries, instead of constructing a term-sized LZ4/CBOR block.
        let (dict_off, dict_len, dict_count, dict_offsets_off, dict_offsets_len) =
            write_raw_dictionary(
                path,
                &mut out,
                &mut at,
                |emit| {
                    for term in view.terms()? {
                        let term = term?;
                        if view.text_postings(term.as_ref())?.is_some() {
                            emit(term.as_ref())?;
                        }
                    }
                    Ok(())
                },
                None,
            )?;
        let postings_start = at;
        let mut postings = StreamingVarWriter::new();
        for term in view.terms()? {
            let term = term?;
            let Some(posting) = view.text_postings(term.as_ref())? else {
                continue;
            };
            let blob = encode_posting_block(&posting.0, &posting.1);
            postings.push(&blob, &mut out, &mut at)?;
        }
        let (postings_skip, postings_off, postings_len, postings_count) =
            postings.finish(&mut out, &mut at, postings_start)?;
        if dict_count != postings_count {
            bail!("streamed text dictionary/posting ordinal mismatch: {dict_count} != {postings_count}");
        }
        let dir = vec![
            ColumnRef {
                name: "text_doclen".to_owned(),
                role: ROLE_TEXT_DOCLEN,
                byte_offset: doclen_off,
                byte_len: doclen_len,
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
                codec: CODEC_RAW_VAR,
                skip_index: Vec::new(),
            },
            ColumnRef {
                name: "dict_offsets".to_owned(),
                role: ROLE_DICT_OFFSETS,
                byte_offset: dict_offsets_off,
                byte_len: dict_offsets_len,
                elem_count: dict_count + 1,
                width: 8,
                codec: CODEC_FIXED,
                skip_index: Vec::new(),
            },
            ColumnRef {
                name: "text_postings".to_owned(),
                role: ROLE_TEXT_POSTINGS,
                byte_offset: postings_off,
                byte_len: postings_len,
                elem_count: postings_count,
                width: 0,
                codec: CODEC_LZ4_VAR,
                skip_index: postings_skip,
            },
        ];
        let mut dir_bytes = Vec::new();
        ciborium::into_writer(&dir, &mut dir_bytes)
            .map_err(|error| anyhow!("cbor encode streamed segment directory: {error}"))?;
        let dir_offset = at;
        write_counted(&mut out, &mut at, &dir_bytes)?;
        let footer = Footer {
            dir_offset,
            dir_len: dir_bytes.len() as u64,
            crc32: crc32fast::hash(&dir_bytes),
            magic2: MAGIC2,
        };
        write_counted(&mut out, &mut at, &footer.to_bytes())?;
        out.flush()
            .with_context(|| format!("flush {}", temp.display()))?;
        out.get_ref()
            .sync_all()
            .with_context(|| format!("fsync {}", temp.display()))?;
        drop(out);
        std::fs::rename(&temp, path)
            .with_context(|| format!("rename {} -> {}", temp.display(), path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// Existing composed-reader entrypoint.
pub(crate) fn write_text_stream(path: &Path, seq: u64, view: &ComposedSegmentReader) -> Result<()> {
    write_text_projection(path, seq, view)
}
