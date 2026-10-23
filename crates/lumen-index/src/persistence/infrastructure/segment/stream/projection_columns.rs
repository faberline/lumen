//! The columns every scalar projection writes the same way: charged postings,
//! the present bitset, and the dictionary in its LZ4 or raw codec.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::codecs::write_varint;
use crate::persistence::infrastructure::segment::format::header_block;
use crate::persistence::infrastructure::segment::stream::dictionary_spool::DictionarySpool;
use crate::persistence::infrastructure::segment::stream::scalar_projection::ScalarProjectionScratch;
use crate::persistence::infrastructure::segment::stream::var_writer::{
    new_stream_temp, pad_to_page, write_counted, write_zeroes, StreamingVarWriter,
};
use crate::persistence::infrastructure::segment::stream::write_stream_directory;
use crate::persistence::infrastructure::segment::*;

/// Stream borrowed UTF-8 terms to `out` and keep only u64 entry boundaries in
/// a private spool.  The spool is copied into the final fixed column after the
/// raw bytes, so neither the writer nor the directory owns a term-sized buffer.
pub(super) fn write_raw_dictionary(
    target: &Path,
    out: &mut BufWriter<File>,
    at: &mut u64,
    mut terms: impl FnMut(&mut dyn FnMut(&str) -> Result<()>) -> Result<()>,
    expected: Option<&DictionarySpool>,
) -> Result<(u64, u64, u64, u64, u64)> {
    let (offset_path, offset_file) = new_stream_temp(target)?;
    let result = (|| {
        let mut offsets = BufWriter::new(offset_file);
        let start = *at;
        offsets.write_all(&0u64.to_le_bytes())?;
        let mut count = 0u64;
        terms(&mut |term| {
            if let Some(spool) = expected {
                let ordinal = u32::try_from(count).context("raw dictionary ordinal exceeds u32")?;
                let prior = spool
                    .reader
                    .keyword_term_at_ordinal_cow(ordinal)
                    .ok_or_else(|| anyhow!("raw dictionary changed before ordinal {ordinal}"))?;
                if prior.as_bytes() != term.as_bytes() {
                    bail!("raw dictionary changed between validated spool and target write")
                }
            }
            write_counted(out, at, term.as_bytes())?;
            count = count
                .checked_add(1)
                .ok_or_else(|| anyhow!("raw dictionary exceeds u64 ordinal capacity"))?;
            if count > u64::from(u32::MAX) {
                bail!("raw dictionary exceeds u32 ordinal capacity")
            }
            offsets.write_all(&(*at - start).to_le_bytes())?;
            Ok(())
        })?;
        offsets.flush()?;
        drop(offsets);
        let data_len = *at - start;
        if expected.is_some_and(|spool| spool.count != count) {
            bail!("raw dictionary count changed between validated spool and target write")
        }
        pad_to_page(out, at)?;
        let offsets_off = *at;
        let mut source = File::open(&offset_path)
            .with_context(|| format!("open raw dictionary offsets {}", offset_path.display()))?;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = source.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            write_counted(out, at, &buffer[..n])?;
        }
        let offsets_len = *at - offsets_off;
        if offsets_len
            != count
                .checked_add(1)
                .and_then(|n| n.checked_mul(8))
                .ok_or_else(|| anyhow!("raw dictionary offsets overflow"))?
        {
            bail!("raw dictionary offsets spool length mismatch")
        }
        Ok((start, data_len, count, offsets_off, offsets_len))
    })();
    let _ = std::fs::remove_file(&offset_path);
    result
}

fn leb128_len(mut value: u64) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

/// Validate a posting once, then replay it into exactly one charged legacy
/// blob.  The second pass checks capacity before every varint push.
pub(super) fn projection_posting(
    n_docs: u32,
    scratch: ScalarProjectionScratch,
    kind: &'static str,
    mut visit: impl FnMut(&mut dyn FnMut(u32) -> Result<()>) -> Result<bool>,
) -> Result<Option<Vec<u8>>> {
    let mut count = 0u64;
    use sha2::{Digest, Sha256};
    let mut payload = 0usize;
    let mut expected_rows = Sha256::new();
    let mut previous = None;
    let live = visit(&mut |row| {
        if row >= n_docs || previous.is_some_and(|old| old >= row) {
            bail!("scalar projection {kind} has unsorted or out-of-range posting row");
        }
        expected_rows.update(row.to_le_bytes());
        payload = payload
            .checked_add(leb128_len(u64::from(row - previous.unwrap_or(0))))
            .ok_or_else(|| anyhow!("scalar projection posting length overflow"))?;
        previous = Some(row);
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("scalar projection posting count overflow"))?;
        Ok(())
    })?;
    if !live {
        if count != 0 {
            bail!("scalar projection {kind} reported absent posting with rows")
        }
        return Ok(None);
    }
    if count == 0 {
        bail!("scalar projection {kind} reported live empty posting");
    }
    let required = leb128_len(count)
        .checked_add(payload)
        .ok_or_else(|| anyhow!("scalar projection posting length overflow"))?;
    scratch.require(kind, required)?;
    let mut blob = Vec::with_capacity(required);
    if leb128_len(count) > required {
        bail!("scalar projection {kind} changed between rewindable posting passes");
    }
    write_varint(&mut blob, count);
    let mut previous = None;
    let mut seen = 0u64;
    let mut actual_rows = Sha256::new();
    let replayed = visit(&mut |row| {
        if row >= n_docs || previous.is_some_and(|old| old >= row) {
            bail!("scalar projection {kind} changed or unsorted its replay posting");
        }
        let width = leb128_len(u64::from(row - previous.unwrap_or(0)));
        if blob
            .len()
            .checked_add(width)
            .map_or(true, |next| next > required)
        {
            bail!("scalar projection {kind} changed between rewindable posting passes");
        }
        actual_rows.update(row.to_le_bytes());
        write_varint(&mut blob, u64::from(row - previous.unwrap_or(0)));
        previous = Some(row);
        seen += 1;
        Ok(())
    })?;
    if !replayed
        || seen != count
        || blob.len() != required
        || expected_rows.finalize() != actual_rows.finalize()
    {
        bail!("scalar projection {kind} changed between rewindable posting passes");
    }
    Ok(Some(blob))
}

pub(super) fn write_present_projection(
    out: &mut BufWriter<File>,
    at: &mut u64,
    n_docs: u32,
    mut present: impl FnMut(u32) -> Result<bool>,
) -> Result<(u64, u64, u64)> {
    let pad = (8 - (*at % 8)) % 8;
    write_zeroes(out, at, pad as usize)?;
    let offset = *at;
    let mut words = 0u64;
    let mut word = 0u64;
    for id in 0..n_docs {
        if present(id)? {
            word |= 1u64 << (id % 64);
        }
        if id % 64 == 63 {
            write_counted(out, at, &word.to_le_bytes())?;
            words += 1;
            word = 0;
        }
    }
    if n_docs % 64 != 0 {
        write_counted(out, at, &word.to_le_bytes())?;
        words += 1;
    }
    Ok((offset, *at - offset, words))
}

pub(super) fn projection_dictionary(
    target: &Path,
    seq: u64,
    scratch: ScalarProjectionScratch,
    mut terms: impl FnMut(&mut dyn FnMut(&str) -> Result<()>) -> Result<()>,
) -> Result<DictionarySpool> {
    if scratch.raw_dictionary {
        return projection_raw_dictionary(target, seq, terms);
    }
    let (path, file) = new_stream_temp(target)?;
    let cleanup = path.clone();
    let result = (|| {
        let mut out = BufWriter::new(file);
        let mut at = 0u64;
        write_counted(&mut out, &mut at, &header_block(seq, 0, 0, 0))?;
        pad_to_page(&mut out, &mut at)?;
        let start = at;
        let mut dict = StreamingVarWriter::bounded_projection();
        let mut last = Vec::new();
        let mut has_last = false;
        terms(&mut |term| {
            scratch.require("dictionary term", term.len())?;
            if has_last && last.as_slice() >= term.as_bytes() {
                bail!("scalar projection terms are not strictly sorted");
            }
            last.clear();
            last.extend_from_slice(term.as_bytes());
            has_last = true;
            dict.push(term.as_bytes(), &mut out, &mut at)
        })?;
        let (skip, off, len, count) = dict.finish(&mut out, &mut at, start)?;
        write_stream_directory(
            &mut out,
            &mut at,
            vec![ColumnRef {
                name: "dict".to_owned(),
                role: ROLE_DICT,
                byte_offset: off,
                byte_len: len,
                elem_count: count,
                width: 0,
                codec: CODEC_LZ4_VAR,
                skip_index: skip,
            }],
        )?;
        out.flush()?;
        out.get_ref().sync_all()?;
        drop(out);
        Ok(DictionarySpool {
            reader: SegmentReader::open(&path)?,
            path,
            count,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(cleanup);
    }
    result
}

fn projection_raw_dictionary(
    target: &Path,
    seq: u64,
    terms: impl FnMut(&mut dyn FnMut(&str) -> Result<()>) -> Result<()>,
) -> Result<DictionarySpool> {
    let (path, file) = new_stream_temp(target)?;
    let cleanup = path.clone();
    let result = (|| {
        let mut out = BufWriter::new(file);
        let mut at = 0u64;
        write_counted(&mut out, &mut at, &header_block(seq, 0, 0, 0))?;
        pad_to_page(&mut out, &mut at)?;
        let (dict_off, dict_len, count, offsets_off, offsets_len) =
            write_raw_dictionary(target, &mut out, &mut at, terms, None)?;
        write_stream_directory(
            &mut out,
            &mut at,
            vec![
                ColumnRef {
                    name: "dict".to_owned(),
                    role: ROLE_DICT,
                    byte_offset: dict_off,
                    byte_len: dict_len,
                    elem_count: count,
                    width: 0,
                    codec: CODEC_RAW_VAR,
                    skip_index: Vec::new(),
                },
                ColumnRef {
                    name: "dict_offsets".to_owned(),
                    role: ROLE_DICT_OFFSETS,
                    byte_offset: offsets_off,
                    byte_len: offsets_len,
                    elem_count: count + 1,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
            ],
        )?;
        out.flush()?;
        out.get_ref().sync_all()?;
        drop(out);
        Ok(DictionarySpool {
            reader: SegmentReader::open(&path)?,
            path,
            count,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(cleanup);
    }
    result
}

pub(super) fn write_projection_dictionary(
    target: &Path,
    scratch: ScalarProjectionScratch,
    out: &mut BufWriter<File>,
    at: &mut u64,
    mut terms: impl FnMut(&mut dyn FnMut(&str) -> Result<()>) -> Result<()>,
    expected: Option<&DictionarySpool>,
) -> Result<(Vec<ColumnRef>, u64)> {
    if scratch.raw_dictionary {
        let (off, len, count, offsets_off, offsets_len) =
            write_raw_dictionary(target, out, at, terms, expected)?;
        return Ok((
            vec![
                ColumnRef {
                    name: "dict".to_owned(),
                    role: ROLE_DICT,
                    byte_offset: off,
                    byte_len: len,
                    elem_count: count,
                    width: 0,
                    codec: CODEC_RAW_VAR,
                    skip_index: Vec::new(),
                },
                ColumnRef {
                    name: "dict_offsets".to_owned(),
                    role: ROLE_DICT_OFFSETS,
                    byte_offset: offsets_off,
                    byte_len: offsets_len,
                    elem_count: count + 1,
                    width: 8,
                    codec: CODEC_FIXED,
                    skip_index: Vec::new(),
                },
            ],
            count,
        ));
    }
    let start = *at;
    let mut writer = StreamingVarWriter::bounded_projection();
    terms(&mut |term| {
        scratch.require("dictionary term", term.len())?;
        writer.push(term.as_bytes(), out, at)
    })?;
    let (skip, off, len, count) = writer.finish(out, at, start)?;
    Ok((
        vec![ColumnRef {
            name: "dict".to_owned(),
            role: ROLE_DICT,
            byte_offset: off,
            byte_len: len,
            elem_count: count,
            width: 0,
            codec: CODEC_LZ4_VAR,
            skip_index: skip,
        }],
        count,
    ))
}
