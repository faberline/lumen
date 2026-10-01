//! Writes the final LSEG from the merged run: the dictionary, postings and
//! doc-length columns, then a directory streamed with its CRC.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::Ordering as AtomicOrdering;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::codecs::write_varint;
use crate::persistence::infrastructure::segment::format::{header_block, present_column_ref};
use crate::persistence::infrastructure::segment::text_row_stage::row_var_writer::{
    DiskBytes, RowVarWriter,
};
use crate::persistence::infrastructure::segment::text_row_stage::sorted_run::RunReader;
use crate::persistence::infrastructure::segment::text_row_stage::{
    TextRowStageOptions, ROW_STAGE_NONCE,
};
use crate::persistence::infrastructure::segment::*;

struct DiskColumn {
    column: ColumnRef,
    skip: Option<DiskBytes>,
}
impl serde::Serialize for DiskColumn {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s =
            serializer.serialize_struct("ColumnRef", if self.skip.is_some() { 8 } else { 7 })?;
        s.serialize_field("name", &self.column.name)?;
        s.serialize_field("role", &self.column.role)?;
        s.serialize_field("byte_offset", &self.column.byte_offset)?;
        s.serialize_field("byte_len", &self.column.byte_len)?;
        s.serialize_field("elem_count", &self.column.elem_count)?;
        s.serialize_field("width", &self.column.width)?;
        s.serialize_field("codec", &self.column.codec)?;
        if let Some(skip) = &self.skip {
            s.serialize_field("skip_index", skip)?;
        }
        s.end()
    }
}
struct DirectoryOutput<'a> {
    out: &'a mut BufWriter<File>,
    len: u64,
    crc: crc32fast::Hasher,
}
impl Write for DirectoryOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.out.write_all(bytes)?;
        self.len = self
            .len
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("text directory length overflow"))?;
        self.crc.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

pub(super) fn counted(out: &mut BufWriter<File>, at: &mut u64, bytes: &[u8]) -> Result<()> {
    out.write_all(bytes)?;
    *at = at
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow!("segment byte offset overflow"))?;
    Ok(())
}
fn zeroes(out: &mut BufWriter<File>, at: &mut u64, mut n: usize) -> Result<()> {
    const Z: [u8; 4096] = [0; 4096];
    while n > 0 {
        let take = n.min(Z.len());
        counted(out, at, &Z[..take])?;
        n -= take;
    }
    Ok(())
}
fn align(out: &mut BufWriter<File>, at: &mut u64) -> Result<()> {
    let size = usize::try_from(*at).context("segment exceeds platform size")?;
    zeroes(out, at, page_align(size) - size)
}

pub(super) fn write_lseg_from_run(
    path: &Path,
    seq: u64,
    document_len: u32,
    run: Option<&Path>,
    options: TextRowStageOptions,
    workspace: &Path,
) -> Result<()> {
    let max_term = options.run_bytes()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("text row path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("row"),
        ROW_STAGE_NONCE.fetch_add(1, AtomicOrdering::Relaxed)
    ));
    let result = (|| {
        let mut out = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?,
        );
        let mut at = 0u64;
        counted(
            &mut out,
            &mut at,
            &header_block(seq, 1, 1, document_len as u64),
        )?;
        let doclen_off = at;
        counted(&mut out, &mut at, &document_len.to_le_bytes())?;
        let doclen_len = at - doclen_off;
        let padding = ((8 - at % 8) % 8) as usize;
        zeroes(&mut out, &mut at, padding)?;
        let present_off = at;
        counted(&mut out, &mut at, &1u64.to_le_bytes())?;
        let present_len = at - present_off;
        align(&mut out, &mut at)?;
        let mut input = match run {
            Some(path) => Some(RunReader::open(path, max_term)?),
            None => None,
        };
        let dict_off = at;
        let mut dict = RowVarWriter::new(workspace)?;
        while let Some(reader) = input.as_mut() {
            let Some((term, _)) = reader.current.as_ref() else {
                break;
            };
            if term.len() > max_term {
                bail!("single text term of {} bytes exceeds staging scratch payload {}; use dedicated large-term record encoding",term.len(),max_term);
            }
            dict.push(term, &mut out, &mut at)?;
            reader.advance()?;
        }
        let (dict_skip, dict_off, dict_len, dict_count) =
            dict.finish(&mut out, &mut at, dict_off, workspace)?;
        let mut input = match run {
            Some(path) => Some(RunReader::open(path, max_term)?),
            None => None,
        };
        let postings_off = at;
        let mut postings = RowVarWriter::new(workspace)?;
        while let Some(reader) = input.as_mut() {
            let Some((_, count)) = reader.current.as_ref() else {
                break;
            };
            let mut blob = Vec::with_capacity(12);
            write_varint(&mut blob, 1);
            write_varint(&mut blob, 0);
            write_varint(&mut blob, *count as u64);
            postings.push(&blob, &mut out, &mut at)?;
            reader.advance()?;
        }
        let (postings_skip, postings_off, postings_len, postings_count) =
            postings.finish(&mut out, &mut at, postings_off, workspace)?;
        if dict_count != postings_count {
            bail!("staged text dictionary/posting ordinal mismatch");
        }
        let dir = vec![
            ColumnRef {
                name: "text_doclen".to_owned(),
                role: ROLE_TEXT_DOCLEN,
                byte_offset: doclen_off,
                byte_len: doclen_len,
                elem_count: 1,
                width: 4,
                codec: CODEC_FIXED,
                skip_index: Vec::new(),
            },
            present_column_ref(present_off, present_len, 1),
            ColumnRef {
                name: "dict".to_owned(),
                role: ROLE_DICT,
                byte_offset: dict_off,
                byte_len: dict_len,
                elem_count: dict_count,
                width: 0,
                codec: CODEC_LZ4_VAR,
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
                skip_index: Vec::new(),
            },
        ];
        let mut skips = [None, None, Some(dict_skip), Some(postings_skip)].into_iter();
        let dir: Vec<_> = dir
            .into_iter()
            .map(|column| DiskColumn {
                column,
                skip: skips.next().unwrap(),
            })
            .collect();
        let dir_offset = at;
        let mut output = DirectoryOutput {
            out: &mut out,
            len: 0,
            crc: crc32fast::Hasher::new(),
        };
        ciborium::into_writer(&dir, &mut output)
            .map_err(|e| anyhow!("encode staged text directory: {e}"))?;
        let footer = Footer {
            dir_offset,
            dir_len: output.len,
            crc32: output.crc.finalize(),
            magic2: MAGIC2,
        };
        at = at
            .checked_add(footer.dir_len)
            .ok_or_else(|| anyhow!("text directory offset overflow"))?;
        counted(&mut out, &mut at, &footer.to_bytes())?;
        out.flush()?;
        out.get_ref().sync_all()?;
        drop(out);
        fs::rename(&temp, path)?;
        // The name is part of the prepared artifact's durable ownership. A
        // file sync alone does not preserve a rename across power loss.
        File::open(
            path.parent()
                .ok_or_else(|| anyhow!("text row has no parent"))?,
        )?
        .sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
