//! Streaming Text segment seal support for a composed view.
//!
//! This is included below `segment.rs`, so it deliberately uses that module's
//! private on-disk codec types.  It never collects the text dictionary or all
//! postings: each cursor pass owns at most one term and its one decoded posting.

pub(crate) mod dictionary_spool;
pub(crate) mod hash;
pub(crate) mod keyword;
pub(crate) mod number;
pub(crate) mod projection_columns;
pub(crate) mod scalar_projection;
pub(crate) mod set;
pub(crate) mod text_projection;
pub(crate) mod var_writer;
pub(crate) mod vector;

use super::codecs::write_varint;
use super::*;
use crate::persistence::infrastructure::segment::stream::var_writer::{
    new_stream_temp, write_counted, write_zeroes,
};
use anyhow::{anyhow, Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn stream_atomic(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>, &mut u64) -> Result<()>,
) -> Result<()> {
    let (temp, file) = new_stream_temp(path)?;
    let result = (|| {
        let mut out = BufWriter::new(file);
        let mut at = 0u64;
        write(&mut out, &mut at)?;
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

fn write_stream_directory(
    out: &mut BufWriter<File>,
    at: &mut u64,
    dir: Vec<ColumnRef>,
) -> Result<()> {
    let mut bytes = Vec::new();
    ciborium::into_writer(&dir, &mut bytes)
        .map_err(|error| anyhow!("cbor encode streamed segment directory: {error}"))?;
    let footer = Footer {
        dir_offset: *at,
        dir_len: bytes.len() as u64,
        crc32: crc32fast::hash(&bytes),
        magic2: MAGIC2,
    };
    write_counted(out, at, &bytes)?;
    write_counted(out, at, &footer.to_bytes())
}

fn write_present_stream(
    out: &mut BufWriter<File>,
    at: &mut u64,
    n_docs: u32,
    mut present: impl FnMut(u32) -> bool,
) -> Result<(u64, u64, u64)> {
    let pad = (8 - (*at % 8)) % 8;
    write_zeroes(out, at, pad as usize)?;
    let offset = *at;
    let mut words = 0u64;
    let mut word = 0u64;
    for id in 0..n_docs {
        if present(id) {
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

fn encode_bitmap_posting(posting: &roaring::RoaringBitmap) -> Vec<u8> {
    let mut out = Vec::new();
    write_varint(&mut out, posting.len());
    let mut previous = 0u32;
    for id in posting {
        write_varint(&mut out, id.wrapping_sub(previous) as u64);
        previous = id;
    }
    out
}

#[cfg(test)]
mod tests;
