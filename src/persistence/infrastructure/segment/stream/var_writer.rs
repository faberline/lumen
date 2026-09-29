//! The file-backed VAR column writer and the byte-counted output primitives
//! every streamed seal writes through, including the unique temp file each seal
//! renames into place.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::var_column::{
    shared_prefix, VarBlockBody, VarBlockMeta, VarEntry,
};
use crate::persistence::infrastructure::segment::*;

static STREAM_TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// A file-backed version of `VarColumnWriter` in
/// [`var_column`](crate::persistence::infrastructure::segment::var_column).
/// Its current block is the only materialized var-column data.  The skip
/// index is intentionally kept in memory because it is small metadata needed
/// by the CBOR directory.
pub(super) struct StreamingVarWriter {
    pending: Vec<VarEntry>,
    pending_bytes: usize,
    full_pending_bytes: usize,
    bound_decoded_block: bool,
    prev: Vec<u8>,
    block_first: u32,
    next_id: u32,
    index: Vec<VarBlockMeta>,
}

impl StreamingVarWriter {
    pub(super) fn new() -> Self {
        Self {
            pending: Vec::new(),
            pending_bytes: 0,
            full_pending_bytes: 0,
            bound_decoded_block: false,
            prev: Vec::new(),
            block_first: 0,
            next_id: 0,
            index: Vec::new(),
        }
    }

    /// Keep reconstructed prefix-delta strings within one bounded block.
    /// Prefix compression still applies, but shared prefixes cannot turn a tiny
    /// compressed block into a collection-sized decoded allocation.
    pub(super) fn bounded_projection() -> Self {
        Self {
            bound_decoded_block: true,
            ..Self::new()
        }
    }

    pub(super) fn push(
        &mut self,
        entry: &[u8],
        out: &mut BufWriter<File>,
        at: &mut u64,
    ) -> Result<()> {
        self.full_pending_bytes = self
            .full_pending_bytes
            .checked_add(entry.len())
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| anyhow!("stream decoded block size overflow"))?;
        let shared = shared_prefix(&self.prev, entry) as u32;
        let suffix = entry[shared as usize..].to_vec();
        self.pending_bytes = self
            .pending_bytes
            .checked_add(4 + suffix.len())
            .ok_or_else(|| anyhow!("stream var block size overflow"))?;
        self.pending.push(VarEntry { shared, suffix });
        self.prev.clear();
        self.prev.extend_from_slice(entry);
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("text dictionary exceeds u32 ordinal capacity"))?;
        if self.pending_bytes >= VAR_BLOCK_BYTES
            || (self.bound_decoded_block && self.full_pending_bytes >= VAR_BLOCK_BYTES)
        {
            self.flush(out, at)?;
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut BufWriter<File>, at: &mut u64) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let body = VarBlockBody {
            entries: std::mem::take(&mut self.pending),
        };
        let mut raw = Vec::new();
        ciborium::into_writer(&body, &mut raw)
            .map_err(|error| anyhow!("encode streamed var block: {error}"))?;
        let compressed = lz4_flex::compress_prepend_size(&raw);
        let length: u32 = compressed
            .len()
            .try_into()
            .context("streamed var block exceeds u32 compressed length")?;
        let offset = *at;
        write_counted(out, at, &length.to_le_bytes())?;
        write_counted(out, at, &compressed)?;
        self.index.push(VarBlockMeta {
            first_entry: self.block_first,
            entry_count: body.entries.len() as u32,
            offset,
            length,
        });
        self.pending_bytes = 0;
        self.full_pending_bytes = 0;
        self.prev.clear();
        self.block_first = self.next_id;
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        out: &mut BufWriter<File>,
        at: &mut u64,
        byte_offset: u64,
    ) -> Result<(Vec<u8>, u64, u64, u64)> {
        self.flush(out, at)?;
        let mut skip_index = Vec::new();
        ciborium::into_writer(&SparseVarIndex { blocks: self.index }, &mut skip_index)
            .map_err(|error| anyhow!("encode streamed var skip-index: {error}"))?;
        Ok((
            skip_index,
            byte_offset,
            *at - byte_offset,
            self.next_id as u64,
        ))
    }
}

pub(super) fn write_counted(out: &mut BufWriter<File>, at: &mut u64, bytes: &[u8]) -> Result<()> {
    out.write_all(bytes)?;
    *at = at
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow!("segment byte offset overflow"))?;
    Ok(())
}

pub(super) fn write_zeroes(out: &mut BufWriter<File>, at: &mut u64, count: usize) -> Result<()> {
    const ZEROES: [u8; 4096] = [0; 4096];
    let mut left = count;
    while left != 0 {
        let n = left.min(ZEROES.len());
        write_counted(out, at, &ZEROES[..n])?;
        left -= n;
    }
    Ok(())
}

pub(super) fn pad_to_page(out: &mut BufWriter<File>, at: &mut u64) -> Result<()> {
    let aligned = page_align(usize::try_from(*at).context("segment exceeds platform size")?);
    write_zeroes(out, at, aligned - *at as usize)
}

pub(super) fn new_stream_temp(path: &Path) -> Result<(PathBuf, File)> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("segment path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("create segment dir {}", dir.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("segment path has no file name: {}", path.display()))?;
    for _ in 0..32 {
        let nonce = STREAM_TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp = dir.join(format!(".{name}.stream-{}-{nonce}.tmp", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("create {}", temp.display())),
        }
    }
    bail!(
        "could not allocate unique stream temp for {}",
        path.display()
    )
}
