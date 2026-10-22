//! The row stage's VAR column writer. Its skip metadata spools to disk, so the
//! writer heap stays independent of dictionary cardinality.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use crate::persistence::infrastructure::segment::text_row_stage::lseg_run::counted;
use crate::persistence::infrastructure::segment::text_row_stage::{fresh_path, MAX_BLOCK_ENTRIES};
use crate::persistence::infrastructure::segment::var_column::{
    shared_prefix, VarBlockMeta, VarEntry,
};
use crate::persistence::infrastructure::segment::*;

// Skip metadata is a disk spool, independent of dictionary cardinality.
pub(super) struct RowVarWriter {
    pending: Vec<VarEntry>,
    pending_bytes: usize,
    prev: Vec<u8>,
    block_first: u32,
    next_id: u32,
    metadata: BufWriter<File>,
    metadata_path: PathBuf,
    blocks: u64,
}
impl RowVarWriter {
    pub(super) fn new(workspace: &Path) -> Result<Self> {
        let metadata_path = fresh_path(workspace, "block-meta");
        let metadata = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&metadata_path)?,
        );
        Ok(Self {
            pending: Vec::with_capacity(MAX_BLOCK_ENTRIES),
            pending_bytes: 0,
            prev: Vec::new(),
            block_first: 0,
            next_id: 0,
            metadata,
            metadata_path,
            blocks: 0,
        })
    }
    pub(super) fn push(
        &mut self,
        entry: &[u8],
        out: &mut BufWriter<File>,
        at: &mut u64,
    ) -> Result<()> {
        if !self.pending.is_empty()
            && (self.pending.len() == MAX_BLOCK_ENTRIES
                || self
                    .pending_bytes
                    .checked_add(entry.len())
                    .is_none_or(|n| n > VAR_BLOCK_BYTES))
        {
            self.flush(out, at)?;
        }
        let shared = u32::try_from(shared_prefix(&self.prev, entry))?;
        let suffix = entry[shared as usize..].to_vec();
        self.pending_bytes = self
            .pending_bytes
            .checked_add(suffix.len())
            .ok_or_else(|| anyhow!("text var block size overflow"))?;
        self.pending.push(VarEntry { shared, suffix });
        self.prev.clear();
        self.prev.extend_from_slice(entry);
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("text dictionary exceeds u32 ordinal capacity"))?;
        if self.pending_bytes >= VAR_BLOCK_BYTES {
            self.flush(out, at)?;
        }
        Ok(())
    }
    fn flush(&mut self, out: &mut BufWriter<File>, at: &mut u64) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // A suffix byte takes at most two CBOR bytes. Per-entry framing,
        // keys and integer sizes fit in another 64 bytes.
        let capacity = self
            .pending_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.pending.len() * 64 + 64))
            .ok_or_else(|| anyhow!("text block encoded size overflow"))?;
        let mut raw = BoundedBytes(Vec::with_capacity(capacity));
        #[derive(serde::Serialize)]
        struct Body<'a> {
            entries: &'a [VarEntry],
        }
        ciborium::into_writer(
            &Body {
                entries: &self.pending,
            },
            &mut raw,
        )
        .map_err(|e| anyhow!("encode text staged var block: {e}"))?;
        let compressed = lz4_flex::compress_prepend_size(&raw.0);
        let length = u32::try_from(compressed.len())?;
        let offset = *at;
        counted(out, at, &length.to_le_bytes())?;
        counted(out, at, &compressed)?;
        self.metadata.write_all(&self.block_first.to_le_bytes())?;
        self.metadata
            .write_all(&(self.pending.len() as u32).to_le_bytes())?;
        self.metadata.write_all(&offset.to_le_bytes())?;
        self.metadata.write_all(&length.to_le_bytes())?;
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| anyhow!("text index count overflow"))?;
        self.pending.clear();
        self.pending_bytes = 0;
        self.prev.clear();
        self.block_first = self.next_id;
        Ok(())
    }
    pub(super) fn finish(
        mut self,
        out: &mut BufWriter<File>,
        at: &mut u64,
        offset: u64,
        workspace: &Path,
    ) -> Result<(DiskBytes, u64, u64, u64)> {
        self.flush(out, at)?;
        self.metadata.flush()?;
        drop(self.metadata);
        let skip_path = fresh_path(workspace, "skip-index");
        let mut skip = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&skip_path)?,
        );
        let source = MetaSpool {
            path: self.metadata_path,
            count: self.blocks,
        };
        #[derive(serde::Serialize)]
        struct Index<'a> {
            blocks: &'a MetaSpool,
        }
        ciborium::into_writer(&Index { blocks: &source }, &mut skip)
            .map_err(|e| anyhow!("encode text staged skip index: {e}"))?;
        skip.flush()?;
        let len = skip.get_ref().metadata()?.len();
        Ok((
            DiskBytes {
                path: skip_path,
                len,
            },
            offset,
            *at - offset,
            self.next_id as u64,
        ))
    }
}

pub(super) struct BoundedBytes(pub(super) Vec<u8>);

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0.capacity() - self.0.len() {
            return Err(std::io::Error::other(
                "text codec exceeded reserved output capacity",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MetaSpool {
    path: PathBuf,
    count: u64,
}
impl serde::Serialize for MetaSpool {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::{ser::Error, ser::SerializeSeq};
        let mut input = BufReader::new(File::open(&self.path).map_err(S::Error::custom)?);
        let count = usize::try_from(self.count).map_err(S::Error::custom)?;
        let mut seq = serializer.serialize_seq(Some(count))?;
        for _ in 0..count {
            let mut bytes = [0u8; 20];
            input.read_exact(&mut bytes).map_err(S::Error::custom)?;
            seq.serialize_element(&VarBlockMeta {
                first_entry: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
                entry_count: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                length: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            })?;
        }
        let mut extra = [0u8; 1];
        if input.read(&mut extra).map_err(S::Error::custom)? != 0 {
            return Err(S::Error::custom(
                "text block metadata spool has trailing bytes",
            ));
        }
        seq.end()
    }
}

pub(super) struct DiskBytes {
    path: PathBuf,
    len: u64,
}
impl serde::Serialize for DiskBytes {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::{ser::Error, ser::SerializeSeq};
        let mut input = BufReader::new(File::open(&self.path).map_err(S::Error::custom)?);
        let len = usize::try_from(self.len).map_err(S::Error::custom)?;
        // Vec<u8> in ColumnRef is an integer array, not a CBOR byte string.
        let mut seq = serializer.serialize_seq(Some(len))?;
        let mut buffer = [0u8; 4096];
        let mut remaining = len;
        while remaining != 0 {
            let n = remaining.min(buffer.len());
            input
                .read_exact(&mut buffer[..n])
                .map_err(S::Error::custom)?;
            for byte in &buffer[..n] {
                seq.serialize_element(byte)?;
            }
            remaining -= n;
        }
        seq.end()
    }
}
