//! Var-width columns: entries prefix-delta encoded into LZ4 blocks, with a
//! sparse skip index that locates the block holding an entry. Dictionaries and
//! posting blobs are both written this way.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::persistence::infrastructure::segment::{
    ColumnRef, CODEC_LZ4_VAR, ROLE_DICT, VAR_BLOCK_BYTES,
};

#[cfg(test)]
use crate::persistence::infrastructure::segment::VAR_SKIP_INDEX_DECODES;

// ---------------------------------------------------------------------------
// Variable-width column machinery (Phase 2e-A)
// ---------------------------------------------------------------------------
//
// A var-width column is a sorted run of byte-string entries (the Keyword / Set
// string dictionaries) stored as a sequence of LZ4-framed blocks in the VAR
// region (after the page-padded fixed region). Each block holds up to
// `VAR_BLOCK_BYTES` of raw, prefix-delta-encoded entries; the per-column
// skip-index ([`SparseVarIndex`]) carries one [`VarBlockMeta`] per block so a
// lookup binary-searches to the owning block, decompresses it once (caching it
// in the moka byte-weighted cache on the reader), then scans the block.
//
// The framing: LZ4 64KB blocks (`u32` len + `lz4_flex::compress_prepend_size`
// / `decompress_size_prepended`), a sparse skip-index (`BlockMeta` +
// binary-search `locate`), and a prefix-delta string encoder
// (`delta_encode` / `shared_prefix`). No WAL/fsync/per-record-crc and no
// memtable — a segment is broker-durable and discard-on-torn, so the only
// integrity gate is the directory footer crc32.

/// Per-column skip-index over a var-width column's LZ4 blocks. CBOR-encoded
/// into [`ColumnRef::skip_index`] so the reader never seeks for it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct SparseVarIndex {
    pub(super) blocks: Vec<VarBlockMeta>,
}

/// Locates one LZ4 block of a var-width column and records the logical entry
/// id range it covers. `first_entry` is the dict-id of the block's first entry
/// (entries are globally id-ordered, so a target id binary-searches to the
/// block whose `first_entry <= id`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct VarBlockMeta {
    /// Dict-id of the first entry stored in this block.
    pub(super) first_entry: u32,
    /// Number of entries packed into this block.
    pub(super) entry_count: u32,
    /// Byte offset of the block's `u32` compressed-length prefix, relative to
    /// the file start.
    pub(super) offset: u64,
    /// Compressed length on disk (the bytes after the `u32` length prefix).
    pub(super) length: u32,
}

pub(super) fn decode_var_skip_index(column: &ColumnRef) -> Option<SparseVarIndex> {
    if column.codec != CODEC_LZ4_VAR {
        return None;
    }
    #[cfg(test)]
    VAR_SKIP_INDEX_DECODES.with(|count| count.set(count.get() + 1));
    ciborium::from_reader(&column.skip_index[..]).ok()
}

/// The shared-prefix length of two byte strings (the delta-encode leg).
pub(super) fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// One prefix-delta entry inside a decoded var block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct VarEntry {
    /// Bytes shared with the previous entry in the block (delta-encode).
    pub(super) shared: u32,
    /// New bytes appended after the shared prefix.
    #[serde(with = "var_suffix_bytes")]
    pub(super) suffix: Vec<u8>,
}

// New checkpoints encode byte payloads directly. The visitor also accepts
// integer arrays written by 0.6.0, so existing segment files remain readable.
mod var_suffix_bytes {
    use serde::{
        de::{Error, SeqAccess, Visitor},
        Deserializer, Serializer,
    };
    use std::fmt;

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        struct Bytes;
        impl<'de> Visitor<'de> for Bytes {
            type Value = Vec<u8>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a byte string or legacy byte array")
            }
            fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                Ok(bytes.to_vec())
            }
            fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
                Ok(bytes)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut bytes =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(64 * 1024));
                while let Some(byte) = sequence.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(bytes)
            }
        }
        // The owned suffix can exceed the decoder's borrowed scratch buffer.
        // Request an owned byte buffer so large entries use its streaming path.
        deserializer.deserialize_byte_buf(Bytes)
    }
}

/// The decoded body of one var block: prefix-delta entries. A whole block is
/// CBOR-encoded then LZ4-compressed; on read it is decompressed once and the
/// entries are reconstructed by rolling the shared prefix forward.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct VarBlockBody {
    pub(super) entries: Vec<VarEntry>,
}

impl VarBlockBody {
    /// Reconstruct the full byte strings in this block (rolling the shared
    /// prefix forward). Returns one `Vec<u8>` per entry, in id order.
    pub(super) fn reconstruct(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(self.entries.len());
        let mut prev: Vec<u8> = Vec::new();
        for e in &self.entries {
            let shared = (e.shared as usize).min(prev.len());
            let mut s = prev[..shared].to_vec();
            s.extend_from_slice(&e.suffix);
            out.push(s.clone());
            prev = s;
        }
        out
    }
}

/// Accumulates a var-width column into LZ4 64KB blocks appended to `buf`,
/// building the per-column skip-index as it goes. Entries are pushed in
/// dict-id order (`0, 1, 2, ...`); the writer flushes a block once the
/// accumulated *raw* bytes reach [`VAR_BLOCK_BYTES`].
struct VarColumnWriter {
    /// Prefix-delta entries pending in the current (not-yet-flushed) block.
    pending: Vec<VarEntry>,
    /// Raw byte count accumulated in `pending` (drives the 64KB flush cap).
    pending_bytes: usize,
    /// Previous entry's full bytes, for the prefix delta. Reset per block so a
    /// block decodes self-contained.
    prev: Vec<u8>,
    /// Dict-id of the first entry in the current block.
    block_first: u32,
    /// Total entries pushed so far (== dict-id of the next entry).
    next_id: u32,
    /// Skip-index accumulated as blocks flush.
    index: Vec<VarBlockMeta>,
}

impl VarColumnWriter {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            pending_bytes: 0,
            prev: Vec::new(),
            block_first: 0,
            next_id: 0,
            index: Vec::new(),
        }
    }

    /// Push the next dictionary entry (in id order). Prefix-deltas it against
    /// the previous entry IN THE SAME BLOCK, then flushes the block to `buf`
    /// when the raw byte budget is reached.
    fn push(&mut self, entry: &[u8], buf: &mut Vec<u8>) -> Result<()> {
        let shared = shared_prefix(&self.prev, entry) as u32;
        let suffix = entry[shared as usize..].to_vec();
        self.pending_bytes += 4 + suffix.len();
        self.pending.push(VarEntry { shared, suffix });
        self.prev = entry.to_vec();
        self.next_id += 1;
        if self.pending_bytes >= VAR_BLOCK_BYTES {
            self.flush_block(buf)?;
        }
        Ok(())
    }

    /// Flush the pending entries as one LZ4-framed block: `u32` compressed
    /// length + `lz4_flex::compress_prepend_size(cbor(body))`. Records a
    /// [`VarBlockMeta`] in the skip-index. No-op when empty.
    fn flush_block(&mut self, buf: &mut Vec<u8>) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let body = VarBlockBody {
            entries: std::mem::take(&mut self.pending),
        };
        let mut raw = Vec::new();
        ciborium::into_writer(&body, &mut raw).map_err(|e| anyhow!("encode var block: {e}"))?;
        let compressed = lz4_flex::compress_prepend_size(&raw);
        let offset = buf.len() as u64;
        buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        buf.extend_from_slice(&compressed);
        self.index.push(VarBlockMeta {
            first_entry: self.block_first,
            entry_count: body.entries.len() as u32,
            offset,
            length: compressed.len() as u32,
        });
        // Reset for the next block: blocks decode self-contained, so the prefix
        // delta restarts from empty.
        self.pending_bytes = 0;
        self.prev = Vec::new();
        self.block_first = self.next_id;
        Ok(())
    }

    /// Flush any trailing partial block and return the encoded skip-index +
    /// the column's `(byte_offset, byte_len)` span. `byte_offset` is the start
    /// of the column's first block (captured before the first `push`).
    fn finish(mut self, buf: &mut Vec<u8>, byte_offset: u64) -> Result<(Vec<u8>, u64, u64)> {
        self.flush_block(buf)?;
        let index = SparseVarIndex { blocks: self.index };
        let mut idx_bytes = Vec::new();
        ciborium::into_writer(&index, &mut idx_bytes)
            .map_err(|e| anyhow!("encode var skip-index: {e}"))?;
        let byte_len = buf.len() as u64 - byte_offset;
        Ok((idx_bytes, byte_offset, byte_len))
    }
}

/// Write a sorted byte-string dictionary `dict` (entry `i` has dict-id `i`) as
/// a var-width LZ4 column appended to `buf`, returning its [`ColumnRef`]
/// (role [`ROLE_DICT`], codec [`CODEC_LZ4_VAR`], skip-index carried inline).
pub(super) fn append_dict_column(buf: &mut Vec<u8>, dict: &[Vec<u8>]) -> Result<ColumnRef> {
    let byte_offset = buf.len() as u64;
    let mut w = VarColumnWriter::new();
    for entry in dict {
        w.push(entry, buf)?;
    }
    let (skip_index, off, byte_len) = w.finish(buf, byte_offset)?;
    Ok(ColumnRef {
        name: "dict".to_string(),
        role: ROLE_DICT,
        byte_offset: off,
        byte_len,
        elem_count: dict.len() as u64,
        width: 0,
        codec: CODEC_LZ4_VAR,
        skip_index,
    })
}

/// Append a var-width column whose entries are arbitrary byte blobs (NOT
/// prefix-similar dictionary strings) under `role`/`name`, reusing the shared
/// LZ4-blocked [`VarColumnWriter`]. The prefix-delta still applies (it just
/// finds `shared == 0` for unrelated blobs, so the suffix is the whole blob);
/// LZ4 then does the real compression. Returns the column's [`ColumnRef`].
pub(super) fn append_var_blob_column(
    buf: &mut Vec<u8>,
    entries: &[Vec<u8>],
    role: u8,
    name: &str,
) -> Result<ColumnRef> {
    let byte_offset = buf.len() as u64;
    let mut w = VarColumnWriter::new();
    for entry in entries {
        w.push(entry, buf)?;
    }
    let (skip_index, off, byte_len) = w.finish(buf, byte_offset)?;
    Ok(ColumnRef {
        name: name.to_string(),
        role,
        byte_offset: off,
        byte_len,
        elem_count: entries.len() as u64,
        width: 0,
        codec: CODEC_LZ4_VAR,
        skip_index,
    })
}
