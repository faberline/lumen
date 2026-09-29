//! The file framing every segment shares: the header and footer, the present
//! bitset, and `finalize_and_write`, which lays the columns out page-aligned
//! behind a CRC-checked CBOR directory.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};

use crate::persistence::infrastructure::segment::{
    page_align, ColumnRef, Footer, Header, CODEC_FIXED, FOOTER_LEN, FORMAT_VER, HEADER_LEN,
    HOST_ENDIAN_MARKER, MAGIC1, MAGIC2, ROLE_PRESENT,
};

impl Header {
    /// Serialize the scalar fields (40 bytes) little-endian. The caller pads
    /// the remainder of the [`HEADER_LEN`] block with zeros. The first 24 bytes
    /// are byte-identical to the pre-2e-B layout; the two trailing `u64`s are
    /// appended so an older reader's 24-byte parse still works and a newer
    /// reader recovers the BM25 scalars (0 for a non-text segment).
    fn to_prefix_bytes(&self) -> [u8; 40] {
        let mut out = [0u8; 40];
        out[0..4].copy_from_slice(&self.magic1.to_le_bytes());
        out[4..8].copy_from_slice(&self.format_ver.to_le_bytes());
        out[8..16].copy_from_slice(&self.applied_seq.to_le_bytes());
        out[16..20].copy_from_slice(&self.host_endian_marker.to_le_bytes());
        out[20..24].copy_from_slice(&self.n_docs.to_le_bytes());
        out[24..32].copy_from_slice(&self.doc_count.to_le_bytes());
        out[32..40].copy_from_slice(&self.total_doc_len.to_le_bytes());
        out
    }

    /// Parse the scalar fields from the head of a header block. Requires the
    /// original 24-byte prefix; the two BM25 `u64`s default to 0 when the buffer
    /// is shorter (older segment, or torn tail of a header that is otherwise
    /// always [`HEADER_LEN`] zero-padded). Returns `Err` only when even the
    /// 24-byte prefix is missing (torn file) — never panics.
    pub(super) fn from_bytes(buf: &[u8]) -> Result<Header> {
        if buf.len() < 24 {
            bail!("header too short: {} bytes", buf.len());
        }
        let mut cur = std::io::Cursor::new(buf);
        let magic1 = cur.read_u32::<LittleEndian>()?;
        let format_ver = cur.read_u32::<LittleEndian>()?;
        let applied_seq = cur.read_u64::<LittleEndian>()?;
        let host_endian_marker = cur.read_u32::<LittleEndian>()?;
        let n_docs = cur.read_u32::<LittleEndian>()?;
        // The two BM25 scalars live at bytes 24..40; absent (short buffer) => 0,
        // which is exactly a non-text / pre-2e-B segment's value.
        let doc_count = if buf.len() >= 32 {
            cur.read_u64::<LittleEndian>()?
        } else {
            0
        };
        let total_doc_len = if buf.len() >= 40 {
            cur.read_u64::<LittleEndian>()?
        } else {
            0
        };
        Ok(Header {
            magic1,
            format_ver,
            applied_seq,
            host_endian_marker,
            n_docs,
            doc_count,
            total_doc_len,
        })
    }
}

impl Footer {
    /// Serialize the 24 footer bytes little-endian.
    pub(super) fn to_bytes(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        out[0..8].copy_from_slice(&self.dir_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.dir_len.to_le_bytes());
        out[16..20].copy_from_slice(&self.crc32.to_le_bytes());
        out[20..24].copy_from_slice(&self.magic2.to_le_bytes());
        out
    }

    /// Parse the footer from a (>= 24-byte) tail slice. Returns `Err` on a
    /// short slice — never panics.
    pub(super) fn from_bytes(buf: &[u8]) -> Result<Footer> {
        if buf.len() < FOOTER_LEN {
            bail!("footer too short: {} bytes", buf.len());
        }
        let mut cur = std::io::Cursor::new(buf);
        Ok(Footer {
            dir_offset: cur.read_u64::<LittleEndian>()?,
            dir_len: cur.read_u64::<LittleEndian>()?,
            crc32: cur.read_u32::<LittleEndian>()?,
            magic2: cur.read_u32::<LittleEndian>()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Build the 4096-byte zero-padded header block for `n_docs` rows at
/// `applied_seq`. Non-text writers pass `doc_count = total_doc_len = 0` (the
/// BM25 scalars are only meaningful for a Text segment); a 2e-B Text segment
/// passes the corpus `N` and `Σ|d|` so the reader can derive `avgdl` from the
/// header alone. The first 24 header bytes are byte-identical regardless.
pub(super) fn header_block(
    applied_seq: u64,
    n_docs: u32,
    doc_count: u64,
    total_doc_len: u64,
) -> Vec<u8> {
    let header = Header {
        magic1: MAGIC1,
        format_ver: FORMAT_VER,
        applied_seq,
        host_endian_marker: HOST_ENDIAN_MARKER,
        n_docs,
        doc_count,
        total_doc_len,
    };
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&header.to_prefix_bytes());
    buf.resize(HEADER_LEN, 0); // zero-pad the rest of the header block
    debug_assert_eq!(buf.len(), HEADER_LEN);
    buf
}

/// Append a `u64[ceil(n/64)]` present bitset (bit `i` set == doc `i` present)
/// to `buf`, returning `(byte_offset, byte_len, n_words)`. Shared by every
/// fixed-width writer.
pub(super) fn append_present_bitset(
    buf: &mut Vec<u8>,
    present: &[bool],
) -> Result<(u64, u64, u64)> {
    // The present bitset is a `u64` column, so its start must be 8-aligned for a
    // zero-copy `try_cast_slice::<u8, u64>` on read. The u64 Number/Hash forward
    // columns already end 8-aligned (this pad is a no-op there, so their bytes
    // are unchanged); the f32 vector column can end mid-word and needs it.
    let pad = (8 - (buf.len() % 8)) % 8;
    buf.resize(buf.len() + pad, 0);
    let off = buf.len();
    let n_words = (present.len() + 63) / 64;
    let mut words = vec![0u64; n_words];
    for (i, &p) in present.iter().enumerate() {
        if p {
            words[i / 64] |= 1u64 << (i % 64);
        }
    }
    let bytes: &[u8] = bytemuck::try_cast_slice(&words)
        .map_err(|e| anyhow!("cast present bitset to bytes: {e:?}"))?;
    buf.extend_from_slice(bytes);
    let len = words.len() * std::mem::size_of::<u64>();
    Ok((off as u64, len as u64, n_words as u64))
}

/// Build the directory entry for a present bitset. Authored once so every
/// writer emits an identical `present` [`ColumnRef`] (fixed-width, u64 words).
pub(super) fn present_column_ref(off: u64, len: u64, n_words: u64) -> ColumnRef {
    ColumnRef {
        name: "present".to_string(),
        role: ROLE_PRESENT,
        byte_offset: off,
        byte_len: len,
        elem_count: n_words,
        width: 8,
        codec: CODEC_FIXED,
        skip_index: Vec::new(),
    }
}

/// Finalize an assembled file: page-pad the fixed-width region tail, append the
/// CBOR directory (crc'd), append the 24-byte footer, then atomically write
/// `path` via a temp file + rename (mirrors `rdb.rs`). Shared by every writer
/// so the header/dir/footer/atomic-write tail is authored exactly once.
pub(super) fn finalize_and_write(path: &Path, mut buf: Vec<u8>, dir: Vec<ColumnRef>) -> Result<()> {
    // Pad the fixed-width region tail to the next page boundary.
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);

    // --- DIRECTORY (CBOR) ---
    let mut dir_bytes: Vec<u8> = Vec::new();
    ciborium::into_writer(&dir, &mut dir_bytes)
        .map_err(|e| anyhow!("cbor encode segment directory: {e}"))?;

    let dir_offset = buf.len() as u64;
    let dir_len = dir_bytes.len() as u64;
    let dir_crc = crc32fast::hash(&dir_bytes);
    buf.extend_from_slice(&dir_bytes);

    // --- FOOTER (24 bytes, very tail) ---
    let footer = Footer {
        dir_offset,
        dir_len,
        crc32: dir_crc,
        magic2: MAGIC2,
    };
    buf.extend_from_slice(&footer.to_bytes());

    // --- Atomic write: temp file + rename (mirror rdb.rs). ---
    let dir_path = path
        .parent()
        .ok_or_else(|| anyhow!("segment path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(dir_path)
        .with_context(|| format!("create segment dir {}", dir_path.display()))?;
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("segment path has no file name: {}", path.display()))?;
    let tmp = dir_path.join(format!(".{file_name}.tmp"));

    {
        let mut f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(&buf)
            .with_context(|| format!("write {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// The order-preserving `SortableF64` bit key of a raw `f64` (Phase 2h-3).
/// MUST byte-match `storage::SortableF64::new(x)`'s inner `u64`: a non-negative
/// value flips the top bit (placing it above negatives); a negative flips all
/// bits (reversing magnitude order among negatives). The resulting `u64` is
/// MONOTONE in numeric order, so an ascending `u64` column is an ascending
/// numeric column. NaN can never reach the seal (rejected at index time), so we
/// do not special-case it here — the seal only feeds finite values.
#[inline]
pub(super) fn sortable_bits(x: f64) -> u64 {
    let bits = x.to_bits();
    if x.is_sign_negative() {
        !bits
    } else {
        bits ^ (1u64 << 63)
    }
}
