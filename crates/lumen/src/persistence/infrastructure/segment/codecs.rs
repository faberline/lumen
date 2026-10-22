//! The posting-block codecs: LEB128 varints, the text `(docid, tf)` blob, the
//! docid-only blob Keyword, Set and Number postings use, a cursor for probing
//! ascending docids, and the fixed-width `u32` column writer.

use anyhow::{anyhow, Result};

// ---------------------------------------------------------------------------
// Text posting-block codec (Phase 2e-B)
// ---------------------------------------------------------------------------
//
// Text term-frequency is NOT rebuildable from a forward column the way the
// Keyword/Set inverted indexes are, so one token's posting list is STORED as a
// self-describing blob — the "entry" pushed into the shared [`VarColumnWriter`]
// at the token's dict-id. The blob is a tight LEB128 delta-varint stream:
//
//   [count : LEB128]
//   then `count` postings, in the SAME ascending-docid order they hold in the
//   live `Postings` SoA, each as:
//     [docid_gap : LEB128]   (gap from the previous docid; first gap == docid0)
//     [tf        : LEB128]
//
// Ascending docids make the gaps small; LZ4 then squeezes the blob in the var
// block. `decode_posting_block` rebuilds `(docids, tfs)` by rolling the gap
// forward, reproducing the exact streams the live BM25 scan reads.

/// Append `v` to `out` as an unsigned LEB128 varint.
pub(super) fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Read an unsigned LEB128 varint from `buf` at `*pos`, advancing `*pos`.
/// `None` on a truncated / overlong (>10-byte) varint — never panics.
pub(super) fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        // A u64 is at most 10 LEB128 bytes; reject an overlong run rather than
        // silently wrapping the shift.
        if shift >= 64 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
    }
}

/// Encode one token's `(docids, tfs)` postings into a delta-varint blob. The
/// caller guarantees `docids` is ascending and `docids.len() == tfs.len()`.
pub(super) fn encode_posting_block(docids: &[u32], tfs: &[u32]) -> Vec<u8> {
    debug_assert_eq!(docids.len(), tfs.len());
    let mut out = Vec::with_capacity(docids.len() * 2 + 2);
    write_varint(&mut out, docids.len() as u64);
    let mut prev: u32 = 0;
    for (i, &id) in docids.iter().enumerate() {
        // First gap is `id - 0 == id`; subsequent gaps are strictly positive
        // (docids ascending & distinct).
        let gap = id.wrapping_sub(prev);
        write_varint(&mut out, gap as u64);
        write_varint(&mut out, tfs[i] as u64);
        prev = id;
    }
    out
}

/// Decode a posting blob back into `(docids, tfs)`. `None` on a truncated /
/// malformed blob — never panics. Reproduces the exact ascending-docid streams
/// [`encode_posting_block`] consumed.
pub(super) fn decode_posting_block(blob: &[u8]) -> Option<(Vec<u32>, Vec<u32>)> {
    let mut pos = 0usize;
    let count = read_varint(blob, &mut pos)? as usize;
    let mut docids = Vec::with_capacity(count);
    let mut tfs = Vec::with_capacity(count);
    let mut prev: u32 = 0;
    for _ in 0..count {
        let gap = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
        let tf = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
        let id = prev.checked_add(gap)?;
        docids.push(id);
        tfs.push(tf);
        prev = id;
    }
    Some((docids, tfs))
}

/// Membership probe over an ascending, distinct docid slice for a stream of
/// queries that is *usually* ascending (#4246): a query at or beyond the last
/// one advances a cursor (amortized O(1) per query), and a query that steps
/// backwards falls back to a binary search without moving the cursor, so the
/// answer is exact in every order. This is what lets
/// [`SegmentReader::text_posting_scan`](super::SegmentReader::text_posting_scan)
/// visitors filter a 500k-entry posting against tombstones, live overlays and
/// a candidate set at decode speed instead of a binary search per entry.
pub(crate) struct SortedIdCursor<'a> {
    ids: &'a [u32],
    pos: usize,
}

impl<'a> SortedIdCursor<'a> {
    pub(crate) fn new(ids: &'a [u32]) -> Self {
        debug_assert!(ids.windows(2).all(|w| w[0] < w[1]));
        SortedIdCursor { ids, pos: 0 }
    }

    /// Whether `id` is in the slice.
    #[inline]
    pub(crate) fn contains(&mut self, id: u32) -> bool {
        if self.pos > 0 && self.ids[self.pos - 1] >= id {
            return self.ids.binary_search(&id).is_ok();
        }
        while self.pos < self.ids.len() && self.ids[self.pos] < id {
            self.pos += 1;
        }
        self.pos < self.ids.len() && self.ids[self.pos] == id
    }
}

// ---------------------------------------------------------------------------
// Keyword docid-only posting-block codec (Phase 2h-1)
// ---------------------------------------------------------------------------
//
// A Keyword (or Set) term holds a doc at most once, so — unlike Text — there is
// NO term-frequency to carry. A term's posting list is therefore stored as a
// docid-only delta-varint blob (the "entry" pushed into the shared
// [`VarColumnWriter`] at the term's dict-id):
//
//   [count : LEB128]
//   then `count` ascending docids, each as:
//     [docid_gap : LEB128]   (gap from the previous docid; first gap == docid0)
//
// `count` is the term's document frequency (`df`), readable on its own — the
// boolean planner's rarest-first ordering only needs `df`, so it decodes just
// this prefix (mirrors `text_token_df`). `decode_docid_block` rebuilds the
// ascending docid stream, which is fed straight into a `RoaringBitmap` so the
// segment-driven AND/OR algebra is bit-identical to the in-RAM `terms` bitmap.

/// Encode a term's ascending `docids` into a count-prefixed delta-varint blob.
/// The caller guarantees `docids` is strictly ascending and distinct. Mirrors
/// [`encode_posting_block`] minus the per-doc `tf` leg.
pub(super) fn encode_docid_block(docids: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(docids.len() + 2);
    write_varint(&mut out, docids.len() as u64);
    let mut prev: u32 = 0;
    for &id in docids {
        // First gap is `id - 0 == id`; subsequent gaps are strictly positive
        // (docids ascending & distinct).
        let gap = id.wrapping_sub(prev);
        write_varint(&mut out, gap as u64);
        prev = id;
    }
    out
}

/// Decode a docid-only posting blob back into the ascending docid stream. `None`
/// on a truncated / malformed blob — never panics. Reproduces the exact
/// ascending-docid stream [`encode_docid_block`] consumed.
pub(super) fn decode_docid_block(blob: &[u8]) -> Option<Vec<u32>> {
    let mut pos = 0usize;
    let count = read_varint(blob, &mut pos)? as usize;
    let mut docids = Vec::with_capacity(count);
    let mut prev: u32 = 0;
    for _ in 0..count {
        let gap = u32::try_from(read_varint(blob, &mut pos)?).ok()?;
        let id = prev.checked_add(gap)?;
        docids.push(id);
        prev = id;
    }
    Some(docids)
}

/// Append a fixed-width `u32` column (one of the Keyword/Set id/offset/packed
/// columns) to `buf`, returning `(byte_offset, byte_len)`. The column start is
/// 4-aligned so `try_cast_slice::<u8, u32>` succeeds on read; the page-aligned
/// fixed-region start plus 4-aligned running padding keeps every `u32` column
/// 4-aligned.
pub(super) fn append_u32_column(buf: &mut Vec<u8>, values: &[u32]) -> Result<(u64, u64)> {
    let pad = (4 - (buf.len() % 4)) % 4;
    buf.resize(buf.len() + pad, 0);
    let off = buf.len() as u64;
    let bytes: &[u8] =
        bytemuck::try_cast_slice(values).map_err(|e| anyhow!("cast u32 column: {e:?}"))?;
    buf.extend_from_slice(bytes);
    let len = (values.len() * std::mem::size_of::<u32>()) as u64;
    Ok((off, len))
}
