//! A multi-token AND match ranked from its rarest token: each other token is
//! probed with a streaming cursor when its postings are dense next to the
//! driver's, or with a binary search when they are sparse, and the scores come
//! out unsorted for the rank cache.

use roaring::RoaringBitmap;

use crate::index::domain::query::rank::{bm25_contrib, text_doc_len_at};
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::tok_probe::TokProbe;

pub(super) struct AndRankInput<'a> {
    pub(super) idx: &'a TextIndex,
    pub(super) posts: &'a [TokProbe<'a>],
    pub(super) idfs: &'a [f32],
    pub(super) drive: usize,
    pub(super) drive_len: usize,
    pub(super) avgdl: f32,
}

/// A forward-only cursor over one non-driving token's active postings
/// (`TokProbe::iter_active`), used by the dense-AND zipper in
/// `build_and_ranked`. `advance_to` walks the underlying merge iterator only
/// as far as it needs to catch up to `target` — never backward, which is
/// safe because the driver's own walk visits ids in strictly ascending
/// order — so intersecting an N-token AND whose OTHER postings are
/// comparably sized to the driver costs `O(drive_len + Σ other postings)`
/// total instead of `O(drive_len × Σ log(other postings))` for the
/// per-doc binary-search probe (`TokProbe::tf`).
enum ZipCursor<'a> {
    /// FAST LANE: this token has no segment and no staged-row contribution,
    /// so its whole active posting is exactly one ascending `(docids, tfs)`
    /// live slice pair — advancing is a raw index scan over two `&[u32]`
    /// slices, with none of the per-step 3-way-merge bookkeeping
    /// (`TokProbeIter::next`'s array/`flatten`/`min` over seg/live/staged)
    /// that a general probe needs. This is the common case for the
    /// 500k-hot-doc AND fixture (no sealed segment, no in-flight staged
    /// rows for most of a query's lifetime).
    LiveOnly {
        ids: &'a [u32],
        tfs: &'a [u32],
        pos: usize,
    },
    /// GENERAL: any token with a segment and/or staged-row contribution.
    /// This is the REAL 500k-hot-doc path once a checkpoint has published
    /// (every hot token then has a segment posting), so it must be as tight
    /// as the live lane: the three ascending sources are advanced
    /// independently by raw index scans and precedence (staged > live >
    /// segment-minus-tombstones, exactly `TokProbe::tf`) is resolved only AT
    /// `target` — never a per-element three-way merge, never a per-element
    /// tombstone lookup (the tombstone test is per DRIVER doc, shared across
    /// all cursors through `advance_to`'s `tombstoned` cache).
    General {
        seg_ids: &'a [u32],
        seg_tfs: &'a [u32],
        seg_pos: usize,
        live_ids: &'a [u32],
        live_tfs: &'a [u32],
        live_pos: usize,
        staged: &'a [(u32, u32)],
        staged_pos: usize,
        tombstones: &'a RoaringBitmap,
    },
}

impl<'a> ZipCursor<'a> {
    fn new(p: &'a TokProbe<'a>) -> Self {
        if p.seg.is_none() && p.staged.is_empty() {
            if let Some(live) = p.live {
                return ZipCursor::LiveOnly {
                    ids: &live.docids,
                    tfs: &live.tfs,
                    pos: 0,
                };
            }
        }
        ZipCursor::General {
            seg_ids: p.seg.as_ref().map(|s| &s.0[..]).unwrap_or(&[]),
            seg_tfs: p.seg.as_ref().map(|s| &s.1[..]).unwrap_or(&[]),
            seg_pos: 0,
            live_ids: p.live.map(|l| &l.docids[..]).unwrap_or(&[]),
            live_tfs: p.live.map(|l| &l.tfs[..]).unwrap_or(&[]),
            live_pos: 0,
            staged: &p.staged[..],
            staged_pos: 0,
            tombstones: p.tombstones,
        }
    }

    /// Advances past every entry `< target`, then reports `target`'s tf if
    /// the cursor now sits exactly on it. Yields the identical `Option<u32>`
    /// `TokProbe::tf(target)` would, for any `target` no smaller than a
    /// previous call's `target` on this cursor. `tombstoned` is the caller's
    /// per-`target` memo of `tombstones.contains(target)`: every probe of one
    /// AND comes from the same `TextIndex` and therefore shares one tombstone
    /// set, so the first cursor that needs the answer for this `target`
    /// computes it and the rest reuse it (`None` = not computed yet).
    fn advance_to(&mut self, target: u32, tombstoned: &mut Option<bool>) -> Option<u32> {
        match self {
            ZipCursor::LiveOnly { ids, tfs, pos } => {
                while *pos < ids.len() && ids[*pos] < target {
                    *pos += 1;
                }
                if *pos < ids.len() && ids[*pos] == target {
                    Some(tfs[*pos])
                } else {
                    None
                }
            }
            ZipCursor::General {
                seg_ids,
                seg_tfs,
                seg_pos,
                live_ids,
                live_tfs,
                live_pos,
                staged,
                staged_pos,
                tombstones,
            } => {
                while *staged_pos < staged.len() && staged[*staged_pos].0 < target {
                    *staged_pos += 1;
                }
                if *staged_pos < staged.len() && staged[*staged_pos].0 == target {
                    return Some(staged[*staged_pos].1);
                }
                while *live_pos < live_ids.len() && live_ids[*live_pos] < target {
                    *live_pos += 1;
                }
                if *live_pos < live_ids.len() && live_ids[*live_pos] == target {
                    return Some(live_tfs[*live_pos]);
                }
                while *seg_pos < seg_ids.len() && seg_ids[*seg_pos] < target {
                    *seg_pos += 1;
                }
                if *seg_pos < seg_ids.len() && seg_ids[*seg_pos] == target {
                    let dead = *tombstoned.get_or_insert_with(|| tombstones.contains(target));
                    if !dead {
                        return Some(seg_tfs[*seg_pos]);
                    }
                }
                None
            }
        }
    }
}

/// `ceil(log2(x))` for `x >= 1` (`0` for `x <= 1`) — sized only for the
/// dense/sparse zipper heuristic below.
#[inline]
fn ceil_log2(x: usize) -> u32 {
    if x <= 1 {
        0
    } else {
        usize::BITS - (x - 1).leading_zeros()
    }
}

/// Chooses the streaming zipper probe over the per-doc binary-search probe
/// when the OTHER tokens' postings are dense enough, relative to the driver,
/// that a full streaming pass over them is expected to do no more work than
/// the binary searches would. The binary-search alternative probes EVERY
/// other token once per driver doc, so its comparison cost is
/// `drive_len × Σ_{k≠drive} ceil(log2(upper_bound_len(k)))`, against the
/// zipper's `Σ_{k≠drive} upper_bound_len(k)` streamed entries — the
/// inequality compares those two sums. (An earlier form multiplied
/// `drive_len` by a SINGLE `log2(max len)` — off by the token count — and
/// judged the 20-token, df≈500k `title_ngram` "durable search" AND to be
/// sparse, sending it down 19 × 500k segment binary searches per query: the
/// ~0.5-1 s cold `match` the 500k durable perf cell collapsed on.) A
/// sparse/rare driver against much larger other postings still fails the
/// inequality and keeps the binary-search probe, unchanged.
fn should_use_zipper(posts: &[TokProbe<'_>], drive: usize, drive_len: usize) -> bool {
    if posts.len() <= 1 {
        return false;
    }
    let mut sum_other = 0usize;
    let mut sum_log_other = 0usize;
    for (k, p) in posts.iter().enumerate() {
        if k == drive {
            continue;
        }
        let len = p.upper_bound_len().max(1);
        sum_other += len;
        sum_log_other += ceil_log2(len).max(1) as usize;
    }
    sum_other <= drive_len.saturating_mul(sum_log_other)
}

/// Streams the driving (rarest) token's active postings (`TokProbe::iter_active`,
/// no per-token merged-`Vec` materialization — the 500k-hot-doc AND perf fix)
/// and probes every other token either with a streaming zipper cursor
/// (`ZipCursor`, dense case) or `TokProbe::tf` (a binary search, sparse
/// case) — see `should_use_zipper`. The per-doc score still sums each
/// token's BM25 contribution in ORIGINAL TOKEN ORDER (index `k` low to high,
/// driver's own contribution taken from the SAME merge pass instead of a
/// redundant re-lookup), so the f32 result is byte-identical on both probe
/// strategies and to the old per-token merged-postings walk. Returns the
/// UNSORTED scores — see `build_single_token_ranked`'s doc comment on why.
pub(super) fn build_and_ranked(input: AndRankInput<'_>) -> Vec<(u32, f32)> {
    let mut ranked = Vec::with_capacity(input.drive_len);
    let segment_doc_lens = input
        .idx
        .segment
        .as_ref()
        .and_then(|seg| seg.text_doc_lens());
    if should_use_zipper(input.posts, input.drive, input.drive_len) {
        let mut cursors: Vec<Option<ZipCursor<'_>>> = input
            .posts
            .iter()
            .enumerate()
            .map(|(k, p)| {
                if k == input.drive {
                    None
                } else {
                    Some(ZipCursor::new(p))
                }
            })
            .collect();
        'docs: for (id, drive_tf) in input.posts[input.drive].iter_active() {
            let doc_len = text_doc_len_at(input.idx, segment_doc_lens, id) as f32;
            let mut score = 0.0f32;
            // One tombstone lookup per driver doc at most, shared by every
            // cursor (see `ZipCursor::advance_to`).
            let mut tombstoned: Option<bool> = None;
            for (k, cursor) in cursors.iter_mut().enumerate() {
                let tf = if k == input.drive {
                    drive_tf
                } else {
                    match cursor
                        .as_mut()
                        .expect("non-driver index always has a cursor")
                        .advance_to(id, &mut tombstoned)
                    {
                        Some(tf) => tf,
                        None => continue 'docs,
                    }
                };
                score += bm25_contrib(input.idfs[k], tf as f32, doc_len, input.avgdl);
            }
            ranked.push((id, score));
        }
    } else {
        'docs2: for (id, drive_tf) in input.posts[input.drive].iter_active() {
            let doc_len = text_doc_len_at(input.idx, segment_doc_lens, id) as f32;
            let mut score = 0.0f32;
            for (k, p) in input.posts.iter().enumerate() {
                let tf = if k == input.drive {
                    drive_tf
                } else {
                    match p.tf(id) {
                        Some(tf) => tf,
                        None => continue 'docs2,
                    }
                };
                score += bm25_contrib(input.idfs[k], tf as f32, doc_len, input.avgdl);
            }
            ranked.push((id, score));
        }
    }
    ranked
}

#[cfg(test)]
mod tests;
