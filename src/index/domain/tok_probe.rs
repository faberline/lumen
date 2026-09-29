//! TokProbe: a lazy, non-materializing view of one token's active postings that
//! the AND branch of a match streams and probes instead of merging them.

use roaring::RoaringBitmap;

use crate::index::domain::postings::Postings;

/// A LAZY view of a token's active postings for the `And`-intersection
/// streaming path (see `TextIndex::tok_probe`). Holds only borrowed slices /
/// a shared `Arc` plus a small staged-row `Vec` — NEVER a merged copy of the
/// segment or live postings. `tf(id)` is a set of binary searches (same cost
/// class as `TokPostings::tf`, no allocation); `iter_active()` streams the
/// merged, tombstone-filtered, precedence-resolved `(docid, tf)` pairs in
/// ascending order for the DRIVING (rarest) token only, so an N-token AND
/// never materializes more than one token's postings.
pub(crate) struct TokProbe<'a> {
    pub(crate) seg: Option<std::sync::Arc<(Vec<u32>, Vec<u32>)>>,
    pub(crate) live: Option<&'a Postings>,
    /// `(docid, tf)` pairs, ascending by docid — one entry per staged row that
    /// carries this token (bounded by `staged_rows.len()`, not corpus size).
    pub(crate) staged: Vec<(u32, u32)>,
    pub(crate) tombstones: &'a RoaringBitmap,
}

impl<'a> TokProbe<'a> {
    /// Cheap short-circuit for a token with NO source at all (mirrors
    /// `tok_postings` returning `None` for an unknown token). Does NOT detect
    /// the rare fully-tombstoned-segment-with-no-live/staged case — that
    /// still resolves correctly (just without the early return) because an
    /// empty active posting naturally yields zero matches either as the
    /// driving token (empty `iter_active`) or as a probe (every `tf` misses).
    #[inline]
    pub(crate) fn definitely_absent(&self) -> bool {
        self.seg.is_none() && self.live.is_none() && self.staged.is_empty()
    }

    /// A cheap UPPER BOUND on this token's active postings count, used only to
    /// pick which token drives the intersection (a heuristic, not the exact
    /// `df` — the exact `df` for `idf` comes from `iter_active().count()`).
    #[inline]
    pub(crate) fn upper_bound_len(&self) -> usize {
        self.staged.len()
            + self.live.map_or(0, |p| p.docids.len())
            + self.seg.as_ref().map_or(0, |s| s.0.len())
    }

    /// The EXACT active postings count — `|staged ∪ live ∪ (segment −
    /// tombstones)|`, identical to `iter_active().count()` — without walking
    /// the segment lane row by row. The segment lane is the long one at scale
    /// (a stop-token in a 500k corpus has df≈500k), so the count is derived
    /// as `|overlay| + |segment| − |segment ∩ (tombstones ∪ overlay)|` where
    /// `overlay = staged ∪ live`: the subtraction term is found by galloping
    /// through the segment ids for each id of the (sparse) excluded set, so
    /// the cost is bounded by the overlay and tombstone sizes, not by the
    /// segment length. A probe with no segment lane falls back to the
    /// streaming count, which is then bounded by the overlay itself.
    pub(crate) fn active_len(&self) -> usize {
        let seg_ids: &[u32] = match &self.seg {
            Some(seg) => &seg.0[..],
            None => return self.iter_active().count(),
        };
        let live_ids: &[u32] = self.live.map(|p| &p.docids[..]).unwrap_or(&[]);
        let staged = &self.staged[..];
        // |staged ∪ live| by a two-pointer merge count.
        let mut overlay = 0usize;
        let (mut si, mut li) = (0usize, 0usize);
        while si < staged.len() || li < live_ids.len() {
            let sid = staged.get(si).map(|&(id, _)| id);
            let lid = live_ids.get(li).copied();
            let id = match (sid, lid) {
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => break,
            };
            if sid == Some(id) {
                si += 1;
            }
            if lid == Some(id) {
                li += 1;
            }
            overlay += 1;
        }
        // |segment ∩ (tombstones ∪ live ∪ staged)| by one ascending pass over
        // the three excluded sources, galloping through the segment ids.
        let mut excluded = 0usize;
        let mut pos = 0usize;
        let mut tomb = self.tombstones.iter().peekable();
        let (mut si, mut li) = (0usize, 0usize);
        loop {
            if pos >= seg_ids.len() {
                break;
            }
            let tid = tomb.peek().copied();
            let sid = staged.get(si).map(|&(id, _)| id);
            let lid = live_ids.get(li).copied();
            let id = match [tid, sid, lid].into_iter().flatten().min() {
                Some(id) => id,
                None => break,
            };
            if tid == Some(id) {
                tomb.next();
            }
            if sid == Some(id) {
                si += 1;
            }
            if lid == Some(id) {
                li += 1;
            }
            if gallop_to(seg_ids, &mut pos, id) {
                excluded += 1;
            }
        }
        overlay + seg_ids.len() - excluded
    }

    /// Random-access tf lookup with precedence staged > live > segment (the
    /// same precedence `tok_postings`/`unstaged_tok_postings` compose): a
    /// staged row always wins; a live posting overrides a reused base id even
    /// when that base id is ALSO tombstoned; a pure-segment id is dropped when
    /// tombstoned. No allocation.
    #[inline]
    pub(crate) fn tf(&self, id: u32) -> Option<u32> {
        if let Ok(pos) = self.staged.binary_search_by_key(&id, |&(i, _)| i) {
            return Some(self.staged[pos].1);
        }
        if let Some(live) = self.live {
            if let Some(tf) = live.tf(id) {
                return Some(tf);
            }
        }
        if let Some(seg) = &self.seg {
            if let Ok(pos) = seg.0.binary_search(&id) {
                if !self.tombstones.contains(id) {
                    return Some(seg.1[pos]);
                }
            }
        }
        None
    }

    /// Streams the merged, precedence-resolved, tombstone-filtered `(docid,
    /// tf)` pairs in ascending docid order — a three-way zipper over the
    /// segment/live/staged sorted sources, with NO intermediate `Vec`. Used
    /// for the driving token's candidate walk and for an exact `df` count
    /// (`iter_active().count()`), both bounded by this token's OWN active
    /// postings length, never by the corpus.
    pub(crate) fn iter_active(&self) -> TokProbeIter<'_> {
        TokProbeIter {
            seg_ids: self.seg.as_ref().map(|s| &s.0[..]).unwrap_or(&[]),
            seg_tfs: self.seg.as_ref().map(|s| &s.1[..]).unwrap_or(&[]),
            seg_pos: 0,
            live_ids: self.live.map(|p| &p.docids[..]).unwrap_or(&[]),
            live_tfs: self.live.map(|p| &p.tfs[..]).unwrap_or(&[]),
            live_pos: 0,
            staged: &self.staged[..],
            staged_pos: 0,
            tombstones: self.tombstones,
        }
    }
}

/// Advances `*pos` through the sorted `ids` to the first index whose id is
/// `>= target`, galloping (exponential probe then binary search) from the
/// current position so a sequence of ascending targets costs
/// `O(Σ log gap)` instead of `O(Σ log n)`. Returns whether `ids[*pos]` is
/// exactly `target`.
fn gallop_to(ids: &[u32], pos: &mut usize, target: u32) -> bool {
    let n = ids.len();
    if *pos >= n || ids[*pos] >= target {
        return *pos < n && ids[*pos] == target;
    }
    let mut lo = *pos;
    let mut step = 1usize;
    let mut hi = lo + step;
    while hi < n && ids[hi] < target {
        lo = hi;
        step <<= 1;
        hi = lo + step;
    }
    let end = hi.min(n);
    *pos = match ids[lo + 1..end].binary_search(&target) {
        Ok(i) | Err(i) => lo + 1 + i,
    };
    *pos < n && ids[*pos] == target
}

pub(crate) struct TokProbeIter<'a> {
    seg_ids: &'a [u32],
    seg_tfs: &'a [u32],
    seg_pos: usize,
    live_ids: &'a [u32],
    live_tfs: &'a [u32],
    live_pos: usize,
    staged: &'a [(u32, u32)],
    staged_pos: usize,
    tombstones: &'a RoaringBitmap,
}

impl<'a> Iterator for TokProbeIter<'a> {
    type Item = (u32, u32);
    fn next(&mut self) -> Option<(u32, u32)> {
        loop {
            let seg_id = self.seg_ids.get(self.seg_pos).copied();
            let live_id = self.live_ids.get(self.live_pos).copied();
            let staged_id = self.staged.get(self.staged_pos).map(|&(id, _)| id);
            let id = match [seg_id, live_id, staged_id].into_iter().flatten().min() {
                Some(id) => id,
                None => return None,
            };
            let mut tf = None;
            if staged_id == Some(id) {
                tf = Some(self.staged[self.staged_pos].1);
                self.staged_pos += 1;
            }
            if live_id == Some(id) {
                if tf.is_none() {
                    tf = Some(self.live_tfs[self.live_pos]);
                }
                self.live_pos += 1;
            }
            if seg_id == Some(id) {
                if tf.is_none() && !self.tombstones.contains(id) {
                    tf = Some(self.seg_tfs[self.seg_pos]);
                }
                self.seg_pos += 1;
            }
            if let Some(tf) = tf {
                return Some((id, tf));
            }
            // Pure-segment id, tombstoned, no live/staged override: skip and
            // continue the merge (matches `unstaged_tok_postings`'s subtraction).
        }
    }
}

#[cfg(test)]
mod tests;
