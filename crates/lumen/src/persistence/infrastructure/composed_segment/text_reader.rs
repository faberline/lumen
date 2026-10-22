//! Text postings merged across layers: each layer contributes the rows no newer
//! layer covers, filtered by coverage without materializing the older postings.

use crate::persistence::infrastructure::composed_segment::{
    note_text_term_probes, ComposedSegmentReader, CoverageUnions, TextPostingAt,
};
use crate::persistence::infrastructure::segment::codecs::SortedIdCursor;
use crate::persistence::infrastructure::segment::reader_cache::CachedTextPosting;
use roaring::RoaringBitmap;
use std::sync::Arc;

/// Copies every `(id, tf)` of a posting sorted by ascending unique `id` whose
/// id is NOT in `covered`, preserving order. Sparse coverage gallops: each
/// covered id is located by binary search from the last hit and the runs
/// between hits are copied whole, O(|covered| · log df + df memcpy). Dense
/// coverage walks both sorted sequences in lockstep, O(df + |covered|).
pub(super) fn retain_uncovered_sorted(
    ids: &[u32],
    tfs: &[u32],
    covered: &RoaringBitmap,
    out_ids: &mut Vec<u32>,
    out_tfs: &mut Vec<u32>,
) {
    debug_assert_eq!(ids.len(), tfs.len());
    out_ids.reserve(ids.len());
    out_tfs.reserve(ids.len());
    if covered.is_empty() || ids.is_empty() {
        out_ids.extend_from_slice(ids);
        out_tfs.extend_from_slice(tfs);
        return;
    }
    let last = *ids.last().expect("non-empty");
    if covered.len().saturating_mul(16) < ids.len() as u64 {
        let (mut start, mut pos) = (0usize, 0usize);
        for id in covered.iter() {
            if id > last || pos >= ids.len() {
                break;
            }
            match ids[pos..].binary_search(&id) {
                Ok(offset) => {
                    let hit = pos + offset;
                    out_ids.extend_from_slice(&ids[start..hit]);
                    out_tfs.extend_from_slice(&tfs[start..hit]);
                    start = hit + 1;
                    pos = hit + 1;
                }
                Err(offset) => pos += offset,
            }
        }
        out_ids.extend_from_slice(&ids[start..]);
        out_tfs.extend_from_slice(&tfs[start..]);
        return;
    }
    let mut hidden = covered.iter().peekable();
    for (&id, &tf) in ids.iter().zip(tfs) {
        while hidden.peek().is_some_and(|&h| h < id) {
            hidden.next();
        }
        if hidden.peek() == Some(&id) {
            continue;
        }
        out_ids.push(id);
        out_tfs.push(tf);
    }
}

impl ComposedSegmentReader {
    pub(crate) fn text_postings_arc(&self, token: &str) -> Option<Arc<(Vec<u32>, Vec<u32>)>> {
        note_text_term_probes(1);
        if self.dense_base_only() {
            return self.base.text_postings_arc(token);
        }
        if let Some(hit) = self.query_cache.text_postings.get(token) {
            return Some(hit);
        }
        let merged = self.merge_text_postings(token)?;
        self.query_cache
            .text_postings
            .insert(token.to_owned(), merged.clone());
        Some(merged)
    }

    /// The layered `(docids, tfs)` merge behind [`Self::text_postings_arc`]
    /// for a composition that is not a bare dense base. Pure in the
    /// composition, so `text_postings_arc` caches its result per token in
    /// `query_cache`: a cold 20-token ngram AND against a 500k-row base with
    /// live delta layers otherwise re-decoded and re-merged 20 × 500k rows
    /// on every request (#4246).
    pub(super) fn merge_text_postings(&self, token: &str) -> Option<CachedTextPosting> {
        // Byte-identical to `merge_text_postings_reference` (the
        // `#[cfg(test)]` original), which paid one O(df) `retain` sweep per
        // layer per token. Here the base posting is filtered ONCE by the
        // union of every layer's coverage, and each layer's posting by the
        // union of the coverage newer than it; a torn map still yields `None`
        // and every reader is probed exactly once per token.
        let unions = self.coverage_unions();
        let (mut base_ids, mut base_tfs) = (Vec::new(), Vec::new());
        if let Some(base) = self.base.text_postings_arc(token) {
            if let Some(map) = &self.base_map {
                let mut mapped = Vec::with_capacity(base.0.len());
                for (&local, &tf) in base.0.iter().zip(&base.1) {
                    let id = *map.ids.get(local as usize)?;
                    if !unions.all.contains(id) {
                        mapped.push((id, tf));
                    }
                }
                if !unions.base_map_ascending {
                    mapped.sort_unstable_by_key(|&(id, _)| id);
                }
                base_ids.reserve_exact(mapped.len());
                base_tfs.reserve_exact(mapped.len());
                for (id, tf) in mapped {
                    base_ids.push(id);
                    base_tfs.push(tf);
                }
            } else {
                retain_uncovered_sorted(
                    &base.0,
                    &base.1,
                    &unions.all,
                    &mut base_ids,
                    &mut base_tfs,
                );
            }
        }
        let mut newer = Vec::new();
        for (index, layer) in self.layers.iter().enumerate() {
            if let Some(posting) = layer.reader.text_postings_arc(token) {
                let hidden = &unions.newer_than[index];
                for (&local, &tf) in posting.0.iter().zip(&posting.1) {
                    let id = *layer.ids.get(local as usize)?;
                    if !hidden.contains(id) {
                        newer.push((id, tf));
                    }
                }
            }
        }
        if newer.is_empty() {
            if base_ids.is_empty() {
                return None;
            }
            return Some(Arc::new((base_ids, base_tfs)));
        }
        // Every surviving id is unique: a layer row survives only from the
        // newest layer covering it, and no surviving base row is covered.
        newer.sort_unstable_by_key(|&(id, _)| id);
        let total = base_ids.len() + newer.len();
        let (mut ids, mut tfs) = (Vec::with_capacity(total), Vec::with_capacity(total));
        let (mut b, mut n) = (0, 0);
        while b < base_ids.len() && n < newer.len() {
            if base_ids[b] < newer[n].0 {
                ids.push(base_ids[b]);
                tfs.push(base_tfs[b]);
                b += 1;
            } else {
                ids.push(newer[n].0);
                tfs.push(newer[n].1);
                n += 1;
            }
        }
        ids.extend_from_slice(&base_ids[b..]);
        tfs.extend_from_slice(&base_tfs[b..]);
        for &(id, tf) in &newer[n..] {
            ids.push(id);
            tfs.push(tf);
        }
        Some(Arc::new((ids, tfs)))
    }

    fn coverage_unions(&self) -> &CoverageUnions {
        self.query_cache.coverage_unions.get_or_init(|| {
            let mut newer_than = vec![RoaringBitmap::new(); self.layers.len()];
            let mut acc = RoaringBitmap::new();
            for (index, layer) in self.layers.iter().enumerate().rev() {
                newer_than[index] = acc.clone();
                acc |= &layer.coverage;
            }
            let base_map_ascending = self
                .base_map
                .as_ref()
                .is_none_or(|map| map.ids.windows(2).all(|w| w[0] < w[1]));
            CoverageUnions {
                all: acc,
                newer_than,
                base_map_ascending,
            }
        })
    }

    /// The original layered merge, kept as the oracle for the rewrite above.
    #[cfg(test)]
    pub(super) fn merge_text_postings_reference(&self, token: &str) -> Option<CachedTextPosting> {
        // Decode one token at a time. Each pass holds two sorted postings
        // and removes covered older rows before merging the newer values.
        let mut out = Vec::new();
        if let Some(base) = self.base.text_postings_arc(token) {
            for (&local, &tf) in base.0.iter().zip(&base.1) {
                let id = if let Some(map) = &self.base_map {
                    *map.ids.get(local as usize)?
                } else {
                    local
                };
                out.push((id, tf));
            }
            if self.base_map.is_some() {
                out.sort_unstable_by_key(|&(id, _)| id);
            }
        }
        for layer in &self.layers {
            out.retain(|&(id, _)| !layer.coverage.contains(id));
            if let Some(posting) = layer.reader.text_postings_arc(token) {
                let mut incoming = Vec::with_capacity(posting.0.len());
                for (&local, &tf) in posting.0.iter().zip(&posting.1) {
                    incoming.push((*layer.ids.get(local as usize)?, tf));
                }
                incoming.sort_unstable_by_key(|&(id, _)| id);
                let mut merged = Vec::with_capacity(out.len() + incoming.len());
                let mut older = out.into_iter().peekable();
                let mut newer = incoming.into_iter().peekable();
                while let (Some(left), Some(right)) = (older.peek(), newer.peek()) {
                    if left.0 < right.0 {
                        merged.push(older.next()?);
                    } else {
                        merged.push(newer.next()?);
                    }
                }
                merged.extend(older);
                merged.extend(newer);
                out = merged;
            }
        }
        if out.is_empty() {
            return None;
        }
        let (ids, tfs) = out.into_iter().unzip();
        Some(Arc::new((ids, tfs)))
    }
    pub(crate) fn text_token_df(&self, token: &str) -> usize {
        if self.dense_base_only() {
            return self.base.text_token_df(token);
        }
        self.text_postings_arc(token).map_or(0, |p| p.0.len())
    }

    /// `token`'s posting for a small candidate set (#4246). A posting that is
    /// already resident — the composition's per-query cache, or the dense
    /// base's bounded posting cache — comes back whole as
    /// [`TextPostingAt::Cached`]. A cold posting is never materialized:
    /// the base is streamed through
    /// [`crate::persistence::infrastructure::segment::SegmentReader::text_posting_scan`]
    /// and each layer's (small) posting is walked, yielding the exact composed
    /// df and the tf of every id in `candidates` (ascending, distinct) as
    /// [`TextPostingAt::Sparse`]. Ids `hidden` returns true for count toward
    /// neither — the caller folds its own overlays (tombstones, live and
    /// staged rows) through it. Composition semantics are those of
    /// [`Self::text_postings_arc`]: the newest layer covering an id owns its
    /// row, an uncovered id falls back to the mapped base, and a torn map
    /// yields `None`. For a dense base `None` also means the dictionary lacks
    /// the token, so a `None` can always be re-resolved through
    /// `text_postings_arc` cheaply.
    pub(crate) fn text_posting_at(
        &self,
        token: &str,
        candidates: &[u32],
        mut hidden: impl FnMut(u32) -> bool,
    ) -> Option<TextPostingAt> {
        note_text_term_probes(1);
        debug_assert!(candidates.windows(2).all(|w| w[0] < w[1]));
        if self.dense_base_only() {
            if let Some(hit) = self.base.text_posting_cached(token) {
                return Some(TextPostingAt::Cached(hit));
            }
            let mut df = 0usize;
            let mut hits = Vec::new();
            let mut wanted = SortedIdCursor::new(candidates);
            self.base.text_posting_scan(token, |id, tf| {
                if hidden(id) {
                    return;
                }
                df += 1;
                if wanted.contains(id) {
                    hits.push((id, tf));
                }
            })?;
            return Some(TextPostingAt::Sparse { df, hits });
        }
        if let Some(hit) = self.query_cache.text_postings.get(token) {
            return Some(TextPostingAt::Cached(hit));
        }
        let unions = self.coverage_unions();
        let mut df = 0usize;
        let mut hits = Vec::new();
        let mut torn = false;
        {
            let map = self.base_map.as_deref();
            let mut wanted = SortedIdCursor::new(candidates);
            let scanned = self.base.text_posting_scan(token, |local, tf| {
                let id = match map {
                    Some(map) => match map.ids.get(local as usize) {
                        Some(&id) => id,
                        None => {
                            torn = true;
                            return;
                        }
                    },
                    None => local,
                };
                if unions.all.contains(id) || hidden(id) {
                    return;
                }
                df += 1;
                if wanted.contains(id) {
                    hits.push((id, tf));
                }
            });
            if scanned.is_none() {
                // A torn or absent base posting contributes nothing, exactly
                // as `merge_text_postings` treats a `None` base read.
                df = 0;
                hits.clear();
            }
        }
        if torn {
            return None;
        }
        for (index, layer) in self.layers.iter().enumerate() {
            let Some(posting) = layer.reader.text_postings_arc(token) else {
                continue;
            };
            let newer = &unions.newer_than[index];
            for (&local, &tf) in posting.0.iter().zip(&posting.1) {
                let id = *layer.ids.get(local as usize)?;
                if newer.contains(id) || hidden(id) {
                    continue;
                }
                df += 1;
                if candidates.binary_search(&id).is_ok() {
                    hits.push((id, tf));
                }
            }
        }
        // Every surviving id is unique (see `merge_text_postings`), so a
        // sort is all the merge needs.
        hits.sort_unstable_by_key(|&(id, _)| id);
        Some(TextPostingAt::Sparse { df, hits })
    }
}
