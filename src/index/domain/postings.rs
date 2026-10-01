//! Posting lists: a token's docid-sorted (docid, tf) arrays, and the forms BM25
//! scoring resolves them into, whether live, sealed, combined or projected onto
//! a few candidates.

/// A token's posting list as flat, docid-sorted parallel arrays (struct-of-arrays).
/// The BM25 scan reads `docids`/`tfs` sequentially (cache-friendly), and `df` is
/// exactly `docids.len()`. Replaces the old `BTreeMap<u32,u32>` whose per-doc
/// access chased heap-scattered tree nodes.
#[derive(Debug, Default, Clone)]
pub(crate) struct Postings {
    pub(in crate::index) docids: Vec<u32>,
    pub(in crate::index) tfs: Vec<u32>,
}

impl Postings {
    /// Build a posting list from ascending `(docid, tf)` pairs. Crate-internal,
    /// used by the Text segment writer round-trip test to fabricate postings
    /// without the full index. Phase 2e-B.
    #[cfg(test)]
    pub(crate) fn from_sorted(docids: Vec<u32>, tfs: Vec<u32>) -> Self {
        debug_assert_eq!(docids.len(), tfs.len());
        Postings { docids, tfs }
    }
    /// The docid-sorted doc ids of this posting list (used by the Text segment
    /// writer to delta-encode the posting block). Phase 2e-B.
    pub(crate) fn docids(&self) -> &[u32] {
        &self.docids
    }
    /// The term frequencies, parallel to [`Self::docids`]. Phase 2e-B.
    pub(crate) fn tfs(&self) -> &[u32] {
        &self.tfs
    }
    /// Insert/overwrite `id`'s tf, keeping `docids` sorted. For the monotonic
    /// bulk-index path the insertion point is the tail (O(1) amortized); a reused
    /// id (interner reuses ids; an overwrite drops first) lands in sorted order.
    pub(crate) fn upsert(&mut self, id: u32, tf: u32) {
        match self.docids.last().copied() {
            None => {
                self.docids.push(id);
                self.tfs.push(tf);
                return;
            }
            Some(last) if last < id => {
                self.docids.push(id);
                self.tfs.push(tf);
                return;
            }
            Some(last) if last == id => {
                if let Some(last_tf) = self.tfs.last_mut() {
                    *last_tf = tf;
                }
                return;
            }
            _ => {}
        }
        match self.docids.binary_search(&id) {
            Ok(pos) => self.tfs[pos] = tf,
            Err(pos) => {
                self.docids.insert(pos, id);
                self.tfs.insert(pos, tf);
            }
        }
    }
    pub(in crate::index) fn upsert_add(&mut self, id: u32, delta: u32) {
        match self.docids.last().copied() {
            None => {
                self.docids.push(id);
                self.tfs.push(delta);
                return;
            }
            Some(last) if last < id => {
                self.docids.push(id);
                self.tfs.push(delta);
                return;
            }
            Some(last) if last == id => {
                if let Some(last_tf) = self.tfs.last_mut() {
                    *last_tf += delta;
                }
                return;
            }
            _ => {}
        }
        match self.docids.binary_search(&id) {
            Ok(pos) => self.tfs[pos] += delta,
            Err(pos) => {
                self.docids.insert(pos, id);
                self.tfs.insert(pos, delta);
            }
        }
    }
    pub(in crate::index) fn remove(&mut self, id: u32) -> bool {
        match self.docids.binary_search(&id) {
            Ok(pos) => {
                self.docids.remove(pos);
                self.tfs.remove(pos);
                true
            }
            Err(_) => false,
        }
    }
    /// tf of `id`, if present (random access for the filtered-AND predicate path).
    pub(super) fn tf(&self, id: u32) -> Option<u32> {
        self.docids.binary_search(&id).ok().map(|pos| self.tfs[pos])
    }
    pub(super) fn df(&self) -> usize {
        self.docids.len()
    }
}

/// A token's active posting resolved for a SMALL candidate set only (#4246):
/// the exact composed `df` plus the `(docid, tf)` pairs of those candidates the
/// token covers, ascending. Built by [`TextIndex::tok_postings_at`] without
/// decoding, caching or merging the token's full posting, so a 1-candidate
/// `and[filter, match]` over a 500k-doc stop token costs one streamed pass per
/// distinct token instead of a 4 MiB materialization per token occurrence.
///
/// [`TextIndex::tok_postings_at`]: crate::index::domain::text_index::TextIndex::tok_postings_at
pub(in crate::index) struct SparsePosting {
    pub(super) df: usize,
    pub(super) docids: Vec<u32>,
    pub(super) tfs: Vec<u32>,
}

/// A token's postings resolved for BM25 scoring, from EITHER the live in-RAM
/// `Postings` (borrowed, zero-copy) or a sealed Text segment's decoded posting
/// block (owned `Vec`s; text tf is STORED — Phase 2e-B). Both variants expose
/// the identical `(docids, tfs)` u32 streams in the SAME ascending-docid order,
/// so the BM25 score expression is fed bit-identical inputs on both paths.
pub(in crate::index) enum TokPostings<'a> {
    Live(&'a Postings),
    /// The candidate-only projection of the active posting (#4246). `df()` is
    /// the exact composed df, NOT the projection's length; `docids()`/`tfs()`
    /// and `tf(id)` are exact for every id the projection was built for and
    /// silent for every other id, which the callers never ask about.
    Sparse(std::sync::Arc<SparsePosting>),
    /// The cache-resident segment posting, shared by `Arc` (Phase 2m). The hot
    /// no-tombstone path holds the BOUNDED Text posting cache's `Arc` DIRECTLY —
    /// a per-candidate `match_doc_score` probe is then a `.tf(id)` binary-search
    /// over the shared streams with NO re-decode and NO per-call vector copy (the
    /// `filtered_search` 25x disk fix). Only built when a segment is attached.
    Segment(std::sync::Arc<(Vec<u32>, Vec<u32>)>),
    /// The active posting when a sealed base and live overlay both contribute
    /// to a token.  The two streams are merged in doc-id order, with the live
    /// overlay winning for a reused base id.
    Combined {
        docids: Vec<u32>,
        tfs: Vec<u32>,
    },
}

impl<'a> TokPostings<'a> {
    #[inline]
    pub(in crate::index) fn docids(&self) -> &[u32] {
        match self {
            TokPostings::Live(p) => &p.docids,
            TokPostings::Sparse(p) => &p.docids,
            TokPostings::Segment(p) => &p.0,
            TokPostings::Combined { docids, .. } => docids,
        }
    }
    #[inline]
    pub(in crate::index) fn tfs(&self) -> &[u32] {
        match self {
            TokPostings::Live(p) => &p.tfs,
            TokPostings::Sparse(p) => &p.tfs,
            TokPostings::Segment(p) => &p.1,
            TokPostings::Combined { tfs, .. } => tfs,
        }
    }
    #[inline]
    pub(super) fn df(&self) -> usize {
        match self {
            TokPostings::Sparse(p) => p.df,
            _ => self.docids().len(),
        }
    }
    /// tf of `id` via binary-search (docids ascending), or `None` — the same
    /// random-access probe `Postings::tf` does, on either source.
    #[inline]
    pub(super) fn tf(&self, id: u32) -> Option<u32> {
        match self {
            TokPostings::Live(p) => p.tf(id),
            TokPostings::Sparse(p) => p.docids.binary_search(&id).ok().map(|pos| p.tfs[pos]),
            TokPostings::Segment(p) => p.0.binary_search(&id).ok().map(|pos| p.1[pos]),
            TokPostings::Combined { docids, tfs } => {
                docids.binary_search(&id).ok().map(|pos| tfs[pos])
            }
        }
    }
}
