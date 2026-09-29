//! External-ID lookups.

use crate::persistence::infrastructure::segment::{SegmentReader, ROLE_EID};

impl SegmentReader {
    // -----------------------------------------------------------------------
    // Collection EID column (Phase 2f-1)
    // -----------------------------------------------------------------------

    /// The external_id string for docid `id`, read from the [`ROLE_EID`]
    /// by-position dictionary column, or `None` if `id` is out of range, the
    /// column is absent (not an eid meta segment), or its block is torn /
    /// non-UTF-8 — never panics. The string is owned (it lives in the
    /// moka-cached decompressed block, not on the mmap page; see
    /// [`Self::keyword_at`] for why var-column reads can't borrow `&self`).
    pub fn eid_at(&self, id: u32) -> Option<String> {
        let (block, within) = self.dict_block_at(ROLE_EID, id)?;
        let bytes = block.get(within)?;
        String::from_utf8(bytes.clone()).ok()
    }

    /// The logical entry count of the [`ROLE_EID`] column — the number of
    /// external_ids stored, i.e. the collection's dense docid count. 0 if this
    /// is not an eid meta segment.
    pub fn eid_count(&self) -> u32 {
        self.column(ROLE_EID)
            .and_then(|c| u32::try_from(c.elem_count).ok())
            .unwrap_or(0)
    }

    /// Materialize every external_id in dense docid order `[0..eid_count)`.
    /// `None` if any entry is torn (a partial interner would be a wrong
    /// rebuild, so the whole reopen aborts rather than silently dropping a
    /// doc). Used by `Collection::open_from_segments` to rebuild the
    /// `Interner`.
    pub fn eids_all(&self) -> Option<Vec<String>> {
        let n = self.eid_count();
        let mut out = Vec::with_capacity(n as usize);
        for id in 0..n {
            out.push(self.eid_at(id)?);
        }
        Some(out)
    }
}
