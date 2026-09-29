//! A document's delete from one field: the live overlay goes, and a sealed base
//! doc is masked by the field's tombstones until the next seal bakes the delete
//! in.

use crate::index::domain::field_index::FieldIndex;
#[cfg(test)]
use crate::storage::DROP_EID_CALLS;

impl FieldIndex {
    /// Remove every posting written by doc-id `id` (external_id `eid`, needed
    /// only for the String-keyed vector backend) and return the number of
    /// bytes freed (approximate, used to keep `bytes` honest).
    pub(crate) fn drop_eid(&mut self, id: u32, eid: &str) -> u64 {
        #[cfg(test)]
        DROP_EID_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
        match self {
            FieldIndex::Text { idx, .. } => {
                idx.clear_match_rank_cache();
                // A live overlay is the current value for this field. Remove it
                // before looking at the sealed base, so a reused base id does not
                // create a new tombstone or remove the base twice.
                if let Some(freed) = idx.drop_live_overlay(id, eid) {
                    return freed;
                }
                // QUERY-TIME TOMBSTONE (Phase 2h-4): when the field is SEALED and
                // `id` is a base docid `< seg.n_docs`, its postings live on the
                // IMMUTABLE on-disk blocks (the RAM `tokens` AND the corpus-deriving
                // `distinct` were dropped at seal), so neither `tokens.get_mut` nor
                // `distinct.remove` can reach them. Record the id in `tombstones`
                // (the BM25 scan subtracts it from every token posting BEFORE `df`
                // and scoring), and DECREMENT the LIVE corpus scalars by the doc's
                // length read from the segment DocLen column — so `doc_count`,
                // `total_doc_len`, and therefore `avgdl` track an in-RAM oracle that
                // physically removed the doc. The re-seal bakes the delete in
                // (`tokens_for_seal`'s `live(id)` gather) and clears the set.
                // Live overlays were removed above. Only an id with no current
                // overlay may reach this sealed-base tombstone path.
                if let Some(seg) = &idx.segment {
                    if id < seg.n_docs() {
                        // Tombstone and decrement the immutable base once. A
                        // current live overlay already returned above.
                        if !idx.tombstones.contains(id) && seg.text_is_present(id) {
                            let doc_len = seg.text_doc_len(id);
                            idx.tombstones.insert(id);
                            idx.doc_count = idx.doc_count.saturating_sub(1);
                            idx.total_doc_len = idx.total_doc_len.saturating_sub(doc_len as u64);
                            // The on-disk posting bytes don't shrink; report the
                            // same per-doc estimate as the in-RAM removal.
                            let freed = (doc_len as usize * (1 + eid.len())) as u64;
                            idx.bytes = idx.bytes.saturating_sub(freed);
                            return freed;
                        }
                    }
                }
                0
            }
            FieldIndex::Keyword(k) => {
                // A sealed ID can have a newer sparse or dense overlay. Remove
                // that overlay and its posting before masking the immutable base.
                let value = k.remove_keyword(id).or_else(|| {
                    k.segment
                        .as_ref()
                        .filter(|seg| id < seg.n_docs() && !k.tombstones.contains(id))
                        .and_then(|seg| seg.keyword_at(id))
                });
                if k.segment.as_ref().is_some_and(|seg| id < seg.n_docs()) {
                    k.tombstones.insert(id);
                }
                let Some(value) = value else {
                    return 0;
                };
                if let Some(posting) = k.terms.get_mut(&value) {
                    posting.remove(id);
                    if posting.len() < 2 {
                        k.dup_values.remove(&value);
                    }
                    if posting.is_empty() {
                        k.terms.remove(&value);
                    }
                }
                let freed = (value.len() + eid.len()) as u64;
                k.bytes = k.bytes.saturating_sub(freed);
                freed
            }
            FieldIndex::Number(n) => {
                let key = n.remove_number(id).or_else(|| n.number_at(id));
                if n.segment.as_ref().is_some_and(|seg| id < seg.n_docs()) {
                    n.tombstones.insert(id);
                }
                let Some(key) = key else {
                    return 0;
                };
                if let Some(posting) = n.values.get_mut(&key) {
                    posting.remove(id);
                    if posting.len() < 2 {
                        n.dup_values.remove(&key);
                    }
                    if posting.is_empty() {
                        n.values.remove(&key);
                    }
                }
                let freed = (8 + eid.len()) as u64;
                n.bytes = n.bytes.saturating_sub(freed);
                freed
            }
            FieldIndex::Set(s) => {
                let members = s.forward.remove(&id).or_else(|| s.set_members(id));
                if s.segment.as_ref().is_some_and(|seg| id < seg.n_docs()) {
                    s.tombstones.insert(id);
                }
                let Some(members) = members else {
                    return 0;
                };
                let mut freed = 0;
                for member in members {
                    if let Some(posting) = s.elements.get_mut(&member) {
                        posting.remove(id);
                        if posting.len() < 2 {
                            s.dup_values.remove(&member);
                        }
                        if posting.is_empty() {
                            s.elements.remove(&member);
                        }
                    }
                    freed += (member.len() + eid.len()) as u64;
                }
                s.bytes = s.bytes.saturating_sub(freed);
                freed
            }
            FieldIndex::Vector { spec, idx, bytes } => {
                // Trait-object remove always succeeds (Ok). We don't
                // track per-eid byte sizes precisely here — subtract a
                // proportional share of the dimension instead so
                // `stats` stays approximately honest.
                let approx = (spec.dim as u64) * 4 + eid.len() as u64;
                match idx.remove(eid) {
                    Ok(true) => {
                        *bytes = bytes.saturating_sub(approx);
                        approx
                    }
                    _ => 0,
                }
            }
            FieldIndex::Hash(h) => {
                let previous = h.forward.remove(&id).or_else(|| h.hash_at(id));
                if h.segment.as_ref().is_some_and(|seg| id < seg.n_docs()) {
                    h.tombstones.insert(id);
                }
                if previous.is_some() {
                    h.bytes = h.bytes.saturating_sub(12);
                    12
                } else {
                    0
                }
            }
        }
    }
}
