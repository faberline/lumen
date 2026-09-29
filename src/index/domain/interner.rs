//! A collection's external-id interner: the dense u32 doc id each external id
//! maps to, with retirement that compacts a hash-collision bucket.

use std::hash::{Hash, Hasher};

use rustc_hash::FxHasher;

use crate::index::domain::fast_hash::FastHashMap;

/// Per-collection external_id ↔ dense u32 doc-id map. Posting lists and the
/// query path carry the `u32` (cheap to copy/hash, no per-match String clone);
/// the `String` external_id is resolved only at the API boundary. Append-only:
/// re-indexing an eid returns the same id; deletes do not reuse ids (they
/// compact on the next snapshot round-trip).
#[derive(Debug, Default)]
pub(crate) struct Interner {
    pub(crate) to_hash: FastHashMap<u64, InternerBucket>,
    pub(crate) to_eid: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum InternerBucket {
    One(u32),
    Many(Vec<u32>),
}

impl Interner {
    pub(crate) fn intern(&mut self, eid: &str) -> u32 {
        self.intern_with_status(eid).0
    }

    fn intern_with_status(&mut self, eid: &str) -> (u32, bool) {
        let hash = hash_external_id(eid);
        if let Some(id) = self.id_with_hash(eid, hash) {
            return (id, false);
        }
        let id = self.to_eid.len() as u32;
        self.to_eid.push(eid.to_string());
        self.insert_hash(hash, id);
        (id, true)
    }

    pub(crate) fn intern_owned_with_status(&mut self, eid: String) -> (u32, bool) {
        let hash = hash_external_id(&eid);
        if let Some(id) = self.id_with_hash(&eid, hash) {
            return (id, false);
        }
        let id = self.to_eid.len() as u32;
        self.to_eid.push(eid);
        self.insert_hash(hash, id);
        (id, true)
    }

    fn insert_hash(&mut self, hash: u64, id: u32) {
        match self.to_hash.get_mut(&hash) {
            Some(InternerBucket::One(existing)) => {
                let existing = *existing;
                self.to_hash
                    .insert(hash, InternerBucket::Many(vec![existing, id]));
            }
            Some(InternerBucket::Many(ids)) => ids.push(id),
            None => {
                self.to_hash.insert(hash, InternerBucket::One(id));
            }
        }
    }

    pub(crate) fn id(&self, eid: &str) -> Option<u32> {
        self.id_with_hash(eid, hash_external_id(eid))
    }

    fn id_with_hash(&self, eid: &str, hash: u64) -> Option<u32> {
        match self.to_hash.get(&hash)? {
            InternerBucket::One(id) => (self.resolve(*id) == eid).then_some(*id),
            InternerBucket::Many(ids) => ids.iter().copied().find(|id| self.resolve(*id) == eid),
        }
    }

    pub(crate) fn resolve(&self, id: u32) -> &str {
        &self.to_eid[id as usize]
    }

    /// Remove the last interned id together with its lookup entry. Retired
    /// collections drain in reverse id order, so taking the vector tail does
    /// not shift any remaining id. The hash bucket is compacted at the same
    /// item boundary instead of leaving a collection-sized lookup map for the
    /// final destructor.
    pub(crate) fn take_last_for_retirement(&mut self) -> Option<(u32, String)> {
        let id = self.to_eid.len().checked_sub(1)? as u32;
        let external_id = self.to_eid.pop()?;
        self.remove_hash_id(hash_external_id(&external_id), id);
        Some((id, external_id))
    }

    fn remove_hash_id(&mut self, hash: u64, id: u32) {
        let mut remove_bucket = false;
        if let Some(bucket) = self.to_hash.get_mut(&hash) {
            match bucket {
                InternerBucket::One(existing) => {
                    debug_assert_eq!(*existing, id);
                    remove_bucket = true;
                }
                InternerBucket::Many(ids) => {
                    let position = ids
                        .iter()
                        .position(|candidate| *candidate == id)
                        .expect("retired id must remain in its interner hash bucket");
                    ids.swap_remove(position);
                    match ids.as_slice() {
                        [] => remove_bucket = true,
                        [remaining] => *bucket = InternerBucket::One(*remaining),
                        _ => {}
                    }
                }
            }
        } else {
            debug_assert!(false, "retired id must have an interner hash bucket");
        }
        if remove_bucket {
            self.to_hash.remove(&hash);
        }
    }
}

fn hash_external_id(eid: &str) -> u64 {
    let mut hasher = FxHasher::default();
    eid.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests;
