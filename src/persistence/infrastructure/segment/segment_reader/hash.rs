//! Hash lookups.

use crate::persistence::infrastructure::segment::{SegmentReader, ROLE_HASH};

impl SegmentReader {
    /// The raw `u64` hash for doc `id`, or `None` if `id` is out of range, the
    /// doc is absent, or the column is torn/misaligned. Never panics. The hash
    /// is stored directly (no transform), so the returned value is bit-equal to
    /// the live forward entry.
    pub fn hash_at(&self, id: u32) -> Option<u64> {
        if !self.is_present(id) {
            return None;
        }
        let hash = self.column(ROLE_HASH)?;
        let hash_bytes = self.column_bytes(hash)?;
        let hash_words: &[u64] = bytemuck::try_cast_slice(hash_bytes).ok()?;
        hash_words.get(id as usize).copied()
    }
}
