//! Vector lookups: one doc's vector and the whole column as a slice.

use crate::persistence::infrastructure::segment::{SegmentReader, ROLE_VECTOR};

impl SegmentReader {
    /// A zero-copy borrow of doc `id`'s `dim`-long vector straight off the
    /// mmap, or `None` if `id` is out of range, the doc is absent, the column
    /// is torn/misaligned, or the per-doc window overruns the column. The
    /// returned slice borrows `&self` (it lives on the page), so a flat kNN scan
    /// reads the corpus with no heap copy. Never panics.
    pub fn vector_at(&self, id: u32, dim: usize) -> Option<&[f32]> {
        if dim == 0 || !self.is_present(id) {
            return None;
        }
        let all = self.vectors_slice(dim)?;
        let start = (id as usize).checked_mul(dim)?;
        let end = start.checked_add(dim)?;
        all.get(start..end)
    }

    /// The whole vector forward column as one contiguous `f32[n_docs * dim]`
    /// slice (zero-copy), or `None` if the column is torn/misaligned or its
    /// element count disagrees with `n_docs * dim`. The slice borrows `&self`.
    pub fn vectors_slice(&self, dim: usize) -> Option<&[f32]> {
        if dim == 0 {
            return None;
        }
        let vector = self.column(ROLE_VECTOR)?;
        let vector_bytes = self.column_bytes(vector)?;
        let floats: &[f32] = bytemuck::try_cast_slice(vector_bytes).ok()?;
        // Guard against a directory that disagrees with the header geometry.
        let expect = (self.n_docs as usize).checked_mul(dim)?;
        if floats.len() < expect {
            return None;
        }
        Some(&floats[..expect])
    }
}
