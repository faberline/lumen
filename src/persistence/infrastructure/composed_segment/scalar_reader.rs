//! Point reads and scalar postings: the newest layer covering a row wins, and
//! postings translate each layer's local rows to runtime IDs.

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::SegmentReader;
use anyhow::Result;
use roaring::RoaringBitmap;
use std::borrow::Cow;
use std::sync::Arc;

impl ComposedSegmentReader {
    /// A compacted field may use its own dense local row space. The map
    /// keeps it independent of collection ID allocation and other fields.
    pub(crate) fn from_mapped_base(reader: Arc<SegmentReader>, ids: Vec<u32>) -> Result<Self> {
        let base_terms = reader
            .has_text_postings()
            .then(|| reader.keyword_ordinal_count().map(u64::from))
            .flatten();
        let mut view = Self::from_base(reader.clone()).with_delta(reader, ids)?;
        view.base_map = view.layers.pop();
        // The mapping layer IS the base reader: the dictionary is unchanged, so
        // the base's own term count still holds.
        view.distinct_terms = base_terms;
        Ok(view)
    }

    pub(super) fn dense_base_only(&self) -> bool {
        self.base_map.is_none() && self.layers.is_empty()
    }

    pub(crate) fn base_reader(&self) -> Option<&Arc<SegmentReader>> {
        self.dense_base_only().then_some(&self.base)
    }
    pub(crate) fn n_docs(&self) -> u32 {
        self.n_docs
    }
    /// Sparse IDs covered by the newest appended delta, including deletions.
    pub(crate) fn covered_ids(&self) -> &[u32] {
        self.layers.last().map_or(&[], |layer| layer.ids.as_slice())
    }
    pub(crate) fn applied_seq(&self) -> u64 {
        self.layers.last().map_or_else(
            || self.base.applied_seq(),
            |layer| layer.reader.applied_seq(),
        )
    }
    fn winner(&self, id: u32) -> (&SegmentReader, u32) {
        for layer in self.layers.iter().rev() {
            if let Some(&local) = layer.local_by_global.get(&id) {
                return (&layer.reader, local);
            }
        }
        let local = self.base_map.as_ref().map_or(id, |map| {
            map.local_by_global.get(&id).copied().unwrap_or(u32::MAX)
        });
        (&self.base, local)
    }

    /// After checkpoint publication, private winners are precisely the layers
    /// applied after its cut. They already hide the older catalog row.
    pub(crate) fn has_private_winner(&self, id: u32) -> bool {
        self.layers
            .iter()
            .rev()
            .find(|layer| layer.coverage.contains(id))
            .is_some_and(|layer| layer.private)
    }
    pub(crate) fn vector_at(&self, id: u32, dim: usize) -> Option<&[f32]> {
        let (reader, row) = self.winner(id);
        reader.vector_at(row, dim)
    }
    pub(crate) fn keyword_at(&self, id: u32) -> Option<String> {
        let (r, i) = self.winner(id);
        r.keyword_at(i)
    }
    /// Internal scalar projection view. Raw dictionary values borrow their
    /// source mmap; legacy LZ4 values carry the selected owned fallback.
    pub(crate) fn keyword_at_cow(&self, id: u32) -> Option<Cow<'_, str>> {
        let (reader, local) = self.winner(id);
        reader.keyword_at_cow(local)
    }
    pub(crate) fn number_at(&self, id: u32) -> Option<f64> {
        let (r, i) = self.winner(id);
        r.number_at(i)
    }
    pub(crate) fn set_at(&self, id: u32) -> Option<Vec<String>> {
        let (r, i) = self.winner(id);
        r.set_at(i)
    }
    /// `(present, count)` without constructing a row of owned strings.
    pub(crate) fn set_row_member_count(&self, id: u32) -> Option<(bool, u32)> {
        let (reader, local) = self.winner(id);
        reader.set_row_member_count(local)
    }
    /// Internal scalar projection member view. The caller must consume this
    /// Cow before advancing to the next member.
    pub(crate) fn set_member_at_cow(&self, id: u32, member: u32) -> Option<Cow<'_, str>> {
        let (reader, local) = self.winner(id);
        reader.set_member_at_cow(local, member)
    }
    pub(crate) fn hash_at(&self, id: u32) -> Option<u64> {
        let (r, i) = self.winner(id);
        r.hash_at(i)
    }
    pub(crate) fn text_is_present(&self, id: u32) -> bool {
        let (r, i) = self.winner(id);
        r.text_is_present(i)
    }
    pub(crate) fn text_doc_len(&self, id: u32) -> u32 {
        let (r, i) = self.winner(id);
        r.text_doc_len(i)
    }
    /// Every row's text doc length by global id, for hot BM25 walks that
    /// would otherwise resolve `winner(id)` (one `BTreeMap` probe per layer)
    /// for every scored docid. A bare dense base borrows its column; any
    /// other composition materializes the winners once per composition into
    /// `query_cache` (O(n_docs + Σ layer rows), no per-row map probe) and
    /// returns exactly what `text_doc_len(id)` returns for every
    /// `id < n_docs()` (#4246).
    pub(crate) fn text_doc_lens(&self) -> Option<&[u32]> {
        if let Some(base) = self.base_reader() {
            return base.text_doc_lens();
        }
        Some(
            self.query_cache
                .text_doc_lens
                .get_or_init(|| self.materialize_text_doc_lens()),
        )
    }

    fn materialize_text_doc_lens(&self) -> Vec<u32> {
        let mut lens = vec![0u32; self.n_docs as usize];
        match &self.base_map {
            None => {
                if let Some(column) = self.base.text_doc_lens() {
                    let n = column.len().min(lens.len());
                    lens[..n].copy_from_slice(&column[..n]);
                }
            }
            Some(map) => {
                for (local, &global) in map.ids.iter().enumerate() {
                    if let Some(slot) = lens.get_mut(global as usize) {
                        *slot = self.base.text_doc_len(local as u32);
                    }
                }
            }
        }
        // Applied in order so the newest layer holding a row wins, exactly
        // as `winner` walks the layers newest-first.
        for layer in &self.layers {
            for (local, &global) in layer.ids.iter().enumerate() {
                if let Some(slot) = lens.get_mut(global as usize) {
                    *slot = layer.reader.text_doc_len(local as u32);
                }
            }
        }
        lens
    }

    fn postings(
        &self,
        read: impl Fn(&SegmentReader) -> Option<RoaringBitmap>,
    ) -> Option<RoaringBitmap> {
        let local = read(&self.base).unwrap_or_default();
        let mut out = if let Some(map) = &self.base_map {
            let mut global = RoaringBitmap::new();
            for id in local {
                global.insert(*map.ids.get(id as usize)?);
            }
            global
        } else {
            local
        };
        for layer in &self.layers {
            out -= &layer.coverage;
            if let Some(local) = read(&layer.reader) {
                for id in local {
                    out.insert(*layer.ids.get(id as usize)?);
                }
            }
        }
        Some(out)
    }
    pub(crate) fn keyword_postings(&self, value: &str) -> Option<RoaringBitmap> {
        if self.dense_base_only() {
            return self.base.keyword_postings(value);
        }
        self.postings(|reader| reader.keyword_postings(value))
            .filter(|p| !p.is_empty())
    }
    pub(crate) fn keyword_df(&self, value: &str) -> Option<u64> {
        if self.dense_base_only() {
            return self.base.keyword_df(value);
        }
        self.keyword_postings(value).map(|p| p.len())
    }
    pub(crate) fn set_postings(&self, value: &str) -> Option<RoaringBitmap> {
        if self.dense_base_only() {
            return self.base.set_postings(value);
        }
        self.postings(|reader| reader.set_postings(value))
            .filter(|p| !p.is_empty())
    }
    pub(crate) fn set_df(&self, value: &str) -> Option<u64> {
        if self.dense_base_only() {
            return self.base.set_df(value);
        }
        self.set_postings(value).map(|p| p.len())
    }
    pub(crate) fn number_value_postings(&self, bits: u64) -> Option<RoaringBitmap> {
        if self.dense_base_only() {
            return self.base.number_value_postings(bits);
        }
        self.postings(|reader| reader.number_value_postings(bits))
            .filter(|p| !p.is_empty())
    }
    pub(crate) fn number_value_df(&self, bits: u64) -> Option<u64> {
        if self.dense_base_only() {
            return self.base.number_value_df(bits);
        }
        self.number_value_postings(bits).map(|p| p.len())
    }
    pub(crate) fn number_range(
        &self,
        low: Option<(u64, bool)>,
        high: Option<(u64, bool)>,
    ) -> Option<RoaringBitmap> {
        if self.dense_base_only() {
            return self.base.number_range(low, high);
        }
        self.postings(|reader| reader.number_range(low, high))
    }
    pub(crate) fn number_range_df(
        &self,
        low: Option<(u64, bool)>,
        high: Option<(u64, bool)>,
    ) -> Option<u64> {
        if self.dense_base_only() {
            return self.base.number_range_df(low, high);
        }
        self.number_range(low, high).map(|p| p.len())
    }
    pub(crate) fn number_range_distinct_count(
        &self,
        low: Option<(u64, bool)>,
        high: Option<(u64, bool)>,
    ) -> Option<u64> {
        if self.dense_base_only() {
            return self.base.number_range_distinct_count(low, high);
        }
        let mut keys = self.number_keys(low, high, false).ok()?;
        let mut count = 0;
        while let Some(key) = keys.next().ok()? {
            if self
                .number_value_postings(key)
                .is_some_and(|p| !p.is_empty())
            {
                count += 1;
            }
        }
        Some(count)
    }
}
