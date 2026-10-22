//! Building a view from a base and appending a delta layer, and the exact
//! distinct-term count: kept additively while a layer only adds terms and
//! refolded after a compaction.

use crate::persistence::infrastructure::composed_segment::{
    note_text_term_probes, ComposedSegmentReader, DeltaLayer,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;
use std::sync::Arc;

impl ComposedSegmentReader {
    pub(crate) fn from_base(base: Arc<SegmentReader>) -> Self {
        // A sealed base is written from a live projection, so every dictionary
        // entry it holds has at least one document: its dictionary size IS the
        // composed distinct-term count, read from the column header (#4246).
        let distinct_terms = base
            .has_text_postings()
            .then(|| base.keyword_ordinal_count().map(u64::from))
            .flatten();
        Self {
            n_docs: base.n_docs(),
            base,
            base_map: None,
            layers: Vec::new(),
            has_catalog_base: true,
            distinct_terms,
            query_cache: Arc::default(),
        }
    }

    pub(crate) fn with_delta(&self, reader: Arc<SegmentReader>, ids: Vec<u32>) -> Result<Self> {
        if !self.has_catalog_base || self.layers.iter().any(|layer| layer.private) {
            bail!("cannot append catalog delta without its published prefix");
        }
        self.can_append_incremental()?;
        let layer = Arc::new(DeltaLayer::new(reader, ids)?);
        let n_docs = layer.ids.iter().try_fold(self.n_docs, |n_docs, &global| {
            let candidate = global
                .checked_add(1)
                .ok_or_else(|| anyhow!("delta runtime ID overflow"))?;
            Ok::<u32, anyhow::Error>(n_docs.max(candidate))
        })?;
        let distinct_terms = self.additive_distinct_terms(&layer);
        let mut layers = self.layers.clone();
        layers.push(layer);
        Ok(Self {
            base: self.base.clone(),
            base_map: self.base_map.clone(),
            layers,
            n_docs,
            has_catalog_base: self.has_catalog_base,
            distinct_terms,
            query_cache: Arc::default(),
        })
    }

    /// The composed distinct-term count after appending `layer`, or `None` when
    /// this attach is not additive and the invariant on `distinct_terms` would
    /// break.
    ///
    /// The attach is additive only when no covered runtime ID already holds a
    /// Text value here: an insert can never empty an older posting, while an
    /// update or a delete can. Both loops are bounded by the layer — the
    /// covered IDs and the delta dictionary — never by the base dictionary, so
    /// the cost is one checkpoint interval of writes paid once at publication.
    fn additive_distinct_terms(&self, layer: &DeltaLayer) -> Option<u64> {
        let known = self.distinct_terms?;
        if layer.ids.iter().any(|&id| self.text_is_present(id)) {
            return None;
        }
        let added = layer.reader.keyword_ordinal_count()?;
        let mut fresh = 0u64;
        for ordinal in 0..added {
            let term = layer.reader.keyword_term_at_ordinal_cow(ordinal)?;
            if !self.has_dictionary_term(&term) {
                fresh += 1;
            }
        }
        known.checked_add(fresh)
    }

    /// Whether any composed layer's dictionary holds `term`. One binary search
    /// per layer and no posting decode.
    pub(crate) fn has_dictionary_term(&self, term: &str) -> bool {
        self.base.text_has_term(term)
            || self
                .layers
                .iter()
                .any(|layer| layer.reader.text_has_term(term))
    }

    /// The exact number of dictionary terms with a non-empty composed posting,
    /// when it is known without walking the dictionary. See `distinct_terms`.
    pub(crate) fn known_distinct_terms(&self) -> Option<u64> {
        self.distinct_terms
    }

    /// Re-derive `distinct_terms` after a compaction folded the base in
    /// (#4246). The merged base is a projection of composed postings
    /// (`segment_rdb::read_delta_values`), so its dictionary size IS its live
    /// term count; each remaining layer is then re-folded by the same additive
    /// rule `with_delta` applies, which re-checks the invariant per layer and
    /// yields `None` the moment a layer hides a term-bearing row. Cost is
    /// bounded by the remaining layers, never by the base — and a full merge
    /// is how a reader voided by an update gets its O(1) count back.
    pub(super) fn refold_distinct_terms(
        base: &Arc<SegmentReader>,
        base_map: Option<Arc<DeltaLayer>>,
        layers: &[Arc<DeltaLayer>],
        n_docs: u32,
    ) -> Option<u64> {
        let mut view = Self {
            base: base.clone(),
            base_map,
            layers: Vec::with_capacity(layers.len()),
            n_docs,
            has_catalog_base: true,
            distinct_terms: base
                .has_text_postings()
                .then(|| base.keyword_ordinal_count().map(u64::from))
                .flatten(),
            query_cache: Arc::default(),
        };
        for layer in layers {
            view.distinct_terms = view.additive_distinct_terms(layer);
            view.distinct_terms?;
            view.layers.push(layer.clone());
        }
        view.distinct_terms
    }

    /// Source `0` is the base; source `k` is `layers[k - 1]` — the order
    /// [`super::term_cursor::StringTermCursor`] reports dictionary ordinals in.
    fn source_reader(&self, source: usize) -> &SegmentReader {
        match source.checked_sub(1) {
            None => self.base.as_ref(),
            Some(layer) => self.layers[layer].reader.as_ref(),
        }
    }

    /// Whether `source`'s local row `local` is still the live composed row:
    /// mapped to its runtime ID, not covered by a NEWER layer, and not in
    /// `dead` (the index's pending tombstones). `None` for a row map that does
    /// not know `local` — a torn layer, reported rather than counted.
    fn source_row_live(&self, source: usize, local: u32, dead: &RoaringBitmap) -> Option<bool> {
        let (global, newer) = match source.checked_sub(1) {
            None => {
                let global = match &self.base_map {
                    Some(map) => *map.ids.get(local as usize)?,
                    None => local,
                };
                (global, &self.layers[..])
            }
            Some(layer) => (
                *self.layers[layer].ids.get(local as usize)?,
                &self.layers[layer + 1..],
            ),
        };
        Some(!dead.contains(global) && !newer.iter().any(|layer| layer.coverage.contains(global)))
    }

    /// Whether the posting at `source`'s dictionary ordinal `dict_id` has a
    /// live composed docid, decoded only that far. `None` on a torn block or
    /// row map.
    fn source_posting_live(
        &self,
        source: usize,
        dict_id: u32,
        dead: &RoaringBitmap,
    ) -> Option<bool> {
        let mut torn = false;
        let any = self
            .source_reader(source)
            .text_posting_any_at(dict_id, |local| {
                self.source_row_live(source, local, dead)
                    .unwrap_or_else(|| {
                        torn = true;
                        true
                    })
            })?;
        (!torn).then_some(any)
    }

    /// Whether `token` still has a live composed document under `dead`
    /// (#4246). The newest layer holding it is probed first — an insert lands
    /// there and nothing can cover it — and each posting is decoded only as
    /// far as its first live docid; no posting is materialized or cached.
    /// `Some(false)` for an absent token, `None` for a torn dictionary,
    /// posting block, or row map.
    pub(crate) fn text_term_has_live_doc(&self, token: &str, dead: &RoaringBitmap) -> Option<bool> {
        note_text_term_probes(1);
        for source in (0..=self.layers.len()).rev() {
            let Some(dict_id) = self.source_reader(source).text_dict_id(token) else {
                continue;
            };
            if self.source_posting_live(source, dict_id, dead)? {
                return Some(true);
            }
        }
        Some(false)
    }

    /// The exact number of dictionary terms with a live composed document
    /// under `dead` — what `distinct_terms` carries when it is known, computed
    /// here for a reader with pending tombstones or a hidden row (#4246).
    ///
    /// ONE merged dictionary walk. Per term the cursor already names the
    /// layers holding it and their ordinals, so there is no re-lookup; the
    /// newest of them is probed first and each posting is decoded only as far
    /// as its first live docid. An insert-mostly corpus therefore costs one
    /// varint decode per term with no per-term allocation beyond the cursor,
    /// where the materializing walk it replaces composed and copied every
    /// posting (43 µs per term, 9 s at 200k documents). `None` on a torn
    /// dictionary, posting block, or row map; a partial count is never
    /// reported.
    pub(crate) fn live_text_term_count(&self, dead: &RoaringBitmap) -> Option<u64> {
        let mut cursor = self.string_terms(false).ok()?;
        let mut count = 0u64;
        loop {
            let Some((_term, ordinals)) = cursor.next_entry_cow(true).ok()? else {
                break;
            };
            note_text_term_probes(1);
            for &(source, dict_id) in ordinals.iter().rev() {
                if self.source_posting_live(source, dict_id, dead)? {
                    count += 1;
                    break;
                }
            }
        }
        Some(count)
    }
}
