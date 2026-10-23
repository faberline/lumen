//! Replacing layers after a merge or compaction: preparing the replacement's
//! row maps and coverage off the publication path, then installing it by
//! checking identities and moving layer Arcs only.

use crate::persistence::infrastructure::composed_segment::{
    ComposedSegmentReader, DeltaLayer, PreparedScalarReplacement,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Build a compact private row space from immutable layers in age order.
/// Deleted rows remain in the row map, so partial merges cannot reveal old data.
pub(crate) fn compose_checkpoint_layers(
    layers: Vec<(Arc<SegmentReader>, Vec<String>)>,
) -> Result<(ComposedSegmentReader, Vec<String>)> {
    if layers.is_empty() {
        bail!("checkpoint compaction needs at least one layer");
    }
    let mut external = BTreeSet::new();
    for (reader, ids) in &layers {
        if reader.n_docs() as usize != ids.len() {
            bail!("checkpoint row map does not match segment row count");
        }
        let mut seen = BTreeSet::new();
        for eid in ids {
            if !seen.insert(eid) {
                bail!("checkpoint row map contains duplicate external ID");
            }
            external.insert(eid.clone());
        }
    }
    let output_ids: Vec<String> = external.into_iter().collect();
    u32::try_from(output_ids.len()).map_err(|_| anyhow!("compaction row space exceeds u32"))?;
    let dense: BTreeMap<&str, u32> = output_ids
        .iter()
        .enumerate()
        .map(|(id, eid)| (eid.as_str(), id as u32))
        .collect();
    let mut layers = layers.into_iter().map(|(reader, ids)| {
        let mapped = ids.iter().map(|eid| dense[eid.as_str()]).collect();
        (reader, mapped)
    });
    let (reader, ids) = layers.next().expect("checked nonempty");
    let mut view = ComposedSegmentReader::from_mapped_base(reader, ids)?;
    for (reader, ids) in layers {
        view = view.with_delta(reader, ids)?;
    }
    Ok((view, output_ids))
}

impl ComposedSegmentReader {
    /// Only the layer-Arc vector is copied when a private delta is appended.
    /// Its new row map is priced separately from the selected source rows.
    pub(crate) fn private_append_metadata_bound(&self) -> Option<usize> {
        self.layers
            .len()
            .checked_add(1)?
            .checked_mul(2 * std::mem::size_of::<Arc<DeltaLayer>>())?
            .checked_add(std::mem::size_of::<Self>())
    }

    pub(crate) fn prepare_replacement(
        &self,
        base: Option<&Arc<SegmentReader>>,
        inputs: &[Arc<SegmentReader>],
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<PreparedScalarReplacement> {
        if !self.has_catalog_base {
            bail!("compaction requires a published catalog base");
        }
        let prepared = if let Some(base) = base {
            self.replace_base_inputs(base, inputs, reader.clone(), ids)?
        } else {
            self.replace_delta_inputs(inputs, reader.clone(), ids)?
        };
        let replacement = if base.is_some() {
            prepared
                .base_map
                .as_ref()
                .expect("mapped replacement base")
                .clone()
        } else {
            prepared
                .layers
                .iter()
                .find(|layer| Arc::ptr_eq(&layer.reader, &reader))
                .ok_or_else(|| anyhow!("prepared replacement layer is absent"))?
                .clone()
        };
        Ok(PreparedScalarReplacement {
            base: self.base.clone(),
            inputs: inputs.to_vec(),
            replacement,
            includes_base: base.is_some(),
            n_docs: prepared.n_docs,
            has_catalog_base: prepared.has_catalog_base,
            catalog_len: self
                .layers
                .iter()
                .take_while(|layer| !layer.private)
                .count(),
        })
    }

    pub(crate) fn install_prepared_replacement(
        &self,
        prepared: &PreparedScalarReplacement,
    ) -> Result<Self> {
        if !self.has_catalog_base
            || !prepared.has_catalog_base
            || !Arc::ptr_eq(&self.base, &prepared.base)
        {
            bail!("compacted base input no longer matches live base");
        }
        let matches = |window: &[Arc<DeltaLayer>]| {
            window.len() == prepared.inputs.len()
                && window
                    .iter()
                    .zip(&prepared.inputs)
                    .all(|(layer, input)| Arc::ptr_eq(&layer.reader, input))
        };
        if prepared.inputs.len() > prepared.catalog_len {
            bail!("prepared compaction includes private layers");
        }
        let n_docs = self.n_docs.max(prepared.n_docs);
        let live_catalog_len = self
            .layers
            .iter()
            .take_while(|layer| !layer.private)
            .count();
        if prepared.includes_base {
            if prepared.inputs.len() > live_catalog_len
                || !matches(&self.layers[..prepared.inputs.len()])
            {
                bail!("compacted base inputs no longer match live layers");
            }
            let layers = self.layers[prepared.inputs.len()..].to_vec();
            let distinct_terms = Self::refold_distinct_terms(
                &prepared.replacement.reader,
                Some(prepared.replacement.clone()),
                &layers,
                n_docs,
            );
            return Ok(Self {
                base: prepared.replacement.reader.clone(),
                base_map: Some(prepared.replacement.clone()),
                layers,
                n_docs,
                has_catalog_base: true,
                distinct_terms,
                query_cache: Arc::default(),
            });
        }
        if prepared.inputs.is_empty() {
            bail!("empty compaction input identity");
        }
        if prepared.inputs.len() > live_catalog_len {
            bail!("compaction inputs no longer fit catalog prefix");
        }
        let start = self
            .layers
            .windows(prepared.inputs.len())
            .take(live_catalog_len - prepared.inputs.len() + 1)
            .position(matches)
            .ok_or_else(|| anyhow!("compaction inputs no longer match live layers"))?;
        let mut layers = self.layers.clone();
        layers.splice(
            start..start + prepared.inputs.len(),
            [prepared.replacement.clone()],
        );
        Ok(Self {
            base: self.base.clone(),
            base_map: self.base_map.clone(),
            layers,
            n_docs,
            has_catalog_base: true,
            // A window merge below the base holds the invariant: while it was
            // additive no term-bearing row was ever hidden, so merging those
            // layers drops no row and the live term set is unchanged (#4246).
            distinct_terms: self.distinct_terms,
            query_cache: Arc::default(),
        })
    }

    pub(crate) fn immutable_base_reader(&self) -> Arc<SegmentReader> {
        self.base.clone()
    }

    pub(crate) fn replace_base_inputs(
        &self,
        base: &Arc<SegmentReader>,
        deltas: &[Arc<SegmentReader>],
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<Self> {
        let catalog_len = self
            .layers
            .iter()
            .take_while(|layer| !layer.private)
            .count();
        if !self.has_catalog_base
            || !Arc::ptr_eq(base, &self.base)
            || deltas.len() > catalog_len
            || !self
                .layers
                .iter()
                .zip(deltas)
                .all(|(layer, input)| Arc::ptr_eq(&layer.reader, input))
        {
            bail!("compacted base inputs no longer match live layers");
        }
        let mut coverage = self
            .base_map
            .as_ref()
            .map(|map| map.coverage.clone())
            .unwrap_or_else(|| (0..self.base.n_docs()).collect());
        for layer in &self.layers[..deltas.len()] {
            coverage |= &layer.coverage;
        }
        let mut replacement = Self::from_mapped_base(reader, ids)?;
        if replacement.base_map.as_ref().unwrap().coverage != coverage {
            bail!("compacted base row coverage differs from captured inputs");
        }
        replacement.layers = self.layers[deltas.len()..].to_vec();
        replacement.n_docs = self.n_docs.max(replacement.n_docs);
        replacement.distinct_terms = Self::refold_distinct_terms(
            &replacement.base,
            replacement.base_map.clone(),
            &replacement.layers,
            replacement.n_docs,
        );
        Ok(replacement)
    }

    pub(crate) fn delta_readers(&self) -> Vec<Arc<SegmentReader>> {
        self.layers
            .iter()
            .filter(|layer| !layer.private)
            .map(|layer| layer.reader.clone())
            .collect()
    }

    pub(crate) fn replace_delta_inputs(
        &self,
        inputs: &[Arc<SegmentReader>],
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<Self> {
        if inputs.is_empty() {
            bail!("empty compaction input identity");
        }
        let catalog_len = self
            .layers
            .iter()
            .take_while(|layer| !layer.private)
            .count();
        if !self.has_catalog_base || inputs.len() > catalog_len {
            bail!("compaction inputs include private layers or lack a catalog base");
        }
        let start = self
            .layers
            .windows(inputs.len())
            .take(catalog_len - inputs.len() + 1)
            .position(|window| {
                window
                    .iter()
                    .zip(inputs)
                    .all(|(layer, input)| Arc::ptr_eq(&layer.reader, input))
            })
            .ok_or_else(|| anyhow!("compaction inputs no longer match live layers"))?;
        self.replace_delta_range(start, inputs.len(), reader, ids)
    }

    /// Replace an exact, adjacent live delta window after its immutable
    /// compacted reader has been durably published. The replacement must cover
    /// precisely the same external IDs, including deleted rows, so later
    /// layers keep their precedence unchanged.
    pub(crate) fn replace_delta_range(
        &self,
        start: usize,
        count: usize,
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<Self> {
        let end = start
            .checked_add(count)
            .ok_or_else(|| anyhow!("delta replacement range overflow"))?;
        let catalog_len = self
            .layers
            .iter()
            .take_while(|layer| !layer.private)
            .count();
        if !self.has_catalog_base || count == 0 || end > catalog_len {
            bail!("delta replacement range is outside live layers");
        }
        let replacement = Arc::new(DeltaLayer::new(reader, ids)?);
        let mut coverage = RoaringBitmap::new();
        for layer in &self.layers[start..end] {
            coverage |= &layer.coverage;
        }
        if coverage != replacement.coverage {
            bail!("delta replacement coverage differs from live range");
        }
        let mut layers = self.layers.clone();
        layers.splice(start..end, [replacement]);
        Ok(Self {
            base: self.base.clone(),
            base_map: self.base_map.clone(),
            layers,
            n_docs: self.n_docs,
            has_catalog_base: self.has_catalog_base,
            // A compacted window can drop only a row a newer layer in the window
            // hid — and while the count was known no term-bearing row was ever
            // hidden, so the merge changes no live term. `None` stays `None`.
            distinct_terms: self.distinct_terms,
            query_cache: Arc::default(),
        })
    }
}
