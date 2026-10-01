//! Checkpoint publication replaces only the captured private prefix.
//!
//! The cut pins the complete immutable view. Row maps and coverage checks run
//! in preparation. Publication checks identities and moves layer Arcs only.

use super::*;

#[derive(Clone, Debug)]
pub(crate) struct ScalarCheckpointCut {
    view: Option<Arc<ComposedSegmentReader>>,
    catalog_len: usize,
}

pub(crate) struct PreparedScalarPublication {
    cut: ScalarCheckpointCut,
    catalog: ComposedSegmentReader,
}

impl ComposedSegmentReader {
    /// Used only before the first catalog checkpoint. The empty base is an
    /// implementation detail and must not become a compaction input.
    pub(crate) fn from_private_empty_base(base: Arc<SegmentReader>) -> Result<Self> {
        if base.n_docs() != 0 {
            bail!("private initial base must have no rows");
        }
        let mut view = Self::from_base(base);
        view.has_catalog_base = false;
        Ok(view)
    }

    pub(crate) fn catalog_base_reader(&self) -> Option<Arc<SegmentReader>> {
        self.has_catalog_base.then(|| self.base.clone())
    }

    pub(crate) fn with_private_delta(
        &self,
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<Self> {
        self.can_append_incremental()?;
        let mut layer = DeltaLayer::new(reader, ids)?;
        layer.private = true;
        let mut result = self.clone();
        for &id in &layer.ids {
            result.n_docs = result.n_docs.max(
                id.checked_add(1)
                    .ok_or_else(|| anyhow!("private runtime ID overflow"))?,
            );
        }
        result.layers.push(Arc::new(layer));
        Ok(result)
    }

    /// The caller owns the capture barrier and already pins this field view.
    pub(crate) fn checkpoint_cut(self: &Arc<Self>) -> Result<ScalarCheckpointCut> {
        let catalog_len = self
            .layers
            .iter()
            .take_while(|layer| !layer.private)
            .count();
        if self.layers[catalog_len..]
            .iter()
            .any(|layer| !layer.private)
        {
            bail!("catalog layers must precede all private layers");
        }
        if !self.has_catalog_base && catalog_len != 0 {
            bail!("catalog delta has no published base");
        }
        Ok(ScalarCheckpointCut {
            view: Some(self.clone()),
            catalog_len,
        })
    }

    /// No row-map construction or file IO occurs here. The returned view and
    /// the preparation both retain the old files until outside the apply lease.
    pub(crate) fn install_checkpoint_publication(
        &self,
        prepared: &PreparedScalarPublication,
    ) -> Result<Self> {
        let Some(captured) = &prepared.cut.view else {
            if self.has_catalog_base
                || self.base.n_docs() != 0
                || self.base_map.is_some()
                || self.layers.iter().any(|layer| !layer.private)
            {
                bail!("empty checkpoint cut requires the initial private composition");
            }
            let mut result = prepared.catalog.clone();
            anyhow::ensure!(
                result.layers.len() + self.layers.len() <= MAX_INCREMENTAL_LAYERS,
                "checkpoint private suffix exceeds incremental layer cap"
            );
            result.layers.extend_from_slice(&self.layers);
            result.n_docs = result.n_docs.max(self.n_docs);
            return Ok(result);
        };
        if self.has_catalog_base != captured.has_catalog_base
            || !Arc::ptr_eq(&self.base, &captured.base)
            || !same_map(&self.base_map, &captured.base_map)
            || self.layers.len() < captured.layers.len()
            || !self
                .layers
                .iter()
                .zip(&captured.layers)
                .all(|(a, b)| Arc::ptr_eq(a, b))
            || self.layers[captured.layers.len()..]
                .iter()
                .any(|layer| !layer.private)
        {
            bail!("checkpoint captured scalar prefix no longer matches live view");
        }
        let mut result = prepared.catalog.clone();
        anyhow::ensure!(
            result.layers.len() + self.layers.len() - captured.layers.len()
                <= MAX_INCREMENTAL_LAYERS,
            "checkpoint private suffix exceeds incremental layer cap"
        );
        result
            .layers
            .extend_from_slice(&self.layers[captured.layers.len()..]);
        result.n_docs = result.n_docs.max(self.n_docs);
        Ok(result)
    }
}

fn same_map(a: &Option<Arc<DeltaLayer>>, b: &Option<Arc<DeltaLayer>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

impl PreparedScalarPublication {
    /// A detached catalog-only view for compaction preparation. It deliberately
    /// omits post-cut private layers, which only live publication may restore.
    pub(crate) fn catalog_view(&self) -> ComposedSegmentReader {
        self.catalog.clone()
    }

    /// Dry identity check used before CURRENT changes. It does not allocate row
    /// maps or publish anything.
    pub(crate) fn validate_live(&self, live: &ComposedSegmentReader) -> Result<()> {
        let _ = live.install_checkpoint_publication(self)?;
        Ok(())
    }

    pub(crate) fn install_without_composition(&self) -> Result<ComposedSegmentReader> {
        if self.cut.view.is_some() {
            bail!("checkpoint scalar composition disappeared after capture");
        }
        Ok(self.catalog.clone())
    }
}

impl ScalarCheckpointCut {
    /// Records that this field had only a mutable overlay at the cut. A later
    /// first private composition may be appended, but a catalog replacement is
    /// an identity change and cannot bind against this cut.
    pub(crate) fn empty() -> Self {
        Self {
            view: None,
            catalog_len: 0,
        }
    }
    /// The delta must cover every captured private row, even an absent value.
    /// Extra rows from the ordinary in-memory dirty journal are permitted.
    pub(crate) fn prepare_delta(
        &self,
        reader: Arc<SegmentReader>,
        ids: Vec<u32>,
    ) -> Result<PreparedScalarPublication> {
        let view = self
            .view
            .as_ref()
            .ok_or_else(|| anyhow!("empty cut requires full publication"))?;
        if !view.has_catalog_base {
            bail!("initial private state requires a full catalog publication");
        }
        let mut catalog = (**view).clone();
        catalog.layers.truncate(self.catalog_len);
        catalog.can_append_incremental()?;
        let layer = Arc::new(DeltaLayer::new(reader, ids)?);
        for private in &view.layers[self.catalog_len..] {
            if !private.coverage.is_subset(&layer.coverage) {
                bail!("checkpoint delta omits a captured private row");
            }
        }
        for &id in &layer.ids {
            catalog.n_docs = catalog.n_docs.max(
                id.checked_add(1)
                    .ok_or_else(|| anyhow!("checkpoint runtime ID overflow"))?,
            );
        }
        catalog.layers.push(layer);
        Ok(PreparedScalarPublication {
            cut: self.clone(),
            catalog,
        })
    }

    /// `catalog` is the real full base, or the real empty initial base plus its
    /// first sparse delta. It is fully prepared before CURRENT can change.
    pub(crate) fn prepare_full(
        &self,
        catalog: ComposedSegmentReader,
    ) -> Result<PreparedScalarPublication> {
        if !catalog.has_catalog_base || catalog.layers.iter().any(|layer| layer.private) {
            bail!("full checkpoint replacement must contain only catalog layers");
        }
        anyhow::ensure!(
            catalog.layers.len() <= MAX_INCREMENTAL_LAYERS,
            "checkpoint catalog exceeds incremental layer cap"
        );
        let mut covered = catalog.base_map.as_ref().map_or_else(
            || (0..catalog.base.n_docs()).collect(),
            |map| map.coverage.clone(),
        );
        for layer in &catalog.layers {
            covered |= &layer.coverage;
        }
        if let Some(view) = &self.view {
            let base_covered = view.base_map.as_ref().map_or_else(
                || (0..view.base.n_docs()).collect(),
                |map| map.coverage.clone(),
            );
            if !base_covered.is_subset(&covered)
                || view
                    .layers
                    .iter()
                    .any(|layer| !layer.coverage.is_subset(&covered))
            {
                bail!("full checkpoint omits captured scalar row coverage");
            }
        }
        Ok(PreparedScalarPublication {
            cut: self.clone(),
            catalog,
        })
    }
}

#[cfg(test)]
mod tests;
