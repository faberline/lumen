//! FlatCpuIndex as a VectorIndex backend: exact kNN over every live slot, and
//! installing, replacing and attaching the checkpoint base and delta segments
//! its slots read from.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};

use crate::index::domain::vector::distance::distance;
use crate::index::domain::vector::flat_cpu_index::{
    FlatCpuIndex, FlatInner, FlatLayer, FlatLocation, FlatVecs,
};
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::domain::vector::VectorIndex;

impl VectorIndex for FlatCpuIndex {
    fn checkpoint_vector(&self, eid: &str) -> Result<Option<Vec<f32>>> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Ok(inner
            .flat
            .as_ref()
            .and_then(|flat| {
                flat.eid_to_row
                    .get(eid)
                    .copied()
                    .filter(|slot| !flat.tomb.contains(*slot))
                    .map(|slot| flat.row(slot as usize).to_vec())
            })
            .or_else(|| inner.store.get_decoded(eid)))
    }

    fn checkpoint_codebook_for_preparation(&self) -> Result<Option<ScalarCodebook>> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Ok(inner.store.codebook)
    }
    fn restore_checkpoint_vector(&self, eid: &str, vector: &[f32]) -> Result<()> {
        self.add(eid, vector)
    }
    fn add(&self, eid: &str, vector: &[f32]) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Self::set_ram(&mut inner, eid, vector)
    }
    fn remove(&self, eid: &str) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        if let Some(flat) = inner.flat.as_mut() {
            if let Some(slot) = flat.eid_to_row.get(eid).copied() {
                if !flat.tomb.contains(slot) {
                    flat.tomb.insert(slot);
                    flat.locations[slot as usize] = None;
                    flat.data.remove(&slot);
                    inner.store.drop(eid);
                    return Ok(true);
                }
            }
        }
        let removed = inner.store.drop(eid);
        if removed {
            inner.flat = None;
        }
        Ok(removed)
    }
    fn search_knn_filtered(
        &self,
        query: &[f32],
        k: usize,
        allow: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<(String, f32)>> {
        use rayon::prelude::*;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        let dim = inner.store.spec.dim as usize;
        if query.len() != dim {
            bail!(
                "kNN query dim mismatch: expected {}, got {}",
                dim,
                query.len()
            );
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let metric = inner.store.spec.metric;
        Self::ensure_flat(&mut inner);
        let flat = inner.flat.as_ref().unwrap();
        let slots: Vec<usize> = flat
            .live_slots()
            .filter(|slot| allow(&flat.eids[*slot]))
            .collect();
        let candidates = slots
            .into_par_iter()
            .map(|slot| (slot, -distance(metric, query, flat.row(slot))))
            .collect();
        Ok(Self::topk(candidates, k, &flat.eids))
    }
    fn search_knn_batch(&self, queries: &[Vec<f32>], k: usize) -> Result<Vec<Vec<(String, f32)>>> {
        for query in queries {
            if query.len()
                != self
                    .inner
                    .lock()
                    .map_err(|_| anyhow!("flat lock poisoned"))?
                    .store
                    .spec
                    .dim as usize
            {
                bail!("kNN query dim mismatch");
            }
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        let metric = inner.store.spec.metric;
        Self::ensure_flat(&mut inner);
        let flat = inner.flat.as_ref().unwrap();
        let slots: Vec<usize> = flat.live_slots().collect();
        Ok(queries
            .iter()
            .map(|query| {
                Self::topk(
                    slots
                        .iter()
                        .map(|slot| (*slot, -distance(metric, query, flat.row(*slot))))
                        .collect(),
                    k,
                    &flat.eids,
                )
            })
            .collect())
    }
    fn len(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| Self::live_len(&inner))
            .unwrap_or(0)
    }
    fn dump_for_snapshot(&self) -> Result<(Vec<(String, Vec<f32>)>, Option<ScalarCodebook>)> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        if let Some(flat) = inner.flat.as_ref() {
            Ok((
                flat.live_slots()
                    .map(|slot| (flat.eids[slot].clone(), flat.row(slot).to_vec()))
                    .collect(),
                inner.store.codebook.clone(),
            ))
        } else {
            Ok((
                inner.store.iter_decoded().collect(),
                inner.store.codebook.clone(),
            ))
        }
    }
    fn seal_to_segment_prod(&self, path: &std::path::Path) -> Result<Option<Vec<String>>> {
        self.seal_to_segment_prod(path)
    }
    fn seal_to_segment_prod_at(
        &self,
        path: &std::path::Path,
        seq: u64,
    ) -> Result<Option<Vec<String>>> {
        self.seal_checkpoint(path, Some(seq))
    }
    fn seal_releases_ram(&self) -> bool {
        true
    }
    fn resident_vector_payload_rows(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| {
                let mut ids: std::collections::HashSet<&str> = inner
                    .store
                    .raw
                    .keys()
                    .chain(inner.store.encoded.keys())
                    .map(String::as_str)
                    .collect();
                if let Some(flat) = &inner.flat {
                    ids.extend(
                        flat.data
                            .keys()
                            .map(|slot| flat.eids[*slot as usize].as_str()),
                    );
                }
                ids.len()
            })
            .unwrap_or(0)
    }
    fn has_checkpoint_mapping(&self) -> bool {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .flat
                    .as_ref()
                    .is_some_and(|flat| flat.seg.is_some() || !flat.layers.is_empty())
            })
            .unwrap_or(false)
    }

    fn checkpoint_resident_bytes(&self) -> Option<u64> {
        let inner = self.inner.lock().expect("flat lock poisoned");
        let row_bytes = |eid: &str| u64::from(inner.store.spec.dim) * 4 + eid.len() as u64;
        let stored = inner.store.raw.keys().chain(inner.store.encoded.keys());
        let mut bytes: u64 = stored.map(|eid| row_bytes(eid)).sum();
        if let Some(flat) = &inner.flat {
            for slot in flat.data.keys() {
                let eid = &flat.eids[*slot as usize];
                if !inner.store.raw.contains_key(eid) && !inner.store.encoded.contains_key(eid) {
                    bytes += row_bytes(eid);
                }
            }
        }
        Some(bytes)
    }
    #[cfg(test)]
    fn __seal_flat_to_segment(&self, path: &std::path::Path) -> Result<Option<u32>> {
        self.seal_checkpoint(path, None)
            .map(|rows| rows.map(|rows| rows.len() as u32))
    }

    fn install_checkpoint_base(
        &self,
        reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        external_ids: &[String],
        acknowledged: &HashMap<String, bool>,
    ) -> Result<()> {
        if reader.n_docs() as usize != external_ids.len() {
            bail!("vector base row map does not match payload");
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Self::ensure_flat(&mut inner);
        let mut old = inner.flat.take().unwrap();
        let mut eids = external_ids.to_vec();
        let mut eid_to_row: HashMap<String, u32> = eids
            .iter()
            .enumerate()
            .map(|(slot, eid)| (eid.clone(), slot as u32))
            .collect();
        let mut tomb: roaring::RoaringBitmap = (0..eids.len() as u32)
            .filter(|row| reader.vector_at(*row, old.dim).is_none())
            .collect();
        let mut locations: Vec<Option<FlatLocation>> = (0..eids.len() as u32)
            .map(|row| (!tomb.contains(row)).then_some(FlatLocation::Base(row)))
            .collect();
        let mut data = HashMap::new();
        // Move only newer RAM payloads. No captured vector payload is retained.
        for (eid, keep_newer) in acknowledged {
            if *keep_newer {
                inner.store.drop(eid);
                continue;
            }
            let Some(old_slot) = old.eid_to_row.get(eid).copied() else {
                continue;
            };
            let slot = *eid_to_row.entry(eid.clone()).or_insert_with(|| {
                let slot = eids.len() as u32;
                eids.push(eid.clone());
                locations.push(None);
                slot
            });
            if old.tomb.contains(old_slot) {
                tomb.insert(slot);
                continue;
            }
            tomb.remove(slot);
            let location = old.locations[old_slot as usize];
            if let Some(FlatLocation::Ram) = location {
                if let Some(value) = old.data.remove(&old_slot) {
                    data.insert(slot, value);
                }
            }
            locations[slot as usize] = location;
        }
        inner.flat = Some(FlatVecs {
            data,
            eids,
            dim: old.dim,
            seg: Some(reader),
            n_base: external_ids.len(),
            tomb,
            eid_to_row,
            locations,
            layers: old.layers,
        });
        Ok(())
    }

    fn checkpoint_delta_readers(
        &self,
    ) -> Vec<Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
        self.inner
            .lock()
            .expect("flat lock poisoned")
            .flat
            .as_ref()
            .map(|flat| {
                flat.layers
                    .iter()
                    .map(|layer| layer.reader.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn checkpoint_base_reader(
        &self,
    ) -> Option<Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
        self.inner
            .lock()
            .expect("flat lock poisoned")
            .flat
            .as_ref()
            .and_then(|flat| flat.seg.clone())
    }

    fn replace_checkpoint_base(
        &self,
        base: &Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        inputs: &[Arc<crate::persistence::infrastructure::segment::SegmentReader>],
        reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        external_ids: &[String],
    ) -> Result<()> {
        if reader.n_docs() as usize != external_ids.len() {
            bail!("compacted vector base row count mismatch");
        }
        let rows: HashMap<&str, u32> = external_ids
            .iter()
            .enumerate()
            .map(|(row, eid)| (eid.as_str(), row as u32))
            .collect();
        if rows.len() != external_ids.len() {
            bail!("duplicate compacted vector base ID");
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        let flat = inner
            .flat
            .as_mut()
            .ok_or_else(|| anyhow!("vector base replacement has no live base"))?;
        if !flat
            .seg
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, base))
            || inputs.len() > flat.layers.len()
            || !flat
                .layers
                .iter()
                .zip(inputs)
                .all(|(layer, input)| Arc::ptr_eq(&layer.reader, input))
        {
            bail!("compacted vector base inputs no longer match live layers");
        }
        let retargets: Vec<(usize, u32)> = flat.locations.iter().enumerate().filter_map(|(slot, source)| {
            (matches!(source, Some(FlatLocation::Base(_)))
                || matches!(source, Some(FlatLocation::Layer { layer, .. }) if (*layer as usize) < inputs.len()))
                .then_some(slot)
        }).map(|slot| {
            let row = *rows.get(flat.eids[slot].as_str()).ok_or_else(|| anyhow!("compacted vector base lost a live ID"))?;
            if reader.vector_at(row, flat.dim).is_none() { bail!("compacted vector base lost a live row"); }
            Ok((slot, row))
        }).collect::<Result<_>>()?;
        for source in &mut flat.locations {
            if let Some(FlatLocation::Layer { layer, .. }) = source {
                if (*layer as usize) >= inputs.len() {
                    *layer -= u32::try_from(inputs.len())?;
                }
            }
        }
        for (slot, row) in retargets {
            flat.locations[slot] = Some(FlatLocation::Base(row));
        }
        flat.layers.drain(..inputs.len());
        flat.seg = Some(reader);
        flat.n_base = external_ids.len();
        Ok(())
    }

    fn replace_checkpoint_deltas(
        &self,
        inputs: &[Arc<crate::persistence::infrastructure::segment::SegmentReader>],
        reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        external_ids: &[String],
    ) -> Result<()> {
        if inputs.is_empty() || reader.n_docs() as usize != external_ids.len() {
            bail!("invalid compacted vector inputs or row map");
        }
        let rows: HashMap<&str, u32> = external_ids
            .iter()
            .enumerate()
            .map(|(row, eid)| (eid.as_str(), row as u32))
            .collect();
        if rows.len() != external_ids.len() {
            bail!("duplicate compacted vector external ID");
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        let flat = inner
            .flat
            .as_mut()
            .ok_or_else(|| anyhow!("vector compaction has no live layers"))?;
        let start = flat
            .layers
            .windows(inputs.len())
            .position(|window| {
                window
                    .iter()
                    .zip(inputs)
                    .all(|(layer, expected)| Arc::ptr_eq(&layer.reader, expected))
            })
            .ok_or_else(|| anyhow!("compacted vector inputs no longer match live layers"))?;
        let end = start + inputs.len();
        let output_layer = u32::try_from(start)?;
        // Validate every retarget before changing a slot. RAM mutations and
        // tombstones are newer than these immutable inputs and stay untouched.
        let retargets: Vec<(usize, u32)> = flat.locations.iter().enumerate().filter_map(|(slot, source)| {
            matches!(source, Some(FlatLocation::Layer { layer, .. }) if (start..end).contains(&(*layer as usize)))
                .then_some(slot)
        }).map(|slot| {
            let row = *rows.get(flat.eids[slot].as_str())
                .ok_or_else(|| anyhow!("compacted vector lost a live input ID"))?;
            if reader.vector_at(row, flat.dim).is_none() {
                bail!("compacted vector lost a live input row");
            }
            Ok((slot, row))
        }).collect::<Result<_>>()?;
        for source in &mut flat.locations {
            if let Some(FlatLocation::Layer { layer, .. }) = source {
                if (*layer as usize) >= end {
                    *layer -= u32::try_from(inputs.len() - 1)?;
                }
            }
        }
        for (slot, row) in retargets {
            flat.locations[slot] = Some(FlatLocation::Layer {
                layer: output_layer,
                row,
            });
        }
        flat.layers.splice(start..end, [FlatLayer { reader }]);
        Ok(())
    }

    fn attach_checkpoint_delta(
        &self,
        reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        external_ids: &[String],
        acknowledged: &[bool],
    ) -> Result<()> {
        if reader.n_docs() as usize != external_ids.len()
            || external_ids.len() != acknowledged.len()
        {
            bail!("vector checkpoint row map does not match payload");
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Self::ensure_flat(&mut inner);
        let FlatInner { store, flat } = &mut *inner;
        let flat = flat.as_mut().unwrap();
        let layer = flat.layers.len() as u32;
        flat.layers.push(FlatLayer {
            reader: reader.clone(),
        });
        for (row, (eid, ack)) in external_ids.iter().zip(acknowledged).enumerate() {
            if !ack {
                continue;
            };
            let slot = *flat.eid_to_row.entry(eid.clone()).or_insert_with(|| {
                let slot = flat.eids.len() as u32;
                flat.eids.push(eid.clone());
                flat.locations.push(None);
                slot
            });
            flat.data.remove(&slot);
            store.drop(eid);
            if reader.vector_at(row as u32, flat.dim).is_some() {
                flat.tomb.remove(slot);
                flat.locations[slot as usize] = Some(FlatLocation::Layer {
                    layer,
                    row: row as u32,
                });
            } else {
                flat.tomb.insert(slot);
                flat.locations[slot as usize] = None;
            }
        }
        Ok(())
    }
}
