//! Preparing a checkpoint's publication outside the capture barrier: the scalar
//! views a capture pinned turned into catalog replacements, and the scalar row
//! maps a compaction installs.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};

use crate::index::application::checkpoint_capture::CheckpointCapture;
use crate::index::application::engine::Engine;
use crate::index::application::live_base::scalar_prepared_segment;
use crate::index::domain::field_index::FieldIndex;

impl Engine {
    /// Prepare captured scalar views before the durable generation pointer moves.
    /// Row maps are made outside the apply lease, while the cut holds only Arcs.
    pub(crate) fn prepare_scalar_checkpoint_publications(
        &self,
        capture: &mut CheckpointCapture,
    ) -> Result<()> {
        let mut plans = Vec::new();
        {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            for (name, cuts) in &capture.scalar_cuts {
                let identity = capture
                    .collections
                    .get(name)
                    .ok_or_else(|| anyhow!("scalar checkpoint collection identity missing"))?;
                let Some(coll) = state.collections.get(name).filter(|coll| {
                    coll.collection_generation == identity.generation
                        && coll.version == identity.schema_version
                        && coll.deleted_at.is_none()
                }) else {
                    continue;
                };
                for (field, cut) in cuts {
                    let retire = capture
                        .field_dirty
                        .get(name)
                        .and_then(|fields| fields.get(field))
                        .into_iter()
                        .flatten()
                        .map(|(eid, revision)| {
                            let id = coll.interner.id(eid).ok_or_else(|| {
                                anyhow!(
                                    "captured scalar external ID is absent from live collection"
                                )
                            })?;
                            Ok((id, eid.clone(), *revision))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let base = capture
                        .prepared
                        .get(name)
                        .into_iter()
                        .flatten()
                        .find(|prepared| prepared.name == *field)
                        .and_then(|prepared| scalar_prepared_segment(&prepared.index));
                    let deltas: Vec<_> = capture.prepared_deltas.get(name).into_iter().flatten()
                        .filter(|delta| delta.field == *field)
                        .map(|delta| {
                            let ids = delta.external_ids.iter().map(|eid| coll.interner.id(eid)
                                .ok_or_else(|| anyhow!("scalar checkpoint external ID is absent from live collection")))
                                .collect::<Result<Vec<_>>>()?;
                            Ok((delta.reader.clone(), ids))
                        }).collect::<Result<Vec<_>>>()?;
                    if base.is_some() || !deltas.is_empty() {
                        plans.push((
                            name.clone(),
                            field.clone(),
                            cut.clone(),
                            base,
                            deltas,
                            retire,
                        ));
                    }
                }
            }
        }
        for (name, field, cut, base, deltas, retire) in plans {
            let publication = if let Some(base) = base {
                let mut catalog = (*base).clone();
                for (reader, ids) in &deltas {
                    catalog = catalog.with_delta(reader.clone(), ids.clone())?;
                }
                cut.prepare_full(catalog)?
            } else {
                if deltas.len() != 1 {
                    bail!("scalar delta checkpoint has no full catalog base");
                }
                let (reader, ids) = deltas.first().expect("checked one delta");
                cut.prepare_delta(reader.clone(), ids.clone())?
            };
            capture
                .scalar_publications
                .entry(name.clone())
                .or_default()
                .insert(field.clone(), publication);
            capture
                .scalar_retire
                .entry(name)
                .or_default()
                .insert(field, retire);
        }
        // This is the final dry prefix check before the generation caller may
        // validate its staged layout and advance CURRENT. A later mutation may
        // append only a private layer; a changed catalog prefix refuses here.
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        for (name, publications) in &capture.scalar_publications {
            let identity = capture
                .collections
                .get(name)
                .ok_or_else(|| anyhow!("scalar checkpoint identity missing before publication"))?;
            let Some(coll) = state.collections.get(name).filter(|coll| {
                coll.collection_generation == identity.generation
                    && coll.version == identity.schema_version
                    && coll.deleted_at.is_none()
            }) else {
                continue;
            };
            for (field, publication) in publications {
                let index = coll.fields.get(field).ok_or_else(|| {
                    anyhow!("scalar checkpoint field disappeared before publication")
                })?;
                let live = match index {
                    FieldIndex::Keyword(index) => index.segment.as_ref(),
                    FieldIndex::Number(index) => index.segment.as_ref(),
                    FieldIndex::Set(index) => index.segment.as_ref(),
                    _ => bail!("scalar checkpoint publication changed field kind"),
                };
                match live {
                    Some(live) => publication.validate_live(live)?,
                    None => {
                        let _ = publication.install_without_composition()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Prepare scalar row maps outside CaptureBarrier. The read lock pins only
    /// schema and stable runtime IDs while we copy the selected IDs. Readers
    /// can still query. Coverage and replacement layers are built after that
    /// lock is released; binding checks their exact immutable input identities.
    pub(crate) fn prepare_checkpoint_compactions(
        &self,
        capture: &mut CheckpointCapture,
    ) -> Result<()> {
        self.prepare_scalar_checkpoint_publications(capture)?;
        let mut plans = Vec::new();
        {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            for (name, compactions) in &capture.prepared_compactions {
                let identity = capture
                    .collections
                    .get(name)
                    .ok_or_else(|| anyhow!("compaction collection identity missing"))?;
                let Some(coll) = state.collections.get(name).filter(|coll| {
                    coll.collection_generation == identity.generation
                        && coll.version == identity.schema_version
                        && coll.deleted_at.is_none()
                }) else {
                    // Binding skips this obsolete collection too. Restore is
                    // separately checked by the publication epoch guard.
                    continue;
                };
                let mut views = BTreeMap::new();
                for compaction in compactions {
                    let index = coll
                        .fields
                        .get(&compaction.field)
                        .ok_or_else(|| anyhow!("compacted field is absent from live collection"))?;
                    let segment = match index {
                        FieldIndex::Keyword(index) => &index.segment,
                        FieldIndex::Number(index) => &index.segment,
                        FieldIndex::Set(index) => &index.segment,
                        FieldIndex::Hash(index) => &index.segment,
                        FieldIndex::Text { idx, .. } => &idx.segment,
                        FieldIndex::Vector { .. } => continue,
                    };
                    let view = if let Some(publication) = capture
                        .scalar_publications
                        .get(name)
                        .and_then(|fields| fields.get(&compaction.field))
                    {
                        std::sync::Arc::new(publication.catalog_view())
                    } else {
                        segment
                            .as_ref()
                            .ok_or_else(|| anyhow!("compacted field has no live base"))?
                            .clone()
                    };
                    views.insert(compaction.field.clone(), view);
                }
                let map_ids = |eids: &[String]| -> Result<Vec<u32>> {
                    eids.iter()
                        .map(|eid| {
                            coll.interner.id(eid).ok_or_else(|| {
                                anyhow!("compacted external ID is absent from live collection")
                            })
                        })
                        .collect()
                };
                let mut deltas = Vec::new();
                for delta in capture.prepared_deltas.get(name).into_iter().flatten() {
                    if views.contains_key(&delta.field)
                        && !capture
                            .scalar_publications
                            .get(name)
                            .is_some_and(|fields| fields.contains_key(&delta.field))
                    {
                        deltas.push((
                            delta.field.clone(),
                            delta.reader.clone(),
                            map_ids(&delta.external_ids)?,
                        ));
                    }
                }
                let mut outputs = Vec::new();
                for (index, compaction) in compactions.iter().enumerate() {
                    if views.contains_key(&compaction.field) {
                        outputs.push((index, map_ids(&compaction.external_ids)?));
                    }
                }
                plans.push((name.clone(), views, deltas, outputs));
            }
        }
        for (name, mut views, deltas, outputs) in plans {
            // The newly written delta is installed first at publication. Use
            // that same order while preparing every successive merge.
            for (field, reader, ids) in deltas {
                let view = views.get_mut(&field).expect("selected scalar view");
                *view = std::sync::Arc::new(view.with_delta(reader, ids)?);
            }
            for (index, ids) in outputs {
                let compacted = &mut capture
                    .prepared_compactions
                    .get_mut(&name)
                    .expect("captured compaction collection")[index];
                let view = views
                    .get_mut(&compacted.field)
                    .expect("selected scalar view");
                let prepared = view.prepare_replacement(
                    compacted.base.as_ref(),
                    &compacted.inputs,
                    compacted.reader.clone(),
                    ids,
                )?;
                *view = std::sync::Arc::new(view.install_prepared_replacement(&prepared)?);
                compacted.scalar = Some(prepared);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
