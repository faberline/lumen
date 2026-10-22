//! Reopening one validated generation into a fresh engine, with the recovery
//! profile's phase timings.

use crate::index::application::{engine::Engine, recovery_profile::RecoveryPhase};
use crate::persistence::domain::generation_manifest::SegmentKind;
use crate::persistence::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::field_deltas::read_delta_values;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::materialize_flat_reopen_tree;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, SegmentRdbStore, GENERATION_MANIFEST_V2, GENERATION_MANIFEST_V3,
    HNSW_GRAPH_CACHE_DIR,
};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

impl SegmentRdbStore {
    pub(super) fn reopen_once_with_graph_policy(
        &self,
        engine: &Engine,
        record: &GenerationRecord,
        collections: usize,
        defer_until_aof: bool,
    ) -> Result<()> {
        let manifest_started = self.recovery_profile.enabled().then(Instant::now);
        let manifest = if record.legacy {
            None
        } else {
            Some(
                self.recovery_profile
                    .phase_start(RecoveryPhase::ManifestDecode, || {
                        read_generation_manifest(&record.path)
                    })?,
            )
        };
        if let Some(started) = manifest_started {
            self.add_recovery_timing(|timings| {
                timings.manifest_decode_ms = started.elapsed().as_millis() as u64;
            });
        }
        let graph_cache = self.root.join(HNSW_GRAPH_CACHE_DIR);
        let graph_cache = std::fs::symlink_metadata(&graph_cache)
            .ok()
            .filter(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
            .map(|_| graph_cache);
        let defer_hnsw = defer_until_aof
            || manifest.as_ref().is_some_and(|manifest| {
                (graph_cache.is_some()
                    && matches!(
                        manifest.schema_version,
                        GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
                    ))
                    || manifest.collections.iter().any(|collection| {
                        collection.segments.iter().any(|segment| {
                            (matches!(segment.kind, SegmentKind::Delta)
                                || segment.local_rows.is_some())
                                && segment
                                    .field
                                    .as_ref()
                                    .and_then(|field| collection.schema.get(field))
                                    .and_then(|spec| spec.get("type"))
                                    .and_then(serde_json::Value::as_str)
                                    == Some("vector")
                        })
                    })
            });
        let base_decode_started = self.recovery_profile.enabled().then(Instant::now);
        let mut mapped_bases = BTreeMap::<String, BTreeMap<String, Vec<String>>>::new();
        self.recovery_profile
            .phase_start(RecoveryPhase::BaseRowsDecode, || -> Result<()> {
                for collection in manifest.iter().flat_map(|manifest| &manifest.collections) {
                    for segment in &collection.segments {
                        if !matches!(segment.kind, SegmentKind::Base) {
                            continue;
                        }
                        if let Some(local) = &segment.local_rows {
                            let rows = decode_sparse_local_rows(
                                &record.path.join(&local.path),
                                local.count,
                            )?;
                            let ids = rows.into_external_ids();
                            mapped_bases
                                .entry(collection.collection_id.clone())
                                .or_default()
                                .insert(
                                    segment
                                        .field
                                        .clone()
                                        .ok_or_else(|| anyhow!("mapped base has no field"))?,
                                    ids,
                                );
                        }
                    }
                }
                Ok(())
            })?;
        if let Some(started) = base_decode_started {
            self.add_recovery_timing(|timings| {
                timings.base_decode_ms = started.elapsed().as_millis() as u64;
            });
        }
        let compatibility_tree =
            self.recovery_profile
                .phase_start(RecoveryPhase::FlatCompatibilityTree, || {
                    manifest
                        .as_ref()
                        .filter(|manifest| manifest.schema_version == GENERATION_MANIFEST_V3)
                        .map(|manifest| materialize_flat_reopen_tree(&record.path, manifest))
                        .transpose()
                })?;
        let reopen_started = self.recovery_profile.enabled().then(Instant::now);
        let reopened = self
            .recovery_profile
            .phase_start(RecoveryPhase::EngineReopen, || {
                engine
                    .reopen_from_segment_dir_with_base_rows(
                        &record.path,
                        defer_hnsw,
                        &mapped_bases,
                        self.recovery_profile
                            .enabled()
                            .then_some(&self.recovery_profile),
                    )
                    .with_context(|| format!("reopen checkpoint {}", record.path.display()))
            });
        if let Some(tree) = compatibility_tree {
            for path in tree {
                let _ = std::fs::remove_dir_all(path);
            }
        }
        let reopened = reopened?;
        if let Some(manifest) = manifest.as_ref().filter(|m| {
            matches!(
                m.schema_version,
                GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
            )
        }) {
            let capture = crate::index::application::checkpoint_capture::CheckpointCapture {
                prepared: BTreeMap::new(),
                prepared_deltas: BTreeMap::new(),
                prepared_compactions: BTreeMap::new(),
                scalar_cuts: BTreeMap::new(),
                scalar_publications: BTreeMap::new(),
                scalar_retire: BTreeMap::new(),
                live_delta_inputs: BTreeMap::new(),
                live_base_inputs: BTreeMap::new(),
                collections: manifest
                    .collections
                    .iter()
                    .map(|c| {
                        (
                            c.collection_id.clone(),
                            crate::index::application::checkpoint_capture::CheckpointCollectionIdentity {
                                generation: c.collection_generation,
                                data_version: c.data_version,
                                schema_version: c.schema_version,
                            },
                        )
                    })
                    .collect(),
                next_generation: manifest.next_collection_generation,
                field_dirty: BTreeMap::new(),
                frozen_changes: BTreeMap::new(),
                reused: BTreeSet::new(),
                initial_sparse: BTreeSet::new(),
                field_deltas: Arc::new(BTreeMap::new()),
                record_cut: None,
            };
            let delta_decode_started = self.recovery_profile.enabled().then(Instant::now);
            self.recovery_profile.phase_start(
                RecoveryPhase::DeltaDecodeApply,
                || -> Result<()> {
                    for collection in &manifest.collections {
                        for segment in &collection.segments {
                            if matches!(segment.kind, SegmentKind::Delta) {
                                let local = segment
                                    .local_rows
                                    .as_ref()
                                    .ok_or_else(|| anyhow!("delta has no row map"))?;
                                let ids = decode_sparse_local_rows(
                                    &record.path.join(&local.path),
                                    local.count,
                                )?;
                                let reader =
                                    std::sync::Arc::new(SegmentReader::open(
                                        &record.path.join(&segment.path),
                                    )?);
                                let field = segment
                                    .field
                                    .as_deref()
                                    .ok_or_else(|| anyhow!("delta has no field"))?;
                                let spec: crate::shared_kernel::types::schema::FieldSpec = serde_json::from_value(
                                    collection
                                        .schema
                                        .get(field)
                                        .cloned()
                                        .ok_or_else(|| anyhow!("delta field missing"))?,
                                )?;
                                let external_ids = ids.into_external_ids();
                                if spec.field_type == crate::shared_kernel::types::schema::FieldType::Vector
                                    && spec.vector_spec()?.is_some_and(|vector| {
                                        vector.backend != crate::shared_kernel::types::schema::VectorBackend::FlatCpu
                                    })
                                {
                                    let values = read_delta_values(&reader, &spec)?;
                                    let rows = external_ids.into_iter().zip(values).collect();
                                    engine.apply_checkpoint_delta(
                                        &collection.collection_id,
                                        field,
                                        rows,
                                    )?;
                                } else {
                                    engine.attach_checkpoint_delta_reader(
                                        &collection.collection_id,
                                        field,
                                        reader,
                                        external_ids,
                                    )?;
                                }
                            }
                        }
                    }
                    Ok(())
                },
            )?;
            if let Some(started) = delta_decode_started {
                self.add_recovery_timing(|timings| {
                    timings.delta_decode_ms = started.elapsed().as_millis() as u64;
                });
            }
            if defer_hnsw && !defer_until_aof {
                let vector_finish_started = self.recovery_profile.enabled().then(Instant::now);
                self.recovery_profile.phase_start(
                    RecoveryPhase::CheckpointHnswGraph,
                    || -> Result<()> {
                        if let Some(cache) = graph_cache.as_deref() {
                            engine.finish_checkpoint_vectors_with_graph_cache(Some(cache))?;
                        } else {
                            engine.finish_checkpoint_vectors()?;
                        }
                        Ok(())
                    },
                )?;
                if let Some(started) = vector_finish_started {
                    self.add_recovery_timing(|timings| {
                        timings.vector_finish_ms = started.elapsed().as_millis() as u64;
                    });
                }
            }
            let identity_hydration_started = self.recovery_profile.enabled().then(Instant::now);
            self.recovery_profile
                .phase_start(RecoveryPhase::IdentityHydration, || {
                    engine.hydrate_checkpoint_identities(&record.path, &capture)
                })?;
            if let Some(started) = identity_hydration_started {
                self.add_recovery_timing(|timings| {
                    timings.identity_hydration_ms = started.elapsed().as_millis() as u64;
                });
            }
            if let Some(started) = reopen_started {
                self.add_recovery_timing(|timings| {
                    timings.reopen_ms = started.elapsed().as_millis() as u64;
                });
            }
            return Ok(());
        }
        if collections == 0 {
            if reopened != 0 {
                bail!(
                    "empty generation {} reopened with unexpected sequence {reopened}",
                    record.name
                );
            }
        } else if reopened != record.sequence {
            bail!(
                "generation {} expected sequence {} but reopened {reopened}",
                record.name,
                record.sequence
            );
        }
        engine.bind_legacy_checkpoint_origin(&record.path)?;
        if let Some(started) = reopen_started {
            self.add_recovery_timing(|timings| {
                timings.reopen_ms = started.elapsed().as_millis() as u64;
            });
        }
        Ok(())
    }
}
