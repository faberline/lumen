//! A checkpoint's vector fields after recovery: each HNSW field rebuilt from
//! its rows or loaded from the optional graph cache, and the graph caches saved
//! back.

use std::time::Instant;

use anyhow::{anyhow, bail, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;

fn graph_cache_field_path(
    root: &std::path::Path,
    collection: &str,
    field: &str,
) -> std::path::PathBuf {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update((collection.len() as u64).to_le_bytes());
    hash.update(collection.as_bytes());
    hash.update(field.as_bytes());
    root.join(format!("{:x}", hash.finalize()))
}

fn ensure_real_cache_directory(path: &std::path::Path) -> Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        bail!("optional graph cache path is not a real directory");
    }
    storage_durable::set_private_directory_mode(path)?;
    Ok(())
}

impl Engine {
    pub(crate) fn finish_checkpoint_vectors(&self) -> Result<()> {
        self.finish_checkpoint_vectors_with_graph_cache(None)
    }

    pub(crate) fn finish_checkpoint_vectors_with_graph_cache(
        &self,
        cache: Option<&std::path::Path>,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for (collection_name, coll) in &mut state.collections {
            for (field_name, field) in &mut coll.fields {
                if let FieldIndex::Vector { spec, idx, bytes } = field {
                    if spec.backend == crate::shared_kernel::types::schema::VectorBackend::HnswCpu {
                        let field_started = Instant::now();
                        let snapshot_started = Instant::now();
                        let (vectors, codebook) = match idx.dump_for_snapshot() {
                            Ok(snapshot) => snapshot,
                            Err(error) => {
                                tracing::error!(
                                    collection = %collection_name,
                                    field = %field_name,
                                    rows = idx.len(),
                                    cache_result = "not_attempted",
                                    restore_result = "error",
                                    snapshot_ms = snapshot_started.elapsed().as_millis() as u64,
                                    cache_prepare_ms = 0u64,
                                    cache_fingerprint_ms = 0u64,
                                    cache_manifest_ms = 0u64,
                                    cache_payload_hash_ms = 0u64,
                                    cache_materialize_ms = 0u64,
                                    hnsw_deserialize_ms = 0u64,
                                    graph_validate_ms = 0u64,
                                    fallback_rebuild_ms = 0u64,
                                    restore_total_ms = 0u64,
                                    field_total_ms = field_started.elapsed().as_millis() as u64,
                                    %error,
                                    "HNSW graph restore timing"
                                );
                                return Err(error);
                            }
                        };
                        let snapshot_elapsed = snapshot_started.elapsed();
                        let rows = vectors.len();
                        *bytes = vectors
                            .iter()
                            .map(|(eid, value)| (eid.len() + value.len() * 4) as u64)
                            .sum();
                        let directory = cache
                            .map(|root| graph_cache_field_path(root, collection_name, field_name));
                        let (restored, timing) = match HnswCpuIndex::restore_with_graph_cache_timed(
                            *spec,
                            vectors,
                            codebook,
                            directory.as_deref(),
                        ) {
                            Ok(restored) => restored,
                            Err(failure) => {
                                let timing = failure.timing;
                                tracing::error!(
                                    collection = %collection_name,
                                    field = %field_name,
                                    rows,
                                    cache_result = timing.cache_result.map(|result| result.as_str()).unwrap_or("absent"),
                                    restore_result = "error",
                                    snapshot_ms = snapshot_elapsed.as_millis() as u64,
                                    cache_prepare_ms = timing.cache_prepare.as_millis() as u64,
                                    cache_fingerprint_ms = timing.cache_fingerprint.as_millis() as u64,
                                    cache_manifest_ms = timing.cache_manifest.as_millis() as u64,
                                    cache_payload_hash_ms = timing.cache_payload_hash.as_millis() as u64,
                                    cache_materialize_ms = timing.cache_materialize.as_millis() as u64,
                                    hnsw_deserialize_ms = timing.cache_deserialize.as_millis() as u64,
                                    graph_validate_ms = timing.cache_validate.as_millis() as u64,
                                    fallback_rebuild_ms = timing.fallback_rebuild.as_millis() as u64,
                                    restore_total_ms = timing.total.as_millis() as u64,
                                    field_total_ms = field_started.elapsed().as_millis() as u64,
                                    error = %failure.error,
                                    "HNSW graph restore timing"
                                );
                                return Err(failure.error);
                            }
                        };
                        let cache_result = timing
                            .cache_result
                            .map(|result| result.as_str())
                            .unwrap_or("absent");
                        tracing::info!(
                            collection = %collection_name,
                            field = %field_name,
                            rows,
                            cache_result,
                            restore_result = if cache_result == "hit" { "hit" } else { "rebuild" },
                            snapshot_ms = snapshot_elapsed.as_millis() as u64,
                            cache_prepare_ms = timing.cache_prepare.as_millis() as u64,
                            cache_fingerprint_ms = timing.cache_fingerprint.as_millis() as u64,
                            cache_manifest_ms = timing.cache_manifest.as_millis() as u64,
                            cache_payload_hash_ms = timing.cache_payload_hash.as_millis() as u64,
                            cache_materialize_ms = timing.cache_materialize.as_millis() as u64,
                            hnsw_deserialize_ms = timing.cache_deserialize.as_millis() as u64,
                            graph_validate_ms = timing.cache_validate.as_millis() as u64,
                            fallback_rebuild_ms = timing.fallback_rebuild.as_millis() as u64,
                            restore_total_ms = timing.total.as_millis() as u64,
                            field_total_ms = field_started.elapsed().as_millis() as u64,
                            "HNSW graph restore timing"
                        );
                        *idx = Box::new(restored);
                    }
                }
            }
        }
        self.publish_storage_bytes(&state);
        Ok(())
    }

    /// Caller holds the standalone writer fence and checkpoint save gate.
    pub(crate) fn has_hnsw_graphs(&self) -> Result<bool> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        Ok(state.collections.values().any(|collection| collection.fields.values().any(|field| {
            matches!(field, FieldIndex::Vector { spec, idx, .. }
                if spec.backend == crate::shared_kernel::types::schema::VectorBackend::HnswCpu && idx.len() > 0)
        })))
    }

    /// Caller holds the standalone writer fence and checkpoint save gate.
    pub(crate) fn save_hnsw_graph_caches(&self, root: &std::path::Path) -> Result<usize> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut live_paths = std::collections::BTreeSet::new();
        for (collection_name, collection) in &state.collections {
            for (field_name, field) in &collection.fields {
                if matches!(field, FieldIndex::Vector { spec, idx, .. }
                    if spec.backend == crate::shared_kernel::types::schema::VectorBackend::HnswCpu && idx.len() > 0)
                {
                    live_paths.insert(graph_cache_field_path(root, collection_name, field_name));
                }
            }
        }
        if !live_paths.is_empty() || root.exists() {
            ensure_real_cache_directory(root)?;
            // Checkpoint plus synced AOF remain authoritative under the writer
            // fence. Removed fields no longer need their optional old cache.
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                let name = entry.file_name();
                let owned = name.to_str().is_some_and(|name| {
                    name.len() == 64
                        && name
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                });
                if owned && entry.file_type()?.is_dir() && !live_paths.contains(&entry.path()) {
                    std::fs::remove_dir_all(entry.path())?;
                }
            }
        }
        let mut saved = 0;
        for (collection_name, collection) in &state.collections {
            for (field_name, field) in &collection.fields {
                if let FieldIndex::Vector { spec, idx, .. } = field {
                    if spec.backend != crate::shared_kernel::types::schema::VectorBackend::HnswCpu
                        || idx.len() == 0
                    {
                        continue;
                    }
                    ensure_real_cache_directory(root)?;
                    let directory = graph_cache_field_path(root, collection_name, field_name);
                    ensure_real_cache_directory(&directory)?;
                    saved += usize::from(idx.save_graph_cache(&directory)?);
                }
            }
        }
        Ok(saved)
    }
}
