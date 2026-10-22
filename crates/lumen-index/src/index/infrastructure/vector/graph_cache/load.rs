//! Loading a saved graph: each stage checks the directory, fingerprint,
//! manifest and payload hash before the graph is deserialized, and every point
//! is checked against the checkpoint vectors, with the time each stage took.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::RwLock;
use std::time::Instant;

use anyhow::{anyhow, ensure, Context, Result};
use hnsw_rs::anndists::dist::{DistDot, DistL2};
use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::hnswio::HnswIo;
use self_cell::MutBorrow;

use crate::index::domain::vector::distance::normalize_unit_safe;
use crate::index::domain::vector::hnsw_cpu_index::{
    hnsw_search_ef, HnswBackend, HnswCpuIndex, HnswCpuInner, HNSW_EF_CONSTRUCTION,
    HNSW_MAX_NB_CONNECTION,
};
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::domain::vector::vector_store::VectorStore;
use crate::index::infrastructure::vector::graph_cache::{
    file_hash, fingerprint, platform, real_directory, LoadFailure, LoadTiming, LoadedGraph,
    Manifest, OwnedGraph, BASENAME, FORMAT, MANIFEST,
};
use crate::shared_kernel::types::schema::{VectorMetric, VectorSpec};

pub(in crate::index) fn load_timed(
    spec: VectorSpec,
    vectors: &[(String, Vec<f32>)],
    codebook: Option<ScalarCodebook>,
    directory: &Path,
) -> std::result::Result<(Option<HnswCpuIndex>, LoadTiming), LoadFailure> {
    let mut timing = LoadTiming::default();

    macro_rules! stage {
        ($field:ident, $body:expr) => {{
            let started = Instant::now();
            let result = (|| -> Result<_> { $body })();
            timing.$field = started.elapsed();
            result
        }};
    }

    let result = (|| -> Result<Option<HnswCpuIndex>> {
        let available = stage!(prepare, {
            if vectors.is_empty() || !directory.exists() {
                Ok(false)
            } else {
                real_directory(directory).map(|()| true)
            }
        });
        if !available? {
            return Ok(None);
        }

        let path = directory.join(MANIFEST);
        let manifest: Manifest = stage!(manifest, {
            let metadata = std::fs::symlink_metadata(&path)?;
            // Bound parsing by the authoritative rows, including JSON string escaping.
            let max_manifest = vectors.iter().fold(1024usize * 1024, |sum, (eid, _)| {
                sum.saturating_add(eid.len().saturating_mul(6).saturating_add(128))
            });
            ensure!(
                metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && metadata.len() <= max_manifest as u64,
                "invalid graph cache manifest file"
            );
            let manifest: Manifest = serde_json::from_slice(&std::fs::read(&path)?)?;
            ensure!(
                manifest.format == FORMAT && manifest.platform == platform(),
                "incompatible graph cache"
            );
            Ok::<_, anyhow::Error>(manifest)
        })?;
        let expected_fingerprint = stage!(fingerprint, fingerprint(spec, vectors, codebook))?;
        ensure!(
            manifest.fingerprint == expected_fingerprint,
            "graph cache differs from durable vectors"
        );
        ensure!(
            manifest.ids.len() == vectors.len()
                && manifest.points >= vectors.len()
                && manifest.points <= vectors.len().saturating_mul(2),
            "invalid cached graph population"
        );
        ensure!(
            manifest.next_id >= manifest.points && manifest.next_id < usize::MAX,
            "invalid cached allocator"
        );

        stage!(payload_hash, {
            for (name, expected_size, expected_hash) in [
                (
                    "graph.hnsw.graph",
                    manifest.graph_bytes,
                    &manifest.graph_sha256,
                ),
                (
                    "graph.hnsw.data",
                    manifest.data_bytes,
                    &manifest.data_sha256,
                ),
            ] {
                let (size, hash) = file_hash(&directory.join(name), false)?;
                ensure!(
                    size == expected_size && &hash == expected_hash,
                    "graph cache payload checksum mismatch"
                );
            }
            Ok::<_, anyhow::Error>(())
        })?;

        let (store, eid_to_id, id_to_eid, expected_points) = stage!(materialize, {
            let mut store = VectorStore::new(spec);
            store.codebook = codebook;
            let mut ordered: Vec<_> = vectors.iter().collect();
            ordered.sort_by(|left, right| left.0.cmp(&right.0));
            let mut eid_to_id = HashMap::with_capacity(vectors.len());
            let mut id_to_eid = HashMap::with_capacity(vectors.len());
            let mut expected_points = HashMap::with_capacity(vectors.len());
            for ((eid, vector), (cached_eid, id)) in ordered.into_iter().zip(&manifest.ids) {
                ensure!(
                    eid == cached_eid && *id < manifest.next_id,
                    "graph cache identity mismatch"
                );
                ensure!(
                    id_to_eid.insert(*id, eid.clone()).is_none(),
                    "duplicate cached graph identity"
                );
                ensure!(
                    eid_to_id.insert(eid.clone(), *id).is_none(),
                    "duplicate durable vector identity"
                );
                store.put(eid, vector)?;
                let decoded = store
                    .get_decoded(eid)
                    .context("cached store vector missing")?;
                expected_points.insert(
                    *id,
                    if spec.metric == VectorMetric::Cosine {
                        normalize_unit_safe(&decoded)
                    } else {
                        decoded
                    },
                );
            }
            Ok::<_, anyhow::Error>((store, eid_to_id, id_to_eid, expected_points))
        })?;
        let owned = stage!(deserialize, {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                OwnedGraph::try_new(MutBorrow::new(HnswIo::new(directory, BASENAME)), |owner| {
                    let loader = owner.borrow_mut();
                    Ok::<_, anyhow::Error>(match spec.metric {
                        VectorMetric::L2 => LoadedGraph::L2(loader.load_hnsw::<f32, DistL2>()?),
                        VectorMetric::Cosine => {
                            LoadedGraph::Cosine(loader.load_hnsw::<f32, DistDot>()?)
                        }
                        VectorMetric::Dot => LoadedGraph::Dot(loader.load_hnsw::<f32, DistDot>()?),
                    })
                })
            }))
            .map_err(|_| anyhow!("optional HNSW loader rejected malformed graph"))?
        })?;
        stage!(validate, {
            owned.with_dependent(|_, graph| match graph {
                LoadedGraph::L2(h) => {
                    validate_points(h, manifest.points, manifest.next_id, &expected_points)
                }
                LoadedGraph::Cosine(h) | LoadedGraph::Dot(h) => {
                    validate_points(h, manifest.points, manifest.next_id, &expected_points)
                }
            })
        })?;
        Ok(Some(HnswCpuIndex {
            inner: RwLock::new(HnswCpuInner {
                store,
                eid_to_id,
                id_to_eid,
                next_id: manifest.next_id,
                hnsw: HnswBackend::Cached(Box::new(owned)),
                ef_search: hnsw_search_ef(),
            }),
            exact_scan_fallbacks: AtomicU64::new(0),
        }))
    })();

    result
        .map(|index| (index, timing))
        .map_err(|error| LoadFailure { error, timing })
}

fn validate_points<D: hnsw_rs::anndists::dist::Distance<f32> + Send + Sync>(
    graph: &Hnsw<'_, f32, D>,
    points: usize,
    next_id: usize,
    expected: &HashMap<usize, Vec<f32>>,
) -> Result<()> {
    ensure!(
        graph.get_nb_point() == points,
        "graph cache point count mismatch"
    );
    ensure!(
        graph.get_max_nb_connection() as usize == HNSW_MAX_NB_CONNECTION
            && graph.get_ef_construction() == HNSW_EF_CONSTRUCTION,
        "cached graph construction quality differs"
    );
    let mut seen = std::collections::HashSet::with_capacity(expected.len());
    for point in graph.get_point_indexation() {
        ensure!(
            point.get_origin_id() < next_id,
            "cached allocator overlaps an existing graph node"
        );
        if let Some(vector) = expected.get(&point.get_origin_id()) {
            ensure!(
                seen.insert(point.get_origin_id()),
                "duplicate graph point identity"
            );
            ensure!(
                point.get_v().len() == vector.len()
                    && point
                        .get_v()
                        .iter()
                        .zip(vector)
                        .all(|(a, b)| a.to_bits() == b.to_bits()),
                "cached graph vector differs from durable store"
            );
        }
    }
    ensure!(
        seen.len() == expected.len(),
        "cached graph lost a live identity"
    );
    Ok(())
}
