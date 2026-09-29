//! Optional HNSW graph acceleration. Checkpoint vectors remain authoritative.

pub(in crate::index) mod load;

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use hnsw_rs::anndists::dist::{DistDot, DistL2};
use hnsw_rs::{api::AnnT, hnsw::Hnsw, hnswio::HnswIo};
use self_cell::{self_cell, MutBorrow};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::index::domain::vector::distance::normalize_unit_safe;
use crate::index::domain::vector::hnsw_cpu_index::{
    HnswBackend, HnswCpuIndex, HnswCpuInner, HNSW_EF_CONSTRUCTION, HNSW_MAX_LAYER,
    HNSW_MAX_NB_CONNECTION,
};
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::infrastructure::vector::graph_cache::load::load_timed;
use crate::shared_kernel::types::schema::VectorSpec;

const BASENAME: &str = "graph";
const MANIFEST: &str = "manifest.json";
const FORMAT: &str = "lumen-hnsw-cache-v1/hnsw_rs-0.3.4-dump-v3";

/// Bounded per-field timings for an optional graph-cache restore attempt.
///
/// These are diagnostic-only. They deliberately retain every cache-integrity
/// check so a large cold start can identify its active phase without changing
/// recovery authority.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::index) struct LoadTiming {
    pub(in crate::index) prepare: Duration,
    pub(in crate::index) fingerprint: Duration,
    pub(in crate::index) manifest: Duration,
    pub(in crate::index) payload_hash: Duration,
    pub(in crate::index) materialize: Duration,
    pub(in crate::index) deserialize: Duration,
    pub(in crate::index) validate: Duration,
}

#[derive(Debug)]
pub(in crate::index) struct LoadFailure {
    pub(in crate::index) error: anyhow::Error,
    pub(in crate::index) timing: LoadTiming,
}

enum LoadedGraph<'a> {
    L2(Hnsw<'a, f32, DistL2>),
    Cosine(Hnsw<'a, f32, DistDot>),
    Dot(Hnsw<'a, f32, DistDot>),
}

pub(in crate::index) fn load(
    spec: VectorSpec,
    vectors: &[(String, Vec<f32>)],
    codebook: Option<ScalarCodebook>,
    directory: &Path,
) -> Result<Option<HnswCpuIndex>> {
    load_timed(spec, vectors, codebook, directory)
        .map_err(|failure| failure.error)
        .map(|(index, _)| index)
}

// The loader owns every borrowed mapping for exactly as long as the graph.
// No leaked loader, forged 'static lifetime or new unsafe implementation.
self_cell!(
    pub(in crate::index) struct OwnedGraph {
        owner: MutBorrow<HnswIo>,
        #[not_covariant]
        dependent: LoadedGraph,
    }
);

impl OwnedGraph {
    pub(in crate::index) fn insert(&self, vector: &[f32], id: usize) {
        self.with_dependent(|_, graph| match graph {
            LoadedGraph::L2(h) => h.insert((vector, id)),
            LoadedGraph::Dot(h) => h.insert((vector, id)),
            LoadedGraph::Cosine(h) => h.insert((&normalize_unit_safe(vector), id)),
        });
    }

    pub(in crate::index) fn search(
        &self,
        vector: &[f32],
        k: usize,
        ef: usize,
    ) -> Vec<(usize, f32)> {
        self.with_dependent(|_, graph| {
            let hits = match graph {
                LoadedGraph::L2(h) => h.search(vector, k, ef),
                LoadedGraph::Dot(h) => h.search(vector, k, ef),
                LoadedGraph::Cosine(h) => h.search(&normalize_unit_safe(vector), k, ef),
            };
            hits.into_iter()
                .map(|hit| (hit.d_id, hit.distance))
                .collect()
        })
    }

    fn dump(&self, path: &Path) -> Result<String> {
        self.with_dependent(|_, graph| match graph {
            LoadedGraph::L2(h) => h.file_dump(path, BASENAME),
            LoadedGraph::Dot(h) | LoadedGraph::Cosine(h) => h.file_dump(path, BASENAME),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    platform: String,
    fingerprint: String,
    next_id: usize,
    points: usize,
    ids: Vec<(String, usize)>,
    graph_bytes: u64,
    graph_sha256: String,
    data_bytes: u64,
    data_sha256: String,
}

fn platform() -> String {
    format!(
        "{}-{}-{}-{}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        usize::BITS,
        cfg!(target_endian = "little")
    )
}

fn fingerprint(
    spec: VectorSpec,
    vectors: &[(String, Vec<f32>)],
    codebook: Option<ScalarCodebook>,
) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(FORMAT.as_bytes());
    for setting in [HNSW_MAX_NB_CONNECTION, HNSW_EF_CONSTRUCTION, HNSW_MAX_LAYER] {
        hash.update((setting as u64).to_le_bytes());
    }
    hash.update(serde_json::to_vec(&spec)?);
    if let Some(book) = codebook {
        hash.update([1]);
        hash.update(book.min.to_bits().to_le_bytes());
        hash.update(book.max.to_bits().to_le_bytes());
        hash.update((book.dim as u64).to_le_bytes());
    } else {
        hash.update([0]);
    }
    let mut ordered: Vec<_> = vectors.iter().collect();
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    hash.update((ordered.len() as u64).to_le_bytes());
    for (eid, vector) in ordered {
        hash.update((eid.len() as u64).to_le_bytes());
        hash.update(eid.as_bytes());
        hash.update((vector.len() as u64).to_le_bytes());
        for value in vector {
            hash.update(value.to_bits().to_le_bytes());
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn real_directory(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "cache directory is not a real directory"
    );
    Ok(())
}

fn file_hash(path: &Path, sync: bool) -> Result<(u64, String)> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "cache payload is not a regular file"
    );
    let mut file = OpenOptions::new().read(true).write(sync).open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let size = file.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    if sync {
        file.sync_all()?;
    }
    Ok((metadata.len(), format!("{:x}", hash.finalize())))
}

fn graph_points(backend: &HnswBackend) -> usize {
    match backend {
        HnswBackend::L2(h) => h.get_nb_point(),
        HnswBackend::Cosine(h) | HnswBackend::Dot(h) => h.get_nb_point(),
        HnswBackend::Cached(h) => h.with_dependent(|_, graph| match graph {
            LoadedGraph::L2(h) => h.get_nb_point(),
            LoadedGraph::Cosine(h) | LoadedGraph::Dot(h) => h.get_nb_point(),
        }),
    }
}

pub(in crate::index) fn save(inner: &HnswCpuInner, directory: &Path) -> Result<bool> {
    if inner.store.len() == 0 {
        return Ok(false);
    }
    real_directory(directory)?;
    let vectors: Vec<_> = inner.store.iter_decoded().collect();
    let points = graph_points(&inner.hnsw);
    // Large orphan-only graphs are an optional cache miss, not a recovery
    // requirement. The ordinary authoritative rebuild compacts them.
    if points > vectors.len().saturating_mul(2) {
        return Ok(false);
    }
    // Only this private staging namespace belongs to the cache writer. Never
    // follow links or remove unrelated entries while recovering a partial dump.
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let owned = name
            .to_str()
            .and_then(|name| name.strip_prefix(".graph-stage-"))
            .is_some_and(|suffix| {
                suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
            });
        if owned && entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        }
    }
    let staging = tempfile::Builder::new()
        .prefix(".graph-stage-")
        .tempdir_in(directory)?;
    let name = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match &inner.hnsw {
        HnswBackend::L2(h) => h.file_dump(staging.path(), BASENAME),
        HnswBackend::Dot(h) | HnswBackend::Cosine(h) => h.file_dump(staging.path(), BASENAME),
        HnswBackend::Cached(h) => h.dump(staging.path()),
    }))
    .map_err(|_| anyhow!("optional HNSW dump panicked"))??;
    ensure!(name == BASENAME, "unexpected graph dump basename");
    let (graph_bytes, graph_sha256) = file_hash(&staging.path().join("graph.hnsw.graph"), true)?;
    let (data_bytes, data_sha256) = file_hash(&staging.path().join("graph.hnsw.data"), true)?;
    let mut ids: Vec<_> = inner
        .eid_to_id
        .iter()
        .map(|(eid, id)| (eid.clone(), *id))
        .collect();
    ids.sort_by(|left, right| left.0.cmp(&right.0));
    let manifest = Manifest {
        format: FORMAT.into(),
        platform: platform(),
        fingerprint: fingerprint(inner.store.spec, &vectors, inner.store.codebook)?,
        next_id: inner.next_id,
        points,
        ids,
        graph_bytes,
        graph_sha256,
        data_bytes,
        data_sha256,
    };
    // Rename replaces directory entries, never writes through an old symlink.
    // A crash between payloads leaves mismatched old checksums and a cache miss.
    for name in ["graph.hnsw.graph", "graph.hnsw.data"] {
        std::fs::rename(staging.path().join(name), directory.join(name))?;
    }
    storage_durable::atomic_write_strict(
        directory.join(MANIFEST),
        &serde_json::to_vec(&manifest)?,
    )?;
    Ok(true)
}

#[cfg(test)]
mod tests;
