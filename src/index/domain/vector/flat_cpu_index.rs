//! The exact brute-force backend: FlatCpuIndex, the flat buffer whose slots
//! read from the checkpoint base, a delta layer or RAM, and building, sealing
//! and ranking over it.

mod backend;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Result};

use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::domain::vector::vector_store::VectorStore;
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::VectorSpec;

// ---------------------------------------------------------------------------
// Exact CPU brute-force backend (`flat-cpu`)
// ---------------------------------------------------------------------------

/// Exact CPU brute-force kNN. No graph, no build cost: it stores the raw
/// vectors in a contiguous `[N*dim]` buffer and scans them all per query —
/// parallel across rows (rayon) with an auto-vectorized distance kernel. For
/// moderate N this beats both an approximate index's build cost and a
/// single-threaded exact scan (e.g. pgvector's `seqscan`), while giving 100%
/// recall. The flat buffer is cached and rebuilt lazily after a mutation.
pub struct FlatCpuIndex {
    pub(super) inner: Mutex<FlatInner>,
}

pub(super) struct FlatInner {
    pub(super) store: VectorStore,
    pub(super) flat: Option<FlatVecs>,
}

#[derive(Clone, Copy)]
enum FlatLocation {
    Base(u32),
    Layer { layer: u32, row: u32 },
    Ram,
}

struct FlatLayer {
    reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
}

/// A stable EID slot has one current source.  Mmap sources are immutable;
/// `data` contains only current, uncheckpointed decoded payloads.
pub(super) struct FlatVecs {
    pub(super) data: HashMap<u32, Vec<f32>>,
    eids: Vec<String>,
    dim: usize,
    pub(super) seg: Option<Arc<crate::persistence::infrastructure::segment::SegmentReader>>,
    pub(super) n_base: usize,
    tomb: roaring::RoaringBitmap,
    eid_to_row: HashMap<String, u32>,
    locations: Vec<Option<FlatLocation>>,
    layers: Vec<FlatLayer>,
}

impl FlatVecs {
    #[inline]
    fn row(&self, slot: usize) -> &[f32] {
        match self.locations[slot].expect("query selected a deleted vector slot") {
            FlatLocation::Base(row) => self
                .seg
                .as_ref()
                .and_then(|seg| seg.vector_at(row, self.dim))
                .expect("base vector row is present"),
            FlatLocation::Layer { layer, row } => self.layers[layer as usize]
                .reader
                .vector_at(row, self.dim)
                .expect("delta vector row is present"),
            FlatLocation::Ram => self
                .data
                .get(&(slot as u32))
                .map(Vec::as_slice)
                .expect("RAM vector slot is present"),
        }
    }

    fn live_slots(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.eids.len())
            .filter(|slot| !self.tomb.contains(*slot as u32) && self.locations[*slot].is_some())
    }
}

impl FlatCpuIndex {
    pub fn new(spec: VectorSpec) -> Self {
        Self {
            inner: Mutex::new(FlatInner {
                store: VectorStore::new(spec),
                flat: None,
            }),
        }
    }

    pub fn restore(
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
    ) -> Result<Self> {
        let idx = Self::new(spec);
        if codebook.is_some() {
            idx.inner
                .lock()
                .map_err(|_| anyhow!("flat lock poisoned"))?
                .store
                .codebook = codebook;
        }
        for (eid, value) in vectors {
            idx.add(&eid, &value)?;
        }
        Ok(idx)
    }

    fn ensure_flat(inner: &mut FlatInner) {
        if inner.flat.is_some() {
            return;
        }
        let dim = inner.store.spec.dim as usize;
        let mut data = HashMap::with_capacity(inner.store.len());
        let mut eids = Vec::with_capacity(inner.store.len());
        let mut eid_to_row = HashMap::with_capacity(inner.store.len());
        for (slot, (eid, value)) in inner.store.iter_decoded().enumerate() {
            let slot = slot as u32;
            data.insert(slot, value);
            eid_to_row.insert(eid.clone(), slot);
            eids.push(eid);
        }
        let locations = (0..eids.len()).map(|_| Some(FlatLocation::Ram)).collect();
        inner.flat = Some(FlatVecs {
            data,
            eids,
            dim,
            seg: None,
            n_base: 0,
            tomb: roaring::RoaringBitmap::new(),
            eid_to_row,
            locations,
            layers: Vec::new(),
        });
    }

    #[inline]
    fn live_len(inner: &FlatInner) -> usize {
        inner
            .flat
            .as_ref()
            .map(|flat| flat.live_slots().count())
            .unwrap_or_else(|| inner.store.len())
    }

    fn seal_checkpoint(
        &self,
        path: &std::path::Path,
        sequence: Option<u64>,
    ) -> Result<Option<Vec<String>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| anyhow!("flat lock poisoned"))?;
        Self::ensure_flat(&mut inner);
        let flat = inner.flat.as_mut().unwrap();
        let rows: Vec<(String, Vec<f32>)> = flat
            .live_slots()
            .map(|slot| (flat.eids[slot].clone(), flat.row(slot).to_vec()))
            .collect();
        let refs: Vec<Option<&[f32]>> = rows
            .iter()
            .map(|(_, value)| Some(value.as_slice()))
            .collect();
        crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
            path,
            sequence.unwrap_or(rows.len() as u64),
            flat.dim,
            &refs,
        )?;
        let reader =
            Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(path)?);
        let eids: Vec<String> = rows.into_iter().map(|(eid, _)| eid).collect();
        flat.data.clear();
        flat.eid_to_row = eids
            .iter()
            .enumerate()
            .map(|(i, eid)| (eid.clone(), i as u32))
            .collect();
        flat.eids = eids;
        flat.locations = (0..flat.eids.len())
            .map(|row| Some(FlatLocation::Base(row as u32)))
            .collect();
        flat.tomb.clear();
        flat.layers.clear();
        flat.n_base = flat.eids.len();
        flat.seg = Some(reader);
        let eids = flat.eids.clone();
        inner.store.raw.clear();
        inner.store.encoded.clear();
        Ok(Some(eids))
    }

    fn seal_to_segment_prod(&self, path: &std::path::Path) -> Result<Option<Vec<String>>> {
        self.seal_checkpoint(path, None)
    }

    pub fn open_from_segment(
        spec: VectorSpec,
        seg: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        row_eids: Vec<String>,
    ) -> Result<Self> {
        let dim = spec.dim as usize;
        if seg.n_docs() as usize != row_eids.len() {
            bail!("vector sidecar row count does not match segment");
        }
        let eid_to_row = row_eids
            .iter()
            .enumerate()
            .map(|(i, eid)| (eid.clone(), i as u32))
            .collect();
        let n = row_eids.len();
        let absent: roaring::RoaringBitmap = (0..n as u32)
            .filter(|row| seg.vector_at(*row, dim).is_none())
            .collect();
        let locations = (0..n as u32)
            .map(|row| (!absent.contains(row)).then_some(FlatLocation::Base(row)))
            .collect();
        Ok(Self {
            inner: Mutex::new(FlatInner {
                store: VectorStore::new(spec),
                flat: Some(FlatVecs {
                    data: HashMap::new(),
                    eids: row_eids,
                    dim,
                    seg: Some(seg),
                    n_base: n,
                    tomb: absent,
                    eid_to_row,
                    locations,
                    layers: Vec::new(),
                }),
            }),
        })
    }

    fn topk(mut candidates: Vec<(usize, f32)>, k: usize, eids: &[String]) -> Vec<(String, f32)> {
        let want = k.min(candidates.len());
        if want > 0 && want < candidates.len() {
            candidates.select_nth_unstable_by(want - 1, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates.truncate(want);
        }
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates
            .into_iter()
            .map(|(slot, score)| (eids[slot].clone(), score))
            .collect()
    }

    fn set_ram(inner: &mut FlatInner, eid: &str, vector: &[f32]) -> Result<()> {
        let dim = inner.store.spec.dim as usize;
        if vector.len() != dim {
            bail!(
                "vector dim mismatch: expected {}, got {}",
                dim,
                vector.len()
            );
        }
        inner.store.put(eid, vector)?;
        let decoded = inner
            .store
            .get_decoded(eid)
            .unwrap_or_else(|| vector.to_vec());
        Self::ensure_flat(inner);
        let flat = inner.flat.as_mut().unwrap();
        let slot = *flat.eid_to_row.entry(eid.to_owned()).or_insert_with(|| {
            let slot = flat.eids.len() as u32;
            flat.eids.push(eid.to_owned());
            flat.locations.push(None);
            slot
        });
        flat.data.insert(slot, decoded);
        flat.locations[slot as usize] = Some(FlatLocation::Ram);
        flat.tomb.remove(slot);
        Ok(())
    }
}

impl std::fmt::Debug for FlatCpuIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatCpuIndex")
            .field("len", &self.len())
            .finish()
    }
}
