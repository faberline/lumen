//! HnswCpuIndex as a VectorIndex backend: adds and removes under the graph's
//! write lock with the per-thread timing handoff the committed apply reads,
//! filtered kNN with its exact fallback, and sealing the vectors to a
//! checkpoint segment.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::index::domain::vector::distance::distance;
use crate::index::domain::vector::hnsw_cpu_index::{
    HnswBackend, HnswCpuIndex, HNSW_DEFAULT_MAX_ELEMENTS,
};
use crate::index::domain::vector::quantize::{decode_sq, ScalarCodebook};
use crate::index::domain::vector::VectorIndex;
#[cfg(test)]
use crate::index::domain::vector::HNSW_CHECKPOINT_FULL_SCANS;

thread_local! {
    // Committed apply serializes live HNSW mutations under the Engine writer
    // lock. A thread-local handoff keeps timing state out of the index and
    // lets the caller publish it only after that outer lock drops.
    static HNSW_LAST_WRITE_LOCK_TIMING: std::cell::RefCell<Option<(Duration, Duration)>> =
        const { std::cell::RefCell::new(None) };
    /// A rebuild is a rare add-path event. Keep its timing separate from the
    /// normal add interval so the caller can identify it without new shared
    /// state on the index.
    static HNSW_LAST_GRAPH_REBUILD_TIMING: std::cell::RefCell<Option<Duration>> =
        const { std::cell::RefCell::new(None) };
}

impl VectorIndex for HnswCpuIndex {
    fn save_graph_cache(&self, directory: &std::path::Path) -> Result<bool> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        crate::index::infrastructure::vector::graph_cache::save(&inner, directory)
    }
    fn checkpoint_vector(&self, external_id: &str) -> Result<Option<Vec<f32>>> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        Ok(inner.store.get_decoded(external_id))
    }

    fn checkpoint_codebook_for_preparation(&self) -> Result<Option<ScalarCodebook>> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        Ok(inner.store.codebook)
    }

    fn add(&self, external_id: &str, vector: &[f32]) -> Result<()> {
        HNSW_LAST_WRITE_LOCK_TIMING.with(|timing| *timing.borrow_mut() = None);
        HNSW_LAST_GRAPH_REBUILD_TIMING.with(|timing| *timing.borrow_mut() = None);
        let write_wait_started = Instant::now();
        let mut inner = self
            .inner
            .write()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        let write_wait = write_wait_started.elapsed();
        let write_hold_started = Instant::now();
        let mut graph_rebuild = None;
        let result = (|| {
            if vector.len() != inner.store.spec.dim as usize {
                bail!(
                    "vector dim mismatch on add: expected {}, got {}",
                    inner.store.spec.dim,
                    vector.len()
                );
            }
            // Replace path: if the eid already has a vector, allocate a
            // new internal id and orphan the old one. hnsw_rs 0.3 has no
            // public "remove" — orphaning is the documented workaround.
            // The forward map decides what is reachable.
            let id = inner.next_id;
            inner.next_id += 1;
            inner.store.put(external_id, vector)?;
            // We always feed the *decoded* vector to HNSW so the same graph
            // works whether SQ is on or off. The codebook only affects
            // storage and recall, not the graph topology.
            if inner.store.codebook.is_some() {
                let decoded = inner
                    .store
                    .get_decoded(external_id)
                    .ok_or_else(|| anyhow!("just-inserted vector vanished"))?;
                inner.hnsw.insert(&decoded, id);
            } else {
                inner.hnsw.insert(vector, id);
            }
            if let Some(old_id) = inner.eid_to_id.insert(external_id.to_string(), id) {
                inner.id_to_eid.remove(&old_id);
            }
            inner.id_to_eid.insert(id, external_id.to_string());

            let live_len = inner.store.len();
            if inner.next_id >= 2 * live_len && live_len > 0 {
                let rebuild_started = Instant::now();
                let fresh_hnsw =
                    HnswBackend::new(inner.store.spec.metric, HNSW_DEFAULT_MAX_ELEMENTS);
                inner.eid_to_id.clear();
                inner.id_to_eid.clear();
                inner.next_id = 0;
                let mut live_vecs: Vec<(String, Vec<f32>)> = inner.store.iter_decoded().collect();
                live_vecs.sort_by(|a, b| a.0.cmp(&b.0));
                for (eid, v) in live_vecs {
                    let new_id = inner.next_id;
                    inner.next_id += 1;
                    fresh_hnsw.insert(&v, new_id);
                    inner.eid_to_id.insert(eid.clone(), new_id);
                    inner.id_to_eid.insert(new_id, eid);
                }
                inner.hnsw = fresh_hnsw;
                graph_rebuild = Some(rebuild_started.elapsed());
            }
            Ok(())
        })();
        drop(inner);
        let write_hold = write_hold_started.elapsed();
        HNSW_LAST_WRITE_LOCK_TIMING.with(|timing| {
            *timing.borrow_mut() = Some((write_wait, write_hold));
        });
        HNSW_LAST_GRAPH_REBUILD_TIMING.with(|timing| {
            *timing.borrow_mut() = graph_rebuild;
        });
        result
    }

    fn take_hnsw_write_lock_timing(&self) -> Option<(Duration, Duration)> {
        HNSW_LAST_WRITE_LOCK_TIMING.with(|timing| timing.borrow_mut().take())
    }

    fn take_hnsw_graph_rebuild_timing(&self) -> Option<Duration> {
        HNSW_LAST_GRAPH_REBUILD_TIMING.with(|timing| timing.borrow_mut().take())
    }

    fn remove(&self, external_id: &str) -> Result<bool> {
        HNSW_LAST_WRITE_LOCK_TIMING.with(|timing| *timing.borrow_mut() = None);
        let write_wait_started = Instant::now();
        let mut inner = self
            .inner
            .write()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        let write_wait = write_wait_started.elapsed();
        let write_hold_started = Instant::now();
        let removed = inner.store.drop(external_id);
        if let Some(id) = inner.eid_to_id.remove(external_id) {
            inner.id_to_eid.remove(&id);
        }
        drop(inner);
        let write_hold = write_hold_started.elapsed();
        HNSW_LAST_WRITE_LOCK_TIMING.with(|timing| {
            *timing.borrow_mut() = Some((write_wait, write_hold));
        });
        Ok(removed)
    }

    fn search_knn_filtered(
        &self,
        query: &[f32],
        k: usize,
        allow: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<(String, f32)>> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        if query.len() != inner.store.spec.dim as usize {
            bail!(
                "kNN query dim mismatch: expected {}, got {}",
                inner.store.spec.dim,
                query.len()
            );
        }
        let n = inner.store.len();
        if n == 0 || k == 0 {
            return Ok(Vec::new());
        }
        // hnsw_rs exposes no mid-traversal filter hook and does not support node
        // removal. Bound graph expansion by the configured search beam or the
        // initial over-fetch, whichever is larger. Asking the graph for the
        // whole corpus also raises its traversal ef to corpus size, while an
        // exact filtered scan already provides the required fallback. In
        // particular, repeating a traversal cannot certify an unresolved
        // nearest orphan, so that case goes directly to the exact live store.
        let graph_len = inner.next_id;
        let mut pool = k.saturating_mul(5).min(graph_len);
        let ef = inner.ef_search;
        let pool_limit = pool.max(ef).min(graph_len);
        loop {
            let raw = inner.hnsw.search(query, pool, ef);
            let mut out: Vec<(String, f32)> = Vec::with_capacity(k);
            let mut has_unresolved_orphan = false;
            for (id, dist) in &raw {
                let Some(eid) = inner.id_to_eid.get(id) else {
                    if out.len() < k {
                        has_unresolved_orphan = true;
                    }
                    continue; // orphaned by a replace
                };
                if !allow(eid) {
                    continue;
                }
                out.push((eid.clone(), -dist));
                if out.len() == k {
                    break;
                }
            }

            if out.len() == k && !has_unresolved_orphan {
                return Ok(out);
            }

            if has_unresolved_orphan || pool >= pool_limit {
                break;
            }
            pool = pool.saturating_mul(2).min(pool_limit);
        }

        // When the bounded graph search or an orphan prevents a conclusive
        // approximate answer, scan the exact live store instead of returning
        // a short or degraded result. Membership and scoring are unchanged.
        self.exact_scan_fallbacks.fetch_add(1, Ordering::Relaxed);
        let metric = inner.store.spec.metric;
        // The read guard pins the store until the selected IDs are copied.
        // Borrow raw vectors and candidate IDs. For SQ, reject the ID before
        // decoding its vector; only allowed values need a temporary f32 view.
        let mut cand: Vec<(&str, f32)> = if let Some(codebook) = inner.store.codebook.as_ref() {
            inner
                .store
                .encoded
                .iter()
                .filter(|(eid, _)| allow(eid))
                .map(|(eid, bytes)| {
                    let vector = decode_sq(bytes, codebook);
                    (eid.as_str(), -distance(metric, query, &vector))
                })
                .collect()
        } else {
            inner
                .store
                .raw
                .iter()
                .filter(|(eid, _)| allow(eid))
                .map(|(eid, vector)| (eid.as_str(), -distance(metric, query, vector)))
                .collect()
        };
        let want = k.min(cand.len());
        if want > 0 && want < cand.len() {
            cand.select_nth_unstable_by(want - 1, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            cand.truncate(want);
        }
        cand.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(cand
            .into_iter()
            .map(|(eid, score)| (eid.to_owned(), score))
            .collect())
    }

    fn len(&self) -> usize {
        self.inner.read().map(|i| i.store.len()).unwrap_or(0)
    }

    fn dump_for_snapshot(&self) -> Result<(Vec<(String, Vec<f32>)>, Option<ScalarCodebook>)> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        let vectors: Vec<(String, Vec<f32>)> = inner.store.iter_decoded().collect();
        Ok((vectors, inner.store.codebook))
    }

    /// PRODUCTION seal (issue #3951): persist the HNSW corpus into the SAME
    /// columnar vector-segment format [`FlatCpuIndex::seal_to_segment_prod`]
    /// writes — decoded `f32` rows in row order, plus the row→eid mapping the
    /// caller persists as the `<field>.eids.lseg` sidecar.
    ///
    /// This is not optional: `Collection::open_from_segments` requires the
    /// `<field>.eids.lseg` sidecar unconditionally for any `Vector` field, so
    /// with the inherited no-op default a default-backend vector field wrote
    /// neither file, and `SegmentRdbStore::save_inner`'s pre-commit
    /// verification reopen (and, unpatched, every later real restart) failed
    /// for any collection that had one.
    ///
    /// The GRAPH is deliberately not what gets persisted — only the vectors
    /// are. `HnswCpuIndex::open_from_segment` reads them back off the mmap and
    /// re-inserts them, paying the build cost once at reopen, so the field
    /// answers kNN with the backend its schema declares on both sides of a
    /// restart. Persisting the graph itself would pin this index's internal
    /// layout into the on-disk format for no behavioural gain.
    ///
    /// No scalar quantization on the wire, matching the flat seal contract:
    /// the segment stores decoded `f32`, so recovery reads plain rows.
    ///
    /// [`FlatCpuIndex::seal_to_segment_prod`]: crate::index::domain::vector::flat_cpu_index::FlatCpuIndex::seal_to_segment_prod
    fn seal_to_segment_prod(&self, path: &std::path::Path) -> Result<Option<Vec<String>>> {
        self.seal_checkpoint(path, None)
    }

    fn seal_to_segment_prod_at(
        &self,
        path: &std::path::Path,
        sequence: u64,
    ) -> Result<Option<Vec<String>>> {
        self.seal_checkpoint(path, Some(sequence))
    }
}

impl HnswCpuIndex {
    fn seal_checkpoint(
        &self,
        path: &std::path::Path,
        sequence: Option<u64>,
    ) -> Result<Option<Vec<String>>> {
        let inner = self
            .inner
            .read()
            .map_err(|_| anyhow!("hnsw lock poisoned"))?;
        let dim = inner.store.spec.dim as usize;
        #[cfg(test)]
        HNSW_CHECKPOINT_FULL_SCANS.with(|count| count.set(count.get() + 1));
        let rows: Vec<(String, Vec<f32>)> = inner.store.iter_decoded().collect();
        drop(inner);
        let n = rows.len();
        // Split the pairs into the two shapes the writer and the caller each
        // need, without cloning the eids: the vectors are borrowed for the
        // duration of the write and the Strings are moved straight out.
        let mut row_eids: Vec<String> = Vec::with_capacity(n);
        let mut row_vecs: Vec<Vec<f32>> = Vec::with_capacity(n);
        for (eid, v) in rows {
            row_eids.push(eid);
            row_vecs.push(v);
        }
        let vectors: Vec<Option<&[f32]>> = row_vecs.iter().map(|v| Some(v.as_slice())).collect();
        crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
            path,
            sequence.unwrap_or(n as u64),
            dim,
            &vectors,
        )?;
        // Reopen what was just written, in every build. Once the caller commits
        // this checkpoint it drops the in-RAM store, so these bytes become the
        // only copy of the vectors; a segment that cannot be read back has to
        // fail HERE, while the RAM copy is still there to retry from, rather
        // than at the next restart with nothing left to recover. The reopen is
        // one header read against a file the page cache still holds — it is not
        // the cost that would justify compiling it out.
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(path)
            .map_err(|e| {
                anyhow!(
                    "vector segment written to {} could not be read back: {e}",
                    path.display()
                )
            })?;
        if reader.n_docs() as usize != n {
            bail!(
                "vector segment written to {} reopened with {} rows, expected {n}",
                path.display(),
                reader.n_docs()
            );
        }
        Ok(Some(row_eids))
    }
}
