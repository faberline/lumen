use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::tests::{normalize, rand_vec, spec};
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorSpec};

#[test]
fn snapshot_round_trip_preserves_neighbours() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(13);
    let dim = 32u32;
    let s = spec(dim, VectorMetric::L2, None);
    let idx = HnswCpuIndex::new(s);
    for i in 0..50 {
        let v = rand_vec(&mut rng, dim as usize);
        idx.add(&format!("e{i}"), &v).unwrap();
    }
    // Use a stored vector as the query so the top-1 neighbour is an
    // exact match — that ranking is stable across the two
    // independently-built (approximate) HNSW graphs.
    let q = idx.inner.read().unwrap().store.get_decoded("e25").unwrap();
    let before = idx.search_knn(&q, 5).unwrap();

    let (vecs, cb) = idx.dump_for_snapshot().unwrap();

    // Invariant 1 (deterministic): snapshot preserves the exact set
    // of stored vectors. This is the real durability contract — the
    // approximate graph is rebuilt, but no vector is lost or altered.
    let restored = HnswCpuIndex::restore(s, vecs.clone(), cb).unwrap();
    assert_eq!(restored.len(), idx.len());
    let (vecs2, _) = restored.dump_for_snapshot().unwrap();
    let mut a: Vec<_> = vecs.iter().map(|(e, v)| (e.clone(), v.clone())).collect();
    let mut b: Vec<_> = vecs2.iter().map(|(e, v)| (e.clone(), v.clone())).collect();
    a.sort_by(|x, y| x.0.cmp(&y.0));
    b.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(
        a, b,
        "snapshot→restore must preserve every (eid, vector) exactly"
    );

    // Invariant 2 (robust): the exact-match query still tops kNN
    // after restore.
    let after = restored.search_knn(&q, 5).unwrap();
    assert_eq!(before[0].0, "e25");
    assert_eq!(after[0].0, "e25", "exact-match neighbour survives restore");
}

// -----------------------------------------------------------------
// Phase 2k-1: composed base-segment + live-tail + tombstone model for
// the flat-cpu index. After a reopen-from-segment the base vectors live
// ONLY on the mmap; a TAIL of adds and a mix of base+tail DELETEs must
// compose so kNN is byte-identical to an in-RAM oracle that ran the same
// ops. This is the direct proof that the three pieces compose correctly.
// -----------------------------------------------------------------
#[test]
fn reopen_base_seg_plus_tail_plus_tombstone_equals_inram_oracle() {
    use rand::SeedableRng;

    fn flat_spec(dim: u32, metric: VectorMetric) -> VectorSpec {
        VectorSpec {
            dim,
            metric,
            backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
            quantize: None,
        }
    }

    for metric in [VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot] {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xBADCAFE ^ metric as u64);
        let dim = 12u32;
        let s = flat_spec(dim, metric);

        // 30 BASE vectors. (Dot wants unit-norm inputs.)
        let mk = |rng: &mut rand::rngs::StdRng| -> Vec<f32> {
            let raw = rand_vec(rng, dim as usize);
            if matches!(metric, VectorMetric::Dot) {
                normalize(raw)
            } else {
                raw
            }
        };
        let base_idx = FlatCpuIndex::new(s);
        let mut all: Vec<(String, Vec<f32>)> = Vec::new();
        for i in 0..30usize {
            let v = mk(&mut rng);
            base_idx.add(&format!("b{i}"), &v).unwrap();
            all.push((format!("b{i}"), v));
        }

        // SEAL the base to a segment, then REOPEN from it: the base vectors are
        // now ONLY on the mmap (open_from_segment does NOT store.put them).
        let dir = tempfile::tempdir().unwrap();
        let seg_path = dir.path().join("emb.lseg");
        let row_eids = base_idx
            .seal_to_segment_prod(&seg_path)
            .unwrap()
            .expect("flat-cpu seal returns row eids");
        let reader = std::sync::Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&seg_path).unwrap(),
        );
        let reopened = FlatCpuIndex::open_from_segment(s, reader, row_eids).unwrap();

        // The store must NOT hold the base vectors (they live on the mmap) —
        // this is the RAM-bound invariant. `len` still reports all 30 (live).
        {
            let inner = reopened.inner.lock().unwrap();
            assert_eq!(
                inner.store.len(),
                0,
                "{metric:?}: reopen must NOT re-store base vectors"
            );
            let flat = inner.flat.as_ref().unwrap();
            assert!(flat.seg.is_some(), "{metric:?}: segment attached");
            assert_eq!(flat.n_base, 30, "{metric:?}: all 30 rows are base");
            assert!(flat.data.is_empty(), "{metric:?}: empty tail after reopen");
        }
        assert_eq!(reopened.len(), 30, "{metric:?}: reopened live count");

        // ADD a TAIL of 10 vectors (appended in `data`, base stays on mmap).
        let oracle = FlatCpuIndex::new(s);
        for (eid, v) in &all {
            oracle.add(eid, v).unwrap();
        }
        for i in 0..10usize {
            let v = mk(&mut rng);
            let eid = format!("t{i}");
            reopened.add(&eid, &v).unwrap();
            oracle.add(&eid, &v).unwrap();
            all.push((eid, v));
        }
        assert_eq!(reopened.len(), 40, "{metric:?}: base 30 + tail 10");

        // DELETE a mix: some BASE rows (on the mmap) and some TAIL rows.
        for eid in ["b3", "b17", "b29", "t0", "t7"] {
            assert!(reopened.remove(eid).unwrap(), "{metric:?}: {eid} was live");
            assert!(oracle.remove(eid).unwrap());
        }
        // Double-remove of a base id is a no-op (already tombstoned).
        assert!(
            !reopened.remove("b3").unwrap(),
            "{metric:?}: double-remove is no-op"
        );
        assert_eq!(reopened.len(), 35, "{metric:?}: 40 - 5 deleted");

        // kNN must be BYTE-IDENTICAL to the in-RAM oracle: same eids, same order,
        // same f32 score bits — across a battery of probes (including exact-match
        // probes that land on base, tail, and deleted rows).
        let mut probes: Vec<Vec<f32>> = vec![all[5].1.clone(), all[35].1.clone(), all[3].1.clone()];
        for _ in 0..6 {
            probes.push(mk(&mut rng));
        }
        for (pi, q) in probes.iter().enumerate() {
            for k in [1usize, 5, 12, 40] {
                let a = reopened.search_knn(q, k).unwrap();
                let b = oracle.search_knn(q, k).unwrap();
                let ab: Vec<(String, u32)> =
                    a.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect();
                let bb: Vec<(String, u32)> =
                    b.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect();
                assert_eq!(
                    ab, bb,
                    "{metric:?}: probe {pi} k={k} kNN diverged from in-RAM oracle (base-seg + tail + tombstone compose broke)"
                );
                // A deleted id must never appear.
                for (e, _) in &a {
                    assert!(
                        !["b3", "b17", "b29", "t0", "t7"].contains(&e.as_str()),
                        "{metric:?}: deleted id {e} leaked into kNN"
                    );
                }
            }
        }

        // Snapshot of the reopened (sealed) index must read every LIVE row off
        // the mmap+tail (NOT store) and match the oracle's live set.
        let (mut snap, _) = reopened.dump_for_snapshot().unwrap();
        let (mut osnap, _) = oracle.dump_for_snapshot().unwrap();
        snap.sort_by(|a, b| a.0.cmp(&b.0));
        osnap.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            snap, osnap,
            "{metric:?}: snapshot of reopened index must match oracle live set"
        );
    }
}

#[test]
fn remove_drops_vector_from_subsequent_search() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(17);
    let idx = HnswCpuIndex::new(spec(16, VectorMetric::L2, None));
    for i in 0..20 {
        let v = rand_vec(&mut rng, 16);
        idx.add(&format!("e{i}"), &v).unwrap();
    }
    // The vector at e7 is its own top neighbour; remove it.
    let q_idx = 7;
    let q_eid = format!("e{q_idx}");
    let q = idx.inner.read().unwrap().store.get_decoded(&q_eid).unwrap();
    assert!(idx.remove(&q_eid).unwrap());
    let hits = idx.search_knn(&q, 5).unwrap();
    assert!(
        !hits.iter().any(|(e, _)| e == &q_eid),
        "removed eid {q_eid} still in {hits:?}"
    );
}
