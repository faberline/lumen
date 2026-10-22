use crate::index::domain::vector::distance::l2_squared;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::tests::{rand_vec, spec};
use crate::index::domain::vector::{VectorIndex, HNSW_SEARCH_POOLS, VECTOR_STORE_OWNED_SCANS};
use crate::shared_kernel::types::schema::{VectorMetric, VectorQuantize};

#[test]
fn hnsw_add_reports_one_consumable_write_lock_timing() {
    let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    index.add("one", &[1.0, 0.0, 0.0]).unwrap();
    let _timing = index
        .take_hnsw_write_lock_timing()
        .expect("HNSW add must report its lock split");
    assert!(
        index.take_hnsw_write_lock_timing().is_none(),
        "committed apply must not publish the same HNSW add twice"
    );
    assert!(
        index.take_hnsw_graph_rebuild_timing().is_none(),
        "the first HNSW add does not rebuild the graph"
    );
}

#[test]
fn hnsw_rebuild_reports_one_consumable_timing() {
    let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    index.add("one", &[1.0, 0.0, 0.0]).unwrap();
    let _ = index.take_hnsw_write_lock_timing();
    index.remove("one").unwrap();
    let _ = index.take_hnsw_write_lock_timing();
    index.add("one", &[0.0, 1.0, 0.0]).unwrap();

    assert!(
        index.take_hnsw_graph_rebuild_timing().is_some(),
        "replacing the only orphaned HNSW vector rebuilds the graph"
    );
    assert!(
        index.take_hnsw_graph_rebuild_timing().is_none(),
        "committed apply must not publish the same rebuild twice"
    );
}

#[test]
fn hnsw_poisoned_write_operations_clear_stale_lock_timing() {
    fn poison(index: &HnswCpuIndex) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = index.inner.write().expect("fresh HNSW lock");
            panic!("poison HNSW lock for timing test");
        }));
    }

    let add_index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    add_index.add("one", &[1.0, 0.0, 0.0]).unwrap();
    poison(&add_index);
    assert!(add_index.add("two", &[0.0, 1.0, 0.0]).is_err());
    assert!(
        add_index.take_hnsw_write_lock_timing().is_none(),
        "a failed poisoned add must not expose timing from a prior write"
    );

    let remove_index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    remove_index.add("one", &[1.0, 0.0, 0.0]).unwrap();
    poison(&remove_index);
    assert!(remove_index.remove("one").is_err());
    assert!(
        remove_index.take_hnsw_write_lock_timing().is_none(),
        "a failed poisoned remove must not expose timing from a prior write"
    );
}

#[test]
fn hnsw_exact_fallback_does_not_enter_the_owned_store_iterator() {
    for quantize in [None, Some(VectorQuantize::Sq)] {
        let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, quantize));
        index.set_ef_search(32);
        for i in 0..128 {
            index.add(&format!("v{i}"), &[i as f32, 1.0, 0.0]).unwrap();
        }
        VECTOR_STORE_OWNED_SCANS.with(|scans| scans.set(0));
        let before = index.exact_scan_fallbacks();
        assert!(index
            .search_knn_filtered(&[0.0, 1.0, 0.0], 5, &|_| false)
            .unwrap()
            .is_empty());
        assert_eq!(index.exact_scan_fallbacks(), before + 1);
        assert_eq!(
            VECTOR_STORE_OWNED_SCANS.with(|scans| scans.get()),
            0,
            "fallback must filter and score borrowed values, quantize={quantize:?}"
        );
    }
}

#[test]
fn hnsw_nearest_orphans_do_not_repeat_an_inconclusive_graph_search() {
    const N: usize = 256;
    const K: usize = 5;
    let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    // Exhaustive on this small fixture, so topology cannot hide the orphan.
    index.set_ef_search(N + K);
    for i in 0..N {
        index
            .add(&format!("v{i:03}"), &[i as f32, 0.0, 0.0])
            .unwrap();
    }
    for i in 0..K {
        index
            .add(&format!("v{i:03}"), &[(1000 + i) as f32, 0.0, 0.0])
            .unwrap();
    }
    HNSW_SEARCH_POOLS.with(|pools| pools.borrow_mut().clear());
    let before = index.exact_scan_fallbacks();
    let hits = index
        .search_knn_filtered(&[0.0, 0.0, 0.0], K, &|_| true)
        .unwrap();
    let expected: Vec<_> = (K..2 * K)
        .map(|i| (format!("v{i:03}"), -(i as f32)))
        .collect();
    assert_eq!(hits, expected);
    assert_eq!(index.exact_scan_fallbacks(), before + 1);
    let pools = HNSW_SEARCH_POOLS.with(|pools| pools.borrow().clone());
    assert_eq!(
        pools.len(),
        1,
        "an unresolved nearest orphan must go directly to the exact fallback: {pools:?}"
    );
}

#[test]
fn hnsw_selective_search_bounds_graph_pool_before_exact_fallback() {
    const N: usize = 512;
    const K: usize = 5;
    const EF: usize = 64;
    let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    index.set_ef_search(EF);
    for i in 0..N {
        index
            .add(&format!("v{i:03}"), &[i as f32, 0.0, 0.0])
            .unwrap();
    }
    HNSW_SEARCH_POOLS.with(|pools| pools.borrow_mut().clear());
    let before = index.exact_scan_fallbacks();
    let hits = index
        .search_knn_filtered(&[0.0, 0.0, 0.0], K, &|eid| eid == "v511")
        .unwrap();
    assert_eq!(hits, vec![("v511".to_owned(), -511.0)]);
    assert_eq!(index.exact_scan_fallbacks(), before + 1);
    let pools = HNSW_SEARCH_POOLS.with(|pools| pools.borrow().clone());
    assert!(
        !pools.is_empty(),
        "the declared graph backend must be consulted"
    );
    assert!(
        pools.iter().all(|&pool| pool <= EF.max(5 * K)),
        "a selective filter must not grow traversal to corpus size: {pools:?}"
    );
}

#[test]
fn hnsw_clean_permissive_search_keeps_the_graph_answer() {
    let index = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    index.set_ef_search(64);
    for i in 0..128 {
        index
            .add(&format!("v{i:03}"), &[i as f32, 0.0, 0.0])
            .unwrap();
    }
    HNSW_SEARCH_POOLS.with(|pools| pools.borrow_mut().clear());
    let before = index.exact_scan_fallbacks();
    let hits = index
        .search_knn_filtered(&[0.0, 0.0, 0.0], 5, &|_| true)
        .unwrap();
    let expected: Vec<_> = (0..5).map(|i| (format!("v{i:03}"), -(i as f32))).collect();
    assert_eq!(hits, expected);
    assert_eq!(index.exact_scan_fallbacks(), before);
    assert_eq!(
        HNSW_SEARCH_POOLS.with(|pools| pools.borrow().clone()),
        vec![25]
    );
}

#[test]
fn hnsw_returns_self_as_top_neighbour() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let idx = HnswCpuIndex::new(spec(64, VectorMetric::L2, None));
    let mut all = Vec::new();
    for i in 0..200 {
        let v = rand_vec(&mut rng, 64);
        idx.add(&format!("e{i}"), &v).unwrap();
        all.push((format!("e{i}"), v));
    }
    // Each inserted vector should be its own nearest neighbour.
    let (eid, q) = &all[42];
    let hits = idx.search_knn(q, 1).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, *eid);
}

#[test]
fn hnsw_1000_vectors_topk_returns_reasonable_neighbours() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(11);
    let dim = 128;
    let idx = HnswCpuIndex::new(spec(dim, VectorMetric::L2, None));
    let mut data: Vec<(String, Vec<f32>)> = Vec::new();
    for i in 0..1_000 {
        let v = rand_vec(&mut rng, dim as usize);
        idx.add(&format!("v{i}"), &v).unwrap();
        data.push((format!("v{i}"), v));
    }
    let q = rand_vec(&mut rng, dim as usize);
    let hits = idx.search_knn(&q, 10).unwrap();
    assert_eq!(hits.len(), 10);
    // Scores should be monotone-non-increasing (higher = better).
    for w in hits.windows(2) {
        assert!(w[0].1 >= w[1].1, "non-monotone scores: {:?}", hits);
    }
    // The top-10 should be a reasonable approximation of the true
    // top-10 by brute force — at minimum, overlap ≥ 4. (HNSW with
    // 1k random points and dim=128 is approximate, not exact.)
    let mut by_dist: Vec<(String, f32)> = data
        .iter()
        .map(|(e, v)| (e.clone(), l2_squared(&q, v).sqrt()))
        .collect();
    by_dist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let truth_top10: std::collections::HashSet<&str> =
        by_dist.iter().take(10).map(|(e, _)| e.as_str()).collect();
    let hnsw_top10: std::collections::HashSet<&str> =
        hits.iter().map(|(e, _)| e.as_str()).collect();
    let overlap = truth_top10.intersection(&hnsw_top10).count();
    assert!(overlap >= 4, "overlap with truth top-10 was {overlap}");
}

#[test]
fn hnsw_cold_collection_reopen_does_not_scan_owned_vectors_for_coverage() {
    use std::collections::BTreeMap;

    use crate::index::domain::collection::Collection;
    use crate::index::domain::field_index::FieldIndex;
    use crate::shared_kernel::types::schema::{FieldSpec, FieldType, VectorBackend};

    let schema = BTreeMap::from([(
        "v".to_owned(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(3),
            metric: Some(VectorMetric::L2),
            backend: Some(VectorBackend::HnswCpu),
            quantize: None,
        },
    )]);
    let root = tempfile::tempdir().unwrap();
    let mut original = Collection::new(schema.clone()).unwrap();
    for (eid, vector) in [
        ("one", [1.0, 0.0, 0.0]),
        ("two", [0.0, 1.0, 0.0]),
        ("three", [0.0, 0.0, 1.0]),
    ] {
        let id = original.interner.intern(eid);
        let FieldIndex::Vector { idx, .. } = original.fields.get("v").unwrap() else {
            panic!("the declared vector field must exist");
        };
        idx.add(eid, &vector).unwrap();
        original
            .eid_fields
            .entry(id)
            .or_default()
            .insert("v".to_owned());
    }
    original.interner.intern("without-vector");
    original.seal_to_segments(root.path(), 7).unwrap();
    drop(original);

    VECTOR_STORE_OWNED_SCANS.with(|scans| scans.set(0));
    let reopened = Collection::open_from_segments(root.path(), schema, 1).unwrap();
    let scans_after_reopen = VECTOR_STORE_OWNED_SCANS.with(|scans| scans.get());

    for eid in ["one", "two", "three"] {
        let id = reopened.interner.id(eid).unwrap();
        assert!(reopened.eid_fields.get(&id).unwrap().contains("v"));
    }
    let absent = reopened.interner.id("without-vector").unwrap();
    assert!(!reopened.eid_fields.contains_key(&absent));
    let FieldIndex::Vector { idx, .. } = reopened.fields.get("v").unwrap() else {
        panic!("the reopened vector field must exist");
    };
    let hits = idx
        .search_knn_filtered(&[1.0, 0.0, 0.0], 1, &|_| true)
        .unwrap();
    assert_eq!(hits[0].0, "one");
    assert_eq!(
        scans_after_reopen, 0,
        "cold field coverage must use persisted row IDs without an owned vector snapshot"
    );
}

fn cold_vector_coverage_fixture(
    backend: crate::shared_kernel::types::schema::VectorBackend,
) -> (
    tempfile::TempDir,
    std::collections::BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec>,
) {
    use crate::persistence::infrastructure::segment::{eid_writer, vector_writer};
    use crate::shared_kernel::types::schema::{FieldSpec, FieldType};
    let root = tempfile::tempdir().unwrap();
    let eids = ["live", "missing", "retired"];
    eid_writer::write_eid_segment(&root.path().join("_collection.lmeta.lseg"), 7, &eids).unwrap();
    eid_writer::write_eid_segment(&root.path().join("v.eids.lseg"), 7, &eids).unwrap();
    vector_writer::write_vector_segment(
        &root.path().join("v.lseg"),
        7,
        3,
        &[Some(&[1.0, 0.0, 0.0]), None, Some(&[0.0, 1.0, 0.0])],
    )
    .unwrap();
    let schema = std::collections::BTreeMap::from([(
        "v".to_owned(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(3),
            metric: Some(VectorMetric::L2),
            backend: Some(backend),
            quantize: None,
        },
    )]);
    (root, schema)
}

#[test]
fn hnsw_cold_collection_vector_coverage_skips_absent_rows_and_keeps_deletes() {
    use crate::index::domain::collection::Collection;
    use crate::index::domain::field_index::FieldIndex;
    use crate::index::infrastructure::checkpoint_fs::CheckpointLayout;
    use crate::shared_kernel::types::schema::VectorBackend;
    for (backend, deferred) in [
        (VectorBackend::FlatCpu, false),
        (VectorBackend::HnswCpu, true),
    ] {
        let (root, schema) = cold_vector_coverage_fixture(backend);
        let mut reopened = Collection::open_from_segments_with_vectors(
            root.path(),
            schema.clone(),
            1,
            deferred,
            CheckpointLayout::Legacy,
            None,
            None,
        )
        .unwrap();
        let live = reopened.interner.id("live").unwrap();
        let missing = reopened.interner.id("missing").unwrap();
        let retired = reopened.interner.id("retired").unwrap();
        assert!(reopened.eid_fields.get(&live).unwrap().contains("v"));
        assert!(reopened.eid_fields.get(&retired).unwrap().contains("v"));
        assert!(!reopened.eid_fields.contains_key(&missing));
        reopened
            .fields
            .get_mut("v")
            .unwrap()
            .drop_eid(retired, "retired");
        reopened.eid_fields.remove(&retired);
        reopened.seal_to_segments(root.path(), 8).unwrap();
        drop(reopened);
        let reopened = Collection::open_from_segments_with_vectors(
            root.path(),
            schema,
            1,
            deferred,
            CheckpointLayout::Legacy,
            None,
            None,
        )
        .unwrap();
        assert!(reopened.eid_fields.get(&live).unwrap().contains("v"));
        assert!(!reopened.eid_fields.contains_key(&missing));
        assert!(!reopened.eid_fields.contains_key(&retired));
        let FieldIndex::Vector { idx, .. } = reopened.fields.get("v").unwrap() else {
            panic!("reopened vector field must exist");
        };
        let hits = idx
            .search_knn_filtered(&[1.0, 0.0, 0.0], 3, &|_| true)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "live");
    }
}

#[test]
fn hnsw_cold_collection_reopen_still_rejects_missing_vector_rows() {
    use crate::index::domain::collection::Collection;
    use crate::shared_kernel::types::schema::VectorBackend;
    let (root, schema) = cold_vector_coverage_fixture(VectorBackend::HnswCpu);
    let error = Collection::open_from_segments(root.path(), schema, 1)
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "vector segment is missing row 1 of 3 (eid `missing`)"
    );
}

#[test]
fn hnsw_cold_collection_flat_sidecar_mismatch_still_fails_closed() {
    use crate::index::domain::collection::Collection;
    use crate::index::infrastructure::checkpoint_fs::CheckpointLayout;
    use crate::persistence::infrastructure::segment::eid_writer;
    use crate::shared_kernel::types::schema::VectorBackend;
    for (backend, deferred) in [
        (VectorBackend::FlatCpu, false),
        (VectorBackend::HnswCpu, true),
    ] {
        let (root, schema) = cold_vector_coverage_fixture(backend);
        eid_writer::write_eid_segment(&root.path().join("v.eids.lseg"), 7, &["live"]).unwrap();
        let error = Collection::open_from_segments_with_vectors(
            root.path(),
            schema,
            1,
            deferred,
            CheckpointLayout::Legacy,
            None,
            None,
        )
        .err()
        .unwrap();
        assert_eq!(
            error.to_string(),
            "vector sidecar row count does not match segment"
        );
    }
}
