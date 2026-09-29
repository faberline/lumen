use super::*;
use crate::index::domain::vector::hnsw_cpu_index::HnswGraphCacheResult;
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorQuantize};

#[test]
fn graph_cache_allocator_must_exceed_orphan_ids_too() {
    let (spec, index) = fixture(VectorMetric::L2);
    {
        let mut inner = index.inner.write().unwrap();
        inner.hnsw.insert(&[0.4, 0.2], 100);
        inner.next_id = 101;
    }
    let directory = tempfile::tempdir().unwrap();
    save(&index.inner.read().unwrap(), directory.path()).unwrap();
    let path = directory.path().join(MANIFEST);
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    manifest["next_id"] = serde_json::json!(33);
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    assert!(
        load(spec, &vectors, codebook, directory.path()).is_err(),
        "a future live ID must never collide with an orphan node"
    );
}

#[test]
fn graph_cache_preserves_a_stable_scalar_codebook() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::Cosine,
        backend: crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        quantize: Some(VectorQuantize::Sq),
    };
    let index = HnswCpuIndex::new(spec);
    index.add("bounds", &[0.0, 1.0]).unwrap();
    for i in 1..32 {
        index
            .add(&format!("sq-{i}"), &[i as f32 / 32.0, 0.5])
            .unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    save(&index.inner.read().unwrap(), directory.path()).unwrap();
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    let loaded = load(spec, &vectors, codebook, directory.path())
        .unwrap()
        .unwrap();
    for (eid, value) in &vectors {
        assert_eq!(loaded.checkpoint_vector(eid).unwrap().as_ref(), Some(value));
    }
    assert_eq!(
        loaded.search_knn(&[0.251, 0.5], 5).unwrap(),
        index.search_knn(&[0.251, 0.5], 5).unwrap()
    );
}

#[test]
fn graph_cache_removes_only_its_abandoned_staging_directory() {
    let (_, index) = fixture(VectorMetric::L2);
    let directory = tempfile::tempdir().unwrap();
    let partial = tempfile::Builder::new()
        .prefix(".graph-stage-")
        .tempdir_in(directory.path())
        .unwrap()
        .keep();
    std::fs::write(partial.join("partial"), b"partial cache").unwrap();
    let unrelated = directory.path().join("keep-this-directory");
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::write(unrelated.join("sentinel"), b"keep").unwrap();
    save(&index.inner.read().unwrap(), directory.path()).unwrap();
    assert!(!partial.exists());
    assert_eq!(std::fs::read(unrelated.join("sentinel")).unwrap(), b"keep");
}

fn fixture(metric: VectorMetric) -> (VectorSpec, HnswCpuIndex) {
    let spec = VectorSpec {
        dim: 2,
        metric,
        backend: crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        quantize: None,
    };
    let index = HnswCpuIndex::new(spec);
    for i in 0..32 {
        index
            .add(&format!("id-{i}"), &[(i as f32 + 1.0) / 64.0, 0.2])
            .unwrap();
    }
    (spec, index)
}

#[test]
fn graph_cache_roundtrips_all_metrics_and_can_be_saved_again() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OwnedGraph>();
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let (spec, index) = fixture(metric);
        let expected = index.search_knn(&[0.173, 0.2], 10).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let (vectors, codebook) = index.dump_for_snapshot().unwrap();
        save(&index.inner.read().unwrap(), directory.path()).unwrap();
        let loaded = load(spec, &vectors, codebook, directory.path())
            .unwrap()
            .unwrap();
        assert_eq!(loaded.search_knn(&[0.173, 0.2], 10).unwrap(), expected);
        save(&loaded.inner.read().unwrap(), directory.path()).unwrap();
        let loaded_again = load(spec, &vectors, codebook, directory.path())
            .unwrap()
            .unwrap();
        drop(directory);
        assert_eq!(
            loaded_again.search_knn(&[0.173, 0.2], 10).unwrap(),
            expected
        );
    }
}

#[test]
fn graph_cache_rejects_stale_content_incompatible_metadata_and_bad_payloads() {
    let (spec, index) = fixture(VectorMetric::L2);
    let directory = tempfile::tempdir().unwrap();
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    save(&index.inner.read().unwrap(), directory.path()).unwrap();
    let mut stale = vectors.clone();
    stale[0].1[0] += 1.0;
    assert!(load(spec, &stale, codebook, directory.path()).is_err());
    assert!(load(
        VectorSpec {
            metric: VectorMetric::Dot,
            ..spec
        },
        &vectors,
        codebook,
        directory.path()
    )
    .is_err());
    for mutation in 0..6 {
        save(&index.inner.read().unwrap(), directory.path()).unwrap();
        let path = directory.path().join(MANIFEST);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        match mutation {
            0 => manifest["platform"] = serde_json::json!("foreign-platform"),
            1 => manifest["format"] = serde_json::json!("unknown-version"),
            2 => manifest["ids"][1][1] = manifest["ids"][0][1].clone(),
            3 => manifest["next_id"] = serde_json::json!(0),
            4 => manifest["points"] = serde_json::json!(0),
            _ => manifest["ids"][0][0] = serde_json::json!("foreign-id"),
        }
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(
            load(spec, &vectors, codebook, directory.path()).is_err(),
            "mutation {mutation}"
        );
    }
    save(&index.inner.read().unwrap(), directory.path()).unwrap();
    std::fs::write(directory.path().join("graph.hnsw.graph"), b"corrupt").unwrap();
    assert!(load(spec, &vectors, codebook, directory.path()).is_err());
    let recovered =
        HnswCpuIndex::restore_with_graph_cache(spec, vectors, codebook, Some(directory.path()))
            .unwrap();
    assert_eq!(
        recovered.search_knn(&[0.173, 0.2], 10).unwrap(),
        index.search_knn(&[0.173, 0.2], 10).unwrap()
    );
}

#[test]
fn graph_cache_restore_timing_distinguishes_hit_and_rejected_fallback() {
    let (spec, index) = fixture(VectorMetric::L2);
    let directory = tempfile::tempdir().unwrap();
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    let expected = index.search_knn(&[0.173, 0.2], 10).unwrap();
    save(&index.inner.read().unwrap(), directory.path()).unwrap();

    let (loaded, hit_timing) = HnswCpuIndex::restore_with_graph_cache_timed(
        spec,
        vectors.clone(),
        codebook,
        Some(directory.path()),
    )
    .unwrap();
    assert_eq!(hit_timing.cache_result, Some(HnswGraphCacheResult::Hit));
    assert_eq!(hit_timing.fallback_rebuild, Duration::ZERO);
    assert!(hit_timing.total >= hit_timing.cache_deserialize);
    assert_eq!(loaded.search_knn(&[0.173, 0.2], 10).unwrap(), expected);

    std::fs::write(directory.path().join("graph.hnsw.graph"), b"corrupt").unwrap();
    let (rebuilt, rejected_timing) = HnswCpuIndex::restore_with_graph_cache_timed(
        spec,
        vectors,
        codebook,
        Some(directory.path()),
    )
    .unwrap();
    assert_eq!(
        rejected_timing.cache_result,
        Some(HnswGraphCacheResult::Rejected)
    );
    assert!(
        rejected_timing.cache_payload_hash > Duration::ZERO,
        "a rejected payload must retain its completed timing"
    );
    assert!(rejected_timing.total >= rejected_timing.fallback_rebuild);
    assert_eq!(
        rebuilt.search_knn(&[0.173, 0.2], 10).unwrap(),
        expected,
        "a rejected optional cache must retain the authoritative recovery outcome"
    );
}

#[test]
fn graph_cache_format_requires_the_locked_codec_version() {
    let lock: toml::Value = toml::from_str(include_str!("../../../../../Cargo.lock")).unwrap();
    let package = lock["package"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"].as_str() == Some("hnsw_rs"))
        .unwrap();
    assert_eq!(
        package["version"].as_str(),
        Some("0.3.4"),
        "a codec upgrade needs an explicit cache format review"
    );
}

#[cfg(unix)]
#[test]
fn graph_cache_refuses_symlink_directories_without_touching_the_target() {
    let (spec, index) = fixture(VectorMetric::L2);
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("sentinel"), b"keep").unwrap();
    let alias = directory.path().join("alias");
    std::os::unix::fs::symlink(outside.path(), &alias).unwrap();
    assert!(save(&index.inner.read().unwrap(), &alias).is_err());
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    assert!(load(spec, &vectors, codebook, &alias).is_err());
    assert_eq!(
        std::fs::read(outside.path().join("sentinel")).unwrap(),
        b"keep"
    );
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
}

#[test]
fn graph_cache_roundtrip_keeps_replaced_and_deleted_ids_out_of_results() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        quantize: None,
    };
    let index = HnswCpuIndex::new(spec);
    for i in 0..64 {
        index.add(&format!("row-{i}"), &[i as f32, 1.0]).unwrap();
    }
    index.add("row-3", &[100.0, 1.0]).unwrap();
    index.remove("row-4").unwrap();
    let expected = index.search_knn(&[100.0, 1.0], 10).unwrap();
    let directory = tempfile::tempdir().unwrap();
    assert!(
        save(&index.inner.read().unwrap(), directory.path()).unwrap(),
        "a populated HNSW graph must have a cache"
    );
    let (vectors, codebook) = index.dump_for_snapshot().unwrap();
    let loaded = load(spec, &vectors, codebook, directory.path())
        .unwrap()
        .expect("identical durable vectors must accept their graph cache");
    assert_eq!(loaded.len(), 63);
    assert_eq!(loaded.search_knn(&[100.0, 1.0], 10).unwrap(), expected);
    assert_eq!(loaded.checkpoint_vector("row-4").unwrap(), None);
    loaded.add("after-load", &[101.0, 1.0]).unwrap();
    assert_eq!(
        loaded.search_knn(&[101.0, 1.0], 1).unwrap()[0].0,
        "after-load"
    );
}
