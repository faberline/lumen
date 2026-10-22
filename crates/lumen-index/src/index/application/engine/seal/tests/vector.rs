//! Dual-path diff test: a flat-cpu Vector field's exact kNN scan served from an
//! mmap'd segment must return IDENTICAL top-k (eids + byte-identical distances)
//! as the in-RAM scan (Stage 2 Phase 2d).

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::QueryNode;
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::schema::{VectorBackend, VectorMetric};
use crate::shared_kernel::types::search::SearchRequest;

const DIM: usize = 8;

fn vec_fieldspec(metric: VectorMetric) -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Vector,
        analyzer: None,
        multi: None,
        dim: Some(DIM as u32),
        metric: Some(metric),
        // The slice is FlatCpu/exact only — HNSW is untouched.
        backend: Some(VectorBackend::FlatCpu),
        quantize: None,
    }
}

fn schema(metric: VectorMetric) -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("emb".into(), vec_fieldspec(metric));
    CreateCollectionRequest { fields }
}

fn index_vec(e: &Engine, eid: &str, v: &[f32]) {
    e.index(
        "c",
        IndexRequest {
            items: vec![crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "emb".into(),
                value: FieldValue::Vector(v.to_vec()),
                version: None,
            }],
            request_id: None,
        },
    )
    .unwrap();
}

fn knn(query: Vec<f32>, k: u32) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Knn(crate::shared_kernel::types::query::KnnQuery {
            field: "emb".into(),
            vector: query,
            k,
        }),
        limit: k,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

/// Ordered (eid, score_bits) pairs — the kNN result is RANKED, so order is
/// part of the contract; scores compared as exact f32 bits.
fn run(e: &Engine, query: Vec<f32>, k: u32) -> Vec<(String, u32)> {
    e.search("c", knn(query, k))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(120))]

    /// PATH A (segment OFF, scan reads in-RAM `FlatVecs::data`) must equal
    /// PATH B (the corpus sealed to an f32 mmap segment, scan reads each
    /// row zero-copy off the page) — IDENTICAL ranked top-k, eids and
    /// byte-identical distances (the f32 bits on disk are the same bits).
    #[test]
    fn knn_segment_matches_live(
        raw in proptest::collection::vec(
            proptest::collection::vec(-4.0f32..4.0, DIM..=DIM),
            1..40,
        ),
        qraw in proptest::collection::vec(-4.0f32..4.0, DIM..=DIM),
        k in 1u32..12,
        metric in prop::sample::select(vec![
            VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot,
        ]),
    ) {
        // --- PATH A: build the flat-cpu corpus, run kNN (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema(metric)).unwrap();
        for (i, v) in raw.iter().enumerate() {
            index_vec(&e, &format!("d{i}"), v);
        }
        let a = run(&e, qraw.clone(), k);

        // --- PATH B: seal `emb` to a segment, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_vector_field_to_segment("c", "emb", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, raw.len(), "all vectors sealed");
        let b = run(&e, qraw, k);

        // IDENTICAL ranked top-k: same eids in the same order, byte-exact scores.
        prop_assert_eq!(a, b, "kNN top-k diverged after seal");
    }
}

/// A direct, planner-free check: after sealing, the flat buffer's in-RAM
/// `data` is dropped and every row is served from the segment, yet a kNN
/// scan returns the same ranked neighbours as before the seal.
#[test]
fn knn_served_from_segment_after_seal() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema(VectorMetric::L2)).unwrap();
    // Points on a 1-D ray so the nearest order is deterministic.
    for i in 0..10usize {
        let mut v = vec![0.0f32; DIM];
        v[0] = i as f32;
        index_vec(&e, &format!("p{i}"), &v);
    }
    let mut q = vec![0.0f32; DIM];
    q[0] = 0.0;
    let before = run(&e, q.clone(), 5);

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_vector_field_to_segment("c", "emb", dir.path())
        .unwrap();
    assert_eq!(n, 10);

    let after = run(&e, q, 5);
    assert_eq!(before, after, "kNN diverged when served from the segment");
    // Nearest to [0,..] is p0, then p1, ... (L2 on the ray).
    let eids: Vec<String> = after.iter().map(|(e, _)| e.clone()).collect();
    assert_eq!(eids, ["p0", "p1", "p2", "p3", "p4"]);
}
