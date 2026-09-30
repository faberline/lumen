//! Dual-path diff test: segment-backed Hash (Hamming) read must be
//! byte-identical to the live in-RAM read (Stage 2 Phase 2d).

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{HammingQuery, QueryNode};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn fieldspec(t: FieldType) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

/// A single `sig` Hash field.
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("sig".into(), fieldspec(FieldType::Hash));
    CreateCollectionRequest { fields }
}

fn index_hash(e: &Engine, eid: &str, hash: u64) {
    e.index(
        "c",
        IndexRequest {
            items: vec![crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "sig".into(),
                value: FieldValue::String(format!("{hash:016x}")),
                version: None,
            }],
            request_id: None,
        },
    )
    .unwrap();
}

fn hamming(hash: u64, max: u32) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Hamming(HammingQuery {
            field: "sig".into(),
            hash: format!("{hash:016x}"),
            max_distance: max,
        }),
        limit: 100_000,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

/// (external_id, score_bits) keyed map — order-independent, byte-exact.
fn run(e: &Engine, hash: u64, max: u32) -> BTreeMap<String, u32> {
    e.search("c", hamming(hash, max))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, brute-force over `forward`) must equal PATH B
    /// (Hash field sealed to an mmap segment, hash read via the segment) —
    /// same matching docs AND byte-identical Hamming similarity scores —
    /// over a randomized corpus and query.
    #[test]
    fn hamming_segment_matches_live(
        hashes in proptest::collection::vec(any::<u64>(), 1..60),
        q in any::<u64>(),
        max in 0u32..=64,
    ) {
        // --- PATH A: build, query (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, h) in hashes.iter().enumerate() {
            index_hash(&e, &format!("d{i}"), *h);
        }
        let a = run(&e, q, max);

        // --- PATH B: seal `sig`, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_hash_field_to_segment("c", "sig", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, hashes.len(), "all docs sealed");
        let b = run(&e, q, max);

        prop_assert_eq!(a, b, "hamming result/scores diverged after seal");
    }
}

/// A doc indexed AFTER sealing (docid >= n_docs) must still match through
/// `hash_at`'s live-tail fallback; the sealed doc is served from the mmap.
#[test]
fn hamming_live_tail_after_seal() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_hash(&e, "sealed", 0x0000_0000_0000_0000); // id 0
    index_hash(&e, "other", 0xFFFF_FFFF_FFFF_FFFF); // id 1

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_hash_field_to_segment("c", "sig", dir.path())
        .unwrap();
    assert_eq!(n, 2);

    index_hash(&e, "tail", 0x0000_0000_0000_0003); // id 2 (2 bits set), live tail

    // Query hash 0, max distance 2: sealed (dist 0) + tail (dist 2) match,
    // `other` (dist 64) does not. Proves segment read + live-tail fallback.
    let got = run(&e, 0, 2);
    let mut want = BTreeMap::new();
    want.insert("sealed".to_string(), (1.0f32).to_bits()); // dist 0 → 64/64
    want.insert("tail".to_string(), ((64 - 2) as f32 / 64.0).to_bits());
    assert_eq!(got, want);

    // Direct check on hash_at: segment for [0,2), live tail for id 2.
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Hash(h) = coll.fields.get("sig").unwrap() else {
        panic!("sig must be a Hash field");
    };
    assert!(h.segment.is_some(), "segment attached");
    assert_eq!(h.hash_at(0), Some(0)); // segment
    assert_eq!(h.hash_at(1), Some(u64::MAX)); // segment
    assert_eq!(h.hash_at(2), Some(3)); // live tail
    assert_eq!(h.hash_at(99), None);
}
