//! The fixtures the Engine's tests share (the users collection and its index
//! items, the record-cost schema, a keyword-only collection), and the
//! engine-wide storage-bytes gauge: published after a write, a restore, a
//! replacement's activation, an eviction and a stats read.

use std::collections::BTreeMap;

use crate::index::application::engine::collections::DropOutcome;
use crate::index::application::engine::Engine;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::shared_kernel::types::document::{
    FieldValue, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};

pub(in crate::index) fn build_users_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "bio".into(),
        FieldSpec {
            field_type: FieldType::Text,
            analyzer: Some(Analyzer::WhitespaceLower),
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    fields.insert(
        "email".into(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    fields.insert(
        "tags".into(),
        FieldSpec {
            field_type: FieldType::Set,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    fields.insert(
        "age".into(),
        FieldSpec {
            field_type: FieldType::Number,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

pub(in crate::index) fn item(
    eid: &str,
    field: &str,
    value: FieldValue,
) -> crate::shared_kernel::types::document::IndexItem {
    crate::shared_kernel::types::document::IndexItem {
        external_id: eid.into(),
        field: field.into(),
        value,
        version: None,
    }
}

pub(super) fn record_cost_schema(
    vector_backend: Option<crate::shared_kernel::types::schema::VectorBackend>,
) -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "text".into(),
        FieldSpec {
            field_type: FieldType::Text,
            analyzer: Some(Analyzer::Ngram),
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    fields.insert(
        "email".into(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    if let Some(backend) = vector_backend {
        fields.insert(
            "vector".into(),
            FieldSpec {
                field_type: FieldType::Vector,
                analyzer: None,
                multi: None,
                dim: Some(3),
                metric: Some(crate::shared_kernel::types::schema::VectorMetric::Cosine),
                backend: Some(backend),
                quantize: None,
            },
        );
    }
    CreateCollectionRequest { fields }
}

pub(super) fn kw_only_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "email".into(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

pub(super) fn index_kw(e: &Engine, collection_id: &str, eid: &str) {
    e.index(
        collection_id,
        IndexRequest {
            items: vec![item(
                eid,
                "email",
                FieldValue::String(format!("{eid}@x.com")),
            )],
            request_id: None,
        },
    )
    .unwrap();
}

/// Ground truth read of one collection's own byte footprint, independent
/// of the engine-wide gauge under test.
fn collection_live_bytes(e: &Engine, collection_id: &str) -> u64 {
    let state = e.state.read().unwrap();
    state
        .collections
        .get(collection_id)
        .unwrap()
        .fields
        .values()
        .map(|fi| fi.bytes())
        .sum()
}

/// Capacity control scrapes the engine metric directly; it must not need
/// a separate `/stats` request after a normal write, a segment/snapshot
/// restore, or a full-document replacement. The same `index` method is
/// used for AOF replay, so this also protects the post-restart path.
#[test]
fn mutations_and_restore_publish_storage_bytes_without_stats() {
    let e = Engine::new();
    e.create_collection("a", kw_only_schema()).unwrap();
    index_kw(&e, "a", "before-restart");
    let before_restart_bytes = collection_live_bytes(&e, "a");
    assert!(before_restart_bytes > 0);
    assert_eq!(e.metrics().storage_bytes.get(), before_restart_bytes);

    let snapshot = e.snapshot().unwrap();
    let restored = Engine::new();
    restored.restore(snapshot).unwrap();
    assert_eq!(
        restored.metrics().storage_bytes.get(),
        collection_live_bytes(&restored, "a"),
        "restore must publish the rebuilt segment footprint before any stats request"
    );

    let mut fields = BTreeMap::new();
    fields.insert(
        "email".to_string(),
        FieldValue::String("after-restart@example.com".to_string()),
    );
    restored
        .replace_docs(
            "a",
            ReplaceDocsRequest {
                docs: vec![ReplaceDocItem {
                    external_id: "before-restart".to_string(),
                    version: None,
                    fields,
                }],
            },
        )
        .unwrap();
    assert_eq!(
        restored.metrics().storage_bytes.get(),
        collection_live_bytes(&restored, "a"),
        "full replacement must refresh the capacity gauge"
    );

    restored.delete("a", "before-restart", None).unwrap();
    assert_eq!(
        restored.metrics().storage_bytes.get(),
        collection_live_bytes(&restored, "a"),
        "delete must refresh the capacity gauge for scale-down decisions"
    );

    assert_eq!(
        restored.drop_collection("a", false).unwrap(),
        DropOutcome::Marked
    );
    assert_eq!(
        restored.metrics().storage_bytes.get(),
        0,
        "soft-deleted collections must stop contributing to live capacity"
    );
}

#[test]
fn activate_replacement_swaps_the_complete_collection_set() {
    let active = Engine::new();
    active.create_collection("old", kw_only_schema()).unwrap();
    index_kw(&active, "old", "old-doc");

    let replacement = Engine::new();
    replacement
        .create_collection("new", kw_only_schema())
        .unwrap();
    index_kw(&replacement, "new", "new-doc");
    let expected_bytes = collection_live_bytes(&replacement, "new");

    active.activate_replacement(replacement).unwrap();

    assert!(
        active.stats("old").is_err(),
        "collections absent from the replacement must be removed"
    );
    assert_eq!(active.stats("new").unwrap().documents_indexed, 1);
    assert_eq!(active.metrics().storage_bytes.get(), expected_bytes);
}

/// #1397 R2 / AC2: `evict_not_owned` must publish the ENGINE-WIDE byte
/// total (summed across every live collection) after eviction, not just
/// whichever collection the loop happened to touch last. Two
/// collections, both with enough documents spread across the shard
/// map's virtual buckets that eviction touches both (a real
/// `balanced()` map, same shape production reshard uses) — the old
/// last-writer-wins gauge update would equal only the
/// later-in-iteration-order collection's post-eviction bytes ("b"
/// sorts after "a" in the `BTreeMap` the loop walks), not the sum.
#[test]
fn evict_not_owned_publishes_engine_wide_byte_total() {
    let e = Engine::new();
    e.create_collection("a", kw_only_schema()).unwrap();
    e.create_collection("b", kw_only_schema()).unwrap();
    for i in 0..40 {
        index_kw(&e, "a", &format!("a{i:02}"));
    }
    for i in 0..40 {
        index_kw(&e, "b", &format!("b{i:02}"));
    }

    let map = VirtualBucketShardMap::balanced(1, 8, 2).unwrap();
    let touches_a = (0..40).any(|i| map.route_document("a", None, &format!("a{i:02}")).shard != 0);
    let touches_b = (0..40).any(|i| map.route_document("b", None, &format!("b{i:02}")).shard != 0);
    assert!(
        touches_a && touches_b,
        "fixture must evict from both collections to exercise the \
             engine-wide sum (touches_a={touches_a}, touches_b={touches_b})"
    );

    let outcome = e.evict_not_owned(&map, 0).unwrap();
    assert_eq!(outcome.collections_touched, 2);
    assert!(outcome.documents_evicted > 0);

    let expected_total = collection_live_bytes(&e, "a") + collection_live_bytes(&e, "b");
    assert!(expected_total > 0);
    assert_eq!(
        e.metrics().storage_bytes.get(),
        expected_total,
        "gauge must equal the sum of both collections' post-eviction bytes, \
             not just one of them"
    );
}

/// #1397 R2 / AC2: `stats()` has the same last-writer-wins defect as
/// `evict_not_owned` — it must also publish the engine-wide byte total,
/// summed across every live collection, even though the API response it
/// returns stays scoped to the one requested collection.
#[test]
fn stats_publishes_engine_wide_byte_total() {
    let e = Engine::new();
    e.create_collection("a", kw_only_schema()).unwrap();
    e.create_collection("b", kw_only_schema()).unwrap();
    for i in 0..10 {
        index_kw(&e, "a", &format!("a{i:02}"));
    }
    for i in 0..25 {
        index_kw(&e, "b", &format!("b{i:02}"));
    }

    let bytes_a = collection_live_bytes(&e, "a");
    let bytes_b = collection_live_bytes(&e, "b");
    assert!(bytes_a > 0 && bytes_b != bytes_a);

    // Calling stats on the SMALLER collection last is the case the old
    // per-collection-only update got wrong: a last-writer-wins gauge
    // would equal `bytes_a` alone, not `bytes_a + bytes_b`.
    let stats_b = e.stats("b").unwrap();
    assert_eq!(stats_b.storage.total_bytes, bytes_b);
    e.stats("a").unwrap();

    assert_eq!(
        e.metrics().storage_bytes.get(),
        bytes_a + bytes_b,
        "gauge must equal the sum across both collections after a \
             single-collection stats() call"
    );
}
