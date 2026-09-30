use std::collections::{BTreeMap, BTreeSet};

use crate::index::application::engine::Engine;
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::sharding::domain::bucket_move::{bucket_moves, BucketMove};
use crate::sharding::domain::merge_delta::merge_snapshot_delta;
use crate::sharding::domain::reshard_batch::{
    snapshot_reshard_batches, ADMIN_ROUTE_BODY_LIMIT_BYTES, MAX_BATCH_BYTES,
};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    query::{MatchOp, MatchQuery, QueryNode},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
    search::SearchRequest,
};

#[test]
fn bucket_moves_reports_only_reassigned_buckets() {
    let from = VirtualBucketShardMap::new(1, vec![0, 0, 1, 1], 2).unwrap();
    let to = VirtualBucketShardMap::new(2, vec![0, 1, 1, 0], 2).unwrap();

    assert_eq!(
        bucket_moves(&from, &to).unwrap(),
        vec![
            BucketMove {
                bucket: 1,
                from_shard: 0,
                to_shard: 1,
            },
            BucketMove {
                bucket: 3,
                from_shard: 1,
                to_shard: 0,
            },
        ]
    );
}

#[test]
fn snapshot_reshard_batches_emit_bounded_restorable_partials() {
    let collection_id = "users";
    let source = Engine::new();
    source
        .create_collection(
            collection_id,
            CreateCollectionRequest {
                fields: BTreeMap::from([
                    ("body".into(), field(FieldType::Text)),
                    ("email".into(), field(FieldType::Keyword)),
                    ("age".into(), field(FieldType::Number)),
                    ("tags".into(), field(FieldType::Set)),
                    ("phash".into(), field(FieldType::Hash)),
                ]),
            },
        )
        .unwrap();
    for i in 0..24 {
        let external_id = format!("doc-{i:02}");
        source
            .index(
                collection_id,
                IndexRequest {
                    request_id: None,
                    items: vec![
                        item(&external_id, "body", FieldValue::String("engineer".into())),
                        item(
                            &external_id,
                            "email",
                            FieldValue::String(format!("{external_id}@example.com")),
                        ),
                        item(&external_id, "age", FieldValue::Number(i as f64)),
                        item(
                            &external_id,
                            "tags",
                            FieldValue::StringList(vec!["blue".into(), "green".into()]),
                        ),
                        item(
                            &external_id,
                            "phash",
                            FieldValue::String(format!("{i:016x}")),
                        ),
                    ],
                },
            )
            .unwrap();
    }

    let snapshot = source.snapshot().unwrap();
    let from = VirtualBucketShardMap::new(1, vec![0, 0, 0, 0], 1).unwrap();
    let to = VirtualBucketShardMap::new(2, vec![0, 1, 0, 1], 2).unwrap();
    let all_buckets: BTreeSet<u32> = (0..4).collect();
    let batches =
        snapshot_reshard_batches(&snapshot, &from, &to, &all_buckets, 3, MAX_BATCH_BYTES).unwrap();

    assert!(!batches.is_empty());
    assert!(batches
        .iter()
        .all(|b| b.external_ids.values().map(BTreeSet::len).sum::<usize>() <= 3));
    assert!(batches.iter().any(|b| b.to_shard == 1));

    let mut target_snapshot = SnapshotV1 {
        version: snapshot.version,
        collections: BTreeMap::new(),
    };
    for batch in &batches {
        target_snapshot = merge_snapshot_delta(target_snapshot, batch.snapshot.clone()).unwrap();
    }
    let moved = Engine::new();
    moved.restore(target_snapshot).unwrap();
    let hits = moved
        .search(
            collection_id,
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "body".into(),
                    text: "engineer".into(),
                    op: MatchOp::And,
                }),
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .len();
    let moved_ids: usize = batches
        .iter()
        .map(|batch| {
            batch
                .external_ids
                .values()
                .map(BTreeSet::len)
                .sum::<usize>()
        })
        .sum();
    assert_eq!(hits, moved_ids);
    assert!(moved_ids < 24, "split should move only reassigned buckets");
}

/// AC4 (#1396 R4): a moved bucket's delta whose full snapshot serializes
/// well over 8 MiB (large per-doc text bodies, not merely a high id
/// count) still migrates completely via byte-capped batches — the
/// fixture picks a document count/size that would collapse to a single
/// oversized batch under the old id-count-only cap
/// (`max_external_ids_per_batch` set high enough that byte size, not id
/// count, is the binding constraint).
#[test]
fn snapshot_reshard_batches_splits_oversized_bucket_delta_by_bytes() {
    let collection_id = "docs";
    let source = std::sync::Arc::new(Engine::new());
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let checkpoint = crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(
        checkpoint_dir.path(),
    )
    .unwrap();
    source
        .create_collection(
            collection_id,
            CreateCollectionRequest {
                fields: BTreeMap::from([("body".into(), field(FieldType::Text))]),
            },
        )
        .unwrap();

    // A text field's snapshot wire size is driven by its *inverted
    // index* (`FieldIndexSnapshot::Text`'s `forward`: per-doc unique
    // token set, and `tokens`: per-term postings) — repeating one word
    // many times within a doc collapses to a single unique token and
    // stays tiny on the wire, so this fixture instead gives every doc a
    // large, shared vocabulary of distinct tokens: ~2500 unique tokens
    // per doc across 200 docs comes to well over 8 MiB serialized
    // (~25 KiB/doc of forward token strings alone, plus per-term
    // postings), comfortably over both `MAX_BATCH_BYTES` (4 MiB) and
    // the route's 8 MiB body limit if emitted as one batch.
    const VOCAB_SIZE: usize = 2500;
    let vocab: Vec<String> = (0..VOCAB_SIZE).map(|i| format!("tok{i}")).collect();
    let big_body = vocab.join(" ");
    let ids: Vec<String> = (0..200).map(|i| format!("d-{i:04}")).collect();
    for (position, id) in ids.iter().enumerate() {
        // Build the same large reshard source through bounded pending
        // changes. Its already checkpointed index is not pending work.
        if position != 0 && position % 32 == 0 {
            checkpoint.save(&source, 0).unwrap();
        }
        source
            .index(
                collection_id,
                IndexRequest {
                    request_id: None,
                    items: vec![item(id, "body", FieldValue::String(big_body.clone()))],
                },
            )
            .unwrap();
    }

    let snapshot = source.snapshot().unwrap();
    // Single physical shard -> two, everything in one bucket moves.
    let from = VirtualBucketShardMap::new(1, vec![0], 1).unwrap();
    let to = VirtualBucketShardMap::new(2, vec![1], 2).unwrap();
    // A generous id-count cap so byte size, not id count, is what
    // forces the split.
    let all_buckets: BTreeSet<u32> = BTreeSet::from([0]);
    let batches =
        snapshot_reshard_batches(&snapshot, &from, &to, &all_buckets, 10_000, MAX_BATCH_BYTES)
            .unwrap();

    assert!(
        batches.len() > 1,
        "expected the oversized delta to split into more than one batch, got {}",
        batches.len()
    );
    for batch in &batches {
        let wire_bytes = serde_json::to_vec(batch).unwrap().len();
        assert!(
            wire_bytes < ADMIN_ROUTE_BODY_LIMIT_BYTES,
            "batch serialized to {wire_bytes} bytes, over the route's {ADMIN_ROUTE_BODY_LIMIT_BYTES} byte body limit"
        );
    }

    // Every id made it into exactly one batch, and merging them back
    // together restores every document.
    let moved_ids: BTreeSet<String> = batches
        .iter()
        .flat_map(|b| b.external_ids.values().flat_map(|s| s.iter().cloned()))
        .collect();
    assert_eq!(moved_ids, ids.iter().cloned().collect::<BTreeSet<_>>());

    let mut target_snapshot = SnapshotV1 {
        version: snapshot.version,
        collections: BTreeMap::new(),
    };
    for batch in &batches {
        target_snapshot = merge_snapshot_delta(target_snapshot, batch.snapshot.clone()).unwrap();
    }
    let moved = Engine::new();
    moved.restore(target_snapshot).unwrap();
    let hits = moved
        .search(
            collection_id,
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "body".into(),
                    text: "tok0".into(),
                    op: MatchOp::And,
                }),
                limit: 1000,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .len();
    assert_eq!(
        hits,
        ids.len(),
        "every moved document should be restorable from the byte-capped batches"
    );
}

fn field(field_type: FieldType) -> FieldSpec {
    FieldSpec {
        field_type,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn item(external_id: &str, field: &str, value: FieldValue) -> IndexItem {
    IndexItem {
        external_id: external_id.into(),
        field: field.into(),
        value,
        version: None,
    }
}

mod prune;
