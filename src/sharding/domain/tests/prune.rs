use std::collections::{BTreeMap, BTreeSet};

use crate::sharding::domain::prune_chunk::{snapshot_reshard_prune_chunks, ReshardPruneChunk};
use crate::sharding::domain::reshard_batch::{ADMIN_ROUTE_BODY_LIMIT_BYTES, MAX_BATCH_BYTES};
use crate::sharding::domain::snapshot_subset::snapshot_bucket_subset;
use crate::sharding::domain::tests::{field, item};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::storage::Engine;
use crate::types::{CreateCollectionRequest, FieldType, FieldValue, IndexRequest};

#[test]
fn snapshot_bucket_subset_matches_route_document_membership() {
    let collection_id = "users";
    let source = Engine::new();
    source
        .create_collection(
            collection_id,
            CreateCollectionRequest {
                fields: BTreeMap::from([("email".into(), field(FieldType::Keyword))]),
            },
        )
        .unwrap();
    for i in 0..16 {
        let external_id = format!("doc-{i:02}");
        source
            .index(
                collection_id,
                IndexRequest {
                    request_id: None,
                    items: vec![item(
                        &external_id,
                        "email",
                        FieldValue::String(format!("{external_id}@example.com")),
                    )],
                },
            )
            .unwrap();
    }
    let snapshot = source.snapshot().unwrap();
    let virtual_bucket_count = 8;
    let buckets = BTreeSet::from([0u32, 3]);
    let scoped = snapshot_bucket_subset(&snapshot, virtual_bucket_count, &buckets).unwrap();

    let map = VirtualBucketShardMap::balanced(0, virtual_bucket_count, 1).unwrap();
    let expected: BTreeSet<String> = snapshot.collections[collection_id]
        .eid_fields
        .keys()
        .filter(|external_id| {
            buckets.contains(&map.route_document(collection_id, None, external_id).bucket)
        })
        .cloned()
        .collect();

    let got: BTreeSet<String> = scoped
        .collections
        .get(collection_id)
        .map(|c| c.eid_fields.keys().cloned().collect())
        .unwrap_or_default();
    assert_eq!(got, expected);
    assert!(!expected.is_empty(), "test buckets should select some docs");
    assert!(
        expected.len() < 16,
        "bucket-scoped export should not return everything"
    );
}

/// AC1 (#1457 R1): a bucket group whose id set ALONE serializes past a
/// small byte cap must still complete via multiple independently
/// bounded prune chunks (every chunk under the route's hard body
/// limit), and the union of every chunk's `keep_ids` for a
/// `(bucket, collection_id)` group reconstructs the full authoritative
/// set regardless of how many chunks it split into.
#[test]
fn snapshot_reshard_prune_chunks_splits_large_keep_set_by_bytes() {
    let collection_id = "docs";
    let source = Engine::new();
    source
        .create_collection(
            collection_id,
            CreateCollectionRequest {
                fields: BTreeMap::from([("email".into(), field(FieldType::Keyword))]),
            },
        )
        .unwrap();
    let ids: Vec<String> = (0..20_000)
        .map(|i| format!("doc-with-a-fairly-long-external-id-{i:06}"))
        .collect();
    for id in &ids {
        source
            .index(
                collection_id,
                IndexRequest {
                    request_id: None,
                    items: vec![item(
                        id,
                        "email",
                        FieldValue::String(format!("{id}@example.com")),
                    )],
                },
            )
            .unwrap();
    }
    let snapshot = source.snapshot().unwrap();
    let to = VirtualBucketShardMap::new(2, vec![0, 1], 2).unwrap();
    let buckets = BTreeSet::from([0u32, 1u32]);
    let collection_ids = BTreeSet::from([collection_id.to_string()]);
    const SMALL_CAP: usize = 64 * 1024;
    let chunks =
        snapshot_reshard_prune_chunks(&snapshot, &to, &buckets, &collection_ids, SMALL_CAP)
            .unwrap();

    assert!(
        chunks.len() > 2,
        "expected the large keep set to split into multiple chunks, got {}",
        chunks.len()
    );
    for chunk in &chunks {
        let wire_bytes = serde_json::to_vec(chunk).unwrap().len();
        assert!(
            wire_bytes < ADMIN_ROUTE_BODY_LIMIT_BYTES,
            "chunk serialized to {wire_bytes} bytes, over the route's {ADMIN_ROUTE_BODY_LIMIT_BYTES} byte body limit"
        );
    }

    let mut by_group: BTreeMap<(u32, String), Vec<&ReshardPruneChunk>> = BTreeMap::new();
    for chunk in &chunks {
        by_group
            .entry((chunk.bucket, chunk.collection_id.clone()))
            .or_default()
            .push(chunk);
    }
    let mut reconstructed: BTreeSet<String> = BTreeSet::new();
    for group in by_group.values() {
        let total = group[0].total_chunks;
        let mut seen: BTreeSet<u32> = BTreeSet::new();
        for c in group {
            assert_eq!(c.total_chunks, total);
            assert!(seen.insert(c.chunk_index), "duplicate chunk_index");
            reconstructed.extend(c.keep_ids.iter().cloned());
        }
        assert_eq!(seen, (0..total).collect::<BTreeSet<_>>());
    }
    assert_eq!(reconstructed, ids.iter().cloned().collect::<BTreeSet<_>>());
}

/// AC2 (#1457 R2, the edge #1443 disclosed): a `(bucket, collection_id)`
/// pair with zero matching docs in the snapshot — modeling a collection
/// whose every moved-bucket document was deleted before the final pass —
/// still gets exactly one chunk carrying an empty `keep_ids`, so a
/// receiver still prunes any stale copies it holds rather than the pair
/// being silently omitted because the snapshot has nothing to say about
/// it.
#[test]
fn snapshot_reshard_prune_chunks_emits_empty_scope_for_emptied_collection() {
    let empty_collection_id = "emptied";
    let populated_collection_id = "populated";
    let source = Engine::new();
    for collection_id in [empty_collection_id, populated_collection_id] {
        source
            .create_collection(
                collection_id,
                CreateCollectionRequest {
                    fields: BTreeMap::from([("email".into(), field(FieldType::Keyword))]),
                },
            )
            .unwrap();
    }
    source
        .index(
            populated_collection_id,
            IndexRequest {
                request_id: None,
                items: vec![item(
                    "doc-1",
                    "email",
                    FieldValue::String("doc-1@example.com".into()),
                )],
            },
        )
        .unwrap();
    // `empty_collection_id` stays empty: models a collection whose only
    // moved-bucket docs were all deleted before the final pass.
    let snapshot = source.snapshot().unwrap();
    let to = VirtualBucketShardMap::new(1, vec![0], 1).unwrap();
    let buckets = BTreeSet::from([0u32]);
    let collection_ids = BTreeSet::from([
        empty_collection_id.to_string(),
        populated_collection_id.to_string(),
    ]);
    let chunks =
        snapshot_reshard_prune_chunks(&snapshot, &to, &buckets, &collection_ids, MAX_BATCH_BYTES)
            .unwrap();

    let empty_chunks: Vec<&ReshardPruneChunk> = chunks
        .iter()
        .filter(|c| c.collection_id == empty_collection_id)
        .collect();
    assert_eq!(
        empty_chunks.len(),
        1,
        "an emptied collection must still get exactly one (empty) chunk"
    );
    assert_eq!(empty_chunks[0].total_chunks, 1);
    assert!(empty_chunks[0].keep_ids.is_empty());
    assert_eq!(empty_chunks[0].bucket, 0);

    let populated_chunks: Vec<&ReshardPruneChunk> = chunks
        .iter()
        .filter(|c| c.collection_id == populated_collection_id)
        .collect();
    assert_eq!(populated_chunks.len(), 1);
    assert_eq!(
        populated_chunks[0].keep_ids,
        BTreeSet::from(["doc-1".to_string()])
    );
}
