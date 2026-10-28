//! Exercise the public compatibility paths as a caller would after the DDD split.

use crate::routing::{SearchShardTarget, VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT};
use crate::storage::Engine;
use crate::types::{CreateCollectionRequest, IndexRequest, SearchRequest};
use serde_json::json;

#[test]
fn public_routing_preserves_bucket_identity_across_map_versions() {
    let before = VirtualBucketShardMap::balanced(17, DEFAULT_VIRTUAL_BUCKET_COUNT, 4).unwrap();
    let route = before.route_document("products", None, "sku-42");
    // A fixed persisted routing example detects a hash, separator or bucket-count change.
    assert_eq!(route.bucket, 3470);
    assert_eq!(route.shard, 2);
    assert_eq!(route.map_version, 17);
    let after = before.split_one_shard(18).unwrap();
    let moved = after.route_document("products", None, "sku-42");
    assert_eq!(moved.bucket, route.bucket);
    assert_eq!(moved.map_version, 18);
    assert_eq!(moved.physical_shard_count, 5);
    assert_eq!(
        after.route_document("products", Some("sku-42"), "another-document"),
        moved
    );
    assert_eq!(
        after.search_target("products", None),
        SearchShardTarget::All
    );
    assert_eq!(
        after.search_target("products", Some("sku-42")),
        SearchShardTarget::One(moved)
    );
}

#[test]
fn public_schema_index_and_query_types_keep_exact_keyword_bounds() {
    let engine = Engine::new();
    let schema: CreateCollectionRequest =
        serde_json::from_value(json!({"fields":{"key":{"type":"keyword"}}})).unwrap();
    engine.create_collection("products", schema).unwrap();
    let input: IndexRequest = serde_json::from_value(json!({"items":[
        {"external_id":"ten", "field":"key", "value":"10"},
        {"external_id":"two", "field":"key", "value":"2"},
        {"external_id":"upper", "field":"key", "value":"A"},
        {"external_id":"lower", "field":"key", "value":"a"}
    ]}))
    .unwrap();
    engine.index("products", input).unwrap();

    let range: SearchRequest = serde_json::from_value(json!({
        "query":{"range":{"field":"key", "gte":"10", "lt":"A"}}
    }))
    .unwrap();
    let result = engine.search("products", range).unwrap();
    let mut ids: Vec<_> = result.hits.into_iter().map(|hit| hit.external_id).collect();
    ids.sort();
    assert_eq!(result.total, 2);
    assert_eq!(ids, ["ten", "two"]);

    let exact: SearchRequest = serde_json::from_value(json!({
        "query":{"term":{"field":"key", "value":"A"}}
    }))
    .unwrap();
    let result = engine.search("products", exact).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.hits[0].external_id, "upper");
}
