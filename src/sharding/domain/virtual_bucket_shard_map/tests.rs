use crate::sharding::domain::shard_index::{shard_host, shard_index};
use crate::sharding::domain::shard_route::SearchShardTarget;
use crate::sharding::domain::virtual_bucket_shard_map::{route_hash, VirtualBucketShardMap};

#[test]
fn shard_index_is_deterministic() {
    let a = shard_index("data-table:42", 3);
    let b = shard_index("data-table:42", 3);
    assert_eq!(a, b);
    assert!(a < 3);
}

#[test]
fn shard_index_spreads() {
    let mut seen = std::collections::HashSet::new();
    for i in 0..256 {
        seen.insert(shard_index(&format!("c:{i}"), 3));
    }
    assert!(seen.len() > 1, "shard hash collapsed to a single bucket");
}

#[test]
fn shard_index_single_shard_always_zero() {
    for s in ["a", "very-long-string", "中文"] {
        assert_eq!(shard_index(s, 1), 0);
    }
}

#[test]
fn virtual_bucket_map_preserves_single_shard_compatibility() {
    let map = VirtualBucketShardMap::balanced(7, 128, 1).unwrap();
    for external_id in ["a", "b", "large-collection-row"] {
        let route = map.route_document("catalog", None, external_id);
        assert_eq!(route.map_version, 7);
        assert_eq!(route.virtual_bucket_count, 128);
        assert_eq!(route.physical_shard_count, 1);
        assert_eq!(route.shard, 0);
    }
}

#[test]
fn virtual_bucket_map_distributes_one_large_collection_by_external_id() {
    let map = VirtualBucketShardMap::balanced(1, 1024, 8).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for i in 0..512 {
        seen.insert(
            map.route_document("one-big-collection", None, &format!("doc-{i}"))
                .shard,
        );
    }
    assert!(
        seen.len() > 1,
        "external_id routing collapsed one collection to one shard"
    );
}

#[test]
fn split_one_shard_moves_only_into_the_new_shard() {
    // 8 buckets, 2 balanced shards: shard0=[0,2,4,6], shard1=[1,3,5,7].
    let before = VirtualBucketShardMap::balanced(0, 8, 2).unwrap();
    let after = before.split_one_shard(1).unwrap();

    assert_eq!(after.version(), 1);
    assert_eq!(after.virtual_bucket_count(), 8);
    assert_eq!(after.physical_shard_count(), 3);

    let mut moved = Vec::new();
    for bucket in 0..8 {
        let old_shard = before.assignment_for_bucket(bucket).unwrap();
        let new_shard = after.assignment_for_bucket(bucket).unwrap();
        if old_shard != new_shard {
            // Every move must land on the brand-new shard, never on
            // another pre-existing shard.
            assert_eq!(new_shard, 2, "bucket {bucket} moved to an old shard");
            moved.push(bucket);
        }
    }
    // 4 buckets/shard, new_physical_shard_count=3 -> 4/3=1 bucket moves
    // from each of the 2 old shards = 2 buckets total, the lowest
    // bucket id on each source shard (0 from shard0, 1 from shard1).
    assert_eq!(moved, vec![0, 1]);
}

#[test]
fn split_one_shard_is_deterministic_and_idempotent_shape() {
    let map = VirtualBucketShardMap::balanced(3, 97, 5).unwrap();
    let a = map.split_one_shard(4).unwrap();
    let b = map.split_one_shard(4).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.physical_shard_count(), 6);
    // Every bucket must still resolve to a valid shard index.
    for bucket in 0..97 {
        assert!(a.assignment_for_bucket(bucket).unwrap() < 6);
    }
}

#[test]
fn split_one_shard_never_leaves_the_new_shard_empty_when_source_has_buckets() {
    let map = VirtualBucketShardMap::balanced(0, 64, 4).unwrap();
    let after = map.split_one_shard(1).unwrap();
    let new_shard_bucket_count = (0..64)
        .filter(|&b| after.assignment_for_bucket(b).unwrap() == 4)
        .count();
    assert!(
        new_shard_bucket_count > 0,
        "split produced an empty new shard"
    );
}

#[test]
fn versioned_bucket_maps_can_reassign_one_bucket() {
    let key = key_for_bucket("catalog", 4, 1);
    let before = VirtualBucketShardMap::new(1, vec![0, 0, 1, 1], 2).unwrap();
    let after = VirtualBucketShardMap::new(2, vec![0, 1, 1, 1], 2).unwrap();

    let old_route = before.route_key("catalog", &key);
    let new_route = after.route_key("catalog", &key);

    assert_eq!(old_route.bucket, 1);
    assert_eq!(new_route.bucket, 1);
    assert_eq!(old_route.shard, 0);
    assert_eq!(new_route.shard, 1);
    assert_eq!(old_route.map_version, 1);
    assert_eq!(new_route.map_version, 2);
}

#[test]
fn search_target_scatter_without_key_and_targets_with_key() {
    let map = VirtualBucketShardMap::balanced(3, 256, 4).unwrap();
    assert_eq!(map.search_target("catalog", None), SearchShardTarget::All);
    let SearchShardTarget::One(route) = map.search_target("catalog", Some("tenant-a")) else {
        panic!("routing key should target one shard");
    };
    assert!(route.shard < 4);
    assert_eq!(route.map_version, 3);
}

#[test]
fn shard_host_formats_dns() {
    let h = shard_host("lumen", 2, "lumen-peer");
    assert_eq!(h, "lumen-2.lumen-peer");
}

fn key_for_bucket(collection_id: &str, bucket_count: u32, desired_bucket: u32) -> String {
    for i in 0..10_000 {
        let key = format!("key-{i}");
        if route_hash(collection_id, &key) % bucket_count == desired_bucket {
            return key;
        }
    }
    panic!("could not find test key for bucket {desired_bucket}");
}
