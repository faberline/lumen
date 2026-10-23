use std::sync::Mutex;

use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};
use crate::sharding::infrastructure::shard_map_env::{
    check_fan_in_shard_count, fan_in_shard_count, routed_activation_shard_count,
    routed_pod_topology, routed_shard_count_from_env, shard_map_from_env,
};

// Every test here and in `crate::app::config`'s tests that mutates process
// env (`ClusterConfig::from_env`'s SHARD_COUNT/REPLICAS_PER_SHARD/
// VOTER_COUNT/POD_NAME and `shard_map_from_env`'s SHARD_MAP_VERSION/
// SHARD_MAP_ASSIGNMENTS/VIRTUAL_BUCKET_COUNT) shares this one lock — env vars are
// process-global, and `cargo test`'s default parallel runner would
// otherwise interleave two tests' `set_var`/`remove_var` calls (a
// per-function-local `static LOCK` does *not* serialize across
// functions; each fn body owns a distinct static).
pub(crate) static ENV_LOCK: Mutex<()> = Mutex::new(());

// ---- shard_map_from_env (#1384) ------------------------------------

fn clear_shard_map_env() {
    unsafe {
        std::env::remove_var("SHARD_MAP_VERSION");
        std::env::remove_var("SHARD_MAP_ASSIGNMENTS");
        std::env::remove_var("VIRTUAL_BUCKET_COUNT");
    }
}

#[test]
fn shard_map_from_env_falls_back_to_balanced_when_unset() {
    // #1384 AC4: no shard-map env at all (today's default deployment,
    // and every deployment before the operator ever commits a real
    // split) must produce byte-identical routing to the pre-#1384
    // `VirtualBucketShardMap::balanced` construction.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_shard_map_env();

    let map = shard_map_from_env(4).unwrap();
    let expected = VirtualBucketShardMap::balanced(0, DEFAULT_VIRTUAL_BUCKET_COUNT, 4).unwrap();
    assert_eq!(map, expected);
    assert_eq!(map.version(), 0);
    assert_eq!(map.virtual_bucket_count(), DEFAULT_VIRTUAL_BUCKET_COUNT);
    assert_eq!(map.physical_shard_count(), 4);
}

#[test]
fn shard_map_from_env_falls_back_to_balanced_when_assignments_blank() {
    // The ConfigMap key is written unconditionally for
    // SHARD_MAP_VERSION/VIRTUAL_BUCKET_COUNT but SHARD_MAP_ASSIGNMENTS
    // is only ever present once assignments are non-empty
    // (`serving_configmap`) — an empty/whitespace value must not be
    // parsed as "one bucket assigned to shard \"\"".
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_shard_map_env();
    unsafe {
        std::env::set_var("SHARD_MAP_VERSION", "2");
        std::env::set_var("VIRTUAL_BUCKET_COUNT", "16");
        std::env::set_var("SHARD_MAP_ASSIGNMENTS", "  ");
    }

    let map = shard_map_from_env(4).unwrap();
    assert_eq!(map, VirtualBucketShardMap::balanced(2, 16, 4).unwrap());
    clear_shard_map_env();
}

#[test]
fn shard_map_from_env_honors_explicit_assignments() {
    // #1384 AC1: an explicit SHARD_MAP_ASSIGNMENTS (the cutover-flipped
    // map a driver split commits) is what a pod started after it must
    // route by — not the balanced default.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_shard_map_env();
    unsafe {
        std::env::set_var("SHARD_MAP_VERSION", "1");
        std::env::set_var("VIRTUAL_BUCKET_COUNT", "8");
        std::env::set_var("SHARD_MAP_ASSIGNMENTS", "1,1,1,1,0,0,0,0");
    }

    let map = shard_map_from_env(2).unwrap();
    assert_eq!(map.version(), 1);
    assert_eq!(map.virtual_bucket_count(), 8);
    assert_eq!(map.physical_shard_count(), 2);
    assert_eq!(map.assignment_for_bucket(0), Some(1));
    assert_eq!(map.assignment_for_bucket(4), Some(0));
    assert_ne!(
        map,
        VirtualBucketShardMap::balanced(1, 8, 2).unwrap(),
        "explicit assignments must override the balanced default"
    );
    clear_shard_map_env();
}

#[test]
fn shard_map_from_env_rejects_out_of_range_assignment() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_shard_map_env();
    unsafe {
        std::env::set_var("SHARD_MAP_ASSIGNMENTS", "0,1,2");
    }

    assert!(
        shard_map_from_env(2).is_err(),
        "bucket assigned to shard 2 with only 2 physical shards must error"
    );
    clear_shard_map_env();
}

#[test]
fn shard_map_from_env_rejects_non_numeric_version() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_shard_map_env();
    unsafe {
        std::env::set_var("SHARD_MAP_VERSION", "not-a-number");
    }

    assert!(shard_map_from_env(2).is_err());
    clear_shard_map_env();
}

// ---- fan_in_shard_count / check_fan_in_shard_count (#1398 R4) ------

#[test]
fn fan_in_shard_count_derives_from_loaded_dirs_when_unset() {
    // AC3: `--search-shard-segment-dirs a,b,c` with no SHARD_COUNT must
    // route across all three dirs, not clap's old default of 1.
    assert_eq!(fan_in_shard_count(None, 3), 3);
    assert_eq!(fan_in_shard_count(None, 1), 1);
}

#[test]
fn fan_in_shard_count_honors_explicit_value() {
    // An explicit --shard-count/SHARD_COUNT (including an explicit `1`,
    // which is indistinguishable from clap's removed default only by
    // being `Some`) always wins over the loaded-dir count.
    assert_eq!(fan_in_shard_count(Some(1), 3), 1);
    assert_eq!(fan_in_shard_count(Some(5), 3), 5);
}

#[test]
fn check_fan_in_shard_count_passes_when_counts_match() {
    let map = VirtualBucketShardMap::balanced(0, 16, 3).unwrap();
    assert!(check_fan_in_shard_count(&map, 3).is_ok());
}

#[test]
fn check_fan_in_shard_count_fails_fast_on_mismatch() {
    // AC3: a mismatched explicit count (here: a map built for 3 shards
    // but only 2 dirs actually loaded) must fail startup with a clear
    // message naming both numbers, not silently under-route.
    let map = VirtualBucketShardMap::balanced(0, 16, 3).unwrap();
    let err = check_fan_in_shard_count(&map, 2).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains('3'),
        "error should name the declared count: {msg}"
    );
    assert!(
        msg.contains('2'),
        "error should name the loaded-dir count: {msg}"
    );
}

// ---- routed_pod_topology / routed_shard_count_from_env (#1398) -----

#[test]
fn routed_pod_topology_derives_prefix_and_shard_index() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("POD_NAME", "search-5");
    }
    let (prefix, shard) = routed_pod_topology(4).unwrap();
    assert_eq!(prefix, "search");
    assert_eq!(shard, 1); // 5 % 4 == 1
    unsafe {
        std::env::remove_var("POD_NAME");
    }
}

#[test]
fn routed_pod_topology_matches_cluster_dims_math() {
    // Same rsplit_once('-') + `% shard_count` math as
    // `raft_runtime::cluster::ClusterDims::pod_ordinal`/`shard_index` —
    // must never drift apart.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("POD_NAME", "search-7");
    }
    let (_, shard) = routed_pod_topology(3).unwrap();
    let dims = raft_runtime::cluster::ClusterDims {
        shard_count: 3,
        replicas_per_shard: 1,
        voter_count: 1,
        pod_name: "search-7".to_string(),
    };
    assert_eq!(shard, dims.shard_index().unwrap());
    unsafe {
        std::env::remove_var("POD_NAME");
    }
}

#[test]
fn routed_pod_topology_rejects_missing_pod_name_and_bad_suffix() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::remove_var("POD_NAME");
    }
    assert!(routed_pod_topology(4).is_err());
    unsafe {
        std::env::set_var("POD_NAME", "search-abc");
    }
    assert!(routed_pod_topology(4).is_err());
    unsafe {
        std::env::remove_var("POD_NAME");
    }
}

#[test]
fn routed_pod_topology_rejects_zero_shard_count() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("POD_NAME", "search-0");
    }
    assert!(routed_pod_topology(0).is_err());
    unsafe {
        std::env::remove_var("POD_NAME");
    }
}

#[test]
fn routed_shard_count_from_env_none_when_unset_or_single_shard() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
    assert_eq!(routed_shard_count_from_env().unwrap(), None);
    unsafe {
        std::env::set_var("SHARD_COUNT", "1");
    }
    assert_eq!(
        routed_shard_count_from_env().unwrap(),
        None,
        "shardCount:1 must never activate routing (#1398 AC5)"
    );
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
}

#[test]
fn routed_shard_count_from_env_some_when_multi_shard() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
    }
    assert_eq!(routed_shard_count_from_env().unwrap(), Some(4));
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
}

#[test]
fn routed_shard_count_from_env_errors_on_non_u32() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "not-a-number");
    }
    assert!(routed_shard_count_from_env().is_err());
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
}

#[test]
fn routed_shard_count_from_env_none_when_replicas_per_shard_above_one() {
    // #1442 R3: a multi-replica-per-shard raft topology (each shard is
    // its own raft group) must never activate routing even with
    // SHARD_COUNT > 1 — `routed_pod_topology`'s ordinal math assumes
    // exactly one pod per shard.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
        std::env::set_var("REPLICAS_PER_SHARD", "3");
    }
    assert_eq!(routed_shard_count_from_env().unwrap(), None);
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
}

#[test]
fn routed_shard_count_from_env_some_when_replicas_per_shard_absent_or_one() {
    // #1442 R3: absent REPLICAS_PER_SHARD is treated the same as an
    // explicit `1` — the operator strips the env entirely at
    // `replicasPerShard <= 1` (`service_k8s::render::serving_statefulset`).
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
    assert_eq!(routed_shard_count_from_env().unwrap(), Some(4));
    unsafe {
        std::env::set_var("REPLICAS_PER_SHARD", "1");
    }
    assert_eq!(routed_shard_count_from_env().unwrap(), Some(4));
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
}

// ---- routed_activation_shard_count (#1442 R3) -----------------------

#[test]
fn routed_activation_shard_count_none_when_fan_in_dirs_present() {
    // A fan-in invocation (`--search-shard-segment-dirs` non-empty) must
    // never activate routed cross-pod routing, even when SHARD_COUNT
    // describes the routed topology — the two paths are structurally
    // mutually exclusive.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
    assert_eq!(
        routed_activation_shard_count(/* search_shard_segment_dirs_empty */ false).unwrap(),
        None
    );
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
}

#[test]
fn routed_activation_shard_count_none_for_raft_replica_topology() {
    // Raft-env (REPLICAS_PER_SHARD > 1) must never activate routing even
    // when the fan-in guard alone would allow it.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
        std::env::set_var("REPLICAS_PER_SHARD", "3");
    }
    assert_eq!(
        routed_activation_shard_count(/* search_shard_segment_dirs_empty */ true).unwrap(),
        None
    );
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
}

#[test]
fn routed_activation_shard_count_some_when_both_guards_clear() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "4");
        std::env::remove_var("REPLICAS_PER_SHARD");
    }
    assert_eq!(
        routed_activation_shard_count(/* search_shard_segment_dirs_empty */ true).unwrap(),
        Some(4)
    );
    unsafe {
        std::env::remove_var("SHARD_COUNT");
    }
}
