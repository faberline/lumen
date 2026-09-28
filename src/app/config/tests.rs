use crate::app::config::ClusterConfig;
use crate::sharding::infrastructure::shard_map_env::tests::ENV_LOCK;

fn cfg(shard_count: u32, replicas_per_shard: u32, voter_count: u32, pod: &str) -> ClusterConfig {
    ClusterConfig {
        shard_count,
        replicas_per_shard,
        voter_count,
        pod_name: pod.into(),
    }
}

#[test]
fn pod_ordinal_extracts_trailing_int() {
    assert_eq!(cfg(3, 3, 3, "lumen-0").pod_ordinal().unwrap(), 0);
    assert_eq!(cfg(3, 3, 3, "lumen-7").pod_ordinal().unwrap(), 7);
    assert_eq!(cfg(3, 3, 3, "lumen-42").pod_ordinal().unwrap(), 42);
}

#[test]
fn pod_ordinal_rejects_bad_suffix() {
    assert!(cfg(3, 3, 3, "lumen-").pod_ordinal().is_err());
    assert!(cfg(3, 3, 3, "lumen-abc").pod_ordinal().is_err());
    assert!(cfg(3, 3, 3, "lumen-3-foo").pod_ordinal().is_err());
}

#[test]
fn pod_ordinal_rejects_no_dash() {
    assert!(cfg(3, 3, 3, "lumen").pod_ordinal().is_err());
}

#[test]
fn shard_and_replica_indices_partition_correctly() {
    // 3 shards × 3 replicas: pod-0/3/6 → shard 0, pod-1/4/7 → shard 1, etc.
    let c = cfg(3, 3, 3, "lumen-7");
    assert_eq!(c.shard_index().unwrap(), 1);
    assert_eq!(c.replica_index().unwrap(), 2);
    assert!(c.is_voter().unwrap());

    let c = cfg(3, 3, 2, "lumen-8");
    assert_eq!(c.shard_index().unwrap(), 2);
    assert_eq!(c.replica_index().unwrap(), 2);
    assert!(
        !c.is_voter().unwrap(),
        "replica 2 is a learner when voter_count=2"
    );
}

#[test]
fn from_env_round_trips() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    unsafe {
        std::env::set_var("SHARD_COUNT", "3");
        std::env::set_var("REPLICAS_PER_SHARD", "3");
        std::env::set_var("VOTER_COUNT", "3");
        std::env::set_var("POD_NAME", "lumen-4");
    }
    let cfg = ClusterConfig::from_env().unwrap();
    assert_eq!(cfg.shard_count, 3);
    assert_eq!(cfg.replicas_per_shard, 3);
    assert_eq!(cfg.voter_count, 3);
    assert_eq!(cfg.pod_name, "lumen-4");
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
        std::env::remove_var("VOTER_COUNT");
        std::env::remove_var("POD_NAME");
    }
}

#[test]
fn from_env_errors_on_missing_var() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
        std::env::remove_var("VOTER_COUNT");
        std::env::remove_var("POD_NAME");
    }
    assert!(ClusterConfig::from_env().is_err());
}

#[test]
fn from_env_errors_on_non_u32() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("SHARD_COUNT", "not-a-number");
        std::env::set_var("REPLICAS_PER_SHARD", "3");
        std::env::set_var("VOTER_COUNT", "3");
        std::env::set_var("POD_NAME", "lumen-0");
    }
    assert!(ClusterConfig::from_env().is_err());
    unsafe {
        std::env::remove_var("SHARD_COUNT");
        std::env::remove_var("REPLICAS_PER_SHARD");
        std::env::remove_var("VOTER_COUNT");
        std::env::remove_var("POD_NAME");
    }
}
