use std::collections::BTreeMap;

use crate::operator::application::reshard_driver::tests::{lumen_with, spec, status_with_blocking};
use crate::operator::application::reshard_driver::trigger::{
    compute_target_map, current_shard_map, should_start_split,
};
use crate::operator::domain::lumen_spec::status::{LumenReshardStatus, LumenStatus};
use crate::operator::domain::lumen_spec::topology::{ReshardPhase, ReshardWorkflowSpec};

// ---- should_start_split (AC4 + R3) -------------------------------

#[test]
fn should_start_split_false_when_max_shard_bytes_unset() {
    let lumen = lumen_with(
        spec(1, 1, None),
        Some(status_with_blocking("urgentThresholdCrossed")),
    );
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_false_without_a_crossed_threshold() {
    let lumen = lumen_with(spec(1, 1, Some(1_000_000)), Some(LumenStatus::default()));
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_true_on_prepare_threshold_crossed() {
    let lumen = lumen_with(
        spec(1, 1, Some(1_000_000)),
        Some(status_with_blocking("prepareThresholdCrossed")),
    );
    assert!(should_start_split(&lumen));
}

#[test]
fn should_start_split_false_for_raft_ha() {
    let lumen = lumen_with(
        spec(2, 3, Some(1_000_000)),
        Some(status_with_blocking("urgentThresholdCrossed")),
    );
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_false_when_already_mid_workflow() {
    let mut s = spec(1, 1, Some(1_000_000));
    s.reshard_policy.workflow = ReshardWorkflowSpec {
        phase: ReshardPhase::Splitting,
        target_shard_count: Some(2),
        ..Default::default()
    };
    let lumen = lumen_with(s, Some(status_with_blocking("urgentThresholdCrossed")));
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_false_when_max_shards_reached() {
    let mut s = spec(4, 1, Some(1_000_000));
    s.reshard_policy.max_shards = Some(4);
    let lumen = lumen_with(s, Some(status_with_blocking("urgentThresholdCrossed")));
    assert!(!should_start_split(&lumen));
}

// ---- #1396 AC5: should_start_split re-derives freshness itself -----

#[test]
fn should_start_split_false_on_stale_status_map_version() {
    // A `blockingConditions` entry alone is not enough: if the status
    // subresource's `usageMeasuredAtMapVersion` predates the CR's
    // *current* `spec.shardMap.version` (a lagging/stale status write —
    // e.g. an in-flight scrape landing after a later cutover), the
    // trigger must refuse to fire even though the string condition is
    // present, regardless of what produced that stale status.
    let mut s = spec(2, 1, Some(1_000_000));
    s.shard_map.version = 1; // CR has already moved to map version 1.
    let status = LumenStatus {
        reshard: LumenReshardStatus {
            blocking_conditions: vec!["urgentThresholdCrossed".to_string()],
            usage_measured_at_map_version: Some(0), // stale: still map 0.
            ..Default::default()
        },
        ..Default::default()
    };
    let lumen = lumen_with(s, Some(status));
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_true_on_fresh_status_map_version() {
    // Same shape, but the status was measured at the CR's current map
    // version: a legitimate trigger and must still fire.
    let mut s = spec(2, 1, Some(1_000_000));
    s.shard_map.version = 1;
    let status = LumenStatus {
        reshard: LumenReshardStatus {
            blocking_conditions: vec!["urgentThresholdCrossed".to_string()],
            usage_measured_at_map_version: Some(1), // fresh: matches map 1.
            ..Default::default()
        },
        ..Default::default()
    };
    let lumen = lumen_with(s, Some(status));
    assert!(should_start_split(&lumen));
}

#[test]
fn should_start_split_false_with_no_status_yet() {
    let lumen = lumen_with(spec(1, 1, Some(1_000_000)), None);
    assert!(!should_start_split(&lumen));
}

// ---- #1386 AC1/AC2: post-cutover usage freshness -------------------

#[test]
fn should_start_split_false_on_stale_pre_cutover_usage() {
    // AC1: at `Complete` with a usage measurement whose generation
    // (`usageMeasuredAtMapVersion`) predates the CR's current
    // `shardMap.version` — the exact shape the shard-usage cache is in
    // for one scrape tick right after a split's cutover — the driver
    // must not start a split, regardless of how far past the urgent
    // threshold the (stale) cached percentage is.
    let mut s = spec(2, 1, Some(1_000_000));
    s.shard_map.version = 1; // just cut over to the post-split map
    let mut usage = BTreeMap::new();
    usage.insert(0u32, 900_000u64); // 90%, well past urgent(85%)
    let status = s.reshard_status_with_usage(&usage, 0 /* stale: pre-cutover */);
    assert_eq!(status.blocking_conditions, vec!["usageStalePostCutover"]);
    let lumen = lumen_with(
        s,
        Some(LumenStatus {
            reshard: status,
            ..Default::default()
        }),
    );
    assert!(!should_start_split(&lumen));
}

#[test]
fn should_start_split_true_on_fresh_post_cutover_usage_above_urgent() {
    // AC2: once the usage cache carries a measurement tagged with the
    // CR's *current* `shardMap.version`, a genuinely still-hot shard is
    // a legitimate cascade trigger and must start the next split.
    let mut s = spec(2, 1, Some(1_000_000));
    s.shard_map.version = 1;
    let mut usage = BTreeMap::new();
    usage.insert(1u32, 900_000u64); // 90%, past urgent(85%), fresh
    let status = s.reshard_status_with_usage(&usage, 1 /* fresh: matches shardMap.version */);
    assert_eq!(status.blocking_conditions, vec!["urgentThresholdCrossed"]);
    let lumen = lumen_with(
        s,
        Some(LumenStatus {
            reshard: status,
            ..Default::default()
        }),
    );
    assert!(should_start_split(&lumen));
}

// ---- current/target map helpers -----------------------------------

#[test]
fn current_shard_map_derives_balanced_map_from_shard_count_when_no_explicit_assignments() {
    let lumen = lumen_with(spec(2, 1, None), None);
    let map = current_shard_map(&lumen).unwrap();
    assert_eq!(map.physical_shard_count(), 2);
    assert_eq!(map.virtual_bucket_count(), 8);
}

#[test]
fn compute_target_map_grows_by_exactly_one_shard() {
    let lumen = lumen_with(spec(2, 1, None), None);
    let current = current_shard_map(&lumen).unwrap();
    let target = compute_target_map(&current).unwrap();
    assert_eq!(target.physical_shard_count(), 3);
    assert_eq!(target.version(), current.version() + 1);
}
