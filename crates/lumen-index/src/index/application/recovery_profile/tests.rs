//! The recovery profile: opt-in, it keeps only aggregate counts and timings,
//! and a phase runs its work whether the profile is enabled or not.

use std::time::Duration;

use crate::index::application::recovery_profile::{RecoveryPhase, RecoveryProfile};

#[test]
fn recovery_profile_is_opt_in_and_keeps_only_aggregate_counts_and_timings() {
    let disabled = RecoveryProfile::new(false);
    disabled.collection_opened(Duration::from_millis(9));
    assert!(
        disabled.snapshot().is_none(),
        "disabled recovery must not report"
    );

    let profile = RecoveryProfile::for_test();
    profile.collection_opened(Duration::from_millis(3));
    profile.collection_opened(Duration::from_millis(9));
    profile.vector_opened(
        crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        Duration::from_millis(2),
    );
    profile.vector_opened(
        crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        Duration::from_millis(4),
    );
    profile.coverage_rebuilt(Duration::from_millis(5));

    let report = profile.snapshot().expect("explicit opt-in must report");
    assert_eq!(report.collection_open_count, 2);
    assert_eq!(report.collection_open_total_ms, 12);
    assert_eq!(report.collection_open_max_ms, 9);
    assert_eq!(report.vector_flat_open_count, 1);
    assert_eq!(report.vector_flat_open_ms, 2);
    assert_eq!(report.vector_hnsw_open_count, 1);
    assert_eq!(report.vector_hnsw_open_ms, 4);
    assert_eq!(report.coverage_rebuild_ms, 5);
}

#[test]
fn recovery_phase_start_runs_work_for_enabled_and_disabled_profiles() {
    let disabled = RecoveryProfile::new(false);
    let mut disabled_work = false;
    let disabled_result = disabled.phase_start(RecoveryPhase::CheckpointHnswGraph, || {
        disabled_work = true;
        7
    });
    assert_eq!(disabled_result, 7);
    assert!(disabled_work);
    assert!(disabled.snapshot().is_none());

    let enabled = RecoveryProfile::for_test();
    let mut enabled_work = false;
    let enabled_result = enabled.phase_start(RecoveryPhase::CheckpointHnswGraph, || {
        enabled_work = true;
        11
    });
    assert_eq!(enabled_result, 11);
    assert!(enabled_work);
    assert!(enabled.snapshot().is_some());
}
