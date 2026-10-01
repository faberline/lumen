use std::time::Duration;

use crate::app::observability::metrics::labels::{ApplyKind, CoordinatorStage, MergeStep};
use crate::app::observability::metrics::Metrics;

#[test]
fn coordinator_stage_histogram_renders_kind_and_stage_labels() {
    let metrics = Metrics::new();
    for stage in CoordinatorStage::ALL {
        metrics.observe_coordinator_stage(ApplyKind::Index, stage, Duration::from_millis(2));
    }
    let out = metrics.render();
    for stage in CoordinatorStage::ALL {
        let label = stage.label();
        assert!(out.contains(&format!(
            "lumen_coordinator_stage_seconds_count{{kind=\"index\",stage=\"{label}\"}} 1"
        )));
        assert!(out.contains(&format!(
            "lumen_coordinator_stage_seconds_sum{{kind=\"index\",stage=\"{label}\"}} 0.002"
        )));
    }
    assert!(out.contains("# TYPE lumen_coordinator_stage_seconds histogram"));
}

#[test]
fn render_emits_every_metric() {
    let m = Metrics::new();
    m.incr_index(3, 100);
    m.observe_search(Duration::from_millis(7));
    m.set_raft_leader_known(2, true);
    m.set_reshard_fence_active(true);
    let out = m.render();
    for name in [
        "lumen_index_writes_total",
        // #2519: deprecated back-compat series must still be present.
        "lumen_search_latency_ms_sum",
        "lumen_search_latency_ms_count",
        // #2519: the new histogram + slow-query series.
        "lumen_search_latency_seconds_bucket",
        "lumen_search_latency_seconds_sum",
        "lumen_search_latency_seconds_count",
        "lumen_slow_queries_total",
        "lumen_storage_bytes",
        "lumen_posting_cache_hits_total",
        "lumen_replace_fields_skipped_total",
        "lumen_shard_map_version",
        "lumen_scatter_map_version_mismatches_total",
        "lumen_reshard_fence_active",
        "lumen_reshard_fence_armed_unixtime",
        "lumen_raft_leader_known",
        "lumen_storage_degraded",
        "lumen_storage_full_errors_total",
        "lumen_segment_checkpoint_completed_total",
        "lumen_segment_checkpoint_started_total",
        "lumen_segment_checkpoint_failed_total",
        "lumen_segment_checkpoint_in_flight",
        "lumen_segment_checkpoint_duration_seconds_count",
        "lumen_segment_checkpoint_duration_seconds_sum",
        "lumen_segment_capture_lock_seconds_count",
        "lumen_segment_capture_lock_seconds_sum",
        "lumen_segment_checkpoint_bytes_total",
        "lumen_segment_merge_completed_total",
        "lumen_segment_merge_read_bytes_total",
        "lumen_segment_merge_write_bytes_total",
        "lumen_segment_pending_delta_bytes",
        "lumen_segment_pending_delta_layers",
        "lumen_segment_backpressure_total",
        "lumen_segment_disk_bytes",
        "lumen_process_rss_high_water_bytes",
        "lumen_process_rss_high_water_available",
        "lumen_pending_change_reserved_bytes",
        "lumen_pending_change_active_bytes",
        "lumen_pending_change_frozen_bytes",
        "lumen_pending_change_total_bytes",
        "lumen_pending_change_high_water_bytes",
    ] {
        assert!(out.contains(name), "expected {name} in:\n{out}");
    }
    assert!(
        out.contains("lumen_raft_leader_known{shard=\"2\"} 1"),
        "expected labeled raft series in:\n{out}"
    );
}

/// #2475: a pod whose raft election-state poller never ticked (every
/// standalone/non-raft deployment) must not publish
/// `lumen_raft_leader_known` at all — a permanent `0` there would be
/// indistinguishable from a genuinely stuck leaderless shard to
/// `LumenRaftLeaderAbsent`.
#[test]
fn render_omits_raft_leader_known_when_never_set() {
    let m = Metrics::new();
    let out = m.render();
    assert!(
        !out.contains("lumen_raft_leader_known"),
        "unexpected raft series in:\n{out}"
    );
}

/// #2516: `mark_storage_degraded` flips the gauge to `1` and counts the
/// hit; `clear_storage_degraded` (the periodic re-probe) flips it back
/// without touching the counter — the counter is a lifetime total, not
/// a "currently degraded" signal.
#[test]
fn storage_degraded_marks_and_clears() {
    let m = Metrics::new();
    assert!(!m.is_storage_degraded());
    assert_eq!(m.storage_full_errors_total.get(), 0);

    m.mark_storage_degraded();
    assert!(m.is_storage_degraded());
    assert_eq!(m.storage_full_errors_total.get(), 1);

    // A second hit while already degraded still counts, but the gauge
    // stays at 1 (sticky, not a counter).
    m.mark_storage_degraded();
    assert!(m.is_storage_degraded());
    assert_eq!(m.storage_full_errors_total.get(), 2);

    m.clear_storage_degraded();
    assert!(!m.is_storage_degraded());
    assert_eq!(
        m.storage_full_errors_total.get(),
        2,
        "clearing degraded mode must not reset the lifetime error counter"
    );
}

/// Byte-identical golden-render check (#974): fixed inputs must
/// reproduce the exact pre-refactor `render()` capture, byte for
/// byte — this is the AC2 contract the observability EC claim
/// (`lumen_claim_observability_prometheus_metrics`) depends on.
#[test]
fn render_is_byte_identical_to_pre_refactor_capture() {
    let m = Metrics::new();
    m.incr_index(3, 100);
    m.observe_search(Duration::from_millis(7));
    m.observe_search(Duration::from_millis(9));
    m.incr_duplicates();
    m.incr_collection_created(4);
    m.set_storage_bytes(2048);
    m.posting_cache_hits_total.add(5);
    m.posting_cache_misses_total.add(2);
    m.incr_replace_skipped(6);
    m.set_shard_map_version(3);
    m.incr_scatter_map_version_mismatch();
    // #2475: reach past the wall-clock-stamping setters and set the raw
    // gauges directly so this golden capture stays deterministic.
    m.reshard_fence_active.set(1);
    m.reshard_fence_armed_unixtime.set(1_700_000_000);
    m.set_raft_leader_known(2, true);
    m.mark_storage_degraded();
    let out = m.render();
    let golden = "# HELP lumen_index_writes_total Total index items applied.\n\
# TYPE lumen_index_writes_total counter\n\
lumen_index_writes_total 3\n\
# HELP lumen_index_bytes_total Total bytes written across all field indexes.\n\
# TYPE lumen_index_bytes_total counter\n\
lumen_index_bytes_total 100\n\
# HELP lumen_search_requests_total Total search requests served.\n\
# TYPE lumen_search_requests_total counter\n\
lumen_search_requests_total 2\n\
# HELP lumen_search_latency_ms_sum DEPRECATED (#2519): sum of search latencies in milliseconds; kept for dashboard back-compat, see lumen_search_latency_seconds for the real histogram.\n\
# TYPE lumen_search_latency_ms_sum counter\n\
lumen_search_latency_ms_sum 16\n\
# HELP lumen_search_latency_ms_count DEPRECATED (#2519): count of search latency observations; kept for dashboard back-compat, see lumen_search_latency_seconds for the real histogram.\n\
# TYPE lumen_search_latency_ms_count counter\n\
lumen_search_latency_ms_count 2\n\
# HELP lumen_slow_queries_total Total search queries at/above the slow-query threshold (LUMEN_SLOW_QUERY_MS, default 500ms).\n\
# TYPE lumen_slow_queries_total counter\n\
lumen_slow_queries_total 0\n\
# HELP lumen_duplicates_requests_total Total duplicate-detection requests.\n\
# TYPE lumen_duplicates_requests_total counter\n\
lumen_duplicates_requests_total 1\n\
# HELP lumen_collections_created_total Total collections created or extended.\n\
# TYPE lumen_collections_created_total counter\n\
lumen_collections_created_total 1\n\
# HELP lumen_schema_fields_total Total field declarations registered.\n\
# TYPE lumen_schema_fields_total counter\n\
lumen_schema_fields_total 4\n\
# HELP lumen_storage_bytes Approximate bytes held by all in-memory field indexes.\n\
# TYPE lumen_storage_bytes gauge\n\
lumen_storage_bytes 2048\n\
# HELP lumen_posting_cache_hits_total Posting cache hit count (0 until LSM cache is wired).\n\
# TYPE lumen_posting_cache_hits_total counter\n\
lumen_posting_cache_hits_total 5\n\
# HELP lumen_posting_cache_misses_total Posting cache miss count.\n\
# TYPE lumen_posting_cache_misses_total counter\n\
lumen_posting_cache_misses_total 2\n\
# HELP lumen_replace_fields_skipped_total Total docs:replace fields skipped as unchanged no-ops.\n\
# TYPE lumen_replace_fields_skipped_total counter\n\
lumen_replace_fields_skipped_total 6\n\
# HELP lumen_shard_map_version This pod's live routed shard-map version (0 outside routed deployments).\n\
# TYPE lumen_shard_map_version gauge\n\
lumen_shard_map_version 3\n\
# HELP lumen_scatter_map_version_mismatches_total Scatter search sub-responses whose responding pod's map version differed from the sender's.\n\
# TYPE lumen_scatter_map_version_mismatches_total counter\n\
lumen_scatter_map_version_mismatches_total 1\n\
# HELP lumen_reshard_fence_active 1 while this pod believes a reshard-driver write fence is currently armed on it.\n\
# TYPE lumen_reshard_fence_active gauge\n\
lumen_reshard_fence_active 1\n\
# HELP lumen_reshard_fence_armed_unixtime Unix time the currently (or most recently) armed reshard write fence was armed at.\n\
# TYPE lumen_reshard_fence_armed_unixtime gauge\n\
lumen_reshard_fence_armed_unixtime 1700000000\n\
# HELP lumen_storage_degraded 1 while this node is in ENOSPC degraded read-only mode (a durable write path hit disk-full).\n\
# TYPE lumen_storage_degraded gauge\n\
lumen_storage_degraded 1\n\
# HELP lumen_storage_full_errors_total Total genuine ENOSPC hits observed on a durable write path.\n\
# TYPE lumen_storage_full_errors_total counter\n\
lumen_storage_full_errors_total 1\n\
# HELP lumen_search_latency_seconds Search latency histogram in seconds, bucketed for search SLOs.\n\
# TYPE lumen_search_latency_seconds histogram\n\
lumen_search_latency_seconds_bucket{le=\"0.001\"} 0\n\
lumen_search_latency_seconds_bucket{le=\"0.0025\"} 0\n\
lumen_search_latency_seconds_bucket{le=\"0.005\"} 0\n\
lumen_search_latency_seconds_bucket{le=\"0.01\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"0.025\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"0.05\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"0.1\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"0.25\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"0.5\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"1\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"2.5\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"5\"} 2\n\
lumen_search_latency_seconds_bucket{le=\"+Inf\"} 2\n\
lumen_search_latency_seconds_sum 0.016\n\
lumen_search_latency_seconds_count 2\n\
# HELP lumen_raft_leader_known 1 while this pod's raft election-state poll believes its shard currently has an elected leader, 0 otherwise. Omitted for standalone/non-raft deployments.\n\
# TYPE lumen_raft_leader_known gauge\n\
lumen_raft_leader_known{shard=\"2\"} 1\n";
    assert!(
        out.starts_with(golden),
        "the established metric surface diverged before the appended telemetry (#2475 added \
             lumen_reshard_fence_active + lumen_reshard_fence_armed_unixtime + \
             lumen_raft_leader_known; \
             #2519 added lumen_slow_queries_total + the \
             lumen_search_latency_seconds histogram, and marked \
             lumen_search_latency_ms_sum/_count deprecated in HELP text; \
             #2516 added lumen_storage_degraded + lumen_storage_full_errors_total)"
    );
    let appended = &out[golden.len()..];
    for name in [
        "lumen_segment_checkpoint_completed_total",
        "lumen_segment_checkpoint_duration_seconds",
        "lumen_segment_capture_lock_seconds",
        "lumen_segment_checkpoint_bytes_total",
        "lumen_segment_merge_completed_total",
        "lumen_segment_merge_read_bytes_total",
        "lumen_segment_merge_write_bytes_total",
        "lumen_segment_pending_delta_bytes",
        "lumen_segment_pending_delta_layers",
        "lumen_segment_backpressure_total",
        "lumen_segment_disk_bytes",
        "lumen_process_rss_high_water_bytes",
        "lumen_process_rss_high_water_available",
        "lumen_pending_change_reserved_bytes",
        "lumen_pending_change_active_bytes",
        "lumen_pending_change_frozen_bytes",
        "lumen_pending_change_total_bytes",
        "lumen_pending_change_high_water_bytes",
        "lumen_coordinator_apply_seconds",
        "lumen_coordinator_apply_items_total",
        "lumen_engine_state_write_lock_wait_seconds",
        "lumen_hnsw_add_seconds",
    ] {
        assert!(
            appended.contains(name),
            "missing appended {name} in:\n{appended}"
        );
    }
}

/// A merge job's per-step cost must be readable from the same `/metrics`
/// surface production scrapes: one labelled `phase` row per
/// [`MergeStep`], plus the file and field counts that say whether a slow
/// job was wide (root file count) or deep (payload).
#[test]
fn merge_step_observations_render_one_labelled_phase_row_per_step() {
    let m = Metrics::new();
    m.observe_segment_merge_step(MergeStep::CaptureBefore, Duration::from_micros(1_000), 0);
    m.observe_segment_merge_step(MergeStep::LinkScratch, Duration::from_micros(2_000), 7);
    m.observe_segment_merge_step(MergeStep::LinkGeneration, Duration::from_micros(4_000), 9);
    m.observe_segment_merge_step(MergeStep::Total, Duration::from_micros(8_000), 0);
    m.observe_segment_merge_save_gate(Duration::from_micros(6_000));
    m.incr_segment_merge_fields(3);

    assert_eq!(
        m.segment_merge_step_observation(MergeStep::LinkScratch),
        (2_000, 1, 7),
        "the registry must read back exactly what was observed"
    );
    assert_eq!(
        m.segment_merge_step_observation(MergeStep::Compact),
        (0, 0, 0),
        "an unobserved step reads back as zero, not as another step's value"
    );

    let out = m.render();
    for step in MergeStep::ALL {
        let name = step.name();
        assert!(
            out.contains(&format!(
                "lumen_segment_merge_phase_seconds_count{{phase=\"{name}\"}}"
            )),
            "missing phase row {name} in:\n{out}"
        );
        assert!(
            out.contains(&format!(
                "lumen_segment_merge_phase_files_total{{phase=\"{name}\"}}"
            )),
            "missing phase file row {name} in:\n{out}"
        );
    }
    for expected in [
        "lumen_segment_merge_phase_seconds_sum{phase=\"link_scratch\"} 0.002",
        "lumen_segment_merge_phase_seconds_count{phase=\"link_scratch\"} 1",
        "lumen_segment_merge_phase_files_total{phase=\"link_scratch\"} 7",
        "lumen_segment_merge_phase_seconds_sum{phase=\"total\"} 0.008",
        "lumen_segment_merge_phase_files_total{phase=\"compact\"} 0",
        "lumen_segment_merge_linked_files_total 16",
        "lumen_segment_merge_fields_total 3",
        "lumen_segment_merge_save_gate_held_seconds_sum 0.006",
        "lumen_segment_merge_save_gate_held_seconds_count 1",
    ] {
        assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
    }
}
