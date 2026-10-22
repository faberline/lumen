use std::time::Duration;

use crate::app::observability::metrics::Metrics;

#[test]
fn committed_apply_wait_and_hnsw_add_histograms_render_fixed_buckets() {
    let metrics = Metrics::new();
    let initial = metrics.render();
    for expected in [
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"0.001\"} 0",
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"+Inf\"} 0",
        "lumen_engine_state_write_lock_wait_seconds_sum 0",
        "lumen_engine_state_write_lock_wait_seconds_count 0",
        "lumen_hnsw_add_seconds_bucket{le=\"0.001\"} 0",
        "lumen_hnsw_add_seconds_bucket{le=\"+Inf\"} 0",
        "lumen_hnsw_add_seconds_sum 0",
        "lumen_hnsw_add_seconds_count 0",
    ] {
        assert!(
            initial.contains(expected),
            "missing initial {expected:?} in:\n{initial}"
        );
    }

    metrics.observe_engine_state_write_lock_wait(Duration::from_millis(2));
    metrics.observe_hnsw_add(Duration::from_secs(11));
    let out = metrics.render();
    for expected in [
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"0.001\"} 0",
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"0.005\"} 1",
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"10\"} 1",
        "lumen_engine_state_write_lock_wait_seconds_bucket{le=\"+Inf\"} 1",
        "lumen_engine_state_write_lock_wait_seconds_sum 0.002",
        "lumen_engine_state_write_lock_wait_seconds_count 1",
        "lumen_hnsw_add_seconds_bucket{le=\"0.001\"} 0",
        "lumen_hnsw_add_seconds_bucket{le=\"10\"} 0",
        "lumen_hnsw_add_seconds_bucket{le=\"+Inf\"} 1",
        "lumen_hnsw_add_seconds_sum 11",
        "lumen_hnsw_add_seconds_count 1",
    ] {
        assert!(
            out.contains(expected),
            "missing observed {expected:?} in:\n{out}"
        );
    }
}

#[test]
fn capacity_timing_histograms_split_lock_wait_hold_and_relief_cycle() {
    let metrics = Metrics::new();
    metrics.observe_engine_state_write_lock_hold(Duration::from_millis(3));
    metrics.observe_hnsw_write_lock_wait(Duration::from_millis(5));
    metrics.observe_hnsw_write_lock_hold(Duration::from_millis(7));
    metrics.observe_hnsw_graph_rebuild(Duration::from_millis(9));
    metrics.observe_capacity_relief_checkpoint_merge_cycle(Duration::from_millis(11));

    let out = metrics.render();
    for expected in [
        "lumen_engine_state_write_lock_held_seconds_sum 0.003",
        "lumen_engine_state_write_lock_held_seconds_count 1",
        "lumen_hnsw_write_lock_wait_seconds_sum 0.005",
        "lumen_hnsw_write_lock_wait_seconds_count 1",
        "lumen_hnsw_write_lock_held_seconds_sum 0.007",
        "lumen_hnsw_write_lock_held_seconds_count 1",
        "lumen_hnsw_graph_rebuild_seconds_sum 0.009",
        "lumen_hnsw_graph_rebuild_seconds_count 1",
        "lumen_segment_capacity_relief_checkpoint_merge_seconds_sum 0.011",
        "lumen_segment_capacity_relief_checkpoint_merge_seconds_count 1",
    ] {
        assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
    }
}

#[test]
fn checkpoint_attempt_metrics_render_initial_and_failed_values() {
    let metrics = Metrics::new();
    let initial = metrics.render();
    for expected in [
        "lumen_segment_checkpoint_started_total 0",
        "lumen_segment_checkpoint_failed_total 0",
        "lumen_segment_checkpoint_in_flight 0",
    ] {
        assert!(
            initial.contains(expected),
            "missing initial {expected:?} in:\n{initial}"
        );
    }

    metrics.start_segment_checkpoint_attempt();
    assert_eq!(metrics.segment_checkpoint_in_flight.get(), 1);
    metrics.finish_segment_checkpoint_attempt(true);
    let out = metrics.render();
    for expected in [
        "lumen_segment_checkpoint_started_total 1",
        "lumen_segment_checkpoint_failed_total 1",
        "lumen_segment_checkpoint_in_flight 0",
    ] {
        assert!(
            out.contains(expected),
            "missing observed {expected:?} in:\n{out}"
        );
    }
}

#[test]
fn durable_segment_observations_render_required_totals_and_gauges() {
    let m = Metrics::new();
    m.observe_segment_checkpoint(13, Duration::from_micros(2_500), Duration::from_micros(500));
    m.observe_segment_checkpoint(19, Duration::from_micros(1_250), Duration::from_micros(750));
    m.observe_segment_merge(23, 29);
    m.observe_segment_merge(31, 37);
    m.set_segment_pending_delta(41, 2);
    m.incr_segment_backpressure();
    m.set_segment_disk_bytes(43);

    let out = m.render();
    for expected in [
        "lumen_segment_checkpoint_completed_total 2",
        "lumen_segment_checkpoint_bytes_total 32",
        "lumen_segment_checkpoint_duration_seconds_sum 0.00375",
        "lumen_segment_checkpoint_duration_seconds_count 2",
        "lumen_segment_capture_lock_seconds_sum 0.00125",
        "lumen_segment_capture_lock_seconds_count 2",
        "lumen_segment_merge_completed_total 2",
        "lumen_segment_merge_read_bytes_total 54",
        "lumen_segment_merge_write_bytes_total 66",
        "lumen_segment_pending_delta_bytes 41",
        "lumen_segment_pending_delta_layers 2",
        "lumen_segment_backpressure_total 1",
        "lumen_segment_disk_bytes 43",
    ] {
        assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
    }
}
