//! The durable segment telemetry render appends after the established families:
//! text row staging, segment checkpoints, merges, pending deltas, backpressure
//! and disk bytes, the process-wide gauges, and the checkpoint, lock, HNSW and
//! capacity-relief durations in seconds.

use metrics_prometheus::Sample;

use crate::app::observability::metrics::histogram::{
    render_apply_duration_histogram, render_duration_histogram,
};
use crate::app::observability::metrics::process::PendingChangeAccounting;
use crate::app::observability::metrics::Metrics;

impl Metrics {
    /// Append durable segment telemetry after every established metric family.
    /// The integer atomics use microseconds internally; the published metric
    /// names promise seconds, so conversion happens once here at scrape time.
    pub(super) fn render_segment_telemetry(
        &self,
        pending_changes: PendingChangeAccounting,
    ) -> String {
        let samples = [
            Sample::new("lumen_text_row_stage_rows_total", "counter",
                "Total immutable Text rows successfully prepared by this Engine.", self.text_row_stage_rows_total.get()),
            Sample::new("lumen_text_row_stage_input_bytes_total", "counter",
                "Total input bytes in immutable Text rows successfully prepared by this Engine.", self.text_row_stage_input_bytes_total.get()),
            Sample::new(
                "lumen_segment_checkpoint_completed_total",
                "counter",
                "Total successfully completed durable segment checkpoints.",
                self.segment_checkpoint_completed_total.get(),
            ),
            Sample::new(
                "lumen_segment_checkpoint_started_total",
                "counter",
                "Total durable segment checkpoint attempts that entered the shared checkpoint wrapper.",
                self.segment_checkpoint_started_total.get(),
            ),
            Sample::new(
                "lumen_segment_checkpoint_failed_total",
                "counter",
                "Total durable segment checkpoint attempts that returned an error from the shared checkpoint wrapper.",
                self.segment_checkpoint_failed_total.get(),
            ),
            Sample::new(
                "lumen_segment_checkpoint_in_flight",
                "gauge",
                "Current durable segment checkpoint attempts inside the shared checkpoint wrapper.",
                self.segment_checkpoint_in_flight.get(),
            ),
            Sample::new(
                "lumen_segment_checkpoint_bytes_total",
                "counter",
                "Total actual bytes durably published by completed segment checkpoints.",
                self.segment_checkpoint_bytes_total.get(),
            ),
            Sample::new(
                "lumen_segment_merge_completed_total",
                "counter",
                "Total successfully completed durable segment merges.",
                self.segment_merge_completed_total.get(),
            ),
            Sample::new(
                "lumen_segment_merge_read_bytes_total",
                "counter",
                "Total bytes read by successfully completed durable segment merges.",
                self.segment_merge_read_bytes_total.get(),
            ),
            Sample::new(
                "lumen_segment_merge_write_bytes_total",
                "counter",
                "Total bytes written by successfully completed durable segment merges.",
                self.segment_merge_write_bytes_total.get(),
            ),
            Sample::new(
                "lumen_segment_merge_linked_files_total",
                "counter",
                "Total files hard-linked by background segment merge jobs across both whole-root link passes.",
                self.segment_merge_linked_files_total.get(),
            ),
            Sample::new(
                "lumen_segment_merge_fields_total",
                "counter",
                "Total fields compacted by background segment merge jobs.",
                self.segment_merge_fields_total.get(),
            ),
            Sample::new(
                "lumen_segment_pending_delta_bytes",
                "gauge",
                "Current bytes in unmerged durable segment delta layers.",
                self.segment_pending_delta_bytes.get(),
            ),
            Sample::new(
                "lumen_segment_pending_delta_layers",
                "gauge",
                "Current count of unmerged durable segment delta layers.",
                self.segment_pending_delta_layers.get(),
            ),
            Sample::new(
                "lumen_segment_backpressure_total",
                "counter",
                "Total admissions delayed or refused by real segment backpressure.",
                self.segment_backpressure_total.get(),
            ),
            Sample::new(
                "lumen_segment_disk_bytes",
                "gauge",
                "Current durable segment files on disk in bytes.",
                self.segment_disk_bytes.get(),
            ),
            Sample::new(
                "lumen_process_rss_high_water_bytes",
                "gauge",
                "Linux process peak resident set size from /proc/self/status VmHWM in bytes; require lumen_process_rss_high_water_available=1.",
                self.process_rss_high_water_bytes.get(),
            ),
            Sample::new(
                "lumen_process_rss_high_water_available",
                "gauge",
                "1 when this scrape parsed Linux /proc/self/status VmHWM, 0 when unavailable.",
                self.process_rss_high_water_available.get(),
            ),
            Sample::new(
                "lumen_pending_change_reserved_bytes",
                "gauge",
                "Process-wide bytes reserved before apply for pending durable changes.",
                pending_changes.reserved,
            ),
            Sample::new(
                "lumen_pending_change_active_bytes",
                "gauge",
                "Process-wide active bytes owned by pending durable changes.",
                pending_changes.active,
            ),
            Sample::new(
                "lumen_pending_change_frozen_bytes",
                "gauge",
                "Process-wide frozen bytes retained by incomplete durable checkpoints.",
                pending_changes.frozen,
            ),
            Sample::new(
                "lumen_pending_change_total_bytes",
                "gauge",
                "Process-wide pending durable-change bytes across all ownership states.",
                pending_changes.total,
            ),
            Sample::new(
                "lumen_pending_change_high_water_bytes",
                "gauge",
                "Largest observed process-wide pending durable-change total in bytes.",
                pending_changes.high_water,
            ),
        ];
        let mut out = metrics_prometheus::render(&samples);
        render_duration_histogram(
            &mut out,
            "lumen_segment_checkpoint_duration_seconds",
            "Total duration of completed durable segment checkpoints in seconds.",
            self.segment_checkpoint_duration_us_sum.get(),
            self.segment_checkpoint_duration_count.get(),
        );
        render_duration_histogram(
            &mut out,
            "lumen_segment_capture_lock_seconds",
            "Total capture-lock hold time of completed durable segment checkpoints in seconds.",
            self.segment_capture_lock_us_sum.get(),
            self.segment_capture_lock_count.get(),
        );
        render_duration_histogram(
            &mut out,
            "lumen_segment_merge_save_gate_held_seconds",
            "Total time background segment merge jobs held the publication save gate in seconds.",
            self.segment_merge_save_gate_us_sum.get(),
            self.segment_merge_save_gate_count.get(),
        );
        out.push_str(&self.render_merge_phase_breakdown());
        out.push_str(&self.render_coordinator_apply_histogram());
        out.push_str(&self.render_coordinator_stage_histogram());
        render_apply_duration_histogram(
            &mut out,
            "lumen_engine_state_write_lock_wait_seconds",
            "Time spent waiting to acquire the committed-apply engine state write lock, in seconds.",
            &self.engine_state_write_lock_wait_seconds_buckets,
            &self.engine_state_write_lock_wait_seconds_us_sum,
            &self.engine_state_write_lock_wait_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_engine_state_write_lock_held_seconds",
            "Time the committed-apply engine state write lock was held, in seconds.",
            &self.engine_state_write_lock_held_seconds_buckets,
            &self.engine_state_write_lock_held_seconds_us_sum,
            &self.engine_state_write_lock_held_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_hnsw_add_seconds",
            "Time spent in live HNSW graph additions during committed apply, in seconds.",
            &self.hnsw_add_seconds_buckets,
            &self.hnsw_add_seconds_us_sum,
            &self.hnsw_add_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_hnsw_graph_rebuild_seconds",
            "Time spent rebuilding an HNSW graph during a live add, in seconds.",
            &self.hnsw_graph_rebuild_seconds_buckets,
            &self.hnsw_graph_rebuild_seconds_us_sum,
            &self.hnsw_graph_rebuild_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_hnsw_write_lock_wait_seconds",
            "Time spent waiting to acquire an HNSW graph write lock, in seconds.",
            &self.hnsw_write_lock_wait_seconds_buckets,
            &self.hnsw_write_lock_wait_seconds_us_sum,
            &self.hnsw_write_lock_wait_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_hnsw_write_lock_held_seconds",
            "Time an HNSW graph write lock was held, in seconds.",
            &self.hnsw_write_lock_held_seconds_buckets,
            &self.hnsw_write_lock_held_seconds_us_sum,
            &self.hnsw_write_lock_held_seconds_count,
        );
        render_apply_duration_histogram(
            &mut out,
            "lumen_segment_capacity_relief_checkpoint_merge_seconds",
            "Time for successful capacity-relief checkpoint-plus-merge cycles, in seconds.",
            &self.segment_capacity_relief_checkpoint_merge_seconds_buckets,
            &self.segment_capacity_relief_checkpoint_merge_seconds_us_sum,
            &self.segment_capacity_relief_checkpoint_merge_seconds_count,
        );
        out
    }
}

#[cfg(test)]
mod tests;
