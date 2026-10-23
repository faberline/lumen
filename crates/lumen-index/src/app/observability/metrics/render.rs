//! Metrics::render, the Prometheus text `/metrics` serves: the same metric
//! names on every scrape, with the coordinator stage and apply histograms and
//! the merge phase breakdown, which it renders by hand.

use std::fmt::Write as _;

use metrics_prometheus::{Label, LabeledSample, Sample, SampleGroup};

use crate::app::observability::metrics::histogram::APPLY_SECONDS_BUCKETS_US;
use crate::app::observability::metrics::labels::{ApplyKind, CoordinatorStage, MergeStep};
use crate::app::observability::metrics::{Metrics, NOT_RAFT};
use crate::ingest::domain::change_budget::ChangeBudget;

impl Metrics {
    /// Prometheus text format (0.0.4 compatible). Always emits the same
    /// set of metric names so scrape configs are stable.
    pub fn render(&self) -> String {
        self.refresh_process_rss_high_water();
        let budget = ChangeBudget::process_shared();
        let pending_changes = self.read_pending_change_accounting(&budget);
        let samples = [
            Sample::new(
                "lumen_index_writes_total",
                "counter",
                "Total index items applied.",
                self.index_writes_total.get(),
            ),
            Sample::new(
                "lumen_index_bytes_total",
                "counter",
                "Total bytes written across all field indexes.",
                self.index_bytes_total.get(),
            ),
            Sample::new(
                "lumen_search_requests_total",
                "counter",
                "Total search requests served.",
                self.search_requests_total.get(),
            ),
            Sample::new(
                "lumen_search_latency_ms_sum",
                "counter",
                "DEPRECATED (#2519): sum of search latencies in milliseconds; kept for \
                 dashboard back-compat, see lumen_search_latency_seconds for the real \
                 histogram.",
                self.search_latency_ms_sum.get(),
            ),
            Sample::new(
                "lumen_search_latency_ms_count",
                "counter",
                "DEPRECATED (#2519): count of search latency observations; kept for \
                 dashboard back-compat, see lumen_search_latency_seconds for the real \
                 histogram.",
                self.search_latency_ms_count.get(),
            ),
            Sample::new(
                "lumen_slow_queries_total",
                "counter",
                "Total search queries at/above the slow-query threshold \
                 (LUMEN_SLOW_QUERY_MS, default 500ms).",
                self.slow_queries_total.get(),
            ),
            Sample::new(
                "lumen_duplicates_requests_total",
                "counter",
                "Total duplicate-detection requests.",
                self.duplicates_requests_total.get(),
            ),
            Sample::new(
                "lumen_collections_created_total",
                "counter",
                "Total collections created or extended.",
                self.collections_created_total.get(),
            ),
            Sample::new(
                "lumen_schema_fields_total",
                "counter",
                "Total field declarations registered.",
                self.schema_fields_total.get(),
            ),
            Sample::new(
                "lumen_storage_bytes",
                "gauge",
                "Approximate bytes held by all in-memory field indexes.",
                self.storage_bytes.get(),
            ),
            Sample::new(
                "lumen_posting_cache_hits_total",
                "counter",
                "Posting cache hit count (0 until LSM cache is wired).",
                self.posting_cache_hits_total.get(),
            ),
            Sample::new(
                "lumen_posting_cache_misses_total",
                "counter",
                "Posting cache miss count.",
                self.posting_cache_misses_total.get(),
            ),
            Sample::new(
                "lumen_replace_fields_skipped_total",
                "counter",
                "Total docs:replace fields skipped as unchanged no-ops.",
                self.replace_fields_skipped_total.get(),
            ),
            Sample::new(
                "lumen_shard_map_version",
                "gauge",
                "This pod's live routed shard-map version (0 outside routed deployments).",
                self.shard_map_version.get(),
            ),
            Sample::new(
                "lumen_scatter_map_version_mismatches_total",
                "counter",
                "Scatter search sub-responses whose responding pod's map version differed \
                 from the sender's.",
                self.scatter_map_version_mismatches_total.get(),
            ),
            Sample::new(
                "lumen_reshard_fence_active",
                "gauge",
                "1 while this pod believes a reshard-driver write fence is currently armed \
                 on it.",
                self.reshard_fence_active.get(),
            ),
            Sample::new(
                "lumen_reshard_fence_armed_unixtime",
                "gauge",
                "Unix time the currently (or most recently) armed reshard write fence was \
                 armed at.",
                self.reshard_fence_armed_unixtime.get(),
            ),
            Sample::new(
                "lumen_storage_degraded",
                "gauge",
                "1 while this node is in ENOSPC degraded read-only mode (a durable write \
                 path hit disk-full).",
                self.storage_degraded.get(),
            ),
            Sample::new(
                "lumen_storage_full_errors_total",
                "counter",
                "Total genuine ENOSPC hits observed on a durable write path.",
                self.storage_full_errors_total.get(),
            ),
        ];
        let mut out = metrics_prometheus::render(&samples);
        // #2519: real Prometheus histogram for search latency — hand-rolled
        // rather than via `metrics_prometheus::render_labeled` because a
        // histogram's `_bucket`/`_sum`/`_count` sample names all suffix ONE
        // base name under a single `# HELP`/`# TYPE histogram` pair, while
        // `render_labeled` assumes one bare metric name per row.
        out.push_str(&self.render_search_latency_histogram());
        // #2475: `lumen_raft_leader_known` carries a `shard` label and is
        // omitted entirely (not just left at 0) for standalone/non-raft
        // pods — see `raft_shard`'s doc comment for why a permanent-0
        // series would be a false-positive risk for `LumenRaftLeaderAbsent`.
        let shard = self.raft_shard.get();
        if shard != NOT_RAFT {
            let shard_label = shard.to_string();
            let rows = [LabeledSample::new(
                vec![Label::new("shard", &shard_label)],
                self.raft_leader_known.get(),
            )];
            let groups = [SampleGroup::new(
                "lumen_raft_leader_known",
                "gauge",
                "1 while this pod's raft election-state poll believes its shard currently \
                 has an elected leader, 0 otherwise. Omitted for standalone/non-raft \
                 deployments.",
                &rows,
            )];
            out.push_str(&metrics_prometheus::render_labeled(&groups));
        }
        out.push_str(&self.render_segment_telemetry(pending_changes));
        out
    }

    pub(super) fn render_coordinator_stage_histogram(&self) -> String {
        const NAME: &str = "lumen_coordinator_stage_seconds";
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP {NAME} Time spent in each admitted coordinator request stage, in seconds."
        );
        let _ = writeln!(out, "# TYPE {NAME} histogram");
        for stage in CoordinatorStage::ALL {
            for kind in ApplyKind::ALL {
                let stage_idx = stage.index();
                let kind_idx = kind.index();
                let mut cumulative = 0u64;
                for ((le, _), bucket) in APPLY_SECONDS_BUCKETS_US
                    .iter()
                    .zip(self.coordinator_stage_seconds_buckets[stage_idx][kind_idx].iter())
                {
                    cumulative += bucket.get();
                    let _ = writeln!(
                        out,
                        "{NAME}_bucket{{le=\"{le}\",kind=\"{}\",stage=\"{}\"}} {cumulative}",
                        kind.label(),
                        stage.label()
                    );
                }
                let total = self.coordinator_stage_seconds_count[stage_idx][kind_idx].get();
                let _ = writeln!(
                    out,
                    "{NAME}_bucket{{le=\"+Inf\",kind=\"{}\",stage=\"{}\"}} {total}",
                    kind.label(),
                    stage.label()
                );
                let sum = self.coordinator_stage_seconds_us_sum[stage_idx][kind_idx].get() as f64
                    / 1_000_000.0;
                let _ = writeln!(
                    out,
                    "{NAME}_sum{{kind=\"{}\",stage=\"{}\"}} {sum}",
                    kind.label(),
                    stage.label()
                );
                let _ = writeln!(
                    out,
                    "{NAME}_count{{kind=\"{}\",stage=\"{}\"}} {total}",
                    kind.label(),
                    stage.label()
                );
            }
        }
        out
    }

    /// #4326: render the `kind`-labelled `lumen_coordinator_apply_seconds`
    /// histogram and its `lumen_coordinator_apply_items_total` sibling
    /// counter — see [`Metrics::observe_coordinator_apply`].
    ///
    /// Hand-rolled for the same reason `render_search_latency_histogram`
    /// and `render_merge_phase_breakdown` are: a histogram's
    /// `_sum`/`_count`/`_bucket` names all suffix ONE base name under a
    /// single `# HELP`/`# TYPE histogram` pair, which
    /// `metrics_prometheus::render_labeled` does not model. Every kind in
    /// [`ApplyKind::ALL`] emits a row even at zero, so a scrape config
    /// never has to tolerate a kind appearing only after the first write of
    /// that shape.
    pub(super) fn render_coordinator_apply_histogram(&self) -> String {
        const SECONDS: &str = "lumen_coordinator_apply_seconds";
        const ITEMS: &str = "lumen_coordinator_apply_items_total";
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP {SECONDS} Time the write coordinator spent applying one admitted local record, in seconds."
        );
        let _ = writeln!(out, "# TYPE {SECONDS} histogram");
        for kind in ApplyKind::ALL {
            let idx = kind.index();
            let label = kind.label();
            let mut cumulative = 0u64;
            for ((le, _bound_us), bucket) in APPLY_SECONDS_BUCKETS_US
                .iter()
                .zip(self.coordinator_apply_seconds_buckets[idx].iter())
            {
                cumulative += bucket.get();
                let _ = writeln!(
                    out,
                    "{SECONDS}_bucket{{le=\"{le}\",kind=\"{label}\"}} {cumulative}"
                );
            }
            let total = self.coordinator_apply_seconds_count[idx].get();
            let _ = writeln!(
                out,
                "{SECONDS}_bucket{{le=\"+Inf\",kind=\"{label}\"}} {total}"
            );
            let sum_seconds = self.coordinator_apply_seconds_us_sum[idx].get() as f64 / 1_000_000.0;
            let _ = writeln!(out, "{SECONDS}_sum{{kind=\"{label}\"}} {sum_seconds}");
            let _ = writeln!(out, "{SECONDS}_count{{kind=\"{label}\"}} {total}");
        }
        let _ = writeln!(
            out,
            "# HELP {ITEMS} Items applied per write-coordinator apply, by RaftLogEntry kind."
        );
        let _ = writeln!(out, "# TYPE {ITEMS} counter");
        for kind in ApplyKind::ALL {
            let _ = writeln!(
                out,
                "{ITEMS}{{kind=\"{}\"}} {}",
                kind.label(),
                self.coordinator_apply_items_total[kind.index()].get()
            );
        }
        out
    }

    /// Render the per-[`MergeStep`] cost breakdown of background segment
    /// merge jobs: a `phase`-labelled histogram of step durations plus a
    /// `phase`-labelled counter of the filesystem entries each step touched.
    ///
    /// Hand-rolled for the same reason `render_search_latency_histogram` is:
    /// a histogram's `_sum`/`_count`/`_bucket` names all suffix ONE base name
    /// under a single `# HELP`/`# TYPE histogram` pair, which
    /// `metrics_prometheus::render_labeled` does not model. Every step in
    /// [`MergeStep::ALL`] emits a row even at zero, so a scrape config never
    /// has to tolerate a phase appearing only after the first slow job.
    pub(super) fn render_merge_phase_breakdown(&self) -> String {
        const SECONDS: &str = "lumen_segment_merge_phase_seconds";
        const FILES: &str = "lumen_segment_merge_phase_files_total";
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP {SECONDS} Per-phase duration of background segment merge jobs in seconds."
        );
        let _ = writeln!(out, "# TYPE {SECONDS} histogram");
        for step in MergeStep::ALL {
            let (micros, count, _) = self.segment_merge_step_observation(step);
            let phase = step.name();
            let seconds = micros as f64 / 1_000_000.0;
            let _ = writeln!(
                out,
                "{SECONDS}_bucket{{le=\"+Inf\",phase=\"{phase}\"}} {count}"
            );
            let _ = writeln!(out, "{SECONDS}_sum{{phase=\"{phase}\"}} {seconds}");
            let _ = writeln!(out, "{SECONDS}_count{{phase=\"{phase}\"}} {count}");
        }
        let _ = writeln!(
            out,
            "# HELP {FILES} Filesystem entries touched per phase by background segment merge jobs."
        );
        let _ = writeln!(out, "# TYPE {FILES} counter");
        for step in MergeStep::ALL {
            let (_, _, files) = self.segment_merge_step_observation(step);
            let _ = writeln!(out, "{FILES}{{phase=\"{}\"}} {files}", step.name());
        }
        out
    }
}

#[cfg(test)]
mod tests;
