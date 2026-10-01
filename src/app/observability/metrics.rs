//! Lightweight in-process Prometheus exposition.
//!
//! v1 keeps the metric surface narrow and dep-free: a handful of
//! `AtomicU64` counters/gauges + a single `render` that emits the
//! Prometheus text-format. When request volume grows past what
//! lock-free counters can serve, swap in `prometheus`/`metrics` crates
//! without changing the wire format the scraper sees.
//!
//! The counter/gauge primitives and the Prometheus text-format encoder
//! are generic across every service with a `/metrics` scrape endpoint
//! and live in `libs/metrics-prometheus` (#974); this module wires lumen's
//! metric names, HELP text, and `# TYPE` kinds onto them. `Counter`/
//! `Gauge` deref to the underlying `AtomicU64`, so this module's pub API
//! — field names, method names, and `render()`'s byte output — is
//! unchanged for callers, including the `otel` feature's direct
//! `field.load(Ordering::Relaxed)` reads in `src/bin/lumen.rs`.

pub(crate) mod apply_telemetry;
mod histogram;
pub(crate) mod labels;
mod observe;
mod process;
mod render;
mod search;
mod segment_telemetry;

use metrics_prometheus::{Counter, Gauge};

use crate::app::observability::metrics::histogram::APPLY_SECONDS_BUCKET_COUNT;
use crate::app::observability::metrics::labels::{APPLY_KIND_COUNT, MERGE_STEP_COUNT};
use crate::app::observability::metrics::search::{
    slow_query_threshold_ms_from_env, SEARCH_LATENCY_BUCKET_COUNT,
};

#[cfg(doc)]
use crate::app::observability::metrics::histogram::APPLY_SECONDS_BUCKETS_US;
#[cfg(doc)]
use crate::app::observability::metrics::labels::{ApplyKind, MergeStep};
#[cfg(doc)]
use crate::app::observability::metrics::search::{
    DEFAULT_SLOW_QUERY_THRESHOLD_MS, SEARCH_LATENCY_BUCKETS_US,
};

/// #2475: sentinel `raft_shard` value meaning "never touched by the raft
/// election-state poller" — see [`Metrics::raft_shard`]. Out of range for
/// any real shard index, so it stays distinguishable from every real shard.
const NOT_RAFT: u64 = u64::MAX;

/// All metrics carry the `{collection, shard, partition}` label set per
/// the README §5 contract. v1 in-memory single-shard reports
/// `shard="0", partition="0"` as constants; future LSM/Raft tiers will
/// vary `partition` and `shard` respectively.
#[derive(Debug, Default)]
pub struct Metrics {
    pub index_writes_total: Counter,
    pub index_bytes_total: Counter,
    /// Successful immutable Text row preparation in this Engine.
    pub(crate) text_row_stage_rows_total: Counter,
    pub(crate) text_row_stage_input_bytes_total: Counter,
    pub search_requests_total: Counter,
    /// #2519 DEPRECATED: kept only for dashboard back-compat. New
    /// consumers should read the `lumen_search_latency_seconds` histogram
    /// (`search_latency_buckets`/`search_latency_us_sum`) instead — it
    /// carries the same observations at real bucket + second-precision
    /// sum resolution instead of a lossy millisecond-rounded sum/count
    /// pair.
    pub search_latency_ms_sum: Counter,
    /// #2519 DEPRECATED: see `search_latency_ms_sum`. Doubles as the
    /// histogram's total observation count (`+Inf` bucket / `_count`) in
    /// `render_search_latency_histogram` — one `observe_search` call is
    /// exactly one observation of both series, so a second atomic would
    /// only duplicate this one.
    pub search_latency_ms_count: Counter,
    /// #2519: exclusive per-bucket search-latency observation counts
    /// backing `lumen_search_latency_seconds_bucket{le=...}` — see
    /// [`SEARCH_LATENCY_BUCKETS_US`] for the bucket bounds/semantics and
    /// `render_search_latency_histogram` for how these become the
    /// cumulative counts a Prometheus histogram requires.
    pub search_latency_buckets: [Counter; SEARCH_LATENCY_BUCKET_COUNT],
    /// #2519: sum of search latencies in whole microseconds — an integer
    /// atomic (not a float) for lock-free accumulation; `render()` divides
    /// by 1e6 to produce `lumen_search_latency_seconds_sum`.
    pub search_latency_us_sum: Counter,
    /// #2519: total search queries whose latency met or exceeded
    /// `slow_query_threshold_ms` (`LUMEN_SLOW_QUERY_MS`, default
    /// [`DEFAULT_SLOW_QUERY_THRESHOLD_MS`]ms) — see
    /// [`Metrics::observe_search`].
    pub slow_queries_total: Counter,
    /// #2519: the resolved `LUMEN_SLOW_QUERY_MS` threshold in
    /// milliseconds, read once in [`Metrics::new`] (server startup)
    /// rather than per `observe_search` call — mirrors `LUMEN_HNSW_EF`'s
    /// read-once-at-construction convention (`hnsw_search_ef` in
    /// `src/vector_index.rs`) instead of adding env-var lock traffic to
    /// the search hot path. Not published as its own series;
    /// `lumen_slow_queries_total`'s HELP text documents the env var for
    /// operators reading `/metrics` directly.
    pub slow_query_threshold_ms: Gauge,
    pub duplicates_requests_total: Counter,
    pub collections_created_total: Counter,
    pub schema_fields_total: Counter,
    pub storage_bytes: Gauge,
    pub posting_cache_hits_total: Counter,
    pub posting_cache_misses_total: Counter,
    /// #1293: `docs:replace` server-side no-op suppression — fields whose
    /// incoming value matched the currently indexed state and were skipped
    /// (no posting-list rewrite, no HNSW tombstone/reinsert). Distinct from
    /// `index_writes_total`, which only ever counts fields actually written.
    pub replace_fields_skipped_total: Counter,
    /// #1467 R5: this pod's live routed shard-map version, `0` for every
    /// non-routed deployment shape (standalone, primary/replica, no
    /// `operator` feature) that never sets it. The reshard driver's
    /// `advance_convergence` scrapes this over `/metrics` — the same
    /// admin-reachable surface its usage loop already polls — to require
    /// every serving pod to actually report the new map version, not just
    /// that its StatefulSet rollout finished (rollout completion alone does
    /// not prove the ConfigMap write each pod reads its map from has
    /// propagated to every pod).
    pub shard_map_version: Gauge,
    /// #1467 R6: count of scatter (routing-key-less) search sub-requests
    /// where the responding pod's live shard-map version differed from the
    /// scattering pod's own declared version. Signal for a mixed-map
    /// rolling-restart window landing a scatter search mid-flight;
    /// non-fatal by design (see `routing_remote.rs`'s scatter exemption
    /// doc — availability over completeness). `0` outside routed
    /// deployments.
    pub scatter_map_version_mismatches_total: Counter,
    /// #2475: this pod's raft shard index, or the `NOT_RAFT` sentinel until
    /// its raft election-state poller (`spawn_cluster_state_poller` in
    /// `src/bin/lumen.rs`) has ticked at least once. `render()` reads the
    /// sentinel to omit `lumen_raft_leader_known` entirely for
    /// standalone/non-raft deployments — publishing a permanently-`0`
    /// series there would be indistinguishable from a genuinely stuck
    /// leaderless shard to `render::prometheus_rule`'s
    /// `LumenRaftLeaderAbsent` alert.
    pub raft_shard: Gauge,
    /// #2475: `1` while this pod's raft election-state poll believes its
    /// shard currently has an elected leader (itself or a peer), `0` while
    /// it does not. Meaningful only once `raft_shard` has left the
    /// `NOT_RAFT` sentinel; see [`Metrics::set_raft_leader_known`].
    pub raft_leader_known: Gauge,
    /// #2475: `1` while this pod believes a reshard-driver write fence
    /// (`POST /admin/reshard:fence`, see `crate::app::http::write_fence::WriteFence`) is
    /// currently armed on it, `0` once cleared or never armed.
    pub reshard_fence_active: Gauge,
    /// #2475: unix-epoch seconds the currently (or most recently) armed
    /// write fence was armed at. Pairs with `reshard_fence_active` so
    /// `LumenReshardWorkflowStalled` can tell a long-armed fence (the
    /// driver's fenced final `CatchingUp` pass never came back to clear it)
    /// from one still mid-pass.
    pub reshard_fence_armed_unixtime: Gauge,
    /// #2516: `1` while this node is in ENOSPC degraded read-only mode
    /// (a durable write path — local AOF/WAL append, segment/RDB
    /// checkpoint save, or raft log append — hit
    /// `io::ErrorKind::StorageFull`), `0` otherwise. Sticky: stays `1`
    /// until the periodic re-probe (`LUMEN_STORAGE_FULL_REPROBE_SECS`, see
    /// `src/bin/lumen.rs`) confirms the data dir accepts a write again, or
    /// the process restarts. `crate::app::http::guards::enforce_storage_writable` reads
    /// this to fast-fail mutating endpoints without touching the durable
    /// path; `render::prometheus_rule`'s `LumenStorageDegraded` alert reads
    /// the published `lumen_storage_degraded` series.
    pub storage_degraded: Gauge,
    /// #2516: total genuine ENOSPC hits observed on a durable write path,
    /// monotonic across the process lifetime (never reset when
    /// `storage_degraded` clears) — see [`Metrics::mark_storage_degraded`].
    pub storage_full_errors_total: Counter,
    /// Completed durable segment checkpoints. This remains zero until the
    /// segment checkpoint path calls [`Metrics::observe_segment_checkpoint`].
    pub segment_checkpoint_completed_total: Counter,
    /// Checkpoint attempts that entered the shared synchronous checkpoint
    /// wrapper, including attempts that later fail before publication.
    pub segment_checkpoint_started_total: Counter,
    /// Checkpoint attempts that returned an error from the shared synchronous
    /// checkpoint wrapper.
    pub segment_checkpoint_failed_total: Counter,
    /// Checkpoint attempts currently inside the shared synchronous wrapper.
    pub segment_checkpoint_in_flight: Gauge,
    /// Actual bytes durably published by completed segment checkpoints.
    pub segment_checkpoint_bytes_total: Counter,
    /// Sum of completed segment checkpoint durations in microseconds. Rendered
    /// as seconds by `lumen_segment_checkpoint_duration_seconds_sum`.
    pub segment_checkpoint_duration_us_sum: Counter,
    /// Number of completed segment checkpoint duration observations.
    pub segment_checkpoint_duration_count: Counter,
    /// Sum of capture-lock hold durations in microseconds. Rendered as seconds
    /// by `lumen_segment_capture_lock_seconds_sum`.
    pub segment_capture_lock_us_sum: Counter,
    /// Number of capture-lock hold duration observations.
    pub segment_capture_lock_count: Counter,
    /// Completed durable segment merges. This remains zero until the segment
    /// merge path calls [`Metrics::observe_segment_merge`].
    pub segment_merge_completed_total: Counter,
    /// Bytes read while completed durable segment merges run.
    pub segment_merge_read_bytes_total: Counter,
    /// Bytes written while completed durable segment merges run.
    pub segment_merge_write_bytes_total: Counter,
    /// Per-[`MergeStep`] sum of step durations in microseconds, rendered as
    /// seconds by `lumen_segment_merge_phase_seconds_sum{phase=...}`.
    pub segment_merge_step_us_sum: [Counter; MERGE_STEP_COUNT],
    /// Per-[`MergeStep`] observation count, rendered as
    /// `lumen_segment_merge_phase_seconds_count{phase=...}`.
    pub segment_merge_step_count: [Counter; MERGE_STEP_COUNT],
    /// Per-[`MergeStep`] count of filesystem entries the step touched —
    /// hard-links created, files inherited, files validated. Zero for a step
    /// whose cost is not file-count-shaped, which is exactly the signal that
    /// separates "the root is wide" from "the payload is large".
    pub segment_merge_step_files: [Counter; MERGE_STEP_COUNT],
    /// Total files hard-linked by completed merge jobs across both
    /// whole-root link passes (`link_scratch` + `link_generation`).
    pub segment_merge_linked_files_total: Counter,
    /// Total fields compacted by merge jobs. This is a per-field count,
    /// unlike `segment_merge_completed_total`, which one job increments once
    /// per published output.
    pub segment_merge_fields_total: Counter,
    /// Sum of the whole publication-side `save_gate` hold in microseconds,
    /// rendered as `lumen_segment_merge_save_gate_held_seconds_sum`.
    pub segment_merge_save_gate_us_sum: Counter,
    /// Observation count for `segment_merge_save_gate_us_sum`.
    pub segment_merge_save_gate_count: Counter,
    /// Bytes that remain in unmerged segment delta layers.
    pub segment_pending_delta_bytes: Gauge,
    /// Number of unmerged segment delta layers.
    pub segment_pending_delta_layers: Gauge,
    /// Total admissions delayed or refused due to segment backpressure.
    pub segment_backpressure_total: Counter,
    /// Actual durable segment files on disk in bytes.
    pub segment_disk_bytes: Gauge,
    /// #4326: per-[`ApplyKind`] exclusive per-bucket observation counts
    /// backing `lumen_coordinator_apply_seconds_bucket{le=...,kind=...}` —
    /// see [`APPLY_SECONDS_BUCKETS_US`] for the bucket bounds and
    /// [`Metrics::render_coordinator_apply_histogram`] for how these become
    /// the cumulative counts a Prometheus histogram requires.
    pub coordinator_apply_seconds_buckets:
        [[Counter; APPLY_SECONDS_BUCKET_COUNT]; APPLY_KIND_COUNT],
    /// #4326: per-[`ApplyKind`] sum of write-coordinator per-record apply
    /// durations in whole microseconds — an integer atomic for lock-free
    /// accumulation; `render()` divides by 1e6 to produce
    /// `lumen_coordinator_apply_seconds_sum{kind=...}`.
    pub coordinator_apply_seconds_us_sum: [Counter; APPLY_KIND_COUNT],
    /// #4326: per-[`ApplyKind`] total apply observations, backing
    /// `lumen_coordinator_apply_seconds_count{kind=...}` and the `+Inf`
    /// bucket.
    pub coordinator_apply_seconds_count: [Counter; APPLY_KIND_COUNT],
    /// #4326: per-[`ApplyKind`] total items applied (docs for
    /// `index`/`replace`, external ids for `unindex`, `1` otherwise),
    /// backing `lumen_coordinator_apply_items_total{kind=...}` — divide by
    /// `coordinator_apply_seconds_count`'s sibling sum for per-item apply
    /// cost.
    pub coordinator_apply_items_total: [Counter; APPLY_KIND_COUNT],
    /// #4246: bounded request-stage histograms, labelled by operation kind.
    pub coordinator_stage_seconds_buckets:
        [[[Counter; APPLY_SECONDS_BUCKET_COUNT]; APPLY_KIND_COUNT]; 3],
    pub coordinator_stage_seconds_us_sum: [[Counter; APPLY_KIND_COUNT]; 3],
    pub coordinator_stage_seconds_count: [[Counter; APPLY_KIND_COUNT]; 3],
    /// Exclusive finite-bucket observations for time spent waiting to acquire
    /// the committed-apply engine state write lock.
    pub engine_state_write_lock_wait_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub engine_state_write_lock_wait_seconds_us_sum: Counter,
    pub engine_state_write_lock_wait_seconds_count: Counter,
    /// Exclusive finite-bucket observations for time the committed-apply
    /// engine state write lock is held. Published after the guard drops.
    pub engine_state_write_lock_held_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub engine_state_write_lock_held_seconds_us_sum: Counter,
    pub engine_state_write_lock_held_seconds_count: Counter,
    /// Exclusive finite-bucket observations for live HNSW graph additions in
    /// committed apply. Flat CPU additions do not record this family.
    pub hnsw_add_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub hnsw_add_seconds_us_sum: Counter,
    pub hnsw_add_seconds_count: Counter,
    /// HNSW graph rebuild time. Normal additions do not record this family.
    pub hnsw_graph_rebuild_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub hnsw_graph_rebuild_seconds_us_sum: Counter,
    pub hnsw_graph_rebuild_seconds_count: Counter,
    /// HNSW graph write-lock acquisition time, excluding the work while held.
    pub hnsw_write_lock_wait_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub hnsw_write_lock_wait_seconds_us_sum: Counter,
    pub hnsw_write_lock_wait_seconds_count: Counter,
    /// HNSW graph write-lock hold time, excluding the time waiting to acquire it.
    pub hnsw_write_lock_held_seconds_buckets: [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub hnsw_write_lock_held_seconds_us_sum: Counter,
    pub hnsw_write_lock_held_seconds_count: Counter,
    /// Completed capacity-relief cycles that ran checkpoint then merge.
    pub segment_capacity_relief_checkpoint_merge_seconds_buckets:
        [Counter; APPLY_SECONDS_BUCKET_COUNT],
    pub segment_capacity_relief_checkpoint_merge_seconds_us_sum: Counter,
    pub segment_capacity_relief_checkpoint_merge_seconds_count: Counter,
    /// Linux `/proc/self/status` `VmHWM` in bytes at the latest scrape. This
    /// is meaningful only while `process_rss_high_water_available` is `1`.
    pub process_rss_high_water_bytes: Gauge,
    /// `1` when this scrape parsed Linux `VmHWM`; `0` when this process cannot
    /// report it. Consumers must require this signal before accepting the RSS
    /// value, so an unavailable platform cannot qualify with a zero gauge.
    pub process_rss_high_water_available: Gauge,
    /// Process-wide bytes reserved before apply for pending durable changes.
    pub pending_change_reserved_bytes: Gauge,
    /// Process-wide active bytes owned by pending durable changes.
    pub pending_change_active_bytes: Gauge,
    /// Process-wide frozen bytes retained by incomplete durable checkpoints.
    pub pending_change_frozen_bytes: Gauge,
    /// Process-wide pending durable-change bytes across all ownership states.
    pub pending_change_total_bytes: Gauge,
    /// Largest observed process-wide pending durable-change total. This stays
    /// monotonic for the process lifetime, including peaks between scrapes.
    pub pending_change_high_water_bytes: Gauge,
}

impl Metrics {
    pub fn new() -> Self {
        let metrics = Self::default();
        // #2475: seed the "never touched by the raft poller" sentinel;
        // every other field's `Default` (0) is already the right initial
        // value.
        metrics.raft_shard.set(NOT_RAFT);
        // #2519: resolve `LUMEN_SLOW_QUERY_MS` once at construction time.
        metrics
            .slow_query_threshold_ms
            .set(slow_query_threshold_ms_from_env());
        metrics
    }
}
