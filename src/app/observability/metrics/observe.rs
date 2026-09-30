//! What callers record on Metrics, other than searches: index writes, segment
//! checkpoints, merges and backpressure, coordinator applies and stages, engine
//! state and HNSW lock timings, capacity-relief cycles, duplicates, skipped
//! replaces and created collections, and the storage, shard map, raft leader,
//! reshard fence and storage-degraded state.

use std::time::Duration;

use crate::app::observability::metrics::histogram::{
    duration_to_micros, observe_apply_duration_histogram, ApplyDurationObservations,
    APPLY_SECONDS_BUCKETS_US,
};
use crate::app::observability::metrics::labels::{ApplyKind, CoordinatorStage, MergeStep};
use crate::app::observability::metrics::Metrics;

impl Metrics {
    pub fn incr_index(&self, items: u64, bytes: u64) {
        self.index_writes_total.add(items);
        self.index_bytes_total.add(bytes);
    }

    /// Record one successfully completed durable segment checkpoint. The
    /// checkpoint implementation must call this only after its durable commit
    /// succeeds. `checkpoint_bytes` is the actual durable segment output;
    /// `elapsed` is the complete operation duration; `capture_lock_hold` is
    /// only the interval in which the capture lock blocks concurrent writers.
    pub fn observe_segment_checkpoint(
        &self,
        checkpoint_bytes: u64,
        elapsed: Duration,
        capture_lock_hold: Duration,
    ) {
        self.segment_checkpoint_completed_total.incr();
        self.segment_checkpoint_bytes_total.add(checkpoint_bytes);
        let elapsed_us = duration_to_micros(elapsed);
        self.segment_checkpoint_duration_us_sum.add(elapsed_us);
        self.segment_checkpoint_duration_count.incr();
        let hold_us = duration_to_micros(capture_lock_hold);
        self.segment_capture_lock_us_sum.add(hold_us);
        self.segment_capture_lock_count.incr();
    }

    /// Mark one shared synchronous checkpoint attempt as started. The caller
    /// must pair this with [`Metrics::finish_segment_checkpoint_attempt`].
    pub fn start_segment_checkpoint_attempt(&self) {
        self.segment_checkpoint_started_total.incr();
        self.segment_checkpoint_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Mark one shared synchronous checkpoint attempt as finished. Failed
    /// attempts remain distinct from durable completions and their bytes.
    pub fn finish_segment_checkpoint_attempt(&self, failed: bool) {
        if failed {
            self.segment_checkpoint_failed_total.incr();
        }
        self.segment_checkpoint_in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record one successfully completed durable segment merge. The merge
    /// implementation must call this only after its output is durable and
    /// visible. `read_bytes` and `write_bytes` are actual merge I/O.
    pub fn observe_segment_merge(&self, read_bytes: u64, write_bytes: u64) {
        self.segment_merge_completed_total.incr();
        self.segment_merge_read_bytes_total.add(read_bytes);
        self.segment_merge_write_bytes_total.add(write_bytes);
    }

    /// Record one timed `step` of a background merge job. `files` is the
    /// number of filesystem entries the step touched, or `0` for a step whose
    /// cost is not file-shaped. Called a handful of times per merge job and
    /// never from a search path.
    ///
    /// The two whole-root hard-link steps also accumulate
    /// `segment_merge_linked_files_total`, so a scrape can compare "files
    /// linked" against "fields compacted" without summing label rows.
    pub fn observe_segment_merge_step(&self, step: MergeStep, elapsed: Duration, files: u64) {
        let index = step.index();
        self.segment_merge_step_us_sum[index].add(duration_to_micros(elapsed));
        self.segment_merge_step_count[index].incr();
        self.segment_merge_step_files[index].add(files);
        if matches!(step, MergeStep::LinkScratch | MergeStep::LinkGeneration) {
            self.segment_merge_linked_files_total.add(files);
        }
    }

    /// Record the whole publication-side `save_gate` hold of one merge job.
    pub fn observe_segment_merge_save_gate(&self, held: Duration) {
        self.segment_merge_save_gate_us_sum
            .add(duration_to_micros(held));
        self.segment_merge_save_gate_count.incr();
    }

    /// Record that one merge job compacted `fields` fields.
    pub fn incr_segment_merge_fields(&self, fields: u64) {
        self.segment_merge_fields_total.add(fields);
    }

    /// Read back one step's `(microsecond sum, observation count, files)`.
    /// This is the same registry production scrapes, so a measurement that
    /// reads it cannot drift from what `/metrics` publishes.
    pub fn segment_merge_step_observation(&self, step: MergeStep) -> (u64, u64, u64) {
        let index = step.index();
        (
            self.segment_merge_step_us_sum[index].get(),
            self.segment_merge_step_count[index].get(),
            self.segment_merge_step_files[index].get(),
        )
    }

    /// Set the current unmerged delta backlog from the segment state.
    pub fn set_segment_pending_delta(&self, bytes: u64, layers: u64) {
        self.segment_pending_delta_bytes.set(bytes);
        self.segment_pending_delta_layers.set(layers);
    }

    /// Record one admission delayed or refused by real segment backpressure.
    pub fn incr_segment_backpressure(&self) {
        self.segment_backpressure_total.incr();
    }

    /// #4326: record one write-coordinator apply of `items` items of `kind`
    /// taking `elapsed` — call once per admitted local record, timed from
    /// just before `prepare_local_record` (which covers any reprice/
    /// `wait_grow_to` retry) to just after `Engine::apply_prepared_raft_entry`
    /// returns. `seconds_sum / items_total` is the per-item apply cost a
    /// large-scale perf probe reads from `GET /metrics`.
    pub fn observe_coordinator_apply(&self, kind: ApplyKind, items: u64, elapsed: Duration) {
        let idx = kind.index();
        let us = duration_to_micros(elapsed);
        if let Some(bucket_idx) = APPLY_SECONDS_BUCKETS_US
            .iter()
            .position(|&(_, bound_us)| us <= bound_us)
        {
            self.coordinator_apply_seconds_buckets[idx][bucket_idx].incr();
        }
        self.coordinator_apply_seconds_us_sum[idx].add(us);
        self.coordinator_apply_seconds_count[idx].incr();
        self.coordinator_apply_items_total[idx].add(items);
    }

    /// Record one coordinator stage without changing request scheduling.
    pub fn observe_coordinator_stage(
        &self,
        kind: ApplyKind,
        stage: CoordinatorStage,
        elapsed: Duration,
    ) {
        let stage_idx = stage.index();
        let kind_idx = kind.index();
        let us = duration_to_micros(elapsed);
        if let Some(bucket_idx) = APPLY_SECONDS_BUCKETS_US
            .iter()
            .position(|&(_, bound_us)| us <= bound_us)
        {
            self.coordinator_stage_seconds_buckets[stage_idx][kind_idx][bucket_idx].incr();
        }
        self.coordinator_stage_seconds_us_sum[stage_idx][kind_idx].add(us);
        self.coordinator_stage_seconds_count[stage_idx][kind_idx].incr();
    }

    /// Record only the wait to acquire the committed-apply engine state write
    /// lock. This is separate from work performed while the lock is held.
    pub fn observe_engine_state_write_lock_wait(&self, elapsed: Duration) {
        observe_apply_duration_histogram(
            &self.engine_state_write_lock_wait_seconds_buckets,
            &self.engine_state_write_lock_wait_seconds_us_sum,
            &self.engine_state_write_lock_wait_seconds_count,
            elapsed,
        );
    }

    /// Record the engine writer interval only after the state guard drops.
    pub fn observe_engine_state_write_lock_hold(&self, elapsed: Duration) {
        observe_apply_duration_histogram(
            &self.engine_state_write_lock_held_seconds_buckets,
            &self.engine_state_write_lock_held_seconds_us_sum,
            &self.engine_state_write_lock_held_seconds_count,
            elapsed,
        );
    }

    /// Record only one live HNSW graph-add call in committed apply.
    pub fn observe_hnsw_add(&self, elapsed: Duration) {
        let mut observations = ApplyDurationObservations::default();
        observations.record(elapsed);
        self.observe_hnsw_add_observations(&observations);
    }

    /// Record one HNSW graph rebuild apart from the containing add call.
    pub fn observe_hnsw_graph_rebuild(&self, elapsed: Duration) {
        let mut observations = ApplyDurationObservations::default();
        observations.record(elapsed);
        self.observe_hnsw_graph_rebuild_observations(&observations);
    }

    /// Record only the wait to acquire the HNSW graph write lock.
    pub fn observe_hnsw_write_lock_wait(&self, elapsed: Duration) {
        observe_apply_duration_histogram(
            &self.hnsw_write_lock_wait_seconds_buckets,
            &self.hnsw_write_lock_wait_seconds_us_sum,
            &self.hnsw_write_lock_wait_seconds_count,
            elapsed,
        );
    }

    /// Record only the interval the HNSW graph write lock is held.
    pub fn observe_hnsw_write_lock_hold(&self, elapsed: Duration) {
        observe_apply_duration_histogram(
            &self.hnsw_write_lock_held_seconds_buckets,
            &self.hnsw_write_lock_held_seconds_us_sum,
            &self.hnsw_write_lock_held_seconds_count,
            elapsed,
        );
    }

    /// Record a successful capacity-relief checkpoint-plus-merge cycle.
    pub fn observe_capacity_relief_checkpoint_merge_cycle(&self, elapsed: Duration) {
        observe_apply_duration_histogram(
            &self.segment_capacity_relief_checkpoint_merge_seconds_buckets,
            &self.segment_capacity_relief_checkpoint_merge_seconds_us_sum,
            &self.segment_capacity_relief_checkpoint_merge_seconds_count,
            elapsed,
        );
    }

    /// Set the byte size of the current durable segment files on disk.
    pub fn set_segment_disk_bytes(&self, bytes: u64) {
        self.segment_disk_bytes.set(bytes);
    }

    pub fn incr_duplicates(&self) {
        self.duplicates_requests_total.incr();
    }

    /// #1293: record `n` `docs:replace` fields skipped as unchanged no-ops.
    pub fn incr_replace_skipped(&self, fields: u64) {
        self.replace_fields_skipped_total.add(fields);
    }

    pub fn incr_collection_created(&self, fields: u64) {
        self.collections_created_total.incr();
        self.schema_fields_total.add(fields);
    }

    pub fn set_storage_bytes(&self, bytes: u64) {
        self.storage_bytes.set(bytes);
    }

    /// #1467 R5: record this pod's live routed shard-map version.
    pub fn set_shard_map_version(&self, version: u64) {
        self.shard_map_version.set(version);
    }

    /// #1467 R6: record one scatter sub-response whose responding pod's map
    /// version differed from the scattering pod's own declared version.
    pub fn incr_scatter_map_version_mismatch(&self) {
        self.scatter_map_version_mismatches_total.incr();
    }

    /// #2475: record this pod's shard index + whether its raft
    /// election-state poll currently believes that shard has an elected
    /// leader. Called every `spawn_cluster_state_poller` tick in raft mode
    /// only; standalone/non-raft pods never call this, so `raft_shard`
    /// stays at the `NOT_RAFT` sentinel forever and `render()` omits
    /// `lumen_raft_leader_known` for them.
    pub fn set_raft_leader_known(&self, shard: u32, known: bool) {
        self.raft_shard.set(shard as u64);
        self.raft_leader_known.set(known as u64);
    }

    /// #2475: arm/clear the reshard write-fence-active signal, called from
    /// `POST /admin/reshard:fence`'s handler. Arming also stamps
    /// `reshard_fence_armed_unixtime` to "now".
    pub fn set_reshard_fence_active(&self, active: bool) {
        self.reshard_fence_active.set(active as u64);
        if active {
            self.reshard_fence_armed_unixtime.set(unix_now_secs());
        }
    }

    /// #2516: flip into ENOSPC degraded read-only mode and count the hit.
    /// Called from every durable-write-path origin that classifies its
    /// failure as `io::ErrorKind::StorageFull` (see
    /// `crate::ingest::application::write_coordinator::errors::is_storage_full`) —
    /// the coordinator apply loop's local AOF persist, the periodic RDB/segment checkpoint
    /// snapshotters, and (when the `raft-wal` feature is active) raft log
    /// append. Idempotent: calling it while already degraded still counts
    /// the new hit but leaves the gauge at `1`.
    pub fn mark_storage_degraded(&self) {
        self.storage_full_errors_total.incr();
        self.storage_degraded.set(1);
    }

    /// #2516: `true` while this node is in ENOSPC degraded read-only mode.
    pub fn is_storage_degraded(&self) -> bool {
        self.storage_degraded.get() != 0
    }

    /// #2516: clear degraded mode once the periodic re-probe confirms the
    /// data dir accepts a write again.
    pub fn clear_storage_degraded(&self) {
        self.storage_degraded.set(0);
    }
}

/// #2475: current unix-epoch seconds, saturating to `0` on a pre-epoch
/// clock rather than panicking (metrics rendering must never fail).
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
