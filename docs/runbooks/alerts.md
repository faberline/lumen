# Alert and health-signal runbook

Each section below covers one health endpoint or one metric that the source
registers. Each section gives what the signal means, a command that checks it,
and the fix when it is bad. Run the checks on a pod. In a cluster, first
forward the port: `kubectl port-forward <pod> 7373:7373`.

## Health endpoints

### `/healthz`

- Meaning: the process is alive. It returns `ok` (200) while the HTTP server runs. It does not check storage or readiness.
- Check: `curl -fsS http://127.0.0.1:7373/healthz`
- Fix: restart the pod when this fails and the process does not come back.

### `/readyz`

- Meaning: `ok` (200) when the pod can take traffic. `restart required` (503) after a configuration change that needs a restart. `draining` (503) while the pod shuts down.
- Check: `curl -fsS http://127.0.0.1:7373/readyz`
- Fix: for `restart required`, restart the pod. For `draining`, wait for the shutdown to finish; the pod then restarts.

## Metrics

### `lumen_index_writes_total`

- Meaning: total index items applied.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_index_writes_total`
- Fix: none; informational.

### `lumen_index_bytes_total`

- Meaning: total bytes written across all field indexes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_index_bytes_total`
- Fix: none; informational.

### `lumen_search_requests_total`

- Meaning: total search requests served.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_search_requests_total`
- Fix: none; informational.

### `lumen_search_latency_ms`

- Meaning: DEPRECATED (#2519). Sum and count of search latency in milliseconds, kept for dashboard back-compatibility. Use `lumen_search_latency_seconds`.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_search_latency_ms`
- Fix: none; read `lumen_search_latency_seconds` instead.

### `lumen_slow_queries_total`

- Meaning: search queries at or above the slow-query threshold (`LUMEN_SLOW_QUERY_MS`, default 500 ms).
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_slow_queries_total`
- Fix: on a rise, find the slow queries in the log and check their shape against `docs/limits.md`.

### `lumen_duplicates_requests_total`

- Meaning: total duplicate-detection requests.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_duplicates_requests_total`
- Fix: none; informational.

### `lumen_collections_created_total`

- Meaning: total collections created or extended.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_collections_created_total`
- Fix: none; informational.

### `lumen_schema_fields_total`

- Meaning: total field declarations registered.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_schema_fields_total`
- Fix: none; informational.

### `lumen_storage_bytes`

- Meaning: approximate bytes held by all in-memory field indexes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_storage_bytes`
- Fix: compare with the pod memory limit. If it is close, raise the limit or add pods.

### `lumen_storage_degraded`

- Meaning: 1 while this node is in ENOSPC degraded read-only mode, after a durable write hit a full disk.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_storage_degraded`
- Fix: free disk space or grow the volume. Writes stay refused until the node leaves this mode. Alert on a value of 1.

### `lumen_storage_full_errors_total`

- Meaning: genuine ENOSPC (disk full) hits on a durable write path.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_storage_full_errors_total`
- Fix: on any rise, free disk space or grow the volume.

### `lumen_posting_cache_hits_total`

- Meaning: posting cache hit count.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_posting_cache_hits_total`
- Fix: none; informational.

### `lumen_posting_cache_misses_total`

- Meaning: posting cache miss count.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_posting_cache_misses_total`
- Fix: none; informational.

### `lumen_replace_fields_skipped_total`

- Meaning: `docs:replace` fields skipped as unchanged no-ops.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_replace_fields_skipped_total`
- Fix: none; informational.

### `lumen_shard_map_version`

- Meaning: this pod's live routed shard-map version. 0 outside routed deployments.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_shard_map_version`
- Fix: if pods show different versions, see `lumen_scatter_map_version_mismatches_total`.

### `lumen_scatter_map_version_mismatches_total`

- Meaning: scatter search sub-responses whose responding pod had a different map version from the sender.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_scatter_map_version_mismatches_total`
- Fix: on a rise, pods run different shard maps. Compare `lumen_shard_map_version` on each pod.

### `lumen_reshard_fence_active`

- Meaning: 1 while this pod believes a reshard-driver write fence is armed on it.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_reshard_fence_active`
- Fix: none while a reshard runs. If it stays at 1 after the reshard ends, check the reshard driver before any other action.

### `lumen_reshard_fence_armed_unixtime`

- Meaning: Unix time at which the current or most recent reshard write fence was armed.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_reshard_fence_armed_unixtime`
- Fix: none; informational. Subtract it from the current time to see how long the fence has been armed.

### `lumen_raft_leader_known`

- Meaning: 1 while this pod's raft poll sees an elected leader for its shard, 0 otherwise. Absent on standalone pods.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_raft_leader_known`
- Fix: a value of 0 for long means no leader. Check the raft pods and the network between them.

### `lumen_coordinator_stage_seconds`

- Meaning: time in each admitted write-coordinator request stage, in seconds, by stage and kind.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_coordinator_stage_seconds_bucket`
- Fix: find the stage with the highest latency and check its cause in the logs.

### `lumen_coordinator_apply_seconds`

- Meaning: time the write coordinator spent applying one admitted local record, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_coordinator_apply_seconds_bucket`
- Fix: if the high percentiles grow, check `lumen_engine_state_write_lock_wait_seconds` first.

### `lumen_coordinator_apply_items_total`

- Meaning: items applied per write-coordinator apply, by RaftLogEntry kind.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_coordinator_apply_items_total`
- Fix: none; informational.

### `lumen_engine_state_write_lock_wait_seconds`

- Meaning: time spent waiting for the committed-apply engine state write lock, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_engine_state_write_lock_wait_seconds_bucket`
- Fix: a long wait usually follows a long hold by another apply. Check `lumen_engine_state_write_lock_held_seconds`.

### `lumen_engine_state_write_lock_held_seconds`

- Meaning: time the committed-apply engine state write lock was held, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_engine_state_write_lock_held_seconds_bucket`
- Fix: a long hold means a large apply. Reduce the batch size; the item cap is `MAX_INDEX_ITEMS` in `docs/limits.md`.

### `lumen_hnsw_add_seconds`

- Meaning: time spent in live HNSW graph additions during committed apply, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_hnsw_add_seconds_bucket`
- Fix: none while the high percentiles stay flat. If they grow, check `lumen_hnsw_graph_rebuild_seconds`.

### `lumen_hnsw_graph_rebuild_seconds`

- Meaning: time spent rebuilding an HNSW graph during a live add, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_hnsw_graph_rebuild_seconds_bucket`
- Fix: frequent rebuilds mean the write rate is too high for the graph. Spread the writes or lower the batch size.

### `lumen_hnsw_write_lock_wait_seconds`

- Meaning: time spent waiting to acquire an HNSW graph write lock, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_hnsw_write_lock_wait_seconds_bucket`
- Fix: a long wait follows a long hold. Check `lumen_hnsw_write_lock_held_seconds`.

### `lumen_hnsw_write_lock_held_seconds`

- Meaning: time an HNSW graph write lock was held, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_hnsw_write_lock_held_seconds_bucket`
- Fix: long holds delay searches. Check `lumen_hnsw_graph_rebuild_seconds`.

### `lumen_pending_change_active_bytes`

- Meaning: process-wide active bytes owned by pending durable changes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_pending_change_active_bytes`
- Fix: if it nears `HARD_LIMIT` (256 MiB, see `docs/limits.md`), writes are refused. Reduce the write rate and check the checkpoints.

### `lumen_pending_change_frozen_bytes`

- Meaning: process-wide frozen bytes kept by incomplete durable checkpoints.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_pending_change_frozen_bytes`
- Fix: a value that does not fall means a stuck checkpoint. Check `lumen_segment_checkpoint_in_flight` and `lumen_segment_checkpoint_failed_total`.

### `lumen_pending_change_reserved_bytes`

- Meaning: process-wide bytes reserved before apply for pending durable changes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_pending_change_reserved_bytes`
- Fix: none while it stays well below `HARD_LIMIT`. If it nears the limit, see `lumen_pending_change_total_bytes`.

### `lumen_pending_change_total_bytes`

- Meaning: process-wide pending durable-change bytes across all ownership states.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_pending_change_total_bytes`
- Fix: if it nears `HARD_LIMIT` (256 MiB), reduce the write rate and check the checkpoints.

### `lumen_pending_change_high_water_bytes`

- Meaning: largest observed process-wide pending durable-change total, in bytes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_pending_change_high_water_bytes`
- Fix: none; informational. Compare it with `HARD_LIMIT`.

### `lumen_process_rss_high_water_bytes`

- Meaning: Linux peak resident set size from `VmHWM` in `/proc/self/status`, in bytes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_process_rss_high_water_bytes`
- Fix: compare it with the pod memory limit. If it is close, raise the limit or reduce the load.

### `lumen_process_rss_high_water_available`

- Meaning: 1 when this scrape read `VmHWM` from Linux, 0 when it was unavailable. The high-water value is valid only at 1.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_process_rss_high_water_available`
- Fix: none; informational. On a value of 0, do not use `lumen_process_rss_high_water_bytes`.

### `lumen_segment_backpressure_total`

- Meaning: admissions delayed or refused by real segment backpressure.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_backpressure_total`
- Fix: on a rise, writes outrun the merges. Check `lumen_segment_pending_delta_layers` and the merge completions.

### `lumen_segment_capacity_relief_checkpoint_merge_seconds`

- Meaning: time for successful capacity-relief checkpoint-plus-merge cycles, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_capacity_relief_checkpoint_merge_seconds_bucket`
- Fix: none; informational. Long cycles add write delay.

### `lumen_segment_capture_lock_seconds`

- Meaning: capture-lock hold time of completed durable checkpoints, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_capture_lock_seconds_bucket`
- Fix: long holds delay writes. Check `lumen_segment_checkpoint_duration_seconds`.

### `lumen_segment_checkpoint_bytes_total`

- Meaning: bytes durably published by completed segment checkpoints.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_bytes_total`
- Fix: none; informational.

### `lumen_segment_checkpoint_completed_total`

- Meaning: successfully completed durable segment checkpoints.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_completed_total`
- Fix: none; informational. Compare it with `lumen_segment_checkpoint_started_total`.

### `lumen_segment_checkpoint_duration_seconds`

- Meaning: total duration of completed durable segment checkpoints, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_duration_seconds_bucket`
- Fix: long checkpoints keep frozen bytes high. Check the disk speed.

### `lumen_segment_checkpoint_failed_total`

- Meaning: checkpoint attempts that returned an error from the checkpoint wrapper.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_failed_total`
- Fix: on any rise, read the pod log for the checkpoint error and check `lumen_storage_degraded`.

### `lumen_segment_checkpoint_in_flight`

- Meaning: checkpoint attempts running now.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_in_flight`
- Fix: a value that stays above 0 for a long time means a stuck checkpoint. Read the pod log.

### `lumen_segment_checkpoint_started_total`

- Meaning: checkpoint attempts that entered the shared checkpoint wrapper.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_checkpoint_started_total`
- Fix: none; informational.

### `lumen_segment_disk_bytes`

- Meaning: durable segment files on disk, in bytes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_disk_bytes`
- Fix: compare it with the volume size. Add disk before the volume fills.

### `lumen_segment_merge_completed_total`

- Meaning: successfully completed durable segment merges.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_completed_total`
- Fix: none; informational.

### `lumen_segment_merge_fields_total`

- Meaning: fields compacted by background segment merge jobs.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_fields_total`
- Fix: none; informational.

### `lumen_segment_merge_linked_files_total`

- Meaning: files hard-linked by background segment merge jobs across both whole-root link passes.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_linked_files_total`
- Fix: none; informational.

### `lumen_segment_merge_phase_files_total`

- Meaning: filesystem entries touched per phase by background segment merge jobs.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_phase_files_total`
- Fix: none; informational.

### `lumen_segment_merge_phase_seconds`

- Meaning: per-phase duration of background segment merge jobs, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_phase_seconds_bucket`
- Fix: find the slow phase label. A slow save phase points at `lumen_segment_merge_save_gate_held_seconds`.

### `lumen_segment_merge_read_bytes_total`

- Meaning: bytes read by successfully completed durable segment merges.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_read_bytes_total`
- Fix: none; informational.

### `lumen_segment_merge_write_bytes_total`

- Meaning: bytes written by successfully completed durable segment merges.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_write_bytes_total`
- Fix: none; informational.

### `lumen_segment_merge_save_gate_held_seconds`

- Meaning: total time background segment merge jobs held the publication save gate, in seconds.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_merge_save_gate_held_seconds_bucket`
- Fix: long holds block checkpoints. Check the disk speed.

### `lumen_segment_pending_delta_bytes`

- Meaning: current bytes in unmerged durable segment delta layers.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_pending_delta_bytes`
- Fix: a rising value means the merges fall behind. Check the merge completions.

### `lumen_segment_pending_delta_layers`

- Meaning: current count of unmerged durable segment delta layers.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_segment_pending_delta_layers`
- Fix: a high count triggers backpressure (`lumen_segment_backpressure_total`). Check the merges.

### `lumen_text_row_stage_rows_total`

- Meaning: immutable Text rows successfully prepared by this engine.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_text_row_stage_rows_total`
- Fix: none; informational.

### `lumen_text_row_stage_input_bytes_total`

- Meaning: input bytes in immutable Text rows successfully prepared by this engine.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_text_row_stage_input_bytes_total`
- Fix: none; informational.

### `lumen_search_latency_seconds`

- Meaning: search latency histogram in seconds, bucketed for search SLOs.
- Check: `curl -fsS http://127.0.0.1:7373/metrics | grep lumen_search_latency_seconds_bucket`
- Fix: on a high p99, check `lumen_slow_queries_total` and `lumen_segment_pending_delta_layers`.

### `lumen_write_phase`

- Meaning: not a Prometheus series. It is a structured log event (target `lumen_write_phase`), one line per write phase, tagged with `attempt_id`.
- Check: `kubectl logs <pod> | grep lumen_write_phase | tail`
- Fix: none; use it to find the slow phase of one write.
