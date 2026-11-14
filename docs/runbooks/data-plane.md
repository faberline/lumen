# Runbook — lumen data plane

On-call procedures for a `Lumen` serving instance: the pods that answer search
and write requests on port `7373`. The operator control plane is covered in
[operator-control-plane.md](operator-control-plane.md).

Scope: one Managed `Lumen` instance in a namespace, named `<name>`. Replace
`<ns>` and `<name>` in the commands below. Support limits come from
[STATUS](../../STATUS.md); this runbook does not claim HA, TLS or backup
behaviour beyond it.

## 1. Health

Port-forward the Service, then check the probes:

```bash
kubectl -n <ns> port-forward service/<name> 7373:7373
curl -fsS http://127.0.0.1:7373/healthz   # process is alive, body "ok"
curl -fsS http://127.0.0.1:7373/readyz    # engine can serve, body "ok"
curl -fsS http://127.0.0.1:7373/version   # version, git_sha, built_at
```

- `/healthz` returns `200 ok` while the process runs. It says nothing about
  readiness.
- `/readyz` returns `503` in two cases: `restart required` (the writer needs a
  restart) and `draining` (the pod is shutting down on SIGTERM). Both are
  final states of that pod, not of the instance.

When `/readyz` fails, read the body first. `draining` clears when the pod
exits; if it stays, check the pod's `kubectl describe` for a stuck termination.
`restart required` does not clear by itself: read `kubectl logs` for the cause,
then restart that pod.

The probe and scrape routes are auth-exempt: `/healthz`, `/readyz`, `/metrics`,
`/openapi.json` and `/docs` do not need a credential.

## 2. Metrics

`/metrics` is served on the same port (`7373`), path `/metrics`, in Prometheus
text format. The shipped `ServiceMonitor` (`k8s/components/observability/`)
scrapes it every 15 seconds.

The series below are the ones this runbook uses. Each one is published by the
serving binary's metrics renderer in `crates/lumen-index`.

| Series | Type | Meaning |
|---|---|---|
| `lumen_index_writes_total` | counter | Accepted index writes. A flat line during active ingest is a write-path stall. |
| `lumen_search_latency_seconds` | histogram | Search latency. Use the `_bucket` series for percentiles. |
| `lumen_slow_queries_total` | counter | Queries at or above the `LUMEN_SLOW_QUERY_MS` threshold. |
| `lumen_posting_cache_hits_total` | counter | Posting-cache hits. A falling hit rate with rising latency points at cache pressure. |
| `lumen_storage_degraded` | gauge | `1` while the pod is in ENOSPC read-only mode. |
| `lumen_storage_full_errors_total` | counter | Writes rejected because storage is full. |
| `lumen_storage_bytes` | gauge | Bytes stored by the instance. |
| `lumen_segment_disk_bytes` | gauge | Bytes of segment files on disk. |
| `lumen_segment_checkpoint_started_total` | counter | Checkpoints started. |
| `lumen_segment_checkpoint_completed_total` | counter | Checkpoints that completed. |
| `lumen_segment_checkpoint_failed_total` | counter | Checkpoints that failed. |
| `lumen_segment_checkpoint_duration_seconds` | histogram | Checkpoint wall-clock duration. |
| `lumen_segment_pending_delta_bytes` | gauge | Pending change bytes waiting for a checkpoint. |
| `lumen_pending_change_total_bytes` | gauge | Total pending change memory. |
| `lumen_pending_change_high_water_bytes` | gauge | Highest pending change memory seen. |
| `lumen_segment_backpressure_total` | counter | Writes slowed or refused by checkpoint backpressure. |
| `lumen_raft_leader_known` | gauge | `1` on a replica whose shard has a known leader, `0` when it has none. Labelled by `shard`. Absent on a non-Raft deployment. |
| `lumen_process_rss_high_water_bytes` | gauge | Peak resident memory of the process. |
| `lumen_reshard_fence_active` | gauge | `1` while a reshard write fence is armed. |
| `lumen_reshard_fence_armed_unixtime` | gauge | Unix time the reshard fence was armed. |

Alerts the operator renders for each instance (in
`crates/lumen-operator/src/application/render/monitoring.rs`):

| Alert | Fires when | Section |
|---|---|---|
| `LumenNoReadyServingPods` | the StatefulSet has 0 ready replicas for 2 minutes | 3 |
| `LumenPodCrashLooping` | a pod restarts more than 3 times in 15 minutes | 7 |
| `LumenRaftLeaderAbsent` | a shard has no replica with a known leader for 2 minutes | 4 |
| `LumenPvcNearFull` | a `raft-<name>-<n>` PVC has under 10% free for 10 minutes | 5 |
| `LumenStorageDegraded` | a pod reports `lumen_storage_degraded == 1` for 1 minute | 5 |
| `LumenSlowQueries` | slow queries exceed 0.1/s for 10 minutes | 6 |
| `LumenReshardWorkflowStalled` | a reshard fence stays armed over 15 minutes | 4 |

The two `LumenConsumerLag*` alerts in `k8s/components/observability/` read a
series that this build does not publish. Do not use them as evidence until
that is fixed.

## 3. Pod not ready

```bash
kubectl -n <ns> get pods -l app.kubernetes.io/instance=<name> -o wide
kubectl -n <ns> describe pod <name>-<n> | tail -40
kubectl -n <ns> logs <name>-<n> --tail=200
curl -s http://127.0.0.1:7373/readyz   # via the port-forward above, for this pod
```

| Observation | Next action |
|---|---|
| Pod `Pending` | Read the scheduler event in `describe`. Capacity or affinity, not Lumen. |
| `CrashLoopBackOff` | `kubectl logs ... --previous`. Look for a startup refusal; [deployment](../deployment.md) lists the storage-root refusal cases. |
| `/readyz` `draining` | The pod is terminating. Wait for it to exit. |
| `/readyz` `restart required` | Read the logs for the cause, then restart that one pod. |
| Container `Running` but `/readyz` unreachable | Check the `http` port and the Service selector against the pod labels. |

When the whole instance is not ready, `LumenNoReadyServingPods` fires. Check
the StatefulSet first:

```bash
kubectl -n <ns> get statefulset <name> -o wide
```

## 4. One replica of three lost, or leaderless

Three replicas per shard tolerate the loss of one voter for a Raft-backed
instance. [STATUS](../../STATUS.md) records Raft shutdown and failover as
supported for a Raft-backed process, and it does not certify GKE regional
high availability. Do not promise survival of a lost replica beyond that.

When a replica is lost:

1. Find the pod and its PVC:

   ```bash
   kubectl -n <ns> get pods -l app.kubernetes.io/instance=<name> -o wide
   kubectl -n <ns> get pvc raft-<name>-<n>
   ```

2. Watch the shard's leader signal on the surviving replicas:

   ```bash
   curl -s http://127.0.0.1:7373/metrics | grep lumen_raft_leader_known
   ```

   `lumen_raft_leader_known` must be `1` on the surviving replicas of the shard
   after the election. If it stays `0` for 2 minutes, `LumenRaftLeaderAbsent`
   fires.

3. Leader-consistency reads fail with `503` on a replica that cannot serve
   them. When this replica is not the leader, the error code is
   `read_consistency_not_leader`, and the message names the current leader.
   When the shard has no reachable leader, the code is
   `read_consistency_no_leader`. Send the request to the named leader, or use
   a looser read consistency where the caller allows it.

4. When the lost pod does not come back, read its logs and the PVC events. Do
   not delete the PVC without a verified backup (section 9).

## 5. Disk or PVC full

```bash
kubectl -n <ns> exec <name>-<n> -- df -h
kubectl -n <ns> get pvc raft-<name>-<n> -o wide
curl -s http://127.0.0.1:7373/metrics | grep -E 'lumen_storage_(bytes|degraded)|lumen_storage_full_errors_total|lumen_segment_disk_bytes'
```

- `LumenPvcNearFull`: less than 10% free for 10 minutes. Expand the PVC through
  its StorageClass if the class allows expansion, or free space that is not
  Lumen data.
- `LumenStorageDegraded`: the pod is in ENOSPC read-only mode. Mutating writes
  return `507` with error code `storage_full`. Free or add space, then confirm
  `lumen_storage_degraded` returns to `0` and `lumen_storage_full_errors_total`
  stops rising.

Do not delete files inside the raft or segment directories to free space. That
can break the recovery rules in [deployment](../deployment.md).

## 6. Slow search

```bash
kubectl -n <ns> top pods -l app.kubernetes.io/instance=<name>
curl -s http://127.0.0.1:7373/metrics | grep -E 'lumen_search_latency_seconds_bucket|lumen_slow_queries_total|lumen_posting_cache_hits_total'
```

- `LumenSlowQueries` is the alert. It counts queries at or above the
  `LUMEN_SLOW_QUERY_MS` threshold (default 500 ms, per the operator's
  monitoring renderer). Check the threshold the pod runs with before you act
  on the count; see [configuration](../configuration.md) for runtime settings.
- Compare the `lumen_search_latency_seconds` percentiles with the time the
  alert started. A shift on one pod points at that pod; a shift on all pods
  points at load or a shared dependency.
- A falling `lumen_posting_cache_hits_total` rate with rising latency points at
  cache pressure. CPU saturation (`kubectl top`) points at capacity.
- Slow search during a checkpoint backlog: check section 7 first.

## 7. Checkpoint falling behind

```bash
curl -s http://127.0.0.1:7373/metrics | grep -E 'lumen_segment_checkpoint_(started|completed|failed)_total|lumen_segment_pending_delta_bytes|lumen_pending_change_(total|high_water)_bytes|lumen_segment_backpressure_total'
```

Read the rates, not the totals:

- `started` rising faster than `completed` means checkpoints are not finishing.
- `failed` rising means checkpoints are failing. Read the logs for the cause.
- `lumen_segment_pending_delta_bytes` and `lumen_pending_change_total_bytes`
  rising for a long time mean pending work is building up.
- `lumen_segment_backpressure_total` rising means writes are already being
  slowed by backpressure.

Logs name each checkpoint decision at startup:
`segment checkpoint startup decision`. Read them with
`kubectl logs <name>-<n>`.

## 8. Restart and recovery time

- `LumenPodCrashLooping` fires when a pod restarts more than 3 times in 15
  minutes. Read `kubectl logs ... --previous` and
  `kubectl describe pod` (look for `OOMKilled`).
- Startup logs name the recovery path taken:
  `segment checkpoint startup decision` for the segment checkpoint
  (`initialized_empty_root`, `recovered_uncommitted_empty`,
  `restored_current_empty`, `restored_current_generation` or
  `adopted_legacy_0428`), and `AOF startup decision` for the AOF
  (`aof_decision=no_tail` or `aof_decision=tail_replayed`).
- This build publishes no recovery-time metric. Measure it from the pod's
  start time to the first `200` on `/readyz`.

## 9. Backup and restore

Use the steps in [deployment](../deployment.md#smoke-checks) for the smoke
checks and the backup commands in [deployment](../deployment.md) for
`lumen standalone backup` and `lumen standalone restore`. For GKE, see
[GKE](../gke.md). This runbook does not restate those steps.

Restore with `--replace` only after you have confirmed the target. The current
GKE backup and restore results are not regional production evidence; see
[STATUS](../../STATUS.md).

## 10. Logs

```bash
kubectl -n <ns> logs <name>-<n> --tail=200
kubectl -n <ns> logs <name>-<n> --previous
kubectl -n <ns> get events --sort-by=.lastTimestamp
```

Error responses from the data plane carry a JSON `code` field (for example
`storage_full`, `read_consistency_not_leader` or `read_consistency_no_leader`).
Use that code, the HTTP status and the time window to match a client error to
the pod logs. This runbook does not name a per-request log field.

## Related

- [operator-control-plane.md](operator-control-plane.md) — the operator that
  reconciles the instance.
- [deployment.md](../deployment.md) — install, smoke checks, backup and restore.
- [gke.md](../gke.md) — the GKE release claim and support tiers.
- [configuration.md](../configuration.md) — runtime settings such as
  `LUMEN_SLOW_QUERY_MS`.
- [indexing.md](../indexing.md) — write, checkpoint and recovery rules.
- [STATUS](../../STATUS.md) — current support and limits.
