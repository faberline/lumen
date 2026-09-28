//! The ServiceMonitor and PrometheusRule objects.

use serde_json::{json, Value};
use service_k8s::render::RenderCtx;

use crate::operator::application::render::COMPONENT;

// ---- Observability (optional) ---------------------------------------------

pub(super) fn service_monitor(cx: &RenderCtx<'_>) -> Value {
    json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": cx.meta(cx.name, COMPONENT),
        "spec": {
            "selector": { "matchLabels": cx.selector(COMPONENT) },
            "endpoints": [{ "port": "http", "path": "/metrics", "interval": "30s" }],
        },
    })
}

// #2475: alerts are added here only when the metric an `expr` reads is
// actually published today. `LumenRaftLeaderAbsent` reads this pod's
// self-scraped `lumen_raft_leader_known` gauge (`src/metrics.rs`, wired in
// `src/bin/lumen.rs`). `LumenPvcNearFull` reads the kubelet's
// `kubelet_volume_stats_*` series against the `raft-<name>-<ordinal>`
// StatefulSet PVC name pattern (`volumeClaimTemplates` name is `raft`, see
// `serving_statefulset`). `LumenStorageDegraded` (#2516) reads
// `lumen_storage_degraded`, the self-scraped gauge a pod sets to `1` the
// moment a durable write path (AOF append, segment/RDB checkpoint save, or
// raft log append) actually hits ENOSPC and the pod enters sticky degraded
// read-only mode (`Metrics::mark_storage_degraded`, `src/coordinator.rs` /
// `src/bin/lumen.rs` / `src/raft_sm.rs`) — it is the "disk is now actually
// full and writes are failing" companion to `LumenPvcNearFull`'s "disk is
// nearly full" early warning. `LumenReshardWorkflowStalled` is a PARTIAL proxy:
// the reshard driver's phase machine (`LumenStatus.reshard`, CR status) is
// not published to Prometheus by any customresourcestate config this
// operator ships, so this alert reads the driver's write-fence instead
// (`lumen_reshard_fence_active`/`_armed_unixtime`, `src/api.rs`'s
// `reshard_fence` handler) — it only catches a fence left armed past the
// fenced final `CatchingUp` pass's expected duration, not an early stall in
// `PrepareSplit`/`Splitting` (which never arms a fence at all). A full fix
// needs either a customresourcestate config or a driver-side liveness gauge
// and is out of this WI's scope. `LumenSlowQueries` (#2519) reads
// `lumen_slow_queries_total` (`src/metrics.rs`'s `Metrics::observe_search`),
// incremented once per search whose latency meets or exceeds the
// `LUMEN_SLOW_QUERY_MS` threshold (default 500ms).
pub(super) fn prometheus_rule(cx: &RenderCtx<'_>) -> Value {
    json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PrometheusRule",
        "metadata": cx.meta(cx.name, COMPONENT),
        "spec": {
            "groups": [{
                "name": "lumen.slo",
                "rules": [
                    {
                        "alert": "LumenNoReadyServingPods",
                        "expr": format!(
                            "kube_statefulset_status_replicas_ready{{statefulset=\"{}\", namespace=\"{}\"}} == 0",
                            cx.name, cx.ns
                        ),
                        "for": "2m",
                        "labels": { "severity": "critical" },
                        "annotations": {
                            "summary": "No ready lumen serving pods for {{ $labels.statefulset }}",
                            "runbook": "kubectl get pods -n {{ $labels.namespace }} -l app.kubernetes.io/instance={{ $labels.statefulset }} -o wide; check pod events/logs for crash or readiness-probe failure.",
                        },
                    },
                    {
                        "alert": "LumenBackupCronJobFailed",
                        "expr": format!(
                            "kube_job_status_failed{{namespace=\"{}\", job_name=~\"^{}-backup-.*\"}} >= 2",
                            cx.ns, cx.name
                        ),
                        "for": "5m",
                        "labels": { "severity": "warning" },
                        "annotations": {
                            "summary": "lumen backup CronJob {{ $labels.job_name }} has failed repeatedly (>=2 retained failed Jobs) in {{ $labels.namespace }}",
                            "runbook": "kubectl logs -n {{ $labels.namespace }} job/{{ $labels.job_name }}; a single failed Job is retained (not alerted) as a flake tolerance, so this means the CronJob is failing on every recent run.",
                        },
                    },
                    {
                        "alert": "LumenPodCrashLooping",
                        "expr": format!(
                            "increase(kube_pod_container_status_restarts_total{{namespace=\"{}\", pod=~\"^{}-[0-9]+$\"}}[15m]) > 3",
                            cx.ns, cx.name
                        ),
                        "for": "5m",
                        "labels": { "severity": "warning" },
                        "annotations": {
                            "summary": "lumen pod {{ $labels.pod }} is crash-looping in {{ $labels.namespace }}",
                            "runbook": "kubectl logs -n {{ $labels.namespace }} {{ $labels.pod }} --previous; check for OOMKilled (kubectl describe pod) or a bad rollout image.",
                        },
                    },
                    {
                        "alert": "LumenRaftLeaderAbsent",
                        "expr": format!(
                            "max(lumen_raft_leader_known{{namespace=\"{}\"}}) by (shard) == 0",
                            cx.ns
                        ),
                        "for": "2m",
                        "labels": { "severity": "critical" },
                        "annotations": {
                            "summary": "No lumen replica of shard {{ $labels.shard }} reports a known raft leader in {{ $labels.namespace }}",
                            "runbook": "kubectl get pods -n {{ $labels.namespace }} -o wide; map shard {{ $labels.shard }} to its StatefulSet ordinals (README §Dynamic Shard Topology) and check those pods for a minority-partition network split or a majority of voters down.",
                        },
                    },
                    {
                        "alert": "LumenReshardWorkflowStalled",
                        "expr": format!(
                            "lumen_reshard_fence_active{{namespace=\"{}\"}} == 1 and (time() - lumen_reshard_fence_armed_unixtime{{namespace=\"{}\"}}) > 900",
                            cx.ns, cx.ns
                        ),
                        "for": "1m",
                        "labels": { "severity": "warning" },
                        "annotations": {
                            "summary": "lumen reshard write fence has stayed armed for over 15m in {{ $labels.namespace }} -- the driver's final catch-up pass may be stuck",
                            "runbook": "kubectl get lumen -n {{ $labels.namespace }} -o jsonpath='{.items[*].status.reshard}'; check the reshard-driver operator pod's logs for a stuck :apply/:prune/:evict admin call, then POST /admin/reshard:fence with empty buckets to clear a wedged fence if the workflow is abandoned. Coverage note: this alert only detects a stall in the fenced final CatchingUp pass, not an early PrepareSplit/Splitting stall (#2475).",
                        },
                    },
                    {
                        "alert": "LumenPvcNearFull",
                        "expr": format!(
                            "kubelet_volume_stats_available_bytes{{namespace=\"{}\", persistentvolumeclaim=~\"^raft-{}-[0-9]+$\"}} / kubelet_volume_stats_capacity_bytes{{namespace=\"{}\", persistentvolumeclaim=~\"^raft-{}-[0-9]+$\"}} < 0.1",
                            cx.ns, cx.name, cx.ns, cx.name
                        ),
                        "for": "10m",
                        "labels": { "severity": "warning" },
                        "annotations": {
                            "summary": "lumen raft PVC {{ $labels.persistentvolumeclaim }} has less than 10% free space in {{ $labels.namespace }}",
                            "runbook": "kubectl get pvc -n {{ $labels.namespace }} {{ $labels.persistentvolumeclaim }} -o wide; the owning pod is {{ $labels.persistentvolumeclaim }} with its `raft-` prefix stripped -- exec in and run `df -h`, then expand volumeClaimTemplates (if the StorageClass supports online resize) or prune old snapshots/segments.",
                        },
                    },
                    {
                        "alert": "LumenStorageDegraded",
                        "expr": format!(
                            "max(lumen_storage_degraded{{namespace=\"{}\"}}) by (pod) == 1",
                            cx.ns
                        ),
                        "for": "1m",
                        "labels": { "severity": "critical" },
                        "annotations": {
                            "summary": "lumen pod {{ $labels.pod }} is in ENOSPC degraded read-only mode in {{ $labels.namespace }} -- mutating writes are being fast-failed with 507 storage_full",
                            "runbook": "kubectl exec -n {{ $labels.namespace }} {{ $labels.pod }} -- df -h; a durable write (AOF append, segment/RDB checkpoint, or raft log append) hit ENOSPC on the raft-<ordinal> PVC -- LumenPvcNearFull should have fired earlier as the early warning, so also check why it didn't. Free space (prune old snapshots/segments, or expand volumeClaimTemplates if the StorageClass supports online resize); the pod's periodic re-probe (LUMEN_STORAGE_FULL_REPROBE_SECS, default 30s) clears this automatically once a probe write succeeds, or restart the pod. If disk pressure traces back to an unfinished reshard leaving stale buckets, see LumenReshardWorkflowStalled too.",
                        },
                    },
                    {
                        "alert": "LumenSlowQueries",
                        "expr": format!(
                            "rate(lumen_slow_queries_total{{namespace=\"{}\"}}[5m]) > 0.1",
                            cx.ns
                        ),
                        "for": "10m",
                        "labels": { "severity": "warning" },
                        "annotations": {
                            "summary": "lumen is serving slow queries (>0.1/s at/above the LUMEN_SLOW_QUERY_MS threshold) for over 10m in {{ $labels.namespace }}",
                            "runbook": "kubectl top pods -n {{ $labels.namespace }}; check lumen_search_latency_seconds_bucket for the shifted percentile, look for a hot shard/collection, undersized HNSW ef, or resource pressure, and consider raising LUMEN_SLOW_QUERY_MS if the new baseline is expected.",
                        },
                    },
                ],
            }],
        },
    })
}
