//! The serving pods' environment and ConfigMap, and the optional NetworkPolicy
//! (#2603).

use serde_json::{json, Value};
use service_k8s::render::{self, RenderCtx};

use crate::operator::application::render::{
    instance, serving_dns_names, CLIENT_PORT, COMPONENT, EMBEDDED_DATA_DIR, PEER_TLS_MOUNT_PATH,
    RAFT_PORT, SERVING_TLS_MOUNT_PATH,
};
use crate::operator::domain::lumen_spec::Lumen;

/// The optional per-instance NetworkPolicy (#2603), rendered only when
/// `spec.networkPolicy` is set.
///
/// Lumen's two ports have genuinely different audiences: `7373` is the search
/// API any workload may call, `7374` carries Raft — append entries, vote
/// requests, snapshot transfer — and must be reachable only from this
/// instance's own pods. The shared helper expresses exactly that split, so the
/// isolation posture is one contract across every service that adopts it
/// rather than six hand-written policies that drift.
///
/// The backup CronJob is deliberately *not* selected: it runs under its own
/// `<instance>-backup` component label, calls the client Service like any other
/// in-cluster client, and needs egress to object storage — the serving pods'
/// posture would be wrong for it in both directions.
pub(super) fn serving_network_policy(cx: &RenderCtx<'_>, name: &str) -> Value {
    render::common::network_policy(render::common::NetworkPolicy {
        cx,
        name,
        component: COMPONENT,
        client_ports: vec![CLIENT_PORT],
        peer_ports: vec![RAFT_PORT],
        // Lumen's operator path never configures an external WAL relay; backups
        // reach object storage over TLS, which the shared baseline already
        // allows.
        extra_egress: vec![],
    })
}

/// Container env layered onto the shared pod identity/downward-API scaffold:
/// Lumen's literal runtime knobs + the config-driven values (so a ConfigMap
/// edit can roll pods).
pub(super) fn serving_env(lumen: &Lumen) -> Vec<Value> {
    let cfg = format!("{}-config", instance(lumen));
    let from_cfg = |key: &str| json!({ "name": key, "valueFrom": { "configMapKeyRef": { "name": cfg, "key": key } } });
    let mut env = vec![
        json!({ "name": "LUMEN_HOST", "value": "0.0.0.0" }),
        json!({ "name": "LUMEN_WAL", "value": "auto" }),
        json!({ "name": "LUMEN_GRACE_SECS", "value": lumen.spec.serving.grace_secs.to_string() }),
        from_cfg("LUMEN_PORT"),
        from_cfg("LUMEN_LOG_FORMAT"),
        from_cfg("LUMEN_AUTH"),
        // #1384: mirror `serving_configmap`'s shard-map keys onto the
        // serving container so `lumen::config::shard_map_from_env` (used by
        // `serve()`'s `EngineShardSearch::new_with_shard_map` wiring) can
        // actually see the operator/reshard-driver-committed map instead of
        // always falling back to the balanced default.
        from_cfg("SHARD_MAP_VERSION"),
        from_cfg("VIRTUAL_BUCKET_COUNT"),
    ];
    if lumen.spec.log_level.is_some() {
        env.push(from_cfg("LUMEN_LOG_LEVEL"));
    }
    // SHARD_MAP_ASSIGNMENTS is only written into the ConfigMap once
    // assignments are non-empty (see `serving_configmap` below); a
    // `configMapKeyRef` to an absent key would fail the pod at start, so
    // this must mirror that same condition exactly.
    if !lumen.spec.shard_map.assignments.is_empty() {
        env.push(from_cfg("SHARD_MAP_ASSIGNMENTS"));
    }
    if let Some(bootstrap) = &lumen.spec.serving.bootstrap {
        env.push(json!({
            "name": "LUMEN_BOOTSTRAP_SEED_URI",
            "value": bootstrap.seed_uri,
        }));
        if let Some(limit) = bootstrap.max_bytes_per_sec {
            env.push(json!({
                "name": "LUMEN_BOOTSTRAP_MAX_BYTES_PER_SEC",
                "value": limit.to_string(),
            }));
        }
    }
    // #2890 R2: the four env vars `lumen::tls::PeerTlsConfig::from_env` reads,
    // pointing at the projected Secret. `LUMEN_PEER_MTLS=on` is what makes the
    // peer listener *require* a client certificate rather than merely offer
    // TLS, so it is set alongside the paths and never on its own.
    if lumen.spec.peer_tls_secret.is_some() {
        env.push(json!({ "name": "LUMEN_PEER_MTLS", "value": "on" }));
        env.push(json!({ "name": "LUMEN_PEER_TLS_CERT", "value": format!("{PEER_TLS_MOUNT_PATH}/tls.crt") }));
        env.push(json!({ "name": "LUMEN_PEER_TLS_KEY", "value": format!("{PEER_TLS_MOUNT_PATH}/tls.key") }));
        env.push(json!({ "name": "LUMEN_PEER_TLS_CA", "value": format!("{PEER_TLS_MOUNT_PATH}/ca.crt") }));
    }
    // #3113 R1: the serving listener's own four. `LUMEN_TLS=on` is what turns
    // the client port from h2c into a TLS listener that refuses rather than
    // downgrades; the paths alone would leave it cleartext, and the flag alone
    // would leave it with nothing to serve, so all four move together.
    if lumen.spec.serving_tls_secret.is_some() {
        env.push(json!({ "name": "LUMEN_TLS", "value": "on" }));
        env.push(json!({ "name": "LUMEN_TLS_CERT", "value": format!("{SERVING_TLS_MOUNT_PATH}/tls.crt") }));
        env.push(json!({ "name": "LUMEN_TLS_KEY", "value": format!("{SERVING_TLS_MOUNT_PATH}/tls.key") }));
        env.push(
            json!({ "name": "LUMEN_TLS_CA", "value": format!("{SERVING_TLS_MOUNT_PATH}/ca.crt") }),
        );
        // The names the leaf must answer to, from the operator that asked for
        // it — the pod has no other way to learn which Service it fronts, and
        // guessing from its own hostname would accept a certificate issued for
        // a different Service in the same namespace.
        env.push(json!({
            "name": "LUMEN_TLS_SERVER_NAMES",
            "value": serving_dns_names(lumen).join(","),
        }));
    }
    // #1387: `LUMEN_WAL=auto` above resolves to `Embedded` (`MemWal::new()`,
    // RAM-only) whenever `resolve_wal_backend` sees no raft cluster context —
    // exactly the `replicasPerShard <= 1` regime (its raft peer-identity env
    // is stripped in `serving_statefulset` below). Without `LUMEN_DATA_DIR`
    // that mode never touches the already-mounted `raft` PVC, so a pod
    // restart — including the reshard cutover's own rolling restart — wipes
    // all data despite the volume being durable. `replicasPerShard > 1` pods
    // run raft (already PVC-backed via `LUMEN_RAFT_DATA_DIR`) and are
    // unaffected by this block. `--persistence=segment` (not the CBOR
    // default) is deliberate: it activates the local AOF (`src/persistence/infrastructure/aof/`)
    // alongside the periodic segment checkpoint, giving `everysec`-fsync
    // crash durability (~1s RPO bound) instead of only surviving cleanly
    // between `LUMEN_SNAPSHOT_SECS` (default 300s) CBOR snapshots.
    if lumen.spec.replicas_per_shard <= 1 {
        env.push(json!({ "name": "LUMEN_DATA_DIR", "value": EMBEDDED_DATA_DIR }));
        env.push(json!({ "name": "LUMEN_PERSISTENCE", "value": "segment" }));
    }
    // #2477: pure exposure of the pre-existing `LUMEN_ADMISSION_*` env
    // grammar (`service_http::AdmissionConfig`) — no new semantics, only a
    // declarative path onto the same env vars `serve()` already reads.
    if let Some(admission) = &lumen.spec.admission {
        if let Some(v) = admission.read_capacity {
            env.push(json!({ "name": "LUMEN_ADMISSION_READ_CAPACITY", "value": v.to_string() }));
        }
        if let Some(v) = admission.write_capacity {
            env.push(json!({ "name": "LUMEN_ADMISSION_WRITE_CAPACITY", "value": v.to_string() }));
        }
        if let Some(v) = admission.admin_capacity {
            env.push(json!({ "name": "LUMEN_ADMISSION_ADMIN_CAPACITY", "value": v.to_string() }));
        }
        if let Some(v) = admission.refill_secs {
            env.push(json!({ "name": "LUMEN_ADMISSION_REFILL_SECS", "value": v.to_string() }));
        }
        if let Some(v) = admission.max_keys {
            env.push(json!({ "name": "LUMEN_ADMISSION_MAX_KEYS", "value": v.to_string() }));
        }
    }
    if let Some(limit) = lumen.spec.body_limit_bytes {
        env.push(json!({ "name": "LUMEN_BODY_LIMIT_BYTES", "value": limit.to_string() }));
    }
    env
}

pub(super) fn serving_configmap(lumen: &Lumen, cx: &RenderCtx<'_>) -> Value {
    let name = format!("{}-config", cx.name);
    let mut data = json!({
        "SHARD_COUNT": lumen.spec.shard_count.to_string(),
        "SHARD_MAP_VERSION": lumen.spec.shard_map.version.to_string(),
        "VIRTUAL_BUCKET_COUNT": lumen.spec.shard_map.virtual_bucket_count.to_string(),
        "LUMEN_LOG_FORMAT": lumen.spec.log_format.as_env(),
        "LUMEN_PORT": CLIENT_PORT.to_string(),
        "LUMEN_RAFT_PORT": RAFT_PORT.to_string(),
        "LUMEN_AUTH": lumen.spec.auth.as_env(),
    });
    if !lumen.spec.shard_map.assignments.is_empty() {
        data["SHARD_MAP_ASSIGNMENTS"] = json!(lumen
            .spec
            .shard_map
            .assignments
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(","));
    }
    if let Some(level) = &lumen.spec.log_level {
        data["LUMEN_LOG_LEVEL"] = json!(level);
    }
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": cx.meta(&name, COMPONENT),
        "data": data,
    })
}
