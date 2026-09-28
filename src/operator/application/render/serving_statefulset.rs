//! The serving StatefulSet, layered on the shared workload primitive.

use serde_json::{json, Value};
use service_k8s::render::{self, RenderCtx, ServiceStatefulSet, WorkloadVolumeClaim};

use crate::operator::application::render::serving_config::serving_env;
use crate::operator::application::render::{
    serving_service_account_name, CLIENT_PORT, COMPONENT, EMBEDDED_DATA_DIR, HEADLESS_ENV_KEY,
    PEER_TLS_KEYS, PEER_TLS_MOUNT_PATH, PEER_TLS_VOLUME, RAFT_PORT, SERVING_TLS_KEYS,
    SERVING_TLS_MOUNT_PATH, SERVING_TLS_VOLUME,
};
use crate::operator::domain::lumen_spec::Lumen;

/// The serving fleet: the shared workload primitive provides the StatefulSet's
/// identity, headless binding, downward-API pod identity, and common pod
/// template shell; Lumen layers its own ConfigMap-driven env, auth-secret
/// mount, PVC, probes, and observability annotations on top. At
/// `replicasPerShard <= 1` (single shard) the single-member path strips the
/// raft-only env vars and resets the apply-time replica count to exactly 1
/// (#1317) — more than one pod would be an uncoordinated shard-0 copy with no
/// consensus link.
pub(super) fn serving_statefulset(
    lumen: &Lumen,
    cx: &RenderCtx<'_>,
    headless: &str,
    profile: Option<&crate::operator::domain::capacity::preflight::ResolvedProfile>,
) -> Value {
    let s = &lumen.spec.serving;
    let sa_name = serving_service_account_name(lumen);
    let res = render::requested_resources(&s.cpu, &s.memory);
    let mut volume_mounts = vec![json!({ "name": "tmp", "mountPath": "/tmp" })];
    let mut volumes = vec![json!({ "name": "tmp", "emptyDir": {} })];
    // #2890 R2: the instance's Raft identity, projected read-only. `items`
    // rather than a whole-Secret mount so the three keys the peer transport
    // loads are the three keys that reach the pod — an extra key added to the
    // Secret later cannot silently become part of what the container sees.
    if let Some(secret) = lumen.spec.peer_tls_secret.as_deref() {
        volumes.push(json!({
            "name": PEER_TLS_VOLUME,
            "secret": {
                "secretName": secret,
                "items": PEER_TLS_KEYS
                    .iter()
                    .map(|key| json!({ "key": key, "path": key }))
                    .collect::<Vec<_>>(),
            },
        }));
        volume_mounts.push(json!({
            "name": PEER_TLS_VOLUME,
            "mountPath": PEER_TLS_MOUNT_PATH,
            "readOnly": true,
        }));
    }
    // #3113 R2: the serving leaf, projected the same way and to its own path.
    // Kubernetes refreshes a projected Secret in place, so a renewed leaf
    // reaches the container without a new pod — which is what makes R9's
    // "no Pod rollout" a property of the projection rather than of the
    // controller's restraint.
    if let Some(secret) = lumen.spec.serving_tls_secret.as_deref() {
        volumes.push(json!({
            "name": SERVING_TLS_VOLUME,
            "secret": {
                "secretName": secret,
                "items": SERVING_TLS_KEYS
                    .iter()
                    .map(|key| json!({ "key": key, "path": key }))
                    .collect::<Vec<_>>(),
            },
        }));
        volume_mounts.push(json!({
            "name": SERVING_TLS_VOLUME,
            "mountPath": SERVING_TLS_MOUNT_PATH,
            "readOnly": true,
        }));
    }
    // Probes follow the port. A kubelet that spoke cleartext to a TLS listener
    // would read every failed handshake as an unhealthy pod and restart a
    // container that was serving correctly (#3113 R2/AC2).
    let probe_scheme = if lumen.spec.serving_tls_secret.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    };
    let spread = |key: &str| {
        json!({
            "maxSkew": 1,
            "topologyKey": key,
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": { "matchLabels": cx.selector(COMPONENT) },
        })
    };
    let image_pull_policy = lumen
        .spec
        .image_pull_policy
        .as_deref()
        .unwrap_or("IfNotPresent");
    let mut pvc_template = json!({
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": s.raft_storage.clone() } },
        },
    });
    if let Some(sc) = &s.raft_storage_class {
        pvc_template["spec"]["storageClassName"] = json!(sc);
    }
    // Embedded mode keeps its index and AOF below the same PVC as the Raft
    // state. The shared renderer validates this as one exact direct child of
    // the `raft` parent mount and orders the pair parent-first. Replicated
    // shards use the Raft backend directly and must not receive this overlay.
    if lumen.spec.replicas_per_shard <= 1 {
        volume_mounts.push(json!({
            "name": "raft",
            "mountPath": EMBEDDED_DATA_DIR,
            "subPath": "data",
            "readOnly": false,
        }));
    }
    let serving_plan = ServiceStatefulSet {
        cx,
        name: cx.name,
        component: COMPONENT,
        image: lumen.spec.image.as_str(),
        image_pull_policy,
        command: vec!["lumen".into(), "serve".into()],
        args: vec![],
        ports: vec![
            json!({ "name": "http", "containerPort": CLIENT_PORT, "protocol": "TCP" }),
            json!({ "name": "raft", "containerPort": RAFT_PORT, "protocol": "TCP" }),
        ],
        headless_service: headless,
        shard_count: lumen.spec.shard_count,
        replicas_per_shard: lumen.spec.replicas_per_shard,
        voter_count: lumen.spec.voter_count,
        headless_env_key: HEADLESS_ENV_KEY,
        // External SA name wins when configured (#2497); default to the
        // operator-owned per-instance SA when unset. Resolved through the same
        // helper the auth-delegator binding uses, so the identity the pods run
        // as and the identity that is granted delegated review cannot diverge.
        service_account_name: Some(&sa_name),
        env: serving_env(lumen),
        env_from: vec![],
        resources: res,
        pod_annotations: Some(json!({
            "prometheus.io/scrape": "true",
            "prometheus.io/port": CLIENT_PORT.to_string(),
            "prometheus.io/path": "/metrics",
        })),
        pod_security_context: Some(render::restricted_pod_security_context()),
        container_security_context: Some(render::restricted_container_security_context()),
        termination_grace_period_seconds: Some(s.grace_secs),
        readiness_probe: Some(json!({
            "httpGet": { "path": "/readyz", "port": "http", "scheme": probe_scheme },
            "initialDelaySeconds": 5, "periodSeconds": 10,
            "timeoutSeconds": 3, "failureThreshold": 60,
        })),
        liveness_probe: Some(json!({
            "httpGet": { "path": "/healthz", "port": "http", "scheme": probe_scheme },
            "initialDelaySeconds": 15, "periodSeconds": 30,
            "timeoutSeconds": 5, "failureThreshold": 3,
        })),
        startup_probe: Some(json!({
            "httpGet": { "path": "/healthz", "port": "http", "scheme": probe_scheme },
            "periodSeconds": 5, "timeoutSeconds": 3, "failureThreshold": 120,
        })),
        lifecycle: None,
        volumes,
        volume_mounts,
        affinity: Some(crate::operator::domain::capacity::placement::cross_namespace_dedicated_data_node_affinity()),
        // `profile` supplies the operator-resolved capacity selector and toleration;
        // `spec.placement` layers additional user selectors/tolerations.
        // The cross-namespace anti-affinity above stays operator-owned so data members of any
        // instance or namespace are kept mutually exclusive per node.
        node_selector: {
            let mut sel = std::collections::BTreeMap::new();
            if let Some(profile) = profile {
                sel.insert(profile.selector_key.clone(), profile.selector_value.clone());
            }
            for (k, v) in &lumen.spec.placement.node_selector {
                sel.insert(k.clone(), v.clone());
            }
            (!sel.is_empty()).then(|| json!(sel))
        },
        tolerations: {
            let mut tols = Vec::new();
            if let Some(profile) = profile {
                tols.push(json!({
                    "key": profile.selector_key,
                    "operator": "Equal",
                    "value": profile.selector_value,
                    "effect": "NoSchedule",
                }));
            }
            for t in &lumen.spec.placement.tolerations {
                tols.push(json!(t));
            }
            tols
        },
        topology_spread_constraints: vec![
            spread("topology.kubernetes.io/zone"),
            spread("kubernetes.io/hostname"),
        ],
        revision_history_limit: Some(5),
        update_strategy: Some(json!({ "type": "RollingUpdate" })),
        volume_claim: Some(WorkloadVolumeClaim {
            name: "raft".into(),
            template: pvc_template,
            mount_path: "/var/lib/lumen",
            read_only: false,
        }),
    };
    // Keep Kubernetes' legacy Service env injection from replacing the
    // ConfigMap-backed numeric `LUMEN_PORT` with a `tcp://...` value.
    let mut sts = render::service_statefulset_with_service_links(serving_plan, false);
    if lumen.spec.replicas_per_shard <= 1 {
        if let Some(spec) = sts["spec"].as_object_mut() {
            let replicas = if lumen.spec.shard_count > 1 {
                lumen.spec.shard_count as i32
            } else {
                // Single shard, single member, no raft consensus (#1317):
                // clamp to exactly 1 — see `LumenSpec::storage_pod_count`
                // for why more than one pod here means uncoordinated shard-0 copies.
                1
            };
            spec.insert("replicas".into(), json!(replicas));
        }
        if let Some(env) = sts["spec"]["template"]["spec"]["containers"][0]["env"].as_array_mut() {
            // `shard_count > 1` at `replicasPerShard <= 1` is the routed
            // serving topology (#1398): each pod still needs its own stable
            // headless DNS name to forward cross-shard requests one hop to
            // the owning pod (`lumen::routing::shard_host`), so
            // `HEADLESS_ENV_KEY` is only stripped alongside the raft peer
            // env when there is truly one physical shard and nothing to
            // route to.
            let strip_headless = lumen.spec.shard_count <= 1;
            env.retain(|value| {
                let Some(name) = value["name"].as_str() else {
                    return true;
                };
                if name == HEADLESS_ENV_KEY {
                    return !strip_headless;
                }
                !matches!(name, "REPLICAS_PER_SHARD" | "VOTER_COUNT")
            });
        }
    }
    sts
}
