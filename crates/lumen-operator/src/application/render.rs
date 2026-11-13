//! Pure rendering: a [`Lumen`] spec → the set of child Kubernetes objects that
//! realize it. No cluster, no I/O — every object is a self-contained
//! `serde_json::Value` carrying `apiVersion`, `kind`, full `metadata` (labels +
//! owner reference), and `spec`/`data`. This is the operator's source of truth
//! and its primary test surface: assert the rendered objects, no kind needed.
//!
//! The objects mirror `k8s/base` + the staging/prod overlays exactly: a
//! serving StatefulSet (always — its `volumeClaimTemplates`-backed `raft` PVC
//! is the WAL's only durable home, even at `replicasPerShard:1`), its
//! headless Service, a ClusterIP Service, ConfigMap, PDB, serving
//! ServiceAccount, and a dedicated backup ServiceAccount. The backup identity
//! is intentionally cloud-neutral: deployment harnesses may annotate it for
//! Workload Identity without giving object-storage credentials to serving
//! pods.
//! Stateful data pods are not a direct HPA target. The reconcile loop in [`super::reconcile`]
//! server-side-applies whatever this returns.

pub mod backup;
pub mod identity;
pub mod monitoring;
pub mod serving_config;
pub mod serving_statefulset;

use serde_json::{json, Value};
use service_k8s::render::{self, RenderCtx};
use service_k8s::service::PruneTarget;

use crate::application::render::backup::{backup_cron_job, backup_service_account};
use crate::application::render::identity::attach_service_account_annotations;
use crate::application::render::monitoring::{prometheus_rule, service_monitor};
use crate::application::render::serving_config::{serving_configmap, serving_network_policy};
use crate::application::render::serving_statefulset::serving_statefulsets;
use crate::domain::lumen_spec::Lumen;

const APP: &str = "lumen";

const MANAGER: &str = "lumen-operator";

const API_VERSION: &str = "lumen.dev/v1alpha1";

const KIND: &str = "Lumen";

const COMPONENT: &str = "server";

const CLIENT_PORT: i32 = 7373;

const RAFT_PORT: i32 = 7374;

const BACKUP_COMPONENT: &str = "backup";

/// Component label for the cluster-scoped auth-delegation binding (#2876). Its
/// own value, not `server`: the sweep in [`super::reconcile`] selects on it,
/// and sharing a component with the namespaced serving children would make
/// that selector match objects the sweep has no business deleting.
const AUTH_DELEGATION_COMPONENT: &str = "auth-delegation";

/// The built-in ClusterRole granting `create` on `tokenreviews` and
/// `subjectaccessreviews` — the two APIs delegated request auth is made of.
/// Kubernetes ships and maintains it; lumen binds it rather than copying it.
const AUTH_DELEGATOR_ROLE: &str = "system:auth-delegator";

/// Which namespace a cluster-scoped child belongs to (#2876). The recommended
/// label set has no equivalent, and the object has no `metadata.namespace` of
/// its own to read.
const OWNER_NAMESPACE_LABEL: &str = "lumen.dev/owner-namespace";

/// The same owner namespace under Faberline's cross-repository key. The shared
/// GKE test cluster's teardown and reaper (faberline/aw
/// docs/standards/test-cluster.md) delete cluster-scoped objects carrying it
/// once that namespace is gone, so a run that dies before the sweep below can
/// reach the binding still leaves nothing behind.
const FABERLINE_OWNER_NAMESPACE_LABEL: &str = "faberline.dev/owner-namespace";

const HEADLESS_ENV_KEY: &str = "LUMEN_HEADLESS_SERVICE";

// #1387: embedded-mode persistence subtree, disjoint from the raft backend's
// `/var/lib/lumen/raft` default (`LUMEN_RAFT_DATA_DIR` in `bin/lumen.rs`) so
// both can coexist on the one `raft` PVC mount across a `replicasPerShard`
// change without colliding.
const EMBEDDED_DATA_DIR: &str = "/var/lib/lumen/data";

/// Where `spec.peerTlsSecret` is projected into every Raft member (#2890).
/// Lumen-specific and disjoint from `/var/lib/lumen`: peer identity is
/// read-only credential material, not index state, and must not land on the
/// PVC that survives a pod.
const PEER_TLS_MOUNT_PATH: &str = "/var/run/secrets/lumen-peer";

/// The pod-local volume carrying it.
const PEER_TLS_VOLUME: &str = "lumen-peer-tls";

/// The three keys the Secret must carry — the same contract Relay and Defer
/// project, and exactly what `peer_tls::PeerTlsConfig` loads.
pub const PEER_TLS_KEYS: [&str; 3] = ["tls.crt", "tls.key", "ca.crt"];

/// Where `spec.servingTlsSecret` is projected (#3113 R2). A separate path from
/// [`PEER_TLS_MOUNT_PATH`] because the two identities are separate: one mount
/// per listener means a misconfiguration points a listener at nothing rather
/// than at the other listener's key.
const SERVING_TLS_MOUNT_PATH: &str = "/var/run/secrets/lumen-serving";

/// The pod-local volume carrying it.
const SERVING_TLS_VOLUME: &str = "lumen-serving-tls";

/// The three keys the serving Secret must carry — same shape as the peer
/// Secret, different subject.
pub const SERVING_TLS_KEYS: [&str; 3] = ["tls.crt", "tls.key", "ca.crt"];

/// The Kubernetes Service DNS names the serving leaf answers to (#3113 R2).
///
/// Both forms, because both are real: in-cluster callers resolve the short
/// `<service>.<namespace>.svc` and `lumen connect` addresses the fully
/// qualified one, and a certificate carrying only one of them fails hostname
/// verification for half its callers. Kept identical to what
/// [`super::certificate::serving_profile`] requests — the names the operator
/// asks for and the names the pod is told to expect are one list, read twice.
pub fn serving_dns_names(lumen: &Lumen) -> Vec<String> {
    let (name, ns) = (instance(lumen), namespace(lumen));
    vec![
        format!("{name}.{ns}.svc"),
        format!("{name}.{ns}.svc.cluster.local"),
    ]
}

/// Resolve the instance name (defaults to `lumen` only when metadata is absent,
/// which never happens for a real CR).
fn instance(lumen: &Lumen) -> String {
    lumen
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| APP.to_string())
}

/// Resolve the namespace (defaults to `default` for unit construction).
fn namespace(lumen: &Lumen) -> String {
    lumen
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string())
}

/// lumen's render identity for the shared [`service_k8s::render`] helpers.
fn ctx<'a>(lumen: &Lumen, name: &'a str, ns: &'a str) -> RenderCtx<'a> {
    RenderCtx {
        app: APP,
        manager: MANAGER,
        api_version: API_VERSION,
        kind: KIND,
        name,
        ns,
        owner: owner_ref(lumen),
    }
}

/// The owner reference that ties a child to its `Lumen` CR, enabling
/// cascading garbage collection. Omitted when the CR has no `uid` (only in
/// unit construction); a live reconcile always has one.
fn owner_ref(lumen: &Lumen) -> Option<Value> {
    let uid = lumen.metadata.uid.clone()?;
    let name = lumen.metadata.name.clone()?;
    Some(render::owner_ref(API_VERSION, KIND, &name, &uid))
}

/// Stateful data pods are never a direct HPA target. A vanilla HPA changes
/// total pods, cannot preserve whole per-shard replica layers, and cannot
/// perform the Raft membership transition required before a replica delta.
/// The retained handoff loop consults this function to prune HPAs emitted by
/// older Lumen versions for every topology.
pub(crate) fn wants_hpa(_lumen: &Lumen) -> bool {
    false
}

/// The exact labels older Lumen versions stamped on their rendered HPA object
/// (mirrors [`service_k8s::render::RenderCtx::labels`]'s five recommended
/// labels). Exposed crate-private so `super::reconcile`'s HPA handoff loop
/// (#1385, R2) can confirm a live HPA found at this CR's name was actually
/// rendered by lumen — not a user-created object with a coincidentally
/// matching name — before deleting it.
pub(crate) fn hpa_labels(lumen: &Lumen) -> std::collections::BTreeMap<String, String> {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), APP.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), instance(lumen));
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        COMPONENT.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        MANAGER.to_string(),
    );
    labels.insert("app.kubernetes.io/part-of".to_string(), APP.to_string());
    labels
}

/// Render every child object for `lumen`, in dependency order (namespace-scoped
/// config first, then workloads).
///
/// The serving fleet is always a StatefulSet — with its durable
/// `volumeClaimTemplates`-backed `raft` PVC and headless Service — regardless
/// of `replicasPerShard`. No topology renders a direct HPA: single-member
/// scale-out would create uncoordinated copies, while raft-HA needs a
/// membership-aware whole-layer transition before changing pod count.
pub fn render(lumen: &Lumen) -> Vec<Value> {
    render_with_profile_opt(lumen, None)
}

/// Render child objects using an explicitly resolved capacity profile.
pub fn render_with_profile(
    lumen: &Lumen,
    profile: &crate::domain::capacity::preflight::ResolvedProfile,
) -> Vec<Value> {
    render_with_profile_opt(lumen, Some(profile))
}

fn render_with_profile_opt(
    lumen: &Lumen,
    profile: Option<&crate::domain::capacity::preflight::ResolvedProfile>,
) -> Vec<Value> {
    let name = instance(lumen);
    let ns = namespace(lumen);
    let cx = ctx(lumen, &name, &ns);
    let headless = format!("{name}-headless");
    let mut out = Vec::new();
    // Skip rendering the workload ServiceAccount entirely when the deployer
    // points at a pre-existing, externally-managed one (#2497): the operator
    // must never create, own, or delete an SA it doesn't render.
    if lumen.spec.service_account_name.is_none() {
        let mut sa = render::service_account(&cx, COMPONENT);
        attach_service_account_annotations(&mut sa, &lumen.spec.service_account_annotations);
        out.push(sa);
    }
    let mut bsa = backup_service_account(&cx);
    attach_service_account_annotations(&mut bsa, &lumen.spec.service_account_annotations);
    out.push(bsa);
    out.push(serving_configmap(lumen, &cx));
    out.extend(serving_statefulsets(lumen, &cx, &headless, profile));
    out.extend([
        render::headless_service_with_ports(
            &cx,
            &headless,
            COMPONENT,
            vec![
                json!({ "name": "http", "port": CLIENT_PORT, "targetPort": "http", "protocol": "TCP" }),
                json!({ "name": "raft", "port": RAFT_PORT, "targetPort": "raft", "protocol": "TCP" }),
            ],
        ),
        render::client_service(&cx, &name, COMPONENT, CLIENT_PORT),
    ]);
    out.extend(serving_pdbs(lumen, &cx, &name));
    if lumen.spec.network_policy {
        out.push(serving_network_policy(&cx, &name));
    }
    if lumen.spec.observability {
        out.push(service_monitor(&cx));
        out.push(prometheus_rule(&cx));
    }
    // Optional scheduled backup runner: only when a policy is configured (#808).
    if let Some(cj) = backup_cron_job(lumen, &cx) {
        out.push(cj);
    }
    out
}

/// PodDisruptionBudgets for the serving fleet: one for a single shard, one per
/// shard when `shardCount > 1` (#228). A PDB only limits voluntary evictions
/// (node drains, the eviction API); it does not control StatefulSet rolling
/// updates.
fn serving_pdbs(lumen: &Lumen, cx: &RenderCtx<'_>, name: &str) -> Vec<Value> {
    if lumen.spec.shard_count <= 1 {
        return vec![render::pdb(cx, name, COMPONENT, 1)];
    }
    // Quorum-safe: a shard may lose as many voters as keeps a majority.
    let voters = lumen.spec.voter_count.max(1);
    let quorum = voters / 2 + 1;
    let max_unavailable = voters.saturating_sub(quorum).max(1) as i32;
    (0..lumen.spec.shard_count)
        .map(|shard| {
            let mut pdb = render::pdb(cx, &format!("{name}-s{shard}"), COMPONENT, max_unavailable);
            let mut selector = cx.selector(COMPONENT);
            selector["lumen.dev/shard"] = json!(shard.to_string());
            pdb["spec"]["selector"]["matchLabels"] = selector;
            pdb
        })
        .collect()
}

/// Children a previous spec rendered that this one no longer wants (#2603).
///
/// The NetworkPolicy qualifies, and it qualifies because it is the one
/// conditional child whose *presence* changes runtime behavior rather than
/// just adding an object. The single serving StatefulSet and PDB also qualify
/// once the shard count goes above one (#228). Leaving a stale ServiceMonitor around scrapes a
/// metric nobody reads; leaving a stale NetworkPolicy around keeps dropping
/// traffic the spec has stopped asking to drop. Flipping `networkPolicy` to
/// `false` therefore has to actively remove it, or the field is opt-in only.
///
pub fn prunes(lumen: &Lumen) -> Vec<PruneTarget> {
    let mut targets = Vec::new();
    // Going above one shard replaces the single serving StatefulSet and PDB
    // (named after the instance) with per-shard ones (`<name>-s<k>`) (#228).
    if lumen.spec.shard_count > 1 {
        targets.push(PruneTarget {
            api_version: "apps/v1",
            kind: "StatefulSet",
            name: instance(lumen),
        });
        targets.push(PruneTarget {
            api_version: "policy/v1",
            kind: "PodDisruptionBudget",
            name: instance(lumen),
        });
    }
    if !lumen.spec.network_policy {
        targets.push(PruneTarget {
            api_version: "networking.k8s.io/v1",
            kind: "NetworkPolicy",
            // Resolved through the same `instance` helper `render` uses, so this is
            // the exact inverse of the branch that creates it rather than an
            // independent guess at the name.
            name: instance(lumen),
        });
    }
    targets
}

/// The ServiceAccount the serving pods actually run as (#2497, #2876).
///
/// Two callers depend on this being one answer: [`serving_statefulset`] puts it
/// in the pod spec, and [`auth_delegator_binding`] grants it delegated review.
/// If they resolved it separately, a spec that names an external SA would run
/// pods as one identity and authorize a different one — and the symptom would
/// be every request failing authentication, not an obviously wrong manifest.
///
/// [`auth_delegator_binding`]: crate::application::render::identity::auth_delegator_binding
pub(crate) fn serving_service_account_name(lumen: &Lumen) -> String {
    lumen
        .spec
        .service_account_name
        .clone()
        .unwrap_or_else(|| instance(lumen))
}

#[cfg(test)]
mod tests;
