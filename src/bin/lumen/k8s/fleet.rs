//! `lumen k8s fleet render`: the `LumenFleet` that declares every data plane.

use crate::cli::k8s::{K8sFleetProfile, K8sFleetRenderArgs};
use crate::k8s::render::{profile_spec_body, InstanceBody};

/// `spec.defaults` for `k8s fleet render --profile template`, at four-space
/// indent.
///
/// Names no token source, because there is no longer one to name: a caller's
/// identity comes from the cluster's TokenRequest/TokenReview path, not from
/// anything a fleet or its instances configure (#2872). A template that still
/// carried a credential field would hand every app team a spec the API server
/// now rejects outright.
const FLEET_TEMPLATE_DEFAULTS: &str = "\
    \x20   image: __IMAGE__\n\
    \x20   imagePullPolicy: IfNotPresent\n\
    \x20   shardCount: REPLACE_ME__SHARD_COUNT\n\
    \x20   replicasPerShard: REPLACE_ME__REPLICAS_PER_SHARD\n\
    \x20   voterCount: REPLACE_ME__VOTER_COUNT\n\
    \x20   logFormat: json\n\
    \x20   auth: required\n\
    \x20   # Serving leaf every data plane presents on :7373, one Secret name\n\
    \x20   # per tenant namespace. Stated rather than left out: an unset\n\
    \x20   # serving Secret is not an error, it is cleartext (#3113).\n\
    \x20   servingTlsSecret: REPLACE_ME__SERVING_TLS_SECRET\n\
    \x20   # Workload Identity KSA every data plane runs as.\n\
    \x20   serviceAccountName: REPLACE_ME__WORKLOAD_IDENTITY_KSA\n\
    \x20   placement:\n\
    \x20     # Which node pool every data plane lands on.\n\
    \x20     nodeSelector:\n\
    \x20       REPLACE_ME__NODE_POOL_LABEL: REPLACE_ME__NODE_POOL_VALUE\n\
    \x20     # Uncomment when the pool is tainted to repel other workloads.\n\
    \x20     # tolerations:\n\
    \x20     #   - key: REPLACE_ME__TAINT_KEY\n\
    \x20     #     operator: Equal\n\
    \x20     #     value: REPLACE_ME__TAINT_VALUE\n\
    \x20     #     effect: NoSchedule\n\
    \x20   serving:\n\
    \x20     cpu: \"1\"\n\
    \x20     memory: 4Gi\n\
    \x20     # SSD vs standard disk is the StorageClass; omit to take the\n\
    \x20     # cluster default.\n\
    \x20     raftStorageClass: REPLACE_ME__STORAGE_CLASS\n";

/// Render a `LumenFleet` — the single cluster-scoped object the platform team
/// applies into the control-plane namespace to declare every data plane.
///
/// The split the document is built to teach: `defaults` holds what the
/// platform owns and every tenant shares (image, node pool, StorageClass,
/// ServiceAccount); each `instances[].spec` holds what one app team owns (its
/// CPU/memory request — which is what makes replica-shard autoscaling
/// trigger — its disk size, and its credential source).
pub(super) fn render_fleet_yaml(args: &K8sFleetRenderArgs) -> String {
    let default_version = env!("CARGO_PKG_VERSION");
    let (default_name, default_image, body) = match args.profile {
        K8sFleetProfile::Dev => ("search", "lumen:latest".to_string(), InstanceBody::Dev),
        K8sFleetProfile::Prod => (
            "lumen",
            format!("ghcr.io/faberline/lumen:{default_version}"),
            InstanceBody::Prod,
        ),
        K8sFleetProfile::Template => (
            "REPLACE_ME__FLEET_NAME",
            "REPLACE_ME__REGISTRY/lumen:REPLACE_ME__IMAGE_TAG".to_string(),
            InstanceBody::Template,
        ),
    };
    let name = args.name.as_deref().unwrap_or(default_name);
    let image = args.image.as_deref().unwrap_or(&default_image);

    // `defaults` is a whole LumenSpec. dev/prod reuse the instance profile
    // bodies verbatim, one level deeper, so a fleet's "prod" and a standalone
    // "prod" CR cannot come to disagree. `template` gets its own body: every
    // value there is a REPLACE_ME with no semantics to keep in step, and it
    // has to name knobs (node pool, StorageClass, ServiceAccount) that only
    // make sense on the fleet — appending them to the shared body would emit
    // `serving:` twice and silently drop the first one.
    let defaults: String = match args.profile {
        K8sFleetProfile::Template => FLEET_TEMPLATE_DEFAULTS.replace("__IMAGE__", image),
        _ => profile_spec_body(body, image)
            .lines()
            .map(|line| format!("  {line}\n"))
            .collect(),
    };

    let mut yaml = format!(
        "# One object, applied once by the platform team. Every data-plane\n\
         # namespace this cluster serves is declared below; the operator\n\
         # materializes one `Lumen` per entry into the namespace named.\n\
         #\n\
         # The namespaces must already exist — the fleet never creates them.\n\
         apiVersion: lumen.dev/v1alpha1\n\
         kind: LumenFleet\n\
         metadata:\n  name: {name}\n\
         spec:\n\
         \x20 # Platform-owned: what every tenant shares.\n\
         \x20 defaults:\n{defaults}"
    );

    match args.profile {
        K8sFleetProfile::Dev => {
            yaml.push_str(
                "  instances:\n\
                 \x20   - namespace: default\n",
            );
        }
        K8sFleetProfile::Prod => {
            yaml.push_str(
                "    placement:\n\
                 \x20     nodeSelector:\n\
                 \x20       cloud.google.com/gke-nodepool: lumen\n\
                 \x20 # App-team-owned: what one tenant sets for itself. Each\n\
                 \x20 # `spec` is a merge patch over `defaults` above — name only\n\
                 \x20 # what differs; everything unnamed is inherited.\n\
                 \x20 instances:\n\
                 \x20   - namespace: team-a\n\
                 \x20     spec:\n\
                 \x20       serving:\n\
                 \x20         cpu: \"4\"\n\
                 \x20         memory: 16Gi\n\
                 \x20         raftStorage: 200Gi\n\
                 \x20   - namespace: team-b\n\
                 \x20     spec:\n\
                 \x20       serving:\n\
                 \x20         cpu: \"1\"\n\
                 \x20         memory: 4Gi\n",
            );
        }
        K8sFleetProfile::Template => {
            yaml.push_str(
                "  # App-team-owned: what one tenant sets for itself. Each\n\
                 \x20 # `spec` is a merge patch over `defaults` above — name only\n\
                 \x20 # what differs; everything unnamed is inherited. A `null`\n\
                 \x20 # value removes an inherited field.\n\
                 \x20 instances:\n\
                 \x20   - namespace: REPLACE_ME__APP_NAMESPACE\n\
                 \x20     spec:\n\
                 \x20       serving:\n\
                 \x20         # Requests, not just limits: replica-shard\n\
                 \x20         # autoscaling triggers off these.\n\
                 \x20         cpu: REPLACE_ME__CPU\n\
                 \x20         memory: REPLACE_ME__MEMORY\n\
                 \x20         raftStorage: REPLACE_ME__DISK\n\
                 \x20 # Retain (default) leaves an instance running when its entry\n\
                 \x20 # is removed; Delete removes it and its PVCs.\n\
                 \x20 prunePolicy: Retain\n",
            );
        }
    }
    cli_std::artifact::ensure_trailing_newline(&yaml)
}

#[cfg(test)]
mod tests;
