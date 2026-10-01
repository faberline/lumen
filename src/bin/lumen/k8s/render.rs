//! The operator and instance manifests `lumen k8s operator render` and `lumen
//! k8s instance render` write.

use anyhow::Result;

use crate::cli::k8s::{K8sInstanceProfile, K8sInstanceRenderArgs, K8sOperatorRenderArgs};

pub(super) fn render_operator_yaml(args: &K8sOperatorRenderArgs) -> Result<String> {
    let namespace = &args.namespace;
    let image = &args.image;
    let monitoring = args.monitoring;
    if image.is_empty()
        || image.starts_with('-')
        || image
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        anyhow::bail!(
            "operator image must be a non-empty, whitespace-free OCI image reference that does not start with '-'"
        );
    }
    let mut out = String::new();
    out.push_str(&cli_std::artifact::replace_kubernetes_namespace(
        &cli_std::artifact::strip_source_ownership_markers(include_str!(
            "../../../../k8s/operator/rbac.yaml"
        )),
        "lumen-system",
        namespace,
    ));
    out.push_str("\n---\n");
    let deployment = cli_std::artifact::replace_kubernetes_namespace(
        &cli_std::artifact::strip_source_ownership_markers(include_str!(
            "../../../../k8s/operator/deployment.yaml"
        )),
        "lumen-system",
        namespace,
    );

    // Derived, not hardcoded (#2532): the checked-in manifest pins this
    // workspace's own version, so a release bump that misses `deployment.yaml`
    // fails this render instead of silently handing out a stale image.
    let checked_in_image = format!(
        "          image: ghcr.io/faberline/lumen:{}",
        env!("CARGO_PKG_VERSION")
    );
    if !deployment.contains(&checked_in_image) {
        anyhow::bail!(
            "checked-in operator manifest does not pin this build's image \
             (`{checked_in_image}`) — bump k8s/operator/deployment.yaml with the release"
        );
    }
    out.push_str(&deployment.replacen(&checked_in_image, &format!("          image: {image}"), 1));
    // The operator's own scrape target (#2621). Unconditional: it is plain
    // core/v1, so it applies on a cluster with no monitoring stack at all, and
    // shipping it always means turning monitoring on later never has to come
    // back and add the target. Kept in the same order as
    // k8s/operator/kustomization.yaml so the two paths stay comparable.
    out.push_str("\n---\n");
    out.push_str(&cli_std::artifact::replace_kubernetes_namespace(
        &cli_std::artifact::strip_source_ownership_markers(include_str!(
            "../../../../k8s/operator/service.yaml"
        )),
        "lumen-system",
        namespace,
    ));
    // The PDB ships with the Deployment: `replicas: 2` only survives a node
    // drain if evictions are serialized (#2602). Render consumers get the same
    // operator layer the kustomize consumers get.
    out.push_str("\n---\n");
    out.push_str(&cli_std::artifact::replace_kubernetes_namespace(
        &cli_std::artifact::strip_source_ownership_markers(include_str!(
            "../../../../k8s/operator/pdb.yaml"
        )),
        "lumen-system",
        namespace,
    ));
    // Opt-in tail, byte-identical to the `operator-monitoring` component so a
    // kustomize consumer and a render consumer get the same alerts. Gated
    // because these two are monitoring.coreos.com CRDs: emitting them
    // unconditionally would make `kubectl apply` of the whole render fail on
    // any cluster without prometheus-operator, taking the operator down with
    // the alerts.
    if monitoring {
        for manifest in [
            include_str!("../../../../k8s/components/operator-monitoring/servicemonitor.yaml"),
            include_str!("../../../../k8s/components/operator-monitoring/prometheusrule.yaml"),
        ] {
            out.push_str("\n---\n");
            out.push_str(&rewrite_monitoring_namespace(
                &cli_std::artifact::strip_source_ownership_markers(manifest),
                namespace,
            ));
        }
    }
    Ok(cli_std::artifact::ensure_trailing_newline(&out))
}

/// Rewrite the control-plane namespace in the two monitoring manifests.
///
/// `cli_std::artifact::replace_kubernetes_namespace` rewrites the `name:` and
/// `namespace:` keys, which is everything the RBAC/Deployment/Service/PDB
/// layer carries. The monitoring layer carries the namespace in three further
/// shapes, and every one of them fails *silently* if left behind on a
/// `--namespace` render:
///
/// - the ServiceMonitor's `namespaceSelector.matchNames` list item — a
///   selector pointed at an empty namespace discovers no target, so the
///   operator simply never appears in Prometheus;
/// - the PromQL `namespace="..."` matchers in both alert expressions — an
///   expression that matches nothing can never fire, which is exactly the
///   false green row 4 exists to prevent;
/// - the `-n <ns>` in the runbook annotations — commands an on-call would
///   paste against the wrong namespace mid-incident.
fn rewrite_monitoring_namespace(manifest: &str, namespace: &str) -> String {
    cli_std::artifact::replace_kubernetes_namespace(manifest, "lumen-system", namespace)
        .replace("- lumen-system", &format!("- {namespace}"))
        .replace(
            "namespace=\"lumen-system\"",
            &format!("namespace=\"{namespace}\""),
        )
        .replace("-n lumen-system", &format!("-n {namespace}"))
}

/// Standalone custom resource manifests for `--profile <dev|staging|prod|template>`.
///
/// Shared by `lumen k8s instance render` and by tests asserting the four profile shapes.
pub(super) fn render_instance_yaml(args: &K8sInstanceRenderArgs) -> String {
    let default_version = env!("CARGO_PKG_VERSION");
    let (default_name, default_namespace, default_image, body) = match args.profile {
        K8sInstanceProfile::Dev => (
            "search",
            "default",
            "lumen:latest".to_string(),
            InstanceBody::Dev,
        ),
        K8sInstanceProfile::Staging => (
            "lumen",
            "staging",
            // Published releases live at ghcr.io/faberline/lumen:<version>
            // (digest in each release's notes); this is the handed-out default.
            format!("ghcr.io/faberline/lumen:{default_version}"),
            InstanceBody::Staging,
        ),
        K8sInstanceProfile::Prod => (
            "lumen",
            "production",
            // Same published GHCR default as staging; override with `--image`
            // to pin @sha256 or point at a mirrored registry.
            format!("ghcr.io/faberline/lumen:{default_version}"),
            InstanceBody::Prod,
        ),
        K8sInstanceProfile::Template => (
            "REPLACE_ME__LUMEN_NAME",
            "REPLACE_ME__APP_NAMESPACE",
            "REPLACE_ME__REGISTRY/lumen:REPLACE_ME__IMAGE_TAG".to_string(),
            InstanceBody::Template,
        ),
    };
    let name = args.name.as_deref().unwrap_or(default_name);
    let namespace = args.namespace.as_deref().unwrap_or(default_namespace);
    let image = args.image.as_deref().unwrap_or(&default_image);

    let header_comment = "# TLS Secrets are provisioned by the deployment administrator or an external platform.\n# The operator consumes named serving/peer Secrets and performs no issuance.\n";

    let yaml = format!(
        "{header_comment}apiVersion: lumen.dev/v1alpha1\nkind: Lumen\nmetadata:\n  name: {name}\n  namespace: {namespace}\nspec:\n{}",
        profile_spec_body(body, image)
    );
    cli_std::artifact::ensure_trailing_newline(&yaml)
}

/// The `spec:` body for one profile, at two-space indent. Shared by
/// `k8s instance render` and `k8s fleet render` so a fleet's `defaults` and a
/// standalone CR cannot drift into disagreeing about what "prod" means.
pub(super) fn profile_spec_body(body: InstanceBody, image: &str) -> String {
    let mut yaml = format!("  image: {image}\n");
    match body {
        InstanceBody::Dev => {
            // Every profile states its auth posture out loud, even when it
            // agrees with the CRD default (#2678). `auth` fails closed, so a
            // rendered CR that stayed silent would be a `required` instance
            // with no token source: a pod that never passes readiness.
            yaml.push_str("  shardCount: 1\n  replicasPerShard: 1\n  voterCount: 1\n  logFormat: pretty\n  auth: disabled\n  placement:\n    nodeSelector:\n      kubernetes.io/os: linux\n  serving:\n    cpu: \"1\"\n    memory: 4Gi\n");
        }
        InstanceBody::Staging => {
            // #3113 R8: `servingTlsSecret` is stated for the same reason as
            // `peerTlsSecret` below, but the failure it prevents is the
            // opposite one. An unstated peer Secret fails closed — the pods
            // refuse to start and say so. An unstated serving Secret fails
            // *open*: the client port quietly stays h2c, and a fleet serves
            // KSA-bearing requests in cleartext while looking healthy.
            yaml.push_str("  shardCount: 3\n  replicasPerShard: 3\n  voterCount: 3\n  logFormat: json\n  auth: required\n  peerTlsSecret: lumen-peer-tls\n  servingTlsSecret: lumen-serving-tls\n  serving:\n    cpu: \"1\"\n    memory: 4Gi\n  observability: true\n");
        }
        InstanceBody::Prod => {
            // #2890 R7: `peerTlsSecret` is stated, not defaulted. A replicated
            // profile that stayed silent about it would render a CR whose pods
            // refuse to start — the same reasoning as `auth` above, one port
            // over.
            yaml.push_str("  imagePullPolicy: Always\n  shardCount: 6\n  replicasPerShard: 3\n  voterCount: 3\n  logFormat: json\n  logLevel: warn\n  auth: required\n  peerTlsSecret: lumen-peer-tls\n  servingTlsSecret: lumen-serving-tls\n  serving:\n    cpu: \"1\"\n    memory: 4Gi\n    graceSecs: 45\n  observability: true\n");
        }
        InstanceBody::Template => {
            yaml.push_str("  imagePullPolicy: IfNotPresent\n  shardCount: REPLACE_ME__SHARD_COUNT\n  replicasPerShard: REPLACE_ME__REPLICAS_PER_SHARD\n  voterCount: REPLACE_ME__VOTER_COUNT\n  logFormat: json\n  auth: required\n  peerTlsSecret: REPLACE_ME__PEER_TLS_SECRET\n  servingTlsSecret: REPLACE_ME__SERVING_TLS_SECRET\n  serving:\n    cpu: \"1\"\n    memory: 4Gi\n");
        }
    }
    yaml
}

#[derive(Clone, Copy)]
pub(super) enum InstanceBody {
    Dev,
    Staging,
    Prod,
    Template,
}
