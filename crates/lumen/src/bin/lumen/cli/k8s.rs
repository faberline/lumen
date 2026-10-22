//! `lumen k8s`'s arguments: the CRD, the operator, instances, the fleet and the
//! client access handoff.

use std::path::PathBuf;

use clap::{Subcommand, ValueEnum};

#[derive(clap::Args)]
pub(crate) struct K8sArgs {
    #[command(subcommand)]
    pub(crate) cmd: K8sCmd,
}

#[derive(Subcommand)]
pub(crate) enum K8sCmd {
    /// Cluster-scoped API layer: render the Lumen CRD.
    Crd(K8sCrdArgs),
    /// Operator control-plane layer: render/install assets or run the controller.
    Operator(K8sOperatorArgs),
    /// App namespace data-plane declaration: render a Lumen custom resource.
    Instance(K8sInstanceArgs),
    /// Control-plane fleet declaration: render the one cluster-scoped
    /// `LumenFleet` that names every data-plane namespace and its settings.
    /// Use this instead of `instance` when the platform team owns
    /// configuration centrally and app teams only own their own overrides.
    Fleet(K8sFleetArgs),
    /// Client access layer: render the RBAC that lets a named Kubernetes user
    /// mint one client ServiceAccount's token, and that tells Lumen what that
    /// ServiceAccount may do.
    Access(K8sAccessArgs),
}

#[derive(clap::Args)]
pub(crate) struct K8sFleetArgs {
    #[command(subcommand)]
    pub(crate) cmd: K8sFleetCmd,
}

#[derive(Subcommand)]
pub(crate) enum K8sFleetCmd {
    /// Render a cluster-scoped `kind: LumenFleet` declaration.
    Render(K8sFleetRenderArgs),
}

#[derive(clap::Args)]
pub(crate) struct K8sFleetRenderArgs {
    /// Built-in fleet profile.
    #[arg(long, value_enum, default_value_t = K8sFleetProfile::Template)]
    pub(crate) profile: K8sFleetProfile,
    /// LumenFleet name. Also the default name of every `Lumen` it
    /// materializes, so `kubectl get lumen -A` reads as one fleet spread
    /// across namespaces.
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// Serving image for `spec.defaults`. Profile-specific default.
    #[arg(long)]
    pub(crate) image: Option<String>,
    /// Write to this path instead of stdout. A directory receives
    /// `lumenfleet.yaml`.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum K8sFleetProfile {
    /// One local/kind data plane in `default`, auth disabled.
    Dev,
    /// Two tenant namespaces on a dedicated node pool, auth required.
    Prod,
    /// Fill-in-the-blanks skeleton naming every knob a deployer owns:
    /// node pool, StorageClass, ServiceAccount, per-tenant CPU/memory/disk,
    /// and per-tenant credential source.
    Template,
}

#[derive(clap::Args)]
pub(crate) struct K8sCrdArgs {
    #[command(subcommand)]
    pub(crate) cmd: K8sCrdCmd,
}

#[derive(Subcommand)]
pub(crate) enum K8sCrdCmd {
    /// Render the Lumen CustomResourceDefinition YAML.
    Render(K8sFileOutputArgs),
}

#[derive(clap::Args, Clone, Debug, Default)]
pub(crate) struct K8sOperatorRunArgs {}

#[derive(clap::Args)]
pub(crate) struct K8sOperatorArgs {
    #[command(subcommand)]
    pub(crate) cmd: Option<K8sOperatorCmd>,
}

impl Default for K8sOperatorCmd {
    fn default() -> Self {
        Self::Run(K8sOperatorRunArgs::default())
    }
}

#[derive(Subcommand)]
pub(crate) enum K8sOperatorCmd {
    /// Container entrypoint: run the reconcile controller.
    Run(K8sOperatorRunArgs),
    /// Render operator namespace/RBAC/deployment YAML.
    Render(K8sOperatorRenderArgs),
    /// One-shot: grow a running instance's `raft-<name>-<n>` PVCs to match
    /// its CR's `spec.serving.raftStorage` (#809). StatefulSet
    /// `volumeClaimTemplates` are immutable, so a CR edit alone never
    /// resizes existing PVCs; this patches them directly when the bound
    /// `StorageClass` allows expansion. Never shrinks (unsupported by
    /// Kubernetes) and never mutates the CR itself.
    ResizeStorage(K8sOperatorResizeStorageArgs),
}

#[derive(clap::Args)]
pub(crate) struct K8sOperatorRenderArgs {
    /// Namespace that owns the operator control plane.
    #[arg(long, default_value = "lumen-system")]
    pub(crate) namespace: String,
    /// Operator container image. Supply an immutable registry digest for
    /// reproducible cluster deployment; the default is this build's
    /// published GHCR release, matching the checked-in operator manifest.
    #[arg(long, default_value_t = format!("ghcr.io/faberline/lumen:{}", env!("CARGO_PKG_VERSION")))]
    pub(crate) image: String,
    /// Also emit the operator's ServiceMonitor and PrometheusRule (#2621).
    /// Off by default because both are `monitoring.coreos.com/v1` CRDs and a
    /// cluster without prometheus-operator rejects the whole apply; the
    /// scrape *target* Service carries no CRD dependency and is always
    /// rendered. Mirrors the opt-in `k8s/components/operator-monitoring`
    /// kustomize component.
    #[arg(long)]
    pub(crate) monitoring: bool,
    /// Write to this path instead of stdout. A directory receives
    /// `operator.yaml`.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct K8sOperatorResizeStorageArgs {
    /// Namespace of the `Lumen` instance to resize.
    #[arg(long)]
    pub(crate) namespace: String,
    /// `Lumen` CR name.
    #[arg(long)]
    pub(crate) name: String,
    /// Report what would be patched without mutating any PVC.
    #[arg(long)]
    pub(crate) dry_run: bool,
}

#[derive(clap::Args)]
pub(crate) struct K8sInstanceArgs {
    #[command(subcommand)]
    pub(crate) cmd: K8sInstanceCmd,
}

#[derive(Subcommand)]
pub(crate) enum K8sInstanceCmd {
    /// Render a namespaced `kind: Lumen` custom resource.
    Render(K8sInstanceRenderArgs),
}

#[derive(clap::Args)]
pub(crate) struct K8sInstanceRenderArgs {
    /// Built-in instance profile.
    #[arg(long, value_enum, default_value_t = K8sInstanceProfile::Dev)]
    pub(crate) profile: K8sInstanceProfile,
    /// Lumen CR name.
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// Namespace where the app-facing Lumen instance lives.
    #[arg(long)]
    pub(crate) namespace: Option<String>,
    /// Serving image. Defaults are profile-specific.
    #[arg(long)]
    pub(crate) image: Option<String>,
    /// Write to this path instead of stdout. A directory receives `lumen.yaml`.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum K8sInstanceProfile {
    /// Small local/kind CR: one serving pod, embedded WAL, auth disabled.
    Dev,
    /// Pre-prod CR: json logs, raft data-plane shape, observability enabled.
    Staging,
    /// Production-shape CR: auth required, json logs, raft data-plane shape.
    Prod,
    /// Fill-in-the-blanks CR skeleton for app teams.
    Template,
}

#[derive(clap::Args)]
pub(crate) struct K8sAccessArgs {
    #[command(subcommand)]
    pub(crate) cmd: K8sAccessCmd,
}

#[derive(Subcommand)]
pub(crate) enum K8sAccessCmd {
    /// Render the client access bundle: a ServiceAccount, the RBAC that lets
    /// named users mint its token, and the RBAC Lumen reads to authorize it.
    Render(K8sAccessRenderArgs),
}

/// `lumen k8s access render` flags (#2889).
///
/// Everything here is a name. There is no flag that takes a credential,
/// because the bundle this renders contains none: the caller's identity is
/// minted by the API server on demand, and the only durable objects are the
/// two grants that say who may ask for it and what it may then do.
#[derive(clap::Args)]
pub(crate) struct K8sAccessRenderArgs {
    /// Namespace holding the Lumen instance. The whole bundle lands here:
    /// both grants are namespaced, so an access decision never leaks past the
    /// tenant that made it.
    #[arg(long)]
    pub(crate) namespace: String,
    /// The one ServiceAccount every request to Lumen is made as. Lumen sees
    /// this name — `system:serviceaccount:<namespace>:<name>` — and nothing
    /// about whoever minted the token.
    #[arg(long = "client-sa")]
    pub(crate) client_sa: String,
    /// A Kubernetes user allowed to mint that ServiceAccount's token, spelled
    /// exactly as `kubectl auth whoami` prints it for that principal.
    /// Repeatable. A Google account and a Google service account are both just
    /// strings here: they authenticate to the API server, never to Lumen.
    #[arg(long = "issuer", required = true)]
    pub(crate) issuers: Vec<String>,
    /// `<collection-id>=read|write|admin`. Repeatable, one collection each.
    /// A level grants every verb at or below it, so `write` can read what it
    /// writes.
    #[arg(long = "grant")]
    pub(crate) grants: Vec<String>,
    /// Also grant the instance-wide administrative surface — backup, restore,
    /// reshard, checkpoint. Separate from any collection grant on purpose.
    #[arg(long)]
    pub(crate) instance_admin: bool,
    /// Write to this path instead of stdout. A directory receives
    /// `access.yaml`.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct K8sFileOutputArgs {
    /// Write to this path instead of stdout.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}
