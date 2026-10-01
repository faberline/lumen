//! `lumen k8s`: dispatch to the render verbs, run the operator, resize storage,
//! and write or print what a verb rendered.

mod access;
mod fleet;
mod render;

use std::path::Path;

use anyhow::Result;

use crate::cli::k8s::{
    K8sAccessCmd, K8sArgs, K8sCmd, K8sCrdCmd, K8sFleetCmd, K8sInstanceCmd, K8sOperatorCmd,
    K8sOperatorResizeStorageArgs, K8sOperatorRunArgs,
};
use crate::k8s::access::render_access_yaml;
use crate::k8s::fleet::render_fleet_yaml;
use crate::k8s::render::{render_instance_yaml, render_operator_yaml};

#[cfg(feature = "operator")]
use anyhow::Context;
#[cfg(feature = "operator")]
use tracing_subscriber::EnvFilter;

/// `lumen k8s` — cluster artifacts split by lifecycle layer. `operator run`
/// and `operator resize-storage` need kube-rs at runtime; the render paths
/// are offline and work from the static manifests/CR templates embedded in
/// the binary.
pub(crate) async fn k8s(args: K8sArgs) -> Result<()> {
    match args.cmd {
        K8sCmd::Crd(args) => match args.cmd {
            K8sCrdCmd::Render(args) => write_or_print(
                args.out.as_deref(),
                "crd.yaml",
                &crd_yaml(),
                kubectl_apply_next,
            ),
        },
        K8sCmd::Operator(args) => match args.cmd.unwrap_or_default() {
            K8sOperatorCmd::Run(run_args) => run_operator(run_args).await,
            K8sOperatorCmd::Render(args) => {
                let yaml = render_operator_yaml(&args)?;
                write_or_print(
                    args.out.as_deref(),
                    "operator.yaml",
                    &yaml,
                    kubectl_apply_next,
                )
            }
            K8sOperatorCmd::ResizeStorage(args) => resize_storage(args).await,
        },
        K8sCmd::Instance(args) => match args.cmd {
            K8sInstanceCmd::Render(args) => {
                let yaml = render_instance_yaml(&args);
                write_or_print(args.out.as_deref(), "lumen.yaml", &yaml, kubectl_apply_next)
            }
        },
        K8sCmd::Fleet(args) => match args.cmd {
            K8sFleetCmd::Render(args) => {
                let yaml = render_fleet_yaml(&args);
                write_or_print(
                    args.out.as_deref(),
                    "lumenfleet.yaml",
                    &yaml,
                    kubectl_apply_next,
                )
            }
        },
        K8sCmd::Access(args) => match args.cmd {
            K8sAccessCmd::Render(args) => {
                let yaml = render_access_yaml(&args)?;
                write_or_print(
                    args.out.as_deref(),
                    "access.yaml",
                    &yaml,
                    kubectl_apply_next,
                )
            }
        },
    }
}

#[cfg(feature = "operator")]
async fn run_operator(args: K8sOperatorRunArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let _ = args;
    lumen::operator::run().await
}

#[cfg(not(feature = "operator"))]
async fn run_operator(_args: K8sOperatorRunArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without operator support; rebuild with \
         `--features operator` (the published image includes it)"
    )
}

/// `lumen k8s operator resize-storage` (#809): one-shot detect-and-patch for
/// the `raft` PVC's `volumeClaimTemplates` immutability gap — see
/// `lumen::operator::resize::resize_instance`.
#[cfg(feature = "operator")]
async fn resize_storage(args: K8sOperatorResizeStorageArgs) -> Result<()> {
    let client = kube::Client::try_default()
        .await
        .context("build a kube client from the in-cluster/kubeconfig context")?;
    let outcomes =
        lumen::operator::resize::resize_instance(client, &args.namespace, &args.name, args.dry_run)
            .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "outcomes": outcomes,
            "next": "done",
        }))?
    );
    Ok(())
}

#[cfg(not(feature = "operator"))]
async fn resize_storage(_args: K8sOperatorResizeStorageArgs) -> Result<()> {
    anyhow::bail!(
        "this lumen build was compiled without operator support; rebuild with \
         `--features operator` (the published image includes it)"
    )
}

#[cfg(feature = "operator")]
fn crd_yaml() -> String {
    lumen::operator::crd_yaml()
}

#[cfg(not(feature = "operator"))]
fn crd_yaml() -> String {
    cli_std::artifact::ensure_trailing_newline(include_str!("../../../k8s/operator/crd.yaml"))
}

/// Write `body` to `--out` (or stream it to stdout when `out` is `None`).
/// Chainable output (#963): the file-writing branch ends with exactly one
/// deterministic `next: <command>` line built from the resolved target path,
/// so an agent can copy-paste the follow-up; the stream-to-stdout branch
/// never emits one (nothing would separate it from the artifact bytes).
pub(crate) fn write_or_print(
    out: Option<&Path>,
    default_file: &str,
    body: &str,
    next: impl FnOnce(&Path) -> String,
) -> Result<()> {
    if let Some(target) = cli_std::artifact::write_or_print(out, default_file, body)? {
        println!("next: {}", next(&target));
    }
    Ok(())
}

/// `next:` builder shared by every k8s render verb: the rendered manifest's
/// only sensible follow-up is applying it.
fn kubectl_apply_next(target: &Path) -> String {
    format!("kubectl apply -f {}", target.display())
}
