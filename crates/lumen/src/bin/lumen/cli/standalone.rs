//! `lumen standalone`'s arguments: backup, restore, the GKE profile and the
//! Compose deployment.

use std::path::PathBuf;

use clap::Subcommand;

#[derive(clap::Args)]
pub(crate) struct StandaloneArgs {
    #[command(subcommand)]
    pub(crate) cmd: StandaloneCmd,
}

#[derive(Subcommand)]
pub(crate) enum StandaloneCmd {
    Compose(StandaloneComposeArgs),
    Gke(StandaloneGkeArgs),
    Backup(StandaloneBackupArgs),
    Restore(StandaloneRestoreArgs),
}

#[derive(clap::Args)]
pub(crate) struct StandaloneBackupArgs {
    #[arg(long, conflicts_with = "gke", required_unless_present = "gke")]
    pub(crate) compose: Option<PathBuf>,
    #[arg(long, conflicts_with = "compose", required_unless_present = "compose")]
    pub(crate) gke: Option<PathBuf>,
    #[arg(long)]
    pub(crate) out: PathBuf,
    #[arg(long, requires = "compose", conflicts_with = "gke")]
    pub(crate) name: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct StandaloneRestoreArgs {
    #[arg(long, conflicts_with = "gke", required_unless_present = "gke")]
    pub(crate) compose: Option<PathBuf>,
    #[arg(long, conflicts_with = "compose", required_unless_present = "compose")]
    pub(crate) gke: Option<PathBuf>,
    #[arg(long)]
    pub(crate) file: PathBuf,
    #[arg(long, required = true)]
    pub(crate) replace: bool,
    #[arg(long, requires = "compose", conflicts_with = "gke")]
    pub(crate) name: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct StandaloneGkeArgs {
    #[command(subcommand)]
    pub(crate) cmd: StandaloneGkeCmd,
}

#[derive(Subcommand)]
pub(crate) enum StandaloneGkeCmd {
    Init(StandaloneGkeInitArgs),
    Render(StandaloneGkeRenderArgs),
}

#[derive(clap::Args)]
pub(crate) struct StandaloneGkeInitArgs {
    #[arg(long)]
    pub(crate) out: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct StandaloneGkeRenderArgs {
    #[arg(long)]
    pub(crate) file: PathBuf,
    #[arg(long)]
    pub(crate) out: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct StandaloneComposeArgs {
    #[command(subcommand)]
    pub(crate) cmd: StandaloneComposeCmd,
}

#[derive(Subcommand)]
pub(crate) enum StandaloneComposeCmd {
    Patch(StandaloneComposePatchArgs),
}

#[derive(clap::Args)]
pub(crate) struct StandaloneComposePatchArgs {
    #[arg(long)]
    pub(crate) file: PathBuf,
    #[arg(long, default_value = "lumen")]
    pub(crate) name: String,
}
