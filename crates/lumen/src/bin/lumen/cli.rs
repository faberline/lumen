//! The command line: `Cli`, its `Command` tree, and the argument structs of the
//! commands that are not grouped under a child here (dockerfile, upgrade,
//! issue).

pub(crate) mod client;
pub(crate) mod k8s;
pub(crate) mod serve;
pub(crate) mod spec;
pub(crate) mod standalone;

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::cli::client::{
    BackupArgs, ConnectArgs, InspectArgs, QueryArgs, SnapshotExportArgs, SnapshotImportArgs,
};
use crate::cli::k8s::K8sArgs;
use crate::cli::serve::ServeArgs;
use crate::cli::spec::{LlmArgs, SpecArgs};
use crate::cli::standalone::StandaloneArgs;

#[derive(Parser)]
#[command(
    name = "lumen",
    version,
    about = "lumen — search specialist (serving node + CLI)"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) cmd: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Manage the local single-node Compose deployment.
    Standalone(StandaloneArgs),
    /// Run a serving node (HTTP API + background apply loop).
    Serve(ServeArgs),
    /// Print lumen's machine-readable integration spec — offline, no server.
    /// Default: the OpenAPI 3 JSON document; `--format openapi-yaml` for
    /// LLM-readable OpenAPI YAML; `--format json-schema` for the data types;
    /// `--shapes` for the query-shape cookbook; `--fields` for the field-type /
    /// analyzer catalog.
    Spec(SpecArgs),
    /// Print agent-facing task topics — offline, no server. `outline` maps the
    /// available tasks. Topics that include planned behavior distinguish it
    /// from current support. Tasks name canonical sources and runnable
    /// verification steps. Shared provider content stays owned by its library
    /// and is composed into Lumen.
    /// Markdown is the default; use `--format json` for machine-readable output.
    Llm(LlmArgs),
    /// Print runtime image Dockerfiles. Image construction is owned here, not
    /// by `k8s`, because the same artifact feeds compose, kind, and real
    /// registries.
    Dockerfile(DockerfileArgs),
    /// Kubernetes artifacts split by layer: cluster-scoped CRD, operator
    /// control plane, and app-namespace Lumen instances.
    K8s(K8sArgs),
    /// Dump a running node's full SnapshotV1 JSON to stdout or `--out`.
    /// Alias of `export`; this is ad hoc data movement, not scheduled backup
    /// sink transport.
    Dump(SnapshotExportArgs),
    /// Export a running node's full SnapshotV1 JSON to stdout or `--out`.
    /// Use `backup` when you need destination sinks and retention.
    Export(SnapshotExportArgs),
    /// Load a SnapshotV1 JSON document from `--file` or stdin into a running
    /// node by replacing all engine state through `/admin/restore`.
    /// Alias of `import`.
    Load(SnapshotImportArgs),
    /// Import a SnapshotV1 JSON document from `--file` or stdin into a running
    /// node by replacing all engine state through `/admin/restore`.
    Import(SnapshotImportArgs),
    /// Audit a SnapshotV1 JSON document offline — no server, nothing written —
    /// and report which collection/field pairs would come back empty despite
    /// the document's own census saying documents carry them. Those fields
    /// cannot be repaired by restoring: their contents are not in the document,
    /// and re-indexing is the only fix. Run this on a backup BEFORE importing
    /// it, when the backup may have been taken by a build that sealed `set`
    /// fields empty.
    Inspect(InspectArgs),
    /// Self-update this binary from a published GitHub release. Resolves the
    /// running target + version, downloads the matching `lumen-<target>.tar.gz`,
    /// verifies its sha256, and atomically replaces the running executable.
    /// `--check` reports the available version without changing anything.
    Upgrade(UpgradeArgs),
    /// Search, view, and file Lumen issues on the axiom tracker.
    /// `search` and `view` read existing `app:lumen` issues; `create`
    /// files a diagnostics-rich issue tagged `app:lumen`.
    Issue(IssueArgs),
    /// Fetch a snapshot from a running serving fleet's own `/admin/backup`
    /// and ship it to a destination (`file://`, `s3://`, or `gs://`) via
    /// `libs/service-backup`. GCS uses an explicit access token or GKE Workload
    /// Identity. No new snapshot mechanism — this only
    /// schedules and transports the existing admin API. Typically invoked by
    /// the operator's optional backup CronJob (`spec.serving.backup`, see
    /// `lumen llm --topic storage`), but works standalone. Requires the `backup`
    /// feature (pulled in transitively by `operator`).
    Backup(BackupArgs),
    /// Manage a `kubectl port-forward` for the duration of a wrapped command
    /// against a k8s-deployed Lumen instance — no manually tracked
    /// port-forward process (`lumen llm --topic recipes` has a worked
    /// example). Reachability only: the child is handed a URL and nothing
    /// else. Obtaining a Kubernetes ServiceAccount token for it is #2878's
    /// job, and until then there is no credential to obtain.
    Connect(ConnectArgs),
    /// One-shot query wrappers against a reachable lumen node: `index`,
    /// `search`, `duplicates`, `collections list`. Assembles the exact wire
    /// body `lumen spec --shapes` publishes — no interactive REPL. Requires
    /// the `backup` feature (pulled in transitively by `operator`).
    Query(QueryArgs),
}

#[derive(clap::Args)]
pub(crate) struct DockerfileArgs {
    #[command(subcommand)]
    pub(crate) cmd: DockerfileCmd,
}

#[derive(Subcommand)]
pub(crate) enum DockerfileCmd {
    /// Render a Dockerfile to stdout or `--out`.
    Render(DockerfileRenderArgs),
}

#[derive(clap::Args)]
pub(crate) struct DockerfileRenderArgs {
    /// Which runtime image contract to render.
    #[arg(long, value_enum, default_value_t = DockerfileVariant::Release)]
    pub(crate) variant: DockerfileVariant,
    /// Release tag used by `--variant release`; accepts `0.4.5` or `lumen@0.4.5`.
    #[arg(long)]
    pub(crate) version: Option<String>,
    /// Write to this path instead of stdout. A directory receives
    /// `Dockerfile` or `Dockerfile.release`.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum DockerfileVariant {
    /// Build from the workspace source tree.
    Source,
    /// Fetch and verify a published `lumen@<version>` release binary.
    Release,
}

/// `lumen upgrade` flags.
#[derive(clap::Args)]
pub(crate) struct UpgradeArgs {
    /// Report the current and latest version without modifying the binary.
    #[arg(long)]
    pub(crate) check: bool,
    /// Install this exact version (`0.4.3` or `lumen@0.4.3`) instead of the latest.
    #[arg(long = "version")]
    pub(crate) tag: Option<String>,
    /// Reinstall even when already on the selected version.
    #[arg(long)]
    pub(crate) force: bool,
    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub(crate) yes: bool,
}

/// `lumen issue <search|view|create|comment>` flags.
#[derive(clap::Args)]
pub(crate) struct IssueArgs {
    #[command(subcommand)]
    pub(crate) command: IssueCommand,
}

#[derive(Subcommand)]
pub(crate) enum IssueCommand {
    /// Search Lumen issues (app:lumen); omit the query to list recent.
    Search(IssueSearchArgs),
    /// Print one issue by number.
    View(IssueViewArgs),
    /// File a diagnostics-rich Lumen issue.
    Create(IssueCreateArgs),
    /// Comment on an issue and ensure it is open.
    Comment(IssueCommentArgs),
}

#[derive(clap::Args)]
pub(crate) struct IssueSearchArgs {
    /// Search text. Omit to list recent issues.
    #[arg(value_name = "QUERY", num_args = 0..)]
    pub(crate) query: Vec<String>,
    /// Issue state: open, closed, or all.
    #[arg(long, default_value = "open", value_parser = ["open", "closed", "all"])]
    pub(crate) state: String,
    /// Max results.
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: u32,
}

#[derive(clap::Args)]
pub(crate) struct IssueViewArgs {
    /// Issue number.
    pub(crate) number: u64,
}

#[derive(clap::Args)]
pub(crate) struct IssueCreateArgs {
    /// Issue title.
    #[arg(short = 't', long)]
    pub(crate) title: Option<String>,
    /// Free-text description of the problem (trailing words; placed above the
    /// diagnostics block). The only positional — parameters are flags.
    #[arg(value_name = "MSG", num_args = 0..)]
    pub(crate) message: Vec<String>,
    /// Include a running node's `/version`+`/healthz` (e.g. http://localhost:7373).
    #[arg(long)]
    pub(crate) url: Option<String>,
    /// Target repository (`owner/name`); defaults to lumen's release repo.
    #[arg(long)]
    pub(crate) repo: Option<String>,
    /// Add a label (repeatable).
    #[arg(long)]
    pub(crate) label: Vec<String>,
    /// Assemble and print the report without submitting anything.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub(crate) yes: bool,
}

#[derive(clap::Args)]
pub(crate) struct IssueCommentArgs {
    /// Issue number.
    pub(crate) number: u64,
    /// Follow-up note to add after reopening. Omit for cli-std's standard
    /// verification-failed message.
    #[arg(value_name = "MSG", num_args = 0..)]
    pub(crate) message: Vec<String>,
    /// Target repository (`owner/name`); defaults to lumen's release repo.
    #[arg(long)]
    pub(crate) repo: Option<String>,
    /// Print the reopen/comment request without changing GitHub state.
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// Skip the confirmation prompt.
    #[arg(short = 'y', long)]
    pub(crate) yes: bool,
}
