//! The arguments of the commands that talk to a running node: backup, connect,
//! query, and the snapshot dump, load and inspect commands.

use std::path::PathBuf;

use clap::{Subcommand, ValueEnum};

/// `lumen backup` flags (#808): pulls a snapshot over HTTP from a running
/// serving fleet and ships it to a destination via `libs/service-backup`.
#[derive(clap::Args)]
pub(crate) struct BackupArgs {
    /// Base URL of a running lumen serving node, e.g.
    /// `http://<name>.<namespace>.svc.cluster.local:7373` (what the operator's
    /// backup CronJob passes) or `http://localhost:7373` for ad hoc use.
    #[arg(long)]
    pub(crate) url: String,
    /// Destination URI: `file:///path`, `s3://bucket/prefix`, or
    /// `gs://bucket/prefix`. GCS uses an explicit access token or the
    /// GCE/GKE metadata-server Workload Identity token.
    #[arg(long)]
    pub(crate) dest: String,
    /// Drop backup objects older than this many seconds after a successful
    /// put. Omit to keep everything.
    #[arg(long)]
    pub(crate) retention_secs: Option<u64>,
    /// File holding this runner's own audience-bound ServiceAccount token,
    /// read fresh for this run (#2877). In-cluster this is the projected
    /// volume the operator's backup CronJob mounts. A path, never the token:
    /// a credential passed as an argument is visible in the pod spec, in
    /// `ps`, and in every shell history that ever typed it. Omit against a
    /// fleet whose `spec.auth` is `disabled` — it rejects a presented bearer.
    #[arg(long, value_name = "PATH")]
    pub(crate) token_file: Option<std::path::PathBuf>,
}

/// `lumen connect` flags (#1321): manage a `kubectl port-forward` around a
/// wrapped command so an agent never tracks the port-forward process itself.
#[derive(clap::Args)]
pub(crate) struct ConnectArgs {
    /// kubectl context to port-forward through. Omit to use the current context.
    #[arg(long)]
    pub(crate) context: Option<String>,
    /// Namespace of the target Service (or `Lumen` CR when `--cr` is set).
    #[arg(long)]
    pub(crate) namespace: String,
    /// Target Service name. Defaults to the `--cr` name when `--cr` is set
    /// (the client Service shares the CR's own metadata name).
    #[arg(long)]
    pub(crate) service: Option<String>,
    /// `Lumen` CR name. When set (and `--service` is omitted) the Service
    /// name defaults to this CR's own name.
    #[arg(long)]
    pub(crate) cr: Option<String>,
    /// Local port to forward to. Omit to pick a free ephemeral port.
    #[arg(long)]
    pub(crate) local_port: Option<u16>,
    /// Remote (Service) port.
    #[arg(long, default_value_t = 7373)]
    pub(crate) remote_port: u16,
    /// ServiceAccount to authenticate as (#2878). Named, never inferred: this
    /// CLI will not pick one by listing the namespace or by falling back to
    /// `default`, because a token minted for an account nobody chose is
    /// exactly as authorized as one somebody did choose.
    ///
    /// With it, a short-lived audience-bound token is minted through your
    /// kubeconfig and held in this process, and the wrapped command talks to a
    /// loopback proxy that attaches it — so the token is in no environment
    /// variable, no argument list, and no file. Without it the port-forward
    /// carries no credential at all, which only works against a fleet whose
    /// `spec.auth` is `disabled`.
    ///
    /// You need `create` on this ServiceAccount's `token` subresource; `lumen
    /// k8s access render` emits that grant.
    #[arg(long, value_name = "NAME")]
    pub(crate) client_sa: Option<String>,
    /// PEM bundle of the private CA that signed the fleet's serving certificate
    /// (#3113 R6). The deployment administrator or external certificate
    /// platform distributes this public CA separately from the serving Secret.
    ///
    /// With it, the forwarded socket is spoken to over TLS addressed as
    /// `--server-name`, verified against this bundle and against no public root.
    /// The port-forward is transport; the identity being checked is the
    /// Kubernetes Service's, which is what the certificate actually names.
    ///
    /// Requires `--client-sa`: the verifying connection is made by the local
    /// proxy, and the proxy exists to hold a token. A TLS fleet that accepts no
    /// credential is not a deployment this command has to serve.
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with = "plaintext",
        requires = "client_sa"
    )]
    pub(crate) ca_file: Option<std::path::PathBuf>,
    /// The DNS name the serving certificate must present. Defaults to
    /// `<service>.<namespace>.svc`, which is what the operator requests.
    ///
    /// Override it only when the Service is reached under another of its
    /// certified names (its cluster FQDN, say). `localhost` and `127.0.0.1` are
    /// not among them, and a leaf that carried either would be a leaf usable
    /// against every port-forward anyone ever opens.
    #[arg(long, value_name = "DNS")]
    pub(crate) server_name: Option<String>,
    /// Talk to the forwarded port in cleartext — local and kind development,
    /// where no serving certificate has been issued (#3113 R1).
    ///
    /// Required to be said out loud, because the alternative is a default that
    /// downgrades silently: a production fleet reached without `--ca-file`
    /// would fail somewhere inside the wrapped command's first request instead
    /// of here, and the fix would look like a networking problem.
    #[arg(long)]
    pub(crate) plaintext: bool,
    /// The command to run with `LUMEN_URL` set to the local end of the
    /// port-forward — and nothing else. Everything after `--`, e.g. `lumen
    /// connect --namespace prod --cr search --ca-file ca.crt --client-sa agent
    /// -- lumen query collections list`.
    #[arg(last = true, required = true)]
    pub(crate) command: Vec<String>,
}

/// Where `lumen query *` sends its request, and whose token it carries.
///
/// #2873 removed the bearer flag, the environment variable behind it, and the
/// kubectl Secret lookup behind that — every mechanism whose job was to *find*
/// a credential lying around. #2878 restores a `--context`/`--namespace` pair
/// that looks superficially similar and is not the same thing: nothing here
/// reads a stored credential. `--client-sa` names a ServiceAccount, and the
/// token is minted for it, in memory, for this one command, through the
/// identity already in your kubeconfig.
///
/// There is deliberately no environment variable for `--client-sa`. Which
/// account you act as is a decision each invocation makes out loud.
#[derive(clap::Args, Clone)]
pub(crate) struct QueryTarget {
    /// Base URL of a reachable lumen serving node, e.g. `http://localhost:7373`
    /// — what `lumen connect` sets for the wrapped command.
    #[arg(long, env = "LUMEN_URL")]
    pub(crate) url: Option<String>,
    /// kubeconfig context to mint the token through. Omit for the current one.
    #[arg(long)]
    pub(crate) context: Option<String>,
    /// Namespace of the ServiceAccount named by `--client-sa`.
    #[arg(long)]
    pub(crate) namespace: Option<String>,
    /// ServiceAccount to authenticate as (#2878). Named, never inferred.
    /// Omit to send no credential at all — correct only against a fleet whose
    /// `spec.auth` is `disabled`, and what `lumen connect --client-sa` already
    /// arranges for its wrapped command.
    #[arg(long, value_name = "NAME", requires = "namespace")]
    pub(crate) client_sa: Option<String>,
}

/// `lumen query <index|search|duplicates|collections>` flags (#1321): thin
/// one-shot wrappers assembling the exact `lumen spec --shapes` wire body.
#[derive(clap::Args)]
pub(crate) struct QueryArgs {
    #[command(subcommand)]
    pub(crate) command: QueryCommand,
}

#[derive(Subcommand)]
pub(crate) enum QueryCommand {
    /// `POST /collections/{id}/index` — index one or more field values. Wire
    /// body is FLAT: `{"items":[{"external_id","field","value"}]}` — NOT a
    /// nested `{id, fields:{...}}` shape (see `lumen spec --shapes` → "index").
    Index(QueryIndexArgs),
    /// `POST /collections/{id}/search` — term/match/raw-JSON one-shot search.
    Search(QuerySearchArgs),
    /// `POST /collections/{id}/duplicates` — find external_ids sharing a value.
    Duplicates(QueryDuplicatesArgs),
    /// Collection-level read helpers.
    Collections(QueryCollectionsArgs),
}

#[derive(clap::Args)]
pub(crate) struct QueryIndexArgs {
    #[command(flatten)]
    pub(crate) target: QueryTarget,
    /// Target collection id.
    #[arg(long)]
    pub(crate) collection: String,
    /// One item as `EXTERNAL_ID:FIELD=VALUE` (repeatable). `VALUE` is parsed
    /// as JSON when possible (numbers, `[..]` vectors/string-lists), else
    /// kept as a plain string — so `p1:price=79` and
    /// `p1:embedding=[0.1,0.2,0.9]` both work unquoted.
    #[arg(long = "item", value_name = "EXTERNAL_ID:FIELD=VALUE", required = true)]
    pub(crate) items: Vec<String>,
}

#[derive(clap::Args)]
pub(crate) struct QuerySearchArgs {
    #[command(flatten)]
    pub(crate) target: QueryTarget,
    /// Target collection id.
    #[arg(long)]
    pub(crate) collection: String,
    /// Exact term match: `FIELD=VALUE`. Exactly one of `--term`/`--match`/
    /// `--query-json` is required.
    #[arg(long, value_name = "FIELD=VALUE")]
    pub(crate) term: Option<String>,
    /// BM25 text match: `FIELD=TEXT`. Exactly one of `--term`/`--match`/
    /// `--query-json` is required.
    #[arg(long = "match", value_name = "FIELD=TEXT")]
    pub(crate) match_: Option<String>,
    /// Raw `QueryNode` JSON — escape hatch for shapes `--term`/`--match`
    /// don't cover (`lumen spec --shapes` has the full cookbook). Exactly one
    /// of `--term`/`--match`/`--query-json` is required.
    #[arg(long)]
    pub(crate) query_json: Option<String>,
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: u32,
}

#[derive(clap::Args)]
pub(crate) struct QueryDuplicatesArgs {
    #[command(flatten)]
    pub(crate) target: QueryTarget,
    /// Target collection id.
    #[arg(long)]
    pub(crate) collection: String,
    /// Field to find shared values on.
    #[arg(long)]
    pub(crate) field: String,
    #[arg(long, default_value_t = 2)]
    pub(crate) min_group_size: u32,
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    #[arg(long, default_value_t = 0)]
    pub(crate) offset: u32,
}

#[derive(clap::Args)]
pub(crate) struct QueryCollectionsArgs {
    #[command(subcommand)]
    pub(crate) command: QueryCollectionsCommand,
}

#[derive(Subcommand)]
pub(crate) enum QueryCollectionsCommand {
    /// `GET /collections` — list collection ids the serving node exposes.
    List(QueryCollectionsListArgs),
}

#[derive(clap::Args)]
pub(crate) struct QueryCollectionsListArgs {
    #[command(flatten)]
    pub(crate) target: QueryTarget,
}

/// `lumen dump|export` flags (#1095): pulls SnapshotV1 JSON from a running
/// serving fleet and writes the exact bytes to stdout or a local file.
#[derive(clap::Args)]
pub(crate) struct SnapshotExportArgs {
    /// Base URL of a running lumen serving node, e.g. `http://localhost:7373`.
    #[arg(long)]
    pub(crate) url: String,
    /// Write the SnapshotV1 JSON bytes to this path instead of stdout.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

/// `lumen load|import` flags (#1095): reads SnapshotV1 JSON and posts it to
/// `/admin/restore`, replacing the target engine state.
#[derive(clap::Args)]
pub(crate) struct SnapshotImportArgs {
    /// Base URL of a running lumen serving node, e.g. `http://localhost:7373`.
    #[arg(long)]
    pub(crate) url: String,
    /// Read SnapshotV1 JSON bytes from this path. Omit to read stdin.
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
}

/// `lumen inspect` flags: the offline half of the re-index audit
/// (`Engine::reindex_needed`, which the serving node runs on every reopen and
/// logs). Parses a document and prints which of its fields will not come back,
/// which of them it could not judge, and how many documents it read to say so.
/// Nothing is restored, nothing is written, and no node is contacted.
#[derive(clap::Args)]
pub(crate) struct InspectArgs {
    /// Read SnapshotV1 JSON bytes from this path. Omit to read stdin.
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// `text` (default) for an operator-readable report, `json` for the same
    /// facts as a machine-readable document on stdout.
    #[arg(long, value_enum, default_value_t = InspectFormat::Text)]
    pub(crate) format: InspectFormat,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum InspectFormat {
    /// Operator-readable lines plus a trailing `next:` command.
    Text,
    /// A single JSON document: `reindex_needed`, `fields_not_audited`,
    /// `documents_scanned`.
    Json,
}
