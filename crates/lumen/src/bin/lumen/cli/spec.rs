//! The offline self-description's arguments: `lumen llm`'s topics and `lumen
//! spec`'s formats and client generator.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum LlmTopic {
    /// Typed task map for agent context selection (default).
    Outline,
    /// Start one local development or test runtime without Kubernetes.
    RunStandalone,
    /// Inspect the offline search contract before issuing a request.
    LocalSearch,
    /// Declare or review a collection schema.
    ModelSchema,
    /// Select a supported search, filter, range, sort, kNN, or duplicate query.
    SelectQuery,
    /// Separate current query support from the documented Search v2 target.
    Querying,
    /// Connect a source database, CDC stream, or outbox to Lumen.
    IntegrateSourceDb,
    /// Inspect the request-authentication contract: the Kubernetes
    /// ServiceAccount identity Lumen accepts, and the credential kinds it
    /// refuses.
    Authenticate,
    /// Use a bounded Kubernetes port-forward connection.
    ConnectKubernetes,
    /// Render image, CRD, operator, or instance deployment artifacts.
    DeployKubernetes,
    /// Give an external Kubernetes user access to a Lumen instance through a
    /// client ServiceAccount.
    GrantAccess,
    /// Create or restore an administrative backup.
    BackupRestore,
    /// Generate a typed Rust, Python, or TypeScript client.
    GenerateClient,
    /// Inspect standard operational evidence from a running service.
    Diagnose,
    /// Verify one release candidate through local, kind, and public artifact
    /// evidence.
    VerifyRelease,
}

impl LlmTopic {
    pub(crate) const fn id(self) -> &'static str {
        match self {
            Self::Outline => "outline",
            Self::RunStandalone => "run-standalone",
            Self::LocalSearch => "local-search",
            Self::ModelSchema => "model-schema",
            Self::SelectQuery => "select-query",
            Self::Querying => "querying",
            Self::IntegrateSourceDb => "integrate-source-db",
            Self::Authenticate => "authenticate",
            Self::ConnectKubernetes => "connect-kubernetes",
            Self::DeployKubernetes => "deploy-kubernetes",
            Self::GrantAccess => "grant-access",
            Self::BackupRestore => "backup-restore",
            Self::GenerateClient => "generate-client",
            Self::Diagnose => "diagnose",
            Self::VerifyRelease => "verify-release",
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum LlmFormat {
    /// Human/agent-readable Markdown (default).
    Md,
    /// Machine-readable JSON.
    Json,
}

#[derive(Parser)]
pub(crate) struct LlmArgs {
    /// Which agent-facing topic to print.
    #[arg(long, value_enum, default_value_t = LlmTopic::Outline)]
    pub(crate) topic: LlmTopic,
    /// Output format.
    #[arg(long, value_enum, default_value_t = LlmFormat::Md)]
    pub(crate) format: LlmFormat,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum SpecFormat {
    /// Full OpenAPI 3 document as JSON (default).
    Openapi,
    /// Full OpenAPI 3 document as YAML for LLM/agent reading.
    #[value(alias = "yaml", alias = "openapi.yaml")]
    OpenapiYaml,
    /// Just the component schemas (request/response data types).
    JsonSchema,
}

#[derive(Parser)]
pub(crate) struct SpecArgs {
    /// Generate a typed client from this spec instead of printing it.
    #[command(subcommand)]
    pub(crate) gen: Option<SpecSub>,
    /// Schema format to emit when neither `--shapes` nor `--fields` is set.
    #[arg(long, value_enum, default_value_t = SpecFormat::Openapi)]
    pub(crate) format: SpecFormat,
    /// Emit the query-shape cookbook (canonical request examples) instead.
    #[arg(long)]
    pub(crate) shapes: bool,
    /// Emit the field-type / analyzer catalog instead.
    #[arg(long)]
    pub(crate) fields: bool,
}

/// `lumen spec` subcommands.
#[derive(Subcommand)]
pub(crate) enum SpecSub {
    /// Generate a typed API client (TypeScript / Python / Rust) from lumen's
    /// OpenAPI document, written into `--out`.
    Gen(GenArgs),
}

#[derive(Parser)]
pub(crate) struct GenArgs {
    /// Target language for the generated client.
    #[arg(long, value_enum)]
    pub(crate) lang: GenLang,
    /// Pinned generated-client contract, e.g. `python-3.14`. Defaults to
    /// `clients/codegen.toml`; an explicit value overrides that policy once.
    #[arg(long, value_name = "TARGET")]
    pub(crate) target: Option<String>,
    /// Output directory for the generated files.
    #[arg(long)]
    pub(crate) out: PathBuf,
    /// HTTP backend for the TypeScript client (ignored for py/rust).
    #[arg(long, value_enum, default_value_t = GenHttp::Fetch)]
    pub(crate) http: GenHttp,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum GenLang {
    /// TypeScript: types + fetch/axios client + TanStack Query hooks.
    Ts,
    /// Python: pydantic models + generated sync/async HTTP/2 runtime.
    Py,
    /// Rust: serde models + reqwest client.
    Rust,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum GenHttp {
    Fetch,
    Axios,
}
