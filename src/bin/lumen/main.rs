//! `lumen` — the single agent-first CLI: `serve` (serving node), `spec` /
//! `llm` (offline integration contract + agent topics), and `k8s` (operator
//! + CRD generation). Agents start here: `lumen llm --topic outline`.
//!
//! A serving node is symmetric: it answers reads from its local
//! materialized index and accepts writes by publishing them to the
//! configured write log. In single-node mode that log is local; in
//! primary-replica mode Lumen owns ordering and replication via raft_core.
//! Apply happens in the background subscribe loop —
//! see `coordinator` / `wal`.
//!
//! ```text
//! lumen serve                          # single node, in-process log, :7373
//! lumen serve --wal raft               # k8s StatefulSet / HA mode
//! lumen serve --host 0.0.0.0 --port 7373 --log-format json
//! ```
//!
//! ## Contracts inherited from the retired EC shells
//!
//! This sentence was the whole of the `// Contract:` comment in an AW-EC shell under
//! `e2e/`, which ran `cargo test -p lumen --bin lumen` in a subprocess and
//! asserted the child's exit status. `cargo test -p lumen` already runs this binary's
//! colocated unit tests directly, so the shell added a second, nested run and nothing
//! else. It was deleted on 2026-08-20 with the EC machinery it belonged to, and the
//! sentence is the only thing it held that nothing else did. The line below is prefixed
//! with the EC id the shell was filed under.
//!
//! - `lumen-claim-topology-empty-pvc-bootstrap-seed` — A fresh serving process restores
//!   a configured SnapshotV1 seed before WAL or raft catch-up.

use anyhow::Result;
use clap::Parser;

use crate::backup::{
    dispatch_backup, dispatch_snapshot_export, dispatch_snapshot_import, dispatch_snapshot_inspect,
};
use crate::cli::spec::{LlmFormat, SpecFormat, SpecSub};
use crate::cli::{Cli, Command};
use crate::connect::connect;
use crate::dockerfile::dockerfile;
use crate::issue::{issue, TOOL};
use crate::k8s::k8s;
use crate::query::dispatch_query;
use crate::serve::serve;
use crate::spec::spec_gen;

mod backup;
mod cli;
mod connect;
mod dockerfile;
mod issue;
mod k8s;
mod query;
mod serve;
#[cfg(feature = "raft-wal")]
mod shutdown;
mod spec;
mod standalone;

#[tokio::main]
async fn main() -> Result<()> {
    lumen::tls::install_default_crypto_provider();
    let cli = Cli::parse();
    match cli.cmd {
        Command::Standalone(args) => standalone::run(args).await,
        Command::Serve(args) => serve(args).await,
        Command::Spec(args) => {
            // `spec gen` writes a typed client; everything else prints to stdout.
            if let Some(SpecSub::Gen(gen)) = args.gen {
                return spec_gen(gen);
            }
            // Offline self-description: no engine, no server, no I/O beyond stdout.
            let out = if args.shapes {
                serde_json::to_string_pretty(&lumen::spec::query_shapes())?
            } else if args.fields {
                serde_json::to_string_pretty(&lumen::spec::field_catalog())?
            } else {
                match args.format {
                    SpecFormat::Openapi => lumen::spec::openapi_json(),
                    SpecFormat::OpenapiYaml => lumen::spec::openapi_yaml(),
                    SpecFormat::JsonSchema => lumen::spec::json_schema_json(),
                }
            };
            // Raw spec bytes are a public artifact: this CLI output, the live
            // `/openapi.json` route, `spec gen`, and the committed snapshot all
            // consume `spec::openapi_json()` without an extra wrapper/newline.
            print!("{out}");
            Ok(())
        }
        Command::Llm(args) => {
            // Offline: no engine, no server, no I/O beyond stdout.
            let format = match args.format {
                LlmFormat::Md => cli_std::llm::Format::Md,
                LlmFormat::Json => cli_std::llm::Format::Json,
            };
            let out = lumen::dx::render_llm(args.topic.id(), format)?;
            println!("{out}");
            Ok(())
        }
        Command::Dockerfile(args) => dockerfile(args),
        Command::K8s(args) => k8s(args).await,
        Command::Upgrade(args) => {
            cli_std::upgrade::run(
                &TOOL,
                cli_std::upgrade::Options {
                    check: args.check,
                    tag: args.tag,
                    force: args.force,
                    yes: args.yes,
                },
            )
            .await
        }
        Command::Issue(args) => issue(args).await,
        Command::Backup(args) => dispatch_backup(args).await,
        Command::Dump(args) | Command::Export(args) => dispatch_snapshot_export(args).await,
        Command::Load(args) | Command::Import(args) => dispatch_snapshot_import(args).await,
        Command::Inspect(args) => dispatch_snapshot_inspect(args),
        Command::Connect(args) => connect(args).await,
        Command::Query(args) => dispatch_query(args).await,
    }
}
