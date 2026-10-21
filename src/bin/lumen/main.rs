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

mod backup;
mod cli;
mod connect;
mod dockerfile;
mod issue;
mod k8s;
mod query;
mod run;
mod serve;
#[cfg(feature = "raft-wal")]
mod shutdown;
mod spec;
mod standalone;

fn main() -> Result<()> {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    // A CPU-limited container reports its quota as available parallelism: a
    // 2.5-CPU pod gets two reactor workers. One request that blocks a worker
    // then halves the server's capacity to poll searches and `/metrics`.
    // `TOKIO_WORKER_THREADS` keeps its usual meaning when it is set.
    if std::env::var_os("TOKIO_WORKER_THREADS").is_none() {
        runtime.worker_threads(run::reactor_worker_threads());
    }
    runtime.enable_all().build()?.block_on(run::run())
}
