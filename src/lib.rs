// CODEGEN-BEGIN
//! lumen — standalone search and duplicate-detection index.
//!
//! Solves the gap B-tree indexes can't fill: keyword search (incl. Chinese
//! tokenisation) and duplicate detection. Exposed as a generic
//! `Collection / Field` primitive over `external_id` — lumen never owns
//! the source of truth and has no document concept of its own.
//!
//! - Durable via the configured write log; multi-pod Lumen uses Lumen-owned
//!   primary/replica replication. Rebuildable from the caller.
//! - HTTP/2 transport, client-side collection-shard routing.
//!
//! Full surface and v1 scope: `README.md`.
//!
//! ## Contracts inherited from the retired EC shells
//!
//! These 3 sentences were the whole of the `// Contract:` comment in 3 AW-EC shells
//! under `e2e/`, each of which ran `cargo test -p lumen --lib` in a
//! subprocess and asserted the child's exit status. `cargo test -p lumen` already runs
//! this crate's colocated unit tests directly, so the shells added a second, nested run
//! and nothing else. They were deleted on 2026-08-20 with the EC machinery they
//! belonged to, and the sentence is the only thing they held that nothing else did.
//! Each line below is prefixed with the EC id its shell was filed under.
//!
//! - `lumen-claim-dynamic-versioned-virtual-bucket-map` — Versioned virtual-bucket
//!   routing remains the stable shard ownership contract.
//! - `lumen-claim-exact-keyword-lexicographic-range` — Keyword range queries use
//!   deterministic byte-lexicographic bounds rather than text analysis semantics.
//! - `lumen-claim-security-tls-rustls` — The rustls-backed TLS surface passes the
//!   runtime TLS gate.

mod access;
/// Local append-only log (Stage 2 Phase 2f-3): the binary's "AOF" — a framed,
/// crash-safe record of every APPLIED `(seq, WalRecord)`. Recovery is RDB (the
/// segment checkpoint, up to seq S) → AOF replay (S+1..A) → broker tail (A+1..),
/// so broker retention can be TRIMMED instead of kept from seq 0. Compiled by
/// default; only the runtime segment-persistence path (`--persistence=segment`)
/// drives the apply loop + cold-start through it.
pub use crate::compat::aof;
pub use crate::compat::api;
mod app;
pub use crate::compat::auth;
/// `lumen backup` (#808): fetches a consistent snapshot from a running
/// serving fleet's existing `GET /admin/backup` endpoint and hands it to a
/// `libs/service-backup` destination sink. No new snapshot mechanism — this
/// is transport/scheduling only, meant to be driven by the operator's
/// optional backup CronJob (`spec.backup`, see `service_k8s::render`) or ad hoc.
/// Behind the `backup` feature (pulled in by `operator`) since it needs an
/// HTTP client; `raft-wal` already links one into every shipped binary.
#[cfg(feature = "backup")]
pub use crate::compat::backup;
pub use crate::compat::backup_sink;
mod compat;
pub use crate::compat::config;
pub use crate::compat::coordinator;
pub use crate::sharding::application::consumer;
pub mod dx;
mod index;
mod ingest;
pub use crate::compat::metrics;
/// Native length-prefixed CBOR search wire for Rust clients that need the engine
/// over a lower fixed-cost transport than HTTP/JSON.
pub use crate::compat::native_wire;
/// Write-log entry vocabulary (always compiled; the active write path uses it).
pub use crate::shared_kernel::log_entry;
/// K8s Operator: the `Lumen` CRD plus the reconcile loop that renders + applies
/// the Lumen serving/data-plane resources. The CRD and reconcile loop are behind
/// the `operator` feature so the serving binary never pulls in kube-rs; pure
/// configuration stays in the operator and shared libraries; Lumen consumes
/// externally provisioned TLS Secrets rather than resolving issuers.
pub mod operator;
/// Cluster-state view types backing the read/admin API. This surface is the
/// compatibility bridge for Lumen-owned primary/replica replication.
pub use crate::compat::raft;
/// `EngineSm` — lumen's `Engine` as a shared-`raft_runtime` state machine: the
/// convergence onto `libs/raft-runtime` (#524). The host is the sole applier, so
/// the per-service driver, durable hard state, and the WAL seam are no longer
/// lumen's to own — they live in the shared lib.
#[cfg(feature = "raft-wal")]
pub use crate::compat::raft_sm;
mod persistence;
pub use crate::compat::rdb;
pub use crate::compat::reshard;
pub use crate::compat::routing;
/// Cross-pod shard routing for operator/k8s serving pods (#1398 R1-R3): local
/// reads/writes hit the engine directly, remote-owned buckets forward one hop
/// over h2c. Behind `operator` because it is the only module that needs
/// `reqwest` as a directly-nameable type; every real deployment already links
/// it via `operator`'s `backup` feature, so this adds no new crate.
#[cfg(feature = "operator")]
pub use crate::compat::routing_remote;
mod replication;
pub use crate::compat::segment_checkpoint;
/// Segment-checkpoint persistence store (Stage 2 Phase 2f-2): the disk engine
/// as the running binary's "RDB" — a generation-versioned directory of per-
/// collection segment checkpoints, written atomically (stage + rename) so a torn
/// checkpoint never replaces a good one. Parallels [`rdb::LocalFsRdbStore`].
/// Compiled by default; selected at runtime via `--persistence=segment` (the
/// default binary keeps the CBOR RDB).
pub use crate::compat::segment_rdb;
pub use crate::compat::segment_restore;
mod sharding;
mod shared_kernel;
/// Offline machine-readable self-description (`lumen spec`): OpenAPI / JSON
/// schema, the query-shape cookbook, and the field/analyzer catalog — the
/// agent-integration surface, emitted without a running server.
pub mod spec;
pub use crate::access::infrastructure::tls;
pub use crate::compat::storage;
pub use crate::compat::tokenize;
pub use crate::compat::types;
pub use crate::compat::vector_index;
pub use crate::compat::wal;

/// Product-neutral text-index contracts used by Lumen and other products.
/// Lumen keeps its existing public collection API and storage engine.
pub use index_text as text_index;
// CODEGEN-END
