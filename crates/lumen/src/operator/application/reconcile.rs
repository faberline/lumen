//! lumen's operator wiring onto the shared `libs/service-k8s` controller.
//!
//! The reconcile loop + leader-election lease now live in `libs/service-k8s`
//! (`service_k8s::run` drives the watch + leader-gated apply over h2c-free kube;
//! `service_k8s::lease` is the elector). lumen supplies only its `ManagedService`
//! impl — what to render, which workloads to poll for readiness, and the
//! `Lumen` status subresource to write.
//!
//! Live per-shard storage-usage measurement (#1319 R1): `ManagedService::
//! status_patch` is synchronous and does no I/O by contract (shared with
//! keep/relay/loom via `libs/service-k8s`), so it cannot itself poll pod
//! `/metrics` endpoints. Instead `run()` spawns a lumen-local background
//! loop (`spawn_shard_usage_loop`) that periodically scrapes every storage
//! pod's `lumen_storage_bytes` gauge over its headless-Service DNS name and
//! writes the per-shard max into an in-process cache; `status_patch` reads
//! that cache synchronously (best-effort — an empty/missing cache falls back
//! to the policy-only [`crate::operator::domain::lumen_spec::LumenSpec::reshard_status`]).
//! This keeps the shared `libs/service-k8s` trait untouched.
//!
//! This loop only *reports* a crossed `prepareAtPercent` / `urgentAtPercent`
//! threshold in `status.reshard`; a second, independently leader-gated
//! background loop spawned alongside it — [`crate::operator::reshard_driver::
//! spawn_reshard_driver_loop`] (#1319 R2, #1381) — is what actually drives
//! `workflow.phase` and moves data once a threshold is crossed.
//!
//! ## Post-cutover usage freshness (#1386)
//!
//! Each [`ShardUsageSnapshot`] the loop below writes into the cache is
//! tagged with `spec.shardMap.version` as read off the very same CR the
//! scrape was addressed against — the freshness generation
//! [`crate::operator::domain::lumen_spec::LumenSpec::reshard_status_with_usage`] compares
//! against the CR's *current* `shard_map.version` before ever reporting a
//! crossed threshold. Without this, a split's `Complete` cutover (which
//! bumps `shard_map.version`) races this loop's own
//! [`SHARD_USAGE_POLL_INTERVAL`] cadence: the cache can still hold a
//! pre-cutover, pre-eviction reading for up to that whole interval, and
//! [`crate::operator::application::reshard_driver::trigger::should_start_split`] would otherwise
//! re-fire off that stale number the very next driver tick — the live bug
//! #1384's kind proof caught (a second split starting 20s after the first
//! one's `Complete`, purely off a reading taken before the eviction that
//! same cutover had just performed). Neither loop needs to synchronize
//! with the other directly: the generation tag alone is enough, and it
//! survives an operator failover the same way every other reshard
//! checkpoint does — it rides on the CR itself (`status.reshard.
//! usageMeasuredAtMapVersion`, freshly recomputed by whichever replica
//! next runs `status_patch`), never in this loop's or the driver's
//! in-process state.
//!
//! ## HPA topology-transition handoff (#1385)
//!
//! [`render::render`] no longer emits a HorizontalPodAutoscaler for any data
//! topology ([`render::wants_hpa`] is always false), but `libs/service-k8s`'s
//! shared reconcile contract (`libs/service-k8s::
//! service`) deliberately does not prune children across a render-shape
//! change — that handoff is left to the service. A third independently
//! leader-gated background loop, [`spawn_hpa_handoff_loop`], is lumen's side
//! of that handoff: every tick it lists every `Lumen` CR and, for any whose
//! current shape never wants an HPA, deletes any previously-rendered one
//! if it is still there — scoped and idempotent (R2: only an object whose
//! live name *and* labels match what [`render::hpa_labels`] would have
//! stamped; a missing HPA, or one that doesn't look lumen-rendered, is a
//! no-op, not an error). Without this migration cleanup, a stale HPA can keep
//! mutating the data StatefulSet outside the shared membership-aware capacity
//! contract.
//!
//! ## Contracts inherited from the retired EC shells
//!
//! These 2 sentences were the whole of the `// Contract:` comment in 2 AW-EC shells
//! under `e2e/`, each of which ran `cargo test -p lumen --features operator
//! --lib prune_stale_hpa_deletes_operator_rendered_hpa_on_multi_shard` in a subprocess
//! and asserted the child's exit status. That test is this file's own.
//!
//! Until 2026-08-20 these shells could not be deleted. The project's only declared gate
//! was `cargo test -p lumen`, and with `default = []` that command did not compile this
//! module at all — `crate::operator` gates `pub mod reconcile;` on the `operator`
//! feature — so each shell's `--lib` name filter matched no test, printed `0 passed`,
//! and exited 0. That left the shells as the sole surviving record that these checks
//! should run at all. `CONTRIBUTING.md` declared `cargo test -p lumen
//! --features "operator delegated-auth"` as a required second gate row that day, and
//! that run executes this module's colocated tests directly. That made each shell a
//! second, nested run of a check the gate already covers, so they were deleted the same
//! day. The sentence is the only thing they held that nothing else did. Each line below
//! is prefixed with the EC id its shell was filed under.
//!
//! - `lumen-claim-dynamic-stale-hpa-handoff` — The reconcile loop deletes a stale
//!   operator-rendered HPA when fixed shard topology takes ownership.
//! - `lumen-claim-k8s-topology-hpa-handoff` — The Kubernetes reconcile loop deletes
//!   stale autoscaling state when fixed storage topology takes over.
//!
//! [`ShardUsageSnapshot`]: crate::operator::application::reconcile::shard_usage::ShardUsageSnapshot
//! [`SHARD_USAGE_POLL_INTERVAL`]: crate::operator::application::reconcile::shard_usage::SHARD_USAGE_POLL_INTERVAL
//! [`render::render`]: crate::operator::application::render::render
//! [`render::wants_hpa`]: crate::operator::application::render::wants_hpa
//! [`render::hpa_labels`]: crate::operator::application::render::hpa_labels

pub(crate) mod auth_delegator;
pub(crate) mod hpa;
pub(crate) mod loops;
pub(crate) mod managed_service;
pub(crate) mod observation;
pub(crate) mod plan_verdicts;
pub(crate) mod shard_usage;

use kube::Client;

use crate::operator::application::reconcile::loops::{
    spawn_auth_delegator_sweep_loop, spawn_hpa_handoff_loop,
};
use crate::operator::application::reconcile::shard_usage::spawn_shard_usage_loop;
use crate::operator::domain::lumen_spec::Lumen;

/// `lumen k8s operator run` — run the reconcile controller on the shared
/// `libs/service-k8s` host (leader-gated; safe at `replicas > 1`), alongside
/// the live shard-usage measurement loop (#1319 R1; every replica runs it,
/// not just the leader — see [`spawn_shard_usage_loop`]), the autonomous
/// reshard phase driver (#1319 R2, #1381; independently leader-gated — see
/// [`crate::operator::application::reshard_driver::driver_loop::spawn_reshard_driver_loop`]), and the
/// HPA topology-transition handoff loop (#1385; independently leader-gated —
/// see [`spawn_hpa_handoff_loop`]), the fleet materialization loop
/// (independently leader-gated — see
/// [`crate::operator::application::fleet_reconcile::spawn_fleet_loop`]), and the auth-delegator
/// binding sweep (#2876; independently leader-gated — see
/// [`spawn_auth_delegator_sweep_loop`], which cleans up the one child no owner
/// reference can reach).
pub async fn run() -> anyhow::Result<()> {
    match Client::try_default().await {
        Ok(client) => {
            spawn_shard_usage_loop(client.clone());
            crate::operator::application::reshard_driver::driver_loop::spawn_reshard_driver_loop(
                client.clone(),
            );
            crate::operator::application::fleet_reconcile::spawn_fleet_loop(client.clone());
            spawn_hpa_handoff_loop(client.clone());
            spawn_auth_delegator_sweep_loop(client);
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "reshard live-usage measurement + phase-driver + fleet + HPA-handoff + auth-delegator-sweep loops disabled: could not build a kube client"
            );
        }
    }
    service_k8s::run::<Lumen>().await
}

#[cfg(test)]
mod tests;
