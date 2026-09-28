//! Autonomous reshard phase driver (#1319 R2 executor; #1381).
//!
//! [`super::reconcile`]'s live per-shard usage loop only *reports* a crossed
//! `prepareAtPercent` / `urgentAtPercent` threshold into
//! `status.reshard.blockingConditions`; this module is the piece that acts on
//! it — a second, independently leader-gated background loop that drives
//! `spec.reshardPolicy.workflow.phase` through
//! `PrepareSplit -> Splitting -> CatchingUp -> Complete`, growing storage by
//! exactly one physical shard per split via [`crate::routing::
//! VirtualBucketShardMap::split_one_shard`] and moving data with the already
//!-landed admin verbs (`POST /admin/backup:scoped`, `POST
//! /admin/reshard:apply`, `POST /admin/reshard:evict`, #1380).
//!
//! ## Checkpointing and resume
//!
//! There is no separate checkpoint record. Every phase's actions are
//! recomputed deterministically from three already-persisted **spec** fields
//! — `reshardPolicy.workflow.phase`, `reshardPolicy.workflow.
//! targetShardCount`, and `shardMap` (left untouched until the cutover) —
//! plus `shardCount`. `spec` (not `status`) is the checkpoint because it is
//! the operator's own desired-state write target and survives an operator
//! restart or leader handover unchanged; `status` stays a read-only
//! projection. Concretely:
//!
//! - **Complete** (idle): [`should_start_split`] gates on a crossed
//!   threshold (`status.reshard.blockingConditions` already carries
//!   `prepareThresholdCrossed` / `urgentThresholdCrossed`, computed by
//!   [`super::crd::LumenSpec::reshard_status_with_usage`]), `maxShardBytes`
//!   set (R3 safety rail — recommendation-only otherwise), single-member
//!   (`replicasPerShard <= 1`; see below), and no `maxShards` ceiling
//!   reached. On a match: compute the target map, patch `shardCount` and
//!   `workflow.{phase,targetShardCount}` in one merge patch, phase ->
//!   `PrepareSplit`.
//! - **PrepareSplit**: wait for the StatefulSet's `readyReplicas` to reach
//!   `targetShardCount` (the new pod exists once `shardCount` is bumped, via
//!   the *existing*, independently-leader-gated `libs/service-k8s` apply loop —
//!   this driver never applies child objects itself). Once ready, phase ->
//!   `Splitting`. Restart-safe: re-reads the same live readiness fact every
//!   tick.
//! - **Splitting**: run one migration pass ([`run_migration_pass`] — the
//!   production caller of [`crate::sharding::domain::bucket_move::bucket_moves`] /
//!   [`crate::sharding::domain::reshard_batch::snapshot_reshard_batches`]) copying every moved
//!   bucket from its old shard to the new shard via the admin verbs, then
//!   phase -> `CatchingUp`. `POST /admin/reshard:apply` is an idempotent
//!   additive merge (#1380), so re-running this same pass after a restart
//!   (still `Splitting`) is safe and simply re-applies the same batches.
//! - **CatchingUp** ([`advance_catching_up`], resequenced by #1396 R1/R2):
//!   arm a write-pause fence over every still-moving bucket on its current
//!   (source) owner (R2, see below), run the *same* migration pass again —
//!   an idempotent re-sync that, under the fence, is guaranteed a converged
//!   snapshot of every moving bucket — checkpoint **only the new/target
//!   shard** durably, evict every moved bucket from every old shard
//!   ([`crate::reshard`]'s `evict` is also idempotent), checkpoint every
//!   **source** shard durably, then flip `spec.shardMap` to the target map
//!   in the same patch that clears `workflow.targetShardCount` and resets
//!   phase -> `Complete`, followed by [`trigger_rolling_restart`], then
//!   clear the fence (unconditionally, on every exit path). Calling evict
//!   against the **new**, already-committed map (not the stale old one)
//!   means the driver never needs to retain the old map across a restart —
//!   the source of the classic "lost the old map after cutover" resumability
//!   trap. Checkpointing the target *before* evicting sources — rather than
//!   checkpointing everything only after both migration and eviction, as
//!   this driver did before #1396 — is R1: an eviction becoming durable (or
//!   even being attempted) before the target's copy of the same data is
//!   durably checkpointed can lose data that exists on no durable shard at
//!   all if the process crashes in between; see [`advance_catching_up`]'s
//!   own doc for the full crash-at-every-step analysis.
//!
//! A driver-side error at any step ([`DriveOutcome::Blocked`]) leaves the CR
//! spec untouched; the next tick retries the same phase from the same
//! persisted fields (R3).
//!
//! ## Write-pause fence during the final CatchingUp pass (#1396 R2)
//!
//! Even with the `Splitting` + `CatchingUp` double migration pass, a write
//! that lands on a moved bucket's old (source) shard after the *last*
//! migration-copy read but before that bucket's eviction is never re-copied
//! anywhere — eviction then silently drops it. [`advance_catching_up`] closes
//! this gap with a bounded, status-visible write pause (the mechanism #1381's
//! R5 review sanctioned: "a bounded final pause of writes to still-moving
//! buckets is acceptable if needed for convergence, but must be bounded and
//! reported in status") rather than a repeat-until-converged loop: arming the
//! fence *before* the tick's migration pass means that single pass is already
//! a complete snapshot of every fenced bucket, because no write can land on
//! them while it runs. [`crate::api::WriteFence`] (`POST
//! /admin/reshard:fence`) is the serving-side seam; a fenced write gets `503
//! bucket_write_paused` rather than being silently dropped or racing the map
//! flip. The fence is armed on every **source** shard (the live owners until
//! this tick's own cutover patch), and cleared — unconditionally, on every
//! exit path of [`advance_catching_up`], success or `Blocked` — once the
//! sequence finishes. A driver process that crashes before that explicit
//! clear cannot wedge writes forever: [`WriteFence::blocks`] enforces
//! [`WRITE_FENCE_TTL_SECS`] as a deadline on the *serving* pod itself,
//! independent of the driver's liveness.
//!
//! ## Migration durability (#1389)
//!
//! `Engine::apply_reshard_batch`/`evict_not_owned` (`storage.rs`, #1380)
//! mutate engine state directly rather than through `WriteCoordinator`/the
//! AOF, so — unlike ordinary writes — their durability is not implied by the
//! engine's normal write path; with #1387's embedded persistence it was
//! previously captured only by the next periodic `LUMEN_SNAPSHOT_SECS`
//! checkpoint (default 300s), well after this driver's own cutover restart
//! (~60-90s later) — observed live as 806 migrated batches lost on the
//! target and an eviction silently undone on the source (#1387's report).
//!
//! Two designs were considered: (a) route `:apply`/`:evict` through the AOF/
//! `WriteCoordinator` path as new `RaftLogEntry` variants, or (b) an explicit
//! synchronous checkpoint step the driver invokes and awaits per touched
//! shard before cutover. **(b) was chosen**: a whole `ReshardBatch`'s
//! `SnapshotV1` delta can be large (bounded by `MAX_EXTERNAL_IDS_PER_BATCH`,
//! but still potentially many collections/fields), which sits awkwardly as a
//! single `WalRecord`/AOF frame designed around one bounded mutation
//! (`Index`/`ReplaceDocs`/etc); it would also need new apply-loop branches
//! and idempotency semantics distinct from every existing `RaftLogEntry`
//! variant, whose apply methods mutate exactly one collection deterministically
//! rather than merge a whole delta. (b) reuses `segment_rdb.rs`'s
//! `SegmentRdbStore::save` verbatim — the exact call the periodic snapshotter
//! already makes, just invoked synchronously on demand via a new
//! `POST /admin/checkpoint` admin verb ([`crate::api::CheckpointSink`]) — no
//! new WAL record shape, no new apply-loop branch, no new idempotency
//! reasoning: `save` already re-seals the *entire* current engine state
//! (including whatever `:apply`/`:evict` already mutated) atomically
//! (stage-then-rename), so one checkpoint call captures every migration
//! mutation made so far, not just the most recent one.
//!
//! [`checkpoint_shards`] calls `POST /admin/checkpoint` on an explicit shard
//! set; [`advance_catching_up_fenced`] calls it twice per tick — once for
//! just the new/target shard immediately after migration and before any
//! source eviction is attempted (#1396 R1), and again for every source shard
//! (`0..current.physical_shard_count()`, `evict_old_shards`'s target set)
//! after eviction and before the `shardMap` cutover patch and
//! [`trigger_rolling_restart`]. A checkpoint failure on either call reports
//! [`DriveOutcome::Blocked`] and leaves `spec` at `CatchingUp` untouched
//! (R3): the next tick re-runs the whole idempotent
//! migrate/checkpoint/evict/checkpoint sequence, consistent with #1381's
//! spec-is-the-checkpoint semantics — cutover never fires on a shard whose
//! migration mutations are not yet durable, and eviction is never even
//! attempted before the target's copy of the same data is durable.
//! `checkpoint_shard` (#1396 R3) also now requires the response body to
//! report `persisted == true`; a `200 {"persisted": false}` — the vacuous
//! "no durable store configured" response `admin_checkpoint` returns for
//! [`crate::api::NoopCheckpoint`] deployments — is treated as a failed
//! checkpoint, not a satisfied durability gate.
//!
//! ## Scope rail: single-member only
//!
//! [`should_start_split`] refuses to start a split when `replicasPerShard >
//! 1`. Growing `shardCount` reassigns `shard_index = ordinal % shardCount`
//! for *every* existing pod ordinal once raft has more than one replica per
//! shard (a full raft-group reshuffle, not an added shard), which is unsafe
//! without additional raft-membership migration this WI does not implement.
//! At `replicasPerShard <= 1`, `ordinal % (shardCount+1) == ordinal` for
//! every existing ordinal (`ordinal < shardCount`), so growing by exactly
//! one is pod-ordinal-stable: every existing pod keeps its shard/PVC
//! identity and exactly one new pod (ordinal == old `shardCount`) becomes
//! the new shard — see [`super::crd::LumenSpec::storage_pod_count`].
//!
//! ## Live query routing consumes `spec.shardMap` (#1384)
//!
//! [`super::render::render`] writes `shardMap.{version,assignments}` into the
//! serving ConfigMap, `serving_env` maps `SHARD_MAP_VERSION`/
//! `VIRTUAL_BUCKET_COUNT`/`SHARD_MAP_ASSIGNMENTS` onto container env, and
//! `src/bin/lumen.rs`'s `serve()` builds its `EngineShardSearch` via
//! `EngineShardSearch::new_with_shard_map` fed by
//! `crate::sharding::infrastructure::shard_map_env::shard_map_from_env`, so a pod started after this driver's
//! cutover routes queries by the minimal-move target map computed by
//! [`crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap::split_one_shard`] rather than the
//! balanced default. [`trigger_rolling_restart`] is driven in the same
//! cutover tick that patches `spec.shardMap` so every serving pod picks up
//! the new map without manual intervention: it patches the serving
//! StatefulSet's pod-template annotations, which Kubernetes' native
//! `RollingUpdate` strategy (`serving_statefulset`'s `updateStrategy`) turns
//! into a rolling recreation of every pod against the already-updated
//! ConfigMap — no separate watch/poll loop is needed.
//!
//! [`super::crd::LumenSpec::reshard_status_with_usage`]: crate::operator::domain::lumen_spec::LumenSpec::reshard_status_with_usage
//! [`run_migration_pass`]: crate::operator::application::reshard_driver::migration::run_migration_pass
//! [`checkpoint_shards`]: crate::operator::application::reshard_driver::checkpoint::checkpoint_shards
//! [`advance_catching_up_fenced`]: crate::operator::application::reshard_driver::catch_up_fenced::advance_catching_up_fenced
//! [`super::crd::LumenSpec::storage_pod_count`]: crate::operator::domain::lumen_spec::LumenSpec::storage_pod_count

pub(crate) mod catch_up_fenced;
pub(crate) mod checkpoint;
pub(crate) mod cluster_control;
pub(crate) mod convergence;
pub(crate) mod convergence_stall;
pub(crate) mod driver_loop;
pub(crate) mod fence;
pub(crate) mod migration;
pub(crate) mod oversize;
pub(crate) mod phases;
pub(crate) mod transfer;
pub(crate) mod trigger;

use std::time::Duration;

use kube::ResourceExt;

use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::convergence::advance_convergence;
use crate::operator::application::reshard_driver::oversize::clear_oversize_block;
use crate::operator::application::reshard_driver::phases::{
    advance_catching_up, advance_prepare_split, advance_splitting, start_split,
};
use crate::operator::application::reshard_driver::trigger::should_start_split;
use crate::operator::domain::lumen_spec::topology::ReshardPhase;
use crate::operator::domain::lumen_spec::Lumen;

/// Poll interval for the reshard driver loop.
const DRIVER_POLL_INTERVAL: Duration = Duration::from_secs(20);

/// Leader-election Lease name for [`spawn_reshard_driver_loop`] — distinct
/// from `libs/service-k8s`'s own `S::MANAGER`-named apply-loop Lease so the two
/// independently-leader-gated loops (which may pick different leaders) never
/// contend on one Lease object.
///
/// [`spawn_reshard_driver_loop`]: crate::operator::application::reshard_driver::driver_loop::spawn_reshard_driver_loop
const DRIVER_LEASE_NAME: &str = "lumen-reshard-driver";

/// Upper bound on external_ids carried per `POST /admin/reshard:apply` call,
/// matching the batching contract [`crate::sharding::domain::reshard_batch::snapshot_reshard_batches`]
/// already documents (checkpoint after every batch, not after one full-shard
/// copy).
const MAX_EXTERNAL_IDS_PER_BATCH: usize = 2000;

/// TTL for the write-pause fence [`advance_catching_up`] arms over still-moving
/// buckets during its final migration pass (#1396 R2) — generous relative to
/// one tick's HTTP round trips (a scoped-backup fetch + apply batches across
/// however many source shards a split touches, then evict + checkpoint) while
/// still bounded; the fence is a crash-safety backstop the *serving* pod
/// enforces independent of the driver's own liveness, see
/// [`crate::api::WriteFence`]. Re-armed fresh every tick that needs one, so a
/// healthy, slow-but-progressing driver never races its own TTL.
const WRITE_FENCE_TTL_SECS: u64 = 120;

/// The production default [`ClusterControl::write_fence_ttl_secs`] value
/// (#1443 R1/AC1), exposed so integration tests can fall back to the real
/// default from a `fence_ttl_secs: Option<u64>`-style override field without
/// needing [`WRITE_FENCE_TTL_SECS`] itself to be `pub`.
pub fn default_write_fence_ttl_secs() -> u64 {
    WRITE_FENCE_TTL_SECS
}

/// What one [`drive_tick`] call did, for logging/tests. Never panics; a
/// failed step reports [`DriveOutcome::Blocked`] and leaves the CR spec
/// exactly as it was, so the next tick retries from the same persisted phase.
#[derive(Debug, Clone, PartialEq)]
pub enum DriveOutcome {
    /// Nothing to do this tick (`Complete` with no crossed threshold, an
    /// unsupported topology, or a `maxShards` ceiling reached).
    NoOp(&'static str),
    /// `Complete -> PrepareSplit`: `shardCount`/`targetShardCount` patched.
    StartedSplit { target_shard_count: u32 },
    /// Still `PrepareSplit`: the new pod is not `Ready` yet.
    WaitingForNewShard { target_shard_count: u32 },
    /// `PrepareSplit -> Splitting`: the new pod is `Ready`.
    AdvancedToSplitting,
    /// Still `Splitting`: one migration pass ran (batch count included; `0`
    /// only if there is nothing to move, which should not happen for a
    /// freshly started split).
    MigratedBatches { batches: usize },
    /// `Splitting -> CatchingUp`.
    AdvancedToCatchingUp,
    /// `CatchingUp -> Complete`: re-sync pass ran, old shards evicted, and
    /// `shardMap` flipped to the new version.
    CompletedSplit { new_map_version: u64 },
    /// #1458 R1: `Complete`, but not every serving pod is confirmed `Ready`
    /// on `map_version` yet — the write-pause fence over the buckets that
    /// moved into `map_version` was re-armed this tick, and
    /// `awaitingTopologyConvergence` should surface in `status.reshard`.
    AwaitingTopologyConvergence { map_version: u64 },
    /// #1458 R1: `Complete`, and every serving pod just got confirmed
    /// `Ready` on `map_version` — `workflow.convergedShardMapVersion` was
    /// patched to `map_version` and the write-pause fence was cleared.
    TopologyConverged { map_version: u64 },
    /// A step failed; phase unchanged, safe to retry next tick.
    Blocked(String),
}

/// One phase-driver tick for `lumen`: dispatches on `spec.reshardPolicy.
/// workflow.phase` and performs at most one state transition's worth of
/// work. Safe to call every [`DRIVER_POLL_INTERVAL`] forever — a `Complete`
/// CR with nothing to do returns [`DriveOutcome::NoOp`] immediately.
pub async fn drive_tick(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    lumen: &Lumen,
) -> DriveOutcome {
    let Some(namespace) = lumen.namespace() else {
        return DriveOutcome::Blocked("Lumen object missing metadata.namespace".to_string());
    };
    let name = lumen.name_any();

    match lumen.spec.reshard_policy.workflow.phase {
        ReshardPhase::Complete => {
            // #1458 R4: a workflow back at `Complete` has no legitimate
            // oversize wedge left to report — clear defensively (idempotent
            // if already clear from `run_migration_pass_impl`'s own
            // end-of-pass clear) so a manually-forced phase reset never
            // leaves a stale condition behind.
            clear_oversize_block(&namespace, &name);
            if let Some(outcome) =
                advance_convergence(control, http, &namespace, &name, lumen).await
            {
                outcome
            } else if should_start_split(lumen) {
                start_split(control, &namespace, &name, lumen).await
            } else {
                DriveOutcome::NoOp(
                    "no crossed threshold, unsupported topology, or maxShards reached",
                )
            }
        }
        ReshardPhase::PrepareSplit => {
            advance_prepare_split(control, &namespace, &name, lumen).await
        }
        ReshardPhase::Splitting => advance_splitting(control, http, &namespace, &name, lumen).await,
        ReshardPhase::CatchingUp => {
            advance_catching_up(control, http, &namespace, &name, lumen).await
        }
    }
}

#[cfg(test)]
mod tests;
