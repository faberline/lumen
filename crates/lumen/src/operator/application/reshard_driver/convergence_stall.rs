//! The topology-convergence stall budget (#1467 R7, #1485 R2): how long the
//! driver waits for the serving topology to converge before it reports the wait
//! as stalled.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::operator::application::reshard_driver::DRIVER_POLL_INTERVAL;

/// #1467 R7: bounded escalation budget for [`advance_convergence`] — after
/// this many consecutive `AwaitingTopologyConvergence` ticks for the same
/// `(uid, map_version)` pair without observing convergence, the driver
/// raises a distinct `topologyConvergenceStalled` status condition. The
/// fence itself is NEVER dropped when this budget is exceeded — re-arming
/// continues every tick exactly as before — this only makes an
/// abnormally-long convergence wait observable to operators.
/// `DRIVER_POLL_INTERVAL * CONVERGENCE_STALL_TICKS` = 10 minutes at the
/// current 20s poll interval, the same order of magnitude as
/// `OVERSIZE_RECHECK_TICKS`'s ~5 minutes.
///
/// #1485 R2: [`convergence_stall_cache`]/[`record_convergence_await`] below
/// (this tick-count budget) stay in place as a fast, driver-memory-only
/// signal, but they are no longer the authoritative source for whether the
/// stall budget has been exceeded — [`CONVERGENCE_STALL_SECS`], checked
/// against the durable `workflow.convergenceWaitStartedAt` timestamp, is.
///
/// [`advance_convergence`]: crate::operator::application::reshard_driver::convergence::advance_convergence
const CONVERGENCE_STALL_TICKS: u32 = 30;

/// #1485 R2: wall-clock equivalent of [`CONVERGENCE_STALL_TICKS`] at the
/// current [`DRIVER_POLL_INTERVAL`] — the durable stall budget
/// [`convergence_stall_condition`] applies to `workflow.
/// convergenceWaitStartedAt`. Computing the budget this way (elapsed time
/// since a persisted CR timestamp) rather than from an in-process tick
/// count is what makes both the budget and the `topologyConvergenceStalled`
/// condition it gates survive an operator restart mid-wait. `pub(crate)` so
/// `reconcile.rs`'s own tests can position a wait-start timestamp precisely
/// past the budget without sleeping in a unit test.
pub(crate) const CONVERGENCE_STALL_SECS: u64 =
    CONVERGENCE_STALL_TICKS as u64 * DRIVER_POLL_INTERVAL.as_secs();

/// The production [`CONVERGENCE_STALL_SECS`] value (#1485 R2), exposed the
/// same way [`default_write_fence_ttl_secs`] exposes [`WRITE_FENCE_TTL_SECS`]
/// — so integration tests can back-date `workflow.convergenceWaitStartedAt`
/// past the real budget (simulating an extended wait without sleeping)
/// without needing the constant itself to be `pub`.
///
/// [`WRITE_FENCE_TTL_SECS`]: crate::operator::application::reshard_driver::WRITE_FENCE_TTL_SECS
/// [`default_write_fence_ttl_secs`]: crate::operator::application::reshard_driver::default_write_fence_ttl_secs
pub fn convergence_stall_budget_secs() -> u64 {
    CONVERGENCE_STALL_SECS
}

/// Current wall-clock time as epoch seconds, saturating to `0` on a clock
/// error (mirrors [`KubeClusterControl::trigger_rolling_restart`]'s own
/// inline `SystemTime::now()` call) — the source of every `#1485` durable
/// timestamp this module stamps into `workflow.convergenceWaitStartedAt` /
/// `workflow.convergenceRemediationRestartedAt`.
///
/// [`KubeClusterControl::trigger_rolling_restart`]: crate::operator::infrastructure::kube_cluster_control::KubeClusterControl
pub(super) fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// `"<namespace>/<name>" -> (uid, map_version being awaited, consecutive
/// awaiting ticks)` — tracks how long [`advance_convergence`] has been
/// waiting for [`ClusterControl::serving_topology_converged`] to confirm
/// one particular `map_version`, for the R7 stall escalation. Mirrors
/// [`OversizeBlockCache`]'s shape and `uid`-scoping rationale (a
/// namespace/name pair is not stable identity across delete-and-recreate).
///
/// [`advance_convergence`]: crate::operator::application::reshard_driver::convergence::advance_convergence
/// [`ClusterControl::serving_topology_converged`]: crate::operator::application::reshard_driver::cluster_control::ClusterControl::serving_topology_converged
/// [`OversizeBlockCache`]: crate::operator::application::reshard_driver::oversize::OversizeBlockCache
type ConvergenceStallCache = std::sync::Mutex<BTreeMap<String, (String, u64, u32)>>;

fn convergence_stall_cache() -> &'static ConvergenceStallCache {
    static CACHE: std::sync::OnceLock<ConvergenceStallCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

fn convergence_stall_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

/// Bump (or start) `namespace/name`'s consecutive-awaiting-ticks counter
/// for `map_version` and return `true` once [`CONVERGENCE_STALL_TICKS`] has
/// been exceeded (this tick should report the stalled condition). A
/// `uid`/`map_version` change (a delete-and-recreate, or a fresh split
/// starting a new convergence wait before the prior one finished) resets
/// the counter rather than carrying over an unrelated wait's budget.
/// `pub(crate)` for the same test-seam reason as
/// [`record_oversize_block`].
///
/// [`record_oversize_block`]: crate::operator::application::reshard_driver::oversize::record_oversize_block
pub(crate) fn record_convergence_await(
    namespace: &str,
    name: &str,
    uid: &str,
    map_version: u64,
) -> bool {
    let mut cache = convergence_stall_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = cache
        .entry(convergence_stall_key(namespace, name))
        .or_insert_with(|| (uid.to_string(), map_version, 0));
    if entry.0 != uid || entry.1 != map_version {
        *entry = (uid.to_string(), map_version, 0);
    }
    entry.2 = entry.2.saturating_add(1);
    entry.2 > CONVERGENCE_STALL_TICKS
}

/// Clear `namespace/name`'s convergence-stall tracker — called once
/// convergence is observed (or the workflow is no longer awaiting it), so a
/// resolved wait never leaves the next, unrelated wait starting from a
/// stale budget. `pub(crate)` for the same test-seam reason as
/// [`clear_oversize_block`].
///
/// [`clear_oversize_block`]: crate::operator::application::reshard_driver::oversize::clear_oversize_block
pub(crate) fn clear_convergence_stall(namespace: &str, name: &str) {
    convergence_stall_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&convergence_stall_key(namespace, name));
}

/// Drop every cached convergence-stall entry whose `uid` is not in
/// `live_uids` — the [`prune_oversize_cache`] counterpart for this cache,
/// called from the same poll loop with the same already-listed live-CR set.
///
/// [`prune_oversize_cache`]: crate::operator::application::reshard_driver::oversize::prune_oversize_cache
pub(crate) fn prune_convergence_stall_cache(live_uids: &BTreeSet<String>) {
    convergence_stall_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|_, (uid, _, _)| live_uids.contains(uid));
}

/// Whether an `awaitingTopologyConvergence` wait that began at
/// `wait_started_at` (`workflow.convergenceWaitStartedAt`, #1485 R2) has run
/// longer than [`CONVERGENCE_STALL_SECS`], for `reconcile.rs`'s
/// `status_patch` to layer a `topologyConvergenceStalled` blocking condition
/// onto the policy/usage-derived status. Computed purely from this one
/// persisted CR timestamp — not driver memory — so the answer is the same
/// whether or not the driver process has restarted since the wait began;
/// [`advance_convergence`]'s own bounded-remediation gate uses the exact
/// same computation. `None` (convergence not pending, or no wait recorded
/// yet) is never stalled.
///
/// [`advance_convergence`]: crate::operator::application::reshard_driver::convergence::advance_convergence
pub fn convergence_stall_condition(wait_started_at: Option<u64>) -> bool {
    wait_started_at
        .is_some_and(|started| now_epoch_secs().saturating_sub(started) > CONVERGENCE_STALL_SECS)
}
