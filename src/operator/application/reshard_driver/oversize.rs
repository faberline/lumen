//! The oversized-document wedge (#1444 R2): one document too large for any
//! batch, recorded per instance so catch-up skips the fenced pass for a bounded
//! number of ticks.

use std::collections::{BTreeMap, BTreeSet};

/// How many consecutive [`advance_catching_up`] ticks short-circuit on a
/// recorded [`OversizedDocumentBlock`] (#1444 R2) before attempting the full
/// fenced migration pass again. Bounds how long a document an operator has
/// since fixed (deleted or shrunk) stays wedged after the fix without
/// re-arming the write-pause fence — and reopening the recurring 503 window
/// the fix closes — on every single tick while the condition is genuinely
/// unchanged. `DRIVER_POLL_INTERVAL * OVERSIZE_RECHECK_TICKS` (5 minutes at
/// the current 20s poll interval) is the same order of magnitude as
/// [`WRITE_FENCE_TTL_SECS`]/`SHARD_USAGE_POLL_INTERVAL`-style bounds
/// elsewhere in this driver.
///
/// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
/// [`WRITE_FENCE_TTL_SECS`]: crate::operator::application::reshard_driver::WRITE_FENCE_TTL_SECS
pub(super) const OVERSIZE_RECHECK_TICKS: u32 = 15;

/// Distinguishes an apply failure caused by exactly one document's batch
/// serializing past [`crate::reshard::ADMIN_ROUTE_BODY_LIMIT_BYTES`] — the
/// `snapshot_reshard_batches`/`byte_cap_chunk` floor case
/// (`crate::reshard`'s module doc's "one document cannot be split further")
/// — from any other reason `POST /admin/reshard:apply` can fail (#1444 R2).
/// Deterministic every retry (nothing about the data or the byte cap changes
/// tick to tick), unlike a transient network/5xx error, so this is surfaced
/// as a distinct `status.reshard` blocking condition (see
/// [`oversize_block_condition`]) instead of the generic
/// [`DriveOutcome::Blocked`] message every other failure produces, and used
/// to skip re-arming the write-pause fence on a tick already known to fail
/// identically (see [`advance_catching_up`]).
///
/// [`DriveOutcome::Blocked`]: crate::operator::application::reshard_driver::DriveOutcome::Blocked
/// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedDocumentBlock {
    pub collection: String,
    pub external_id: String,
    pub bytes: usize,
}

impl std::fmt::Display for OversizedDocumentBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "reshard blocked: collection `{}` document `{}` serializes to {} bytes, over the \
             {} byte /admin/reshard:apply body limit; this single document cannot be split into \
             a smaller batch — shrink or remove its large field values (long text, vectors, \
             hashes), or exclude it from the collection, before this split can continue",
            self.collection,
            self.external_id,
            self.bytes,
            crate::reshard::ADMIN_ROUTE_BODY_LIMIT_BYTES
        )
    }
}

impl std::error::Error for OversizedDocumentBlock {}

/// `"<namespace>/<name>" -> (owning CR's metadata.uid, block, ticks skipped
/// on it so far)`, written by [`run_migration_pass_impl`] and consumed by
/// [`advance_catching_up`] (mutating, to decide/count a skip) and
/// [`oversize_block_condition`] (read-only, for `reconcile.rs`'s
/// `status_patch`) — #1444 R2. Mirrors `reconcile.rs`'s own
/// `ShardUsageCache` pattern: a synchronous status projection reads a cache
/// a background loop writes, rather than doing I/O itself.
///
/// Keyed by `namespace/name` (not `uid`, which is not stable input for a
/// lookup before an object exists) but every entry carries the `uid` of the
/// CR it was recorded for (#1458 R4): a namespace/name pair is not a stable
/// identity across a delete-and-recreate — the new CR gets a fresh `uid`
/// from the API server — so every read compares the stored `uid` against
/// the caller's current one and treats a mismatch as no entry, giving a
/// recreated CR a clean `status.reshard` immediately rather than inheriting
/// a stale wedge left by the deleted CR's last tick. [`prune_oversize_cache`]
/// bounds the map by dropping entries whose `uid` is no longer live.
///
/// [`run_migration_pass_impl`]: crate::operator::application::reshard_driver::migration::run_migration_pass_impl
/// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
pub(super) type OversizeBlockCache =
    std::sync::Mutex<BTreeMap<String, (String, OversizedDocumentBlock, u32)>>;

fn oversize_block_cache() -> &'static OversizeBlockCache {
    static CACHE: std::sync::OnceLock<OversizeBlockCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

fn oversize_cache_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

/// Record (or refresh) a discovered oversize wedge for `namespace/name`'s
/// `uid`, resetting its skip counter — a fresh discovery, whether this is
/// the first tick to hit it or a periodic recheck (#1444 R2, see
/// [`OVERSIZE_RECHECK_TICKS`]) that hit the same wedge again. `pub(crate)`
/// rather than private so `reconcile.rs`'s `status_patch` tests can drive the
/// exact cache [`oversize_block_condition`] reads, without widening this past
/// crate-internal visibility.
pub(crate) fn record_oversize_block(
    namespace: &str,
    name: &str,
    uid: &str,
    block: OversizedDocumentBlock,
) {
    oversize_block_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            oversize_cache_key(namespace, name),
            (uid.to_string(), block, 0),
        );
}

/// Clear any recorded oversize wedge for `namespace/name`, regardless of
/// which `uid` recorded it — called whenever a migration pass for it
/// completes without hitting one (whatever was wedged is resolved) and when
/// the workflow returns to phase `Complete` (#1458 R4). `pub(crate)` for the
/// same test-seam reason as [`record_oversize_block`].
pub(crate) fn clear_oversize_block(namespace: &str, name: &str) {
    oversize_block_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&oversize_cache_key(namespace, name));
}

/// Drop every cached entry whose `uid` is not in `live_uids` (#1458 R4) —
/// called once per [`spawn_reshard_driver_loop`] poll, which already lists
/// every live `Lumen` CR cluster-wide, so this needs no extra k8s API call.
/// Bounds the cache's growth across an unbounded number of past
/// delete-and-recreate cycles on the same `namespace/name`.
///
/// [`spawn_reshard_driver_loop`]: crate::operator::application::reshard_driver::driver_loop::spawn_reshard_driver_loop
pub(crate) fn prune_oversize_cache(live_uids: &BTreeSet<String>) {
    oversize_block_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|_, (uid, _, _)| live_uids.contains(uid));
}

/// If `namespace/name`'s current `uid` has a recorded oversize wedge AND has
/// not yet used up its [`OVERSIZE_RECHECK_TICKS`] skip budget, bump its skip
/// counter and return it — the caller ([`advance_catching_up`]) should
/// short-circuit to [`DriveOutcome::Blocked`] without arming the
/// write-pause fence. Returns `None` (no skip) once the budget is exhausted
/// or the cached entry belongs to a different `uid` (#1458 R4 — a stale
/// entry from a deleted-and-recreated CR), letting the next real attempt
/// either clear the wedge (if fixed) or re-record it with a fresh budget.
///
/// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
/// [`DriveOutcome::Blocked`]: crate::operator::application::reshard_driver::DriveOutcome::Blocked
pub(super) fn should_skip_for_oversize(
    namespace: &str,
    name: &str,
    uid: &str,
) -> Option<OversizedDocumentBlock> {
    let mut cache = oversize_block_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (cached_uid, block, ticks) = cache.get_mut(&oversize_cache_key(namespace, name))?;
    if cached_uid != uid || *ticks >= OVERSIZE_RECHECK_TICKS {
        return None;
    }
    *ticks += 1;
    Some(block.clone())
}

/// The oversized-document block currently recorded for `namespace/name`'s
/// current `uid`, if any (#1444 R2; `uid`-scoped by #1458 R4) — read-only,
/// does not affect [`should_skip_for_oversize`]'s skip budget. `reconcile.
/// rs`'s `status_patch` calls this to layer a distinct `status.reshard`
/// blocking condition + remediation message onto the policy/usage-derived
/// status. A cached entry belonging to a different `uid` (a deleted-and-
/// recreated CR under the same `namespace/name`) is treated as no entry, so
/// the recreated CR's status is clean immediately rather than waiting for
/// [`prune_oversize_cache`]'s next poll.
pub fn oversize_block_condition(
    namespace: &str,
    name: &str,
    uid: &str,
) -> Option<OversizedDocumentBlock> {
    oversize_block_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&oversize_cache_key(namespace, name))
        .filter(|(cached_uid, _, _)| cached_uid == uid)
        .map(|(_, block, _)| block.clone())
}
