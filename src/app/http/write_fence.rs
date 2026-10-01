//! The reshard write fence (#1396 R2): a bounded pause on the virtual buckets a
//! reshard's final migration pass copies, which the write guards check.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// A bounded, status-visible write pause on a set of still-moving virtual
/// buckets (#1396 R2), the mechanism #1381's R5 review sanctioned: "a
/// bounded final pause of writes to still-moving buckets is acceptable if
/// needed for convergence, but must be bounded and reported in status."
///
/// Closes the copy-to-evict gap in the reshard driver's `CatchingUp` pass:
/// without a pause, a write landing on a source shard's moving bucket after
/// the last migration-copy read but before that bucket's eviction is never
/// re-copied to the target and is silently dropped by eviction. The driver
/// arms a fence over exactly the buckets its final migration pass is about
/// to copy (`POST /admin/reshard:fence`) immediately before that pass, so
/// the single pass taken under the fence is already a complete/converged
/// snapshot of those buckets — no repeat-until-converged loop is needed —
/// and clears the fence (an empty-`buckets` call to the same verb) on every
/// exit path of that tick, success or `Blocked`.
///
/// Crash safety: a driver process that dies between arming and clearing
/// cannot leave a bucket permanently unwritable. [`WriteFence::blocks`]
/// checks `deadline` on every call and treats an expired fence as unarmed —
/// this check runs on the *serving pod*, independent of whether the driver
/// process that armed it is still alive, so expiry is enforced even if the
/// driver never comes back. The reshard driver re-arms a fresh deadline
/// every tick it needs one, so a healthy, slow-but-progressing driver never
/// races its own TTL; see `operator::application::reshard_driver::WRITE_FENCE_TTL_SECS`.
#[derive(Clone, Default)]
pub struct WriteFence {
    state: Arc<Mutex<Option<FenceState>>>,
}

struct FenceState {
    virtual_bucket_count: u32,
    buckets: BTreeSet<u32>,
    deadline: Instant,
}

impl WriteFence {
    /// Arm the fence over `buckets` (computed against `virtual_bucket_count`)
    /// until `ttl` from now, replacing any prior armed state. Returns `false`
    /// (leaving any prior armed state untouched) when `Instant::now() + ttl`
    /// would overflow (#1443 R3) — the caller must treat that as a failed arm
    /// rather than silently panicking with the fence lock held, which would
    /// poison it for every subsequent write/clear on this pod.
    pub(crate) fn arm(
        &self,
        virtual_bucket_count: u32,
        buckets: BTreeSet<u32>,
        ttl: Duration,
    ) -> bool {
        let Some(deadline) = Instant::now().checked_add(ttl) else {
            return false;
        };
        let mut guard = self.lock();
        *guard = Some(FenceState {
            virtual_bucket_count,
            buckets,
            deadline,
        });
        true
    }

    /// Explicitly disarm, independent of `deadline`.
    pub(crate) fn clear(&self) {
        *self.lock() = None;
    }

    /// `Some(bucket)` when `collection_id`/`external_id` route to a
    /// currently-fenced bucket; `None` (unblocked) once armed but past
    /// `deadline`, or never armed at all.
    pub(super) fn blocks(&self, collection_id: &str, external_id: &str) -> Option<u32> {
        let guard = self.lock();
        let fence = guard.as_ref()?;
        if Instant::now() >= fence.deadline {
            return None;
        }
        let map = VirtualBucketShardMap::balanced(0, fence.virtual_bucket_count, 1).ok()?;
        let bucket = map.route_document(collection_id, None, external_id).bucket;
        fence.buckets.contains(&bucket).then_some(bucket)
    }

    /// A collection-wide mutation has no document id from which to derive one
    /// bucket.  During a reshard cutover it must therefore wait for every
    /// active bucket fence, rather than slipping through a fenced subset.
    pub(super) fn blocks_any(&self) -> bool {
        let guard = self.lock();
        guard
            .as_ref()
            .is_some_and(|fence| Instant::now() < fence.deadline && !fence.buckets.is_empty())
    }

    /// Poison-proof lock acquisition (#1443 R3), matching `SegmentRdbStore`'s
    /// `save_lock` precedent: a panic anywhere else in the process while
    /// holding this lock must never turn into a permanent write outage on
    /// this pod by propagating a poisoned-mutex panic into every later
    /// `arm`/`clear`/`blocks` call.
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<FenceState>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
