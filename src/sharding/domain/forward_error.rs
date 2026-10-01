//! The errors a one-hop shard forward fails with: the owner unreachable, the
//! owner answering with an error status, a shard-map version disagreement
//! during a rolling restart, and a forwarded-hop marker this pod's own map does
//! not confirm.

/// One-hop shard-forward failure — the owning shard was unreachable (pod
/// down/rolling) or its response could not be decoded. Raised via `anyhow`
/// by `sharding::infrastructure::routed_router::RoutedRouter`, with
/// centralized classification in `app::http::api_err::ApiErr`;
/// R2 requires this to surface as a clear, distinctly-kinded retryable
/// error, never a silent local answer.
#[derive(Debug)]
pub struct ShardForwardUnavailable(pub String);

impl std::fmt::Display for ShardForwardUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ShardForwardUnavailable {}

/// The owning shard was reached and answered, but with a non-2xx status
/// (e.g. a forwarded write hit `404`/`422`). Re-emitted locally with the
/// same status so a forwarded error is as legible as a local one; `message`
/// carries the remote's own `{error, message}` envelope verbatim.
#[derive(Debug)]
pub struct ShardForwardRemoteError {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for ShardForwardRemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard forward error ({}): {}", self.status, self.message)
    }
}

impl std::error::Error for ShardForwardRemoteError {}

/// A forwarded request declared a shard-map version that disagrees with
/// this pod's own live map (#1442 R2). A rolling restart after a completed
/// reshard split can run pods on two different `SHARD_MAP_*` env snapshots
/// for a bounded window (pods only read env at boot) — rather than let the
/// one-hop guard force a local answer that may be wrong on either side of
/// the split, the receiver rejects with this distinct, retryable error so
/// the caller (or its own retry policy) waits for the rollout to converge.
#[derive(Debug)]
pub struct ShardMapVersionMismatch {
    pub sender_version: u64,
    pub local_version: u64,
}

impl std::fmt::Display for ShardMapVersionMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "forwarded request's shard-map version {} disagrees with this pod's live version {}",
            self.sender_version, self.local_version
        )
    }
}

impl std::error::Error for ShardMapVersionMismatch {}

/// A forwarded request's one-hop marker (`x-lumen-forwarded`) claimed this
/// pod, but recomputing ownership from this pod's own shard map disagrees
/// (#1442 R1). The marker alone is caller-controlled — an external client
/// can set it directly on a request to any pod, forcing local handling on a
/// bucket that pod doesn't actually own — so it is now validated on
/// receipt rather than trusted blindly; a spoofed or genuinely misrouted
/// forward is rejected, never honored.
#[derive(Debug)]
pub struct ShardForwardMisrouted {
    pub bucket: u32,
    pub owner_shard: u32,
    pub local_shard: u32,
}

impl std::fmt::Display for ShardForwardMisrouted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "forwarded request targets virtual bucket {} (owned by shard {}), but this pod is \
             shard {}; refusing to honor an unverified forwarded-hop marker",
            self.bucket, self.owner_shard, self.local_shard
        )
    }
}

impl std::error::Error for ShardForwardMisrouted {}
