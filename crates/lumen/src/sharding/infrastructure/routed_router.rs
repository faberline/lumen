//! Cross-pod shard routing for operator/k8s serving pods (#1398 R1-R3).
//!
//! [`RoutedRouter`] is the sole implementation of [`RoutedBackend`]:
//! it consults the delivered [`VirtualBucketShardMap`] and
//! either answers locally (this pod owns the target bucket) or forwards to
//! the owning shard's pod over the same h2c client stack every other
//! cross-pod call in this codebase uses (`libs/transport-h2c`, see
//! `operator::application::reshard_driver`'s admin forwarding for the established
//! `reqwest`-over-headless-DNS idiom this module follows). A routing-key-less
//! search scatters to every shard (local direct call + one forward per
//! remote shard) and merges through the same
//! [`crate::sharding::application::search_fanout::merge_shard_search_responses`] primitive
//! [`crate::sharding::application::engine_shard_search::EngineShardSearch`] uses.
//!
//! One-hop forwarding guard: every method checks `x-lumen-forwarded` FIRST.
//! (#1442 R1/R2 hardening of the #1398 guard.) The marker alone is
//! caller-controlled — an external client can set it directly on a request
//! to any pod — so a forwarded request is no longer trusted blindly:
//! `check_forwarded_map_version` rejects it with a distinct retryable error
//! (`ShardMapVersionMismatch`, R2) if the sender's `x-lumen-map-version`
//! disagrees with this pod's live map (the mixed-map window during a
//! rolling restart after a completed reshard split), and `assert_owns`
//! recomputes bucket ownership (R1) for every deterministic-owner path
//! (writes, keyed search) and rejects (`ShardForwardMisrouted`) rather than
//! answering locally when this pod isn't actually the owner — a spoofed or
//! genuinely misrouted forward is rejected, never honored. A forwarded
//! request still never forwards again, so cross-pod routing remains one hop
//! deep. Keyless (scatter) sub-requests are exempt from BOTH checks
//! (#1457 R4, widening the #1442 R1 ownership exemption to the map-version
//! check too): every shard is a legitimate scatter participant with no
//! single owner to validate against, and this pod always answers truthfully
//! for its own local data regardless of which map version the scatter's
//! originating pod was running — gating a keyless sub-request on map-version
//! agreement made every scatter search unavailable pod-wide during the
//! entire rolling-restart window after a completed split, even though no
//! single sub-answer here actually depended on the sender's map version
//! (see `search`'s `SearchShardTarget::All` arm). This is a deliberate
//! availability-over-completeness tradeoff (#1467 R6): a scatter search
//! answered mid-rolling-restart, with sub-responses spanning two map
//! versions, can silently miss or double-count buckets that moved between
//! those versions rather than failing loudly. Because the correctness gap
//! is silent, the exemption is paired with an observable signal instead of
//! only a doc comment: the responding pod compares the sender's declared
//! `x-lumen-map-version` against its own live map on every scattered
//! sub-request and, on a mismatch, increments
//! `lumen_scatter_map_version_mismatches_total` (`src/app/observability/metrics.rs`) and logs
//! a `tracing::warn!` — non-fatal, so a spike there is a signal for
//! operators to correlate with an in-flight rollout, not an outage. A
//! forward carries the
//! caller's `Authorization` and `x-read-consistency` headers through
//! unchanged (R3) plus `x-lumen-forwarded: 1` and `x-lumen-map-version:
//! <sender's map version>` (R2).
//!
//! Behind the `operator` feature only because it is the sole module that
//! needs `reqwest` as a directly-nameable type — every real deployment that
//! can reach this code path already links it transitively via `operator`'s
//! `backup` feature (`dep:reqwest`), so gating here adds no new dependency
//! edge, it only keeps `reqwest::*` out of the unconditionally-compiled
//! `app::http`.
//!
//! [`RoutedBackend`]: crate::sharding::application::ports::routed_backend::RoutedBackend
//! [`VirtualBucketShardMap`]:
//!   crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap

pub(crate) mod backend;
pub(crate) mod forward;
pub(crate) mod scatter;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::HeaderMap;

use crate::index::application::engine::{collections::DropOutcome, Engine};
use crate::index::infrastructure::search_executor::BlockingSearchExecutor;
use crate::ingest::application::ports::write_backend::WriteBackend;
use crate::sharding::domain::forward_error::{ShardForwardMisrouted, ShardMapVersionMismatch};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// Internal one-hop guard header: present on every forwarded request. No
/// longer trusted blindly on receipt (#1442 R1) — an external caller can set
/// this directly too, so every deterministic-owner path re-validates
/// ownership (`assert_owns`) instead of short-circuiting on its presence
/// alone.
const FORWARDED_HEADER: &str = "x-lumen-forwarded";

/// Sender's shard-map version, carried on every forward (#1442 R2). The
/// receiver rejects with a distinct retryable error when this disagrees with
/// its own live map instead of letting the one-hop guard force a (possibly
/// stale/possibly future) local answer during a rolling restart's mixed-map
/// window.
const MAP_VERSION_HEADER: &str = "x-lumen-map-version";

/// Read-consistency header carried through a forward verbatim (R3).
const READ_CONSISTENCY_HEADER: &str = "x-read-consistency";

/// Connections per remote shard's h2c pool — small and fixed, matching
/// `operator::application::reshard_driver`'s admin client sizing; forwarding is bounded
/// one-hop request/response, not a bulk data-mover.
const REMOTE_POOL_CONNECTIONS: usize = 2;

const REMOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// One remote shard's forwarding target: a stable headless-DNS base URL plus
/// its own small h2c connection pool. `None` at the local shard's index in
/// [`RoutedRouter::remotes`] — the local shard is never dialed.
struct RemoteShard {
    base_url: String,
    pool: transport_h2c::H2cPool,
}

/// Routes reads and writes across physical shards for one operator/k8s
/// serving pod (#1398 R1-R3). Local-owned buckets hit `engine`/`local_write`
/// directly; remote-owned buckets forward one hop to the owning pod's
/// stable per-shard DNS name (`routing::shard_host`).
pub struct RoutedRouter {
    engine: Arc<Engine>,
    /// The same bounded blocking bridge the direct HTTP API uses. Routed local
    /// legs must not run CPU-bound Engine work on the forwarding task either.
    search_executor: BlockingSearchExecutor,
    local_write: Arc<dyn WriteBackend>,
    shard_map: VirtualBucketShardMap,
    local_shard: u32,
    remotes: Vec<Option<RemoteShard>>,
}

impl RoutedRouter {
    /// `shard_urls[shard]` is the base URL (`http://host:port`, no trailing
    /// slash) forwarded requests for that shard are sent to; its length must
    /// equal `shard_map.physical_shard_count()` and `local_shard` must be a
    /// valid index into it — both are startup-time invariants of the routed
    /// serving topology (#1398 R1), not runtime conditions, so a mismatch
    /// fails fast instead of routing into a missing shard.
    pub fn new(
        engine: Arc<Engine>,
        local_write: Arc<dyn WriteBackend>,
        shard_map: VirtualBucketShardMap,
        local_shard: u32,
        shard_urls: Vec<String>,
    ) -> Result<Self> {
        let physical = shard_map.physical_shard_count();
        if shard_urls.len() != physical as usize {
            anyhow::bail!(
                "shard_urls has {} entries but the shard map declares {physical} physical shards",
                shard_urls.len()
            );
        }
        if local_shard >= physical {
            anyhow::bail!("local_shard {local_shard} is out of range for {physical} shards");
        }
        let remotes = shard_urls
            .into_iter()
            .enumerate()
            .map(|(shard, base_url)| -> Result<Option<RemoteShard>> {
                if shard as u32 == local_shard {
                    return Ok(None);
                }
                let pool = transport_h2c::H2cPool::with_connections_and(
                    REMOTE_POOL_CONNECTIONS,
                    Some(REMOTE_TIMEOUT),
                    Some("lumen-routed"),
                )
                .context("build routed h2c pool")?;
                Ok(Some(RemoteShard { base_url, pool }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            engine,
            search_executor: BlockingSearchExecutor::new(),
            local_write,
            shard_map,
            local_shard,
            remotes,
        })
    }

    fn already_forwarded(headers: &HeaderMap) -> bool {
        headers.contains_key(FORWARDED_HEADER)
    }

    /// Parses the sender's shard-map version off a forwarded request
    /// (#1442 R2). Absent on a fresh, non-forwarded request — only
    /// [`RoutedRouter::send`] sets this header — so this returns `None`
    /// there; it can also be `None`/unparseable on a forward from a peer
    /// mid-binary-upgrade that predates this header, which
    /// `check_forwarded_map_version` treats as "nothing to compare" rather
    /// than a hard failure.
    fn forwarded_map_version(headers: &HeaderMap) -> Option<u64> {
        headers
            .get(MAP_VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    /// #1442 R2: rejects a forwarded request whose sender ran a different
    /// shard-map version than this pod's live map, with a distinct
    /// retryable error — instead of letting the one-hop guard force a local
    /// answer that may be wrong on either side of a just-completed reshard
    /// split during a rolling restart's mixed-map window. Callers must skip
    /// this for keyless (scatter) sub-requests (#1457 R4): those never rely
    /// on the sender's map version to pick an owner, so gating on agreement
    /// only starved scatter search availability during a rolling restart
    /// without protecting anything. Only `search`'s keyed forward arm and
    /// the deterministic-owner write paths (`index`/`replace_docs`/`delete`)
    /// call this.
    fn check_forwarded_map_version(&self, headers: &HeaderMap) -> Result<()> {
        if let Some(sender_version) = Self::forwarded_map_version(headers) {
            let local_version = self.shard_map.version();
            if sender_version != local_version {
                return Err(anyhow::Error::new(ShardMapVersionMismatch {
                    sender_version,
                    local_version,
                }));
            }
        }
        Ok(())
    }

    /// #1442 R1: validates this pod actually owns `external_id`'s virtual
    /// bucket before honoring a forwarded request's one-hop marker. The
    /// marker alone is caller-controlled (an external client can set
    /// `x-lumen-forwarded` directly on a request to any pod), so every
    /// deterministic-owner path (writes, keyed search) recomputes ownership
    /// on receipt and rejects (`ShardForwardMisrouted`) rather than
    /// answering locally when this pod isn't the real owner — a spoofed or
    /// genuinely misrouted forward is rejected, never honored. Keyless
    /// (scatter) sub-requests have no single owner to validate against and
    /// are deliberately not routed through this check (see `search`'s
    /// `SearchShardTarget::All` arm).
    fn assert_owns(&self, collection_id: &str, external_id: &str) -> Result<()> {
        let route = self
            .shard_map
            .route_document(collection_id, None, external_id);
        if route.shard != self.local_shard {
            return Err(anyhow::Error::new(ShardForwardMisrouted {
                bucket: route.bucket,
                owner_shard: route.shard,
                local_shard: self.local_shard,
            }));
        }
        Ok(())
    }
}

/// Duplicates `routing::parse_cursor`'s tiny base64+JSON offset decode
/// (private to that module) rather than widening its visibility for one
/// caller — this is incidental cursor-codec plumbing, not the reusable
/// merge primitive (`merge_shard_search_responses`) this module already
/// shares with `routing.rs`.
#[cfg(test)]
fn cursor_offset(cursor: Option<&str>) -> u64 {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    let Some(s) = cursor else {
        return 0;
    };
    let Ok(raw) = STANDARD_NO_PAD.decode(s) else {
        return 0;
    };
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&raw) else {
        return 0;
    };
    v.get("offset").and_then(|o| o.as_u64()).unwrap_or(0)
}

/// Percent-encodes one path segment or query value: RFC 3986's unreserved
/// set (`A-Z a-z 0-9 - . _ ~`) passes through, everything else becomes
/// `%XX` (#1442 R4). Forwarded URLs interpolate caller-controlled
/// `external_id`/`field` directly, so an unescaped `/`, `?`, `#`, `%`, or
/// non-ASCII byte would otherwise be misparsed as URL structure (or land as
/// a doubly-decoded literal) on the remote pod. A tiny local encoder rather
/// than the `percent-encoding` crate: it is not a direct dependency of
/// lumen today (only pulled in transitively via other crates' `reqwest`/
/// `url` deps), so this keeps the fix dependency-free per #1442 R4.
fn percent_encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// #2496: recover a forwarded shard's [`DropOutcome`] from
/// `DELETE /collections/{id}`'s status-only response. `force` disambiguates
/// the one code the wire collapses: `204` means `Physical` under
/// `force=true` and `AlreadyMarked` under `force=false` — the two can never
/// both occur for the *same* fan-out call (`Physical` only exists on the
/// `force=true` branch, `AlreadyMarked` only on `force=false`), so `force`
/// alone is enough to recover the exact variant without widening the wire
/// contract.
fn drop_outcome_from_status(status: reqwest::StatusCode, force: bool) -> Result<DropOutcome> {
    match status {
        reqwest::StatusCode::ACCEPTED => Ok(DropOutcome::Marked),
        reqwest::StatusCode::NO_CONTENT => Ok(if force {
            DropOutcome::Physical
        } else {
            DropOutcome::AlreadyMarked
        }),
        reqwest::StatusCode::NOT_FOUND => Ok(DropOutcome::NotFound),
        other => anyhow::bail!("unexpected drop_collection status from shard: {other}"),
    }
}

/// #2496: merge two shards' [`DropOutcome`]s with the same
/// `Physical > Marked > AlreadyMarked > NotFound` precedence
/// [`EngineShardWrite::drop_collection`] uses in-process —
/// "the strongest thing any shard actually did" wins, so a caller never
/// sees a weaker outcome than what happened.
///
/// [`EngineShardWrite::drop_collection`]:
///   crate::sharding::application::engine_shard_write::EngineShardWrite::drop_collection
fn merge_drop_outcomes(a: DropOutcome, b: DropOutcome) -> DropOutcome {
    match (a, b) {
        (DropOutcome::Physical, _) | (_, DropOutcome::Physical) => DropOutcome::Physical,
        (DropOutcome::Marked, _) | (_, DropOutcome::Marked) => DropOutcome::Marked,
        (DropOutcome::AlreadyMarked, _) | (_, DropOutcome::AlreadyMarked) => {
            DropOutcome::AlreadyMarked
        }
        (DropOutcome::NotFound, DropOutcome::NotFound) => DropOutcome::NotFound,
    }
}

#[cfg(test)]
mod tests;
