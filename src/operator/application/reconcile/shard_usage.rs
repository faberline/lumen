//! Live per-shard storage usage (#1319 R1): a loop on every replica scrapes
//! each storage pod's `lumen_storage_bytes` gauge into the local cache
//! `status_patch` reads.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use kube::{Client, ResourceExt};

use crate::operator::domain::lumen_spec::Lumen;

/// The client-facing port lumen's serving Service/StatefulSet expose
/// (`render::CLIENT_PORT`, private to that module). Duplicated here rather
/// than making that constant `pub` — this is the only other file that needs
/// it, and a `pub` const would need its own mirror symbol-table row.
const CLIENT_PORT: u16 = 7373;

/// Poll interval for the live per-shard storage-usage measurement loop
/// (#1319 R1).
pub(super) const SHARD_USAGE_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// One shard-usage measurement (#1386 R1): the raw per-shard bytes plus the
/// `spec.shardMap.version` that was live on the CR at scrape time — the
/// freshness generation [`crate::operator::domain::lumen_spec::LumenSpec::
/// reshard_status_with_usage`] compares against the CR's *current*
/// `spec.shardMap.version` to tell a post-cutover measurement apart from a
/// pre-cutover one this cache is still holding right after a split
/// completes.
#[derive(Clone, Debug)]
pub(super) struct ShardUsageSnapshot {
    pub(super) measured_at_map_version: u64,
    pub(super) usage: BTreeMap<u32, u64>,
}

/// `"<namespace>/<name>" -> ShardUsageSnapshot`, refreshed by
/// [`spawn_shard_usage_loop`] and read by [`status_patch`].
pub(super) type ShardUsageCache = Mutex<BTreeMap<String, ShardUsageSnapshot>>;

pub(super) fn shard_usage_cache() -> &'static ShardUsageCache {
    static CACHE: OnceLock<ShardUsageCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(super) fn cache_key(lumen: &Lumen) -> String {
    format!(
        "{}/{}",
        lumen.namespace().unwrap_or_else(|| "default".to_string()),
        lumen.name_any()
    )
}

/// Parse one gauge's value out of Prometheus text exposition (see
/// `crate::metrics::Registry::render`, e.g. `"lumen_storage_bytes 2048\n"`).
/// Ignores comment (`#`) and blank lines; returns `None` if `metric` is not
/// present or its value does not parse. `pub(crate)` (#1467 R5) so
/// `reshard_driver::KubeClusterControl::serving_pods_report_map_version` can
/// reuse it to parse `lumen_shard_map_version` off the same `/metrics`
/// exposition this module already scrapes for `lumen_storage_bytes`.
pub(crate) fn parse_metric(body: &str, metric: &str) -> Option<u64> {
    body.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (name, value) = line.split_once(' ')?;
        if name != metric {
            return None;
        }
        value.trim().parse::<f64>().ok().map(|bytes| bytes as u64)
    })
}

/// Fetch one pod's `/metrics` and read its `lumen_storage_bytes` gauge.
/// `None` on any network error, non-2xx status, or missing/unparseable
/// metric — an unreachable pod (e.g. mid-rollout) contributes nothing rather
/// than failing the whole measurement tick.
pub(super) async fn pod_storage_bytes(http: &reqwest::Client, url: &str) -> Option<u64> {
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.text().await.ok()?;
    parse_metric(&body, "lumen_storage_bytes")
}

/// Every storage pod's `(shard_index, /metrics URL)`, addressed by its
/// StatefulSet headless-Service DNS name
/// (`<name>-<ordinal>.<name>-headless.<ns>.svc.cluster.local:<port>`).
/// Ordinal-to-shard mapping matches `libs/raft-runtime`'s pod placement:
/// `shard_index = ordinal % shard_count`, `replica_index = ordinal /
/// shard_count`. Pod count and shard-count clamping follow
/// [`crate::operator::domain::lumen_spec::LumenSpec::storage_pod_count`] exactly, including
/// its single-shard-single-replica edge case (#1317).
pub(super) fn pod_metrics_urls(lumen: &Lumen) -> Vec<(u32, String)> {
    let name = lumen.name_any();
    let ns = lumen.namespace().unwrap_or_else(|| "default".to_string());
    let headless = format!("{name}-headless");
    let shard_count = lumen.spec.shard_count.max(1);
    let total = lumen.spec.storage_pod_count().max(0) as u32;
    (0..total)
        .map(|ordinal| {
            let shard_index = ordinal % shard_count;
            let url = format!(
                "http://{name}-{ordinal}.{headless}.{ns}.svc.cluster.local:{CLIENT_PORT}/metrics"
            );
            (shard_index, url)
        })
        .collect()
}

/// Scrape every `(shard_index, url)` pair and reduce to the per-shard
/// maximum observed byte count (a shard's busiest replica, not the sum —
/// replicas of the same shard hold the same raft-replicated data).
/// Unreachable pods are skipped, not treated as zero usage.
pub(super) async fn aggregate_shard_usage(
    http: &reqwest::Client,
    pod_urls: &[(u32, String)],
) -> BTreeMap<u32, u64> {
    let mut usage: BTreeMap<u32, u64> = BTreeMap::new();
    for (shard_index, url) in pod_urls {
        let Some(bytes) = pod_storage_bytes(http, url).await else {
            continue;
        };
        usage
            .entry(*shard_index)
            .and_modify(|max| *max = (*max).max(bytes))
            .or_insert(bytes);
    }
    usage
}

/// One measurement tick for `lumen`: [`pod_metrics_urls`] +
/// [`aggregate_shard_usage`].
async fn measure_shard_usage(http: &reqwest::Client, lumen: &Lumen) -> BTreeMap<u32, u64> {
    let urls = pod_metrics_urls(lumen);
    aggregate_shard_usage(http, &urls).await
}

/// Background loop (#1319 R1): every [`SHARD_USAGE_POLL_INTERVAL`], list
/// every `Lumen` CR cluster-wide, measure its live per-shard storage usage,
/// and refresh [`shard_usage_cache`]. Runs on every replica (not just the
/// leader) since it only populates a local read cache that `status_patch`
/// consults best-effort; the leader-gated `libs/service-k8s` apply loop is
/// still the only writer of the CR's status subresource.
pub(super) fn spawn_shard_usage_loop(client: Client) {
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let api: kube::Api<Lumen> = kube::Api::all(client);
        loop {
            match api.list(&Default::default()).await {
                Ok(list) => {
                    for lumen in list.items {
                        let usage = measure_shard_usage(&http, &lumen).await;
                        if usage.is_empty() {
                            continue;
                        }
                        let key = cache_key(&lumen);
                        // #1386 R1: tag this measurement with the map
                        // version live on the *same* CR read the scrape
                        // itself was addressed against, so a status
                        // computed later can tell whether it predates the
                        // next cutover.
                        let snapshot = ShardUsageSnapshot {
                            measured_at_map_version: lumen.spec.shard_map.version,
                            usage,
                        };
                        let mut cache = shard_usage_cache()
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        cache.insert(key, snapshot);
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "shard usage measurement: list Lumen failed");
                }
            }
            tokio::time::sleep(SHARD_USAGE_POLL_INTERVAL).await;
        }
    });
}
