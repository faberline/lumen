//! Member version reports (#248 slice 1a and 1b): an in-process cache of what each
//! fleet member reported, keyed like `shard_usage`. `status_patch` reads it
//! synchronously. The loop in this module (#248 slice 1b) is the only I/O: it
//! scrapes each pod's `/version` into the cache.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kube::{Client, ResourceExt};

use crate::domain::lumen_spec::member_versions::MemberVersionReport;
use crate::domain::lumen_spec::Lumen;

/// The client-facing port (`render::CLIENT_PORT`), duplicated as in `shard_usage`.
const CLIENT_PORT: u16 = 7373;

/// Poll interval for the member version loop (#248 slice 1b).
pub(super) const MEMBER_VERSION_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// `"<namespace>/<name>" -> reports`. [`record_member_reports`] replaces an
/// entry wholesale.
pub(super) type MemberVersionCache = Mutex<BTreeMap<String, Vec<MemberVersionReport>>>;

pub(super) fn member_version_cache() -> &'static MemberVersionCache {
    static CACHE: OnceLock<MemberVersionCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Replace the cached reports for `key`. The next slice (#248 slice 1b) is
/// the first non-test caller, so it allows dead code until then.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn record_member_reports(key: &str, reports: Vec<MemberVersionReport>) {
    if let Ok(mut cache) = member_version_cache().lock() {
        cache.insert(key.to_string(), reports);
    }
}

pub(super) fn cached_member_reports(key: &str) -> Option<Vec<MemberVersionReport>> {
    member_version_cache().lock().ok()?.get(key).cloned()
}

/// The pod names a fleet's members report under: `<name>-<ordinal>`, the same
/// names that `shard_usage::pod_metrics_urls` scrapes.
pub(super) fn expected_member_names(lumen: &Lumen) -> Vec<String> {
    let name = lumen.name_any();
    let total = lumen.spec.storage_pod_count().max(0) as u32;
    (0..total)
        .map(|ordinal| format!("{name}-{ordinal}"))
        .collect()
}

/// Seconds since the Unix epoch; `0` if the clock is before it.
pub(super) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// `(pod name, /version URL)` for every expected member, addressed by the
/// StatefulSet headless-Service DNS name
/// (`<name>-<ordinal>.<name>-headless.<ns>.svc.cluster.local:<port>`).
pub(super) fn pod_version_urls(lumen: &Lumen) -> Vec<(String, String)> {
    let lumen_name = lumen.name_any();
    let headless = format!("{lumen_name}-headless");
    let ns = lumen.namespace().unwrap_or_else(|| "default".to_string());
    expected_member_names(lumen)
        .into_iter()
        .map(|name| {
            let url =
                format!("http://{name}.{headless}.{ns}.svc.cluster.local:{CLIENT_PORT}/version");
            (name, url)
        })
        .collect()
}

/// One pod's report from its `/version` body. A network error, a non-2xx
/// status, a body that is not JSON, or a missing or mistyped field gives
/// `None`: a scrape failure never panics the loop.
pub(super) async fn fetch_member_report(
    http: &reqwest::Client,
    name: &str,
    url: &str,
    now_secs: u64,
) -> Option<MemberVersionReport> {
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let text = resp.text().await.ok()?;
    let body: serde_json::Value = serde_json::from_str(&text).ok()?;
    let version = body.get("version")?.as_str()?.to_string();
    let compatibility_version = body
        .get("effective_compatibility_version")?
        .as_str()?
        .to_string();
    let capabilities = body
        .get("binary_capabilities")?
        .as_array()?
        .iter()
        .map(|cap| cap.as_str().map(str::to_string))
        .collect::<Option<Vec<String>>>()?;
    Some(MemberVersionReport {
        name: name.to_string(),
        version,
        capabilities,
        compatibility_version,
        observed_at_secs: now_secs,
    })
}

/// Scrape every `(name, url)` in order. A pod that does not answer keeps its
/// entry from `previous` unchanged (its old `observed_at_secs`), so it turns
/// `Stale` later and is never shown as upgraded.
pub(super) async fn collect_member_reports(
    http: &reqwest::Client,
    pods: &[(String, String)],
    previous: &[MemberVersionReport],
    now_secs: u64,
) -> Vec<MemberVersionReport> {
    let mut reports = Vec::with_capacity(pods.len());
    for (name, url) in pods {
        match fetch_member_report(http, name, url, now_secs).await {
            Some(report) => reports.push(report),
            None => {
                if let Some(old) = previous.iter().find(|report| &report.name == name) {
                    reports.push(old.clone());
                }
            }
        }
    }
    reports
}

/// Background loop (#248 slice 1b): every [`MEMBER_VERSION_POLL_INTERVAL`],
/// list every `Lumen` cluster-wide and scrape each member's `/version` into
/// the cache that `status_patch` reads. Every replica runs it, like the
/// shard-usage loop; only the leader-gated apply loop writes the status.
pub(super) fn spawn_member_version_loop(client: Client) {
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
                        let key = super::shard_usage::cache_key(&lumen);
                        let previous = cached_member_reports(&key).unwrap_or_default();
                        let pods = pod_version_urls(&lumen);
                        let reports =
                            collect_member_reports(&http, &pods, &previous, now_secs()).await;
                        if !reports.is_empty() {
                            record_member_reports(&key, reports);
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "member version scrape: list Lumen failed");
                }
            }
            tokio::time::sleep(MEMBER_VERSION_POLL_INTERVAL).await;
        }
    });
}
