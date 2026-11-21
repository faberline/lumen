//! Member version reports (#248 slice 1a): an in-process cache of what each
//! fleet member reported, keyed like `shard_usage`. `status_patch` reads it
//! synchronously; nothing here does I/O.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use kube::ResourceExt;

use crate::domain::lumen_spec::member_versions::MemberVersionReport;
use crate::domain::lumen_spec::Lumen;

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
