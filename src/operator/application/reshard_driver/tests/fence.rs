use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::fence::set_write_fence;
use crate::operator::application::reshard_driver::migration::evict_old_shards;
use crate::operator::application::reshard_driver::tests::{http_client, lumen_with, spec};
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

// ---- #1443 AC4: set_write_fence partial-arm cleanup -----------------

/// Minimal control exposing exactly the shard URLs [`set_write_fence`]
/// needs; used only by the AC4 test below, which calls `set_write_fence`
/// directly rather than driving a full `drive_tick`.
pub(super) struct TwoShardFenceControl {
    pub(super) shard_urls: Vec<String>,
}

#[async_trait]
impl ClusterControl for TwoShardFenceControl {
    async fn patch_spec(&self, _ns: &str, _name: &str, _patch: serde_json::Value) -> Result<()> {
        unreachable!("not used by set_write_fence")
    }
    async fn statefulset_ready_replicas(&self, _ns: &str, _name: &str) -> Result<i64> {
        unreachable!("not used by set_write_fence")
    }
    async fn trigger_rolling_restart(&self, _ns: &str, _name: &str) -> Result<()> {
        unreachable!("not used by set_write_fence")
    }
    async fn admin_token(&self, _ns: &str, _lumen: &Lumen) -> Result<Option<ProjectedToken>> {
        Ok(None)
    }
    fn shard_base_url(&self, _ns: &str, _name: &str, shard: u32) -> String {
        self.shard_urls[shard as usize].clone()
    }
}

#[tokio::test]
async fn set_write_fence_clears_already_armed_shards_on_partial_failure() {
    // Shard A: a real endpoint that records every /admin/reshard:fence
    // call it receives — both the arm attempt and, if R4 works, the
    // best-effort clear triggered by shard B's failure.
    let shard_a = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/reshard:fence"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&shard_a)
        .await;

    // Shard B: a bound-then-closed port — nothing listens there, so
    // every call to it fails outright, simulating an unreachable shard
    // mid-arm.
    let dead_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);
    let shard_b_url = format!("http://{dead_addr}");

    let control = TwoShardFenceControl {
        shard_urls: vec![shard_a.uri(), shard_b_url],
    };
    let current = VirtualBucketShardMap::balanced(0, 8, 2).unwrap();
    let mut buckets = BTreeSet::new();
    buckets.insert(0u32);
    let lumen = lumen_with(spec(2, 1, None), None);

    let result = set_write_fence(
        &control,
        &http_client(),
        "acme",
        "search",
        &lumen,
        &current,
        &buckets,
        30,
    )
    .await;
    assert!(result.is_err(), "arm must surface shard B's failure");

    // Shard A must have received exactly 2 requests: the original arm,
    // then the best-effort clear triggered by shard B's failure — R4's
    // whole point is that shard A never stays fenced indefinitely just
    // because shard B was unreachable.
    let requests = shard_a
        .received_requests()
        .await
        .expect("wiremock request recording enabled");
    assert_eq!(
        requests.len(),
        2,
        "shard A must be armed once, then cleared once after shard B's arm failed"
    );
    let clear_body: serde_json::Value = requests[1].body_json().unwrap();
    assert_eq!(
        clear_body["buckets"].as_array().map(Vec::len),
        Some(0),
        "the second call to shard A must be a clear (empty buckets), not another arm"
    );
}

// ---- #1467 R3: evict_old_shards's in-loop fence re-arm --------------

/// A [`ClusterControl`] over a fixed list of already-bound shard URLs
/// with a test-controlled `write_fence_ttl_secs` — everything
/// [`evict_old_shards`]/[`maybe_rearm_fence`] needs, nothing more.
struct FenceRearmControl {
    pub(super) shard_urls: Vec<String>,
    ttl_secs: u64,
}

#[async_trait]
impl ClusterControl for FenceRearmControl {
    async fn patch_spec(&self, _ns: &str, _name: &str, _patch: serde_json::Value) -> Result<()> {
        unreachable!("not used by evict_old_shards")
    }
    async fn statefulset_ready_replicas(&self, _ns: &str, _name: &str) -> Result<i64> {
        unreachable!("not used by evict_old_shards")
    }
    async fn trigger_rolling_restart(&self, _ns: &str, _name: &str) -> Result<()> {
        unreachable!("not used by evict_old_shards")
    }
    async fn admin_token(&self, _ns: &str, _lumen: &Lumen) -> Result<Option<ProjectedToken>> {
        Ok(None)
    }
    fn shard_base_url(&self, _ns: &str, _name: &str, shard: u32) -> String {
        self.shard_urls[shard as usize].clone()
    }
    fn write_fence_ttl_secs(&self) -> u64 {
        self.ttl_secs
    }
}

/// #1467 R3: a slow, multi-shard eviction round (many old physical
/// shards, each `POST /admin/reshard:evict` round-trip taking real time)
/// must not run on a single fence arm taken once before the loop starts
/// — [`evict_old_shards`] re-checks/re-arms via [`maybe_rearm_fence`]
/// before *every* shard's evict call, not just at the phase boundary
/// immediately before this function is invoked. Proven by driving 3 old
/// shards through a real (mocked) eviction round with a tiny fence TTL
/// and an artificial per-call delay large enough that the un-refreshed
/// TTL fraction would already have lapsed by the final shard — the
/// number of `/admin/reshard:fence` arm requests observed across all 3
/// shards must reflect more than the single caller-side arm.
#[tokio::test]
async fn evict_old_shards_rearms_fence_mid_loop_across_slow_multi_shard_round() {
    let mut mock_shards = Vec::new();
    for _ in 0..3 {
        let mock = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/admin/reshard:fence"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&mock)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/admin/reshard:evict"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(json!({}))
                    .set_delay(Duration::from_millis(150)),
            )
            .mount(&mock)
            .await;
        mock_shards.push(mock);
    }
    let shard_urls: Vec<String> = mock_shards.iter().map(|m| m.uri()).collect();

    // ttl_secs=1 -> rearm_after = 250ms (FENCE_REARM_FRACTION=4).
    // `last_armed_at` starts at "just now" (as if the caller armed it
    // immediately before this call, matching the real phase-boundary
    // arm) so the first two iterations' pre-checks (elapsed ~0ms, then
    // ~150ms) skip re-arming, but by the third iteration's pre-check
    // (elapsed ~300ms) the 250ms fraction has lapsed and an in-loop
    // rearm must fire — proving it is *evict_old_shards's own loop*,
    // not just the caller, keeping the fence fresh across a slow round.
    let control = FenceRearmControl {
        shard_urls,
        ttl_secs: 1,
    };
    let current = VirtualBucketShardMap::balanced(0, 8, 3).unwrap();
    let target = VirtualBucketShardMap::balanced(1, 8, 3).unwrap();
    let mut moving_buckets = BTreeSet::new();
    moving_buckets.insert(0u32);
    let lumen = lumen_with(spec(3, 1, None), None);
    let mut last_armed_at = Instant::now();

    evict_old_shards(
        &control,
        &http_client(),
        "acme",
        "search",
        &lumen,
        &current,
        &target,
        Some(&moving_buckets),
        &mut last_armed_at,
    )
    .await
    .unwrap();

    let mut total_fence_calls = 0usize;
    let mut total_evict_calls = 0usize;
    for mock in &mock_shards {
        let requests = mock
            .received_requests()
            .await
            .expect("wiremock request recording enabled");
        total_fence_calls += requests
            .iter()
            .filter(|r| r.url.path() == "/admin/reshard:fence")
            .count();
        total_evict_calls += requests
            .iter()
            .filter(|r| r.url.path() == "/admin/reshard:evict")
            .count();
    }
    assert_eq!(
        total_evict_calls, 3,
        "every one of the 3 old shards must be evicted exactly once"
    );
    assert!(
        total_fence_calls > 0 && total_fence_calls % 3 == 0,
        "evict_old_shards's own loop must have re-armed at least once (a full arm round \
             is 3 fence calls, one per old shard) — this test never arms the fence itself \
             before calling evict_old_shards, so any fence calls observed at all are proof \
             of the in-loop rearm, got {total_fence_calls}"
    );
}
