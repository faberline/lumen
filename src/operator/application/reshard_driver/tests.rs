use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::checkpoint::checkpoint_shard;
use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::{drive_tick, DriveOutcome};
use crate::operator::domain::lumen_spec::serving::ServingSpec;
use crate::operator::domain::lumen_spec::status::{LumenReshardStatus, LumenStatus};
use crate::operator::domain::lumen_spec::topology::{
    ReshardPhase, ReshardPolicy, ReshardWorkflowSpec, ShardMapSpec,
};
use crate::operator::domain::lumen_spec::{Lumen, LumenSpec};

fn spec(shard_count: u32, replicas_per_shard: u32, max_shard_bytes: Option<u64>) -> LumenSpec {
    LumenSpec {
        image: "lumen:latest".into(),
        image_pull_policy: None,
        placement: Default::default(),
        shard_count,
        shard_map: ShardMapSpec {
            version: 0,
            virtual_bucket_count: 8,
            assignments: Vec::new(),
        },
        replicas_per_shard,
        voter_count: replicas_per_shard,
        log_format: Default::default(),
        log_level: None,
        auth: Default::default(),
        serving: ServingSpec::default(),
        reshard_policy: ReshardPolicy {
            max_shard_bytes,
            ..Default::default()
        },
        observability: false,
        network_policy: false,
        admission: None,
        service_account_name: None,
        service_account_annotations: BTreeMap::new(),
        peer_tls_secret: None,
        serving_tls_secret: None,
        body_limit_bytes: None,
    }
}

fn lumen_with(spec: LumenSpec, status: Option<LumenStatus>) -> Lumen {
    let mut lumen = Lumen::new("search", spec);
    lumen.metadata.namespace = Some("acme".to_string());
    lumen.status = status;
    lumen
}

fn status_with_blocking(condition: &str) -> LumenStatus {
    LumenStatus {
        reshard: LumenReshardStatus {
            blocking_conditions: vec![condition.to_string()],
            // R5's freshness gate requires this to match the CR's
            // current `spec.shard_map.version`; every fixture built with
            // `spec()` hardcodes `shard_map.version: 0`, so `Some(0)`
            // here models a status write that was actually fresh at the
            // scenario's map version, not a value that happens to fail
            // the new check by fixture omission.
            usage_measured_at_map_version: Some(0),
            ..Default::default()
        },
        ..Default::default()
    }
}

// ---- drive_tick state machine (fake control, no real k8s) ---------

/// In-memory [`ClusterControl`]: records the last patch applied to a
/// shared `Lumen` snapshot and simulates a StatefulSet's ready-replica
/// count. No HTTP admin calls are faked here — those go through a real
/// [`axum_test`] server in the integration test below; this fake only
/// covers the k8s-shaped operations `drive_tick` needs before/around
/// them.
struct FakeControl {
    ready_replicas: AtomicI64,
    last_patch: Mutex<Option<serde_json::Value>>,
    restart_calls: AtomicI64,
}

impl FakeControl {
    fn new(ready_replicas: i64) -> Self {
        Self {
            ready_replicas: AtomicI64::new(ready_replicas),
            last_patch: Mutex::new(None),
            restart_calls: AtomicI64::new(0),
        }
    }
}

#[async_trait]
impl ClusterControl for FakeControl {
    async fn patch_spec(&self, _ns: &str, _name: &str, patch: serde_json::Value) -> Result<()> {
        *self.last_patch.lock().unwrap() = Some(patch);
        Ok(())
    }

    async fn statefulset_ready_replicas(&self, _ns: &str, _name: &str) -> Result<i64> {
        Ok(self.ready_replicas.load(Ordering::SeqCst))
    }

    async fn trigger_rolling_restart(&self, _ns: &str, _name: &str) -> Result<()> {
        self.restart_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn admin_token(&self, _ns: &str, _lumen: &Lumen) -> Result<Option<ProjectedToken>> {
        Ok(None)
    }

    fn shard_base_url(&self, _ns: &str, _name: &str, shard: u32) -> String {
        format!("http://unused-in-this-test.invalid/shard-{shard}")
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::new()
}

// ---- #1396 AC3: checkpoint_shard requires persisted == true --------

#[tokio::test]
async fn checkpoint_shard_blocked_when_response_reports_persisted_false() {
    // A 200 with `persisted: false` is the exact shape `admin_checkpoint`
    // returns when the shard has no durable checkpoint sink configured
    // (the vacuous NoopCheckpoint case) — this must never be treated as
    // a satisfied durability gate.
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/checkpoint"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(json!({ "persisted": false })),
        )
        .mount(&server)
        .await;
    let result = checkpoint_shard(&http_client(), &server.uri(), None).await;
    assert!(
        result.is_err(),
        "persisted: false must not satisfy the checkpoint gate"
    );
}

#[tokio::test]
async fn checkpoint_shard_blocked_when_response_omits_persisted_key() {
    // A malformed/older response with no `persisted` key at all must
    // fail closed (defaults to not-durable), not be treated as success.
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/checkpoint"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let result = checkpoint_shard(&http_client(), &server.uri(), None).await;
    assert!(
        result.is_err(),
        "a response missing the persisted key must fail closed"
    );
}

#[tokio::test]
async fn checkpoint_shard_ok_when_response_reports_persisted_true() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/admin/checkpoint"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(json!({ "persisted": true })),
        )
        .mount(&server)
        .await;
    let result = checkpoint_shard(&http_client(), &server.uri(), None).await;
    assert!(
        result.is_ok(),
        "persisted: true must satisfy the checkpoint gate: {result:?}"
    );
}

#[tokio::test]
async fn drive_tick_complete_with_no_trigger_is_noop() {
    let lumen = lumen_with(spec(1, 1, Some(1_000_000)), Some(LumenStatus::default()));
    let control = FakeControl::new(0);
    let outcome = drive_tick(&control, &http_client(), &lumen).await;
    assert_eq!(
        outcome,
        DriveOutcome::NoOp("no crossed threshold, unsupported topology, or maxShards reached")
    );
    assert!(control.last_patch.lock().unwrap().is_none());
}

#[tokio::test]
async fn drive_tick_starts_split_on_crossed_threshold() {
    let lumen = lumen_with(
        spec(2, 1, Some(1_000_000)),
        Some(status_with_blocking("prepareThresholdCrossed")),
    );
    let control = FakeControl::new(0);
    let outcome = drive_tick(&control, &http_client(), &lumen).await;
    assert_eq!(
        outcome,
        DriveOutcome::StartedSplit {
            target_shard_count: 3
        }
    );
    let patch = control.last_patch.lock().unwrap().clone().unwrap();
    assert_eq!(patch["spec"]["shardCount"], json!(3));
    assert_eq!(
        patch["spec"]["reshardPolicy"]["workflow"]["phase"],
        json!("PrepareSplit")
    );
    assert_eq!(
        patch["spec"]["reshardPolicy"]["workflow"]["targetShardCount"],
        json!(3)
    );
}

#[tokio::test]
async fn drive_tick_prepare_split_waits_for_new_pod() {
    let mut s = spec(3, 1, Some(1_000_000));
    s.reshard_policy.workflow = ReshardWorkflowSpec {
        phase: ReshardPhase::PrepareSplit,
        target_shard_count: Some(3),
        ..Default::default()
    };
    let lumen = lumen_with(s, None);
    // Only 2 of the 3 desired pods are ready yet.
    let control = FakeControl::new(2);
    let outcome = drive_tick(&control, &http_client(), &lumen).await;
    assert_eq!(
        outcome,
        DriveOutcome::WaitingForNewShard {
            target_shard_count: 3
        }
    );
    assert!(control.last_patch.lock().unwrap().is_none());
}

#[tokio::test]
async fn drive_tick_prepare_split_advances_once_new_pod_ready() {
    let mut s = spec(3, 1, Some(1_000_000));
    s.reshard_policy.workflow = ReshardWorkflowSpec {
        phase: ReshardPhase::PrepareSplit,
        target_shard_count: Some(3),
        ..Default::default()
    };
    let lumen = lumen_with(s, None);
    let control = FakeControl::new(3);
    let outcome = drive_tick(&control, &http_client(), &lumen).await;
    assert_eq!(outcome, DriveOutcome::AdvancedToSplitting);
    let patch = control.last_patch.lock().unwrap().clone().unwrap();
    assert_eq!(
        patch["spec"]["reshardPolicy"]["workflow"]["phase"],
        json!("Splitting")
    );
}

#[tokio::test]
async fn drive_tick_resumable_after_simulated_restart_mid_prepare_split() {
    // AC2 (narrowed to the k8s-facing half): a driver restart mid
    // PrepareSplit re-derives the exact same wait/advance decision from
    // the persisted CR alone — no in-process state survives between the
    // two calls below (a fresh FakeControl each time simulates a fresh
    // process).
    let mut s = spec(3, 1, Some(1_000_000));
    s.reshard_policy.workflow = ReshardWorkflowSpec {
        phase: ReshardPhase::PrepareSplit,
        target_shard_count: Some(3),
        ..Default::default()
    };
    let lumen = lumen_with(s, None);

    let before_restart = FakeControl::new(2);
    assert_eq!(
        drive_tick(&before_restart, &http_client(), &lumen).await,
        DriveOutcome::WaitingForNewShard {
            target_shard_count: 3
        }
    );

    // "Restart": brand-new control + a freshly-deserialized-shaped Lumen
    // (same spec/status values, simulating a re-fetch from the API
    // server), new pod now ready.
    let lumen_after_restart = lumen_with(lumen.spec.clone(), lumen.status.clone());
    let after_restart = FakeControl::new(3);
    assert_eq!(
        drive_tick(&after_restart, &http_client(), &lumen_after_restart).await,
        DriveOutcome::AdvancedToSplitting
    );
}

mod fence;

mod oversize;

mod trigger;
