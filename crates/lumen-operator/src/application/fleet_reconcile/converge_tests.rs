//! One convergence pass against an in-memory `FleetControl`: a failure on one
//! entry must stay on that entry, and only a failed status write may end the
//! pass with an error.

use super::control::FleetControl;
use super::*;
use crate::domain::lumen_fleet::children::ORPHAN_RETAINED_MESSAGE;
use crate::domain::lumen_fleet::{
    FleetInstance, LumenFleet, LumenFleetSpec, PrunePolicy, RolloutPolicy,
};
use crate::domain::lumen_spec::LumenSpec;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

fn defaults() -> LumenSpec {
    serde_json::from_value(json!({
        "image": "ghcr.io/acme/lumen:0.4.27",
        "serving": { "cpu": "2", "memory": "8Gi", "raftStorage": "50Gi" },
    }))
    .expect("defaults parse")
}

fn fleet(instances: Vec<FleetInstance>, prune_policy: PrunePolicy) -> LumenFleet {
    let mut fleet = LumenFleet::new(
        "f",
        LumenFleetSpec {
            defaults: defaults(),
            instances,
            prune_policy,
            rollout_policy: RolloutPolicy::default(),
        },
    );
    fleet.metadata.generation = Some(1);
    fleet
}

fn instance_in(namespace: &str) -> FleetInstance {
    FleetInstance {
        namespace: namespace.into(),
        name: Some("lumen".into()),
        ..Default::default()
    }
}

#[derive(Default)]
struct State {
    namespaces: BTreeSet<String>,
    /// `(namespace, name)` -> the live object's `lumen.dev/fleet` label.
    live: BTreeMap<(String, String), Option<String>>,
    /// `(namespace, name)` -> the whole live `Lumen` as JSON.
    lives: BTreeMap<(String, String), Value>,
    /// `(namespace, name)` -> the live `Lumen`'s spec.
    specs: BTreeMap<(String, String), Value>,
    listed: Vec<(String, String)>,
    /// `(method, key)` -> the error text the call returns.
    failures: BTreeMap<(String, String), String>,
    calls: Vec<String>,
    patches: Vec<Value>,
}

#[derive(Default)]
struct FakeControl {
    state: Mutex<State>,
}

impl FakeControl {
    fn with_namespace(self, namespace: &str) -> Self {
        self.state
            .lock()
            .unwrap()
            .namespaces
            .insert(namespace.into());
        self
    }

    fn with_live(self, namespace: &str, name: &str, label: Option<&str>) -> Self {
        self.state
            .lock()
            .unwrap()
            .live
            .insert((namespace.into(), name.into()), label.map(str::to_string));
        self
    }

    fn with_spec(self, namespace: &str, name: &str, spec: Value) -> Self {
        self.state
            .lock()
            .unwrap()
            .specs
            .insert((namespace.to_string(), name.to_string()), spec);
        self
    }

    fn with_live_json(self, namespace: &str, name: &str, lumen: Value) -> Self {
        self.state
            .lock()
            .unwrap()
            .lives
            .insert((namespace.to_string(), name.to_string()), lumen);
        self
    }

    fn with_listed(self, namespace: &str, name: &str) -> Self {
        self.state
            .lock()
            .unwrap()
            .listed
            .push((namespace.into(), name.into()));
        self
    }

    fn fail(self, method: &str, key: &str, text: &str) -> Self {
        self.state
            .lock()
            .unwrap()
            .failures
            .insert((method.into(), key.into()), text.into());
        self
    }

    fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn patches(&self) -> Vec<Value> {
        self.state.lock().unwrap().patches.clone()
    }

    /// The injected failure for this call, if any.
    fn check(&self, method: &str, key: &str) -> anyhow::Result<()> {
        match self
            .state
            .lock()
            .unwrap()
            .failures
            .get(&(method.to_string(), key.to_string()))
        {
            Some(text) => Err(anyhow::anyhow!(text.clone())),
            None => Ok(()),
        }
    }

    fn record(&self, call: String) {
        self.state.lock().unwrap().calls.push(call);
    }
}

#[async_trait]
impl FleetControl for FakeControl {
    async fn namespace_exists(&self, namespace: &str) -> anyhow::Result<bool> {
        self.check("namespace_exists", namespace)?;
        Ok(self.state.lock().unwrap().namespaces.contains(namespace))
    }

    async fn lumen_fleet_label(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.check("lumen_fleet_label", &format!("{namespace}/{name}"))?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .live
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn lumen_spec(&self, namespace: &str, name: &str) -> anyhow::Result<Option<Value>> {
        self.check("lumen_spec", &format!("{namespace}/{name}"))?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .specs
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn lumen_live(&self, namespace: &str, name: &str) -> anyhow::Result<Option<Value>> {
        self.check("lumen_live", &format!("{namespace}/{name}"))?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .lives
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn create_lumen(&self, body: Value) -> anyhow::Result<()> {
        let namespace = body["metadata"]["namespace"].as_str().unwrap_or_default();
        let name = body["metadata"]["name"].as_str().unwrap_or_default();
        self.check("create_lumen", &format!("{namespace}/{name}"))?;
        self.record(format!("create:{namespace}/{name}"));
        Ok(())
    }

    async fn apply_lumen(&self, namespace: &str, name: &str, _body: Value) -> anyhow::Result<()> {
        self.check("apply_lumen", &format!("{namespace}/{name}"))?;
        self.record(format!("apply:{namespace}/{name}"));
        Ok(())
    }

    async fn list_fleet_lumens(&self, _fleet: &str) -> anyhow::Result<Vec<(String, String)>> {
        self.check("list_fleet_lumens", "")?;
        Ok(self.state.lock().unwrap().listed.clone())
    }

    async fn delete_lumen(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.check("delete_lumen", &format!("{namespace}/{name}"))?;
        self.record(format!("delete:{namespace}/{name}"));
        Ok(())
    }

    async fn patch_fleet_status(&self, _fleet: &str, patch: Value) -> anyhow::Result<()> {
        self.check("patch_fleet_status", "")?;
        self.state.lock().unwrap().patches.push(patch);
        Ok(())
    }
}

/// The entry states of the one status patch, in order.
fn states(patch: &Value) -> Vec<String> {
    patch["status"]["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|entry| entry["state"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// The message of the entry in `namespace`, or empty when there is none.
fn message_of(patch: &Value, namespace: &str) -> String {
    patch["status"]["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|entry| entry["namespace"] == namespace)
        .map(|entry| entry["message"].as_str().unwrap_or_default().to_string())
        .unwrap_or_default()
}

#[tokio::test]
async fn one_failing_entry_does_not_stop_the_others() {
    let fake = FakeControl::default()
        .with_namespace("ok")
        .with_namespace("denied")
        .with_namespace("conflict")
        .with_namespace("observe")
        .with_live("conflict", "lumen", Some("f"))
        .fail("lumen_fleet_label", "denied/lumen", "forbidden: 403")
        .fail("namespace_exists", "observe", "timeout reading namespace");
    let mut fleet = fleet(
        ["ok", "missing", "denied", "conflict", "observe"]
            .into_iter()
            .map(instance_in)
            .collect(),
        PrunePolicy::Retain,
    );
    fleet.spec.rollout_policy.max_concurrent = 5;

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        states(&patches[0]),
        [
            "Created",
            "NamespaceMissing",
            "ObserveFailed",
            "Waiting",
            "ObserveFailed"
        ]
    );
    assert!(message_of(&patches[0], "denied").contains("403"));
    assert!(message_of(&patches[0], "conflict").contains("waiting for ok/lumen"));
    assert!(message_of(&patches[0], "observe").contains("timeout"));
    assert_eq!(patches[0]["status"]["appliedInstances"], json!(1));
    assert_eq!(patches[0]["status"]["desiredInstances"], json!(5));
}

/// Negative control: before this change the first failing call ended the
/// pass, so the healthy entry after it got no create and no status.
#[tokio::test]
async fn a_failing_entry_first_does_not_skip_the_later_healthy_one() {
    let fake = FakeControl::default().with_namespace("good").fail(
        "namespace_exists",
        "bad",
        "timeout reading namespace",
    );
    let fleet = fleet(
        vec![instance_in("bad"), instance_in("good")],
        PrunePolicy::Retain,
    );

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["ObserveFailed", "Created"]);
    assert!(fake.calls().contains(&"create:good/lumen".to_string()));
    assert_eq!(patches[0]["status"]["appliedInstances"], json!(1));
}

#[tokio::test]
async fn an_observe_failure_never_prunes_a_declared_instance() {
    let fake = FakeControl::default()
        .with_namespace("bad")
        .with_listed("bad", "lumen")
        .fail("lumen_fleet_label", "bad/lumen", "forbidden: 403");
    let fleet = fleet(vec![instance_in("bad")], PrunePolicy::Delete);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["ObserveFailed"]);
    assert!(!fake.calls().iter().any(|call| call.starts_with("delete:")));
}

#[tokio::test]
async fn a_failed_prune_list_prunes_nothing_and_still_writes_status() {
    let fake =
        FakeControl::default()
            .with_namespace("ok")
            .fail("list_fleet_lumens", "", "list denied");
    let fleet = fleet(vec![instance_in("ok")], PrunePolicy::Delete);

    converge(&fake, &fleet).await.expect("pass completes");

    assert!(!fake.calls().iter().any(|call| call.starts_with("delete:")));
    let patches = fake.patches();
    assert_eq!(patches.len(), 1);
    assert_eq!(states(&patches[0]), ["Created"]);
    let message = patches[0]["status"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("prune skipped"), "message: {message}");
    assert!(message.contains("list denied"), "message: {message}");
}

#[tokio::test]
async fn a_failed_status_patch_is_the_only_error() {
    let fake =
        FakeControl::default()
            .with_namespace("ok")
            .fail("patch_fleet_status", "", "status denied");
    let fleet = fleet(vec![instance_in("ok")], PrunePolicy::Retain);

    let err = converge(&fake, &fleet)
        .await
        .expect_err("a failed status write ends the pass");
    assert!(err.to_string().contains("status denied"), "error: {err}");
}

#[tokio::test]
async fn a_failed_delete_stays_on_its_own_entry() {
    let fake = FakeControl::default()
        .with_namespace("ok")
        .with_listed("a", "x")
        .with_listed("b", "y")
        .fail("delete_lumen", "a/x", "delete denied");
    let fleet = fleet(vec![instance_in("ok")], PrunePolicy::Delete);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Created", "ApplyFailed", "Pruned"]);
    assert!(fake.calls().contains(&"delete:b/y".to_string()));
}

#[tokio::test]
async fn a_foreign_child_is_not_adopted_and_an_orphan_is_retained() {
    let fake = FakeControl::default()
        .with_namespace("foreign")
        .with_live("foreign", "lumen", Some("other"))
        .with_listed("old", "lumen");
    let fleet = fleet(vec![instance_in("foreign")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["NotAdopted", "Orphaned"]);
    assert!(message_of(&patches[0], "foreign").contains("left untouched"));
    assert_eq!(message_of(&patches[0], "old"), ORPHAN_RETAINED_MESSAGE);
    assert!(!fake
        .calls()
        .iter()
        .any(|call| call.starts_with("apply:") || call.starts_with("delete:")));
}

fn condition(status: &Value, kind: &str) -> Value {
    status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == kind)
        .cloned()
        .expect("condition present")
}

#[tokio::test]
async fn a_ready_child_makes_the_fleet_ready() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"))
        .with_live_json(
            "a",
            "lumen",
            json!({"status": {"phase": "Ready", "conditions": [{"type": "Ready", "status": "True"}]}}),
        );
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let status = &fake.patches()[0]["status"];
    assert_eq!(status["readyInstances"], 1);
    assert_eq!(condition(status, "Ready")["status"], "True");
    assert_eq!(condition(status, "Degraded")["status"], "False");
    assert_eq!(condition(status, "Orphaned")["status"], "False");
}

#[tokio::test]
async fn a_degraded_child_is_reported_and_blocks_ready() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"))
        .with_live_json(
            "a",
            "lumen",
            json!({"status": {"phase": "Degraded", "conditions": [{"type": "Ready", "status": "False"}]}}),
        );
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let status = &fake.patches()[0]["status"];
    assert_eq!(status["degradedInstances"], 1);
    let ready = condition(status, "Ready");
    assert_eq!(ready["status"], "False");
    assert_eq!(ready["message"], "a/lumen: Degraded");
    assert_eq!(condition(status, "Degraded")["status"], "True");
}

#[tokio::test]
async fn a_retained_orphan_does_not_fail_ready() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"))
        .with_listed("old", "lumen")
        .with_live_json(
            "a",
            "lumen",
            json!({"status": {"phase": "Ready", "conditions": [{"type": "Ready", "status": "True"}]}}),
        );
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let status = &fake.patches()[0]["status"];
    assert_eq!(condition(status, "Ready")["status"], "True");
    assert_eq!(condition(status, "Orphaned")["status"], "True");
    assert_eq!(status["readyInstances"], 1);
}

#[tokio::test]
async fn a_failed_child_read_is_not_ready_and_does_not_fail_the_pass() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"))
        .fail("lumen_live", "a/lumen", "forbidden: 403");
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let status = &fake.patches()[0]["status"];
    let ready = condition(status, "Ready");
    assert_eq!(ready["status"], "False");
    assert_eq!(ready["message"], "a/lumen: no status yet");
    assert_eq!(status["notReadyInstances"], 1);
}

#[tokio::test]
async fn a_child_with_no_status_is_not_ready() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"));
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let status = &fake.patches()[0]["status"];
    let ready = condition(status, "Ready");
    assert_eq!(ready["status"], "False");
    assert_eq!(ready["message"], "a/lumen: no status yet");
    assert_eq!(status["notReadyInstances"], 1);
}

/// A live `Lumen` as JSON with the given generation, observed generation and Ready flag.
fn lumen_json(generation: i64, observed: i64, ready: bool) -> Value {
    json!({
        "metadata": { "generation": generation },
        "status": {
            "observedGeneration": observed,
            "conditions": [{ "type": "Ready", "status": if ready { "True" } else { "False" } }]
        }
    })
}

/// Three declared namespaces, each with a live `Lumen` of this fleet, so each
/// instance needs an apply.
fn three_live() -> FakeControl {
    ["a", "b", "c"]
        .into_iter()
        .fold(FakeControl::default(), |fake, ns| {
            fake.with_namespace(ns).with_live(ns, "lumen", Some("f"))
        })
}

#[tokio::test]
async fn a_default_fleet_applies_one_instance_per_pass() {
    let fake = three_live();
    let fleet = fleet(["a", "b", "c"].map(instance_in).into(), PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Held", "Held"]);
    assert_eq!(fake.calls(), ["apply:a/lumen"]);
    assert!(message_of(&patches[0], "b").contains("rollout slot"));
}

#[tokio::test]
async fn a_settled_instance_takes_no_slot() {
    let fake = three_live().with_spec("a", "lumen", serde_json::to_value(defaults()).unwrap());
    let fleet = fleet(["a", "b", "c"].map(instance_in).into(), PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Applied", "Held"]);
    assert_eq!(fake.calls(), ["apply:a/lumen", "apply:b/lumen"]);
}

#[tokio::test]
async fn a_paused_fleet_makes_no_call() {
    let fake = three_live().with_spec("a", "lumen", serde_json::to_value(defaults()).unwrap());
    let mut fleet = fleet(["a", "b", "c"].map(instance_in).into(), PrunePolicy::Retain);
    fleet.spec.rollout_policy.paused = true;

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Held", "Held", "Held"]);
    assert!(fake.calls().is_empty(), "calls: {:?}", fake.calls());
    assert!(message_of(&patches[0], "a").contains("paused"));
}

#[tokio::test]
async fn a_paused_fleet_never_prunes() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_listed("old", "lumen");
    let mut fleet = fleet(vec![instance_in("a")], PrunePolicy::Delete);
    fleet.spec.rollout_policy.paused = true;

    converge(&fake, &fleet).await.expect("pass completes");

    assert!(!fake.calls().iter().any(|call| call.starts_with("delete:")));
    let patches = fake.patches();
    assert_eq!(
        patches[0]["status"]["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .find(|entry| entry["namespace"] == "old")
            .map(|entry| entry["state"].clone()),
        Some(json!("Orphaned"))
    );
}

#[tokio::test]
async fn a_held_instance_is_not_pruned_or_reported_as_orphan() {
    let fake = three_live().with_listed("b", "lumen");
    let fleet = fleet(["a", "b", "c"].map(instance_in).into(), PrunePolicy::Delete);

    converge(&fake, &fleet).await.expect("pass completes");

    assert!(!fake.calls().iter().any(|call| call.starts_with("delete:")));
    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Held", "Held"]);
}

#[tokio::test]
async fn a_failed_spec_read_is_an_observe_failure() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_live("a", "lumen", Some("f"))
        .fail("lumen_spec", "a/lumen", "forbidden: 403");
    let fleet = fleet(vec![instance_in("a")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["ObserveFailed"]);
    assert!(!fake.calls().iter().any(|call| call.starts_with("apply:")));
}

/// An apply failure stays on its own entry (the 409 coverage that the test
/// above no longer carries, since the gate holds that entry).
#[tokio::test]
async fn an_apply_failure_stays_on_its_own_entry() {
    let fake = FakeControl::default()
        .with_namespace("conflict")
        .with_live("conflict", "lumen", Some("f"))
        .fail("apply_lumen", "conflict/lumen", "conflict: 409");
    let fleet = fleet(vec![instance_in("conflict")], PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["ApplyFailed"]);
    assert!(message_of(&patches[0], "conflict").contains("409"));
    assert_eq!(patches[0]["status"]["appliedInstances"], json!(0));
}

/// Two namespaces `a` and `b`, both with a live `Lumen` of this fleet. `a` is
/// settled (it takes no slot); `b` needs a write.
fn settled_a_and_writing_b() -> FakeControl {
    FakeControl::default()
        .with_namespace("a")
        .with_namespace("b")
        .with_live("a", "lumen", Some("f"))
        .with_live("b", "lumen", Some("f"))
        .with_spec("a", "lumen", serde_json::to_value(defaults()).unwrap())
}

#[tokio::test]
async fn a_stale_child_holds_the_next_write() {
    let fake = settled_a_and_writing_b().with_live_json("a", "lumen", lumen_json(2, 1, true));
    let fleet = fleet(["a", "b"].map(instance_in).into(), PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Waiting"]);
    assert_eq!(fake.calls(), ["apply:a/lumen"]);
    let message = message_of(&patches[0], "b");
    assert!(message.contains("waiting for a/lumen"), "{message}");
    assert!(
        message.contains("stale generation (observed 1 of 2)"),
        "{message}"
    );
    assert_eq!(patches[0]["status"]["appliedInstances"], json!(1));
}

#[tokio::test]
async fn a_not_ready_child_holds_the_next_write() {
    let fake = settled_a_and_writing_b().with_live_json("a", "lumen", lumen_json(2, 2, false));
    let fleet = fleet(["a", "b"].map(instance_in).into(), PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Waiting"]);
    assert!(message_of(&patches[0], "b").contains("waiting for a/lumen: not Ready"));
}

#[tokio::test]
async fn a_caught_up_ready_child_lets_the_next_write_proceed() {
    let fake = settled_a_and_writing_b().with_live_json("a", "lumen", lumen_json(2, 2, true));
    let fleet = fleet(["a", "b"].map(instance_in).into(), PrunePolicy::Retain);

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Applied", "Applied"]);
    assert_eq!(fake.calls(), ["apply:a/lumen", "apply:b/lumen"]);
}

#[tokio::test]
async fn a_created_instance_holds_the_next_write() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_namespace("b");
    let mut fleet = fleet(["a", "b"].map(instance_in).into(), PrunePolicy::Retain);
    fleet.spec.rollout_policy.max_concurrent = 2;

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(states(&patches[0]), ["Created", "Waiting"]);
    assert_eq!(fake.calls(), ["create:a/lumen"]);
    assert!(message_of(&patches[0], "b").contains("waiting for a/lumen"));
    assert_eq!(patches[0]["status"]["appliedInstances"], json!(1));
}

#[tokio::test]
async fn a_waiting_entry_does_not_stop_later_skips_or_the_status_write() {
    let fake = FakeControl::default()
        .with_namespace("a")
        .with_namespace("c");
    let mut fleet = fleet(["a", "b", "c"].map(instance_in).into(), PrunePolicy::Retain);
    fleet.spec.rollout_policy.max_concurrent = 3;

    converge(&fake, &fleet).await.expect("pass completes");

    let patches = fake.patches();
    assert_eq!(patches.len(), 1);
    assert_eq!(
        states(&patches[0]),
        ["Created", "NamespaceMissing", "Waiting"]
    );
}
