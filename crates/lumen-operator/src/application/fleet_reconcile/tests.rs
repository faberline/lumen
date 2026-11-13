//! One fleet convergence pass, against an in-memory [`FleetControl`].

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use anyhow::anyhow;
use async_trait::async_trait;
use serde_json::{json, Value};

use super::control::FleetControl;
use super::converge;
use crate::domain::lumen_fleet::{FleetInstance, LumenFleet, LumenFleetSpec, PrunePolicy};
use crate::domain::lumen_spec::LumenSpec;

fn defaults() -> LumenSpec {
    serde_json::from_value(json!({
        "image": "ghcr.io/acme/lumen:0.4.27",
        "serving": { "cpu": "2", "memory": "8Gi", "raftStorage": "50Gi" },
        "placement": { "nodeSelector": { "cloud.google.com/gke-nodepool": "lumen-ssd" } },
    }))
    .expect("defaults parse")
}

fn fleet(instances: Vec<FleetInstance>, prune_policy: PrunePolicy) -> LumenFleet {
    let mut fleet = LumenFleet::new(
        "search",
        LumenFleetSpec {
            defaults: defaults(),
            instances,
            prune_policy,
        },
    );
    fleet.metadata.generation = Some(1);
    fleet
}

/// An entry with no overrides: the `Lumen` is named after the fleet.
fn entry(namespace: &str) -> FleetInstance {
    FleetInstance {
        namespace: namespace.to_string(),
        name: None,
        spec: None,
    }
}

/// An in-memory cluster. The configuration is set before the pass; `calls`
/// and `status` record what the pass did.
#[derive(Default)]
struct FakeControl {
    namespaces: HashSet<String>,
    /// Live `Lumen`s by (namespace, name); the value is the `lumen.dev/fleet` label.
    live: HashMap<(String, String), Option<String>>,
    /// The error text for one method on one namespace (`""` for the status patch).
    errors: HashMap<(&'static str, String), String>,
    /// What `list_fleet_lumens` returns, as (namespace, name).
    listed: Vec<(String, String)>,
    /// When set, `list_fleet_lumens` fails with this text.
    fail_list: Option<String>,
    calls: Mutex<Vec<String>>,
    status: Mutex<Option<Value>>,
}

impl FakeControl {
    fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespaces.insert(namespace.to_string());
        self
    }

    fn with_live(mut self, namespace: &str, name: &str, label: Option<&str>) -> Self {
        self.live.insert(
            (namespace.to_string(), name.to_string()),
            label.map(String::from),
        );
        self
    }

    fn with_listed(mut self, namespace: &str, name: &str) -> Self {
        self.listed.push((namespace.to_string(), name.to_string()));
        self
    }

    fn fail(mut self, method: &'static str, key: &str, text: &str) -> Self {
        self.errors
            .insert((method, key.to_string()), text.to_string());
        self
    }

    fn check(&self, method: &'static str, key: &str) -> anyhow::Result<()> {
        match self.errors.get(&(method, key.to_string())) {
            Some(text) => Err(anyhow!(text.clone())),
            None => Ok(()),
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn status(&self) -> Value {
        self.status
            .lock()
            .unwrap()
            .clone()
            .expect("status was written")
    }
}

#[async_trait]
impl FleetControl for FakeControl {
    async fn namespace_exists(&self, namespace: &str) -> anyhow::Result<bool> {
        self.check("namespace_exists", namespace)?;
        Ok(self.namespaces.contains(namespace))
    }

    async fn lumen_fleet_label(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<Option<String>>> {
        self.check("lumen_fleet_label", namespace)?;
        Ok(self
            .live
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn create_lumen(&self, body: Value) -> anyhow::Result<()> {
        let namespace = body["metadata"]["namespace"].as_str().unwrap_or_default();
        let name = body["metadata"]["name"].as_str().unwrap_or_default();
        self.check("create_lumen", namespace)?;
        self.record(format!("create {namespace}/{name}"));
        Ok(())
    }

    async fn apply_lumen(&self, namespace: &str, name: &str, _body: Value) -> anyhow::Result<()> {
        self.check("apply_lumen", namespace)?;
        self.record(format!("apply {namespace}/{name}"));
        Ok(())
    }

    async fn list_fleet_lumens(&self, _fleet: &str) -> anyhow::Result<Vec<(String, String)>> {
        if let Some(text) = &self.fail_list {
            return Err(anyhow!(text.clone()));
        }
        Ok(self.listed.clone())
    }

    async fn delete_lumen(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.check("delete_lumen", namespace)?;
        self.record(format!("delete {namespace}/{name}"));
        Ok(())
    }

    async fn patch_fleet_status(&self, _fleet: &str, status: Value) -> anyhow::Result<()> {
        self.check("patch_fleet_status", "")?;
        *self.status.lock().unwrap() = Some(status);
        Ok(())
    }
}

/// The entry states, in the order the pass wrote them.
fn states(status: &Value) -> Vec<String> {
    status["status"]["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["state"].as_str().expect("state").to_string())
        .collect()
}

/// The message of the entry in `namespace` (empty when the entry has none).
fn message_of(status: &Value, namespace: &str) -> String {
    status["status"]["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["namespace"] == namespace)
        .map(|e| e["message"].as_str().unwrap_or_default().to_string())
        .expect("an entry for this namespace")
}

#[tokio::test]
async fn one_failing_entry_does_not_stop_the_others() {
    let control = FakeControl::default()
        .with_namespace("team-denied")
        .fail("lumen_fleet_label", "team-denied", "403 forbidden")
        .with_namespace("team-conflict")
        .with_live("team-conflict", "search", Some("search"))
        .fail("apply_lumen", "team-conflict", "409 conflict")
        .fail("namespace_exists", "team-obs", "etcd timeout")
        .with_namespace("team-ok")
        .with_namespace("team-live")
        .with_live("team-live", "search", Some("search"));
    let fleet = fleet(
        vec![
            entry("team-missing"),
            entry("team-denied"),
            entry("team-conflict"),
            entry("team-obs"),
            entry("team-ok"),
            entry("team-live"),
        ],
        PrunePolicy::Retain,
    );

    converge(&control, &fleet).await.unwrap();

    let status = control.status();
    assert_eq!(
        states(&status),
        [
            "NamespaceMissing",
            "ObserveFailed",
            "ApplyFailed",
            "ObserveFailed",
            "Created",
            "Applied",
        ]
    );
    assert!(message_of(&status, "team-denied").contains("403 forbidden"));
    assert!(message_of(&status, "team-obs").contains("etcd timeout"));
    assert!(message_of(&status, "team-conflict").contains("409 conflict"));
    assert_eq!(status["status"]["appliedInstances"], json!(2));
    assert_eq!(status["status"]["desiredInstances"], json!(6));
}

/// Negative control: a failed read on the first entry used to end the pass
/// with `?`, so the healthy `team-ok` after it was never created.
#[tokio::test]
async fn a_failing_entry_first_does_not_skip_the_later_healthy_one() {
    let control = FakeControl::default()
        .with_namespace("team-denied")
        .fail("lumen_fleet_label", "team-denied", "403 forbidden")
        .with_namespace("team-ok");
    let fleet = fleet(
        vec![entry("team-denied"), entry("team-ok")],
        PrunePolicy::Retain,
    );

    converge(&control, &fleet).await.unwrap();

    assert!(control
        .calls()
        .contains(&"create team-ok/search".to_string()));
    assert_eq!(states(&control.status()), ["ObserveFailed", "Created"]);
}

#[tokio::test]
async fn an_observe_failure_never_prunes_a_declared_instance() {
    let control = FakeControl::default()
        .with_namespace("team-denied")
        .fail("lumen_fleet_label", "team-denied", "read timeout")
        .with_listed("team-denied", "search");
    let fleet = fleet(vec![entry("team-denied")], PrunePolicy::Delete);

    converge(&control, &fleet).await.unwrap();

    let status = control.status();
    assert!(!control
        .calls()
        .contains(&"delete team-denied/search".to_string()));
    assert_eq!(states(&status), ["ObserveFailed"]);
}

#[tokio::test]
async fn a_failed_prune_list_prunes_nothing_and_still_writes_status() {
    let control = FakeControl {
        fail_list: Some("list denied".to_string()),
        ..FakeControl::default()
    }
    .with_namespace("team-ok")
    .with_listed("old", "search");
    let fleet = fleet(vec![entry("team-ok")], PrunePolicy::Delete);

    converge(&control, &fleet).await.unwrap();

    assert!(control.calls().iter().all(|c| !c.starts_with("delete")));
    let status = control.status();
    assert_eq!(states(&status), ["Created"]);
    let message = status["status"]["message"].as_str().expect("message");
    assert!(message.contains("prune skipped"), "message: {message}");
    assert!(message.contains("list denied"), "message: {message}");
}

#[tokio::test]
async fn a_failed_status_patch_is_the_only_error() {
    let failing = FakeControl::default().with_namespace("team-ok").fail(
        "patch_fleet_status",
        "",
        "status denied",
    );
    let err = converge(
        &failing,
        &fleet(vec![entry("team-ok")], PrunePolicy::Retain),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("status denied"), "error: {err}");

    // A failed read on an entry is recorded in status; it does not fail the pass.
    let healthy = FakeControl::default().with_namespace("team-denied").fail(
        "lumen_fleet_label",
        "team-denied",
        "403 forbidden",
    );
    converge(
        &healthy,
        &fleet(vec![entry("team-denied")], PrunePolicy::Retain),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_failed_delete_stays_on_its_own_entry() {
    let control = FakeControl::default()
        .with_listed("o1", "search")
        .with_listed("o2", "search")
        .fail("delete_lumen", "o1", "delete denied");
    let fleet = fleet(vec![], PrunePolicy::Delete);

    converge(&control, &fleet).await.unwrap();

    let status = control.status();
    assert_eq!(states(&status), ["ApplyFailed", "Pruned"]);
    assert!(message_of(&status, "o1").contains("delete denied"));
    assert_eq!(control.calls(), vec!["delete o2/search".to_string()]);
}
