//! The steps both cluster suites share, in order:
//! wait for the serving pods, prove where they run, PUT a collection, index
//! the 100k-doc fixture, search, duplicates, then the durable restart oracle
//! (checkpoint one document, replace the serving pod, read it back before
//! any new write, and prove the replacement accepts a write).
//!
//! Inputs, from the test Job (k8s/gke-test-job/base/job.yaml):
//!   E2E_ENDPOINT    the suite's Service, for example http://lumen:7373
//!   E2E_NAMESPACE   the Job's namespace (downward API)
//!   E2E_POD_NAME    the Job pod's name, in the oracle's document ids
//!   E2E_DOC_COUNT   fixture size; 100000 when unset
//! and the expected identity in `gke-test-env` (`identity`).

use std::time::Duration;

use serde_json::{json, Value};

use super::identity::{assert_on_pool, Identity, POOL_KEY, POOL_VALUE};
use super::support::{
    batch, batch_count, evidence, hits_for, is_dns_label, required, say, wait_ready, Api, Kube,
    Step, DOCS_PER_BATCH,
};

pub const PULL_SECRET: &str = "ghcr-pull";

/// One serving pod in both deploy paths: one shard, one replica.
pub const EXPECTED_STORAGE_PODS: usize = 1;

pub struct Suite {
    pub name: &'static str,
    /// Selects the serving pods of this deploy path and nothing else.
    pub label: String,
    pub kube: Kube,
    pub api: Api,
    pub identity: Identity,
    pub pod_name: String,
    pub docs: u64,
}

impl Suite {
    pub fn from_env(name: &'static str, label: &str) -> Self {
        let api = Api::from_env();
        let kube = Kube::from_env();
        let identity = Identity::from_env();
        let pod_name = required("E2E_POD_NAME");
        assert!(is_dns_label(&pod_name), "invalid E2E_POD_NAME: {pod_name}");
        let docs = match std::env::var("E2E_DOC_COUNT") {
            Ok(n) => n
                .parse::<u64>()
                .ok()
                .filter(|n| *n > 0)
                .unwrap_or_else(|| panic!("invalid E2E_DOC_COUNT: {n}")),
            Err(_) => 100_000,
        };
        Self {
            name,
            label: label.to_string(),
            kube,
            api,
            identity,
            pod_name,
            docs,
        }
    }

    pub fn step(&self, name: impl Into<String>) -> Step {
        Step::begin(self.name, name)
    }

    pub fn evidence(&self, step: &str, data: Value) {
        evidence(self.name, step, data);
    }
}

/// The containers in `list` named `name`.
pub fn named<'a>(list: &'a Value, name: &str) -> Vec<&'a Value> {
    list.as_array()
        .map(|l| l.iter().filter(|c| c["name"] == name).collect())
        .unwrap_or_default()
}

/// How many `env` entries of the containers named `container` set `name` to
/// `value`.
pub fn env_count(pod_spec: &Value, container: &str, name: &str, value: &str) -> usize {
    named(&pod_spec["containers"], container)
        .into_iter()
        .flat_map(|c| c["env"].as_array().into_iter().flatten())
        .filter(|e| e["name"] == name && e["value"] == value)
        .count()
}

/// The `name` mounts of the containers named `container`, with `readOnly`
/// spelled out when the API left it at its default.
pub fn mounts_named(pod_spec: &Value, container: &str, name: &str) -> Vec<Value> {
    named(&pod_spec["containers"], container)
        .into_iter()
        .flat_map(|c| c["volumeMounts"].as_array().into_iter().flatten())
        .filter(|m| m["name"] == name)
        .map(|m| {
            let mut m = m.clone();
            if m.get("readOnly").is_none() {
                m["readOnly"] = json!(false);
            }
            m
        })
        .collect()
}

/// A workload's pod template schedules onto pool-lumen-server and pulls from
/// GHCR.
pub fn assert_pool_placement(what: &str, workload: &Value) {
    let spec = &workload["spec"]["template"]["spec"];
    let selected = spec["nodeSelector"][POOL_KEY] == POOL_VALUE;
    let tolerated = spec["tolerations"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|t| {
            t["key"] == POOL_KEY
                && t["operator"] == "Equal"
                && t["value"] == POOL_VALUE
                && t["effect"] == "NoSchedule"
        });
    let pulls = spec["imagePullSecrets"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|s| s["name"] == PULL_SECRET);
    assert!(
        selected && tolerated && pulls,
        "{what} is missing the {POOL_KEY}={POOL_VALUE} selector, toleration, or {PULL_SECRET} pull secret"
    );
}

/// Steps 3 and 3a: the serving pods are Ready and were placed on
/// pool-lumen-server.
pub fn wait_serving(s: &Suite) {
    let step = s.step("3. wait for serving pods Ready");
    wait_ready(
        &s.kube,
        &s.label,
        EXPECTED_STORAGE_PODS,
        Duration::from_secs(600),
    );
    step.end();
    let step = s.step(format!("3a. assert serving pods on pool-{POOL_VALUE}"));
    assert_pods_on_pool(s);
    step.end();
}

pub fn assert_pods_on_pool(s: &Suite) {
    let pods = s.kube.pods(&s.label);
    let items = pods["items"].as_array().cloned().unwrap_or_default();
    assert!(!items.is_empty(), "no serving pods match {}", s.label);
    items.iter().for_each(assert_on_pool);
    s.evidence(
        "pods",
        Value::Array(
            items
                .iter()
                .map(|p| json!({"name": p["metadata"]["name"], "node": p["spec"]["nodeName"]}))
                .collect(),
        ),
    );
    say(format!(
        "   serving pods scheduled with {POOL_KEY}={POOL_VALUE}"
    ));
}

/// The Service only routes to Ready pods, and a replaced pod's endpoint takes
/// a moment to appear, so wait for /healthz through the Service itself.
async fn wait_service(s: &Suite) {
    assert!(
        s.api
            .wait_for("/healthz", Duration::from_secs(180), Duration::from_secs(2))
            .await,
        "{}/healthz did not answer within 180s",
        s.api.base
    );
    say(format!("   {} answers /healthz", s.api.base));
}

/// /version as the serving pod reports it, checked against the identity.
async fn record_version(s: &Suite, phase: &str) {
    let version = s.api.get("/version", Duration::from_secs(10)).await;
    s.identity.assert_version(&version, phase);
    s.evidence(&format!("version-{phase}"), version.clone());
    say(format!(
        "   /version ({phase}): {} {}",
        version["version"], version["git_sha"]
    ));
}

async fn index_exact(s: &Suite, external_id: &str, value: &str) {
    let body = json!({"items": [{"external_id": external_id, "field": "email", "value": value}]});
    s.api
        .post("/collections/users/index", &body, Duration::from_secs(30))
        .await;
}

async fn search_exact(s: &Suite, value: &str) -> Value {
    let body = json!({"query": {"term": {"field": "email", "value": value}}, "limit": 5});
    s.api
        .post("/collections/users/search", &body, Duration::from_secs(30))
        .await
}

fn serving_uids(s: &Suite) -> Vec<String> {
    let pods = s.kube.pods(&s.label);
    let mut uids: Vec<String> = pods["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| {
            p["metadata"]["uid"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    uids.sort();
    uids
}

/// Steps 4a to 5, through the suite's Service.
pub async fn exercise(s: &Suite) {
    let step = s.step(format!("4a. wait for {}", s.api.base));
    wait_service(s).await;
    step.end();

    let step = s.step("4a2. record /version");
    record_version(s, "before-restart").await;
    step.end();

    let step = s.step("4b. PUT /collections/users");
    let schema = json!({"fields": {"bio": {"type": "text"}, "email": {"type": "keyword"}}});
    s.api
        .put("/collections/users", &schema, Duration::from_secs(30))
        .await;
    step.end();

    let batches = batch_count(s.docs);
    let step = s.step(format!(
        "4d. POST /collections/users/index ({} docs, {batches} batches of ≤{DOCS_PER_BATCH} docs)",
        s.docs
    ));
    let mut items = 0;
    for b in 0..batches {
        if b % 20 == 0 || b + 1 == batches {
            say(format!("   batch {}/{batches}", b + 1));
        }
        let body = batch(s.docs, b);
        items += body["items"].as_array().map_or(0, Vec::len) as u64;
        s.api
            .post("/collections/users/index", &body, Duration::from_secs(120))
            .await;
    }
    assert_eq!(
        items,
        2 * s.docs,
        "the fixture must hold 2 items for each of {} docs",
        s.docs
    );
    step.end();

    let step = s.step("4e. search and duplicates");
    let search = json!({"query": {"match": {"field": "bio", "text": "engineer"}}, "limit": 20});
    let found = s
        .api
        .post(
            "/collections/users/search",
            &search,
            Duration::from_secs(30),
        )
        .await;
    let hits = found["hits"].as_array().map_or(0, Vec::len);
    s.evidence(
        "search",
        json!({"hits": hits, "first": found["hits"].get(0).cloned().unwrap_or(Value::Null)}),
    );
    say(format!("   search hits: {hits}"));
    assert!(hits > 0, "expected search hits > 0, got {hits}");
    // One shard, so duplicates is answered locally (a multi-shard topology
    // would assert the routed refusal instead).
    let dups = json!({"field": "email", "min_group_size": 2, "limit": 100});
    let found = s
        .api
        .post(
            "/collections/users/duplicates",
            &dups,
            Duration::from_secs(30),
        )
        .await;
    let groups = found["groups"].as_array().map_or(0, Vec::len);
    s.evidence(
        "duplicates",
        json!({"groups": groups, "first": found["groups"].get(0).cloned().unwrap_or(Value::Null)}),
    );
    say(format!("   duplicate groups: {groups}"));
    assert!(
        groups > 0,
        "expected at least one duplicate group, got {groups}"
    );
    step.end();

    let step = s.step("5. checkpoint, replace serving pod, and prove durable recovery");
    durable_restart_oracle(s).await;
    step.end();
}

/// The Service, not a NodePort, finds the replacement pod once it is Ready.
async fn durable_restart_oracle(s: &Suite) {
    let pre_id = format!("pre-restart-{}-{}", s.pod_name, s.name);
    let pre_value = format!("{pre_id}@example.invalid");
    let post_id = format!("post-restart-{}-{}", s.pod_name, s.name);
    let post_value = format!("{post_id}@example.invalid");

    index_exact(s, &pre_id, &pre_value).await;
    let checkpoint = s
        .api
        .post("/admin/checkpoint", &json!({}), Duration::from_secs(120))
        .await;
    assert!(
        checkpoint["persisted"] == true,
        "/admin/checkpoint did not return persisted=true: {checkpoint}"
    );

    let before = serving_uids(s);
    s.kube
        .run(&["delete", "pod", "-l", &s.label, "--wait=true"]);
    wait_ready(
        &s.kube,
        &s.label,
        EXPECTED_STORAGE_PODS,
        Duration::from_secs(600),
    );
    let after = serving_uids(s);
    assert!(
        before.iter().all(|uid| !after.contains(uid)),
        "a serving pod survived the restart: before={before:?} after={after:?}"
    );
    wait_service(s).await;
    assert_pods_on_pool(s);
    record_version(s, "after-restart").await;

    let old_hits = hits_for(&search_exact(s, &pre_value).await, &pre_id);
    assert_eq!(
        old_hits, 1,
        "pre-restart document was not readable before any new write"
    );
    index_exact(s, &post_id, &post_value).await;
    let post_hits = hits_for(&search_exact(s, &post_value).await, &post_id);
    assert_eq!(
        post_hits, 1,
        "replacement pod did not accept the post-restart write"
    );

    s.evidence(
        "durable-restart",
        json!({"pre_id": pre_id, "post_id": post_id, "checkpoint": checkpoint, "pre_restart_hits": 1, "post_restart_hits": 1}),
    );
    say(format!(
        "   durable restart preserved {pre_id} and accepted {post_id}"
    ));
}

/// The images the pods run, as evidence.
pub fn pod_images(s: &Suite, container: &str, pods: &Value) {
    let pods: Vec<Value> = pods["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| {
            let images: Vec<Value> = p["status"]["containerStatuses"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| json!({"name": c["name"], "image": c["image"], "imageID": c["imageID"]}))
                .collect();
            json!({"name": p["metadata"]["name"], "node": p["spec"]["nodeName"], "images": images})
        })
        .collect();
    s.evidence(&format!("pods-{container}"), Value::Array(pods));
}
