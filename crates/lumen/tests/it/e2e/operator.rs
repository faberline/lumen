//! Suite `operator`: the Lumen CR `lumen-op` of k8s/overlays/gke-test,
//! reconciled by the shipped operator and reached at http://lumen-op:7373,
//! plus `lumen-op-legacy-missing-catalog`, which must fail on the missing
//! capacity catalog. Job: k8s/gke-test-job/operator.

use std::time::{Duration, Instant};

use serde_json::json;

use super::cluster::{
    assert_pool_placement, env_count, exercise, mounts_named, named, pod_images, wait_serving,
    Suite, EXPECTED_STORAGE_PODS,
};
use super::identity::replicas_of;
use super::support::{say, DumpOnFailure};

const CR: &str = "lumen-op";
const LEGACY_CR: &str = "lumen-op-legacy-missing-catalog";
const OPERATOR: &str = "lumen-operator";
const OPERATOR_LABEL: &str = "app.kubernetes.io/name=lumen-operator";
/// The instance label keeps the legacy CR and the overlay pod out of it.
const LABEL: &str = "app.kubernetes.io/name=lumen,app.kubernetes.io/instance=lumen-op,app.kubernetes.io/component=server";

/// The raft PVC at the parent path, and its `data` child at LUMEN_DATA_DIR.
fn raft_mounts() -> Vec<serde_json::Value> {
    vec![
        json!({"mountPath": "/var/lib/lumen", "name": "raft", "readOnly": false}),
        json!({"mountPath": "/var/lib/lumen/data", "name": "raft", "readOnly": false, "subPath": "data"}),
    ]
}

fn wait_operator_statefulset(s: &Suite) {
    s.kube.run(&[
        "rollout",
        "status",
        &format!("deploy/{OPERATOR}"),
        "--timeout=300s",
    ]);
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if s.kube
            .try_run(&["get", &format!("statefulset/{CR}"), "-o", "name"])
            .is_ok()
        {
            say(format!("   operator reconciled StatefulSet/{CR}"));
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    s.kube
        .show(&["logs", &format!("deploy/{OPERATOR}"), "--tail=60"]);
    panic!("operator did not render StatefulSet/{CR} within 180s");
}

/// Native placement: the CR's exact selector, toleration, and pull secret
/// reach the StatefulSet without a capacity catalog.
fn assert_native_placement(s: &Suite) {
    let sset = s.kube.get(&[&format!("statefulset/{CR}")]);
    assert_pool_placement(&format!("StatefulSet/{CR}"), &sset);
    assert!(
        !s.kube.exists("configmap/lumen-capacity-catalog"),
        "native placement unexpectedly depends on a fake capacity catalog"
    );
    say("   native placement preserved faberline.dev/pool=lumen-server and ghcr-pull without a capacity catalog");
}

fn assert_legacy_missing_catalog_event(s: &Suite) {
    let selector = format!("involvedObject.name={LEGACY_CR},reason=ReconcileFailed");
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        let events = s.kube.get(&["events", "--field-selector", &selector]);
        let message = events["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|e| {
                e["message"]
                    .as_str()
                    .or(e["note"].as_str())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\n");
        if message.contains("capacity catalog") {
            s.evidence("legacy-reconcile-failed", json!({"message": message}));
            say("   legacy empty-selector failure Event names missing capacity catalog");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    s.kube.show(&["get", "events", "--sort-by=.lastTimestamp"]);
    panic!("legacy empty-selector CR did not publish a namespaced ReconcileFailed Event naming the capacity catalog");
}

fn assert_operator_topology(s: &Suite) {
    let sset = s.kube.get(&[&format!("statefulset/{CR}")]);
    let replicas = &sset["spec"]["replicas"];
    say(format!(
        "   StatefulSet replicas: {replicas} (expected {EXPECTED_STORAGE_PODS})"
    ));
    assert!(
        replicas.as_u64() == Some(EXPECTED_STORAGE_PODS as u64),
        "expected StatefulSet/{CR} replicas={EXPECTED_STORAGE_PODS}, got {replicas}"
    );
    let claims = sset["spec"]["volumeClaimTemplates"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let spec = &sset["spec"]["template"]["spec"];
    assert!(
        claims.len() == 1
            && claims[0]["metadata"]["name"] == "raft"
            && named(&spec["containers"], "server").len() == 1,
        "operator StatefulSet must render exactly one raft PVC and one server container"
    );
    assert!(
        mounts_named(spec, "server", "raft") == raft_mounts()
            && env_count(spec, "server", "LUMEN_DATA_DIR", "/var/lib/lumen/data") == 1
            && env_count(spec, "server", "LUMEN_PERSISTENCE", "segment") == 1,
        "single-replica operator StatefulSet lost the exact raft parent/data child mount contract"
    );
    let cm = s.kube.get(&[&format!("cm/{CR}-config")]);
    let data = &cm["data"];
    say(format!(
        "   ConfigMap topology: SHARD_COUNT={} VIRTUAL_BUCKET_COUNT={} SHARD_MAP_VERSION={}",
        data["SHARD_COUNT"], data["VIRTUAL_BUCKET_COUNT"], data["SHARD_MAP_VERSION"]
    ));
    assert!(
        data["SHARD_COUNT"] == "1"
            && !data["VIRTUAL_BUCKET_COUNT"].is_null()
            && !data["SHARD_MAP_VERSION"].is_null(),
        "ConfigMap topology keys are missing or wrong"
    );
}

fn assert_operator_storage_live(s: &Suite) {
    let claim = format!("raft-{CR}-0");
    let pod = s.kube.get(&[&format!("pod/{CR}-0")]);
    let claimed = pod["spec"]["volumes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| {
            v["name"] == "raft" && v["persistentVolumeClaim"]["claimName"] == claim.as_str()
        })
        .count();
    assert!(
        mounts_named(&pod["spec"], "server", "raft") == raft_mounts() && claimed == 1,
        "live serving pod lost the exact raft PVC parent/data child mounts"
    );
    let pvc = s.kube.get(&[&format!("pvc/{claim}")]);
    assert!(
        pvc["metadata"]["name"] == claim.as_str()
            && pvc["status"]["phase"] == "Bound"
            && pvc["spec"]["volumeName"]
                .as_str()
                .is_some_and(|v| !v.is_empty()),
        "{claim} PVC is not bound"
    );
    say(format!(
        "   live pod mounts the bound {claim} PVC at parent and exact data child paths"
    ));
}

/// The operator, the Lumen CR, and the StatefulSet it rendered all ask for
/// the image under test, and their pods run it.
fn assert_identity(s: &Suite) {
    let id = &s.identity;
    let operator = format!("deploy/{OPERATOR}");
    let replicas = replicas_of(&s.kube, &operator);
    id.assert_template_image(&s.kube, &operator, "operator");
    let pods = id.assert_pods(&s.kube, OPERATOR_LABEL, "operator", replicas);
    pod_images(s, "operator", &pods);
    let cr = s.kube.get(&[&format!("lumen/{CR}")]);
    assert!(
        cr["spec"]["image"] == id.image.as_str(),
        "Lumen CR spec.image {} != {}",
        cr["spec"]["image"],
        id.image
    );
    let sset = format!("statefulset/{CR}");
    let replicas = replicas_of(&s.kube, &sset);
    id.assert_template_image(&s.kube, &sset, "server");
    let pods = id.assert_pods(&s.kube, LABEL, "server", replicas);
    pod_images(s, "server", &pods);
    say(format!(
        "   the operator, Lumen/{CR}, and its StatefulSet run {} on pool-lumen-server",
        id.image
    ));
}

#[tokio::test]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn journey() {
    let s = Suite::from_env("operator", LABEL);
    let _dump = DumpOnFailure {
        suite: s.name,
        kube: &s.kube,
    };

    let step = s.step(format!("2. wait for operator StatefulSet/{CR}"));
    wait_operator_statefulset(&s);
    step.end();
    let step = s.step("2a. assert native placement");
    assert_native_placement(&s);
    step.end();
    let step = s.step("2b. assert legacy missing-catalog Event");
    assert_legacy_missing_catalog_event(&s);
    step.end();
    let step = s.step("2c. assert operator storage topology");
    assert_operator_topology(&s);
    step.end();

    wait_serving(&s);

    let step = s.step("3b. assert live operator PVC and mounts");
    assert_operator_storage_live(&s);
    step.end();

    let step = s.step("3c. assert image identity");
    assert_identity(&s);
    step.end();

    exercise(&s).await;

    let step = s.step("6. assert image identity after the restart");
    assert_identity(&s);
    step.end();

    s.evidence("result", json!({"result": "pass"}));
    say(">> [operator] PASS");
}
