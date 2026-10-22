//! Suite `overlay`: the direct-install Deployment `lumen` of
//! k8s/overlays/gke-test (labels app=lumen,role=server), reached at
//! http://lumen:7373. Job: k8s/gke-test-job/overlay.

use serde_json::json;

use super::cluster::{assert_pool_placement, env_count, exercise, named, pod_images, wait_serving, Suite};
use super::identity::replicas_of;
use super::support::{say, DumpOnFailure};

const LABEL: &str = "app=lumen,role=server";

/// The overlay's Deployment carries the dev overlay's shape plus a data PVC
/// at the image's LUMEN_DATA_DIR. Check that the render reached the cluster
/// as written, since nothing here patches it after the fact.
fn assert_overlay_shape(s: &Suite) {
    let deploy = s.kube.get(&["deploy/lumen"]);
    let spec = &deploy["spec"]["template"]["spec"];
    let data_mounts = named(&spec["containers"], "lumen")
        .into_iter()
        .flat_map(|c| c["volumeMounts"].as_array().into_iter().flatten())
        .filter(|m| m["name"] == "data" && m["mountPath"] == "/var/lib/lumen/data")
        .count();
    let data_volumes = spec["volumes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| v["name"] == "data" && v["persistentVolumeClaim"]["claimName"] == "lumen-data")
        .count();
    assert!(
        env_count(spec, "lumen", "LUMEN_WAL", "embedded") == 1 && data_mounts == 1 && data_volumes == 1,
        "overlay Deployment lost LUMEN_WAL=embedded or the lumen-data mount"
    );
    assert_pool_placement("Deployment/lumen", &deploy);
    let pvc = s.kube.get(&["pvc/lumen-data"]);
    assert!(pvc["status"]["phase"] == "Bound", "lumen-data PVC is not bound");
    say("   overlay Deployment runs LUMEN_WAL=embedded on the bound lumen-data PVC in pool-lumen-server");
}

/// Deployment/lumen and its pods run the image under test.
fn assert_identity(s: &Suite) {
    let replicas = replicas_of(&s.kube, "deploy/lumen");
    s.identity.assert_template_image(&s.kube, "deploy/lumen", "lumen");
    let pods = s.identity.assert_pods(&s.kube, LABEL, "lumen", replicas);
    pod_images(s, "lumen", &pods);
    say(format!("   Deployment/lumen and its pods run {} on pool-lumen-server", s.identity.image));
}

#[tokio::test]
#[ignore = "e2e: needs E2E_ENDPOINT"]
async fn journey() {
    let s = Suite::from_env("overlay", LABEL);
    let _dump = DumpOnFailure { suite: s.name, kube: &s.kube };

    let step = s.step("2. assert overlay Deployment shape");
    assert_overlay_shape(&s);
    step.end();

    wait_serving(&s);

    let step = s.step("3c. assert image identity");
    assert_identity(&s);
    step.end();

    exercise(&s).await;

    let step = s.step("6. assert image identity after the restart");
    assert_identity(&s);
    step.end();

    s.evidence("result", json!({"result": "pass"}));
    say(">> [overlay] PASS");
}
