use std::collections::HashMap;

use service_k8s::{ManagedService, ReadyFacts};

use crate::domain::lumen_spec::Lumen;

/// An object as the N-1 operator wrote it: no `shardCount`, `reshard`,
/// `message` or `conditions`. It carries a finalizer, an owner reference and
/// an unknown status field.
fn n_minus_1_lumen_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "lumen.dev/v1alpha1",
        "kind": "Lumen",
        "metadata": {
            "name": "search",
            "namespace": "acme-handover",
            "uid": "uid-1",
            "generation": 3,
            "finalizers": ["lumen.dev/test-finalizer"],
            "ownerReferences": [
                {"apiVersion": "lumen.dev/v1alpha1", "kind": "LumenFleet", "name": "fleet-a", "uid": "uid-fleet"}
            ]
        },
        "spec": {"image": "lumen:0.6.2"},
        "status": {"phase": "Ready", "servingReadyReplicas": 1, "desiredReplicas": 1, "futureField": {"x": 1}}
    })
}

fn ready_none() -> ReadyFacts {
    ReadyFacts {
        ready: HashMap::new(),
    }
}

#[test]
fn an_n_minus_1_shaped_lumen_reads_with_defaults() {
    let lumen: Lumen = serde_json::from_value(n_minus_1_lumen_json()).expect("N-1 object reads");
    assert_eq!(lumen.spec.image, "lumen:0.6.2");
    assert_eq!(lumen.spec.shard_count, 1);
    assert_eq!(lumen.spec.replicas_per_shard, 1);
    assert_eq!(lumen.status.expect("status present").phase, "Ready");
}

#[test]
fn status_patch_leaves_metadata_and_unknown_status_fields_alone() {
    let lumen: Lumen = serde_json::from_value(n_minus_1_lumen_json()).expect("N-1 object reads");
    let patch = lumen.status_patch(&ready_none());

    let keys: Vec<&str> = patch
        .as_object()
        .expect("patch is an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, vec!["status"], "{patch}");
    assert!(patch["status"].get("futureField").is_none(), "{patch}");

    assert_eq!(
        lumen.metadata.finalizers,
        Some(vec!["lumen.dev/test-finalizer".to_string()])
    );
    let owners = lumen
        .metadata
        .owner_references
        .as_ref()
        .expect("owner references");
    assert_eq!(owners[0].name, "fleet-a");
}

#[test]
fn an_n_writes_n_minus_1_reads_lumen_with_unknown_fields_is_accepted() {
    let mut json = n_minus_1_lumen_json();
    json["spec"]["brandNewSpecField"] = serde_json::json!(true);
    json["status"]["brandNewStatusField"] = serde_json::json!("x");
    json["brandNewTopLevel"] = serde_json::json!(1);
    let read = serde_json::from_value::<Lumen>(json);
    assert!(read.is_ok(), "{:?}", read.err());
}

#[test]
fn a_lumen_without_an_image_is_rejected() {
    let mut json = n_minus_1_lumen_json();
    json["spec"].as_object_mut().expect("spec").remove("image");
    assert!(serde_json::from_value::<Lumen>(json).is_err());
}
