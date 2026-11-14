use service_k8s::{ManagedService, ReadyFacts};

use crate::domain::lumen_spec::Lumen;

/// A `Lumen` object as the N-1 operator wrote it: no `shardCount`, `reshard`,
/// `message` or `conditions` in the status, but with finalizers, owner
/// references and a status field the reader does not know.
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
                {
                    "apiVersion": "lumen.dev/v1alpha1",
                    "kind": "LumenFleet",
                    "name": "fleet-a",
                    "uid": "uid-fleet"
                }
            ]
        },
        "spec": {"image": "lumen:0.6.2"},
        "status": {
            "phase": "Ready",
            "servingReadyReplicas": 1,
            "desiredReplicas": 1,
            "futureField": {"x": 1}
        }
    })
}

#[test]
fn an_n_minus_1_shaped_lumen_reads_with_defaults() {
    let lumen: Lumen = serde_json::from_value(n_minus_1_lumen_json())
        .expect("an N-1 shaped Lumen must deserialize");
    assert_eq!(lumen.spec.image, "lumen:0.6.2");
    assert_eq!(lumen.spec.shard_count, 1);
    assert_eq!(lumen.spec.replicas_per_shard, 1);
    assert_eq!(lumen.status.expect("status is present").phase, "Ready");
}

#[test]
fn status_patch_leaves_metadata_and_unknown_status_fields_alone() {
    let lumen: Lumen = serde_json::from_value(n_minus_1_lumen_json())
        .expect("an N-1 shaped Lumen must deserialize");
    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);

    let keys: Vec<&String> = patch
        .as_object()
        .expect("status patch is a JSON object")
        .keys()
        .collect();
    assert_eq!(keys, vec!["status"], "{patch}");
    assert!(
        patch["status"].get("futureField").is_none(),
        "an unknown status field must not be written by the patch: {patch}"
    );
    assert_eq!(
        lumen.metadata.finalizers,
        Some(vec!["lumen.dev/test-finalizer".to_string()])
    );
    assert_eq!(
        lumen
            .metadata
            .owner_references
            .as_ref()
            .expect("owner references kept")[0]
            .name,
        "fleet-a"
    );
}

#[test]
fn an_n_writes_n_minus_1_reads_lumen_with_unknown_fields_is_accepted() {
    let mut json = n_minus_1_lumen_json();
    json["brandNewTopLevel"] = serde_json::json!(1);
    json["spec"]["brandNewSpecField"] = serde_json::json!(true);
    json["status"]["brandNewStatusField"] = serde_json::json!("x");
    let parsed = serde_json::from_value::<Lumen>(json);
    assert!(parsed.is_ok(), "{:?}", parsed.err());
}

#[test]
fn a_lumen_without_an_image_is_rejected() {
    let mut json = n_minus_1_lumen_json();
    json["spec"]
        .as_object_mut()
        .expect("spec is an object")
        .remove("image");
    assert!(serde_json::from_value::<Lumen>(json).is_err());
}
