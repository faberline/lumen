use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde_json::{json, Value};

use super::render;
use crate::domain::lumen_spec::{
    serving::{AuthMode, LogFormat, ServingSpec},
    topology::{ReshardPolicy, ShardMapSpec},
    PlacementSpec,
};
use crate::{Lumen, LumenSpec};

fn single_replica() -> Lumen {
    let mut lumen = Lumen::new(
        "search",
        LumenSpec {
            image: "registry.example.invalid/lumen:test".into(),
            image_pull_policy: None,
            image_pull_secrets: Vec::new(),
            placement: PlacementSpec::default(),
            shard_count: 1,
            shard_map: ShardMapSpec::default(),
            replicas_per_shard: 1,
            voter_count: 1,
            log_format: LogFormat::Pretty,
            log_level: None,
            auth: AuthMode::Off,
            serving: ServingSpec::default(),
            reshard_policy: ReshardPolicy::default(),
            observability: false,
            network_policy: false,
            admission: None,
            service_account_name: None,
            service_account_annotations: BTreeMap::new(),
            peer_tls_secret: None,
            serving_tls_secret: None,
            body_limit_bytes: None,
        },
    );
    lumen.metadata = ObjectMeta {
        name: Some("search".into()),
        namespace: Some("contract".into()),
        uid: Some("wi-4025-impl".into()),
        ..Default::default()
    };
    lumen
}

fn serving_mounts(lumen: &Lumen) -> Vec<Value> {
    render(lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet")["spec"]["template"]["spec"]["containers"][0]
        ["volumeMounts"]
        .as_array()
        .expect("serving volumeMounts")
        .clone()
}

#[test]
fn embedded_data_is_an_exact_child_of_the_raft_pvc_mount() {
    let mounts = serving_mounts(&single_replica());
    let raft_mounts: Vec<_> = mounts
        .iter()
        .filter(|mount| mount["name"] == "raft")
        .collect();

    assert_eq!(
        raft_mounts,
        vec![
            &json!({
                "name": "raft",
                "mountPath": "/var/lib/lumen",
                "readOnly": false,
            }),
            &json!({
                "name": "raft",
                "mountPath": "/var/lib/lumen/data",
                "subPath": "data",
                "readOnly": false,
            }),
        ]
    );
}

fn serving_statefulset_of(lumen: &Lumen) -> Value {
    render(lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet")
}

fn replicated(replicas: u32) -> Lumen {
    let mut lumen = single_replica();
    lumen.spec.replicas_per_shard = replicas;
    lumen.spec.voter_count = replicas;
    lumen
}

fn env_names(sts: &Value) -> Vec<String> {
    sts["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("serving env")
        .iter()
        .filter_map(|value| value["name"].as_str().map(str::to_string))
        .collect()
}

#[test]
fn one_pod_statefulset_has_one_replica_and_one_raft_pvc() {
    let lumen = single_replica();
    let sts = serving_statefulset_of(&lumen);

    assert_eq!(sts["spec"]["replicas"].as_i64(), Some(1));

    let claims = sts["spec"]["volumeClaimTemplates"]
        .as_array()
        .expect("volumeClaimTemplates");
    assert_eq!(claims.len(), 1, "got: {claims:?}");
    assert_eq!(claims[0]["metadata"]["name"], "raft");
    assert_eq!(claims[0]["spec"]["accessModes"], json!(["ReadWriteOnce"]));
    assert_eq!(
        claims[0]["spec"]["resources"]["requests"]["storage"],
        json!(lumen.spec.serving.raft_storage)
    );
}

#[test]
fn one_pod_pod_keeps_the_same_pvc_and_data_path() {
    let mounts = serving_mounts(&single_replica());

    assert!(
        mounts.contains(&json!({
            "name": "raft",
            "mountPath": "/var/lib/lumen",
            "readOnly": false,
        })),
        "got: {mounts:?}"
    );
    assert!(
        mounts.contains(&json!({
            "name": "raft",
            "mountPath": "/var/lib/lumen/data",
            "subPath": "data",
            "readOnly": false,
        })),
        "got: {mounts:?}"
    );
}

#[test]
fn one_pod_render_carries_no_ha_claim_in_labels_or_annotations() {
    let objects = render(&single_replica());
    let forbidden = ["highly available", "high availability", "quorum"];

    let mut checked = 0;
    for object in &objects {
        let mut values: Vec<String> = Vec::new();
        for path in [
            &object["metadata"]["labels"],
            &object["metadata"]["annotations"],
            &object["spec"]["template"]["metadata"]["labels"],
            &object["spec"]["template"]["metadata"]["annotations"],
        ] {
            if let Some(map) = path.as_object() {
                values.extend(map.values().filter_map(|v| v.as_str().map(str::to_string)));
            }
        }
        for value in values {
            checked += 1;
            let lowered = value.to_lowercase();
            for word in forbidden {
                assert!(
                    !lowered.contains(word),
                    "{:?} carries an HA claim {word:?}: {value:?}",
                    object["kind"]
                );
            }
        }
    }
    assert!(checked > 0, "no label or annotation value was checked");
}

#[test]
fn one_pod_env_has_no_raft_peer_or_voter_keys() {
    let names = env_names(&serving_statefulset_of(&single_replica()));

    assert!(
        !names.iter().any(|n| n == "REPLICAS_PER_SHARD"),
        "got: {names:?}"
    );
    assert!(!names.iter().any(|n| n == "VOTER_COUNT"), "got: {names:?}");
}

#[test]
fn three_voter_render_is_the_opposite_of_the_one_pod_render() {
    // Negative control: a pass in the one-Pod tests must not come from a
    // fixture or helper that ignores the spec.
    let lumen = replicated(3);
    let sts = serving_statefulset_of(&lumen);

    assert_eq!(sts["spec"]["replicas"].as_i64(), Some(3));

    let names = env_names(&sts);
    assert!(
        names.iter().any(|n| n == "REPLICAS_PER_SHARD"),
        "got: {names:?}"
    );
    assert!(names.iter().any(|n| n == "VOTER_COUNT"), "got: {names:?}");

    let claims = sts["spec"]["volumeClaimTemplates"]
        .as_array()
        .expect("volumeClaimTemplates");
    assert_eq!(claims[0]["metadata"]["name"], "raft");

    let mounts = serving_mounts(&lumen);
    assert!(
        !mounts
            .iter()
            .any(|m| m["mountPath"] == "/var/lib/lumen/data"),
        "a three-voter Pod must not mount the one-Pod data child: {mounts:?}"
    );
}
