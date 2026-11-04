use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde_json::{Value, json};

use super::render;
use crate::domain::lumen_spec::{
    PlacementSpec,
    serving::{AuthMode, LogFormat, ServingSpec},
    topology::{ReshardPolicy, ShardMapSpec},
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

#[test]
fn single_member_render_leaves_update_strategy_without_partition() {
    let lumen = single_replica();
    let sts = render(&lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");

    assert_eq!(
        sts["spec"]["updateStrategy"],
        json!({ "type": "RollingUpdate" }),
        "single-pod runtime maintains current behavior without partition"
    );
}

#[test]
fn quorum_runtime_render_includes_rolling_update_partition() {
    let mut lumen = single_replica();
    lumen.spec.replicas_per_shard = 3;
    let sts = render(&lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");

    assert_eq!(
        sts["spec"]["updateStrategy"],
        json!({
            "type": "RollingUpdate",
            "rollingUpdate": {
                "partition": 0
            }
        }),
        "replicated quorum runtime must render rollingUpdate.partition"
    );
}
