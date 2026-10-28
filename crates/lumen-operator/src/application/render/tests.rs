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

#[test]
fn serving_statefulset_with_zones_renders_node_affinity_and_donotschedule() {
    let mut lumen = single_replica();
    lumen.spec.replicas_per_shard = 3;
    lumen.spec.voter_count = 3;
    lumen.spec.placement.zones = vec![
        "us-central1-a".into(),
        "us-central1-b".into(),
        "us-central1-c".into(),
    ];
    let objs = render(&lumen);
    let sts = objs
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");
    let pod = &sts["spec"]["template"]["spec"];

    // Hostname anti-affinity stays
    assert_eq!(
        pod["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]["topologyKey"],
        "kubernetes.io/hostname"
    );

    // Node affinity merged
    let terms = &pod["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"];
    assert_eq!(terms[0]["matchExpressions"][0]["key"], "topology.kubernetes.io/zone");
    assert_eq!(terms[0]["matchExpressions"][0]["operator"], "In");
    assert_eq!(
        terms[0]["matchExpressions"][0]["values"],
        json!(["us-central1-a", "us-central1-b", "us-central1-c"])
    );

    // Topology spread constraints: zone has DoNotSchedule when replicasPerShard > 1, hostname stays ScheduleAnyway
    let spreads = pod["topologySpreadConstraints"].as_array().expect("spreads");
    let zone_spread = spreads
        .iter()
        .find(|s| s["topologyKey"] == "topology.kubernetes.io/zone")
        .expect("zone spread");
    assert_eq!(zone_spread["whenUnsatisfiable"], "DoNotSchedule");

    let host_spread = spreads
        .iter()
        .find(|s| s["topologyKey"] == "kubernetes.io/hostname")
        .expect("host spread");
    assert_eq!(host_spread["whenUnsatisfiable"], "ScheduleAnyway");
}

#[test]
fn serving_statefulset_without_zones_renders_no_node_affinity() {
    let lumen = single_replica();
    let objs = render(&lumen);
    let sts = objs
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");
    let pod = &sts["spec"]["template"]["spec"];

    assert!(pod["affinity"].get("nodeAffinity").is_none());
    let spreads = pod["topologySpreadConstraints"].as_array().expect("spreads");
    let zone_spread = spreads
        .iter()
        .find(|s| s["topologyKey"] == "topology.kubernetes.io/zone")
        .expect("zone spread");
    assert_eq!(zone_spread["whenUnsatisfiable"], "ScheduleAnyway");
}
