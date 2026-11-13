use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde_json::{json, Value};

use super::{prunes, render};
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

fn objs_of(lumen: &Lumen, kind: &str) -> Vec<Value> {
    render(lumen)
        .into_iter()
        .filter(|object| object["kind"] == kind)
        .collect()
}

fn sharded(shards: u32, replicas: u32, voters: u32) -> Lumen {
    let mut lumen = single_replica();
    lumen.spec.shard_count = shards;
    lumen.spec.replicas_per_shard = replicas;
    lumen.spec.voter_count = voters;
    lumen
}

fn selector_matches(selector: &Value, labels: &Value) -> bool {
    selector
        .as_object()
        .expect("selector object")
        .iter()
        .all(|(key, value)| labels.get(key.as_str()) == Some(value))
}

fn shard_anti_affinity_selector(statefulset: &Value) -> Value {
    statefulset["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]
        ["requiredDuringSchedulingIgnoredDuringExecution"][0]["labelSelector"]["matchLabels"]
        .clone()
}

#[test]
fn single_shard_keeps_one_statefulset_and_one_pdb() {
    let lumen = single_replica();
    let statefulsets = objs_of(&lumen, "StatefulSet");
    assert_eq!(statefulsets.len(), 1);
    assert_eq!(statefulsets[0]["metadata"]["name"], "search");

    let pdbs = objs_of(&lumen, "PodDisruptionBudget");
    assert_eq!(pdbs.len(), 1);
    assert_eq!(pdbs[0]["metadata"]["name"], "search");
    assert_eq!(pdbs[0]["spec"]["maxUnavailable"], 1);
}

#[test]
fn three_shards_render_three_statefulsets_named_per_shard() {
    let lumen = sharded(3, 1, 1);
    let statefulsets = objs_of(&lumen, "StatefulSet");
    let names: Vec<&str> = statefulsets
        .iter()
        .map(|object| object["metadata"]["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["search-s0", "search-s1", "search-s2"]);

    for (shard, object) in statefulsets.iter().enumerate() {
        let index = shard.to_string();
        assert_eq!(
            object["metadata"]["labels"]["lumen.dev/shard"],
            index.as_str()
        );
        assert_eq!(
            object["spec"]["selector"]["matchLabels"]["lumen.dev/shard"],
            index.as_str()
        );
        assert_eq!(
            object["spec"]["template"]["metadata"]["labels"]["lumen.dev/shard"],
            index.as_str()
        );
    }
}

#[test]
fn shard_pod_selects_only_its_own_shard() {
    let lumen = sharded(2, 1, 1);
    let statefulsets = objs_of(&lumen, "StatefulSet");
    for (shard, object) in statefulsets.iter().enumerate() {
        let term = &object["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]
            ["requiredDuringSchedulingIgnoredDuringExecution"][0];
        assert_eq!(
            term["labelSelector"]["matchLabels"]["lumen.dev/shard"],
            shard.to_string().as_str()
        );
        assert_eq!(term["topologyKey"], "kubernetes.io/hostname");
        assert!(term.get("namespaceSelector").is_none());
    }
}

#[test]
fn different_shards_may_share_a_node() {
    // Negative test: dropping the shard label from the selector would make
    // the runtime-wide rule match every shard's pods.
    let lumen = sharded(2, 1, 1);
    let statefulsets = objs_of(&lumen, "StatefulSet");
    let shard0_selector = shard_anti_affinity_selector(&statefulsets[0]);
    let shard0_labels = &statefulsets[0]["spec"]["template"]["metadata"]["labels"];
    let shard1_labels = &statefulsets[1]["spec"]["template"]["metadata"]["labels"];

    assert!(selector_matches(&shard0_selector, shard0_labels));
    assert!(
        !selector_matches(&shard0_selector, shard1_labels),
        "a shard 1 pod must not be excluded from a node by shard 0 anti-affinity"
    );
    let runtime_wide = json!({
        "app.kubernetes.io/name": "lumen",
        "app.kubernetes.io/component": "server",
    });
    assert!(
        selector_matches(&runtime_wide, shard1_labels),
        "the old runtime-wide selector would have matched shard 1"
    );
}

#[test]
fn shard_env_reads_the_shard_label() {
    let lumen = sharded(3, 1, 1);
    let statefulsets = objs_of(&lumen, "StatefulSet");
    let env = statefulsets[2]["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("env");
    let entry = env
        .iter()
        .find(|entry| entry["name"] == "LUMEN_SHARD_INDEX")
        .expect("LUMEN_SHARD_INDEX env");
    assert_eq!(
        entry["valueFrom"]["fieldRef"]["fieldPath"],
        "metadata.labels['lumen.dev/shard']"
    );
}

#[test]
fn three_voter_shard_has_hard_zone_spread_scoped_to_the_shard() {
    let mut lumen = sharded(2, 3, 3);
    lumen.spec.peer_tls_secret = Some("peer".into());
    let statefulsets = objs_of(&lumen, "StatefulSet");
    let shard0 = &statefulsets[0];
    assert_eq!(shard0["spec"]["replicas"], 3);

    let spreads = shard0["spec"]["template"]["spec"]["topologySpreadConstraints"]
        .as_array()
        .expect("spread constraints");
    let hard: Vec<&Value> = spreads
        .iter()
        .filter(|c| c["whenUnsatisfiable"] == "DoNotSchedule")
        .collect();
    assert_eq!(hard.len(), 1);
    assert_eq!(hard[0]["topologyKey"], "topology.kubernetes.io/zone");
    assert_eq!(hard[0]["maxSkew"], 1);
    assert_eq!(
        hard[0]["labelSelector"]["matchLabels"]["lumen.dev/shard"],
        "0"
    );

    let soft = spreads
        .iter()
        .filter(|c| c["whenUnsatisfiable"] == "ScheduleAnyway")
        .count();
    assert_eq!(soft, 2, "runtime-wide spread stays soft");
}

#[test]
fn single_voter_shard_has_no_hard_zone_spread() {
    let lumen = sharded(2, 1, 1);
    for statefulset in objs_of(&lumen, "StatefulSet") {
        let spreads = statefulset["spec"]["template"]["spec"]["topologySpreadConstraints"]
            .as_array()
            .expect("spread constraints");
        assert!(spreads
            .iter()
            .all(|c| c["whenUnsatisfiable"] != "DoNotSchedule"));
    }
}

#[test]
fn pdb_per_shard_uses_quorum_safe_max_unavailable() {
    let mut lumen = sharded(2, 3, 3);
    lumen.spec.peer_tls_secret = Some("peer".into());
    let pdbs = objs_of(&lumen, "PodDisruptionBudget");
    let names: Vec<&str> = pdbs
        .iter()
        .map(|object| object["metadata"]["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["search-s0", "search-s1"]);
    for (shard, pdb) in pdbs.iter().enumerate() {
        assert_eq!(pdb["spec"]["maxUnavailable"], 1);
        assert_eq!(
            pdb["spec"]["selector"]["matchLabels"]["lumen.dev/shard"],
            shard.to_string().as_str()
        );
    }

    let mut five = sharded(2, 5, 5);
    five.spec.peer_tls_secret = Some("peer".into());
    let five_pdbs = objs_of(&five, "PodDisruptionBudget");
    assert_eq!(five_pdbs[0]["spec"]["maxUnavailable"], 2);
}

#[test]
fn prunes_old_single_statefulset_and_pdb_when_sharded() {
    let mut sharded_lumen = sharded(2, 1, 1);
    sharded_lumen.spec.network_policy = true;
    let targets = prunes(&sharded_lumen);
    assert!(targets
        .iter()
        .any(|t| t.kind == "StatefulSet" && t.name == "search"));
    assert!(targets
        .iter()
        .any(|t| t.kind == "PodDisruptionBudget" && t.name == "search"));

    let mut single = single_replica();
    single.spec.network_policy = true;
    assert!(prunes(&single).is_empty());

    let plain = single_replica();
    let plain_targets = prunes(&plain);
    assert_eq!(plain_targets.len(), 1);
    assert_eq!(plain_targets[0].kind, "NetworkPolicy");
}
