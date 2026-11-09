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

fn assert_operator_independent(objects: &[Value]) {
    // `POD_NAMESPACE` alone is the serving pod's own namespace (downward API),
    // so only an operator name or namespace counts as a reference.
    const FORBIDDEN: [&str; 1] = ["lumen-operator"];
    for object in objects {
        let runtime = format!(
            "{}{}",
            object["spec"]["template"]["spec"], object["spec"]["selector"]
        );
        for word in FORBIDDEN {
            assert!(
                !runtime.contains(word),
                "{} runtime names the operator: {word}",
                object["kind"]
            );
        }
    }
}

#[test]
fn render_is_a_pure_function_of_the_cr() {
    let lumen = single_replica();
    assert_eq!(
        render(&lumen),
        render(&lumen),
        "a restarted operator re-applies identical desired state"
    );

    let sts = serving_statefulset_of(&lumen);
    assert!(sts["spec"]["template"]["metadata"]
        .get("ownerReferences")
        .is_none());
    assert!(sts["spec"]["template"]["metadata"]
        .get("finalizers")
        .is_none());
}

#[test]
fn rendered_objects_never_name_the_operator() {
    assert_operator_independent(&render(&single_replica()));
}

#[test]
fn injecting_an_operator_env_var_fails_the_independence_check() {
    // Negative control: the independence helper must reject an operator
    // reference injected into the serving pod's environment.
    let mut objects = render(&single_replica());
    let sts = objects
        .iter_mut()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");
    let container = &mut sts["spec"]["template"]["spec"]["containers"][0];
    if container["env"].is_null() {
        container["env"] = json!([]);
    }
    container["env"]
        .as_array_mut()
        .expect("serving env")
        .push(json!({ "name": "POD_NAMESPACE", "value": "lumen-operator-system" }));

    let result = std::panic::catch_unwind(|| assert_operator_independent(&objects));
    let message = result
        .err()
        .and_then(|payload| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(message.contains("names the operator"), "got: {message:?}");
}
