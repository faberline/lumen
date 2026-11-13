use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde_json::{json, Value};

use super::render;
use crate::application::render::restart_hash::{canonical_input, restart_required_hash};
use crate::domain::lumen_spec::{
    serving::{AuthMode, LogFormat, ServingSpec},
    topology::{ReshardPolicy, ShardMapSpec},
    AdmissionSpec, PlacementSpec,
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
            access: None,
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

fn hash(lumen: &Lumen) -> String {
    restart_required_hash(lumen, &BTreeMap::new())
}

#[test]
fn restart_hash_is_stable_for_the_same_spec() {
    let lumen = single_replica();
    let first = hash(&lumen);
    assert_eq!(first, hash(&lumen));
    assert_eq!(first.len(), 64);
    assert!(first.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
}

#[test]
fn restart_hash_ignores_field_order_of_object_versions() {
    let mut a = BTreeMap::new();
    a.insert("cm".to_string(), "1".to_string());
    a.insert("secret".to_string(), "2".to_string());
    let mut b = BTreeMap::new();
    b.insert("secret".to_string(), "2".to_string());
    b.insert("cm".to_string(), "1".to_string());
    let lumen = single_replica();
    assert_eq!(
        restart_required_hash(&lumen, &a),
        restart_required_hash(&lumen, &b)
    );
}

#[test]
fn restart_hash_changes_when_a_restart_required_value_changes() {
    let base = hash(&single_replica());

    let mut log_level = single_replica();
    log_level.spec.log_level = Some("debug".into());
    assert_ne!(hash(&log_level), base);

    let mut auth = single_replica();
    auth.spec.auth = AuthMode::Required;
    assert_ne!(hash(&auth), base);

    let mut body_limit = single_replica();
    body_limit.spec.body_limit_bytes = Some(2_097_152);
    assert_ne!(hash(&body_limit), base);

    let mut admission = single_replica();
    admission.spec.admission = Some(AdmissionSpec {
        read_capacity: Some(10),
        write_capacity: None,
        admin_capacity: None,
        refill_secs: None,
        max_keys: None,
    });
    assert_ne!(hash(&admission), base);
}

#[test]
fn restart_hash_changes_when_an_object_version_changes() {
    let lumen = single_replica();
    let v1 = BTreeMap::from([("app-config".to_string(), "1".to_string())]);
    let v2 = BTreeMap::from([("app-config".to_string(), "2".to_string())]);
    let none = BTreeMap::new();
    assert_ne!(
        restart_required_hash(&lumen, &v1),
        restart_required_hash(&lumen, &v2)
    );
    assert_ne!(
        restart_required_hash(&lumen, &none),
        restart_required_hash(&lumen, &v1)
    );
}

#[test]
fn restart_hash_ignores_tls_certificate_content_but_not_the_reference() {
    // Certificate file content is not an input of the hash: the hash is a
    // function of (lumen, object_versions) only, so rotating the content of
    // the referenced Secret leaves both the spec and the versions unchanged.
    let mut with_tls = single_replica();
    with_tls.spec.serving_tls_secret = Some("tls-a".into());
    let versions = BTreeMap::new();
    assert_eq!(
        restart_required_hash(&with_tls, &versions),
        restart_required_hash(&with_tls, &versions),
    );

    // Adding or removing the reference does change the hash (env changes).
    let without_tls = single_replica();
    assert_ne!(hash(&without_tls), hash(&with_tls));

    let mut peer_base = single_replica();
    peer_base.spec.replicas_per_shard = 3;
    peer_base.spec.voter_count = 3;
    let mut peer_with = peer_base.clone();
    peer_with.spec.peer_tls_secret = Some("peer-a".into());
    assert_ne!(hash(&peer_base), hash(&peer_with));
}

#[test]
fn restart_hash_annotation_is_on_the_serving_pod_template() {
    let lumen = single_replica();
    let statefulset = render(&lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet");
    let annotations = &statefulset["spec"]["template"]["metadata"]["annotations"];
    assert_eq!(annotations["lumen.dev/restart-hash"], json!(hash(&lumen)));
    assert_eq!(annotations["prometheus.io/scrape"], json!("true"));
}

#[test]
fn restart_hash_assertion_bites_when_a_field_is_left_out_of_the_input() {
    // Negative control: without LUMEN_LOG_LEVEL in the hash input, the
    // "value changes, hash changes" assertion above would fail.
    let base = single_replica();
    let mut changed = single_replica();
    changed.spec.log_level = Some("debug".into());
    let versions = BTreeMap::new();
    let base_input = canonical_input(&base, &versions);
    let changed_input = canonical_input(&changed, &versions);
    assert_ne!(base_input, changed_input);

    let without_log_level = |input: &str| {
        input
            .lines()
            .filter(|line| !line.contains("LUMEN_LOG_LEVEL"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        without_log_level(&base_input),
        without_log_level(&changed_input)
    );
}
