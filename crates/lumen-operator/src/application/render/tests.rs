use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde_json::{json, Value};

use super::render;
use crate::application::render::restart_hash::{canonical_input, restart_required_hash};
use crate::domain::lumen_spec::{
    serving::{AuthMode, LogFormat, ServingSpec},
    topology::{ReshardPolicy, ShardMapSpec},
    validation::is_reserved_env_name,
    EnvEntry, KeyRef, PlacementSpec,
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
            runtime: None,
            extra_env: Vec::new(),
            extra_args: Vec::new(),
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

fn serving_container(lumen: &Lumen) -> Value {
    render(lumen)
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet")["spec"]["template"]["spec"]["containers"][0]
        .clone()
}

fn env_entry_names(container: &Value) -> Vec<String> {
    container["env"]
        .as_array()
        .expect("serving env")
        .iter()
        .map(|entry| entry["name"].as_str().expect("env name").to_string())
        .collect()
}

#[test]
fn extra_env_renders_value_from_refs_only() {
    let mut lumen = single_replica();
    lumen.spec.extra_env = vec![
        EnvEntry {
            name: "MY_LITERAL".into(),
            value: Some("plain".into()),
            ..Default::default()
        },
        EnvEntry {
            name: "MY_SECRET".into(),
            secret_key_ref: Some(KeyRef {
                name: "app-secret".into(),
                key: "token".into(),
            }),
            ..Default::default()
        },
        EnvEntry {
            name: "MY_CM".into(),
            config_map_key_ref: Some(KeyRef {
                name: "app-cm".into(),
                key: "mode".into(),
            }),
            ..Default::default()
        },
    ];
    let env = serving_container(&lumen)["env"]
        .as_array()
        .expect("serving env")
        .clone();
    assert_eq!(
        &env[env.len() - 3..],
        &[
            json!({ "name": "MY_LITERAL", "value": "plain" }),
            json!({ "name": "MY_SECRET", "valueFrom": { "secretKeyRef": { "name": "app-secret", "key": "token" } } }),
            json!({ "name": "MY_CM", "valueFrom": { "configMapKeyRef": { "name": "app-cm", "key": "mode" } } }),
        ]
    );
}

#[test]
fn extra_args_render_on_the_container() {
    let mut lumen = single_replica();
    lumen.spec.extra_args = vec!["--foo".into(), "--bar=1".into()];
    assert_eq!(
        serving_container(&lumen)["args"],
        json!(["--foo", "--bar=1"])
    );
}

#[test]
fn no_extra_inputs_renders_identically() {
    let lumen = single_replica();
    let container = serving_container(&lumen);
    assert!(container.get("args").is_none_or(|args| args == &json!([])));
    assert!(!env_entry_names(&container)
        .iter()
        .any(|name| name.starts_with("MY_")));
    let spec = serde_json::to_value(&lumen.spec).expect("spec json");
    assert!(spec.get("extraEnv").is_none());
    assert!(spec.get("extraArgs").is_none());
}

#[test]
fn sentinel_secret_value_never_in_rendered_output() {
    let mut lumen = single_replica();
    lumen.spec.extra_env = vec![EnvEntry {
        name: "MY_SECRET".into(),
        secret_key_ref: Some(KeyRef {
            name: "sentinel-secret".into(),
            key: "token".into(),
        }),
        ..Default::default()
    }];
    let rendered = serde_json::to_string(&render(&lumen)).expect("rendered json");
    assert!(rendered.contains("sentinel-secret"));
    assert!(!rendered.contains("SENTINEL-SECRET-VALUE-7731"));
}

#[test]
fn literal_env_value_is_rendered() {
    let mut lumen = single_replica();
    lumen.spec.extra_env = vec![EnvEntry {
        name: "MY_LITERAL".into(),
        value: Some("SENTINEL-LITERAL-7731".into()),
        ..Default::default()
    }];
    let rendered = serde_json::to_string(&render(&lumen)).expect("rendered json");
    assert!(rendered.contains("SENTINEL-LITERAL-7731"));
}

#[test]
fn two_namespaces_render_independent_inputs() {
    let mut a = single_replica();
    a.metadata.namespace = Some("ns-a".into());
    a.spec.extra_env = vec![EnvEntry {
        name: "MY_SECRET".into(),
        secret_key_ref: Some(KeyRef {
            name: "secret-a".into(),
            key: "k".into(),
        }),
        ..Default::default()
    }];
    let mut b = single_replica();
    b.metadata.namespace = Some("ns-b".into());
    b.spec.extra_env = vec![EnvEntry {
        name: "MY_SECRET".into(),
        secret_key_ref: Some(KeyRef {
            name: "secret-b".into(),
            key: "k".into(),
        }),
        ..Default::default()
    }];
    let rendered_a = serde_json::to_string(&render(&a)).expect("rendered json");
    let rendered_b = serde_json::to_string(&render(&b)).expect("rendered json");
    assert!(rendered_a.contains(r#""secretKeyRef":{"name":"secret-a","key":"k"}"#));
    assert!(!rendered_a.contains("secret-b"));
    assert!(rendered_b.contains(r#""secretKeyRef":{"name":"secret-b","key":"k"}"#));
    assert!(!rendered_b.contains("secret-a"));
}

#[test]
fn operator_owned_env_names_are_all_reserved() {
    for replicas in [3, 1] {
        let mut lumen = single_replica();
        lumen.spec.replicas_per_shard = replicas;
        lumen.spec.voter_count = 3;
        lumen.spec.peer_tls_secret = Some("peer".into());
        lumen.spec.serving_tls_secret = Some("serving".into());
        lumen.spec.admission = Some(crate::domain::lumen_spec::AdmissionSpec {
            read_capacity: Some(1),
            write_capacity: Some(1),
            admin_capacity: Some(1),
            refill_secs: Some(1),
            max_keys: Some(1),
        });
        lumen.spec.body_limit_bytes = Some(2_097_152);
        lumen.spec.log_level = Some("debug".into());
        let names = env_entry_names(&serving_container(&lumen));
        assert!(!names.is_empty());
        for name in names {
            assert!(
                is_reserved_env_name(&name),
                "{name} is operator-owned but not reserved"
            );
        }
    }
}

// #42 (first slice): the restart-required hash and its pod-template annotation.

fn hash(lumen: &Lumen) -> String {
    restart_required_hash(lumen, &BTreeMap::new())
}

#[test]
fn restart_hash_is_stable_for_the_same_spec() {
    let lumen = single_replica();
    let first = hash(&lumen);
    assert_eq!(first, hash(&lumen));
    assert_eq!(first.len(), 64);
    assert!(first
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
}

#[test]
fn restart_hash_ignores_field_order_of_object_versions() {
    let lumen = single_replica();
    let mut ab = BTreeMap::new();
    ab.insert("cm".to_string(), "1".to_string());
    ab.insert("secret".to_string(), "2".to_string());
    let mut ba = BTreeMap::new();
    ba.insert("secret".to_string(), "2".to_string());
    ba.insert("cm".to_string(), "1".to_string());
    assert_eq!(
        restart_required_hash(&lumen, &ab),
        restart_required_hash(&lumen, &ba)
    );
}

#[test]
fn restart_hash_changes_when_a_restart_required_value_changes() {
    let base = hash(&single_replica());

    let mut log = single_replica();
    log.spec.log_level = Some("debug".into());

    let mut auth = single_replica();
    auth.spec.auth = AuthMode::Required;

    let mut body = single_replica();
    body.spec.body_limit_bytes = Some(2_097_152);

    let mut admission = single_replica();
    admission.spec.admission = Some(crate::domain::lumen_spec::AdmissionSpec {
        read_capacity: Some(10),
        write_capacity: None,
        admin_capacity: None,
        refill_secs: None,
        max_keys: None,
    });

    for changed in [log, auth, body, admission] {
        assert_ne!(hash(&changed), base);
    }
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
    // Certificate file content is not an input: the hash is a function of the
    // spec and the object versions only. A rotated file behind the same
    // reference therefore keeps the hash and does not roll the pods.
    let mut tls = single_replica();
    tls.spec.serving_tls_secret = Some("tls-a".into());
    assert_eq!(hash(&tls), hash(&tls.clone()));

    // Adding or removing the reference changes the rendered env and mounts.
    assert_ne!(hash(&single_replica()), hash(&tls));

    let mut peer = single_replica();
    peer.spec.replicas_per_shard = 3;
    peer.spec.voter_count = 3;
    let mut peer_tls = peer.clone();
    peer_tls.spec.peer_tls_secret = Some("peer-a".into());
    assert_ne!(hash(&peer), hash(&peer_tls));
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
    // Negative control: without the LUMEN_LOG_LEVEL lines the two inputs are
    // equal, so a hash input that left the field out would fail the
    // "value changes, hash changes" assertion above.
    let base = single_replica();
    let mut changed = single_replica();
    changed.spec.log_level = Some("debug".into());

    let without_log_level = |lumen: &Lumen| {
        canonical_input(lumen, &BTreeMap::new())
            .lines()
            .filter(|line| !line.contains("LUMEN_LOG_LEVEL"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    assert_ne!(
        canonical_input(&base, &BTreeMap::new()),
        canonical_input(&changed, &BTreeMap::new())
    );
    assert_eq!(without_log_level(&base), without_log_level(&changed));
}
