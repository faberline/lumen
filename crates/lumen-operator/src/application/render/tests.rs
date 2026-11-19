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
        .expect("render")
        .into_iter()
        .find(|object| object["kind"] == "StatefulSet" && object["metadata"]["name"] == "search")
        .expect("rendered serving StatefulSet")["spec"]["template"]["spec"]["containers"][0]
        ["volumeMounts"]
        .as_array()
        .expect("serving volumeMounts")
        .clone()
}

#[test]
fn raft_pvc_template_carries_runtime_uid_and_authority_labels() {
    let lumen = single_replica();
    let objects = render(&lumen).expect("render");
    let sts = objects
        .iter()
        .find(|o| o["kind"] == "StatefulSet")
        .expect("StatefulSet");
    let vct = &sts["spec"]["volumeClaimTemplates"][0];
    let labels = &vct["metadata"]["labels"];

    assert_eq!(vct["metadata"]["name"], "raft");
    assert_eq!(labels["app.kubernetes.io/name"], "lumen");
    assert_eq!(labels["app.kubernetes.io/instance"], "search");
    assert_eq!(labels["app.kubernetes.io/component"], "server");
    assert_eq!(labels["app.kubernetes.io/managed-by"], "lumen-operator");
    assert_eq!(labels["app.kubernetes.io/part-of"], "lumen");
    assert_eq!(
        labels[crate::application::render::AUTHORITY_LABEL],
        "lumen-operator"
    );
    assert_eq!(
        labels[crate::application::render::RUNTIME_UID_LABEL],
        "wi-4025-impl"
    );
}

#[test]
fn backup_cronjob_and_pod_template_carry_runtime_uid_and_authority_labels() {
    let mut lumen = single_replica();
    lumen.spec.serving.backup = Some(crate::domain::lumen_spec::serving::ServingBackupSpec {
        policy: service_backup::ScheduledBackupPolicy {
            schedule: "0 2 * * *".into(),
            destination: "s3://backups/lumen".into(),
            retention_secs: Some(86400),
        },
        admin_token_secret: None,
    });
    let objects = render(&lumen).expect("render");
    let cron = objects
        .iter()
        .find(|o| o["kind"] == "CronJob")
        .expect("CronJob");

    let meta_labels = &cron["metadata"]["labels"];
    assert_eq!(meta_labels["app.kubernetes.io/name"], "lumen");
    assert_eq!(meta_labels["app.kubernetes.io/instance"], "search");
    assert_eq!(meta_labels["app.kubernetes.io/component"], "backup");
    assert_eq!(
        meta_labels["app.kubernetes.io/managed-by"],
        "lumen-operator"
    );
    assert_eq!(meta_labels["app.kubernetes.io/part-of"], "lumen");
    assert_eq!(
        meta_labels[crate::application::render::AUTHORITY_LABEL],
        "lumen-operator"
    );
    assert_eq!(
        meta_labels[crate::application::render::RUNTIME_UID_LABEL],
        "wi-4025-impl"
    );

    let pod_labels = &cron["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"];
    assert_eq!(pod_labels["app.kubernetes.io/name"], "lumen");
    assert_eq!(pod_labels["app.kubernetes.io/instance"], "search");
    assert_eq!(pod_labels["app.kubernetes.io/component"], "backup");
    assert_eq!(pod_labels["app.kubernetes.io/managed-by"], "lumen-operator");
    assert_eq!(pod_labels["app.kubernetes.io/part-of"], "lumen");
    assert_eq!(
        pod_labels[crate::application::render::AUTHORITY_LABEL],
        "lumen-operator"
    );
    assert_eq!(
        pod_labels[crate::application::render::RUNTIME_UID_LABEL],
        "wi-4025-impl"
    );
}

#[test]
fn recreation_and_similar_names_produce_distinct_runtime_uids() {
    let mut cr1 = single_replica();
    cr1.metadata.name = Some("search".into());
    cr1.metadata.namespace = Some("prod".into());
    cr1.metadata.uid = Some("uid-generation-1".into());

    let mut cr2 = single_replica();
    cr2.metadata.name = Some("search".into());
    cr2.metadata.namespace = Some("prod".into());
    cr2.metadata.uid = Some("uid-generation-2".into());

    let sts1 = render(&cr1)
        .expect("render")
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();
    let sts2 = render(&cr2)
        .expect("render")
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();

    let uid1 = &sts1["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]
        [crate::application::render::RUNTIME_UID_LABEL];
    let uid2 = &sts2["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]
        [crate::application::render::RUNTIME_UID_LABEL];
    assert_eq!(uid1, "uid-generation-1");
    assert_eq!(uid2, "uid-generation-2");
    assert_ne!(uid1, uid2);

    let mut cr_ab_c = single_replica();
    cr_ab_c.metadata.namespace = Some("a-b".into());
    cr_ab_c.metadata.name = Some("c".into());
    cr_ab_c.metadata.uid = Some("uid-ab-c".into());

    let mut cr_a_bc = single_replica();
    cr_a_bc.metadata.namespace = Some("a".into());
    cr_a_bc.metadata.name = Some("b-c".into());
    cr_a_bc.metadata.uid = Some("uid-a-bc".into());

    let sts_ab_c = render(&cr_ab_c)
        .expect("render")
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();
    let sts_a_bc = render(&cr_a_bc)
        .expect("render")
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();

    let uid_ab_c = &sts_ab_c["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]
        [crate::application::render::RUNTIME_UID_LABEL];
    let uid_a_bc = &sts_a_bc["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]
        [crate::application::render::RUNTIME_UID_LABEL];
    assert_ne!(uid_ab_c, uid_a_bc);
}

#[test]
fn negative_control_omitted_uid_renders_no_runtime_uid_label() {
    let mut cr = single_replica();
    cr.metadata.uid = None;

    let objects = render(&cr).expect("render");
    let sts = objects.iter().find(|o| o["kind"] == "StatefulSet").unwrap();
    let pvc_labels = &sts["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"];

    assert!(pvc_labels
        .get(crate::application::render::RUNTIME_UID_LABEL)
        .is_none());
    assert_eq!(
        pvc_labels[crate::application::render::AUTHORITY_LABEL],
        "lumen-operator"
    );
    assert_eq!(pvc_labels["app.kubernetes.io/instance"], "search");
    for (k, v) in pvc_labels.as_object().unwrap() {
        assert!(!v.as_str().unwrap().is_empty(), "label {k} has empty value");
    }
}

#[test]
fn helper_selector_and_matching_isolation_across_recreation() {
    use crate::application::render::identity::{
        data_artifact_labels, matches_runtime_selector, runtime_uid_selector,
    };

    let mut cr_old = single_replica();
    cr_old.metadata.uid = Some("uid-old-1111".into());

    let mut cr_new = single_replica();
    cr_new.metadata.uid = Some("uid-new-2222".into());

    let labels_old = data_artifact_labels(&cr_old, "server");
    let labels_new = data_artifact_labels(&cr_new, "server");

    let selector_new = runtime_uid_selector("search", "uid-new-2222");

    assert!(matches_runtime_selector(&labels_new, &selector_new));
    assert!(!matches_runtime_selector(&labels_old, &selector_new));

    let mut name_only_selector = BTreeMap::new();
    name_only_selector.insert(
        "app.kubernetes.io/instance".to_string(),
        "search".to_string(),
    );
    assert!(matches_runtime_selector(&labels_old, &name_only_selector));
    assert!(matches_runtime_selector(&labels_new, &name_only_selector));
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
        .expect("render")
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
    let rendered = serde_json::to_string(&render(&lumen).expect("render")).expect("rendered json");
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
    let rendered = serde_json::to_string(&render(&lumen).expect("render")).expect("rendered json");
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
    let rendered_a = serde_json::to_string(&render(&a).expect("render")).expect("rendered json");
    let rendered_b = serde_json::to_string(&render(&b).expect("render")).expect("rendered json");
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
        .expect("render")
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

fn serving_statefulset_of(lumen: &Lumen) -> Value {
    render(lumen)
        .expect("render")
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

#[test]
fn one_pod_statefulset_has_one_replica_and_one_raft_pvc() {
    let lumen = single_replica();
    let statefulset = serving_statefulset_of(&lumen);

    assert_eq!(statefulset["spec"]["replicas"].as_i64(), Some(1));

    let claims = statefulset["spec"]["volumeClaimTemplates"]
        .as_array()
        .expect("volumeClaimTemplates");
    assert_eq!(claims.len(), 1, "exactly one PVC template, got: {claims:?}");
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
        "missing the raft mount at /var/lib/lumen, got: {mounts:?}"
    );
    assert!(
        mounts.contains(&json!({
            "name": "raft",
            "mountPath": "/var/lib/lumen/data",
            "subPath": "data",
            "readOnly": false,
        })),
        "missing the raft data subPath mount, got: {mounts:?}"
    );
}

#[test]
fn one_pod_render_carries_no_ha_claim_in_labels_or_annotations() {
    fn string_values(map: &Value, out: &mut Vec<String>) {
        if let Some(object) = map.as_object() {
            for value in object.values() {
                if let Some(text) = value.as_str() {
                    out.push(text.to_lowercase());
                }
            }
        }
    }

    let mut values = Vec::new();
    for object in render(&single_replica()).expect("render") {
        string_values(&object["metadata"]["labels"], &mut values);
        string_values(&object["metadata"]["annotations"], &mut values);
        string_values(
            &object["spec"]["template"]["metadata"]["labels"],
            &mut values,
        );
        string_values(
            &object["spec"]["template"]["metadata"]["annotations"],
            &mut values,
        );
    }

    // Non-vacuity: the check must actually see label and annotation values.
    assert!(
        !values.is_empty(),
        "no label or annotation values were rendered"
    );
    for value in &values {
        for claim in ["highly available", "high availability", "quorum"] {
            assert!(
                !value.contains(claim),
                "rendered value claims `{claim}`: {value}"
            );
        }
    }
}

#[test]
fn one_pod_env_has_no_raft_peer_or_voter_keys() {
    let names = env_entry_names(&serving_container(&single_replica()));

    assert!(
        !names.iter().any(|name| name == "REPLICAS_PER_SHARD"),
        "one-Pod env must not carry REPLICAS_PER_SHARD, got: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name == "VOTER_COUNT"),
        "one-Pod env must not carry VOTER_COUNT, got: {names:?}"
    );
}

#[test]
fn three_voter_render_is_the_opposite_of_the_one_pod_render() {
    // Negative control: a pass must not come from a fixture that ignores
    // the replica count.
    let lumen = replicated(3);
    let statefulset = serving_statefulset_of(&lumen);
    assert_eq!(statefulset["spec"]["replicas"].as_i64(), Some(3));

    let names = env_entry_names(&serving_container(&lumen));
    assert!(
        names.iter().any(|name| name == "REPLICAS_PER_SHARD"),
        "got: {names:?}"
    );
    assert!(
        names.iter().any(|name| name == "VOTER_COUNT"),
        "got: {names:?}"
    );

    let claims = statefulset["spec"]["volumeClaimTemplates"]
        .as_array()
        .expect("volumeClaimTemplates");
    assert_eq!(claims[0]["metadata"]["name"], "raft");

    let mounts = serving_mounts(&lumen);
    assert!(
        !mounts
            .iter()
            .any(|mount| mount["mountPath"] == "/var/lib/lumen/data"),
        "the three-voter render must not carry the one-Pod data subPath mount, got: {mounts:?}"
    );
}

/// The parts of a rendered object that run at pod time: the pod spec of each
/// workload and the selector of each Service. The `app.kubernetes.io/managed-by`
/// label names the manager on every object; it is bookkeeping, not a runtime
/// dependency, so it is not checked here.
fn runtime_surfaces(object: &Value) -> Vec<&Value> {
    let mut surfaces = Vec::new();
    if let Some(pod) = object.pointer("/spec/template/spec") {
        surfaces.push(pod);
    }
    if let Some(pod) = object.pointer("/spec/jobTemplate/spec/template/spec") {
        surfaces.push(pod);
    }
    if object["kind"] == "Service" {
        surfaces.push(&object["spec"]["selector"]);
    }
    surfaces
}

/// A pod must not depend on the operator: no name, namespace or env var of the
/// operator may appear in what runs, so a restarted operator re-applies the same
/// desired state and the data plane never waits for it.
fn assert_operator_independent(objects: &[Value]) {
    // Operator values only. The env name `POD_NAMESPACE` alone is not checked:
    // the serving pod also has one, and it is not the operator's namespace.
    let forbidden = ["lumen-operator", "lumen-operator-system"];
    for object in objects {
        for surface in runtime_surfaces(object) {
            let text = surface.to_string();
            for needle in forbidden {
                assert!(
                    !text.contains(needle),
                    "rendered {} names the operator in its runtime ({needle})",
                    object["kind"]
                );
            }
        }
    }
}

#[test]
fn render_is_a_pure_function_of_the_cr() {
    let lumen = single_replica();
    assert_eq!(
        render(&lumen).expect("render"),
        render(&lumen).expect("render")
    );

    let statefulset = serving_statefulset_of(&lumen);
    let pod_meta = &statefulset["spec"]["template"]["metadata"];
    assert!(pod_meta.get("ownerReferences").is_none());
    assert!(pod_meta.get("finalizers").is_none());
}

#[test]
fn rendered_objects_never_name_the_operator() {
    assert_operator_independent(&render(&single_replica()).expect("render"));
}

#[test]
#[should_panic(expected = "names the operator")]
fn injecting_an_operator_env_var_fails_the_independence_check() {
    let mut objects = render(&single_replica()).expect("render");
    let statefulset = objects
        .iter_mut()
        .find(|object| object["kind"] == "StatefulSet")
        .expect("rendered serving StatefulSet");
    let container = &mut statefulset["spec"]["template"]["spec"]["containers"][0];
    let env = container
        .as_object_mut()
        .expect("container object")
        .entry("env")
        .or_insert_with(|| json!([]));
    env.as_array_mut()
        .expect("env array")
        .push(json!({"name": "POD_NAMESPACE", "value": "lumen-operator-system"}));
    assert_operator_independent(&objects);
}

/// A spec with `shards` shards, `replicas` members per shard and `voters` voters.
fn sharded(shards: u32, replicas: u32, voters: u32) -> Lumen {
    let mut lumen = single_replica();
    lumen.spec.shard_count = shards;
    lumen.spec.replicas_per_shard = replicas;
    lumen.spec.voter_count = voters;
    lumen
}

fn objs_of(lumen: &Lumen, kind: &str) -> Vec<Value> {
    render(lumen)
        .expect("render")
        .into_iter()
        .filter(|object| object["kind"] == kind)
        .collect()
}

fn statefulset(lumen: &Lumen, name: &str) -> Value {
    objs_of(lumen, "StatefulSet")
        .into_iter()
        .find(|object| object["metadata"]["name"] == name)
        .unwrap_or_else(|| panic!("no StatefulSet named {name}"))
}

/// The hard pod anti-affinity rule of a rendered StatefulSet.
fn hard_affinity(sts: &Value) -> Value {
    sts["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]
        ["requiredDuringSchedulingIgnoredDuringExecution"][0]
        .clone()
}

/// True when every pair of `selector` is present in `labels`.
fn selector_matches(selector: &Value, labels: &Value) -> bool {
    selector
        .as_object()
        .expect("selector is a label map")
        .iter()
        .all(|(key, value)| labels.get(key) == Some(value))
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
    let mut names: Vec<String> = statefulsets
        .iter()
        .map(|sts| sts["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, ["search-s0", "search-s1", "search-s2"]);

    for shard in 0..3 {
        let name = format!("search-s{shard}");
        let sts = statefulset(&lumen, &name);
        let index = shard.to_string();
        assert_eq!(sts["metadata"]["labels"]["lumen.dev/shard"], index.as_str());
        assert_eq!(
            sts["spec"]["selector"]["matchLabels"]["lumen.dev/shard"],
            index.as_str()
        );
        assert_eq!(
            sts["spec"]["template"]["metadata"]["labels"]["lumen.dev/shard"],
            index.as_str()
        );
    }
}

#[test]
fn shard_pod_selects_only_its_own_shard() {
    let lumen = sharded(2, 1, 1);
    let s0 = hard_affinity(&statefulset(&lumen, "search-s0"));
    let s1 = hard_affinity(&statefulset(&lumen, "search-s1"));

    assert_eq!(s0["labelSelector"]["matchLabels"]["lumen.dev/shard"], "0");
    assert_eq!(s1["labelSelector"]["matchLabels"]["lumen.dev/shard"], "1");
    for rule in [&s0, &s1] {
        assert_eq!(rule["topologyKey"], "kubernetes.io/hostname");
        assert!(
            rule.get("namespaceSelector").is_none(),
            "a shard rule applies to the pod's own namespace only: {rule}"
        );
    }
}

#[test]
fn different_shards_may_share_a_node() {
    let lumen = sharded(2, 1, 1);
    let s0 = statefulset(&lumen, "search-s0");
    let s1 = statefulset(&lumen, "search-s1");
    let shard0_rule = hard_affinity(&s0)["labelSelector"]["matchLabels"].clone();
    let shard0_pod = s0["spec"]["template"]["metadata"]["labels"].clone();
    let shard1_pod = s1["spec"]["template"]["metadata"]["labels"].clone();

    assert!(selector_matches(&shard0_rule, &shard0_pod));
    assert!(
        !selector_matches(&shard0_rule, &shard1_pod),
        "a pod of shard 1 must not be excluded from a node by shard 0's rule"
    );

    // The old runtime-wide selector would match shard 1's pods, so the test
    // above fails if the shard label is dropped from the rule.
    let runtime_wide = serde_json::json!({
        "app.kubernetes.io/name": "lumen",
        "app.kubernetes.io/component": "server",
    });
    assert!(selector_matches(&runtime_wide, &shard1_pod));
}

#[test]
fn shard_env_reads_the_shard_label() {
    let lumen = sharded(3, 1, 1);
    let sts = statefulset(&lumen, "search-s2");
    let env = sts["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("container env");
    let entry = env
        .iter()
        .find(|e| e["name"] == "LUMEN_SHARD_INDEX")
        .expect("LUMEN_SHARD_INDEX env");
    assert_eq!(
        entry["valueFrom"]["fieldRef"]["fieldPath"],
        "metadata.labels['lumen.dev/shard']"
    );
}

#[test]
fn three_voter_shard_has_hard_zone_spread_scoped_to_the_shard() {
    let lumen = sharded(2, 3, 3);
    let sts = statefulset(&lumen, "search-s0");
    assert_eq!(sts["spec"]["replicas"], 3);

    let spreads = sts["spec"]["template"]["spec"]["topologySpreadConstraints"]
        .as_array()
        .expect("spread constraints");
    let hard: Vec<&Value> = spreads
        .iter()
        .filter(|s| s["whenUnsatisfiable"] == "DoNotSchedule")
        .collect();
    assert_eq!(hard.len(), 1, "one hard zone rule for a three-voter shard");
    assert_eq!(hard[0]["topologyKey"], "topology.kubernetes.io/zone");
    assert_eq!(hard[0]["maxSkew"], 1);
    assert_eq!(
        hard[0]["labelSelector"]["matchLabels"]["lumen.dev/shard"],
        "0"
    );

    let soft = spreads
        .iter()
        .filter(|s| s["whenUnsatisfiable"] == "ScheduleAnyway")
        .count();
    assert!(
        soft >= 2,
        "runtime-wide spreads stay ScheduleAnyway: {spreads:?}"
    );
}

#[test]
fn single_voter_shard_has_no_hard_zone_spread() {
    let lumen = sharded(2, 1, 1);
    let sts = statefulset(&lumen, "search-s0");
    let spreads = sts["spec"]["template"]["spec"]["topologySpreadConstraints"]
        .as_array()
        .expect("spread constraints");
    assert!(
        spreads
            .iter()
            .all(|s| s["whenUnsatisfiable"] != "DoNotSchedule"),
        "a one-voter shard gets no hard zone rule: {spreads:?}"
    );
}

#[test]
fn pdb_per_shard_uses_quorum_safe_max_unavailable() {
    let lumen = sharded(2, 3, 3);
    let pdbs = objs_of(&lumen, "PodDisruptionBudget");
    let names: Vec<String> = pdbs
        .iter()
        .map(|p| p["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"search-s0".to_string()), "{names:?}");
    assert!(names.contains(&"search-s1".to_string()), "{names:?}");
    assert!(!names.contains(&"search".to_string()), "{names:?}");

    for pdb in &pdbs {
        assert_eq!(pdb["spec"]["maxUnavailable"], 1);
    }
    let pdb1 = pdbs
        .iter()
        .find(|p| p["metadata"]["name"] == "search-s1")
        .expect("search-s1 PDB");
    assert_eq!(
        pdb1["spec"]["selector"]["matchLabels"]["lumen.dev/shard"],
        "1"
    );

    let five = sharded(2, 5, 5);
    let pdb = objs_of(&five, "PodDisruptionBudget")
        .into_iter()
        .find(|p| p["metadata"]["name"] == "search-s0")
        .expect("search-s0 PDB");
    assert_eq!(pdb["spec"]["maxUnavailable"], 2);
}

#[test]
fn prunes_old_single_statefulset_and_pdb_when_sharded() {
    let has = |targets: &[service_k8s::service::PruneTarget], kind: &str, name: &str| {
        targets.iter().any(|t| t.kind() == kind && t.name() == name)
    };

    let mut sharded_policy = sharded(2, 1, 1);
    sharded_policy.spec.network_policy = true;
    let targets = super::prunes(&sharded_policy);
    assert!(has(&targets, "StatefulSet", "search"), "{targets:?}");
    assert!(
        has(&targets, "PodDisruptionBudget", "search"),
        "{targets:?}"
    );
    assert!(!has(&targets, "NetworkPolicy", "search"), "{targets:?}");

    let mut single_policy = single_replica();
    single_policy.spec.network_policy = true;
    assert!(super::prunes(&single_policy).is_empty());

    let single_open = single_replica();
    let targets = super::prunes(&single_open);
    assert!(has(&targets, "NetworkPolicy", "search"), "{targets:?}");
    assert!(!has(&targets, "StatefulSet", "search"), "{targets:?}");
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

    assert_eq!(
        pod["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]
            ["topologyKey"],
        "kubernetes.io/hostname"
    );

    let terms = &pod["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
        ["nodeSelectorTerms"];
    assert_eq!(
        terms[0]["matchExpressions"][0]["key"],
        "topology.kubernetes.io/zone"
    );
    assert_eq!(terms[0]["matchExpressions"][0]["operator"], "In");
    assert_eq!(
        terms[0]["matchExpressions"][0]["values"],
        json!(["us-central1-a", "us-central1-b", "us-central1-c"])
    );

    let spreads = pod["topologySpreadConstraints"]
        .as_array()
        .expect("spreads");
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
    let spreads = pod["topologySpreadConstraints"]
        .as_array()
        .expect("spreads");
    let zone_spread = spreads
        .iter()
        .find(|s| s["topologyKey"] == "topology.kubernetes.io/zone")
        .expect("zone spread");
    assert_eq!(zone_spread["whenUnsatisfiable"], "ScheduleAnyway");
}
