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
fn raft_pvc_template_carries_runtime_uid_and_authority_labels() {
    let lumen = single_replica();
    let objects = render(&lumen);
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
    let objects = render(&lumen);
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
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();
    let sts2 = render(&cr2)
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
        .into_iter()
        .find(|o| o["kind"] == "StatefulSet")
        .unwrap();
    let sts_a_bc = render(&cr_a_bc)
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

    let objects = render(&cr);
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
