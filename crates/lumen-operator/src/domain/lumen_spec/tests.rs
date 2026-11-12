use std::collections::BTreeMap;

use serde_json::json;

use crate::domain::lumen_spec::access::{AccessSpec, ServiceAccountSubject};
use crate::domain::lumen_spec::serving::{AuthMode, LogFormat, ServingSpec};
use crate::domain::lumen_spec::topology::{ReshardPolicy, ShardMapSpec};
use crate::domain::lumen_spec::{
    LumenSpec, PlacementSpec, MAX_BODY_LIMIT_BYTES, MIN_BODY_LIMIT_BYTES,
};

fn test_spec() -> LumenSpec {
    LumenSpec {
        image: "lumen:latest".into(),
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
    }
}

#[test]
fn validate_accepts_valid_direct_gce_machine_type() {
    let mut spec = test_spec();
    assert!(spec.validate().is_ok());

    spec.placement.initial_machine_type = "n2-standard-4".into();
    assert!(spec.validate().is_ok());

    spec.placement.initial_machine_type = "c2-standard-8".into();
    assert!(spec.validate().is_ok());
}

#[test]
fn validate_rejects_service_tier_machine_types() {
    let mut spec = test_spec();
    for invalid in ["lumen-premium", "bronze", "small", "tier-1", "large"] {
        spec.placement.initial_machine_type = invalid.into();
        let err = spec.validate().expect_err("expected error");
        assert!(
            err.contains("initialMachineType"),
            "error message should name initialMachineType: {err}"
        );
    }
}

#[test]
fn validate_accepts_omitted_or_in_range_body_limit() {
    let mut spec = test_spec();
    assert!(spec.validate().is_ok());

    spec.body_limit_bytes = Some(MIN_BODY_LIMIT_BYTES);
    assert!(spec.validate().is_ok());

    spec.body_limit_bytes = Some(8 * 1024 * 1024);
    assert!(spec.validate().is_ok());

    spec.body_limit_bytes = Some(MAX_BODY_LIMIT_BYTES);
    assert!(spec.validate().is_ok());
}

#[test]
fn validate_rejects_out_of_range_body_limit() {
    let mut spec = test_spec();
    for invalid in [0, 512, MIN_BODY_LIMIT_BYTES - 1, MAX_BODY_LIMIT_BYTES + 1] {
        spec.body_limit_bytes = Some(invalid);
        let err = spec.validate().expect_err("expected error");
        assert!(
            err.contains("bodyLimitBytes"),
            "error message should name bodyLimitBytes: {err}"
        );
        assert!(
            err.contains(&MIN_BODY_LIMIT_BYTES.to_string()),
            "error message should name lower bound: {err}"
        );
        assert!(
            err.contains(&MAX_BODY_LIMIT_BYTES.to_string()),
            "error message should name upper bound: {err}"
        );
    }
}

fn subject(namespace: &str, name: &str) -> ServiceAccountSubject {
    ServiceAccountSubject {
        namespace: namespace.into(),
        name: name.into(),
    }
}

#[test]
fn validate_accepts_exact_service_account_subjects() {
    let mut spec = test_spec();
    spec.access = Some(AccessSpec {
        allowed_service_accounts: vec![subject("apps", "web"), subject("team-a.prod", "svc-1")],
    });
    assert!(spec.validate().is_ok());
}

#[test]
fn validate_rejects_wildcard_group_raw_username_and_incomplete_subjects() {
    let invalid = [
        ("*", "web"),
        ("apps", "*"),
        ("apps", ""),
        ("", "web"),
        ("system:serviceaccounts", "web"),
        ("apps", "system:serviceaccount:apps:web"),
        ("apps", "a/b"),
        ("Apps", "web"),
    ];
    for (namespace, name) in invalid {
        let mut spec = test_spec();
        spec.access = Some(AccessSpec {
            allowed_service_accounts: vec![subject(namespace, name)],
        });
        let err = spec.validate().expect_err("expected error");
        assert!(
            err.contains("allowedServiceAccounts"),
            "error message should name allowedServiceAccounts for ({namespace}, {name}): {err}"
        );
    }
}

#[test]
fn deserialization_rejects_incomplete_group_and_unknown_subject_fields() {
    for value in [
        json!({"allowedServiceAccounts": [{"namespace": "apps"}]}),
        json!({"allowedServiceAccounts": [{"name": "web"}]}),
        json!({"allowedServiceAccounts": [{"namespace": "a", "name": "b", "group": "g"}]}),
    ] {
        assert!(
            serde_json::from_value::<AccessSpec>(value.clone()).is_err(),
            "must reject {value}"
        );
    }
}

#[test]
fn explicit_empty_list_is_deny_all_and_round_trips() {
    let access: AccessSpec =
        serde_json::from_value(json!({"allowedServiceAccounts": []})).expect("parse");
    assert_eq!(
        access,
        AccessSpec {
            allowed_service_accounts: vec![]
        }
    );
    assert_eq!(
        serde_json::to_value(&access).expect("serialize"),
        json!({"allowedServiceAccounts": []})
    );

    let mut spec = test_spec();
    spec.access = Some(access.clone());
    let round: LumenSpec =
        serde_json::from_value(serde_json::to_value(&spec).expect("serialize spec"))
            .expect("parse spec");
    assert_eq!(round.access, Some(access));
}

#[test]
fn absent_access_deserializes_to_none() {
    let spec: LumenSpec =
        serde_json::from_value(json!({"image": "lumen:latest"})).expect("parse spec");
    assert_eq!(spec.access, None);
    assert!(serde_json::to_value(&spec)
        .expect("serialize spec")
        .get("access")
        .is_none());
}
