use std::collections::BTreeMap;

use crate::domain::lumen_spec::access::{AccessSpec, ServiceAccountRef};
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

// --- typed whole-runtime access (#341) ---

fn sa(namespace: &str, name: &str) -> ServiceAccountRef {
    ServiceAccountRef {
        namespace: namespace.into(),
        name: name.into(),
    }
}

fn with_access(entries: Vec<ServiceAccountRef>) -> LumenSpec {
    let mut spec = test_spec();
    spec.access = Some(AccessSpec {
        allowed_service_accounts: entries,
    });
    spec
}

#[test]
fn access_accepts_exact_namespace_and_name() {
    let spec = with_access(vec![sa("app-a", "search-client"), sa("app-b", "client.v2")]);
    assert_eq!(spec.validate(), Ok(()));
}

#[test]
fn access_accepts_absent_and_explicit_empty_list() {
    assert_eq!(test_spec().validate(), Ok(()));
    assert_eq!(with_access(vec![]).validate(), Ok(()));
}

/// Negative control: if the checks in `AccessSpec::validate` are removed,
/// every value below is accepted and this test fails.
#[test]
fn access_rejects_wildcards_groups_and_usernames() {
    let long = "a".repeat(64);
    let bad = [
        sa("*", "search-client"),
        sa("app-*", "search-client"),
        sa("app-a", "*"),
        sa("system:serviceaccounts", "search-client"),
        sa("app-a", "system:serviceaccount:app-a:search-client"),
        sa("App-A", "search-client"),
        sa("app-a", ""),
        sa("", "search-client"),
        sa(&long, "search-client"),
        sa("-a", "search-client"),
    ];
    for entry in bad {
        let spec = with_access(vec![entry.clone()]);
        match spec.validate() {
            Err(message) => assert!(
                message.contains("access.allowedServiceAccounts[0]"),
                "{entry:?}: {message}"
            ),
            Ok(()) => panic!("{entry:?} must be rejected"),
        }
    }
}

#[test]
fn access_rejects_a_duplicate_entry() {
    let spec = with_access(vec![sa("app-a", "client"), sa("app-a", "client")]);
    let message = spec.validate().expect_err("duplicate must be rejected");
    assert!(message.contains("listed twice"), "{message}");
    assert!(message.contains("[1]"), "{message}");
}

#[test]
fn access_round_trips_through_json_as_camel_case() {
    let spec = with_access(vec![sa("app-a", "search-client")]);
    let json = serde_json::to_value(&spec).expect("serialize");
    assert_eq!(
        json["access"]["allowedServiceAccounts"],
        serde_json::json!([{"namespace": "app-a", "name": "search-client"}])
    );
    let back: LumenSpec = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.access, spec.access);

    let absent = serde_json::to_value(test_spec()).expect("serialize");
    assert!(absent.get("access").is_none(), "{absent}");
}
