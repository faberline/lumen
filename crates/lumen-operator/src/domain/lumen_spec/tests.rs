use std::collections::BTreeMap;

use crate::domain::lumen_spec::serving::{AuthMode, LogFormat, ServingSpec};
use crate::domain::lumen_spec::topology::{ReshardPolicy, ShardMapSpec};
use crate::domain::lumen_spec::{
    EnvEntry, KeyRef, LumenSpec, PlacementSpec, MAX_BODY_LIMIT_BYTES, MIN_BODY_LIMIT_BYTES,
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
        extra_env: Vec::new(),
        extra_args: Vec::new(),
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

fn spec_with_env(entries: Vec<EnvEntry>) -> LumenSpec {
    let mut spec = test_spec();
    spec.extra_env = entries;
    spec
}

fn literal_entry(name: &str, value: &str) -> EnvEntry {
    EnvEntry {
        name: name.into(),
        value: Some(value.into()),
        ..Default::default()
    }
}

#[test]
fn validate_accepts_each_single_source_env_entry() {
    let secret = EnvEntry {
        name: "MY_SECRET".into(),
        secret_key_ref: Some(KeyRef {
            name: "s".into(),
            key: "k".into(),
        }),
        ..Default::default()
    };
    let config_map = EnvEntry {
        name: "MY_CM".into(),
        config_map_key_ref: Some(KeyRef {
            name: "c".into(),
            key: "k".into(),
        }),
        ..Default::default()
    };
    for entry in [literal_entry("MY_LITERAL", "x"), secret, config_map] {
        assert!(spec_with_env(vec![entry]).validate().is_ok());
    }
}

#[test]
fn validate_rejects_env_entry_with_zero_or_two_sources() {
    let none = EnvEntry {
        name: "MY_NONE".into(),
        ..Default::default()
    };
    let two = EnvEntry {
        name: "MY_TWO".into(),
        value: Some("x".into()),
        secret_key_ref: Some(KeyRef {
            name: "s".into(),
            key: "k".into(),
        }),
        ..Default::default()
    };
    for entry in [none, two] {
        let err = spec_with_env(vec![entry])
            .validate()
            .expect_err("must reject");
        assert!(err.contains("extraEnv[0]"), "{err}");
        assert!(err.contains("exactly one"), "{err}");
    }
}

#[test]
fn validate_rejects_reserved_env_names() {
    for name in [
        "LUMEN_AUTH",
        "LUMEN_PORT",
        "POD_NAME",
        "RAFT_ID",
        "SHARD_COUNT",
        "VIRTUAL_BUCKET_COUNT",
        "VOTER_COUNT",
        "REPLICAS_PER_SHARD",
    ] {
        let err = spec_with_env(vec![literal_entry(name, "x")])
            .validate()
            .expect_err(name);
        assert!(err.contains(name), "{err}");
        assert!(err.contains("operator owns"), "{err}");
    }
    assert!(spec_with_env(vec![literal_entry("MY_SETTING", "x")])
        .validate()
        .is_ok());
}

#[test]
fn validate_rejects_duplicate_env_names() {
    let err = spec_with_env(vec![literal_entry("MY_A", "1"), literal_entry("MY_A", "2")])
        .validate()
        .expect_err("duplicate names");
    assert!(err.contains("duplicate name (MY_A)"), "{err}");
}

#[test]
fn validate_rejects_bad_env_names() {
    for name in ["1BAD", "has space", ""] {
        let err = spec_with_env(vec![literal_entry(name, "x")])
            .validate()
            .expect_err(name);
        assert!(
            err.contains("is not a valid environment variable name"),
            "{err}"
        );
    }
}

#[test]
fn validate_rejects_non_flag_args() {
    for arg in ["foo", "-x", "--has space"] {
        let mut spec = test_spec();
        spec.extra_args = vec![arg.into()];
        assert!(spec.validate().is_err(), "{arg} must be rejected");
    }
    for arg in ["--ok", "--ok=1"] {
        let mut spec = test_spec();
        spec.extra_args = vec![arg.into()];
        assert!(spec.validate().is_ok(), "{arg} must be accepted");
    }
}

#[test]
fn validate_error_never_contains_the_value() {
    let entry = literal_entry("1BAD", "SENTINEL-LITERAL-7731");
    let err = spec_with_env(vec![entry])
        .validate()
        .expect_err("invalid name");
    assert!(!err.contains("SENTINEL-LITERAL-7731"), "{err}");
}
