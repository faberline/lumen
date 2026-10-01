use std::collections::BTreeMap;

use crate::operator::domain::lumen_spec::serving::{AuthMode, LogFormat, ServingSpec};
use crate::operator::domain::lumen_spec::topology::{ReshardPolicy, ShardMapSpec};
use crate::operator::domain::lumen_spec::{
    LumenSpec, PlacementSpec, MAX_BODY_LIMIT_BYTES, MIN_BODY_LIMIT_BYTES,
};

fn test_spec() -> LumenSpec {
    LumenSpec {
        image: "lumen:latest".into(),
        image_pull_policy: None,
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
