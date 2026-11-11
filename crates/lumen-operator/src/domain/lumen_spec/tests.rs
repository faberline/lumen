use std::collections::BTreeMap;

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
        runtime: None,
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

/// Every `env = "..."` option of `lumen serve` has a classification, and every
/// classification names a real option. A new serve option fails here until it
/// is classified as typed, guarded escape hatch or reserved.
#[test]
fn serve_option_inventory_classifies_every_serve_option() {
    use crate::domain::lumen_spec::runtime::SERVE_OPTION_INVENTORY;

    let source = include_str!("../../../../lumen/src/bin/lumen/cli/serve.rs");
    let mut options: Vec<&str> = Vec::new();
    for part in source.split("env = \"").skip(1) {
        let name = part.split('"').next().expect("split yields one part");
        options.push(name);
    }
    assert!(!options.is_empty(), "no serve options found in the source");

    for name in &options {
        assert!(
            SERVE_OPTION_INVENTORY.iter().any(|(n, _)| n == name),
            "serve option `{name}` has no classification in SERVE_OPTION_INVENTORY"
        );
    }
    for (name, _) in SERVE_OPTION_INVENTORY {
        assert!(
            options.contains(name),
            "SERVE_OPTION_INVENTORY lists `{name}`, which serve.rs no longer defines"
        );
    }
}

/// The four typed runtime settings serialize under exactly these camelCase
/// keys, so the CRD and the merged spec agree on the names.
#[test]
fn every_typed_runtime_option_has_a_field() {
    use crate::domain::lumen_spec::runtime::RuntimeSettings;
    use serde_json::json;

    let runtime = RuntimeSettings {
        snapshot_secs: Some(1),
        grace_secs: Some(2),
        otlp_endpoint: Some("http://otlp:4317".into()),
        bootstrap_max_bytes_per_sec: Some(3),
    };
    let value = serde_json::to_value(&runtime).expect("serializes");
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "bootstrapMaxBytesPerSec",
            "graceSecs",
            "otlpEndpoint",
            "snapshotSecs"
        ]
    );
    assert_eq!(value["bootstrapMaxBytesPerSec"], json!(3));
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
