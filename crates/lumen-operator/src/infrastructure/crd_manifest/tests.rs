use serde_json::json;

use crate::domain::lumen_spec::serving::AuthMode;
use crate::infrastructure::crd_manifest::{
    crd_yaml, fleet_crd_yaml, lumen_crd_yaml, FORBIDDEN_CEL_OPERATOR,
};

/// The typed ServiceAccount list is a schema contract: each subject must be
/// an object with both `namespace` and `name` required, so an incomplete
/// subject is refused by the API server, not only by the Rust type.
#[test]
fn the_lumen_crd_offers_typed_allowed_service_accounts() {
    let yaml = lumen_crd_yaml();
    assert!(yaml.contains("allowedServiceAccounts"), "{yaml}");
    let doc: serde_json::Value = serde_yaml::from_str(&yaml).expect("CRD yaml parses");
    let access = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        ["properties"]["access"];
    assert!(
        access["required"]
            .as_array()
            .expect("access.required")
            .contains(&json!("allowedServiceAccounts")),
        "{access}"
    );
    let items = &access["properties"]["allowedServiceAccounts"]["items"];
    let required = items["required"].as_array().expect("items.required");
    assert!(required.contains(&json!("namespace")), "{items}");
    assert!(required.contains(&json!("name")), "{items}");
    assert!(items["properties"]["namespace"].is_object(), "{items}");
    assert!(items["properties"]["name"].is_object(), "{items}");
}

/// `k8s/operator/crd.yaml` is regenerated from the Rust types, never edited
/// by hand: the committed artifact must equal the generator's output.
#[test]
fn the_committed_crd_matches_the_generator() {
    let committed = include_str!("../../../../../k8s/operator/crd.yaml");
    assert_eq!(committed.trim_end(), crd_yaml().trim_end());
}

/// A namespaced fleet could not legally own objects in other namespaces:
/// Kubernetes rejects cross-namespace owner references and its garbage
/// collector deletes the dependents. Cluster scope is the load-bearing
/// property of this CRD, so it is pinned.
#[test]
fn the_fleet_crd_is_cluster_scoped() {
    let yaml = fleet_crd_yaml();
    assert!(yaml.contains("scope: Cluster"), "{yaml}");
    assert!(yaml.contains("lumenfleets"), "{yaml}");
}

/// The override is a free-form object, which a structural CRD schema only
/// accepts with this extension — without it the API server prunes every
/// key of every override and each tenant silently gets the bare defaults.
#[test]
fn the_override_survives_structural_schema_pruning() {
    let yaml = fleet_crd_yaml();
    assert!(
        yaml.contains("x-kubernetes-preserve-unknown-fields"),
        "{yaml}"
    );
    // The defaults are fully schema-validated, so the platform team's own
    // typo is still caught at `kubectl apply`.
    assert!(yaml.contains("shardCount"), "{yaml}");
}

/// #2872 AC3. The identity-audience rule was the only CEL rule this CRD
/// carried, and it guarded a field that no longer exists — so the check
/// that survives is the one on the *shape* of any rule added later.
///
/// `!= null` cannot be caught by asserting on rendered YAML text (see
/// [`FORBIDDEN_CEL_OPERATOR`]); it only fails at the API server. This test
/// is the local half. `kubectl apply --dry-run=server` is the other.
#[test]
fn no_surviving_cel_rule_names_a_retired_field_or_uses_a_forbidden_operator() {
    let yaml = crd_yaml();
    for retired in ["identityAudiences", "identities", "tokensSecret"] {
        assert!(
            !yaml.contains(retired),
            "retired field `{retired}` must not survive in the CRD: {yaml}"
        );
    }
    assert!(!yaml.contains(FORBIDDEN_CEL_OPERATOR), "{yaml}");
}

/// The CSI transport is gone (#2764), and the retirement has to be visible
/// in the artifact operators actually apply — a field left in the CRD is a
/// field somebody sets.
#[test]
fn the_crd_no_longer_offers_a_csi_token_source() {
    let yaml = crd_yaml();
    assert!(!yaml.contains("tokensSecretProviderClass"), "{yaml}");
    assert!(!yaml.contains("SecretProviderClass"), "{yaml}");
}

/// A fleet whose `Lumen` CRD is missing applies cleanly and then fails
/// every instance it declares, so the two CRDs ship as one document.
#[test]
fn one_apply_installs_both_custom_resources() {
    let yaml = crd_yaml();
    assert!(yaml.contains("name: lumens.lumen.dev"), "{yaml}");
    assert!(yaml.contains("name: lumenfleets.lumen.dev"), "{yaml}");
    assert_eq!(
        yaml.matches("\n---\n").count(),
        1,
        "exactly one document separator, so `kubectl apply -f` sees two objects"
    );
}

/// R4/AC9: omitting `spec.auth` must not deploy an open API.
#[test]
fn auth_defaults_to_required() {
    assert_eq!(AuthMode::default(), AuthMode::Required);
    assert_eq!(AuthMode::default().as_env(), "required");
}

/// The per-instance runtime settings reach the API server: the `Lumen` CRD
/// carries them, and the fleet carries them under `defaults` and `instances[]`.
#[test]
fn the_runtime_field_survives_structural_schema_pruning() {
    let lumen = lumen_crd_yaml();
    assert!(lumen.contains("snapshotSecs"), "{lumen}");
    let fleet = fleet_crd_yaml();
    assert!(fleet.matches("snapshotSecs").count() >= 2, "{fleet}");
    assert!(fleet.contains("bootstrapMaxBytesPerSec"), "{fleet}");
}
