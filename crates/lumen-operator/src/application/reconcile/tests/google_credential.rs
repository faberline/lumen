//! #11: the rendered serving runtime carries no Google credential material.
//! The operator and backup ServiceAccounts belong to the deployer, so the
//! annotation check skips only the backup ServiceAccount.

use serde_json::Value;

use crate::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::application::render;

const GOOGLE_MARKERS: [&str; 4] = [
    concat!("GOOGLE_APPLICATION_", "CREDENTIALS"),
    "iam.gke.io/gcp-service-account",
    "credentials.json",
    "keyfile",
];

/// True when a rendered object carries Google credential material. The
/// backup ServiceAccount is the deployer's, so it is checked for the key
/// markers but not for the annotation.
fn leaks_google_material(objects: &[Value], serving_sa: &str) -> bool {
    objects.iter().any(|object| {
        let kind = object
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let name = object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let is_backup_sa = kind == "ServiceAccount" && name != serving_sa;
        let text = object.to_string();
        GOOGLE_MARKERS
            .iter()
            .filter(|marker| !(is_backup_sa && **marker == "iam.gke.io/gcp-service-account"))
            .any(|marker| text.contains(marker))
    })
}

#[test]
fn rendered_runtime_carries_no_google_credential_material() {
    let lumen = hpa_test_lumen("search", "acme", 1, 1);
    let objects = render::render(&lumen);
    let serving_sa = render::serving_service_account_name(&lumen);

    assert!(
        objects.iter().any(|o| o["kind"] == "StatefulSet"),
        "the render must contain the serving StatefulSet, or the check is vacuous"
    );
    assert!(
        objects.iter().any(|o| o["kind"] == "ServiceAccount"),
        "the render must contain a ServiceAccount, or the check is vacuous"
    );
    assert!(
        !leaks_google_material(&objects, &serving_sa),
        "the rendered runtime must not carry Google credential material"
    );
}

#[test]
fn google_annotation_set_by_the_deployer_is_detected_by_the_same_check() {
    let mut lumen = hpa_test_lumen("search", "acme", 1, 1);
    lumen.spec.service_account_annotations.insert(
        "iam.gke.io/gcp-service-account".into(),
        "x@p.iam.gserviceaccount.com".into(),
    );
    let objects = render::render(&lumen);
    let serving_sa = render::serving_service_account_name(&lumen);

    let serving = objects
        .iter()
        .find(|o| {
            o["kind"] == "ServiceAccount"
                && o.pointer("/metadata/name").and_then(Value::as_str) == Some(serving_sa.as_str())
        })
        .expect("the serving ServiceAccount is rendered");
    assert!(
        serving
            .to_string()
            .contains("iam.gke.io/gcp-service-account"),
        "the deployer's annotation must reach the serving ServiceAccount"
    );
    assert!(
        leaks_google_material(&objects, &serving_sa),
        "the check must detect a Google annotation, or the first test proves nothing"
    );
}
