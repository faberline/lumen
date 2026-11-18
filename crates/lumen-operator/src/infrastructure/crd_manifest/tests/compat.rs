use serde_json::json;

use crate::infrastructure::crd_manifest::{fleet_crd_yaml, lumen_crd_yaml};

/// The N-1 CRD snapshot. It is the output of `lumen_crd_yaml()` taken once;
/// never edit it by hand.
const N_MINUS_1_SNAPSHOT: &str = include_str!("../testdata/lumen-crd-n-minus-1.yaml");

fn crd_doc(yaml: &str) -> serde_json::Value {
    serde_yaml::from_str(yaml).expect("CRD yaml parses")
}

/// The `openAPIV3Schema` of the single version of one CRD.
fn first_version_schema(crd_yaml: &str) -> serde_json::Value {
    let schema = &crd_doc(crd_yaml)["spec"]["versions"][0]["schema"]["openAPIV3Schema"];
    assert!(
        !schema.is_null(),
        "CRD has no spec.versions[0].schema.openAPIV3Schema"
    );
    schema.clone()
}

/// Compare an older CRD schema (`old`) with the current one (`new`).
/// Return one message per violation; an empty Vec means "only additions".
pub(super) fn schema_compat_violations(
    old: &serde_json::Value,
    new: &serde_json::Value,
) -> Vec<String> {
    let mut out = Vec::new();
    walk_schema(old, new, "", &mut out);
    out
}

fn walk_schema(
    old: &serde_json::Value,
    new: &serde_json::Value,
    path: &str,
    out: &mut Vec<String>,
) {
    let empty = serde_json::Map::new();
    let old_props = old["properties"].as_object().unwrap_or(&empty);
    let new_props = new["properties"].as_object().unwrap_or(&empty);
    for (name, old_prop) in old_props {
        let here = join_path(path, name);
        match new_props.get(name) {
            None => out.push(format!("removed: {here}")),
            Some(new_prop) => {
                if old_prop["type"] != new_prop["type"] {
                    out.push(format!(
                        "type changed: {here} ({} -> {})",
                        type_name(&old_prop["type"]),
                        type_name(&new_prop["type"])
                    ));
                }
                walk_schema(old_prop, new_prop, &here, out);
            }
        }
    }

    let old_required = required_names(old);
    for name in required_names(new) {
        if !old_required.contains(&name) {
            out.push(format!("new required: {}", join_path(path, name)));
        }
    }

    if let (Some(old_items), Some(new_items)) = (old.get("items"), new.get("items")) {
        walk_schema(old_items, new_items, &format!("{path}[]"), out);
    }
}

fn join_path(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

fn type_name(value: &serde_json::Value) -> &str {
    value.as_str().unwrap_or("none")
}

fn required_names(schema: &serde_json::Value) -> Vec<&str> {
    schema["required"]
        .as_array()
        .map(|names| names.iter().filter_map(serde_json::Value::as_str).collect())
        .unwrap_or_default()
}

/// Assert the fields that a CRD-first handover depends on, for one CRD.
fn assert_identity(doc: &serde_json::Value, scope: &str, kind: &str, label: &str) {
    let spec = &doc["spec"];
    assert_eq!(spec["group"], "lumen.dev", "{label}");
    assert_eq!(spec["scope"], scope, "{label}");
    assert_eq!(spec["names"]["kind"], kind, "{label}");
    let versions = spec["versions"].as_array().expect("versions is an array");
    assert_eq!(versions.len(), 1, "{label}");
    assert_eq!(versions[0]["name"], "v1alpha1", "{label}");
    assert_eq!(versions[0]["served"], true, "{label}");
    assert_eq!(versions[0]["storage"], true, "{label}");
}

#[test]
fn lumen_crd_identity_is_unchanged_from_n_minus_1() {
    assert_identity(
        &crd_doc(N_MINUS_1_SNAPSHOT),
        "Namespaced",
        "Lumen",
        "n-1 snapshot",
    );
    assert_identity(
        &crd_doc(&lumen_crd_yaml()),
        "Namespaced",
        "Lumen",
        "current lumen crd",
    );
}

#[test]
fn fleet_crd_identity_is_unchanged() {
    assert_identity(
        &crd_doc(&fleet_crd_yaml()),
        "Cluster",
        "LumenFleet",
        "current fleet crd",
    );
}

#[test]
fn current_lumen_schema_only_adds_to_the_n_minus_1_schema() {
    let old = first_version_schema(N_MINUS_1_SNAPSHOT);
    let new = first_version_schema(&lumen_crd_yaml());
    let violations = schema_compat_violations(&old, &new);
    assert!(violations.is_empty(), "{violations:#?}");
}

#[test]
fn diff_reports_a_property_that_was_removed() {
    let old = first_version_schema(N_MINUS_1_SNAPSHOT);
    let mut new = old.clone();
    new["properties"]["spec"]["properties"]
        .as_object_mut()
        .expect("spec properties")
        .remove("shardCount");
    let violations = schema_compat_violations(&old, &new);
    assert_eq!(violations, vec!["removed: spec.shardCount".to_string()]);
}

#[test]
fn diff_reports_a_property_whose_type_changed() {
    let old = first_version_schema(N_MINUS_1_SNAPSHOT);
    let mut new = old.clone();
    new["properties"]["spec"]["properties"]["shardCount"]["type"] = json!("string");
    let violations = schema_compat_violations(&old, &new);
    assert_eq!(
        violations,
        vec!["type changed: spec.shardCount (integer -> string)".to_string()]
    );
}

#[test]
fn diff_reports_a_new_required_field() {
    let old = first_version_schema(N_MINUS_1_SNAPSHOT);
    let mut new = old.clone();
    new["properties"]["spec"]["required"] = json!(["image", "brandNewField"]);
    new["properties"]["spec"]["properties"]["brandNewField"] = json!({"type": "string"});
    let violations = schema_compat_violations(&old, &new);
    assert_eq!(
        violations,
        vec!["new required: spec.brandNewField".to_string()]
    );
}

#[test]
fn diff_allows_a_property_that_was_added() {
    let old = first_version_schema(N_MINUS_1_SNAPSHOT);
    let mut new = old.clone();
    new["properties"]["spec"]["properties"]["brandNewField"] = json!({"type": "string"});
    let violations = schema_compat_violations(&old, &new);
    assert!(violations.is_empty(), "{violations:#?}");
}
