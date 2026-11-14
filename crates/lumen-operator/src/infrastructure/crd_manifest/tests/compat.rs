use serde_json::{json, Value};

use crate::infrastructure::crd_manifest::{fleet_crd_yaml, lumen_crd_yaml};

/// The `Lumen` CRD as it shipped at N-1, generated once from `lumen_crd_yaml()`.
/// Never edit by hand: it is the fixed reference the current schema is compared against.
const LUMEN_CRD_N_MINUS_1: &str = include_str!("../testdata/lumen-crd-n-minus-1.yaml");

/// Compare an older CRD schema (`old`) with the current one (`new`).
/// Return one message per violation; an empty Vec means "only additions".
pub(super) fn schema_compat_violations(old: &Value, new: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    walk(old, new, "", &mut violations);
    violations
}

fn child_path(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

fn walk(old: &Value, new: &Value, path: &str, out: &mut Vec<String>) {
    if let (Some(old_type), Some(new_type)) = (old.get("type"), new.get("type")) {
        if old_type != new_type {
            out.push(format!(
                "type changed: {path} ({} -> {})",
                old_type.as_str().unwrap_or("?"),
                new_type.as_str().unwrap_or("?")
            ));
        }
    }

    let empty = serde_json::Map::new();
    let old_props = old
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let new_props = new
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);

    for (name, old_child) in old_props {
        let child = child_path(path, name);
        match new_props.get(name) {
            None => out.push(format!("removed: {child}")),
            Some(new_child) => walk(old_child, new_child, &child, out),
        }
    }

    let old_required = required_names(old);
    for name in required_names(new) {
        if !old_required.contains(&name) {
            out.push(format!("new required: {}", child_path(path, &name)));
        }
    }

    if let (Some(old_items), Some(new_items)) = (old.get("items"), new.get("items")) {
        walk(old_items, new_items, &format!("{path}[]"), out);
    }
}

fn required_names(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(|n| n.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The `openAPIV3Schema` of the first (only) version of a CRD given as YAML.
fn first_version_schema(crd_yaml: &str) -> Value {
    let doc: Value = serde_yaml::from_str(crd_yaml).expect("CRD YAML parses");
    let schema = &doc["spec"]["versions"][0]["schema"]["openAPIV3Schema"];
    assert!(
        schema.is_object(),
        "CRD has no spec.versions[0].schema.openAPIV3Schema"
    );
    schema.clone()
}

fn snapshot_schema() -> Value {
    first_version_schema(LUMEN_CRD_N_MINUS_1)
}

fn spec_props_mut(schema: &mut Value) -> &mut serde_json::Map<String, Value> {
    schema["properties"]["spec"]["properties"]
        .as_object_mut()
        .expect("spec.properties is an object")
}

fn yaml_as_json(yaml: &str) -> Value {
    serde_yaml::from_str(yaml).expect("CRD YAML parses")
}

#[test]
fn lumen_crd_identity_is_unchanged_from_n_minus_1() {
    for doc in [
        yaml_as_json(LUMEN_CRD_N_MINUS_1),
        yaml_as_json(&lumen_crd_yaml()),
    ] {
        let spec = &doc["spec"];
        assert_eq!(spec["group"], "lumen.dev", "{doc}");
        assert_eq!(spec["scope"], "Namespaced", "{doc}");
        assert_eq!(spec["names"]["kind"], "Lumen", "{doc}");
        assert_eq!(spec["versions"].as_array().map(Vec::len), Some(1), "{doc}");
        assert_eq!(spec["versions"][0]["name"], "v1alpha1", "{doc}");
        assert_eq!(spec["versions"][0]["served"], true, "{doc}");
        assert_eq!(spec["versions"][0]["storage"], true, "{doc}");
    }
}

#[test]
fn fleet_crd_identity_is_unchanged() {
    let doc = yaml_as_json(&fleet_crd_yaml());
    let spec = &doc["spec"];
    assert_eq!(spec["group"], "lumen.dev");
    assert_eq!(spec["scope"], "Cluster");
    assert_eq!(spec["names"]["kind"], "LumenFleet");
    assert_eq!(spec["versions"][0]["name"], "v1alpha1");
    assert_eq!(spec["versions"][0]["served"], true);
    assert_eq!(spec["versions"][0]["storage"], true);
}

#[test]
fn current_lumen_schema_only_adds_to_the_n_minus_1_schema() {
    let v = schema_compat_violations(&snapshot_schema(), &first_version_schema(&lumen_crd_yaml()));
    assert!(v.is_empty(), "{v:#?}");
}

#[test]
fn diff_reports_a_property_that_was_removed() {
    let old = snapshot_schema();
    let mut new = old.clone();
    spec_props_mut(&mut new).remove("shardCount");
    let v = schema_compat_violations(&old, &new);
    assert_eq!(v, vec!["removed: spec.shardCount".to_string()]);
}

#[test]
fn diff_reports_a_property_whose_type_changed() {
    let old = snapshot_schema();
    let mut new = old.clone();
    spec_props_mut(&mut new)
        .get_mut("shardCount")
        .expect("snapshot has spec.shardCount")["type"] = json!("string");
    let v = schema_compat_violations(&old, &new);
    assert_eq!(
        v,
        vec!["type changed: spec.shardCount (integer -> string)".to_string()]
    );
}

#[test]
fn diff_reports_a_new_required_field() {
    let old = snapshot_schema();
    let mut new = old.clone();
    new["properties"]["spec"]["required"] = json!(["image", "brandNewField"]);
    spec_props_mut(&mut new).insert("brandNewField".to_string(), json!({"type": "string"}));
    let v = schema_compat_violations(&old, &new);
    assert_eq!(v, vec!["new required: spec.brandNewField".to_string()]);
}

#[test]
fn diff_allows_a_property_that_was_added() {
    let old = snapshot_schema();
    let mut new = old.clone();
    spec_props_mut(&mut new).insert("brandNewField".to_string(), json!({"type": "string"}));
    let v = schema_compat_violations(&old, &new);
    assert!(v.is_empty(), "{v:#?}");
}
