//! Tests for the versioned Managed client-workload template at
//! `k8s/client-workload/`. The tests read and parse the YAML files only; no
//! external tool runs.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_yaml::Value;

const VERSION_LABEL: &str = "lumen.axiom.dev/client-template-version";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../k8s/client-workload")
}

fn load(rel: &str) -> Value {
    let path = root().join(rel);
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
}

/// The item of a YAML sequence whose `name` field is `name`.
fn named<'a>(items: &'a Value, name: &str) -> &'a Value {
    items
        .as_sequence()
        .expect("a sequence")
        .iter()
        .find(|item| item["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("no item named {name}"))
}

fn pod_spec(patch: &Value) -> &Value {
    &patch["spec"]["template"]["spec"]
}

fn container(patch: &Value) -> &Value {
    &pod_spec(patch)["containers"][0]
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("a readable directory") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn patch_disables_default_token_mount_and_uses_existing_ksa() {
    let patch = load("deployment-patch.yaml");
    let pod = pod_spec(&patch);
    assert_eq!(pod["automountServiceAccountToken"].as_bool(), Some(false));
    assert_eq!(
        pod["serviceAccountName"].as_str(),
        Some("REPLACE_ME__CLIENT_KSA")
    );
}

#[test]
fn patch_projects_lumen_audience_token_at_fixed_path() {
    let patch = load("deployment-patch.yaml");
    let pod = pod_spec(&patch);

    let volume = named(&pod["volumes"], "lumen-identity");
    let token = &volume["projected"]["sources"][0]["serviceAccountToken"];
    assert_eq!(token["audience"].as_str(), Some("lumen.axiom.dev"));
    let token_path = token["path"].as_str().expect("a token path");
    assert_eq!(token_path, "token");

    let mount = named(&container(&patch)["volumeMounts"], "lumen-identity");
    let mount_path = mount["mountPath"].as_str().expect("a mount path");
    assert_eq!(mount_path, "/var/run/secrets/lumen.axiom.dev");
    assert_eq!(
        format!("{mount_path}/{token_path}"),
        "/var/run/secrets/lumen.axiom.dev/token"
    );
}

/// Negative control: a `subPath` on the CA mount stops in-place ConfigMap
/// updates from reaching the container. Adding `subPath` to the
/// `lumen-trust` mount makes this test fail.
#[test]
fn ca_mount_has_no_subpath() {
    let patch = load("deployment-patch.yaml");
    let mounts = container(&patch)["volumeMounts"]
        .as_sequence()
        .expect("volume mounts");
    for mount in mounts {
        assert!(
            mount.get("subPath").is_none(),
            "mount {} must not use subPath",
            mount["name"].as_str().unwrap_or("?")
        );
    }
    let trust = named(&container(&patch)["volumeMounts"], "lumen-trust");
    assert_eq!(trust["mountPath"].as_str(), Some("/var/run/lumen/trust"));
}

#[test]
fn no_token_value_in_env_or_secret() {
    let patch = load("deployment-patch.yaml");
    let env = container(&patch)["env"].as_sequence().expect("env");
    let names: Vec<&str> = env.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["LUMEN_URL", "LUMEN_CA_FILE"]);
    for entry in env {
        assert!(
            entry.get("valueFrom").is_none(),
            "env must hold no secret ref"
        );
    }

    let mut files = Vec::new();
    walk(&root(), &mut files);
    assert!(!files.is_empty(), "the template has files");
    for path in files {
        let text = fs::read_to_string(&path).expect("a text file");
        for line in text.lines().map(str::trim) {
            for kind in ["kind: Secret", "kind: Namespace", "kind: ServiceAccount"] {
                assert_ne!(line, kind, "{} creates an object", path.display());
            }
        }
    }
}

#[test]
fn template_version_label_matches_workspace_version() {
    let patch = load("deployment-patch.yaml");
    let version = env!("CARGO_PKG_VERSION");
    assert_eq!(
        patch["metadata"]["labels"][VERSION_LABEL].as_str(),
        Some(version)
    );
    assert_eq!(
        patch["spec"]["template"]["metadata"]["labels"][VERSION_LABEL].as_str(),
        Some(version)
    );
}

#[test]
fn egress_policy_is_optional_component() {
    let base = load("kustomization.yaml");
    let base_text = serde_yaml::to_string(&base).expect("a string");
    assert!(
        !base_text.contains("egress"),
        "the base component must not pull in egress"
    );

    let egress = load("egress/kustomization.yaml");
    assert_eq!(egress["kind"].as_str(), Some("Component"));

    let policy = load("egress/networkpolicy.yaml");
    assert_eq!(policy["kind"].as_str(), Some("NetworkPolicy"));
    let types = policy["spec"]["policyTypes"].as_sequence().expect("types");
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].as_str(), Some("Egress"));
}

#[test]
fn example_renders_expected_kinds() {
    let base = load("kustomization.yaml");
    assert_eq!(
        base["patches"][0]["path"].as_str(),
        Some("deployment-patch.yaml")
    );
    assert_eq!(
        base["patches"][0]["target"]["kind"].as_str(),
        Some("Deployment")
    );

    let example = load("example/kustomization.yaml");
    let resources: Vec<&str> = example["resources"]
        .as_sequence()
        .expect("resources")
        .iter()
        .map(|r| r.as_str().unwrap())
        .collect();
    assert_eq!(resources, ["deployment.yaml"]);
    let components: Vec<&str> = example["components"]
        .as_sequence()
        .expect("components")
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    assert_eq!(components, ["..", "../egress"]);

    let docs = [
        load("example/deployment.yaml"),
        load("egress/networkpolicy.yaml"),
    ];
    let kinds: BTreeSet<&str> = docs
        .iter()
        .map(|doc| doc["kind"].as_str().expect("a kind"))
        .collect();
    let expected: BTreeSet<&str> = ["Deployment", "NetworkPolicy"].into_iter().collect();
    assert_eq!(kinds, expected);
}
