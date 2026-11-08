//! Contract tests for the versioned Managed client-workload Kustomize template
//! at `k8s/client-workload/`. The pure-file tests read the YAML directly; the
//! render test needs `kustomize` on PATH and is ignored by default.

use serde::Deserialize;
use std::path::{Path, PathBuf};

const LABEL: &str = "lumen.axiom.dev/client-template-version";

fn template_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../k8s/client-workload")
}

fn read_yaml(rel: &str) -> serde_yaml::Value {
    let path = template_dir().join(rel);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn pod_spec(patch: &serde_yaml::Value) -> &serde_yaml::Value {
    &patch["spec"]["template"]["spec"]
}

fn volume<'a>(patch: &'a serde_yaml::Value, name: &str) -> &'a serde_yaml::Value {
    pod_spec(patch)["volumes"]
        .as_sequence()
        .expect("volumes is a sequence")
        .iter()
        .find(|v| v["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("volume {name} is missing"))
}

fn all_volume_mounts(patch: &serde_yaml::Value) -> Vec<&serde_yaml::Value> {
    let mut mounts = Vec::new();
    for key in ["containers", "initContainers"] {
        if let Some(containers) = pod_spec(patch)[key].as_sequence() {
            for container in containers {
                if let Some(list) = container["volumeMounts"].as_sequence() {
                    mounts.extend(list.iter());
                }
            }
        }
    }
    mounts
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            walk_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn patch_disables_default_token_mount_and_uses_existing_ksa() {
    let patch = read_yaml("deployment-patch.yaml");
    assert_eq!(
        pod_spec(&patch)["automountServiceAccountToken"],
        serde_yaml::Value::Bool(false),
    );
    assert_eq!(
        pod_spec(&patch)["serviceAccountName"].as_str(),
        Some("REPLACE_ME__CLIENT_KSA"),
    );
}

#[test]
fn patch_projects_lumen_audience_token_at_fixed_path() {
    let patch = read_yaml("deployment-patch.yaml");

    let identity = volume(&patch, "lumen-identity");
    let token = &identity["projected"]["sources"][0]["serviceAccountToken"];
    assert_eq!(token["audience"].as_str(), Some("lumen.axiom.dev"));
    assert_eq!(token["path"].as_str(), Some("token"));

    let mount = all_volume_mounts(&patch)
        .into_iter()
        .find(|m| m["name"].as_str() == Some("lumen-identity"))
        .expect("lumen-identity is mounted");
    let mount_path = mount["mountPath"].as_str().expect("mountPath is a string");
    assert_eq!(mount_path, "/var/run/secrets/lumen.axiom.dev");

    let combined = format!("{mount_path}/{}", token["path"].as_str().unwrap());
    assert_eq!(combined, "/var/run/secrets/lumen.axiom.dev/token");
}

#[test]
fn ca_mount_has_no_subpath() {
    let patch = read_yaml("deployment-patch.yaml");
    let mounts = all_volume_mounts(&patch);
    assert!(!mounts.is_empty(), "the patch mounts at least one volume");
    for mount in mounts {
        assert!(
            mount.get("subPath").is_none(),
            "mount {:?} must not use subPath",
            mount["name"]
        );
    }
}

#[test]
fn no_token_value_in_env_or_secret() {
    let patch = read_yaml("deployment-patch.yaml");
    let containers = pod_spec(&patch)["containers"]
        .as_sequence()
        .expect("containers is a sequence");
    let env_names: Vec<&str> = containers
        .iter()
        .flat_map(|c| c["env"].as_sequence().into_iter().flatten())
        .map(|e| e["name"].as_str().expect("env name is a string"))
        .collect();
    assert_eq!(env_names, vec!["LUMEN_URL", "LUMEN_CA_FILE"]);

    let text = std::fs::read_to_string(template_dir().join("deployment-patch.yaml"))
        .expect("read deployment patch");
    assert!(
        !text.contains("secretKeyRef"),
        "no Secret reference in the patch"
    );

    let mut files = Vec::new();
    walk_files(&template_dir(), &mut files);
    assert!(!files.is_empty(), "the template has files");
    for file in files {
        let body = std::fs::read_to_string(&file).unwrap_or_default();
        for kind in ["kind: Secret", "kind: Namespace", "kind: ServiceAccount"] {
            assert!(
                !body.contains(kind),
                "{} must not contain `{kind}`",
                file.display()
            );
        }
    }
}

#[test]
fn template_version_label_matches_workspace_version() {
    let patch = read_yaml("deployment-patch.yaml");
    let expected = env!("CARGO_PKG_VERSION");
    assert_eq!(
        patch["metadata"]["labels"][LABEL].as_str(),
        Some(expected),
        "metadata.labels"
    );
    assert_eq!(
        patch["spec"]["template"]["metadata"]["labels"][LABEL].as_str(),
        Some(expected),
        "spec.template.metadata.labels"
    );
}

#[test]
fn egress_policy_is_optional_component() {
    let main = read_yaml("kustomization.yaml");
    for key in ["resources", "components"] {
        if let Some(items) = main[key].as_sequence() {
            for item in items {
                assert!(
                    !item.as_str().unwrap_or_default().contains("egress"),
                    "the main kustomization must not list egress ({key})"
                );
            }
        }
    }

    let policy = read_yaml("egress/networkpolicy.yaml");
    assert_eq!(policy["kind"].as_str(), Some("NetworkPolicy"));
    assert_eq!(
        policy["spec"]["policyTypes"],
        serde_yaml::from_str::<serde_yaml::Value>("[Egress]").unwrap(),
    );
}

#[test]
#[ignore = "needs kustomize on PATH; run by development CI"]
fn example_renders_expected_kinds() {
    let out = std::process::Command::new("kustomize")
        .arg("build")
        .arg(template_dir().join("example"))
        .output()
        .expect("run kustomize build");
    assert!(
        out.status.success(),
        "kustomize build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let rendered = String::from_utf8(out.stdout).expect("UTF-8 render");
    let mut kinds: Vec<String> = serde_yaml::Deserializer::from_str(&rendered)
        .map(|doc| serde_yaml::Value::deserialize(doc).expect("render document"))
        .map(|doc| doc["kind"].as_str().expect("kind").to_owned())
        .collect();
    kinds.sort();
    assert_eq!(kinds, vec!["Deployment", "NetworkPolicy"]);
}
