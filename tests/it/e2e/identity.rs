//! What the cluster suites check about the image under test (rust-tests.md
//! rule 8). The run hands the expected identity to the Job in the ConfigMap
//! `gke-test-env` (.github/workflows/gke-test.yml, scripts/kind-cluster.sh):
//!
//!   E2E_EXPECTED_IMAGE           ghcr.io/faberline/lumen@sha256:<64 hex>, the root index
//!   E2E_EXPECTED_RUNTIME_DIGEST  sha256:<64 hex>, its child for the nodes' architecture
//!   E2E_EXPECTED_VERSION         must equal this test binary's Cargo version
//!   E2E_EXPECTED_GIT_SHA         8 lowercase hex, as /version reports it
//!
//! and the suite proves that the workloads ask for that image, that every pod
//! it exercised runs its root or child digest, and that /version reports that
//! version and commit before and after the restart.
//!
//! Nodes are cluster-scoped and the test Job's Role is not
//! (faberline/aw docs/standards/test-cluster.md), so the pool is proved by
//! what put each pod there: a pod bound to a node (`spec.nodeName`) with the
//! selector `faberline.dev/pool=lumen-server` was placed by the scheduler on
//! a node with that label.

use serde_json::Value;

use super::support::{is_lower_hex, required, Kube};

pub const POOL_KEY: &str = "faberline.dev/pool";
pub const POOL_VALUE: &str = "lumen-server";

const IMAGE_REPO: &str = "ghcr.io/faberline/lumen";

pub struct Identity {
    pub image: String,
    pub root_digest: String,
    pub runtime_digest: String,
    pub version: String,
    pub git_sha: String,
}

/// `sha256:<64 lowercase hex>`.
fn is_digest(s: &str) -> bool {
    s.strip_prefix("sha256:").is_some_and(|hex| is_lower_hex(hex, 64))
}

impl Identity {
    pub fn from_env() -> Self {
        Self::parse(
            &required("E2E_EXPECTED_IMAGE"),
            &required("E2E_EXPECTED_RUNTIME_DIGEST"),
            &required("E2E_EXPECTED_VERSION"),
            &required("E2E_EXPECTED_GIT_SHA"),
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    fn parse(image: &str, runtime_digest: &str, version: &str, git_sha: &str) -> Result<Self, String> {
        let root_digest = image
            .strip_prefix(IMAGE_REPO)
            .and_then(|rest| rest.strip_prefix('@'))
            .filter(|d| is_digest(d))
            .ok_or_else(|| format!("invalid E2E_EXPECTED_IMAGE {image}: expected {IMAGE_REPO}@sha256:<64 hex>"))?;
        if version != env!("CARGO_PKG_VERSION") {
            return Err(format!(
                "E2E_EXPECTED_VERSION {version} is not this test binary's version {}",
                env!("CARGO_PKG_VERSION")
            ));
        }
        if !is_lower_hex(git_sha, 8) {
            return Err(format!("invalid E2E_EXPECTED_GIT_SHA {git_sha}: expected 8 lowercase hex"));
        }
        if !is_digest(runtime_digest) {
            return Err(format!("invalid E2E_EXPECTED_RUNTIME_DIGEST {runtime_digest}: expected sha256:<64 hex>"));
        }
        if runtime_digest == root_digest {
            return Err("E2E_EXPECTED_RUNTIME_DIGEST must be the platform child, not the root index digest".into());
        }
        Ok(Self {
            image: image.to_string(),
            root_digest: root_digest.to_string(),
            runtime_digest: runtime_digest.to_string(),
            version: version.to_string(),
            git_sha: git_sha.to_string(),
        })
    }

    /// /version reports the expected version and commit.
    pub fn assert_version(&self, version: &Value, phase: &str) {
        assert!(
            version["version"].is_string() && version["git_sha"].is_string(),
            "/version ({phase}) did not return string version and git_sha: {version}"
        );
        assert!(
            version["version"] == self.version.as_str() && version["git_sha"] == self.git_sha.as_str(),
            "/version ({phase}) must report version={} git_sha={}, got {version}",
            self.version,
            self.git_sha
        );
    }

    /// The workload `what` (kind/name) asks for the image in exactly one
    /// container named `container`.
    pub fn assert_template_image(&self, kube: &Kube, what: &str, container: &str) {
        let workload = kube.get(&[what]);
        let named = named(&workload["spec"]["template"]["spec"]["containers"], container);
        assert!(
            named.len() == 1 && named[0]["image"] == self.image.as_str(),
            "{what} must run one {container} container at {}",
            self.image
        );
    }

    /// Exactly `desired` pods match `label`; each runs one Ready `container`
    /// at the image, whose runtime imageID is the root or the child digest,
    /// on a pool-lumen-server node. Returns the pods.
    pub fn assert_pods(&self, kube: &Kube, label: &str, container: &str, desired: u64) -> Value {
        let pods = kube.pods(label);
        let items = pods["items"].as_array().unwrap_or_else(|| panic!("{label}: no items: {pods}"));
        assert_eq!(items.len() as u64, desired, "{label}: want {desired} pod(s), got {}", items.len());
        for pod in items {
            let name = pod["metadata"]["name"].as_str().unwrap_or("?");
            let specs = named(&pod["spec"]["containers"], container);
            assert!(
                specs.len() == 1 && specs[0]["image"] == self.image.as_str(),
                "{label}: pod {name} must run one {container} container at {}",
                self.image
            );
            let statuses = named(&pod["status"]["containerStatuses"], container);
            assert!(
                statuses.len() == 1 && statuses[0]["ready"] == true,
                "{label}: pod {name} must have one Ready {container} container"
            );
            let image_id = statuses[0]["imageID"].as_str().unwrap_or_default();
            let digest = normalize_runtime_image_id(image_id)
                .unwrap_or_else(|| panic!("unrecognized {container} runtime imageID in pod {name}: {image_id:?}"));
            assert!(
                digest == self.root_digest || digest == self.runtime_digest,
                "{container} runtime imageID {image_id} in pod {name} is neither the root digest {} nor the platform child digest {}",
                self.root_digest,
                self.runtime_digest
            );
            assert_on_pool(pod);
        }
        pods
    }
}

/// The containers (or container statuses) named `name`.
fn named<'a>(list: &'a Value, name: &str) -> Vec<&'a Value> {
    list.as_array().map(|l| l.iter().filter(|c| c["name"] == name).collect()).unwrap_or_default()
}

/// The pod is bound to a node, and the scheduler could only pick a
/// pool-lumen-server node for it.
pub fn assert_on_pool(pod: &Value) {
    let name = pod["metadata"]["name"].as_str().unwrap_or("?");
    assert!(
        pod["spec"]["nodeName"].as_str().is_some_and(|n| !n.is_empty()),
        "pod {name} is not scheduled"
    );
    assert!(
        pod["spec"]["nodeSelector"][POOL_KEY] == POOL_VALUE,
        "pod {name} lacks the node selector {POOL_KEY}={POOL_VALUE}"
    );
}

/// The digest in a container status's imageID, as containerd, CRI-O, and
/// Docker write it.
pub fn normalize_runtime_image_id(raw: &str) -> Option<&str> {
    let pinned = raw.strip_prefix("docker-pullable://").unwrap_or(raw);
    let digest = match pinned.strip_prefix(IMAGE_REPO).and_then(|rest| rest.strip_prefix('@')) {
        Some(digest) => digest,
        None => ["containerd://", "cri-o://", "docker://"].iter().find_map(|p| raw.strip_prefix(p))?,
    };
    is_digest(digest).then_some(digest)
}

/// `.spec.replicas` of `what`, a positive integer.
pub fn replicas_of(kube: &Kube, what: &str) -> u64 {
    let workload = kube.get(&[what]);
    workload["spec"]["replicas"]
        .as_u64()
        .filter(|n| *n > 0)
        .unwrap_or_else(|| panic!("{what} replicas must be a positive integer, got {}", workload["spec"]["replicas"]))
}

mod tests {
    use super::*;

    const ROOT: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const CHILD: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

    #[test]
    fn runtime_image_ids_normalize_to_their_digest() {
        let pinned = format!("{IMAGE_REPO}@{ROOT}");
        assert_eq!(normalize_runtime_image_id(&pinned), Some(ROOT));
        assert_eq!(normalize_runtime_image_id(&format!("docker-pullable://{pinned}")), Some(ROOT));
        for runtime in ["containerd", "cri-o", "docker"] {
            assert_eq!(normalize_runtime_image_id(&format!("{runtime}://{CHILD}")), Some(CHILD));
        }
        for bad in [
            "",
            "sha256:2222",
            "ghcr.io/faberline/other@sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "docker-pullable://containerd://sha256:2222222222222222222222222222222222222222222222222222222222222222",
            "containerd://sha256:22222222222222222222222222222222222222222222222222222222222222ZZ",
            "podman://sha256:2222222222222222222222222222222222222222222222222222222222222222",
        ] {
            assert_eq!(normalize_runtime_image_id(bad), None, "{bad}");
        }
    }

    #[test]
    fn identity_inputs_are_validated() {
        let image = format!("{IMAGE_REPO}@{ROOT}");
        let version = env!("CARGO_PKG_VERSION");
        assert!(Identity::parse(&image, CHILD, version, "0123abcd").is_ok());
        assert!(Identity::parse(&format!("{IMAGE_REPO}:0.6.1"), CHILD, version, "0123abcd").is_err());
        assert!(Identity::parse(&image, ROOT, version, "0123abcd").is_err(), "child must differ from root");
        assert!(Identity::parse(&image, CHILD, "0.0.0", "0123abcd").is_err());
        assert!(Identity::parse(&image, CHILD, version, "0123ABCD").is_err());
        assert!(Identity::parse(&image, "sha256:22", version, "0123abcd").is_err());
    }
}
