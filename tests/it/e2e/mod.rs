//! End-to-end suites (faberline/aw docs/standards/rust-tests.md). Each suite
//! is one module, `e2e::<suite>::`, and every test in it is `#[ignore]`: it
//! needs a server that runs somewhere else, so plain `cargo test` never runs
//! it, and a missing input fails it instead of skipping it.
//!
//!   overlay               the direct-install Deployment `lumen` on the shared
//!                         GKE test cluster (k8s/gke-test-job/overlay)
//!   operator              the shipped operator and its Lumen CRs
//!                         (k8s/gke-test-job/operator)
//!   standalone_container  the image itself under docker on the runner
//!                         (E2E_STANDALONE_IMAGE instead of an endpoint)
//!
//! The cluster suites run in the test Job on pool-lumen-client as
//! `lumen-it --ignored --test-threads=1 e2e::<suite>::`, reach the server
//! through its Service at `E2E_ENDPOINT`, call `kubectl` with the Job's
//! namespaced Role, and check the image identity the run hands them in the
//! ConfigMap `gke-test-env` (`identity`). Results go to the pod log as
//! `E2E_EVIDENCE {json}` lines.
//!
//! `cluster` holds the steps both cluster suites share; `support` the
//! clients, the evidence lines, and the fixture.

mod cluster;
mod identity;
mod operator;
mod overlay;
mod standalone_container;
mod support;

/// libtest passes a filter that matches nothing ("running 0 tests"), so a
/// renamed module would turn a GKE suite into a green no-op. Pin each Job
/// folder's filter to a module here that holds the ignored `journey`, and
/// the workflow's `all` to exactly those folders.
#[test]
fn gke_suite_filters_match_modules() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let read = |path: &str| {
        std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("read {path}: {e}"))
    };
    let suites = ["overlay", "operator"];
    let mut folders: Vec<String> = std::fs::read_dir(root.join("k8s/gke-test-job"))
        .expect("read k8s/gke-test-job")
        .map(|e| e.expect("dir entry").file_name().to_string_lossy().into_owned())
        .filter(|name| name != "base")
        .collect();
    folders.sort();
    let mut expected: Vec<String> = suites.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(folders, expected, "k8s/gke-test-job suite folders");

    for suite in suites {
        let job = read(&format!("k8s/gke-test-job/{suite}/kustomization.yaml"));
        assert_eq!(
            job.matches("args: [").count(),
            1,
            "k8s/gke-test-job/{suite} must set exactly one test filter"
        );
        assert!(
            job.contains(&format!("args: [\"e2e::{suite}::\"]")),
            "k8s/gke-test-job/{suite} must filter e2e::{suite}::"
        );
        let module = read(&format!("tests/it/e2e/{suite}.rs"));
        assert!(
            module.contains("#[ignore = \"e2e: needs E2E_ENDPOINT\"]\nasync fn journey()"),
            "tests/it/e2e/{suite}.rs must hold the ignored `journey`"
        );
    }

    let workflow = read(".github/workflows/gke-test.yml");
    assert!(
        workflow.contains(r#"all) suites='["overlay","operator"]' ;;"#),
        "gke-test.yml `all` must run every suite folder"
    );
}

/// The same guard for the image-targeted suite: every workflow that runs
/// `standalone_container` must filter a module that holds the ignored `bind`
/// and `durable`, so a rename cannot turn the gate into "running 0 tests".
#[test]
fn standalone_filter_matches_module() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let read = |path: &str| {
        std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("read {path}: {e}"))
    };
    let command =
        "cargo test --locked -p lumen --test it -- e2e::standalone_container:: --ignored --test-threads=1";
    for workflow in [
        ".github/workflows/gke-test.yml",
        ".github/workflows/lumen-release-candidate.yml",
    ] {
        let source = read(workflow);
        assert_eq!(
            source.matches(command).count(),
            1,
            "{workflow} must run `{command}` exactly once"
        );
    }
    let module = read("tests/it/e2e/standalone_container.rs");
    for test in ["bind", "durable"] {
        assert!(
            module.contains(&format!(
                "#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn {test}()"
            )),
            "tests/it/e2e/standalone_container.rs must hold the ignored `{test}`"
        );
    }
}
