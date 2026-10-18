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
