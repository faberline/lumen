# Lumen development pipeline

Lumen follows the aw CI standard. One workflow, `.github/workflows/ci.yml`
(workflow name `ci`), runs on every pull request and on every push to `main`.
It reads no issue, pull request or label data. The only required status check
is the `gate` job.

Stages run in this order, and a later stage starts only after the earlier one
passes:

1. `lint`: `cargo fmt --all -- --check` and
   `cargo clippy --workspace --all-targets --locked -- -D warnings`.
2. `test-*`: workspace tests in the default profile, with all features, and
   with each feature profile on its own.
3. `e2e-*`: the test image, image identity, generated client, standalone
   container, GKE (overlay and operator) and the native functional contracts.
4. `gate`: `if: always()`, needs every other job and fails unless each one
   succeeded.

Measured performance does not run on a pull request. It runs in the release
candidate workflow (`.github/workflows/lumen-release-candidate.yml`), in the
`perf-coarse` job: the coarse gates, the index performance gates and the native
performance contracts.

`gke-test.yml` and `gke-perf.yml` stay as manual entries. Release workflows
and tags are unchanged.
