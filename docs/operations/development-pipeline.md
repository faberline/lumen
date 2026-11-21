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

## Path selection

Job `test-select` reads the changed file names of a pull request and the
reviewed map `.github/ci-paths.json`. It sets two outputs that the other jobs
read:

- `tests`: the `test-*` jobs run.
- `image`: the image build and the `e2e-*` jobs that need it run.

Each path has one class:

- `image`: the `test-*` jobs and the image, GKE, container and native jobs run.
- `unit`: the `test-*` jobs run; the image, GKE, container and native jobs do not.
- `docs`: no test job runs. Only `lint` and `gate` run.

A path takes the highest class of every rule that matches it (`image` over
`unit` over `docs`). Root `*.md`, `llms.txt`, `clients/**` and
`docs/benchmarks-scale.md` are class `unit`, because tests read them.

Everything runs on a push to `main`, when a path matches no rule, when the
file list is 3000 files or more, and when the map is missing or invalid. If
the file list cannot be read, the job fails and `gate` fails. A change to
`.github/**`, `Cargo.toml`, `Cargo.lock`, `aw.toml` or `scripts/ci/**` runs
everything. The `gate` job accepts a `skipped` job; it still needs every job.
