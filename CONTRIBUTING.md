# Lumen Contributing

## Brief

Use this guide to change Lumen in this repository. The [README](README.md) owns product
promises. The ecosystem [CONTRIBUTING.md](https://github.com/faberline/core/blob/main/CONTRIBUTING.md) owns repository-wide
authoring rules.

## Authoritative Inputs

Read these sources in order for the part you change:

1. [README.md](README.md) for product purpose, public workflow, capabilities,
   sources, and gates.
2. [STATUS.md](STATUS.md) for current support and limits.
3. [ROADMAP.md](ROADMAP.md) for future outcomes and non-goals.
4. [Architecture](docs/architecture.md) for source-of-truth, caller, data-plane,
   control-plane, and shared-library boundaries.
5. [Indexing](docs/indexing.md) for schema, write, durability, rebuild, and
   activation semantics.
6. [Querying](docs/querying.md) for selection, scoring, result, facet, metric,
   limit, and hydration semantics.
7. [Search v2 migration](docs/migration-search-v2.md) for the versioned
   compatibility window and caller actions.
8. [Protocol](docs/protocol.md) for the canonical HTTP and runtime-behavior
   source map.
9. [Generated clients](clients/README.md) for the artifact model, language
   matrix, connection inputs, and current client limits.
10. [GKE](docs/gke.md) for support tiers, topology, Kubernetes-native
    placement, the Standard Regional profile, and its acceptance contract.
11. [Client integration](docs/client-integration.md) for connection profiles,
    workload projection, request mechanics, and source hydration helpers.
12. `crates/*/src/` and `crates/lumen/tests/` for behavior and executable
   contracts.
13. `crates/lumen-operator/src/domain/lumen_spec.rs` and
   `crates/lumen-operator/src/domain/lumen_fleet.rs` for the generated Kubernetes API.
14. [Deployment](docs/deployment.md), [configuration](docs/configuration.md), and
   [authentication](docs/authentication.md) for maintained operating context.

Generated `k8s/operator/crd.yaml` follows the Rust CRD types. Do not
edit generated schema as a substitute for changing its source.

## Local Workflow

Use the shared Faberline `issue-plan`, `issue-work`, and `issue-review`
skills from [AW](https://github.com/faberline/aw/tree/main/plugins/faberline/skills).
Each skill runs one pass; its `-loop` companion adds an explicit interval.
Installing or updating a skill does not start a loop.

Planning writes the bounded issue contract. Work submits one issue's PR and
runs affected unit/static checks. CI runs the required functional, integration
and performance checks. It alone sets `done` after all required current-input
gates pass, leaving the issue open for independent local review. A required
CI failure reports its evidence and returns the issue to `todo`. Review reads
the issue, diff and current CI; it runs no tests and never merges, closes or
changes lifecycle labels. The controller owns Git and final acceptance.

Use the repository's existing complete gates for product claims. Keep local
work limited to affected unit/static checks; functional, integration and
performance execution belongs in authorized CI. Preserve all feature matrices,
test selection checks, numeric limits and exact-head/base evidence.

Keep Lumen-specific search, schema, shard, and health policy in this
repository. Put reusable Kubernetes mechanisms in the
[faberline/core](https://github.com/faberline/core) `service-k8s` crate after
their shared contract is defined. Do not move Lumen's protected reshard fields
into a generic Fleet policy.

Keep the Lumen audience, token path, anonymous routes, typed access API, and
SubjectAccessReview resource mapping in this repository. Put token and review
mechanisms in the faberline/core `service-auth` crate, generic request-time
client providers in its `openapi-codegen` crate, and Kubernetes object
mechanics in its `service-k8s` crate. Never place a bearer token in a CRD, argument, environment
variable, status, Event, or log.

When a Rust CRD type changes, regenerate the checked-in schema:

```bash
cargo run -p lumen --features operator --bin lumen -- \
  k8s crd render --out k8s/operator
```

When README, STATUS, ROADMAP, protocol, generated-client, indexing, querying,
GKE, client-integration, or migration documentation changes, update the adopted
product-document set when its claims or links overlap. Keep current behavior
separate from target behavior.

## Verification

### Product documents

Use the retained shared validators from a checkout of the approved AW
revision. Set `AW_CHECKOUT` to that checkout's absolute path:

```bash
python3 "${AW_CHECKOUT:?set the approved AW checkout}/scripts/meta/readme_contract.py" check README.md
python3 "${AW_CHECKOUT:?set the approved AW checkout}/scripts/meta/project_docs_contract.py" check .
```

These commands check the README and adopted project-document contract.
A docs-only change does not claim that product gates ran.

### Product behavior

| Gate | Command |
|---|---|
| default features and refusal paths | `cargo test -p lumen` |
| operator and delegated-auth e2e targets | `cargo test -p lumen --features "operator delegated-auth"` |
| release feature set | `cargo test -p lumen --locked --features release --test it -- release_feature_set::` |
| landed-main release candidate oracle | `cargo test -p lumen --test it -- release_candidate::` |
| protected-tag promotion oracle | `cargo test -p lumen --test it -- release_promotion::` |
| full candidate verifier | `scripts/verify-release-candidate.sh --repo faberline/lumen --version <version> --commit <commit> --run-id <id> --run-attempt <attempt> --manifest <path> --manifest-sidecar <path> --artifacts-dir <path> --image <image> --candidate-tag <tag> --amd64-digest <digest> --arm64-digest <digest> --mode full` |
| public release verifier | `scripts/verify-release-artifacts.sh --repo faberline/lumen --tag lumen@<version> --commit <commit> --candidate-run-id <id> --mode public --output <path>` |
| standalone container smoke (`bind`, `durable`; needs docker and a GHCR root digest) | `E2E_STANDALONE_IMAGE=ghcr.io/faberline/lumen@sha256:<root digest> cargo test -p lumen --test it -- e2e::standalone_container:: --ignored --test-threads=1` |

All eight rows are required for a full Lumen behavior claim. None is a superset
of the others. Do not replace the second or third row with `--all-features`;
enabling `jieba` changes the behavior that the fallback test is meant to check.

Run narrower named tests during development. Run each of the eight gates at its
applicable candidate or promotion lifecycle stage before claiming that a release
is complete. Run the public verifier only after publication. Use the exact
capability gates in the README when the claim is narrower or requires a
live-cluster script.

The durable performance matrix (README gate `cargo test --release --locked -p
lumen --test it -- perf_gate:: --ignored --test-threads=1 --nocapture`) runs
locally through `scripts/durable-perf-local.sh`. The candidate workflow's
`perf-coarse` job runs the other `perf_gate::` tests with
`--skip durable_workload`. The
[release gate](docs/deployment.md#release-gate-gke-e2e-and-local-durable-perf)
owns the driver modes, the qualifying `qualify-matrix` run, the CI job split,
and the release inputs. [ROADMAP](ROADMAP.md) owns the workload and its
thresholds.
