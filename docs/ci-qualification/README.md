# PR unit/static and milestone document gates

References [#184](https://github.com/faberline/lumen/issues/184).

This infrastructure candidate adds ordinary read-only PR checks. It does not
activate trusted qualification or change the failure-only lifecycle policy.
The existing GKE jobs and product assertions remain unchanged. The test overlay
now selects `dynamic-rwo` for its direct PVC and the native-placement Lumen
CR's Raft PVC, because all shared node pools now use N4.

## Unit and static coverage

The planner is carried forward from the prepared Core #73 prototype. It reads
fresh Cargo metadata at the exact event head and base, verifies the complete
workspace inventory, and runs the complete workspace regardless of changed
paths. The reverse-dependency selector is advisory; unknown paths therefore
cannot reduce executed coverage. Renames/deletions do not waive old targets.

The current inventory has seven packages and nine library/binary targets.
There are 31 package/feature rows: default, no-default, each named feature,
and all-features where the manifest declares features. Each row runs every
enabled library/binary unit target separately and all-target Clippy with
warnings denied. Formatting covers the workspace. Unit evidence must have
complete named, nonempty rosters with no failed, ignored, filtered, measured,
duplicate or missing results. A legitimate zero-test target still needs
reviewed applicability; this candidate never silently calls it coverage.

Single-feature plus all-feature checks do not prove every feature combination.
In particular the separate combined `operator delegated-auth` behavior gate,
integration tests, CLI/server/network tests, GKE assertions and performance
checks remain obligations of their original contracts, including #114.
No product suites were run locally. The snapshot JSON files are metadata,
not execution evidence. The 31 jobs run at most four at once with a 30-minute
timeout each; configured timeout budgets are not measured runner cost.

PR jobs execute PR-owned code with `contents: read`, no secrets, no persisted
checkout credential, and no lifecycle writes. Their green result cannot
establish immutable qualification provenance. Policy adoption still needs the
separate AW trusted policy/review process and current-head online evidence.
The checked-in qualification blockers are unchanged.

## Milestone document contract

[PR #119](https://github.com/faberline/lumen/pull/119), head
`e91a9402a9b92fbc52d6deeb9d493486f376c190`, remains the original product fix.
Its narrow contract is to map the migrated milestone references to the native
Lumen milestone with the same intended release identity. It changes labels,
heading numbers and URLs together while retaining outcome text and scope.
The dependency branch `codex/119-current-main-dependency` carries only its
milestone-number changes onto current main. PR #185 uses that branch as its
base. The original #119 head is unchanged. The bridge avoids importing old
source history into the PR diff; outcome text is unchanged.

`milestones.json` freezes 46 native GitHub milestone identities fetched on
2026-10-28 and 87 proposed outcome/section mappings from that PR. The mapping
is a proposal for independent review, not a claimed approved product contract.
The workflow binds the validator and fixture SHA-256 bytes. Changed policy
bytes require independent review; this hash check cannot approve a concurrent
workflow edit. The external qualification policy remains the trust boundary.

The offline validator checks nonempty inventory, native title/URL identities,
link-label/URL agreement, heading/tracking agreement, complete outcome sections with nonempty outcome/status fields,
and unknown or duplicated applicability. It scans the maintained root and docs
files, rejects checkout-escaping symlinks, and never executes document snippets.
Fenced snippets cannot satisfy a tracking assertion. This is a milestone
contract check, not a claim to check every external URL or all documentation.

Local controls passed on 2026-10-28:

- 58 isolated Python fixtures passed; no Cargo product process was launched.
- Fresh offline Cargo metadata produced the seven-package/nine-target/31-row
  inventory without a build.
- Current main's documents were correctly refused with 85 mapping errors.
- The original #119 document snapshot passed all 87 assertions in a temporary
  positive fixture. A mutated destination was correctly refused.
- Rendered kustomize output uses `dynamic-rwo` for `lumen-data` and the
  native-placement `lumen-op` CR. The missing-catalog refusal fixture stays
  unchanged and is still expected to refuse before creating storage.
- Workflow YAML parsing and `git diff --check` passed.

All 87 document assertions now pass with the dependency present. Independent
review of the frozen mapping remains required; neither issue is closed.

## Current cloud retry evidence

Original PR #170's operator job was retried as
[job 122663790956](https://github.com/faberline/lumen/actions/runs/40001361412/job/122663790956).
It failed before deployment because `lumen-data` selected default
`standard-rwo` on N4. PR #171's retried
[operator job 122665046101](https://github.com/faberline/lumen/actions/runs/40006999365/job/122665046101)
reported the same refusal. This branch fixes those overlay inputs; it does not
rewrite either original product head or turn those failed attempts into passes.
Fresh cloud execution and independent review are still required.

The first online PR attempt exposed existing workspace formatting differences,
which initially suppressed the unit matrix. Formatting now runs independently;
a red format result cannot skip the 31 unit rows. Formatting-only Rust edits are isolated in commit `783f9f41`. The original
milestone mapping failure is fixed through the document dependency, and the
latest online formatting, document and inventory jobs pass.

## Latest CI repair

PR #185 is stacked on the current-main document bridge described above.
The original #119 branch is unchanged. All 87 document assertions pass locally
and online. The style commit was produced only by `cargo fmt --all`.

At head `48c9bd701f021fad0a238355061045cf59e83076`,
[GKE run 40644554940](https://github.com/faberline/lumen/actions/runs/40644554940)
passed all eight jobs, including standalone container, overlay and operator
execution. This proves the N4 storage input repair at that source revision.
The later head needs its own fresh GKE evidence.

Online unit jobs exposed two empty targets: the `lumen` public compatibility
library and `lumen-bench`. The driver still refuses zero-test targets, and now
runs later targets before reporting all failures. The public CLI's 19 tests
passed in the delegated-auth row at head `3a7ac4e9`; that row still failed on
both empty targets and Clippy. No partial pass is treated as a full row pass.

The repair adds real library caller contracts for versioned routing and exact
keyword bounds through the public schema/index/search paths. It also tests
benchmark cell rejection/selection and summary sample accounting. These are
ordinary Rust unit tests, not a measured performance run. Existing product
assertions and the nonzero evidence rule are unchanged. Clippy's diagnosed
`VectorBackend` default-derivation finding is corrected without changing its
default variant. The added Rust tests require online execution on the new head.

The follow-up preserves all 31 rows and schedules each package's default row
first. The pinned cache stores dependencies, not workspace source crates.
All 58 isolated helper controls pass. Clippy retains Cargo JSON diagnostics;
its `--all-targets -- -D warnings` contract remains intact.

At head `defa0880`, the default/no-default/backup/delegated-auth rows each ran
2 public-library tests, 19 CLI tests and 5 benchmark-helper tests successfully.
Those rows still failed on storage Clippy findings. The next source repair
names existing posting tuples, exposes standard fallible cursor iterators and names the explicit error
path `try_next`, and applies equivalent formatting/arithmetic
lint fixes. Existing cursor assertions are retained and extended to compare
the iterator output. Product execution on the new head remains required.


The subsequent strict pass found that retaining an inherent `next` with a
non-Iterator return type still violated Clippy. Internal callers now use
`try_next`; standard iteration keeps errors as `Result` items. Result ordering,
error propagation and all prior assertions are unchanged. Reference-only merge
oracle code is compiled only for its actual storage unit tests.

Every unit invocation now uses libtest `--include-ignored`. The three previously
ignored indexing measurement/deep-pagination tests run with the other units.
The evidence parser still refuses any ignored/filtered/failed result. No ignore
attribute or timing limit is removed. These PR-profile measurements do not
replace separately required release-performance gates. The async-trait patch
is also updated to 0.1.92, without adding lint exemptions.
