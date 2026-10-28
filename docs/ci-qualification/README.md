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
This candidate does not duplicate or rewrite its nine product documents.

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

- 54 isolated Python fixtures passed; no Cargo product process was launched.
- Fresh offline Cargo metadata produced the seven-package/nine-target/31-row
  inventory without a build.
- Current main's documents were correctly refused with 85 mapping errors.
- The original #119 document snapshot passed all 87 assertions in a temporary
  positive fixture. A mutated destination was correctly refused.
- Rendered kustomize output uses `dynamic-rwo` for `lumen-data` and the
  native-placement `lumen-op` CR. The missing-catalog refusal fixture stays
  unchanged and is still expected to refuse before creating storage.
- Workflow YAML parsing and `git diff --check` passed.

The milestone check is expected to remain red on this infrastructure branch
until the separately reviewed #119 document correction is present. This is a
reported dependency, not permission to exempt docs or close either issue.

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
a red format result cannot skip the 31 unit rows. The original product files
are not reformatted in this infrastructure change. The milestone job also
correctly reported the outstanding #119 mapping errors online.
