# Lumen normal development pipeline

Lumen proposes adoption of AW DEV-PIPELINE-1 from immutable source
`4223a4534f0197e1ea23004b9388ffd1eea34639`.
The adoption issue is [#188](https://github.com/faberline/lumen/issues/188).
This source is already verified by Core. Publication alone does not complete
Lumen adoption. Keep current product failures and unknown coverage blocking.

The sole normal PR entry is `.github/workflows/pr-unit-static.yml`:

1. Preflight freezes head/main and checks freshness, conflicts and one real
   canonical open issue. A draft or unknown result cannot pass.
2. Inventory records all changed paths, renames, deletions and reverse users.
   A protected selector binds the complete unit and integration matrix.
3. Unit/static runs all 31 existing package/feature rows in four batches.
   Every row retains its original commands and shared 30-minute budget.
   Workspace formatting and the existing milestone document contract remain.
4. After all unit/static jobs succeed, run integration targets, the existing
   standalone/overlay/operator suites and measured performance. Keep the
   original images, fixtures, assertions, runtime and numeric limits.

Lumen owns its 32 expanded jobs and 61 check definitions. AW defines the
stage order and evidence contract. Every old required cloud job remains.
The legacy `gke-test.yml` retains manual and release calls. It no longer
starts a second PR unit run. Release workflows and actual tags are unchanged.

## Coverage and evidence

The normal selector uses a conservative full fallback. It executes every
declared check while retaining actual diff and dependency evidence.
An initial protected-policy proposal may plan all tests. That receipt grants
no qualification, `done`, review acceptance or merge authority.

Unit and integration runners compare independent discovered names with
actual complete execution. Zero tests and unknown ignored cases fail.
Known ignored E2E/coarse tests name their required external job. They never
count as locally executed tests. The cloud/performance jobs must also pass.
Each of the 35 ignored names has a source-bound owner or applicability decision.
The 27 external tests must execute in their declared jobs. The eight bounded
report, peer-calibration or full-release interfaces are not executed and never
count as passes. The native owners must also prove all eight original required
Rig scenarios, including every case in the old rig-load directory. Keep search's
200-QPS/p99/error pins and same-host ratchet semantics, and browse's complete
HTTP cursor oracle and deep-page bound. Missing execution blocks qualification.
Do not launch a long release workload on a general PR runner or invent an
exemption to get a green result.

The measured job retains the five coarse release gates, the 500k AND-query
50 ms exclusive limit and the deep-pagination 20x exclusive limit.
Core dependency versions and their applicable HTTP/2 bounds are unchanged.
This adoption does not claim new product performance or release acceptance.

## One issue writer and a source-PR diagnostic helper

`issue-ci.yml` uses only the reviewed AW source. The report job has issues
write and contents/actions/PR read. It reports each failed expanded job and
returns only eligible current work to `todo`. Complete exact-input CI is
required before it labels an OPEN issue `done`.

The separate source-PR helper has PR write and contents/actions/issues read.
Its code allows only a fresh exact-source-PR link diagnostic. It cannot
write issue labels, review, merge, close or status. Both jobs share the
repository mutex. The issue writer still runs if the helper fails.

Keep diagnostics, current ownership and lifecycle handoff separate. Distinct
failed jobs keep distinct comments. Retries deduplicate the same failure.
An old failure never reclaims a new worker's `doing` state or lease.
Missing logs, partial writes and unknown API responses remain visible.

## Local review and completion

The user authorized independent Codex review for this adoption. It replaces
the old Claude tool name and preserves review independence. Review starts
only after complete current-input CI. It runs no candidate program or test.
CI starts no model, review, merge or issue closure.

Only the authorized controller may finish. Recheck exact head/base, issue
contract, plan, run attempts, independent review and provider rules. Preserve
the existing strict protection until every replacement check and the local
review path is verified. Confirm the exact merge before closing #188.
Read back closure. After a confirmed merge, recovery may retry close only.

Before adoption is accepted, prove real source-PR diagnostics and CI-owned
OPEN/done after the protected source reaches main. Never add done by hand.
If a bootstrap merge auto-closes #188, reopen it while this proof is pending.
Preserve labels and owners. A green metadata job is not this live proof.

AW requires no Claude review. This change does not alter other repos,
client settings, schedules, infrastructure, release policy or milestones.
