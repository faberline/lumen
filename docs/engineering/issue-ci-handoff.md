# CI lifecycle handoff (failure-only)

This change wires the reviewed `aw issue-ci` interface in explicit
`lifecycle_mode: failure-only`. A failed required current-head gate can return
a linked issue from `doing` to `todo` with failure/run links. **Success cannot
mark `done`.** Missing or skipped evidence remains pending; nonempty
`qualification_blockers` remain in force. The first workflow step validates
the exact policy and requires the controller policy API before reconciliation. The workflow starts only on
completion of existing `gke-test` runs and adds no product test triggers, cloud
resources, credentials, manual dispatch, or release actions.

## Trusted implementation and policy

`scripts/issue-ci/handoff.py` is an unchanged copy from
`faberline/aw@67af8d020df3ca2ec4b064289408cfb0c2f8b299` at
`plugins/faberline/skills/issue-ci/scripts/handoff.py`.
SHA-256: `c67533b39d45ad62d407af3748856be396941647c349b63464424fa6aff1c198`.
The workflow checks out only the repository default branch with persisted
credentials disabled. It never checks out or executes PR code, reads a PR gate
manifest, or accepts artifacts as authorization. All writers share the
`issue-ci-handoff` concurrency group. Each wake reconciles all managed PRs.
Permissions are contents/actions/PR read and issues write only.

The workflow guard requires explicit failure-only mode and an explicit nonempty list of
qualification blockers. It uses the pinned controller's `load_config` and
requires its `evaluate_policy` API; older controllers refuse before execution.
All-green evidence remains pending even if blockers are accidentally cleared.
A separate reviewed rollout decision is required to authorize any future done
transition; this guard refuses full mode.

CI alone may return a submitted `doing` issue to `todo` after a required current-
head run/job failure. It does not touch an already `done` issue or an unclaimed
`todo` issue. Existing journal checkpoints can finish interrupted recovery
without undoing an AGY claim. Other labels are preserved. Missing/pending/
skipped/cancelled/neutral/stale evidence is never a pass. Reruns use the latest
attempt. AGY owns only `todo -> doing`; review after eventual complete CI
qualification never merges automatically. No review schedule is created here.

The controller requires a same-repository `issue/N` or `agy/issue-N` branch
and the exact body line `Closes #N`. Other branch conventions and forks are
refused. Receipts deduplicate handoffs per head/policy/coding epoch; unrelated
labels are preserved. The controller rechecks current head and complete run
snapshots before writes and lifecycle state before removing old labels.

## Existing gate inventory

The manifest lists every current `gke-test` job, including nested image build
and both GKE matrix expansions. The names were verified against
[existing Actions run 39818094295](https://github.com/faberline/lumen/actions/runs/39818094295)
and the default-branch workflow files. That historical run is inventory evidence,
not qualification for this PR or future heads. Release, image-only dispatch/call,
and diagnostic/performance dispatch workflows are not ordinary PR gates.

The existing product workflow retains its path filters. This handoff-only PR
does not match them, so product E2E is not expected to run. Its absence cannot
be reported as a pass. There is no supported docs-only zero-check policy.

## Unresolved coverage decisions

- No general pull_request unit workflow exists on the default branch.
- Existing gke-test is path-filtered and does not establish affected functional and E2E coverage for every product change.
- No declared affected performance pull_request gate exists; manual/release workflows are not PR qualification.
- A reviewed coverage selection policy and real qualifying jobs are needed before clearing these blockers; no blanket docs-only zero-check bypass is approved.

The user-approved direction is affected local unit checks, affected cloud
unit/functional/E2E/performance qualification for product MRs, and complete
declared release qualification. Rolling that architecture out is separate work.
Do not remove gates, add costly jobs/triggers, or enable full mode merely to get a
green handoff. Branch protection is neither changed nor inferred as policy.

## Local affected verification

Run `python3 -m unittest discover -s scripts/issue-ci/tests -v`.
These tests use metadata fixtures only: no local product E2E/performance or
full-suite execution, no cloud dispatch, and no issue mutations.
