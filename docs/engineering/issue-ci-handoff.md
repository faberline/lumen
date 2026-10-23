# CI lifecycle handoff (blocked staging)

This change stages the reviewed `aw issue-ci` interface. **It does not enable
`doing -> done/todo` mutations yet.** The first workflow step refuses every
handoff while `qualification_blockers` is nonempty. The workflow starts only on
completion of existing `gke-test` runs and adds no product test triggers, cloud
resources, credentials, manual dispatch, or release actions.

## Trusted implementation and policy

`scripts/issue-ci/handoff.py` is an unchanged copy from
`faberline/aw@84e9b72b4a63cc30b47449a2e321981525fa9c34` at
`plugins/faberline/skills/issue-ci/scripts/handoff.py`.
SHA-256: `1abb5570032eaf294a50c5fa0bfb9ae7edd1db7e3f0a98a1f56601720916caf1`.
The workflow checks out only the repository default branch with persisted
credentials disabled. It never checks out or executes PR code, reads a PR gate
manifest, or accepts artifacts as authorization. All writers share the
`issue-ci-handoff` concurrency group. Each wake reconciles all managed PRs.
Permissions are contents/actions/PR read and issues write only.

Once a separate reviewed change resolves the coverage decisions and explicitly
sets `qualification_blockers: []`, CI alone can mark `done` after every listed
workflow and exact expanded job succeeds on the CURRENT PR head. Required job
failure returns the linked issue to `todo` with concise failure and run/job links.
Missing, pending, skipped, cancelled, neutral, stale-head and earlier rerun
attempt evidence cannot qualify. AGY owns only `todo -> doing`. Review follows
passing CI and does not merge automatically. No review schedule is created here.

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
Do not remove gates, add costly jobs/triggers, or clear blockers merely to get a
green handoff. Branch protection is neither changed nor inferred as policy.

## Local affected verification

Run `python3 -m unittest discover -s scripts/issue-ci/tests -v`.
These tests use metadata fixtures only: no local product E2E/performance or
full-suite execution, no cloud dispatch, and no issue mutations.
