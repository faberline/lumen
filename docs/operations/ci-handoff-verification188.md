# CI handoff verification for issue #188

This procedure checks the deployed development pipeline after the source in
[PR #189](https://github.com/faberline/lumen/pull/189) reaches `main`.
It verifies the handoff between a pull request, its linked issue, and the
trusted CI writer. Source approval alone does not complete this check.

## Required observations

1. Use one isolated, same-repository verification branch with a documentation
   change. Keep its source commit fixed across the negative and positive checks.
2. First use a closing reference whose issue is confirmed to be missing. The
   preflight must fail. The trusted source-PR role must report that failure on
   the PR. The real issue must stay open and retain its lifecycle labels. This
   role must not gain issue lifecycle authority.
3. Correct the PR body once to name the real, open canonical issue. Run the
   complete required normal-development CI at that same source commit. Bind
   the evidence to the actual head, current `main`, run, and attempt. Check
   actual test rosters and measurements. A successful processing job, skipped
   test, old-head result, or empty test target does not prove acceptance.
4. Confirm an applied journal from `github-actions[bot]` for the current policy,
   PR, head, and CI run. The issue must be open with the sole progress label
   `done`. CI owns that transition. The controller must not set `done` itself.
5. Obtain independent local Codex approval of the exact current inputs. Then
   merge the verified PR through normal branch protection. Read back the merge,
   its tree and ancestry, and the linked issue closure. Keep the CI journal and
   review evidence with the issue.

## Preserved requirements

Use the complete checks required by the adopted repository policy. Keep strict
base freshness and the GitHub Actions application identity. Keep the original
unit, integration, functional, resource, performance, and client contracts.
Report missing comparison baselines as missing; they are not ratchet passes.

The cloud workflow ends with CI evidence and the issue handoff. Local independent
review and the controller own acceptance, merge, and closure. A failing applicable
check keeps the issue in `todo` with its actual failure reason. This procedure
does not publish releases, create tags, or close milestones.
