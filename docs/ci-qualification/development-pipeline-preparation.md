# Lumen development pipeline preparation

Issue: [#188](https://github.com/faberline/lumen/issues/188).
Stage contract: [AW DEV-PIPELINE-1](https://github.com/faberline/aw/blob/fb002d9198967313a4641f129a9c9648890a7555/docs/standards/development-pipeline.md).

This branch prepares part of the Lumen adapter. Full adoption remains pending.
Core #84 must complete before Lumen adopts the normal-development pipeline.

## Lumen-owned unit layout

`.github/development-test-batches.json` selects four physical unit batches.
The app owns this value. AW sets no batch count.

The planner derives every library/binary and feature profile from fresh Cargo
metadata. A batch retains each row once. The current 31-row inventory comes
from the expanded jobs at the frozen framework commit. Its fixture records
that the source CI failed. It is definition evidence, not test-pass evidence.

Each row runs both original unit and Clippy commands. The existing unit driver
still checks complete nonzero rosters, ignored tests and baseline coverage.
Both commands share the original 30-minute row budget. A timed-out command's
isolated process group is terminated before another row starts.

Each row records its result even if an earlier row failed. The batch records
the full expected and observed case IDs, plan digest, head, base, run and
attempt. Any failed row fails the batch. Source identity is checked before
and after every row, including failures.

## Impact evidence

The inventory job records the exact head/base diff with NUL-separated paths.
It retains deleted paths and both sides of a rename. It reports reverse
dependency users. Unknown paths use the full workspace fallback. Execution
continues to use the complete workspace matrix.

Format, milestone contracts and all unit batches wait for this inventory.
The existing unit-before-cloud boundary remains required.

## Remaining adoption work

- Create one normal PR orchestration path. Remove the duplicate direct and
  GKE unit invocation while preserving manual and release entry points.
- Complete the reviewed qualification policy, affected functional/E2E and
  performance plan, and exact immutable workflow identities.
- Wire independent current-input Claude review and the single result writer.
  Verify every failed nonfinal job's reason comment and safe todo recovery.
- Verify guarded merge, actual merge observation, explicit canonical issue
  closure and close-only retry after the Core rollout dependency passes.
- Run full required hosted product CI and independent Claude review before
  protected adoption. Keep all existing tests, numeric thresholds and
  coverage blockers. Missing test content remains planner work.

The local control suite uses isolated metadata fixtures:

```sh
python3 -m unittest discover -s scripts/pr-gates/tests -v
```

Those fixtures do not execute Cargo, product binaries, E2E or performance
workloads. Their success does not qualify the product or activate the adapter.
