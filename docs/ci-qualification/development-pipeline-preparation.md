# Lumen normal development pipeline adoption

Canonical issue: [#188](https://github.com/faberline/lumen/issues/188).
Integration PR: [#189](https://github.com/faberline/lumen/pull/189).
The stage and local completion rules follow the verified Core #85 flow.
This document describes the candidate. It is not deployment or acceptance proof.

## Stages and app-owned jobs

The PR workflow has one path:

1. Preflight checks the current main, conflicts and one canonical open issue.
2. Inventory records the complete head/base diff and reverse dependencies.
3. Formatting, milestone contracts and unit/static batches run.
4. Functional/E2E and measured performance jobs run after stage 3.

AW defines stage order. Lumen defines its jobs and matrix.
Four physical unit batches retain all 31 manifest-derived cases.
Each case runs the original unit and Clippy commands within one 30-minute
budget. Every enabled library and binary needs a complete nonzero roster.
Every batch records all case IDs, its plan digest, exact head/base, run and
attempt. A failed case fails the batch. A timeout terminates its process group.

The complete integration matrix retains 13 feature profiles.
The original eight cloud checks remain required.
Two hosted contract jobs execute the old EC contracts through current tools.
There are 32 expanded jobs and 61 explicit qualification checks.
This count does not count filtered, ignored or non-applicable tests as passes.

## Content repairs

The user explicitly authorized the bounded repairs in #192 through #197.
They fix static errors, add real facade/bench unit tests, update the pagination
fixture to the existing keyset contract, optimize cold AND scoring and apply
the existing #119 milestone-document proposal.
The frozen milestone fixture remains unchanged.

The original cold workload still uses 500,000 documents and 20 tokens.
Its total must remain strictly below 50 ms.
The pagination workload keeps its corpus, depth, page size, identities and
strict 20x bound. Unsupported offset-plus-sort requests must be rejected.

## Ignored integration contracts

`normal-pr-ignored-tests.json` records the exact 35 original ignored names.
Each row binds its source digest, owner and applicability reason.
Unknown names, changed source, duplicate rows and missing owners fail closed.
The protected proposal needs independent review before production adoption.

Twenty-seven names have actual external execution owners.
The native functional job runs CLI, Kustomize, generated-client and Docker
volume contracts. The native resource/performance job runs the original Rig
endurance, overload and fault scenarios plus isolated disk and competitive
gates. The scenario files, numeric limits and workload sizes remain intact.
Native subprocesses own their ports and cleanup. Tools are pinned by digest.
Execution binds full and selected harness rosters to current head/base and
run-attempt evidence. A selected test must execute once and pass.

Eight names are outside normal PR execution under their existing contracts.
They are explicit report-only probes, explicit peer disk calibration and the
full sixteen-cell 30-minute acceptance workload. Their bodies and interfaces
remain present. These rows are reported as not executed and not passed.
They do not supply release or full performance acceptance evidence.

## Failure and completion ownership

Each failed nonfinal job gets its own issue comment and diagnostic reason.
The single issue writer returns a current failed coding attempt to `todo`.
It preserves newer claims and refuses stale-head results.

After protected-source adoption, complete current CI may set literal `done`.
The issue stays open. CI has no Claude/Codex review, PR merge or issue-close
job. The local controller requires independent Codex review of the exact
current inputs. Only after that review and all necessary CI pass may it merge
the exact head and close #188. Failed review returns the issue to `todo`.

Initial candidate CI does not authorize itself. The source rollout, branch
protection and live OPEN/done acceptance remain separate required evidence.

## Local controls

`python -m unittest discover -s scripts/pr-gates/tests` runs isolated controls.
The writer controls in `scripts/issue-ci/tests` are also required.
These controls do not execute product E2E or performance workloads.
