# Formal Claude review and PR completion

The default branch now declares the formal read-only Claude producer and one CI/outcome writer.
The writer replaces the previous issue-ci workflow. Each wake reconciles both queues.

- Complete trusted qualification is required before paid final review.
- A current-head PASS plus all required evidence enables native squash auto-merge.
- The linked issue may close after a merge to the default branch. Automatic issue closure has not yet been verified for this framework; AW #115 tracks that gap.
- A valid fail/block returns the linked open issue to todo and preserves unrelated labels.
- Stale, missing, skipped or unknown evidence cannot approve merge.

The existing required CI jobs, qualification mode and coverage blockers are unchanged.
The exact previous CI policy is retained for handoff journal continuity.
Current coverage holds still prevent final review and automatic merge.

Shared source: faberline/aw@8e2e93e5ba8feb30b6668de39139980ea3bb085c (AW #112). Authenticated failed or blocked reviews return the issue to todo.
Formal workflow ID: 399790070.
Unified writer ID: 394215197.
Native protection ruleset: 26069021.

Retained blockers:

- No general pull_request unit workflow exists on the default branch.
- Existing gke-test is path-filtered and does not establish affected functional and E2E coverage for every product change.
- No declared affected performance pull_request gate exists; manual/release workflows are not PR qualification.
- A reviewed coverage selection policy and real qualifying jobs are needed before clearing these blockers; no blanket docs-only zero-check bypass is approved.
