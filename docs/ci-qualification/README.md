# Lumen PR qualification framework

This directory records Lumen's normal development checks and protected
qualification proposal for #188 / PR #189. See
[the current adoption contract](development-pipeline-preparation.md).
Candidate CI alone does not approve its own source or change deployed policy.

The unit/static planner inventories the full Cargo workspace at the exact pull
request head and base. It runs each enabled library and binary unit target for
every declared feature profile. It compares the nonempty execution roster with
the test harness's full discovery list before and after each target runs. If
ignored tests require a release run, that run must include every debug test and
match its own lists.
It checks both checkouts for tracked and non-ignored untracked changes before
and after metadata generation and product commands. A detected change fails the
job. This is not proof that files could not change and be restored during a run.
It also runs Clippy with warnings denied and workspace formatting.
Existing GKE, functional, integration and performance gates remain unchanged.

The milestone validator checks frozen, proposed native milestone identities and
outcome links. Its fixture came from the existing #119 migration work; it is not
a new product contract. The candidate applies the existing document proposal
without changing the frozen fixture. The validator must still check every
assertion. The issue planner owns newly found acceptance gaps. A failed or
unknown product requirement must not become a pass.

The shared GKE test overlay uses `dynamic-rwo` for its direct and operator
Raft volumes so that the test storage works with the N4 node pools. This does
not change the existing cloud test assertions.

A passing unit/static workflow does not establish full product qualification.
Functional, end-to-end and performance applicability still require their own
issue contracts and evidence, including the existing #114 work. The default
deployed qualification policy remains unchanged until reviewed protected-source
adoption. The candidate preserves exact nonzero execution and applicability
requirements. Its local completion owner requires independent Codex review.
