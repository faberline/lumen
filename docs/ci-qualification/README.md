# Lumen PR qualification framework

This change adds ordinary read-only pull request checks for issue #184. It does
not approve product acceptance or change the existing failure-only lifecycle.

The unit/static planner inventories the full Cargo workspace at the exact pull
request head and base. It runs each enabled library and binary unit target for
every declared feature profile. It checks that the test roster is complete and
nonempty. It also runs Clippy with warnings denied and workspace formatting.
Existing GKE, functional, integration and performance gates remain unchanged.

The milestone validator checks frozen, proposed native milestone identities and
outcome links. Its fixture came from the existing #119 migration work; it is not
a new product contract. The current main documents do not match that proposal.
The milestone job must fail until the responsible product work resolves the
mapping. The issue planner owns missing product acceptance tickets. A failed or
unknown product requirement must not become a pass.

The shared GKE test overlay uses `dynamic-rwo` for its direct and operator
Raft volumes so that the test storage works with the N4 node pools. This does
not change the existing cloud test assertions.

A passing unit/static workflow does not establish full product qualification.
Functional, end-to-end and performance applicability still require their own
issue contracts and evidence, including the existing #114 work. The default
qualification policy remains failure-only with its existing blockers.
