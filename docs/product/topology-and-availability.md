# Topology and availability

## Topology and availability

- Problem: Stateful Lumen needs explicit placement and quorum boundaries.
- Who: Operators of replicated and sharded runtimes.
- Promise: Lumen exposes current capacity, placement, topology, rollout, split, autoscaling, and HPA boundaries.
- Status rows: `capacity-catalog-placement`, `kubernetes-native-placement`, `fixed-topology`, `per-shard-failure-domain-placement`, `quorum-safe-runtime-rollout`, `automatic-shard-splitting`, `membership-aware-replica-autoscaling`, `generic-horizontal-pod-autoscaling`.
- Limits today: Automated replica changes and generic HPA are not supported.
- Non-goals: Claiming one Pod or two voters is highly available.
- Neighbours: Recovery owns restart safety; Fleet owns cross-runtime convergence.

## Per-shard failure-domain placement (Milestone #8)

- Problem: Replicas need independent failure domains.
- Who: Replicated-runtime operators.
- Promise: Each shard can place members across declared failure domains.
- Outcome: `per-shard-failure-domain-placement`. Tracking: [Milestone #8](https://github.com/faberline/lumen/milestone/8).
- Non-goals: Capacity selection outside the catalog.
- Open: Define placement refusal and recovery behavior.
- Neighbours: Kubernetes-native placement.

## Quorum-safe runtime rollout (Milestone #10)

- Problem: A StatefulSet rollout can remove quorum.
- Who: Replicated-runtime operators.
- Promise: Runtime rollout changes members only when quorum and replication gates allow it.
- Outcome: `quorum-safe-runtime-rollout`. Tracking: [Milestone #10](https://github.com/faberline/lumen/milestone/10).
- Non-goals: Treating a PDB as a quorum gate.
- Open: Define member-at-a-time sequencing.
- Neighbours: Bounded shutdown and failover.

## Kubernetes-native placement (Milestone #8)

- Problem: Platform placement needs a portable Kubernetes contract.
- Who: Kubernetes operators.
- Promise: Lumen uses Kubernetes-native placement controls without machine-type fields in the API.
- Outcome: `kubernetes-native-placement`. Tracking: [Milestone #8](https://github.com/faberline/lumen/milestone/8).
- Non-goals: Cloud-specific machine types in the CRD.
- Open: Define portable capacity and placement mapping.
- Neighbours: Per-shard failure-domain placement.

## Membership-aware replica autoscaling (Milestone #6)

- Problem: Replica count cannot change safely without Raft membership work.
- Who: Operators under sustained load.
- Promise: Lumen can add or remove replicas only through membership-aware transitions.
- Outcome: `membership-aware-replica-autoscaling`. Tracking: [Milestone #6](https://github.com/faberline/lumen/milestone/6).
- Non-goals: Generic HPA control of serving pods.
- Open: Define the safe transition actuator.
- Neighbours: Quorum-safe runtime rollout.

## High-availability shard expansion (Milestone #5)

- Problem: Shard splitting must work with replicated shards.
- Who: High-availability runtime operators.
- Promise: Shard expansion keeps a Raft quorum while it moves ownership.
- Outcome: `high-availability-shard-expansion`. Tracking: [Milestone #5](https://github.com/faberline/lumen/milestone/5).
- Non-goals: Unsafe split during replica transition.
- Open: Define restart and rollback evidence.
- Neighbours: Automatic shard splitting.

## Bounded Raft shutdown and failover (Milestone #44)

- Problem: Shutdown and leadership change must finish within explicit bounds.
- Who: Replicated-runtime operators.
- Promise: Raft shutdown and failover expose bounded, recoverable behavior.
- Status rows: `bounded-raft-shutdown-and-failover`. Tracking: [Milestone #44](https://github.com/faberline/lumen/milestone/44).
- Limits today: This does not add Standalone high availability or GKE certification.
- Non-goals: Unbounded background shutdown.
- Neighbours: Quorum-safe runtime rollout.

## Deterministic consensus conformance (Milestone #43)

- Problem: Adversarial scheduling can hide consensus safety failures.
- Who: Raft-runtime maintainers and Lumen operators.
- Promise: Deterministic replay proves declared recovery and membership invariants.
- Status rows: `deterministic-consensus-conformance`. Tracking: [Milestone #43](https://github.com/faberline/lumen/milestone/43).
- Limits today: This evidence does not replace production network testing.
- Non-goals: Replacing production network testing.
- Neighbours: Managed embedded data durability.

## Distributed search routing and merge (Milestone #11)

- Problem: A multi-shard search needs safe routing and merge rules.
- Who: Distributed-search callers.
- Promise: Lumen routes search work and merges results through one declared contract.
- Outcome: `distributed-search-routing-and-merge`. Tracking: [Milestone #11](https://github.com/faberline/lumen/milestone/11).
- Non-goals: Partial or unordered result claims.
- Open: Define failure, cursor, and merge semantics.
- Neighbours: Distributed facet convergence.

## Non-goals in this area

Generic HorizontalPodAutoscaler control is not a Lumen topology contract.
