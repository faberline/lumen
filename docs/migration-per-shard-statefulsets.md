# Migration: per-shard StatefulSets

## Summary

With `spec.shardCount > 1` the operator renders one StatefulSet and one PodDisruptionBudget per shard. Shard `k` gets the name `<instance>-s<k>`. With `spec.shardCount <= 1` nothing changes: the single StatefulSet and PodDisruptionBudget keep the name `<instance>`.

Multi-shard has no production users yet. So there is no live migration and no downtime plan. There is no rollback command for this change.

## Rationale

Each shard needs its own failure domain. Each shard also needs its own quorum-safe PodDisruptionBudget (`maxUnavailable = voters - quorum`, at least 1), and a three-zone spread for its three voters.

A PodDisruptionBudget limits voluntary evictions only. It does not control StatefulSet rolling updates.

## What changes

The table uses the instance name `search`, and shard `k` with pod ordinal `n` in the shard.

| Object | Before (shardCount 1) | After (shard k) |
|---|---|---|
| StatefulSet | `search` | `search-s<k>` |
| PodDisruptionBudget | `search` | `search-s<k>` |
| Pod | `search-<n>` | `search-s<k>-<ordinal>` |
| PVC | `raft-search-<n>` | `raft-search-s<k>-<ordinal>` |
| Pod DNS (headless Service `search-headless`) | `search-<n>.search-headless` | `search-s<k>-<ordinal>.search-headless` |
| Pod label | none | `lumen.dev/shard=<k>` |
| Container env | none | `LUMEN_SHARD_INDEX`, read from the label `lumen.dev/shard` |
| Host anti-affinity | all pods of the runtime may not share a node | only pods of the same shard may not share a node; different shards and different runtimes may share a node |

The headless Service stays `search-headless`. The client Service `search` is unchanged.

## Required Actions for Operators

1. **Check the old objects.** After `spec.shardCount` goes above 1, the operator lists the old StatefulSet `search` and PodDisruptionBudget `search` as prune targets. The shared reconcile owns the delete. Check that both are gone:

   ```sh
   kubectl get statefulset,pdb -n <namespace>
   ```

   If either one remains, delete it by hand.

2. **Plan for the data.** The PVCs of the old StatefulSet are not reused by the per-shard StatefulSets. Rebuild the data from the source of truth, or restore it from a backup.

3. **Update names that point at pods and claims.** Update every alert, dashboard or script that names `search-<n>` pods, `raft-search-<n>` claims or `statefulset="search"`. The built-in `LumenPvcNearFull` rule matches both `raft-<name>-<ordinal>` and per-shard `raft-<name>-s<k>-<ordinal>` claims; it needs no change.

4. **Clients need no change.** Clients use the Service `search`, not pod names.
