# Lumen contexts

A context groups code that owns one part of the service.
The [architecture guide](../architecture.md) explains the runtime boundaries.
`ddd.toml` declares the module paths and the allowed dependencies.

| Context | Owns | Guide |
|---|---|---|
| `access` | Caller identity, permissions and TLS material | [Access](access.md) |
| `operator` | Kubernetes declarations, rendering and reconciliation | [Operator](operator.md) |
| `sharding` | Bucket ownership, request routing and resharding | [Sharding](sharding.md) |
| `replication` | Raft group, pod role and cluster config | [Replication](replication.md) |
| `ingest` | Write records, pending-change capacity, ordered apply and the raft state machine | [Ingest](ingest.md) |
| `persistence` | Checkpoints, AOF, restore and merge | [Persistence](persistence.md) |
| `storage` | The segment file format, its writers and its readers | [Storage](storage.md) |
| `index` | Field indexes, queries and the engine | [Index](index.md) |
| `shared_kernel` | Shared wire types, log entries, the capture barrier and cross-crate ports | [Shared kernel](shared-kernel.md) |

Most contexts use four layers.
`domain` holds the local vocabulary and rules.
`application` runs use cases.
`infrastructure` handles storage, transport and process state.
`interfaces` contains adapters for callers.
A context can omit a layer it does not need.

`crates/lumen-index/src/app` joins the contexts into the service.
It is the composition root, which means it owns configuration and wiring.
`crates/lumen/src/compat` preserves the public module paths from before the split.
The binary modules under `crates/lumen/src/bin/lumen/` own command-line work.

P1 moves and splits files within one crate.
It preserves behavior and the compatibility paths.
The exceptions in `ddd.toml` record the remaining layer and dependency work.
P2 needs a separate decision for each behavior or dependency change.
