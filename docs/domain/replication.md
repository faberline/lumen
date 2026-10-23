# Replication

This context owns a shard's Raft group, the live Raft role of a pod and the StatefulSet topology (`ClusterConfig`) they come from.

Source: `crates/lumen-replication/src`.
Layers: domain and application.

The raft-runtime library owns the Raft host and peer mechanisms. Lumen owns its cluster view. The crate depends only on the shared kernel and raft-runtime. The application layer implements the kernel's `ClusterStatus` port, which the HTTP layer reads. The engine state-machine adapter (`EngineSm`) is part of [ingest](ingest.md).

[Architecture](../architecture.md) and [indexing](../indexing.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
