# Replication

This context owns the live Raft role of a pod and the adapter that applies a replicated log to its Engine.

Source: `src/replication`.
Layers: domain and application.

The raft-runtime library owns the Raft host and peer mechanisms. Lumen owns its cluster view and the engine state-machine adapter. The application adapter is enabled by raft-wal.

[Architecture](../architecture.md) and [indexing](../indexing.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
