# Sharding

This context owns versioned virtual-bucket ownership, routed requests and bucket transfer during resharding.

Source: `src/sharding`.
Layers: domain, application, infrastructure and interfaces.

A document hashes to a stable virtual bucket. The versioned map assigns that bucket to a physical shard. Changing shard count does not change the document hash contract.

[Architecture](../architecture.md) and [protocol](../protocol.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
