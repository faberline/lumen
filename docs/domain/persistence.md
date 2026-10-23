# Persistence

This context owns checkpoint generations, AOF, restore, background merge, compaction and capacity.

Source: `crates/lumen-index/src/persistence`.
Layers: domain, application, infrastructure and interfaces.

The segment file format and its readers are in [storage](storage.md).
Persistence publishes the sealed segments as checkpoint generations.

Persistence publishes a checkpoint at an apply boundary. It reopens durable state and replays the retained log tail. The capture barrier coordinates this work with apply and restore.

[Indexing](../indexing.md) defines the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
