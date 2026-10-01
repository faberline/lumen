# Persistence

This context owns sealed segment files and readers, checkpoint generations, AOF, restore, background merge, compaction and capacity.

Source: `src/persistence`.
Layers: domain, application, infrastructure and interfaces.

Persistence publishes a checkpoint at an apply boundary. It reopens durable state and replays the retained log tail. The capture barrier coordinates this work with apply and restore.

[Indexing](../indexing.md) defines the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
