# Ingest

This context owns mutation admission, pending-change capacity, record staging, WAL delivery and the ordered apply coordinator.

Source: `src/ingest`.
Layers: domain, application, infrastructure and interfaces.

The coordinator applies committed records to the shard Engine. Capacity refusal must preserve committed records and their order. Existing connections to index and persistence remain declared exceptions during P1.

[Indexing](../indexing.md) defines the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
