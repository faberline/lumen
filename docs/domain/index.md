# Index

This context owns field indexes, text analysis, schema checks, query evaluation and the Engine that applies records.

Source: `src/index`.
Layers: domain, application, infrastructure and interfaces.

One Engine is the aggregate root for one shard. It controls changes to the searchable state. Lumen stores the indexed forms and external IDs. The caller owns source records and hydration.

[Indexing](../indexing.md) and [querying](../querying.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
