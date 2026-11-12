# Lumen glossary

| Term | Meaning |
|---|---|
| Collection | A named set of indexed fields and external IDs. |
| External ID | The caller's identifier for a source record. |
| Hydration | The caller loads source records for the IDs returned by Lumen. |
| Context | A group of modules that owns one part of the service. |
| Composition root | Code that connects contexts and shared libraries. |
| Compatibility facade | A module that preserves an old public path by exporting items from their current modules. |
| Aggregate root | The object that controls changes to a group of related state. Lumen uses one Engine per shard. |
| Shard | One part of the searchable state. |
| Virtual bucket | A stable hash bucket assigned to a physical shard by a versioned map. |
| Raft group | Replicas that agree on one ordered log for a shard. |
| WAL | The write-ahead log that records mutations before apply. |
| AOF | The local append-only file that stores mutation records. |
| Checkpoint | A published snapshot of index state at a sequence boundary. |
| Segment | A file that holds a sealed part of an index. |
| Pending change | A change retained since the last checkpoint. |
| Capture barrier | The shared boundary that coordinates apply, checkpoint capture and restore. |
| Reconciliation | The operator compares declared state with Kubernetes state and applies changes. |
| Standalone | The caller runs and manages a Lumen process. |
| Managed | The operator manages a declared Lumen runtime in Kubernetes. |
| SourceRecord | The caller-owned record. It stays outside Lumen. |
| CollectionDefinition | The definition that owns the schema and lifecycle of a collection. |
| IndexedDocument | The write unit, identified by `(CollectionId, ExternalId)`. |
| IndexedField | A named value of an IndexedDocument, written with `field`. |
| FieldVersion | The wire field `version` of a merge write. It is scoped to `(external_id, field)`. |
| DocumentVersion | The wire field `version` of a full replacement. It is scoped to the whole IndexedDocument. |
| SearchIndex | The derived query model. |
| Row / local row | Physical segment storage only; never a public name for an IndexedDocument. |

## Commands

| Command | Wire form |
|---|---|
| MergeIndexedFields | `POST docs:merge`, alias `POST index` |
| ReplaceIndexedDocument | `PUT docs:replace`, `PUT docs/{external_id}` |
| UnindexDocument | `POST docs:unindex`, `DELETE docs/{external_id}` |
| ClearCollection | `POST docs:truncate` |
| SearchCollection | `POST search` |

The [indexing guide](indexing.md) and [querying guide](querying.md)
define the public write and search behavior.
