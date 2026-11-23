# Limits

Each row is a size or rate that the code enforces. The Constant names the
value in the source, and the File is the file that defines it.

| Limit | Value | Constant | File |
|---|---|---|---|
| Data-plane request body (default) | 8 MiB | `ADMIN_ROUTE_BODY_LIMIT_BYTES` | `crates/lumen-index/src/sharding/domain/reshard_batch.rs` |
| Data-plane request body (override) | set by `LUMEN_BODY_LIMIT_BYTES`; falls back to 8 MiB | `body_limit_bytes_from_env` | `crates/lumen-index/src/sharding/infrastructure/body_limit.rs` |
| Operator body-limit minimum | 1 MiB | `MIN_BODY_LIMIT_BYTES` | `crates/lumen-operator/src/domain/lumen_spec.rs` |
| Operator body-limit maximum | 64 MiB | `MAX_BODY_LIMIT_BYTES` | `crates/lumen-operator/src/domain/lumen_spec.rs` |
| Items per `POST /index` | 10000 | `MAX_INDEX_ITEMS` | `crates/lumen-index/src/index/application/engine/index.rs` |
| Reshard batch bytes | 4 MiB (half of 8 MiB) | `MAX_BATCH_BYTES` | `crates/lumen-index/src/sharding/domain/reshard_batch.rs` |
| Native wire frame | 8 MiB | `MAX_FRAME_BYTES` | `crates/lumen-index/src/index/interfaces/native_wire/codec.rs` |
| Query depth | 32 | `MAX_QUERY_DEPTH` | `crates/lumen-index/src/index/domain/query.rs` |
| Query nodes | 1024 | `MAX_QUERY_NODES` | `crates/lumen-index/src/index/domain/query.rs` |
| `terms` values per query | 1024 | `MAX_TERMS_VALUES` | `crates/lumen-index/src/index/domain/query.rs` |
| kNN `k` | 10000 | `MAX_KNN_K` | `crates/lumen-index/src/index/domain/query.rs` |
| Sort keys | 4 | `MAX_SORT_KEYS` | `crates/lumen-index/src/index/domain/query/sort.rs` |
| Pending change hard limit | 256 MiB | `HARD_LIMIT` | `crates/lumen-index/src/ingest/domain/change_budget.rs` |
| Search `limit` | 1000 | `MAX_SEARCH_LIMIT` | `crates/lumen-shared-kernel/src/types/search.rs` |
| Search `offset` | 10000 | `MAX_SEARCH_OFFSET` | `crates/lumen-shared-kernel/src/types/search.rs` |
| Batch search items | 32 | `MAX_BATCH_SEARCH_SIZE` | `crates/lumen-shared-kernel/src/types/search.rs` |
