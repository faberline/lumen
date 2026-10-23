//! The agent workflow model `lumen llm --topic workflow` serves.

/// The agent workflow model (`lumen llm --topic workflow`) as Markdown — the mental
/// model, declare→ingest→search→hydrate workflow, search-flavor decision map,
/// connection, and non-goals. Where exact wire shape is needed it points at
/// `lumen spec` / `lumen llm --topic recipes` so there is one source of truth.
pub fn llm_workflow_md() -> String {
    r#"# lumen workflow

## What lumen is
lumen is a **search index, not a database**. You (the caller) own the source of
truth — Postgres / AlloyDB / MongoDB / S3. lumen stores only index bits keyed by
your `external_id` and returns **ranked `external_id`s, never documents**. You
hydrate the hits against your own store.

## The integration loop (4 steps)
1. **Declare** a collection schema once — `PUT /collections/{id}` with a map of
   field name → typed field. The type fixes the index; there is no separate
   "index options" knob.
2. **Ingest** — your own pub/sub (CDC / logical replication / app writes) calls
   `POST /collections/{id}/index`. lumen bundles no connector; see
   `examples/consumer_pg_logical.py`. Re-writing `(external_id, field)` fully
   re-indexes that field.
3. **Search** — `POST /collections/{id}/search` with a query (relevance +
   filters + sort). You get back ranked `external_id`s + scores. Prefer the
   `QUERY /collections/{id}` twin when your stack supports it — see "QUERY
   method (RFC 10008)" below.
4. **Hydrate** — look the returned `external_id`s up in YOUR store to get the
   full records. lumen never had them.

## QUERY method (RFC 10008)
Policy: **QUERY-first, POST-always-available** (epic #1296 R1). `QUERY
/collections/{id}` and `QUERY /collections` are dual-registered twins of
`POST /collections/{id}/search` and `POST /collections:search`: same request
body (`SearchRequest` / `BatchSearchRequest`), same handler, and a
byte-identical response for identical bodies.

- Prefer `QUERY` when your HTTP client, proxy, and cache layer support it —
  RFC 10008 QUERY tells intermediaries the request is safe, idempotent, and
  cacheable, which `POST` cannot express.
- `POST` is the permanent fallback, not a deprecated path — every QUERY
  endpoint keeps its POST twin forever, so clients, proxies, and load
  balancers that can't emit `QUERY` (older HTTP libraries, some
  intermediaries) stay fully supported.
- `Content-Type: application/json` is mandatory on `QUERY` requests; a
  missing or mismatched `Content-Type` returns 415, same as the POST twin.
- `OPTIONS`/`HEAD` on both targets advertise `Accept-Query: application/json`
  and list `QUERY` in `Allow`.

## Batch search (multi-collection fan-out)
`POST /collections:search` is an msearch-style batch of independent
`(collection, SearchRequest)` items, executed with server-side concurrent
fan-out — use it instead of N client-side round-trips when a logical action
searches multiple collections at once (per-tenant/per-type partitioning,
for example). `collections:search` is one literal path segment (AIP-136
custom-method syntax), so it never collides with
`/collections/{collection_id}`; collection ids may not contain `:` for the
same reason.

```json
POST /collections:search
{ "searches": [
    { "collection": "users",    "query": {"term": {"field": "tags", "value": "rust"}}, "limit": 10 },
    { "collection": "products", "query": {"match": {"field": "title", "text": "earbuds"}}, "limit": 5 }
] }
→ 200 { "results": [
    { "status": "ok", "response": { "hits": [...], "total": 3, "took_ms": 1 } },
    { "status": "error", "code": "collection_not_found", "message": "..." }
] }
```

- Each item carries a full `SearchRequest` — `limit`, `sort`, `cursor`,
  `collapse`, `routing_key`, `track_total` may all differ per item, exactly
  like `POST /collections/{id}/search`.
- `results` is the same order and length as `searches`.
- **Partial failure never fails the batch.** One bad item (for example an
  unknown collection) reports `{"status":"error","code":"collection_not_found",
  "message":"..."}` for that item while the other items still return
  `{"status":"ok","response":{...}}`. The batch-level HTTP status stays 200
  unless the body is malformed or the batch is over the size limit (400).
- Max batch size is 32 items — this also bounds the concurrent fan-out. An
  over-limit batch is rejected with 400 before any item runs.
- Pagination stays per-item: each result's `cursor` continues independently
  by resubmitting that one item. There is no merged cursor and no
  cross-collection score merging/ranking — that is explicitly out of scope.

## Full-replacement writes (docs:replace)
`PUT /collections/{id}/docs:replace` is a batch **full-replacement** upsert:
each item's `fields` becomes the doc's *entire* indexed state — a declared
schema field the doc has today but that is absent from `fields` is
**implicitly deleted**. `docs:replace` is one literal path segment appended
after `{collection_id}`, so it registers directly in axum next to
`/collections/{collection_id}/docs/{external_id}` without any capture
ambiguity.

```json
PUT /collections/{id}/docs:replace
{ "docs": [
    { "external_id": "row-42", "version": 7, "fields": { "title": "New title", "state": "open" } }
] }
→ 200 { "results": [
    { "status": "ok", "fields_written": 2, "fields_skipped": 0 }
] }
```

- **Own the complete row for a doc?** Use `docs:replace` — replaying the same
  request converges to the same state (PUT semantics). **Own only some
  fields and want to add/update those without touching the rest?** Use
  `POST /collections/{id}/index` instead; `/index` is a merge, `docs:replace`
  is a full replacement.
- `version` is optional **doc-level** last-write-wins over the caller's own
  source-row version — distinct from `IndexItem.version`'s per-`(external_id,
  field)` cell versioning. A strictly-older version arriving later drops the
  *entire* item and is reported as `{"status":"dropped","current_version":...}`,
  not folded into `ok` or `error`.
- Each `ok` result carries `fields_written` and `fields_skipped` counters;
  `fields_skipped` (unchanged-value no-op suppression) is always `0` today.
- **Partial failure never fails the batch.** One bad item (unknown field,
  type mismatch) reports `{"status":"error","code":"...","message":"..."}`
  for that item while its siblings still return `ok`/`dropped`. The
  batch-level HTTP status stays 200 unless the body is malformed or the
  batch is over the size limit (400, max 32 items — the same
  `MAX_BATCH_REPLACE_SIZE` knob family as `collections:search`).
- `PUT /collections/{id}/docs/{external_id}` is single-resource sugar: body
  `{"version": ..., "fields": {...}}`, semantically identical to a one-item
  `docs:replace` batch, unwrapped back into a bare per-item result.

## Which "find" to use
- exact value / membership → `keyword` (`term`, `terms`) or `set`
- numeric range → `number` (`range` with numeric `gt`/`gte`/`lt`/`lte` bounds)
- string / date / datetime range → `keyword` (`range` with string bounds,
  compared byte/lexicographically — the same order ISO-8601 date/datetime
  strings sort chronologically in). String bounds are rejected with 400
  against a non-`keyword` field (and numeric bounds against a non-`number`
  field); `text` is explicitly out of scope for range queries
- full-text relevance → `text` + `match` (BM25). Analyzers: `whitespace_lower`,
  `ngram` (substring/CJK), `jieba` (Chinese)
- semantic similarity → `vector` + `knn` (you supply the embedding)
- perceptual / near-duplicate → `hash` + `hamming`
- hybrid lexical+semantic → `rrf` (fuse `match` + `knn` by rank; put any filter
  INSIDE each leg so the kNN leg stays filter-correct)
- autocomplete / suggest → declare a dedicated `text` field with the `ngram`
  analyzer and use `match`; lumen returns candidate `external_id`s, not
  completion strings
- which `external_id`s share a value → `POST /duplicates`
- nested data-table / "parent whose child matches" → `has_child`; combine it
  with parent-field `sort` for list-row flows that filter by child rows then
  order/count parent rows
- compose any of the above under `and` / `or` / `not`

## Search concept boundaries
These boundaries are explicit so search-engine selection does not infer silent
parity with PostGIS, OpenSearch, or MongoDB features that are not part of
Lumen's current contract.

| Concept | Disposition |
|---------|-------------|
| Geo / spatial search | Roadmap candidate; use PostGIS/MongoDB/OpenSearch or a caller-owned geospatial prefilter today, then pass matching `external_id`s to lumen. |
| Phrase / proximity queries | Roadmap candidate; current `match` is bag-of-words BM25 over analyzer tokens, not phrase order or slop. |
| Fuzzy / typo tolerance | Roadmap candidate; no edit-distance automaton today. For coarse prefix/substring recall, use the `ngram` analyzer recipe. |
| Synonyms | Caller-owned query expansion or normalized companion fields; lumen has no synonym analyzer or managed synonym dictionary. |
| Autocomplete / suggest | Recipe via a dedicated `text` field with `analyzer: "ngram"` plus `match`; hydrate suggestions from the caller's source of truth. |
| Highlighting | Non-goal: responses contain only `external_id` + `score`, and lumen does not store source text to return fragments. |
| Per-field / per-clause boost | Not supported as an arbitrary query knob. Use separate fields/query legs plus `rrf` and, if needed, final reranking in the caller. |
| Document TTL / expiry | Caller-owned lifecycle. Delete/reindex expired `external_id`s from the source-of-truth event stream; collection soft-delete grace is not per-document TTL. |

## Read consistency (`X-Read-Consistency`)
Only meaningful in primary-replica (raft) mode; standalone deployments (no
raft) ignore this header entirely — there is exactly one authoritative copy
per shard, so every level trivially holds.

- `leader` — the default, and what a missing or unrecognized header value
  also falls back to (no formal release exists yet to force a different
  default). Only the pod currently holding leadership for a shard answers;
  any other replica rejects with 503 naming the current leader.
- `any` — unconstrained; the local copy answers regardless of freshness.
- `bounded(<ms>)` — succeeds on the leader (never stale). **On a
  follower/learner it always rejects today**: lumen does not yet measure
  real inter-peer replication lag, so a non-leader replica reports the
  conservative "lag unknown" sentinel and is treated as over any bound
  rather than risk serving a stale read. Until real follower lag reporting
  ships, `bounded(<ms>)` behaves like `leader` with an extra
  follower-rejection path — do not rely on it to read from a follower.

## Routed multi-shard mode: client retry contract
Only applies to a routed multi-shard deployment (`SHARD_COUNT > 1`,
`replicasPerShard <= 1`, cross-pod forwarding — see `lumen llm --topic
deployment`); a standalone or single-shard deployment never returns these
codes. Every code below is a `503`, safe to retry with backoff — a client
that treats them as retryable rather than fatal handles a reshard split or a
rolling restart transparently:

| `code` | Meaning | Client behavior |
|--------|---------|------------------|
| `bucket_write_paused` | The write's virtual bucket is fenced for an in-progress reshard cutover's final migration pass (`POST /admin/reshard:fence`, bounded TTL). | Retry shortly; the pause is always bounded — splits/rollouts complete or the fence TTL lapses on its own. |
| `shard_forward_unavailable` | This pod forwarded the request one hop to the owning shard, but that shard was unreachable (pod down/rolling). | Retry with backoff; expected during a rolling restart. |
| `shard_map_version_mismatch` | The forwarded request declared a shard-map version that disagrees with the receiving pod's own live map — a bounded window where pods in a rolling restart are on two different `SHARD_MAP_*` env snapshots (pods only read env at boot). | Retry with backoff; wait for the rollout to converge rather than trusting either side's answer. |

None of the three above indicate data loss or a wrong answer — each is the
router refusing to give a potentially-wrong answer instead of guessing, in
favor of a retryable rejection.

Two verbs are rejected outright (not retryable) in routed multi-shard mode,
each with a documented alternative:

- `POST /collections/{id}/duplicates` → `501 duplicates_not_routed`.
  Duplicate detection is local-shard-only (filters by `min_group_size`
  before any cross-shard merge could happen) and does not support routed
  multi-shard mode; there is no cross-shard alternative today. Do not retry.
- `POST /collections/{id}/reindex/stream` → `501 reindex_stream_not_routed`.
  The streaming bulk-reindex path bypasses per-item shard ownership and the
  write fence; not supported in routed multi-shard mode. Use `POST
  /collections/{id}/index` instead — it is routed per item and observes the
  write fence like every other write path. Do not retry the stream endpoint
  itself.

## Connection
Any REST client, no driver. HTTP/1.1 is the compatibility/smoke path; the
performance target is high-QPS, large corpus traffic over pooled HTTP/2 streams,
where multiplexing and connection reuse dominate per-request overhead.

- **Production** — `https://<instance>.<namespace>.svc:7373`, a private
  ClusterIP whose TLS the serving pod terminates itself (ALPN `h2,
  http/1.1`). Nothing published sits in front of it, so the connection you
  authenticate is the connection lumen serves. Verify the server against the
  public CA distributed separately by the deployment administrator, in place of
  the public roots; authenticate yourself with a short-lived, audience-bound
  Kubernetes ServiceAccount token, sent as the request's bearer credential,
  which lumen resolves through TokenReview and SubjectAccessReview. The exact
  header form and how to mint the token are `--topic auth`.
- **Local / kind development** — `http://localhost:7373`, h2c, `spec.auth:
  disabled`. Every reachable node serves everyone; keep it off shared
  networks.

Sharded deployments route on the client:
`crc32(collection_id) % shard_count`.

## Do NOT ask lumen to
- store or return documents — it returns `external_id`s; hydrate them yourself
- run transactions or be the system of record
- aggregate (group-by / histogram / percentile / cardinality) — pair it with an
  OLAP store (ClickHouse / Druid / BigQuery / DuckDB)
- generate embeddings or hashes — you compute them; lumen indexes the bits
- return highlights, snippets, stored fields, or document payloads
- enforce per-document TTL/expiry independent of caller-owned delete/reindex
  events

## Exact wire shapes
`lumen spec` (OpenAPI), `lumen spec --shapes` (query cookbook), `lumen spec
--fields` (field/analyzer catalog), or `lumen llm --topic recipes` (task →
ready-to-POST body). `lumen llm --topic integration` covers database/pubsub
adapter boundaries.
"#
    .to_string()
}
