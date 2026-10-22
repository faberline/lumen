//! Offline, machine-readable self-description for agent integration.
//!
//! The `lumen spec` CLI subset emits everything an LLM agent needs to wire
//! lumen into a RAG / tool pipeline — schema, query-shape cookbook, field /
//! analyzer catalog — straight from the installed binary, with no running
//! server and no network. This module is the single source for that surface;
//! the CLI and the (legacy) `lumen-openapi-dump` binary both call into it.

pub(crate) mod dx;
pub(crate) mod llm_auth;
pub(crate) mod llm_deployment;
pub(crate) mod llm_quickstart;
pub(crate) mod llm_storage;
pub(crate) mod llm_workflow;
pub(crate) mod query_shapes;

use serde_json::{json, Value};

use crate::app::spec::query_shapes::query_shapes;

/// The full OpenAPI 3.2 document as pretty JSON (every route + schema,
/// including the #1297 `QUERY` twins injected by `crate::app::http::openapi::openapi`).
pub fn openapi_json() -> String {
    serde_json::to_string_pretty(&openapi_value()).expect("OpenApi value serializes to JSON")
}

/// The full OpenAPI 3.2 document as YAML for LLM/agent reading.
pub fn openapi_yaml() -> String {
    serde_yaml::to_string(&openapi_value()).expect("OpenApi value serializes to YAML")
}

/// `crate::app::http::openapi::openapi()` as a JSON [`Value`] stamped as OpenAPI 3.2 (#1298,
/// epic #1296): utoipa 4.2.3's `OpenApiVersion` enum predates OpenAPI 3.2 and
/// only knows how to serialize the literal `"3.0.3"`, so the typed
/// `utoipa::openapi::OpenApi` — and the live `GET /openapi.json` route that
/// serves it verbatim via `service_http::standard_probe_routes` — keeps
/// declaring 3.0.3. This offline `lumen spec` surface (and the
/// `clients/openapi.json` contract file regenerated from it) is not bound by
/// that typed field, so it stamps the real document version here; the
/// `query`/`x-post-twin` operations are unaffected either way since they are
/// injected upstream in `crate::app::http::openapi::openapi`.
fn openapi_value() -> Value {
    let mut v = serde_json::to_value(crate::app::http::openapi::openapi())
        .expect("OpenApi serializes to JSON");
    if let Value::Object(map) = &mut v {
        map.insert("openapi".to_string(), Value::String("3.2.0".to_string()));
    }
    v
}

/// Just the component schemas (the request/response data types) as pretty JSON
/// — the JSON-Schema view an agent uses to build/validate request bodies.
pub fn json_schema_json() -> String {
    let api = crate::app::http::openapi::openapi();
    // #2871 retired the bearer/identity registry, so `operationalSchemas` no
    // longer carries a `TokenRegistry` entry: nothing reads that file, and a
    // published schema for it would read as a supported deployment shape.
    serde_json::to_string_pretty(&json!({ "components": api.components }))
        .expect("components serialize to JSON")
}

/// The field-type + analyzer + vector-metric catalog — what `type`/`analyzer`/
/// `metric` values a `PUT /collections/{id}` schema may use. Mirrors the
/// `FieldType` / `Analyzer` / `VectorMetric` enums.
pub fn field_catalog() -> Value {
    crate::app::spec::dx::field_catalog()
}

/// The agent-facing LLM topic outline (`lumen llm --topic outline`) as
/// Markdown.
pub fn llm_outline_md() -> String {
    r#"# lumen LLM outline

Use the smallest topic that answers the task:

- `lumen llm --topic workflow` — product model, declare→ingest→search→hydrate, query
  flavor choices, batch search (`POST /collections:search`), full-replacement
  writes (`PUT /collections/{id}/docs:replace`), QUERY-first search
  (RFC 10008 `QUERY /collections/{id}` and `QUERY /collections`, POST always
  available), connection, and non-goals.
- `lumen llm --topic integration` — recommended Postgres/AlloyDB adapter boundary:
  outbox or CDC, external Pub/Sub retry/DLQ ownership, HTTP writes into lumen,
  and no direct external writes to lumen's internal WAL.
- `lumen llm --topic quickstart` — copy-paste local create → index → search flow.
- `lumen llm --topic auth` — request-authentication contract: local auth-off,
  Standalone in-cluster default ServiceAccount tokens, Managed private-audience
  tokens, and the TokenReview/SubjectAccessReview model that replaced the
  retired bearer/Google registry.
- `lumen llm --topic deployment` — Kubernetes-native deployment topology:
  StatefulSet, shardCount, replicasPerShard, HPA boundary, reshard workflow,
  and empty-PVC bootstrap.
- `lumen llm --topic storage` — operator storage/ops contract: the serving fleet is
  always a StatefulSet with a durable PVC-backed WAL, including at
  `replicasPerShard: 1`.
- `lumen llm --topic recipes` — task → ready-to-POST query bodies.
- `lumen spec --format openapi-yaml` — OpenAPI YAML for LLM/agent reading.
- `lumen spec` — OpenAPI JSON, JSON-schema, query-shape, field, analyzer, and
  vector metric catalogs.
- `lumen connect` — manage a `kubectl port-forward` for the duration of a
  wrapped command against a k8s-deployed Lumen instance (`--cr`/`--service` +
  `--namespace`), tearing it down when the command exits. Against an
  `auth: required` fleet, add `--client-sa` to mint a short-lived
  audience-bound token and `--ca-file` to verify the server against the
  externally distributed public CA; the token is attached by a loopback proxy, so it
  reaches no environment variable and no child process.
- `lumen query index|search|duplicates|collections list` — one-shot query
  wrappers against a reachable node (`--url`/`LUMEN_URL`); request bodies match
  `lumen spec --shapes`. There is no credential flag by design — run them under
  `lumen connect`, which owns minting and attaching. See `--topic auth`.
"#
    .to_string()
}

/// The recommended database/pubsub integration boundary (`lumen llm --topic
/// integration`) as Markdown.
pub fn llm_integration_md() -> String {
    let mut out = r#"# lumen integration

## Recommended Postgres / AlloyDB integration
Use this boundary when Postgres or AlloyDB is the source of truth:
1. Commit application data in the database first. If you need crash-safe
   delivery, write an outbox row in the same transaction or consume CDC from
   the committed log; do not make lumen a transaction participant.
2. Run an adapter/sidecar that consumes CDC, Pub/Sub, Kafka, or the outbox and
   translates each source change into lumen HTTP writes (`POST
   /collections/{id}/index` and the delete endpoint). The adapter owns cloud
   envelopes, ACK/retry/DLQ policy, upstream auth, and stale-event filtering.
3. POST to the collection's shard and ACK upstream only after lumen returns
   success. Replaying an upsert of `(external_id, field)` is safe because it
   replaces that field; retry deletes until they succeed.
4. If upstream delivery can arrive out of order, carry a monotonic
   `source_version` / commit LSN in the adapter and suppress stale writes before
   POSTing.
5. Do not publish directly to lumen's internal WAL. External producers use the
   HTTP API so every write goes through validation, routing, and the same
   log/apply path.

## Ownership boundary
- lumen core owns schema validation, sharded HTTP writes, the internal WAL,
  ordered apply, search, and ranked `external_id` responses.
- The adapter owns source-specific envelopes, Pub/Sub subscription settings,
  ACK/retry/DLQ, upstream credentials, source offsets, stale-event suppression,
  and hydration against the source database.
"#
    .to_string();
    out.push_str("\n## Shared generated-client primitive\n");
    out.push_str(openapi_codegen::llm::topic().body);
    out.push_str("\n## Shared h2c client primitive\n");
    out.push_str(transport_h2c::llm::topic().body);
    out
}

/// Task → ready-to-POST body recipes (`lumen llm --topic recipes`) as Markdown,
/// rendered from [`query_shapes`] so the bodies never drift from the canonical
/// cookbook.
pub fn llm_recipes_md() -> String {
    let shapes = query_shapes();
    let endpoint = shapes["search_endpoint"].as_str().unwrap_or("");
    let mut out = String::from("# lumen query recipes\n\n");
    if !endpoint.is_empty() {
        out.push_str(&format!("Search endpoint: `{endpoint}`\n\n"));
    }
    out.push_str(
        "Each recipe is a ready-to-POST request body. Same source as `lumen spec \
         --shapes`.\n\n",
    );
    if let Some(list) = shapes["shapes"].as_array() {
        for s in list {
            let name = s["name"].as_str().unwrap_or("recipe");
            let desc = s["description"].as_str().unwrap_or("");
            let req = serde_json::to_string_pretty(&s["request"]).unwrap_or_default();
            out.push_str(&format!("## {name}\n{desc}\n\n```json\n{req}\n```\n\n"));
        }
    }
    out
}
