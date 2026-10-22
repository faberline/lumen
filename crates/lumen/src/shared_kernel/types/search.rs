//! Search requests and responses: one search, a paged scan of a collection, a
//! batch of searches, and duplicate detection.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::shared_kernel::types::query::{QueryNode, SortSpec};

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// `POST /collections/{id}/search` body.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchRequest {
    pub query: QueryNode,
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// Zero-based deep-page jump applied after filtering and global
    /// score/field ordering. Defaults to zero. `offset` and `cursor` are
    /// mutually exclusive; use an offset for a direct jump, then issue another
    /// offset request or restart cursor pagination from the first page.
    #[serde(default)]
    #[schema(default = 0)]
    pub offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Optional caller-owned routing key for sharded deployments. When present,
    /// a sharded router can target the one shard that owns
    /// `(collection_id, routing_key)` instead of scatter/gathering every shard.
    /// Writes default to `external_id` as the routing key, so one large
    /// collection can still spread across shards without caller help.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    /// Sort results by one or more fields instead of by relevance score.
    /// When absent, results are ranked by score (BM25 / constant) then
    /// external_id. Number and keyword fields are sortable (up to 4 keys);
    /// single number-field sorts use the keyset planner, keyword and composite
    /// sorts use the materialized fallback. Rows missing a sort-key value
    /// follow the per-key `missing` mode: `exclude` (the default) drops them
    /// from the page and from `total`; `first`/`last` keep them — placed
    /// before/after all present values and counted in `total`, like SQL
    /// `NULLS FIRST`/`NULLS LAST`. A query containing `has_child` can be
    /// sorted by parent fields; it routes through the materialized sort path
    /// with exact `total`. `sort` remains incompatible with `knn`, `rrf`, and
    /// `hamming`. Legacy offset cursors remain incompatible with the normal
    /// keyset sort path; the native `offset` field deliberately selects the
    /// exact materialized deep-page path instead. Page sequentially with the
    /// keyset cursor returned by a first-page request, or jump directly with
    /// `offset`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<SortSpec>>,
    /// Whether to compute the exact total match count. Defaults to `true`
    /// (back-compat). When `false`, the planner may early-terminate and
    /// `total` becomes a lower bound (≥ the returned page size).
    #[serde(default = "default_track_total")]
    pub track_total: bool,
    /// Collapse (field-collapse / group-by) on a keyword field: return ONE hit
    /// per distinct value of this field, scored by the MAX member score, ranked
    /// over groups. `hit.external_id` becomes the collapse value, `total` the
    /// distinct-group count. Used for nested `group` search: filter the child
    /// collection, collapse by `parent_row_id` → distinct matching parents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collapse: Option<String>,
}

fn default_limit() -> u32 {
    20
}

fn default_track_total() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchHit {
    pub external_id: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
    pub total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub took_ms: u64,
    /// Server-side engine time in microseconds — the same measurement as
    /// `took_ms` at sub-millisecond resolution, so callers can see how fast the
    /// engine answered when `took_ms` rounds to 0.
    #[serde(default)]
    pub took_us: u64,
}

/// `POST /collections/{id}/search:all` body. This is an explicitly expensive
/// full-materialization operation for export/maintenance workflows; ordinary
/// interactive queries should use [`SearchRequest`] pagination.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchAllRequest {
    pub query: QueryNode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<SortSpec>>,
}

/// Complete matching id set returned by `search:all` from one local read-lock
/// snapshot, or from one snapshot per routed shard (there is intentionally no
/// cross-shard transaction).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchAllResponse {
    pub external_ids: Vec<String>,
    pub total: u64,
    pub took_ms: u64,
    pub took_us: u64,
}

// ---------------------------------------------------------------------------
// Batch search
// ---------------------------------------------------------------------------

/// Maximum number of items accepted in one [`BatchSearchRequest`]. Doubles
/// as the concurrent fan-out bound for `POST /collections:search`; a
/// request with more items than this is rejected with 400 before any
/// per-item work starts.
pub const MAX_BATCH_SEARCH_SIZE: usize = 32;

/// `POST /collections:search` body — an msearch-style batch of independent
/// `(collection, SearchRequest)` items executed with server-side
/// concurrent fan-out. `collections:search` is one literal path segment
/// (AIP-136 custom-method syntax), so it never collides with
/// `/collections/{collection_id}`.
///
/// Each item carries its own full [`SearchRequest`] — `limit`, `sort`,
/// `cursor`, `collapse`, `routing_key`, and `track_total` may all differ
/// per item. There is no cross-collection ranking or merged pagination:
/// results, and cursors, stay independent per item.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchSearchRequest {
    /// At most [`MAX_BATCH_SEARCH_SIZE`] items; a longer batch is rejected
    /// with 400 before any item runs.
    pub searches: Vec<BatchSearchItem>,
}

/// One item of a [`BatchSearchRequest`]. Flattened on the wire, so an item
/// looks like `{"collection": "...", "query": {...}, "limit": 20, ...}` —
/// the same fields `POST /collections/{id}/search` accepts, plus the
/// target `collection`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchSearchItem {
    pub collection: String,
    #[serde(flatten)]
    pub request: SearchRequest,
}

/// `POST /collections:search` response: one [`BatchSearchResult`] per
/// request item, in the same order and with the same length as
/// `searches`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BatchSearchResponse {
    pub results: Vec<BatchSearchResult>,
}

/// One batch-item outcome, tagged by `status`. A per-item failure (for
/// example an unknown collection) never fails the whole batch: the
/// batch-level HTTP status stays 200 and the failure is reported here as
/// `{"status":"error","code":"collection_not_found","message":"..."}`
/// alongside `{"status":"ok","response":{...}}` siblings.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum BatchSearchResult {
    Ok { response: SearchResponse },
    Error { code: String, message: String },
}

// ---------------------------------------------------------------------------
// Duplicates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DuplicatesRequest {
    pub field: String,
    #[serde(default = "default_min_group_size")]
    pub min_group_size: u32,
    #[serde(default = "default_dup_limit")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

pub(super) fn default_min_group_size() -> u32 {
    2
}

fn default_dup_limit() -> u32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DuplicateGroup {
    pub value: serde_json::Value,
    pub external_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DuplicatesResponse {
    pub groups: Vec<DuplicateGroup>,
    pub truncated: bool,
    pub took_ms: u64,
}
