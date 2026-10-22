//! The query tree a search evaluates, and how its hits sort.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::search::default_min_group_size;

/// One sort key. `order` defaults to ascending.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SortSpec {
    pub field: String,
    #[serde(default)]
    pub order: SortOrder,
    /// How to treat rows that have no value for this sort key. Default
    /// `exclude` keeps today's behavior (such rows are dropped from results and
    /// from `total`). `first`/`last` keep them, placed before/after the rows
    /// that do have a value, and count them in `total`.
    #[serde(default)]
    pub missing: SortMissing,
}

/// Placement of rows missing a value for a sort key (SQL NULLS FIRST/LAST).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortMissing {
    /// Drop rows that lack a value for this key (default; today's behavior).
    #[default]
    Exclude,
    /// Place rows lacking a value before all rows that have one.
    First,
    /// Place rows lacking a value after all rows that have one.
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// A search query node. Externally tagged on the wire:
///
/// ```json
/// { "match": { "field": "bio", "text": "engineer" } }
/// { "term":  { "field": "tags", "value": "rust" } }
/// { "knn":   { "field": "embedding", "vector": [0.1, ...], "k": 10 } }
/// { "and":   [ {...}, {...} ] }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QueryNode {
    Match(MatchQuery),
    Term(TermQuery),
    Terms(TermsQuery),
    /// Case-sensitive UTF-8 starts-with match on a `keyword` field. Wire:
    /// `{"prefix":{"field":"path","value":"台北市/"}}`.
    Prefix(PrefixQuery),
    /// Filter to a set of external_ids (#182). Wire: `{"ids": {"values":[...]}}`.
    Ids(IdsQuery),
    Range(RangeQuery),
    Knn(KnnQuery),
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
    Not(Box<QueryNode>),
    /// Nested as a first-class clause: a parent matches if its child collection
    /// has ≥1 doc (linked by `field` = parent_row_id) matching the sub-`query`.
    /// Evaluates to the set of parent docids → composes under and/or/not like
    /// any other clause. Drives data-table `group` search inside arbitrary
    /// boolean trees. Wire: `{"has_child": {"collection","field","query":{...}}}`.
    #[serde(rename = "has_child")]
    HasChild(HasChildQuery),
    /// Perceptual/structural near-duplicate search over a `hash` field: every
    /// doc whose 64-bit hash is within `max_distance` Hamming bits of `hash`,
    /// scored by similarity (closer = higher). Wire:
    /// `{"hamming": {"field","hash":"<hex>","max_distance":N}}`.
    #[serde(rename = "hamming")]
    Hamming(HammingQuery),
    /// Reciprocal Rank Fusion: run each sub-query, rank its hits by score
    /// descending, and fuse into one ranking *by rank* — `score(d) = Σ_i 1/(k +
    /// rank_i(d))` over the sub-queries that returned `d` (rank is 1-based).
    /// Because it fuses ranks, BM25 and cosine score scales need no
    /// normalisation. This is hybrid lexical+semantic retrieval. Put any filter
    /// **inside each leg** (e.g. `{"and":[{"knn":…},{"term":…}]}`) so the kNN leg
    /// stays filter-correct. Wire: `{"rrf": {"queries":[{…},{…}], "k":60}}`.
    #[serde(rename = "rrf")]
    Rrf(RrfQuery),
    /// Non-null / "has a value" predicate over any indexed field: the set of docs
    /// that have ≥1 value in `field`. Composes under and/or/not like any filter.
    /// Drives data-table "is not empty" filters (the inverse is `{"not":{"exists":…}}`).
    /// Wire: `{"exists": {"field": "email"}}`.
    #[serde(rename = "exists")]
    Exists(ExistsQuery),
    /// Docs whose `field` value is SHARED — the value occurs in ≥ `min_group_size`
    /// docs (default 2). The boolean-composable form of `/duplicates`: drops into
    /// and/or/not (e.g. "duplicated email AND city=Taipei"). keyword/number/set
    /// only. Wire: `{"duplicated": {"field":"email","min_group_size":2}}`.
    #[serde(rename = "duplicated")]
    Duplicated(DuplicatedQuery),
}

/// `exists` predicate (see [`QueryNode::Exists`]).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExistsQuery {
    pub field: String,
}

/// `duplicated` predicate (see [`QueryNode::Duplicated`]).
/// Reuses `default_min_group_size` (defined with `DuplicatesRequest`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DuplicatedQuery {
    pub field: String,
    /// Minimum group size to count as duplicated (default 2).
    #[serde(default = "default_min_group_size")]
    pub min_group_size: u32,
}

/// Reciprocal Rank Fusion query (see [`QueryNode::Rrf`]).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RrfQuery {
    /// Sub-queries whose rankings are fused (≥1; typically a `knn` + a `match`).
    pub queries: Vec<QueryNode>,
    /// RRF rank constant — larger flattens the weighting. Default 60.
    #[serde(default = "default_rrf_k")]
    pub k: u32,
}

fn default_rrf_k() -> u32 {
    60
}

/// Hamming near-duplicate query over a `hash` field (see [`QueryNode::Hamming`]).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HammingQuery {
    pub field: String,
    /// The query hash as a 64-bit hex string (optionally `0x`-prefixed).
    pub hash: String,
    /// Inclusive maximum Hamming distance (0..=64) for a match.
    pub max_distance: u32,
}

/// `has_child` sub-query (see [`QueryNode::HasChild`]).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HasChildQuery {
    /// The child collection to evaluate `query` against.
    pub collection: String,
    /// The child's keyword field holding the parent's external_id.
    pub field: String,
    pub query: Box<QueryNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MatchQuery {
    pub field: String,
    pub text: String,
    #[serde(default = "default_match_op")]
    pub op: MatchOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MatchOp {
    And,
    Or,
}

fn default_match_op() -> MatchOp {
    MatchOp::And
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TermQuery {
    pub field: String,
    pub value: FieldValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TermsQuery {
    pub field: String,
    pub values: Vec<FieldValue>,
}

/// Case-sensitive UTF-8 starts-with query for `keyword` fields.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PrefixQuery {
    pub field: String,
    pub value: String,
}

/// `ids` query node (#182): filter to a set of external_ids. Each id is resolved
/// through the collection interner to a docid (unknown ids are skipped). It is
/// constant-scored and composes under and/or/not like term/terms. Removes the
/// need to index a redundant row-id keyword field for `row_id_in`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdsQuery {
    pub values: Vec<String>,
}

/// kNN vector search node. Returns the `k` external_ids closest to
/// `vector` under the field's declared metric. Scores are the negated
/// distance — higher = better, consistent with BM25 / term scoring.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KnnQuery {
    pub field: String,
    pub vector: Vec<f32>,
    pub k: u32,
}

/// A `RangeQuery` bound value. Wire form is untagged: a JSON number decodes
/// as `Number`, a JSON string as `Keyword` — one `RangeQuery` node handles
/// both numeric and keyword ranges, matching the ergonomics clients already
/// generate against (no sibling `KeywordRangeQuery` type). Validated at
/// query time against the target field's declared `FieldType`: `Number` is
/// valid only against `FieldType::Number` fields, `Keyword` only against
/// `FieldType::Keyword` fields (compared byte/lexicographically — the same
/// ordering `keyword` already uses for exact `term`/`terms` match, and the
/// ordering ISO-8601 date/datetime strings rely on for chronological sort).
/// A mismatched bound/field-type pair is rejected with 400 at query time,
/// not silently misparsed. `text`/analyzed fields are out of scope for range
/// queries — comparison is semantically fuzzy after tokenization.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RangeBound {
    Number(f64),
    Keyword(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RangeQuery {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<RangeBound>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<RangeBound>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<RangeBound>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<RangeBound>,
}
