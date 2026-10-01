//! `GET /collections/{collection_id}/stats`: what the engine holds for one
//! collection.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::shared_kernel::types::schema::FieldType;

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Engine-level metadata about one collection.
///
/// Everything here describes **the index**, not the caller's data —
/// lumen is a search specialist, not an analytics engine. For data
/// aggregations (group-by / histogram / percentile / pipeline), pair
/// lumen with an OLAP store (ClickHouse / Druid / BigQuery / DuckDB)
/// and dual-write.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StatsResponse {
    /// Distinct `external_id` count in this collection.
    pub documents_indexed: u64,
    /// Per-field engine metadata.
    pub fields: BTreeMap<String, FieldStats>,
    /// Aggregate storage footprint.
    pub storage: StorageStats,
    /// Read-cache health (`moka` byte-weighted LRU on the LSM path).
    pub cache: CacheStats,
    /// Most recent successful write into this collection, RFC 3339
    /// (UTC). `None` when no write has landed since the last reboot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_indexed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FieldStats {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Distinct terms / values / elements / vectors (depending on type).
    pub unique_terms: u64,
    /// Bytes the engine attributes to this field's indexes.
    pub bytes: u64,
    /// Mean tokens per document, only populated on `text` fields.
    /// Exposes the BM25 length-normalization denominator so callers
    /// can reason about scoring stability without dumping internals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avg_doc_len: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StorageStats {
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CacheStats {
    /// Hit ratio on the posting-list cache. `1.0` when no cache layer
    /// is attached (in-memory engine has no need for one).
    pub posting_hit_ratio: f32,
}
