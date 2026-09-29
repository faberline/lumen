//! The checkpoint generation manifest, `_generation.json`: the collections one
//! generation holds and, for each, the base and delta segments it references
//! with their row maps and payload digests.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::persistence) struct SegmentGenerationManifest {
    pub(in crate::persistence) schema_version: u32,
    pub(in crate::persistence) checkpoint_sequence: u64,
    pub(in crate::persistence) revision: u64,
    pub(in crate::persistence) previous: Option<String>,
    pub(in crate::persistence) next_collection_generation: u64,
    pub(in crate::persistence) collections: Vec<CollectionCatalog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::persistence) struct CollectionCatalog {
    pub(in crate::persistence) collection_id: String,
    pub(in crate::persistence) collection_generation: u64,
    pub(in crate::persistence) schema_version: u32,
    pub(in crate::persistence) data_version: u64,
    pub(in crate::persistence) schema: serde_json::Value,
    pub(in crate::persistence) segments: Vec<SegmentReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::persistence) struct SegmentReference {
    pub(in crate::persistence) role: SegmentRole,
    pub(in crate::persistence) field: Option<String>,
    pub(in crate::persistence) ordinal: u32,
    pub(in crate::persistence) kind: SegmentKind,
    pub(in crate::persistence) format: SegmentFormat,
    pub(in crate::persistence) path: String,
    pub(in crate::persistence) local_rows: Option<LocalRowsReference>,
    #[serde(default)]
    pub(in crate::persistence) applied_seq: Option<u64>,
    #[serde(default)]
    pub(in crate::persistence) payload_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::persistence) enum SegmentRole {
    Field,
    CollectionEids,
    VectorEids,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::persistence) enum SegmentKind {
    Base,
    Delta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::persistence) enum SegmentFormat {
    #[serde(rename = "lseg-v1")]
    LsegV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::persistence) struct LocalRowsReference {
    pub(in crate::persistence) format: String,
    pub(in crate::persistence) path: String,
    pub(in crate::persistence) count: u32,
}
