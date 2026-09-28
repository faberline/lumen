//! A collection's schema: the fields it declares, what each field type can do,
//! and the vector and analyzer settings a field carries.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Schema (DDL)
// ---------------------------------------------------------------------------

/// `PUT /collections/{id}` body.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateCollectionRequest {
    pub fields: BTreeMap<String, FieldSpec>,
}

/// `PUT /collections/{id}` response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateCollectionResponse {
    pub collection_id: String,
    pub version: u32,
    pub fields_count: u32,
}

/// One field declaration inside a collection schema.
///
/// The `dim` / `metric` / `backend` / `quantize` fields are only
/// meaningful when `field_type == FieldType::Vector`; they are
/// rejected by schema validation on any other field type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FieldSpec {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Analyzer for `text`. Ignored on other types. Defaults to
    /// `whitespace_lower` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<Analyzer>,
    /// Convenience flag: `{type: "keyword", multi: true}` is sugar for
    /// `{type: "set"}`. Normalized at schema-validation time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multi: Option<bool>,
    /// Vector dimensionality. Required when `type == "vector"`,
    /// rejected otherwise. Immutable for the field's lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dim: Option<u32>,
    /// Vector distance metric. Required when `type == "vector"`,
    /// rejected otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<VectorMetric>,
    /// Vector index backend. Defaults to `hnsw-cpu` when omitted on a
    /// vector field, rejected on any other field type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<VectorBackend>,
    /// Optional quantization scheme — only meaningful on vector
    /// fields. `sq` enables transparent scalar quantization (f32→u8);
    /// `pq` is reserved for a future product-quantization landing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantize: Option<VectorQuantize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Text,
    Keyword,
    Number,
    Set,
    Vector,
    /// 64-bit perceptual/structural hash (pHash / dHash / SimHash / b-bit
    /// MinHash). The caller computes the hash; lumen indexes the bits and
    /// answers Hamming-distance queries. Wire value is a hex string.
    Hash,
}

/// Operations a field type can truthfully expose to clients. This is shared by
/// runtime validation and `lumen spec --fields`, so a documentation update
/// cannot quietly advertise a query or sort the engine rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldCapabilities {
    pub bm25: bool,
    pub exact: bool,
    pub prefix: bool,
    pub range: bool,
    pub sort: bool,
    pub set_membership: bool,
    pub vector_search: bool,
    pub hamming: bool,
}

impl FieldType {
    pub const ALL: [Self; 6] = [
        Self::Text,
        Self::Keyword,
        Self::Number,
        Self::Set,
        Self::Vector,
        Self::Hash,
    ];

    pub const fn capabilities(self) -> FieldCapabilities {
        match self {
            Self::Text => FieldCapabilities {
                bm25: true,
                exact: false,
                prefix: false,
                range: false,
                sort: false,
                set_membership: false,
                vector_search: false,
                hamming: false,
            },
            Self::Keyword => FieldCapabilities {
                bm25: false,
                exact: true,
                prefix: true,
                range: true,
                sort: true,
                set_membership: false,
                vector_search: false,
                hamming: false,
            },
            Self::Number => FieldCapabilities {
                bm25: false,
                exact: true,
                prefix: false,
                range: true,
                sort: true,
                set_membership: false,
                vector_search: false,
                hamming: false,
            },
            Self::Set => FieldCapabilities {
                bm25: false,
                exact: true,
                prefix: false,
                range: false,
                sort: false,
                set_membership: true,
                vector_search: false,
                hamming: false,
            },
            Self::Vector => FieldCapabilities {
                bm25: false,
                exact: false,
                prefix: false,
                range: false,
                sort: false,
                set_membership: false,
                vector_search: true,
                hamming: false,
            },
            Self::Hash => FieldCapabilities {
                bm25: false,
                exact: false,
                prefix: false,
                range: false,
                sort: false,
                set_membership: false,
                vector_search: false,
                hamming: true,
            },
        }
    }
}

/// Distance metric for `FieldType::Vector`. Wire form is snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetric {
    Cosine,
    Dot,
    L2,
}

/// Index backend for `FieldType::Vector`. Wire forms are
/// `hnsw-cpu` / `flat-cpu` (kebab-case).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum VectorBackend {
    /// Approximate HNSW graph (CPU). Sub-linear, recall < 1.
    HnswCpu,
    /// Exact CPU brute-force: no index/build, parallel + vectorized full scan.
    /// 100% recall; for moderate N it beats both an approximate index's build
    /// cost and a single-threaded exact scan, and is the default CPU choice when
    /// exactness matters.
    FlatCpu,
}

impl Default for VectorBackend {
    fn default() -> Self {
        Self::HnswCpu
    }
}

/// Quantization scheme for `FieldType::Vector`.
///
/// `sq` enables transparent scalar quantization (f32 stored as u8).
/// `pq` is reserved for a future product-quantization landing and
/// is not yet implemented — declaring it will be rejected at schema
/// time until the backing codec ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VectorQuantize {
    Sq,
    Pq,
}

/// Resolved vector field configuration. Built from a `FieldSpec`
/// once schema validation has confirmed all required slots are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct VectorSpec {
    pub dim: u32,
    pub metric: VectorMetric,
    pub backend: VectorBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantize: Option<VectorQuantize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Analyzer {
    WhitespaceLower,
    Jieba,
    Ngram,
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

impl FieldSpec {
    /// Normalize sugar: `{type: "keyword", multi: true}` → `{type: "set"}`.
    /// Sets a default analyzer on `text` if absent. Fills in a default
    /// `hnsw-cpu` backend on a vector field when none is declared.
    pub fn normalize(mut self) -> Self {
        if matches!(self.field_type, FieldType::Keyword) && self.multi == Some(true) {
            self.field_type = FieldType::Set;
            self.multi = None;
        }
        if matches!(self.field_type, FieldType::Text) && self.analyzer.is_none() {
            self.analyzer = Some(Analyzer::WhitespaceLower);
        }
        if matches!(self.field_type, FieldType::Vector) && self.backend.is_none() {
            self.backend = Some(VectorBackend::default());
        }
        self
    }

    /// Resolve the vector-specific sub-shape if the field is a vector
    /// field. Returns `None` for non-vector fields. Returns an error
    /// when the field is declared as a vector but is missing required
    /// `dim` / `metric` slots.
    pub fn vector_spec(&self) -> anyhow::Result<Option<VectorSpec>> {
        if !matches!(self.field_type, FieldType::Vector) {
            return Ok(None);
        }
        let dim = self
            .dim
            .ok_or_else(|| anyhow::anyhow!("vector field is missing `dim`"))?;
        let metric = self
            .metric
            .ok_or_else(|| anyhow::anyhow!("vector field is missing `metric`"))?;
        let backend = self.backend.unwrap_or_default();
        if dim == 0 {
            anyhow::bail!("vector field `dim` must be > 0");
        }
        if matches!(self.quantize, Some(VectorQuantize::Pq)) {
            anyhow::bail!("product quantization (`pq`) is not yet implemented");
        }
        Ok(Some(VectorSpec {
            dim,
            metric,
            backend,
            quantize: self.quantize,
        }))
    }
}
