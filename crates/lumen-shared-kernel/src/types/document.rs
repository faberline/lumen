//! Writing documents: the index, replace and unindex batches, their per-item
//! results, and the field values an item carries.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Index (write)
// ---------------------------------------------------------------------------

/// Maximum number of items accepted in one [`IndexRequest`]. `index` is the
/// high-traffic partial-write path (unlike the full-replacement
/// [`MAX_BATCH_REPLACE_SIZE`]/[`MAX_BATCH_SEARCH_SIZE`] batches, which stay
/// at 32), so the cap is set well above those to avoid breaking bulk
/// loaders while still bounding single-request raft-apply amplification. A
/// request with more items than this is rejected with 400 before any
/// per-item work starts.
///
/// [`MAX_BATCH_SEARCH_SIZE`]: crate::types::search::MAX_BATCH_SEARCH_SIZE
pub const MAX_INDEX_BATCH_SIZE: usize = 1000;

/// `POST /collections/{id}/index` body.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IndexRequest {
    /// At most [`MAX_INDEX_BATCH_SIZE`] items; a longer batch is rejected
    /// with 400 before any item runs.
    pub items: Vec<IndexItem>,
    /// Optional idempotency key. Repeated requests within 5 min are
    /// silently deduplicated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IndexItem {
    pub external_id: String,
    pub field: String,
    pub value: FieldValue,
    /// Optional external version for last-write-wins. When set, lumen keeps the
    /// highest version per `(external_id, field)` and drops strictly-older
    /// writes (cf. Elasticsearch `version_type=external`), so out-of-order
    /// delivery cannot clobber a newer value. When absent, the write applies in
    /// arrival order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
}

/// Polymorphic field value. Validated against the declared `FieldType`
/// at index time; mismatches return 422.
///
/// On the wire, `Vector` is a plain JSON `[f32]` array — `serde(untagged)`
/// resolves the variant by JSON shape (string vs number vs list-of-string
/// vs list-of-number).
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(untagged)]
pub enum FieldValue {
    String(String),
    Number(f64),
    Vector(Vec<f32>),
    StringList(Vec<String>),
    /// An integer beyond 2^53 that fits `i64`, kept exact. Decoded by the
    /// hand-written [`Deserialize`] below; never produced for smaller integers.
    Int64(i64),
}

/// Largest magnitude an integer may have and still decode as `Number(f64)`.
/// Every integer up to 2^53 is exact in an `f64`.
const MAX_SAFE_F64_INTEGER: u128 = 1 << 53;

/// Decodes a JSON integer: `Int64` above 2^53 in magnitude when it fits `i64`,
/// otherwise `Number`. An integer outside the i64 range stays `Number` (lossy),
/// by desk decision A (faberline/aw#556).
pub fn decode_integer<E: de::Error>(n: i128) -> Result<FieldValue, E> {
    if n.unsigned_abs() <= MAX_SAFE_F64_INTEGER {
        Ok(FieldValue::Number(n as f64))
    } else if let Ok(v) = i64::try_from(n) {
        Ok(FieldValue::Int64(v))
    } else {
        Ok(FieldValue::Number(n as f64))
    }
}

/// One array element: a number (kept as `f32`, as `Vector` stores it) or a string.
enum Element {
    Number(f32),
    String(String),
}

impl<'de> Deserialize<'de> for Element {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ElementVisitor;

        impl<'de> Visitor<'de> for ElementVisitor {
            type Value = Element;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a number or string")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Element, E> {
                Ok(Element::String(value.to_owned()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Element, E> {
                Ok(Element::String(value))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Element, E> {
                Ok(Element::Number(value as f32))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Element, E> {
                Ok(Element::Number(value as f32))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Element, E> {
                Ok(Element::Number(value as f32))
            }
        }

        deserializer.deserialize_any(ElementVisitor)
    }
}

/// Hand-written so that integers above 2^53 decode to `Int64` exactly.
/// `serde(untagged)` would turn every integer into `Number(f64)`.
impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldValueVisitor;

        impl<'de> Visitor<'de> for FieldValueVisitor {
            type Value = FieldValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, number, or homogeneous array of strings or numbers")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<FieldValue, E> {
                Ok(FieldValue::String(value.to_owned()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<FieldValue, E> {
                Ok(FieldValue::String(value))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<FieldValue, E> {
                Ok(FieldValue::Number(value))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<FieldValue, E> {
                decode_integer(value as i128)
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<FieldValue, E> {
                decode_integer(value as i128)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<FieldValue, A::Error> {
                // Grow only after each observed element, as the wire decoder does.
                let mut numbers = Vec::new();
                let mut strings = Vec::new();
                let mut kind = None;
                while let Some(element) = seq.next_element::<Element>()? {
                    match element {
                        Element::Number(value) => {
                            if matches!(kind, Some(false)) {
                                return Err(de::Error::custom("mixed field-value array"));
                            }
                            kind = Some(true);
                            numbers.push(value);
                        }
                        Element::String(value) => {
                            if matches!(kind, Some(true)) {
                                return Err(de::Error::custom("mixed field-value array"));
                            }
                            kind = Some(false);
                            strings.push(value);
                        }
                    }
                }
                Ok(match kind {
                    Some(false) => FieldValue::StringList(strings),
                    Some(true) | None => FieldValue::Vector(numbers),
                })
            }
        }

        deserializer.deserialize_any(FieldValueVisitor)
    }
}

impl FieldValue {
    /// True for an empty list (`[]`), which a write treats as "no value".
    pub fn is_missing(&self) -> bool {
        match self {
            FieldValue::Vector(v) => v.is_empty(),
            FieldValue::StringList(v) => v.is_empty(),
            FieldValue::String(_) | FieldValue::Number(_) | FieldValue::Int64(_) => false,
        }
    }

    /// The f64 a number-field query compares with: `Number(x)` is `x`, `Int64(v)`
    /// is `v as f64` (the same value an index write stores), anything else is `None`.
    pub fn as_query_number(&self) -> Option<f64> {
        match self {
            FieldValue::Number(x) => Some(*x),
            FieldValue::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IndexResponse {
    pub indexed: u32,
    pub bytes_written: BTreeMap<String, u64>,
    pub shard_lag_ms: u64,
}

// ---------------------------------------------------------------------------
// Replace docs (full-replacement write)
// ---------------------------------------------------------------------------

/// Maximum number of items accepted in one [`ReplaceDocsRequest`]. Sibling
/// knob to [`MAX_BATCH_SEARCH_SIZE`]; a request with more items than this
/// is rejected with 400 before any per-item work starts.
///
/// [`MAX_BATCH_SEARCH_SIZE`]: crate::types::search::MAX_BATCH_SEARCH_SIZE
pub const MAX_BATCH_REPLACE_SIZE: usize = 32;

/// `PUT /collections/{id}/docs:replace` body — a batch of full-replacement
/// upserts. `docs:replace` is one literal path segment (AIP-136
/// custom-method syntax) appended after `{collection_id}`, so it never
/// collides with `/collections/{collection_id}/docs/{external_id}`.
///
/// Each item's `fields` becomes the doc's *entire* indexed state for that
/// collection: declared schema fields the doc has today but that are
/// absent from `fields` are implicitly deleted. Replaying the same request
/// converges to the same state (PUT semantics) — this is a full
/// replacement, not a merge; use `POST /collections/{id}/index` when a
/// caller owns only some fields of a doc and wants to update those without
/// touching the rest.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceDocsRequest {
    /// At most [`MAX_BATCH_REPLACE_SIZE`] items; a longer batch is rejected
    /// with 400 before any item runs.
    pub docs: Vec<ReplaceDocItem>,
}

/// One item of a [`ReplaceDocsRequest`]: the target doc's full field set.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceDocItem {
    pub external_id: String,
    /// Optional doc-level version for last-write-wins, using the caller's
    /// source-row version. Unlike [`IndexItem::version`]'s per-`(external_id,
    /// field)` cell versioning, this is a single version for the whole doc:
    /// a strictly-older version arriving later drops the *entire* item (all
    /// fields), reported as [`ReplaceDocResult::Dropped`]. When absent, the
    /// write applies in arrival order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// The doc's complete indexed field set. Declared schema fields absent
    /// here are implicitly deleted from the doc.
    pub fields: BTreeMap<String, FieldValue>,
}

/// `PUT /collections/{id}/docs:replace` response: one [`ReplaceDocResult`]
/// per request item, in the same order and with the same length as `docs`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceDocsResponse {
    pub results: Vec<ReplaceDocResult>,
}

/// One batch-item outcome, tagged by `status`. A per-item failure (for
/// example a type-mismatched field value) never fails the whole batch: the
/// batch-level HTTP status stays 200 and the failure is reported here as
/// `{"status":"error","code":"...","message":"..."}` alongside `{"status":
/// "ok",...}` siblings.
///
/// Stale-version semantics (chosen over ok-with-no-write): an item whose
/// `version` is strictly older than the doc's currently stored version is
/// reported as its own `{"status":"dropped","current_version":...}`
/// variant rather than folded into `ok` or `error` — that keeps "no write
/// happened because a newer version already won" distinguishable from both
/// "wrote successfully" and "this item failed validation".
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ReplaceDocResult {
    Ok {
        /// Number of fields written from this item's `fields` map.
        fields_written: u32,
        /// Number of fields skipped as unchanged no-ops: the incoming
        /// value matched the currently indexed state, so no posting-list
        /// rewrite and no HNSW tombstone/reinsert happened for that field
        /// (#1293 server-side no-op suppression). `0` when every field
        /// actually changed, or when this is the doc's first write.
        fields_skipped: u32,
    },
    Dropped {
        /// The version currently stored for this doc, which won over the
        /// stale `version` carried by the request item.
        current_version: u64,
    },
    Error {
        code: String,
        message: String,
    },
}

/// `PUT /collections/{id}/docs/{external_id}` body — single-resource sugar
/// for one [`ReplaceDocItem`]. `external_id` comes from the path, so this
/// carries only `version` and `fields`; posting it is semantically
/// identical to sending a one-item [`ReplaceDocsRequest`] to `docs:replace`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceDocBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    pub fields: BTreeMap<String, FieldValue>,
}

// ---------------------------------------------------------------------------
// Batch document removal
// ---------------------------------------------------------------------------

/// Maximum caller-owned identifiers accepted by one `docs:unindex` command.
/// The limit bounds one durable shard command as well as the HTTP request.
pub const MAX_BATCH_UNINDEX_DOCS_SIZE: usize = 1000;

/// `POST /collections/{id}/docs:unindex` body.
///
/// This is intentionally only an identifier list.  It has no field selector,
/// filter, query, or request id: each selected document loses every indexed
/// field on its owning physical shard.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BatchUnindexDocsRequest {
    #[schema(schema_with = opaque_external_ids_schema)]
    pub external_ids: Vec<String>,
}

fn opaque_external_ids_schema() -> utoipa::openapi::schema::Array {
    use utoipa::openapi::schema::{ArrayBuilder, Object, SchemaType};

    ArrayBuilder::new()
        .items(Object::with_type(SchemaType::String))
        .min_items(Some(1))
        .max_items(Some(MAX_BATCH_UNINDEX_DOCS_SIZE))
        .unique_items(true)
        .build()
}

/// The validation error shared by HTTP admission, routed local writes, and
/// replay/state-machine apply.  Keeping this independent of HTTP errors makes
/// a malformed durable command fail before it can mutate one replica only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchUnindexDocsValidationError {
    Empty,
    TooLarge { got: usize, max: usize },
    DuplicateExternalId { external_id: String },
}

impl std::fmt::Display for BatchUnindexDocsValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("external_ids must contain at least one identifier"),
            Self::TooLarge { got, max } => {
                write!(f, "external_ids has {got} items, max is {max}")
            }
            Self::DuplicateExternalId { external_id } => {
                write!(
                    f,
                    "external_ids contains duplicate identifier `{external_id}`"
                )
            }
        }
    }
}

impl std::error::Error for BatchUnindexDocsValidationError {}

/// Validate the fixed `docs:unindex` command shape without changing any
/// state.  `external_id` remains opaque, so this deliberately imposes no
/// character or length policy beyond JSON string decoding and uniqueness.
pub fn validate_batch_unindex_docs_request(
    req: &BatchUnindexDocsRequest,
) -> Result<(), BatchUnindexDocsValidationError> {
    if req.external_ids.is_empty() {
        return Err(BatchUnindexDocsValidationError::Empty);
    }
    if req.external_ids.len() > MAX_BATCH_UNINDEX_DOCS_SIZE {
        return Err(BatchUnindexDocsValidationError::TooLarge {
            got: req.external_ids.len(),
            max: MAX_BATCH_UNINDEX_DOCS_SIZE,
        });
    }
    let mut seen = std::collections::BTreeSet::new();
    for external_id in &req.external_ids {
        if !seen.insert(external_id) {
            return Err(BatchUnindexDocsValidationError::DuplicateExternalId {
                external_id: external_id.clone(),
            });
        }
    }
    Ok(())
}
