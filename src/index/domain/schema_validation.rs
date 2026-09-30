//! Schema and value checks: a collection's field specs before the collection is
//! created, and a value against its field's type before a replace changes
//! anything.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};

use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::{FieldSpec, FieldType};

pub(in crate::index) fn validate_schema(schema: &BTreeMap<String, FieldSpec>) -> Result<()> {
    for (name, spec) in schema {
        if name.is_empty() {
            bail!("field name cannot be empty");
        }
        if matches!(spec.field_type, FieldType::Text) && spec.analyzer.is_none() {
            bail!("text field `{name}` is missing analyzer (normalize() should default it)");
        }
        if !matches!(spec.field_type, FieldType::Text) && spec.analyzer.is_some() {
            bail!(
                "field `{name}` of type {:?} does not accept an analyzer",
                spec.field_type
            );
        }
        // Reject vector-specific fields on non-vector field types so
        // typos like `{type: "keyword", dim: 768}` fail loudly.
        if !matches!(spec.field_type, FieldType::Vector) {
            if spec.dim.is_some()
                || spec.metric.is_some()
                || spec.backend.is_some()
                || spec.quantize.is_some()
            {
                bail!(
                    "field `{name}` of type {:?} does not accept vector spec keys (dim/metric/backend/quantize)",
                    spec.field_type
                );
            }
        } else {
            // Eager validation so bad vector specs fail at schema time
            // rather than at index time.
            spec.vector_spec()
                .map_err(|e| anyhow!("vector field `{name}`: {e}"))?;
        }
    }
    Ok(())
}

pub(in crate::index) fn value_kind(v: &FieldValue) -> &'static str {
    match v {
        FieldValue::String(_) => "string",
        FieldValue::Number(_) => "number",
        FieldValue::Vector(_) => "f32[]",
        FieldValue::StringList(_) => "string[]",
    }
}

/// Non-mutating type-check mirroring `apply_value`'s match arms, used by
/// `replace_one_doc` to validate every field of a `docs:replace` item
/// *before* any mutation happens (see `replace_one_doc` for why that
/// ordering matters). Kept in sync with `apply_value`'s arms by hand: any
/// new `(FieldIndex, FieldValue)` pairing accepted there must be mirrored
/// here.
pub(in crate::index) fn validate_value(
    fi: &FieldIndex,
    value: &FieldValue,
    field_name: &str,
) -> Result<()> {
    match (fi, value) {
        (FieldIndex::Text { .. }, FieldValue::String(_)) => Ok(()),
        (FieldIndex::Keyword(_), FieldValue::String(_)) => Ok(()),
        (FieldIndex::Number(_), FieldValue::Number(x)) => {
            SortableF64::new(*x).map_err(|e| StorageError::InvalidNumber(e.to_string()))?;
            Ok(())
        }
        (FieldIndex::Set(_), FieldValue::StringList(_)) => Ok(()),
        (FieldIndex::Set(_), FieldValue::String(_)) => Err(StorageError::TypeMismatch {
            field: field_name.to_string(),
            expected: FieldType::Set,
            got: "string (expected array of strings)",
        }
        .into()),
        (FieldIndex::Vector { spec, .. }, FieldValue::Vector(v)) => {
            if v.len() as u32 != spec.dim {
                bail!(
                    "vector field `{field_name}` declared dim={} but got vector of length {}",
                    spec.dim,
                    v.len()
                );
            }
            Ok(())
        }
        (FieldIndex::Hash(_), FieldValue::String(s)) => {
            parse_hash(s)?;
            Ok(())
        }
        (fi, v) => Err(StorageError::TypeMismatch {
            field: field_name.to_string(),
            expected: fi.field_type(),
            got: value_kind(v),
        }
        .into()),
    }
}
