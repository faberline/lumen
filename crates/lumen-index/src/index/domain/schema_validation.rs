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
        FieldValue::Int64(_) => "int64",
        FieldValue::Bool(_) => "boolean",
        FieldValue::Vector(_) => "f32[]",
        FieldValue::StringList(_) => "string[]",
    }
}

/// Accepts only a JSON boolean for a boolean field. Any other value is a
/// typed type mismatch.
// used by the boolean index slice (#430)
#[allow(dead_code)]
pub(in crate::index) fn validate_boolean_value(value: &FieldValue, field_name: &str) -> Result<()> {
    match value {
        FieldValue::Bool(_) => Ok(()),
        other => Err(StorageError::TypeMismatch {
            field: field_name.to_string(),
            expected: FieldType::Boolean,
            got: value_kind(other),
        }
        .into()),
    }
}

/// Non-mutating type-check mirroring `apply_value`'s match arms in
/// `src/index/application/apply.rs`, used by
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
    if value.is_missing() && !matches!(fi, FieldIndex::Vector { .. }) {
        return Ok(());
    }
    match (fi, value) {
        (FieldIndex::Text { .. }, FieldValue::String(_)) => Ok(()),
        (FieldIndex::Keyword(_), FieldValue::String(_)) => Ok(()),
        (FieldIndex::Number(_), FieldValue::Number(x)) => {
            SortableF64::for_value(*x).map_err(|e| StorageError::InvalidNumber(e.to_string()))?;
            Ok(())
        }
        (FieldIndex::Number(_), FieldValue::Int64(v)) => {
            SortableF64::for_value(*v as f64)
                .map_err(|e| StorageError::InvalidNumber(e.to_string()))?;
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

#[cfg(test)]
mod int64_tests {
    use super::*;
    use crate::index::domain::keyword_index::KeywordIndex;
    use crate::index::domain::number_index::NumberIndex;

    #[test]
    fn number_index_accepts_int64_as_f64() {
        let fi = FieldIndex::Number(NumberIndex::default());
        assert!(validate_value(&fi, &FieldValue::Int64(7), "count").is_ok());
    }

    #[test]
    fn keyword_index_refuses_int64() {
        let fi = FieldIndex::Keyword(KeywordIndex::default());
        assert!(validate_value(&fi, &FieldValue::Int64(7), "tag").is_err());
    }
}

#[cfg(test)]
mod boolean_tests {
    use super::*;
    use crate::index::domain::keyword_index::KeywordIndex;
    use crate::index::domain::number_index::NumberIndex;

    fn bool_field() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Boolean,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
            facetable: false,
        }
    }

    #[test]
    fn boolean_accepts_true() {
        assert!(validate_boolean_value(&FieldValue::Bool(true), "flag").is_ok());
    }

    #[test]
    fn boolean_accepts_false() {
        assert!(validate_boolean_value(&FieldValue::Bool(false), "flag").is_ok());
    }

    #[test]
    fn boolean_refuses_the_string_true() {
        let err = validate_boolean_value(&FieldValue::String("true".into()), "flag")
            .expect_err("a string is refused");
        // `expected` is printed with Debug (`Boolean`), so compare lowercased.
        let msg = err.to_string().to_lowercase();
        assert!(msg.contains("boolean"), "{msg}");
        assert!(msg.contains("string"), "{msg}");
    }

    #[test]
    fn boolean_refuses_the_number_zero() {
        let err = validate_boolean_value(&FieldValue::Number(0.0), "flag")
            .expect_err("a number is refused");
        assert!(err.to_string().contains("number"));
    }

    #[test]
    fn boolean_refuses_the_number_one() {
        let err = validate_boolean_value(&FieldValue::Number(1.0), "flag")
            .expect_err("a number is refused");
        assert!(err.to_string().contains("number"));
    }

    #[test]
    fn boolean_refuses_null() {
        // `null` never reaches validation: it does not decode to a FieldValue.
        assert!(serde_json::from_str::<FieldValue>("null").is_err());
    }

    #[test]
    fn bool_value_is_refused_on_a_keyword_field() {
        let fi = FieldIndex::Keyword(KeywordIndex::default());
        let err = validate_value(&fi, &FieldValue::Bool(true), "tag")
            .expect_err("a bool is refused on keyword");
        let msg = err.to_string().to_lowercase();
        assert!(msg.contains("boolean"), "{msg}");
        assert!(msg.contains("keyword"), "{msg}");
    }

    #[test]
    fn bool_value_is_refused_on_a_number_field() {
        let fi = FieldIndex::Number(NumberIndex::default());
        assert!(validate_value(&fi, &FieldValue::Bool(false), "count").is_err());
    }

    #[test]
    fn boolean_field_spec_is_still_declared_not_indexed() {
        assert_eq!(bool_field().field_type, FieldType::Boolean);
    }
}
