//! A borrowed source value in the private wire image: validated against its
//! field's type and dimension without decoding it, and compared with another
//! source value or set member by member, as `apply_value` would see it.

use crate::index::application::apply::committed_replace_plan::{
    ParsedValue, ParsedValues, ReplaceItemError,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::types::schema::FieldType;

pub(super) enum ValidationError {
    Domain(ReplaceItemError),
    Preparation(&'static str),
}

pub(super) fn validate_borrowed(
    ordinal: usize,
    kind: FieldType,
    value: &FastIndexValue<'_>,
    field: &str,
    eid_len: usize,
    dimension: Option<u32>,
    parsed: Option<ParsedValue>,
) -> std::result::Result<(u64, Option<u64>), ValidationError> {
    match (kind, value) {
        (FieldType::Text, FastIndexValue::String(_)) => parsed
            .and_then(|p| p.checksum)
            .map(|checksum| (0, Some(checksum)))
            .ok_or(ValidationError::Preparation(
                "missing precomputed Text checksum",
            )),
        (FieldType::Keyword, FastIndexValue::String(value)) => {
            let bytes = value
                .len()
                .checked_add(eid_len)
                .ok_or(ValidationError::Preparation("Keyword byte count overflow"))?;
            Ok((
                u64::try_from(bytes)
                    .map_err(|_| ValidationError::Preparation("Keyword byte count exceeds u64"))?,
                None,
            ))
        }
        (FieldType::Number, FastIndexValue::Number(value)) => SortableF64::new(*value)
            .map_err(|e| ValidationError::Domain(ReplaceItemError::InvalidNumber(e.to_string())))
            .and_then(|_| {
                u64::try_from(eid_len)
                    .map_err(|_| ValidationError::Preparation("Number id length exceeds u64"))
            })
            .and_then(|eid| {
                eid.checked_add(8)
                    .ok_or(ValidationError::Preparation("Number byte count overflow"))
            })
            .map(|bytes| (bytes, None)),
        (FieldType::Set, FastIndexValue::StringList(values)) => {
            Ok((set_bytes(*values, eid_len)?, None))
        }
        (FieldType::Hash, FastIndexValue::String(_)) if parsed.and_then(|p| p.hash).is_some() => {
            Ok((12, None))
        }
        (FieldType::Hash, FastIndexValue::String(_)) => {
            Err(ValidationError::Domain(ReplaceItemError::InvalidHash {
                ordinal,
            }))
        }
        (FieldType::Vector, FastIndexValue::Vector { len, .. })
            if u32::try_from(*len).ok() == dimension =>
        {
            let bytes =
                u64::try_from(*len)
                    .map_err(|_| ValidationError::Preparation("Vector length exceeds u64"))?
                    .checked_mul(std::mem::size_of::<f32>() as u64)
                    .ok_or(ValidationError::Preparation("Vector byte count overflow"))?
                    .checked_add(u64::try_from(eid_len).map_err(|_| {
                        ValidationError::Preparation("Vector id length exceeds u64")
                    })?)
                    .ok_or(ValidationError::Preparation("Vector byte count overflow"))?;
            parsed
                .and_then(|p| p.checksum)
                .map(|checksum| (bytes, Some(checksum)))
                .ok_or(ValidationError::Preparation(
                    "missing precomputed Vector checksum",
                ))
        }
        (FieldType::Vector, FastIndexValue::Vector { len, .. }) => Err(ValidationError::Domain(
            ReplaceItemError::InvalidVectorDimension {
                field: field.to_owned(),
                expected: dimension.unwrap_or(0),
                got: *len,
            },
        )),
        (FieldType::Set, FastIndexValue::String(_)) => {
            Err(ValidationError::Domain(ReplaceItemError::TypeMismatch {
                field: field.to_owned(),
                expected: FieldType::Set,
                got: "string (expected array of strings)",
            }))
        }
        (expected, got) => Err(ValidationError::Domain(ReplaceItemError::TypeMismatch {
            field: field.to_owned(),
            expected,
            got: fast_kind(got),
        })),
    }
}

pub(super) fn same_source(
    scanner: &FastIndexScanner<'_>,
    left: usize,
    right: usize,
    kind: FieldType,
    left_checksum: Option<u64>,
    right_checksum: Option<u64>,
    parsed: &ParsedValues,
) -> bool {
    if matches!(kind, FieldType::Text | FieldType::Vector) {
        return left_checksum.is_some() && left_checksum == right_checksum;
    }
    let a = scanner.items().nth(left).expect("planned ordinal");
    let b = scanner.items().nth(right).expect("planned ordinal");
    match (kind, a.value, b.value) {
        (FieldType::Keyword, FastIndexValue::String(a), FastIndexValue::String(b)) => a == b,
        (FieldType::Hash, FastIndexValue::String(_), FastIndexValue::String(_)) => {
            parsed.get(&left).and_then(|p| p.hash).is_some()
                && parsed.get(&left).and_then(|p| p.hash) == parsed.get(&right).and_then(|p| p.hash)
        }
        (FieldType::Number, FastIndexValue::Number(a), FastIndexValue::Number(b)) => {
            SortableF64::new(a).ok() == SortableF64::new(b).ok()
        }
        (FieldType::Set, FastIndexValue::StringList(a), FastIndexValue::StringList(b)) => {
            same_set(a, b)
        }
        _ => false,
    }
}

/// Match `apply_value`: duplicate set members have one logical live value.
/// Repeated lexical-min scans avoid materializing a giant source array.
fn set_bytes(
    values: crate::ingest::infrastructure::wal::fast_index_scanner::FastStringList<'_>,
    eid_len: usize,
) -> std::result::Result<u64, ValidationError> {
    let mut total = 0u64;
    let mut previous = None;
    while let Some(next) = next_distinct(values, previous) {
        let bytes = next
            .len()
            .checked_add(eid_len)
            .ok_or(ValidationError::Preparation(
                "Set member byte count overflow",
            ))?;
        total =
            total
                .checked_add(u64::try_from(bytes).map_err(|_| {
                    ValidationError::Preparation("Set member byte count exceeds u64")
                })?)
                .ok_or(ValidationError::Preparation("Set byte count overflow"))?;
        previous = Some(next);
    }
    Ok(total)
}

fn same_set(
    a: crate::ingest::infrastructure::wal::fast_index_scanner::FastStringList<'_>,
    b: crate::ingest::infrastructure::wal::fast_index_scanner::FastStringList<'_>,
) -> bool {
    let mut left = None;
    let mut right = None;
    loop {
        let next_left = next_distinct(a, left);
        let next_right = next_distinct(b, right);
        if next_left != next_right {
            return false;
        }
        let Some(value) = next_left else {
            return true;
        };
        left = Some(value);
        right = next_right;
    }
}

fn next_distinct<'a>(
    values: crate::ingest::infrastructure::wal::fast_index_scanner::FastStringList<'a>,
    previous: Option<&'a str>,
) -> Option<&'a str> {
    values
        .values()
        .filter(|value| previous.is_none_or(|old| *value > old))
        .min()
}

fn fast_kind(value: &FastIndexValue<'_>) -> &'static str {
    match value {
        FastIndexValue::String(_) => "string",
        FastIndexValue::Number(_) => "number",
        FastIndexValue::Vector { .. } => "f32[]",
        FastIndexValue::StringList(_) => "string[]",
    }
}
