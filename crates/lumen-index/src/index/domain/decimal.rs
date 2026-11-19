//! The exact decimal key: a canonical decimal JSON string as a scaled `i128`,
//! with no rounding and no float.

use thiserror::Error;

use crate::shared_kernel::types::document::FieldValue;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecimalError {
    #[error("decimal value is empty")]
    Empty,
    #[error("decimal value `{0}` is not a plain decimal (optional `-`, digits, optional `.` and digits; no sign `+`, exponent or spaces)")]
    Malformed(String),
    #[error("decimal value `{value}` has more than {scale} fraction digits")]
    TooManyFractionDigits { value: String, scale: u8 },
    #[error("decimal value `{value}` has more than {precision} digits")]
    PrecisionOverflow { value: String, precision: u8 },
    #[error("decimal value must be a JSON string, got {0}")]
    NotAString(&'static str),
}

/// `text` is `-?DIGITS(.DIGITS)?`. Returns `text * 10^scale` as an exact integer.
pub fn parse_decimal(text: &str, precision: u8, scale: u8) -> Result<i128, DecimalError> {
    if text.is_empty() {
        return Err(DecimalError::Empty);
    }
    if precision > 38 {
        return Err(DecimalError::PrecisionOverflow {
            value: text.to_string(),
            precision,
        });
    }
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int_digits, frac_digits) = match unsigned.split_once('.') {
        Some((int, frac)) => (int, Some(frac)),
        None => (unsigned, None),
    };
    let is_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !is_digits(int_digits) || frac_digits.is_some_and(|f| !is_digits(f)) {
        return Err(DecimalError::Malformed(text.to_string()));
    }
    let frac_digits = frac_digits.unwrap_or("");
    if frac_digits.len() > usize::from(scale) {
        return Err(DecimalError::TooManyFractionDigits {
            value: text.to_string(),
            scale,
        });
    }
    let significant_int = int_digits.trim_start_matches('0');
    let int_len = if significant_int.is_empty() {
        1
    } else {
        significant_int.len()
    };
    if int_len + usize::from(scale) > usize::from(precision) {
        return Err(DecimalError::PrecisionOverflow {
            value: text.to_string(),
            precision,
        });
    }
    let mut digits = String::with_capacity(int_digits.len() + usize::from(scale));
    digits.push_str(int_digits);
    digits.push_str(frac_digits);
    for _ in frac_digits.len()..usize::from(scale) {
        digits.push('0');
    }
    let magnitude: i128 = digits
        .parse()
        .map_err(|_| DecimalError::PrecisionOverflow {
            value: text.to_string(),
            precision,
        })?;
    Ok(if negative { -magnitude } else { magnitude })
}

/// The wire value of a decimal field: only `FieldValue::String` is accepted.
pub fn parse_decimal_value(
    value: &FieldValue,
    precision: u8,
    scale: u8,
) -> Result<i128, DecimalError> {
    match value {
        FieldValue::String(text) => parse_decimal(text, precision, scale),
        FieldValue::Number(_) => Err(DecimalError::NotAString("number")),
        FieldValue::Vector(_) => Err(DecimalError::NotAString("vector")),
        FieldValue::StringList(_) => Err(DecimalError::NotAString("string list")),
        FieldValue::Int64(_) => Err(DecimalError::NotAString("integer")),
        FieldValue::Bool(_) => Err(DecimalError::NotAString("boolean")),
    }
}

#[cfg(test)]
mod tests;
