use super::*;

fn nines(n: usize) -> String {
    "9".repeat(n)
}

#[test]
fn parses_negative_and_positive_scaled() {
    assert_eq!(parse_decimal("-1.5", 10, 2), Ok(-150));
    assert_eq!(parse_decimal("1.25", 10, 2), Ok(125));
    let negative = parse_decimal("-1.5", 10, 2).unwrap();
    let positive = parse_decimal("1.25", 10, 2).unwrap();
    assert!(negative < positive);
}

#[test]
fn integer_and_short_fraction_are_padded() {
    assert_eq!(parse_decimal("7", 10, 2), Ok(700));
    assert_eq!(parse_decimal("7.5", 10, 2), Ok(750));
    assert_eq!(parse_decimal("0.05", 10, 2), Ok(5));
    assert_eq!(parse_decimal("-0", 10, 2), Ok(0));
    assert_eq!(parse_decimal("-0.00", 10, 2), Ok(0));
    assert_eq!(parse_decimal("007", 10, 2), Ok(700));
}

#[test]
fn scale_zero_accepts_integers_only() {
    assert_eq!(parse_decimal("42", 5, 0), Ok(42));
    assert!(matches!(
        parse_decimal("42.0", 5, 0),
        Err(DecimalError::TooManyFractionDigits { .. })
    ));
}

#[test]
fn more_fraction_digits_than_scale_is_refused() {
    assert!(matches!(
        parse_decimal("1.234", 10, 2),
        Err(DecimalError::TooManyFractionDigits { .. })
    ));
    assert!(matches!(
        parse_decimal("1.250", 10, 2),
        Err(DecimalError::TooManyFractionDigits { .. })
    ));
}

#[test]
fn exponent_form_is_refused() {
    assert!(matches!(
        parse_decimal("1e3", 10, 2),
        Err(DecimalError::Malformed(_))
    ));
    assert!(matches!(
        parse_decimal("1E3", 10, 2),
        Err(DecimalError::Malformed(_))
    ));
    assert!(matches!(
        parse_decimal("1.5e2", 10, 2),
        Err(DecimalError::Malformed(_))
    ));
}

#[test]
fn other_malformed_inputs_are_refused() {
    for input in ["+1", " 1", "1 ", ".5", "5.", "-", "1.2.3", "1_000", "abc"] {
        assert!(
            matches!(parse_decimal(input, 10, 2), Err(DecimalError::Malformed(_))),
            "input {input:?} must be Malformed"
        );
    }
}

#[test]
fn empty_string_is_refused() {
    assert_eq!(parse_decimal("", 10, 2), Err(DecimalError::Empty));
}

#[test]
fn precision_overflow_is_refused() {
    assert!(matches!(
        parse_decimal("1234.5", 5, 2),
        Err(DecimalError::PrecisionOverflow { .. })
    ));
    assert_eq!(parse_decimal("123.45", 5, 2), Ok(12345));
    assert_eq!(parse_decimal("99999.99", 7, 2), Ok(9_999_999));
    assert!(matches!(
        parse_decimal("100000.00", 7, 2),
        Err(DecimalError::PrecisionOverflow { .. })
    ));
}

#[test]
fn thirty_eight_digit_value_is_exact() {
    let max38: i128 = 10i128.pow(38) - 1;
    assert_eq!(parse_decimal(&nines(38), 38, 0), Ok(max38));
    assert_eq!(parse_decimal(&format!("-{}", nines(38)), 38, 0), Ok(-max38));
    assert_eq!(
        parse_decimal(&format!("{}.99", nines(36)), 38, 2),
        Ok(max38)
    );
    assert!(matches!(
        parse_decimal(&nines(39), 38, 0),
        Err(DecimalError::PrecisionOverflow { .. })
    ));
}

#[test]
fn precision_above_38_is_refused() {
    assert!(matches!(
        parse_decimal("1", 39, 0),
        Err(DecimalError::PrecisionOverflow { .. })
    ));
}

#[test]
fn a_json_number_is_refused() {
    assert_eq!(
        parse_decimal_value(&FieldValue::Number(1.5), 10, 2),
        Err(DecimalError::NotAString("number"))
    );
}

#[test]
fn a_string_value_parses() {
    assert_eq!(
        parse_decimal_value(&FieldValue::String("1.25".into()), 10, 2),
        Ok(125)
    );
}

#[test]
fn a_string_list_is_refused() {
    assert_eq!(
        parse_decimal_value(&FieldValue::StringList(vec!["1".into()]), 10, 2),
        Err(DecimalError::NotAString("string list"))
    );
}

#[test]
fn a_vector_is_refused() {
    assert_eq!(
        parse_decimal_value(&FieldValue::Vector(vec![1.0]), 10, 2),
        Err(DecimalError::NotAString("vector"))
    );
}

#[test]
fn scaled_integers_keep_decimal_order() {
    let inputs = ["-10.00", "-1.50", "0.00", "1.25", "10.00"];
    let keys: Vec<i128> = inputs
        .iter()
        .map(|s| parse_decimal(s, 10, 2).unwrap())
        .collect();
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "not ascending: {keys:?}"
    );
}
