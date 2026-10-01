//! Malformed input: the error a rejected record produces stays within the
//! scanner workspace the preflight reserved.

use crate::ingest::infrastructure::wire_cost::allocation_tests::{
    assert_bounds, generic_cbor, generic_index, measure,
};

#[test]
fn malformed_large_variant_error_stays_within_reserved_scanner_workspace() {
    let variant = "bad-variant".repeat(200_000);
    let value = serde_json::json!({"version": 1, "entry": {variant: {}}});
    let mut cbor = Vec::new();
    ciborium::ser::into_writer(&value, &mut cbor).unwrap();
    for bytes in [cbor, serde_json::to_vec(&value).unwrap()] {
        let workspace =
            crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&bytes).unwrap();
        let (result, observed) =
            measure(|| crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&bytes));
        assert!(
            result.is_err(),
            "malformed unknown WAL variant must be refused"
        );
        assert!(
            observed.peak <= workspace,
            "error construction peak {} exceeded workspace {}",
            observed.peak,
            workspace
        );
        drop(result);
    }
}

#[test]
fn malformed_escaped_scalar_error_stays_within_reserved_scanner_workspace() {
    let value = serde_json::json!({"version": "\u{0}".repeat(400_000), "entry": {"DropCollection": {"collection_id":"c", "force":true}}});
    let bytes = serde_json::to_vec(&value).unwrap();
    let workspace = crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&bytes).unwrap();
    let (result, observed) =
        measure(|| crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&bytes));
    assert!(result.is_err(), "a string is not a WAL version");
    assert!(
        observed.peak <= workspace,
        "escaped error construction peak {} exceeded workspace {}",
        observed.peak,
        workspace
    );
    drop(result);
}

#[test]
fn malformed_enum_payloads_keep_diagnostics_bounded() {
    let large = "\u{0}".repeat(200_000);
    let values = [
        (
            serde_json::json!({"version":1,"entry":{"Index":large}}),
            true,
        ),
        (
            serde_json::json!({"version":1,"entry":{"AddField":{"collection_id":"c","field_name":"f","spec":{"type":{"keyword":large}}}}}),
            false,
        ),
    ];
    for (value, reject_cbor) in values {
        let mut cbor = Vec::new();
        ciborium::ser::into_writer(&value, &mut cbor).unwrap();
        // Ciborium's legacy unit_variant ignores its payload. Keep that
        // accepted input as a compatibility control, not a malformed oracle.
        if !reject_cbor {
            assert_bounds(&cbor);
        }
        let inputs = reject_cbor
            .then_some(cbor)
            .into_iter()
            .chain(std::iter::once(serde_json::to_vec(&value).unwrap()));
        for bytes in inputs {
            let workspace =
                crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&bytes).unwrap();
            let (result, observed) =
                measure(|| crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&bytes));
            assert!(result.is_err());
            assert!(
                observed.peak <= workspace,
                "enum diagnostic peak {} exceeded {}",
                observed.peak,
                workspace
            );
            drop(result);
        }
    }
}

#[test]
fn malformed_cbor_byte_value_keeps_diagnostics_bounded() {
    use ciborium::value::Value;
    fn replace(v: &mut Value) -> bool {
        match v {
            Value::Map(entries) => {
                for (key, value) in entries {
                    if key.as_text() == Some("value") {
                        *value = Value::Bytes(vec![255; 200_000]);
                        return true;
                    }
                    if replace(value) {
                        return true;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    if replace(value) {
                        return true;
                    }
                }
            }
            _ => (),
        }
        false
    }
    let mut value: Value =
        ciborium::de::from_reader(generic_cbor(&generic_index()).as_slice()).unwrap();
    assert!(replace(&mut value));
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&value, &mut bytes).unwrap();
    let workspace = crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&bytes).unwrap();
    let (result, observed) =
        measure(|| crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&bytes));
    assert!(result.is_err());
    assert!(
        observed.peak <= workspace,
        "byte diagnostic peak {} exceeded {}",
        observed.peak,
        workspace
    );
}
