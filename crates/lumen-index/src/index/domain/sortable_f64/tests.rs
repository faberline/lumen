use crate::index::domain::sortable_f64::SortableF64;

#[test]
fn sortable_f64_round_trip_and_order() {
    let xs = [
        -f64::INFINITY,
        -1e10,
        -1.0,
        -0.0,
        0.0,
        1.0,
        1e10,
        f64::INFINITY,
    ];
    let mut keys: Vec<SortableF64> = xs.iter().map(|x| SortableF64::new(*x).unwrap()).collect();
    let original = keys.clone();
    keys.sort();
    assert_eq!(keys, original);
    for x in xs {
        assert_eq!(SortableF64::new(x).unwrap().to_f64(), x);
    }
}

#[test]
fn sortable_f64_rejects_nan() {
    assert!(SortableF64::new(f64::NAN).is_err());
}

#[test]
fn for_value_rejects_infinity_and_nan_but_new_keeps_infinity() {
    assert!(SortableF64::for_value(f64::INFINITY).is_err());
    assert!(SortableF64::for_value(f64::NEG_INFINITY).is_err());
    assert!(SortableF64::for_value(f64::NAN).is_err());
    assert!(SortableF64::new(f64::INFINITY).is_ok());
    assert!(SortableF64::for_value(f64::MAX).is_ok());
    assert!(SortableF64::for_value(-0.0).is_ok());
}
