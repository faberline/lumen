use std::fs;
use std::sync::Arc;

use crate::ingest::infrastructure::wal_source_stage::tests::{all_shapes, read_admission, stager};
use crate::ingest::infrastructure::wal_source_stage::MEM_WAL_STAGE_EPOCH;

#[test]
fn native_fast_stage_streams_and_pins_a_validated_payload() {
    let stager = stager();
    let record = all_shapes().remove(1);
    let staged = Arc::new(stager.stage_fast_index(17, &record).unwrap().unwrap());
    let path = staged.root_path().to_owned();
    let mapped = staged.mapped_fast_index().unwrap().unwrap();
    assert_eq!(mapped.payload(), record.encode().unwrap());
    let scanner = crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner::parse(
        mapped.payload(),
    )
    .unwrap();
    assert_eq!(scanner.collection_id(), "orders");
    drop(staged);
    assert!(
        path.exists(),
        "mapped owner pins the private stage directory"
    );
    drop(mapped);
    assert!(!path.exists());
}

#[test]
fn native_fast_stage_keeps_admitted_read_compatible_with_the_public_wire() {
    let stager = stager();
    let record = all_shapes().remove(1);
    let expected = record.encode().unwrap();
    let staged = stager.stage_fast_index(23, &record).unwrap().unwrap();
    assert!(staged.read(staged.decoded_owned_bytes()).is_err());
    let decoded = staged.read(read_admission(&staged)).unwrap();
    assert_eq!(decoded.encode().unwrap(), expected);
    assert_eq!(decoded.version, record.version);
}

#[test]
fn private_generic_stage_maps_only_its_cbor_body_and_pins_the_receipt() {
    let stager = stager();
    let mut record = all_shapes().remove(0);
    let mut expected = Vec::new();
    ciborium::ser::into_writer(&record, &mut expected).unwrap();
    let staged = Arc::new(stager.stage(29, &mut record).unwrap());
    let path = staged.root_path().to_owned();
    let mapped = staged.mapped_generic_cbor().unwrap().unwrap();
    assert_eq!(mapped.bytes(), expected);
    drop(staged);
    assert!(
        path.exists(),
        "mapped owner pins the private stage directory"
    );
    drop(mapped);
    assert!(!path.exists());
}

#[test]
fn private_generic_mapping_refuses_corrupt_envelope_or_payload_suffix() {
    for (name, mutate) in [
        (
            "header",
            Box::new(|bytes: &mut Vec<u8>| bytes[0] = b'X') as Box<dyn Fn(&mut Vec<u8>)>,
        ),
        ("version", Box::new(|bytes: &mut Vec<u8>| bytes[4] = 2)),
        ("cbor", Box::new(|bytes: &mut Vec<u8>| bytes.truncate(5))),
        ("suffix", Box::new(|bytes: &mut Vec<u8>| bytes.push(0))),
    ] {
        let stager = stager();
        let mut record = all_shapes().remove(0);
        let staged = Arc::new(stager.stage(30, &mut record).unwrap());
        let file = staged.root_path().join("records").join(format!(
            "{:020}-{:016x}.record",
            staged.sequence(),
            MEM_WAL_STAGE_EPOCH
        ));
        let mut bytes = fs::read(&file).unwrap();
        mutate(&mut bytes);
        fs::write(file, bytes).unwrap();
        assert!(staged.mapped_generic_cbor().is_err(), "{name}");
    }
}
