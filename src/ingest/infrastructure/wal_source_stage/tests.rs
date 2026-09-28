use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::sync::{Arc, Mutex};

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::committed_stage::{StageFailureInjector, StageFailurePoint};
use crate::ingest::infrastructure::wal_source_stage::{
    StagedWalRecord, WalSourceStager, MEM_WAL_STAGE_EPOCH,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, ReplaceDocItem,
    ReplaceDocsRequest,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};

fn stager() -> WalSourceStager {
    WalSourceStager::for_mem_wal().unwrap()
}

fn read_admission(staged: &StagedWalRecord) -> usize {
    staged
        .decoded_owned_bytes()
        .checked_add(staged.read_scratch_bytes())
        .unwrap()
}

fn field_spec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn all_shapes() -> Vec<WalRecord> {
    let mut fields = BTreeMap::new();
    fields.insert("keyword".into(), field_spec());
    vec![
        WalRecord::new(RaftLogEntry::CreateCollection {
            collection_id: "orders".into(),
            req: CreateCollectionRequest { fields },
        }),
        WalRecord::new(RaftLogEntry::Index {
            collection_id: "orders".into(),
            req: IndexRequest {
                request_id: Some("r".into()),
                items: vec![
                    IndexItem {
                        external_id: "a".into(),
                        field: "string".into(),
                        value: FieldValue::String("value".into()),
                        version: Some(3),
                    },
                    IndexItem {
                        external_id: "b".into(),
                        field: "number".into(),
                        value: FieldValue::Number(f64::from_bits(0x7ff8_0000_0000_0123)),
                        version: None,
                    },
                    IndexItem {
                        external_id: "c".into(),
                        field: "vector".into(),
                        value: FieldValue::Vector(vec![
                            f32::from_bits(0x7fc0_0123),
                            f32::INFINITY,
                            f32::NEG_INFINITY,
                        ]),
                        version: None,
                    },
                    IndexItem {
                        external_id: "d".into(),
                        field: "set".into(),
                        value: FieldValue::StringList(vec!["x".into(), "y".into()]),
                        version: None,
                    },
                ],
            },
        }),
        WalRecord::new(RaftLogEntry::ReplaceDocs {
            collection_id: "orders".into(),
            req: ReplaceDocsRequest {
                docs: vec![ReplaceDocItem {
                    external_id: "a".into(),
                    version: Some(5),
                    fields: BTreeMap::new(),
                }],
            },
        }),
        WalRecord::new(RaftLogEntry::TruncateDocs {
            collection_id: "orders".into(),
        }),
        WalRecord::new(RaftLogEntry::UnindexDocs {
            collection_id: "orders".into(),
            req: BatchUnindexDocsRequest {
                external_ids: vec!["a".into()],
            },
        }),
        WalRecord::new(RaftLogEntry::Delete {
            collection_id: "orders".into(),
            external_id: "a".into(),
            field: Some("keyword".into()),
        }),
        WalRecord::new(RaftLogEntry::DropCollection {
            collection_id: "orders".into(),
            force: true,
        }),
        WalRecord::new(RaftLogEntry::AddField {
            collection_id: "orders".into(),
            field_name: "keyword".into(),
            spec: field_spec(),
        }),
        WalRecord::new(RaftLogEntry::DropField {
            collection_id: "orders".into(),
            field_name: "keyword".into(),
        }),
    ]
}

#[test]
fn consumed_stage_reclaims_only_its_files_while_source_remains() {
    let stager = stager();
    let mut first = all_shapes().remove(0);
    let mut second = all_shapes().remove(1);
    let a = stager.stage(1, &mut first).unwrap();
    let b = stager.stage(2, &mut second).unwrap();
    let path_a = a.root_path().to_path_buf();
    let path_b = b.root_path().to_path_buf();
    drop(a);
    assert!(
        !path_a.exists(),
        "consumed source stage must release its files"
    );
    assert!(path_b.exists());
    assert!(b.read(read_admission(&b)).is_ok());
    let c = stager.stage(3, &mut first).unwrap();
    assert!(c.read(read_admission(&c)).is_ok());
}

#[test]
fn stages_every_wal_shape_with_exact_version_and_nonfinite_bits() {
    let stager = stager();
    for (index, mut original) in all_shapes().into_iter().enumerate() {
        let mut expected = Vec::new();
        ciborium::ser::into_writer(&original, &mut expected).unwrap();
        let staged = stager.stage(index as u64 + 1, &mut original).unwrap();
        let decoded = staged.read(read_admission(&staged)).unwrap();
        let mut actual = Vec::new();
        ciborium::ser::into_writer(&decoded, &mut actual).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(decoded.version, original.version);
    }
}

#[test]
fn clones_share_one_stable_process_unique_source_id() {
    let first = stager();
    let clone = first.clone();
    let second = stager();
    assert_eq!(first.source_id(), clone.source_id());
    assert_ne!(first.source_id(), second.source_id());
}

#[test]
fn staged_reader_keeps_private_root_after_stager_drops_then_last_drop_cleans() {
    let stager = stager();
    let mut record = all_shapes().remove(1);
    let staged = stager.stage(7, &mut record).unwrap();
    let path = staged.root_path().to_owned();
    assert!(path.is_dir());
    drop(stager);
    assert!(path.is_dir());
    assert!(staged.read(read_admission(&staged)).is_ok());
    drop(staged);
    assert!(!path.exists());
}

#[test]
fn corrupt_staged_payload_is_refused_before_decode() {
    let stager = stager();
    let mut record = all_shapes().remove(0);
    let staged = stager.stage(9, &mut record).unwrap();
    let file = staged.root_path().join("records").join(format!(
        "{:020}-{:016x}.record",
        staged.sequence(),
        MEM_WAL_STAGE_EPOCH
    ));
    fs::write(file, b"corrupt").unwrap();
    assert_eq!(
        staged.read(read_admission(&staged)).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn dropping_one_stage_removes_only_its_child_directory() {
    let stager = stager();
    let mut first = all_shapes().remove(0);
    let mut second = all_shapes().remove(1);
    let first = stager.stage(41, &mut first).unwrap();
    let second = stager.stage(42, &mut second).unwrap();
    let first_path = first.root_path().to_owned();
    let second_path = second.root_path().to_owned();
    assert!(first_path.is_dir() && second_path.is_dir());
    drop(first);
    assert!(!first_path.exists());
    assert!(second_path.is_dir());
    assert!(second.read(read_admission(&second)).is_ok());
    drop(second);
    assert!(!second_path.exists());
}

struct FailAt(Mutex<Option<StageFailurePoint>>);
impl StageFailureInjector for FailAt {
    fn check(&self, point: StageFailurePoint) -> io::Result<()> {
        let mut wanted = self.0.lock().unwrap();
        if *wanted == Some(point) {
            *wanted = None;
            return Err(io::Error::other("injected stage fault"));
        }
        Ok(())
    }
}

#[test]
fn every_durable_stage_fault_preserves_the_caller_record() {
    for point in [
        StageFailurePoint::SyncPayload,
        StageFailurePoint::RenamePayload,
        StageFailurePoint::SyncPayloadDirectory,
        StageFailurePoint::SyncMarker,
        StageFailurePoint::PublishMarker,
        StageFailurePoint::SyncMarkerDirectory,
    ] {
        let stager =
            WalSourceStager::for_mem_wal_with_injector(Arc::new(FailAt(Mutex::new(Some(point)))))
                .unwrap();
        let mut record = all_shapes().remove(1);
        let mut before = Vec::new();
        ciborium::ser::into_writer(&record, &mut before).unwrap();
        assert!(stager.stage(11, &mut record).is_err(), "{point:?}");
        let mut after = Vec::new();
        ciborium::ser::into_writer(&record, &mut after).unwrap();
        assert_eq!(after, before, "{point:?}");
    }
}

mod decode_bound;
mod mapped_payload;
