use std::sync::{Arc, Mutex};

use crate::index::application::engine::Engine;
use crate::ingest::application::write_coordinator::errors::{RestartRequired, StorageFullError};
use crate::ingest::application::write_coordinator::tests::keyword_schema;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

/// The embedded segment path may be replaced immediately after its write
/// endpoint returns. Its AOF must therefore be readable *at acknowledgement
/// time*, rather than waiting for writer drop.
/// This is the exact local half of the single-replica pod-restart contract:
/// fresh engine -> replay local AOF -> collection and indexed document.
#[tokio::test]
async fn embedded_aof_is_replayable_when_submit_acknowledges_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open_with_policy(
            &aof_path,
            crate::persistence::infrastructure::aof::FsyncPolicy::EverySec,
        )
        .unwrap(),
    ));
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start_from_with_aof(wal, engine, 0, aof);

    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "u1".into(),
                    field: "email".into(),
                    value: FieldValue::String("u1@example.test".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap();

    // Do not flush or sync the test's writer: a returned write itself is
    // the contract boundary. Before the fix this reader saw an empty AOF
    // because both frames remained in the process-local BufWriter.
    let mut seqs = Vec::new();
    crate::persistence::infrastructure::aof::replay::AofReader::replay(&aof_path, 0, |seq, _| {
        seqs.push(seq)
    })
    .unwrap();
    assert_eq!(seqs, vec![1, 2]);

    let restarted = Arc::new(Engine::new());
    assert_eq!(
        crate::persistence::infrastructure::aof::replay::replay_aof_into(&restarted, &aof_path, 0)
            .unwrap(),
        2
    );
    assert_eq!(restarted.stats("u").unwrap().documents_indexed, 1);
}

#[tokio::test]
async fn partial_error_record_is_persisted_and_replays_its_earlier_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(&aof_path).unwrap(),
    ));
    let engine = Arc::new(Engine::new());
    let coord =
        WriteCoordinator::start_from_with_aof(Arc::new(MemWal::new()), engine.clone(), 0, aof);
    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    let error = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: "u1".into(),
                        field: "email".into(),
                        value: FieldValue::String("ok".into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: "u2".into(),
                        field: "missing".into(),
                        value: FieldValue::String("bad".into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        })
        .await
        .unwrap_err();
    assert!(error
        .downcast_ref::<crate::index::domain::storage_error::StorageError>()
        .is_some());
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
    let mut seqs = Vec::new();
    crate::persistence::infrastructure::aof::replay::AofReader::replay(&aof_path, 0, |seq, _| {
        seqs.push(seq)
    })
    .unwrap();
    assert_eq!(seqs, vec![1, 2]);
    let restarted = Arc::new(Engine::new());
    assert_eq!(
        crate::persistence::infrastructure::aof::replay::replay_aof_into(&restarted, &aof_path, 0)
            .unwrap(),
        2
    );
    assert_eq!(restarted.stats("u").unwrap().documents_indexed, 1);
}

#[tokio::test]
async fn partial_error_aof_gap_is_uncertain_and_blocks_later_append() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(&aof_path).unwrap(),
    ));
    let engine = Arc::new(Engine::new());
    let coord = WriteCoordinator::start_from_with_aof(
        Arc::new(MemWal::new()),
        engine.clone(),
        0,
        aof.clone(),
    );
    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    aof.lock().unwrap().set_inject_storage_full(true);
    let error = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: "u1".into(),
                        field: "email".into(),
                        value: FieldValue::String("kept-live".into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: "u2".into(),
                        field: "missing".into(),
                        value: FieldValue::String("invalid".into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        })
        .await
        .unwrap_err();
    assert!(
        error.downcast_ref::<StorageFullError>().is_some(),
        "{error}"
    );
    // The AOF failure occurs before preparation and apply.
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 0);
    assert!(coord.is_restart_required());
    aof.lock().unwrap().set_inject_storage_full(false);

    let later = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "u3".into(),
                    field: "email".into(),
                    value: FieldValue::String("must-not-append".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap_err();
    assert!(later.downcast_ref::<RestartRequired>().is_some(), "{later}");
    let mut persisted = Vec::new();
    crate::persistence::infrastructure::aof::replay::AofReader::replay(&aof_path, 0, |seq, _| {
        persisted.push(seq)
    })
    .unwrap();
    assert_eq!(persisted, vec![1]);
}

/// #2516: prove the REAL ENOSPC detection/classification/metrics path
/// end to end, through the actual production write path — not a
/// parallel fake. Uses `AofWriter::set_inject_storage_full` (the
/// `#[cfg(test)]` fault-injection seam on the real `AofWriter::append`,
/// scoped to this test's own writer instance so parallel test threads
/// never cross-contaminate) so the apply loop's genuine
/// AOF-persist-failure branch runs.
#[tokio::test]
async fn aof_enospc_marks_degraded_and_requires_restart() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(&aof_path).unwrap(),
    ));
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start_from_with_aof(wal, engine.clone(), 0, aof.clone());

    // A normal write before the disk fills must succeed and must not
    // touch the degraded flag.
    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    assert!(!engine.metrics().is_storage_degraded());

    // Arm the fault injection: the next AofWriter::append hits a
    // synthetic ENOSPC, exercising the real coordinator apply-loop
    // branch that classifies it and flips the sticky flag.
    aof.lock().unwrap().set_inject_storage_full(true);
    let err = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "u1".into(),
                    field: "email".into(),
                    value: FieldValue::String("a@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap_err();
    aof.lock().unwrap().set_inject_storage_full(false);

    assert!(
        err.downcast_ref::<StorageFullError>().is_some(),
        "expected StorageFullError, got: {err}"
    );
    assert!(
        engine.metrics().is_storage_degraded(),
        "ENOSPC on the AOF write path must flip the sticky degraded gauge"
    );
    assert_eq!(engine.metrics().storage_full_errors_total.get(), 1);
    assert!(
        coord.is_restart_required(),
        "an applied mutation with no AOF record cannot be repaired in-process"
    );

    // A successful disk-space probe may clear the ENOSPC gauge. It must not
    // clear the independent durability-gap latch.
    engine.metrics().clear_storage_degraded();
    let error = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "u2".into(),
                    field: "email".into(),
                    value: FieldValue::String("b@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .expect_err("a process with an AOF gap must reject later mutations");
    assert!(error.downcast_ref::<RestartRequired>().is_some(), "{error}");
    assert!(!engine.metrics().is_storage_degraded());
    assert!(coord.is_restart_required());
}

#[tokio::test]
async fn aof_gap_rejects_every_later_applied_record() {
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let mut writer =
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(&aof_path).unwrap();
    writer.inject_failure_once(std::io::ErrorKind::Other);
    let aof = Arc::new(Mutex::new(writer));

    let wal = Arc::new(MemWal::new());
    for collection_id in ["first", "second"] {
        wal.publish(WalRecord::new(RaftLogEntry::CreateCollection {
            collection_id: collection_id.into(),
            req: keyword_schema(),
        }))
        .await
        .unwrap();
    }

    let engine = Arc::new(Engine::new());
    let coord = WriteCoordinator::start_from_with_aof(wal, engine.clone(), 0, aof);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !coord.is_restart_required() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the unresolved AOF head must require restart");

    assert!(coord.is_restart_required());
    assert_eq!(coord.applied_seq(), 0);
    // The unresolved AOF head is rejected before apply.
    assert!(engine.list_collections().unwrap().is_empty());
    let mut persisted = Vec::new();
    crate::persistence::infrastructure::aof::replay::AofReader::replay(&aof_path, 0, |seq, _| {
        persisted.push(seq)
    })
    .unwrap();
    assert!(
        persisted.is_empty(),
        "the unresolved head and every later record must stay outside the AOF"
    );
}
