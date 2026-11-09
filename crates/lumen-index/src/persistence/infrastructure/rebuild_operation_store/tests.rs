use std::path::PathBuf;

use tempfile::TempDir;

use super::*;
use crate::index::domain::rebuild_operation::{RebuildError, RebuildPhase};

#[test]
fn test_interrupt_at_every_phase() {
    let temp = TempDir::new().expect("tempdir");
    let base_path: PathBuf = temp.path().to_path_buf();

    // 1. Begin in Pending phase, save, drop, reopen
    {
        let store = RebuildOperationStore::new(&base_path);
        let op = store.begin("coll_a", "op_1").expect("begin");
        assert_eq!(op.phase, RebuildPhase::Pending);
    }
    {
        let store = RebuildOperationStore::new(&base_path);
        let mut op = store
            .load("coll_a")
            .expect("load")
            .expect("operation exists");
        assert_eq!(op.phase, RebuildPhase::Pending);
        assert_eq!(op.chunks_applied, 0);
        assert_eq!(op.docs_applied, 0);

        // Transition to Streaming, drop, reopen
        store.start_streaming(&mut op).expect("start streaming");
    }
    {
        let store = RebuildOperationStore::new(&base_path);
        let mut op = store
            .load("coll_a")
            .expect("load")
            .expect("operation exists");
        assert_eq!(op.phase, RebuildPhase::Streaming);

        // Apply chunks 0 and 1, drop, reopen
        store.accept_chunk(&mut op, 0, 10).expect("chunk 0");
        store.accept_chunk(&mut op, 1, 20).expect("chunk 1");
    }
    {
        let store = RebuildOperationStore::new(&base_path);
        let mut op = store
            .load("coll_a")
            .expect("load")
            .expect("operation exists");
        assert_eq!(op.phase, RebuildPhase::Streaming);
        assert_eq!(op.chunks_applied, 2);
        assert_eq!(op.docs_applied, 30);

        // Transition to Sealing, drop, reopen
        store.seal(&mut op).expect("seal");
    }
    {
        let store = RebuildOperationStore::new(&base_path);
        let mut op = store
            .load("coll_a")
            .expect("load")
            .expect("operation exists");
        assert_eq!(op.phase, RebuildPhase::Sealing);

        // Complete to Done, drop, reopen
        store.complete(&mut op).expect("complete");
    }
    {
        let store = RebuildOperationStore::new(&base_path);
        let op = store
            .load("coll_a")
            .expect("load")
            .expect("operation exists");
        assert_eq!(op.phase, RebuildPhase::Done);
        assert_eq!(op.chunks_applied, 2);
        assert_eq!(op.docs_applied, 30);
    }
}

#[test]
fn test_competing_begin_conflict_and_recovery() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    let mut op1 = store.begin("coll_b", "op_first").expect("begin first");
    assert_eq!(op1.phase, RebuildPhase::Pending);

    // Competing begin while Pending fails with Conflict
    let conflict_err = store
        .begin("coll_b", "op_competing")
        .expect_err("competing begin must be rejected");
    match conflict_err {
        RebuildStoreError::Conflict {
            collection,
            active_operation_id,
        } => {
            assert_eq!(collection, "coll_b");
            assert_eq!(active_operation_id, "op_first");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    // Advance op1 to Streaming; competing begin still rejected
    store.start_streaming(&mut op1).expect("streaming");
    assert!(matches!(
        store.begin("coll_b", "op_competing"),
        Err(RebuildStoreError::Conflict { .. })
    ));

    // Advance op1 to Sealing; competing begin still rejected
    store.seal(&mut op1).expect("seal");
    assert!(matches!(
        store.begin("coll_b", "op_competing"),
        Err(RebuildStoreError::Conflict { .. })
    ));

    // Finish op1 to Done; now competing begin succeeds
    store.complete(&mut op1).expect("complete");
    let op2 = store
        .begin("coll_b", "op_second")
        .expect("begin second succeeds after Done");
    assert_eq!(op2.operation_id, "op_second");
    assert_eq!(op2.phase, RebuildPhase::Pending);
}

#[test]
fn test_resume_applies_exactly_chunks_after_chunks_applied() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    let mut op = store.begin("coll_c", "op_resume").expect("begin");
    store.start_streaming(&mut op).expect("streaming");
    store.accept_chunk(&mut op, 0, 100).expect("chunk 0");
    store.accept_chunk(&mut op, 1, 200).expect("chunk 1");

    // Reopen store
    let store2 = RebuildOperationStore::new(temp.path());
    let mut resumed = store2
        .load("coll_c")
        .expect("load")
        .expect("operation exists");
    assert_eq!(resumed.chunks_applied, 2);

    // Stale chunks are refused
    assert!(matches!(
        store2.accept_chunk(&mut resumed, 0, 100),
        Err(RebuildStoreError::Operation(
            RebuildError::OutOfOrderChunk {
                expected: 2,
                actual: 0
            }
        ))
    ));
    assert!(matches!(
        store2.accept_chunk(&mut resumed, 1, 200),
        Err(RebuildStoreError::Operation(
            RebuildError::OutOfOrderChunk {
                expected: 2,
                actual: 1
            }
        ))
    ));

    // Exactly the next chunk (seq 2) succeeds
    store2.accept_chunk(&mut resumed, 2, 300).expect("chunk 2");
    assert_eq!(resumed.chunks_applied, 3);
    assert_eq!(resumed.docs_applied, 600);
}

/// Reopens the store and asserts the cancel is durable, not just in memory.
fn assert_durably_cancelled(base: &std::path::Path, collection: &str) {
    let reopened = RebuildOperationStore::new(base)
        .load(collection)
        .expect("load after cancel")
        .expect("cancelled record exists");
    assert_eq!(reopened.phase, RebuildPhase::Cancelled);
    assert!(reopened.writes_to_active_only);
}

#[test]
fn test_cancel_in_each_phase_allows_later_begin() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    // 1. Cancel in Pending
    let mut op1 = store.begin("coll_d", "op_cancel_pending").expect("begin 1");
    store.cancel(&mut op1).expect("cancel in pending");
    assert_eq!(op1.phase, RebuildPhase::Cancelled);
    assert!(op1.writes_to_active_only);
    assert_durably_cancelled(temp.path(), "coll_d");

    // Later begin succeeds
    let mut op2 = store
        .begin("coll_d", "op_cancel_streaming")
        .expect("begin 2");
    store.start_streaming(&mut op2).expect("streaming");
    store.cancel(&mut op2).expect("cancel in streaming");
    assert_eq!(op2.phase, RebuildPhase::Cancelled);
    assert_durably_cancelled(temp.path(), "coll_d");

    // Later begin succeeds
    let mut op3 = store.begin("coll_d", "op_cancel_sealing").expect("begin 3");
    store.start_streaming(&mut op3).expect("streaming");
    store.seal(&mut op3).expect("seal");
    store.cancel(&mut op3).expect("cancel in sealing");
    assert_eq!(op3.phase, RebuildPhase::Cancelled);
    assert_durably_cancelled(temp.path(), "coll_d");

    // Later begin succeeds
    let mut op4 = store.begin("coll_d", "op_done").expect("begin 4");
    store.start_streaming(&mut op4).expect("streaming");
    store.seal(&mut op4).expect("seal");
    store.complete(&mut op4).expect("done");
    assert_eq!(op4.phase, RebuildPhase::Done);

    let op5 = store.begin("coll_d", "op_final").expect("begin 5");
    assert_eq!(op5.phase, RebuildPhase::Pending);
}

#[test]
fn test_fail_in_non_terminal_allows_later_begin() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    let mut op = store.begin("coll_e", "op_fail").expect("begin");
    store.start_streaming(&mut op).expect("streaming");
    store.fail(&mut op, "unrecoverable I/O").expect("fail");
    assert_eq!(op.phase, RebuildPhase::Failed);

    let next_op = store
        .begin("coll_e", "op_after_fail")
        .expect("begin after fail");
    assert_eq!(next_op.phase, RebuildPhase::Pending);
}

#[test]
fn test_corrupt_or_truncated_record_fails_closed() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    // Create a corrupted record file directly
    let path = store.record_path("coll_corrupt");
    std::fs::write(
        &path,
        b"{\"collection\": \"coll_corrupt\", \"phase\": \"pend",
    )
    .expect("write corrupt");

    // Load must fail closed
    let load_err = store
        .load("coll_corrupt")
        .expect_err("corrupt file must fail");
    assert!(matches!(load_err, RebuildStoreError::CorruptRecord { .. }));
    let load_text = load_err.to_string();
    assert!(
        !load_text.contains("pend"),
        "corrupt-record error must not quote record bytes: {load_text}"
    );

    // Begin must also fail closed without overwriting or returning empty/Done
    let begin_err = store
        .begin("coll_corrupt", "op_new")
        .expect_err("begin on corrupt must fail");
    assert!(matches!(begin_err, RebuildStoreError::CorruptRecord { .. }));

    // Empty file also fails closed
    std::fs::write(&path, b"").expect("write empty");
    let load_empty_err = store
        .load("coll_corrupt")
        .expect_err("empty file must fail");
    assert!(matches!(
        load_empty_err,
        RebuildStoreError::CorruptRecord { .. }
    ));
}

#[test]
fn test_no_secret_or_document_content_in_errors() {
    let err1 = RebuildStoreError::Conflict {
        collection: "test_coll".to_string(),
        active_operation_id: "op_active_9".to_string(),
    };
    assert_eq!(
        err1.to_string(),
        "competing rebuild operation for collection 'test_coll' is active: op_active_9"
    );

    let err2 = RebuildStoreError::Stale {
        collection: "test_coll".to_string(),
        operation_id: "op_gone".to_string(),
    };
    assert_eq!(
        err2.to_string(),
        "rebuild operation 'op_gone' for collection 'test_coll' has no record and is no longer active"
    );

    let err3 = RebuildStoreError::CorruptRecord {
        collection: "test_coll".to_string(),
        error: "Eof at line 1 column 42".to_string(),
    };
    assert_eq!(
        err3.to_string(),
        "corrupt or invalid rebuild record for collection 'test_coll': Eof at line 1 column 42"
    );

    let err4 = RebuildError::OutOfOrderChunk {
        expected: 5,
        actual: 2,
    };
    assert_eq!(
        err4.to_string(),
        "out-of-order chunk: expected sequence 5, got 2"
    );

    let err5 = RebuildError::IllegalTransition {
        from: RebuildPhase::Pending,
        to: RebuildPhase::Done,
    };
    assert_eq!(
        err5.to_string(),
        "illegal rebuild transition from Pending to Done"
    );
}

#[test]
fn failed_save_leaves_the_handle_unchanged_and_the_same_seq_retryable() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());
    let mut op = store.begin("coll_f", "op_f").expect("begin");
    store.start_streaming(&mut op).expect("streaming");

    // Make the next save fail: the record file is replaced by a directory.
    let path = store.record_path("coll_f");
    let saved = std::fs::read(&path).expect("read record");
    std::fs::remove_file(&path).expect("remove record");
    std::fs::create_dir(&path).expect("create blocking dir");
    assert!(store.accept_chunk(&mut op, 0, 10).is_err());
    assert_eq!(
        op.chunks_applied, 0,
        "a failed save must not advance the handle"
    );
    assert_eq!(op.docs_applied, 0);

    // Cause removed: the same seq is accepted, not refused as a duplicate.
    std::fs::remove_dir(&path).expect("remove blocking dir");
    std::fs::write(&path, &saved).expect("restore record");
    store
        .accept_chunk(&mut op, 0, 10)
        .expect("seq 0 accepted after repair");
    assert_eq!(op.chunks_applied, 1);
    assert_eq!(op.docs_applied, 10);
    assert_eq!(store.load("coll_f").unwrap().unwrap().chunks_applied, 1);
}

#[test]
fn stale_handle_cannot_overwrite_a_newer_operation() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());
    let mut op_a = store.begin("coll_s", "op_a").expect("begin a");
    store.cancel(&mut op_a).expect("cancel a");
    // A copy of op_a taken before op_b begins.
    let mut stale_a = op_a.clone();
    let op_b = store.begin("coll_s", "op_b").expect("begin b");
    assert_eq!(op_b.phase, RebuildPhase::Pending);

    match store.cancel(&mut stale_a) {
        Err(RebuildStoreError::Conflict {
            active_operation_id,
            ..
        }) => assert_eq!(active_operation_id, "op_b"),
        other => panic!("stale handle must get Conflict, got {other:?}"),
    }
    let on_disk = store.load("coll_s").unwrap().unwrap();
    assert_eq!(on_disk.operation_id, "op_b");
    assert_eq!(on_disk.phase, RebuildPhase::Pending);
}

#[test]
fn concurrent_begin_admits_exactly_one_operation() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let store = store.clone();
            std::thread::spawn(move || store.begin("coll_race", &format!("op_{i}")).is_ok())
        })
        .collect();
    let admitted = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|ok| *ok)
        .count();
    assert_eq!(admitted, 1, "exactly one competing begin may succeed");
}

#[test]
fn collection_names_cannot_escape_the_record_directory() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path().join("rebuild");
    let store = RebuildOperationStore::new(&base);
    for name in ["../escape", "a/b", "..", "/abs"] {
        assert_eq!(
            store.record_path(name).parent(),
            Some(base.as_path()),
            "record for {name:?} must stay in base_dir"
        );
    }
    let op = store.begin("../escape", "op_e").expect("begin");
    assert_eq!(store.load("../escape").unwrap().unwrap(), op);
    assert!(!temp.path().join("escape.rebuild.json").exists());
}
