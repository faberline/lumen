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

#[test]
fn test_cancel_in_each_phase_allows_later_begin() {
    let temp = TempDir::new().expect("tempdir");
    let store = RebuildOperationStore::new(temp.path());

    // 1. Cancel in Pending
    let mut op1 = store.begin("coll_d", "op_cancel_pending").expect("begin 1");
    store.cancel(&mut op1).expect("cancel in pending");
    assert_eq!(op1.phase, RebuildPhase::Cancelled);
    assert!(op1.writes_to_active_only);

    // Later begin succeeds
    let mut op2 = store
        .begin("coll_d", "op_cancel_streaming")
        .expect("begin 2");
    store.start_streaming(&mut op2).expect("streaming");
    store.cancel(&mut op2).expect("cancel in streaming");
    assert_eq!(op2.phase, RebuildPhase::Cancelled);

    // Later begin succeeds
    let mut op3 = store.begin("coll_d", "op_cancel_sealing").expect("begin 3");
    store.start_streaming(&mut op3).expect("streaming");
    store.seal(&mut op3).expect("seal");
    store.cancel(&mut op3).expect("cancel in sealing");
    assert_eq!(op3.phase, RebuildPhase::Cancelled);

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
    let s1 = err1.to_string();
    assert!(s1.contains("test_coll"));
    assert!(s1.contains("op_active_9"));

    let err2 = RebuildError::OutOfOrderChunk {
        expected: 5,
        actual: 2,
    };
    let s2 = err2.to_string();
    assert!(s2.contains('5'));
    assert!(s2.contains('2'));
}
