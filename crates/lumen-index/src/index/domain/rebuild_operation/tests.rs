use super::*;

#[test]
fn test_phase_transitions_happy_path() {
    let mut op = RebuildOperation::new("coll_1", "op_100");
    assert_eq!(op.phase, RebuildPhase::Pending);
    assert_eq!(op.chunks_applied, 0);
    assert_eq!(op.docs_applied, 0);
    assert!(!op.writes_to_active_only);

    op.start_streaming().expect("start streaming");
    assert_eq!(op.phase, RebuildPhase::Streaming);

    op.accept_chunk(0, 10).expect("chunk 0");
    assert_eq!(op.chunks_applied, 1);
    assert_eq!(op.docs_applied, 10);

    op.seal().expect("seal");
    assert_eq!(op.phase, RebuildPhase::Sealing);

    op.complete().expect("complete");
    assert_eq!(op.phase, RebuildPhase::Done);
    assert!(op.phase.is_terminal());
}

#[test]
fn test_illegal_transitions_preserve_state() {
    let mut op = RebuildOperation::new("coll_1", "op_100");

    // Cannot seal or complete from Pending
    assert!(matches!(
        op.seal(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Pending,
            to: RebuildPhase::Sealing
        })
    ));
    assert_eq!(op.phase, RebuildPhase::Pending);

    assert!(matches!(
        op.complete(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Pending,
            to: RebuildPhase::Done
        })
    ));
    assert_eq!(op.phase, RebuildPhase::Pending);

    // Transition to Streaming
    op.start_streaming().expect("streaming");

    // Cannot start_streaming again
    assert!(matches!(
        op.start_streaming(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Streaming,
            to: RebuildPhase::Streaming
        })
    ));
    assert_eq!(op.phase, RebuildPhase::Streaming);

    // Cannot complete directly from Streaming without sealing
    assert!(matches!(
        op.complete(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Streaming,
            to: RebuildPhase::Done
        })
    ));
    assert_eq!(op.phase, RebuildPhase::Streaming);

    // Seal
    op.seal().expect("seal");

    // Cannot accept chunks while sealing
    assert!(matches!(
        op.accept_chunk(0, 5),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Sealing,
            to: RebuildPhase::Streaming
        })
    ));

    // Complete
    op.complete().expect("complete");

    // Cannot transition from terminal Done
    assert!(matches!(
        op.start_streaming(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Done,
            to: RebuildPhase::Streaming
        })
    ));
    assert!(matches!(
        op.seal(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Done,
            to: RebuildPhase::Sealing
        })
    ));
    assert!(matches!(
        op.cancel(),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Done,
            to: RebuildPhase::Cancelled
        })
    ));
    assert!(matches!(
        op.fail("err"),
        Err(RebuildError::IllegalTransition {
            from: RebuildPhase::Done,
            to: RebuildPhase::Failed
        })
    ));
}

#[test]
fn test_ordered_chunks_and_progress() {
    let mut op = RebuildOperation::new("coll_1", "op_100");
    op.start_streaming().expect("streaming");

    // Accepting in order succeeds and updates progress
    op.accept_chunk(0, 42).expect("chunk 0");
    assert_eq!(op.chunks_applied, 1);
    assert_eq!(op.docs_applied, 42);

    op.accept_chunk(1, 58).expect("chunk 1");
    assert_eq!(op.chunks_applied, 2);
    assert_eq!(op.docs_applied, 100);

    // Gap: seq 3 instead of 2
    let err = op.accept_chunk(3, 10).expect_err("gap must fail");
    assert_eq!(
        err,
        RebuildError::OutOfOrderChunk {
            expected: 2,
            actual: 3
        }
    );
    assert_eq!(op.chunks_applied, 2);
    assert_eq!(op.docs_applied, 100);

    // Duplicate: seq 1 instead of 2
    let err = op.accept_chunk(1, 10).expect_err("duplicate must fail");
    assert_eq!(
        err,
        RebuildError::OutOfOrderChunk {
            expected: 2,
            actual: 1
        }
    );
    assert_eq!(op.chunks_applied, 2);
    assert_eq!(op.docs_applied, 100);

    // Stale: seq 0 instead of 2
    let err = op.accept_chunk(0, 10).expect_err("stale must fail");
    assert_eq!(
        err,
        RebuildError::OutOfOrderChunk {
            expected: 2,
            actual: 0
        }
    );
    assert_eq!(op.chunks_applied, 2);
    assert_eq!(op.docs_applied, 100);

    // Exactly next seq 2 succeeds
    op.accept_chunk(2, 50).expect("chunk 2");
    assert_eq!(op.chunks_applied, 3);
    assert_eq!(op.docs_applied, 150);
}

#[test]
fn test_negative_control_chunk_ordering() {
    // Negative control: out-of-order and duplicate chunks MUST be rejected.
    let mut op = RebuildOperation::new("coll_1", "op_100");
    op.start_streaming().expect("streaming");

    // Calling chunk 1 when chunks_applied is 0 must fail
    assert!(op.accept_chunk(1, 10).is_err());
    assert_eq!(op.chunks_applied, 0);

    // After chunk 0 is applied, chunk 0 must fail (duplicate)
    op.accept_chunk(0, 10).expect("chunk 0");
    assert!(op.accept_chunk(0, 10).is_err());
    assert_eq!(op.chunks_applied, 1);
}

#[test]
fn test_cancel_idempotent_and_active_only() {
    // Cancel from Pending
    let mut op = RebuildOperation::new("coll_1", "op_1");
    op.cancel().expect("cancel from pending");
    assert_eq!(op.phase, RebuildPhase::Cancelled);
    assert!(op.writes_to_active_only);

    // Idempotent second cancel
    op.cancel().expect("second cancel is idempotent");
    assert_eq!(op.phase, RebuildPhase::Cancelled);
    assert!(op.writes_to_active_only);

    // Cancel from Streaming
    let mut op = RebuildOperation::new("coll_1", "op_2");
    op.start_streaming().expect("streaming");
    op.cancel().expect("cancel from streaming");
    assert_eq!(op.phase, RebuildPhase::Cancelled);
    assert!(op.writes_to_active_only);

    // Cancel from Sealing
    let mut op = RebuildOperation::new("coll_1", "op_3");
    op.start_streaming().expect("streaming");
    op.seal().expect("sealing");
    op.cancel().expect("cancel from sealing");
    assert_eq!(op.phase, RebuildPhase::Cancelled);
    assert!(op.writes_to_active_only);
}

#[test]
fn test_fail_transitions() {
    let mut op = RebuildOperation::new("coll_1", "op_1");
    op.start_streaming().expect("streaming");
    op.fail("disk error").expect("fail");
    assert_eq!(op.phase, RebuildPhase::Failed);
    assert_eq!(op.failure_reason.as_deref(), Some("disk error"));
    assert!(op.phase.is_terminal());

    // Cannot cancel after failing
    assert!(op.cancel().is_err());
}

#[test]
fn test_serialization_round_trip() {
    let mut op = RebuildOperation::new("coll_alpha", "op_xyz_99");
    op.start_streaming().expect("streaming");
    op.accept_chunk(0, 123).expect("chunk 0");
    op.accept_chunk(1, 456).expect("chunk 1");
    op.seal().expect("seal");

    let json_bytes = serde_json::to_vec(&op).expect("serialize");
    let recovered: RebuildOperation = serde_json::from_slice(&json_bytes).expect("deserialize");

    assert_eq!(op, recovered);
    assert_eq!(recovered.collection, "coll_alpha");
    assert_eq!(recovered.operation_id, "op_xyz_99");
    assert_eq!(recovered.phase, RebuildPhase::Sealing);
    assert_eq!(recovered.chunks_applied, 2);
    assert_eq!(recovered.docs_applied, 579);
    assert!(!recovered.writes_to_active_only);
}
