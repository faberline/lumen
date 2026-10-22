use storage_durable::{FramedLogCursor, FsyncPolicy};

use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::aof::replay::AofReader;
use crate::persistence::infrastructure::aof::tests::{create_entry, index_entry, rec, replay_seqs};
use crate::shared_kernel::log_entry::RaftLogEntry;

#[test]
fn open_uses_always_fsync_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let writer = AofWriter::open(dir.path().join("aof")).unwrap();
    assert_eq!(writer.policy, FsyncPolicy::Always);
}

#[test]
fn strict_sync_makes_the_current_tail_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aof");
    let mut writer = AofWriter::open(&path).unwrap();
    writer
        .append(
            1,
            &WalRecord::new(RaftLogEntry::DropCollection {
                collection_id: "missing".into(),
                force: true,
            }),
        )
        .unwrap();
    writer.sync_strict().unwrap();
    assert_eq!(AofReader::replay(&path, 0, |_, _| {}).unwrap(), 1);
}

#[test]
fn append_then_replay_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    w.append(1, &rec(create_entry("u"))).unwrap();
    w.append(2, &rec(index_entry("u", "u1", "a@x.com")))
        .unwrap();
    w.append(3, &rec(index_entry("u", "u2", "b@x.com")))
        .unwrap();
    w.sync().unwrap();

    let mut seqs = Vec::new();
    let mut kinds = Vec::new();
    let max = AofReader::replay(&path, 0, |seq, r| {
        seqs.push(seq);
        kinds.push(matches!(r.entry, RaftLogEntry::CreateCollection { .. }));
    })
    .unwrap();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(max, 3);
    assert_eq!(kinds, vec![true, false, false]);
}

#[test]
fn append_raw_payload_keeps_the_validated_wire_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raw.aof");
    let payload = rec(index_entry("u", "u1", "raw@x")).encode().unwrap();
    let mut writer = AofWriter::open(&path).unwrap();
    writer.append_raw_payload(9, &payload).unwrap();
    writer.sync().unwrap();
    let mut cursor = FramedLogCursor::open(&path).unwrap();
    let frame = cursor.next_frame().unwrap().unwrap();
    assert_eq!(frame.seq, 9);
    assert_eq!(frame.payload, payload);
}

#[test]
fn append_raw_payload_uses_the_existing_storage_full_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raw-refusal.aof");
    let payload = rec(index_entry("u", "u1", "raw@x")).encode().unwrap();
    let mut writer = AofWriter::open(&path).unwrap();
    writer.set_inject_storage_full(true);
    assert!(writer.append_raw_payload(1, &payload).is_err());
    writer.set_inject_storage_full(false);
    assert_eq!(AofReader::replay(&path, 0, |_, _| {}).unwrap(), 0);
}

#[test]
fn truncate_through_keeps_only_newer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    for s in 1..=6 {
        w.append(s, &rec(index_entry("u", &format!("u{s}"), "x@y")))
            .unwrap();
    }
    w.sync().unwrap();
    w.truncate_through(4).unwrap();
    // Frames 1..=4 dropped; 5, 6 survive.
    assert_eq!(replay_seqs(&path, 0), vec![5, 6]);
    // And the re-opened append handle keeps appending after the survivors.
    w.append(7, &rec(index_entry("u", "u7", "x@y"))).unwrap();
    w.sync().unwrap();
    assert_eq!(replay_seqs(&path, 0), vec![5, 6, 7]);
}

#[cfg(unix)]
#[test]
fn two_phase_trim_wrapper_retains_the_late_suffix_and_busy_plan() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two-phase.aof");
    let mut writer = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    writer
        .append(1, &rec(index_entry("u", "covered", "one@example.test")))
        .unwrap();
    writer
        .append(2, &rec(index_entry("u", "retained", "two@example.test")))
        .unwrap();

    let mut plan = writer.begin_trim(1).unwrap();
    assert!(
        writer.begin_trim(1).is_err(),
        "an active plan must stay owned"
    );
    plan.copy_stable_prefix().unwrap();
    writer
        .append(3, &rec(index_entry("u", "late", "three@example.test")))
        .unwrap();
    writer.finish_trim(plan).unwrap();

    assert_eq!(replay_seqs(&path, 0), vec![2, 3]);
}

#[test]
fn truncate_through_survivors_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    {
        let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
        for s in 1..=5 {
            w.append(s, &rec(index_entry("u", &format!("u{s}"), "x@y")))
                .unwrap();
        }
        w.sync().unwrap();
        w.truncate_through(2).unwrap();
    }
    // A fresh open sees only the survivors and can extend them.
    let mut w2 = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    w2.append(6, &rec(index_entry("u", "u6", "x@y"))).unwrap();
    w2.sync().unwrap();
    assert_eq!(replay_seqs(&path, 0), vec![3, 4, 5, 6]);
}
