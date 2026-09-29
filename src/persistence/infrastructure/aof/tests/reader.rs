use std::cell::Cell;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use storage_durable::{FramedLogCursor, FramedLogWriter, FsyncPolicy};

use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::aof::frame::{decode_payload, HEADER_LEN};
use crate::persistence::infrastructure::aof::replay::{
    replay_aof_into, replay_aof_into_observed, AofReader,
};
use crate::persistence::infrastructure::aof::tests::{
    create_entry, index_entry, rec, replay_seqs, term_query,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::storage::Engine;

/// Write a valid frame whose payload is not a Lumen WAL record between two
/// ordinary AOF records. `FramedLogCursor` can read all three frames only
/// after it has checked each complete payload length and CRC.
fn complete_crc_valid_malformed_middle(path: &Path) {
    {
        let mut writer = AofWriter::open_with_policy(path, FsyncPolicy::Always).unwrap();
        writer.append(1, &rec(create_entry("u"))).unwrap();
        writer.sync().unwrap();
    }
    {
        let mut writer = FramedLogWriter::open(path, FsyncPolicy::Always).unwrap();
        writer.append(2, b"not a Lumen WAL record").unwrap();
        writer.sync().unwrap();
    }
    {
        let mut writer = AofWriter::open_with_policy(path, FsyncPolicy::Always).unwrap();
        writer
            .append(
                3,
                &rec(index_entry("u", "after-malformed", "must-not-apply")),
            )
            .unwrap();
        writer.sync().unwrap();
    }
}

#[test]
fn reader_rejects_complete_crc_valid_malformed_middle_frame_without_visiting_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("complete-malformed-middle.aof");
    complete_crc_valid_malformed_middle(&path);

    let mut cursor = FramedLogCursor::open(&path).unwrap();
    let first = cursor.next_frame().unwrap().unwrap();
    let middle = cursor.next_frame().unwrap().unwrap();
    let suffix = cursor.next_frame().unwrap().unwrap();
    assert_eq!((first.seq, middle.seq, suffix.seq), (1, 2, 3));
    assert!(decode_payload(&middle.payload).is_err());
    assert!(cursor.next_frame().unwrap().is_none());

    let mut visited = Vec::new();
    assert!(AofReader::replay(&path, 0, |seq, _| visited.push(seq)).is_err());
    assert_eq!(visited, vec![1]);
}

#[test]
fn engine_replay_rejects_complete_crc_valid_malformed_middle_frame_at_prefix_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("complete-malformed-middle.aof");
    complete_crc_valid_malformed_middle(&path);
    let engine = Arc::new(Engine::new());

    assert!(replay_aof_into(&engine, &path, 0).is_err());
    let capture = engine.capture_barrier.capture(0).unwrap();
    assert_eq!(capture.stamp().sequence, 1);
    drop(capture);
    assert_eq!(
        engine
            .search("u", term_query("email", "must-not-apply"))
            .unwrap()
            .total,
        0
    );
}

#[test]
fn replay_non_fast_record_uses_the_legacy_decode_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.aof");
    let mut writer = AofWriter::open(&path).unwrap();
    let record = WalRecord {
        version: crate::ingest::domain::wal_record::WAL_FORMAT_VERSION,
        entry: RaftLogEntry::DropCollection {
            collection_id: "missing".into(),
            force: true,
        },
    };
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    let engine = Arc::new(Engine::new());
    let decoded = Cell::new(0);
    assert_eq!(
        replay_aof_into_observed(&engine, &path, 0, || decoded.set(decoded.get() + 1)).unwrap(),
        1
    );
    assert_eq!(decoded.get(), 1);
}

#[test]
fn replay_skips_at_or_below_from_seq() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    for s in 1..=5 {
        w.append(s, &rec(index_entry("u", &format!("u{s}"), "x@y")))
            .unwrap();
    }
    w.sync().unwrap();
    // from_seq = 3 → only seq 4, 5 are replayed (strict `>`).
    assert_eq!(replay_seqs(&path, 3), vec![4, 5]);
    // from_seq = 0 → all.
    assert_eq!(replay_seqs(&path, 0), vec![1, 2, 3, 4, 5]);
    // from_seq = 5 → none.
    assert_eq!(replay_seqs(&path, 5), Vec::<u64>::new());
}

#[test]
fn torn_tail_replays_prefix_then_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    w.append(1, &rec(create_entry("u"))).unwrap();
    w.append(2, &rec(index_entry("u", "u1", "a@x"))).unwrap();
    w.append(3, &rec(index_entry("u", "u2", "b@x"))).unwrap();
    w.sync().unwrap();

    // Simulate a crash mid-append: corrupt the tail by appending a partial,
    // garbage frame (a header claiming a length that overruns EOF).
    let good_len = std::fs::metadata(&path).unwrap().len();
    {
        use std::io::Write as _;
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        // seq=99, len=1_000_000 (way past EOF), crc=0, then a single byte.
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..8].copy_from_slice(&99u64.to_le_bytes());
        hdr[8..12].copy_from_slice(&1_000_000u32.to_le_bytes());
        f.write_all(&hdr).unwrap();
        f.write_all(&[0xAB]).unwrap();
        f.sync_all().unwrap();
    }
    assert!(std::fs::metadata(&path).unwrap().len() > good_len);

    // Replay stops cleanly at the last good frame — no panic, no error.
    assert_eq!(replay_seqs(&path, 0), vec![1, 2, 3]);

    // The next open TRUNCATES the torn tail back to the last good frame.
    let mut w2 = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
    // And a fresh append lands right after frame 3.
    w2.append(4, &rec(index_entry("u", "u3", "c@x"))).unwrap();
    w2.sync().unwrap();
    assert_eq!(replay_seqs(&path, 0), vec![1, 2, 3, 4]);
}

#[test]
fn torn_tail_via_crc_mismatch_stops_at_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.aof");
    let mut w = AofWriter::open_with_policy(&path, FsyncPolicy::Always).unwrap();
    w.append(1, &rec(index_entry("u", "u1", "a@x"))).unwrap();
    w.append(2, &rec(index_entry("u", "u2", "b@x"))).unwrap();
    w.sync().unwrap();

    // Flip a byte in the LAST frame's payload → crc mismatch → torn tail.
    let len = std::fs::metadata(&path).unwrap().len();
    {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(len - 1)).unwrap();
        let mut b = [0u8; 1];
        f.read_exact(&mut b).unwrap();
        f.seek(SeekFrom::Start(len - 1)).unwrap();
        f.write_all(&[b[0] ^ 0xFF]).unwrap();
        f.sync_all().unwrap();
    }
    // Only frame 1 (the un-corrupted prefix) replays.
    assert_eq!(replay_seqs(&path, 0), vec![1]);
}

#[test]
fn replay_missing_file_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist.aof");
    assert_eq!(replay_seqs(&path, 0), Vec::<u64>::new());
}
