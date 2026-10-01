use std::fs;
use std::io::{self, Cursor, ErrorKind, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::ingest::infrastructure::committed_stage::{
    DurableStage, SourceIdentity, SourceKind, StageFailureInjector, StageFailurePoint, StageStore,
    COPY_BUFFER_BYTES, FORMAT_MAGIC,
};

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

fn source() -> SourceIdentity {
    SourceIdentity::new(SourceKind::External, "nats:orders:1").unwrap()
}

fn store(root: &Path, point: StageFailurePoint) -> StageStore {
    StageStore::with_injector(root, Arc::new(FailAt(Mutex::new(Some(point))))).unwrap()
}

struct BoundedReader {
    remaining: usize,
    largest_buffer: usize,
}

impl Read for BoundedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.largest_buffer = self.largest_buffer.max(output.len());
        let count = self.remaining.min(output.len());
        output[..count].fill(b'x');
        self.remaining -= count;
        Ok(count)
    }
}

#[test]
fn receipt_requires_every_payload_and_marker_durability_step() {
    for point in [
        StageFailurePoint::SyncPayload,
        StageFailurePoint::RenamePayload,
        StageFailurePoint::SyncPayloadDirectory,
        StageFailurePoint::SyncMarker,
        StageFailurePoint::PublishMarker,
        StageFailurePoint::SyncMarkerDirectory,
    ] {
        let root = tempfile::tempdir().unwrap();
        let error = store(root.path(), point)
            .stage(7, 3, source(), Cursor::new(b"payload"))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other, "{point:?}");
        if point != StageFailurePoint::SyncMarkerDirectory {
            assert!(
                store(root.path(), StageFailurePoint::SyncPayload)
                    .recover()
                    .unwrap()
                    .is_empty(),
                "{point:?} must not publish a recovery-visible marker"
            );
        }
    }
}

#[test]
fn staging_reads_only_the_fixed_copy_buffer() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let mut reader = BoundedReader {
        remaining: COPY_BUFFER_BYTES * 3 + 7,
        largest_buffer: 0,
    };
    let stage = store.stage(5, 1, source(), &mut reader).unwrap();
    assert_eq!(stage.byte_len(), (COPY_BUFFER_BYTES * 3 + 7) as u64);
    assert_eq!(reader.largest_buffer, COPY_BUFFER_BYTES);
}

#[test]
fn recovery_is_sorted_and_validates_exact_payload() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    store.stage(9, 2, source(), Cursor::new(b"nine")).unwrap();
    store.stage(4, 2, source(), Cursor::new(b"four")).unwrap();
    let stages = store.recover().unwrap();
    assert_eq!(
        stages
            .iter()
            .map(DurableStage::sequence)
            .collect::<Vec<_>>(),
        [4, 9]
    );
    let mut bytes = Vec::new();
    stages[0].open().unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"four");

    fs::write(&stages[0].record_path, b"changed").unwrap();
    assert!(store.recover().is_err());
}

#[test]
fn orphan_payload_is_not_auto_removed_and_marker_corruption_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let orphan = store.records_dir().join(".payload-orphan.tmp");
    fs::write(&orphan, b"uncommitted").unwrap();
    assert!(store.recover().unwrap().is_empty());
    assert!(orphan.exists());

    let stage = store.stage(8, 1, source(), Cursor::new(b"eight")).unwrap();
    fs::write(&stage.marker_path, b"unknown").unwrap();
    assert!(store.recover().is_err());
}

#[test]
fn recovery_rejects_missing_and_truncated_committed_pairs() {
    let missing_root = tempfile::tempdir().unwrap();
    let missing_store = StageStore::new(missing_root.path()).unwrap();
    let missing = missing_store
        .stage(20, 1, source(), Cursor::new(b"twenty"))
        .unwrap();
    fs::remove_file(&missing.record_path).unwrap();
    assert!(missing_store.recover().is_err());

    let truncated_root = tempfile::tempdir().unwrap();
    let truncated_store = StageStore::new(truncated_root.path()).unwrap();
    let truncated = truncated_store
        .stage(21, 1, source(), Cursor::new(b"twenty-one"))
        .unwrap();
    fs::write(&truncated.marker_path, FORMAT_MAGIC).unwrap();
    assert!(truncated_store.recover().is_err());
}

#[test]
fn removal_requires_verified_watermark_and_never_happens_on_drop() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let stage = store
        .stage(12, 7, source(), Cursor::new(b"twelve"))
        .unwrap();
    let marker = stage.marker_path.clone();
    drop(stage);
    assert!(marker.exists());
    let stage = store.recover().unwrap().pop().unwrap();
    assert!(store
        .remove_after_verified_watermark(stage, store.cleanup_proof(source(), 7, 11))
        .is_err());
    let stage = store.recover().unwrap().pop().unwrap();
    store
        .remove_after_verified_watermark(stage, store.cleanup_proof(source(), 7, 12))
        .unwrap();
    assert!(store.recover().unwrap().is_empty());
}

#[test]
fn recovery_rejects_symlink_marker_when_supported() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let store = StageStore::new(root.path()).unwrap();
        symlink(
            "/dev/null",
            store
                .markers_dir()
                .join("00000000000000000001-0000000000000001.commit"),
        )
        .unwrap();
        assert!(store.recover().is_err());
    }
}

#[test]
fn open_rejects_a_payload_path_replaced_by_symlink_when_supported() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let store = StageStore::new(root.path()).unwrap();
        let stage = store
            .stage(30, 1, source(), Cursor::new(b"thirty"))
            .unwrap();
        fs::remove_file(&stage.record_path).unwrap();
        symlink("/dev/null", &stage.record_path).unwrap();
        assert!(stage.open().is_err());
    }
}

#[test]
fn every_failed_durability_step_retries_the_same_payload_to_a_receipt() {
    for point in [
        StageFailurePoint::SyncPayload,
        StageFailurePoint::RenamePayload,
        StageFailurePoint::SyncPayloadDirectory,
        StageFailurePoint::SyncMarker,
        StageFailurePoint::PublishMarker,
        StageFailurePoint::SyncMarkerDirectory,
    ] {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path(), point);
        assert!(store.stage(41, 9, source(), Cursor::new(b"retry")).is_err());
        let receipt = store.stage(41, 9, source(), Cursor::new(b"retry")).unwrap();
        assert_eq!(receipt.sequence(), 41, "{point:?}");
        assert_eq!(store.recover().unwrap().len(), 1, "{point:?}");
    }
}

#[test]
fn retry_rejects_conflicting_payload_and_foreign_cleanup_proof() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let stage = store.stage(50, 4, source(), Cursor::new(b"first")).unwrap();
    assert!(store.stage(50, 4, source(), Cursor::new(b"other")).is_err());
    let other_root = tempfile::tempdir().unwrap();
    let other = StageStore::new(other_root.path()).unwrap();
    assert!(other
        .remove_after_verified_watermark(stage, other.cleanup_proof(source(), 4, 50))
        .is_err());
}

#[test]
fn identical_retry_removes_only_its_redundant_payload_temp() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let unrelated = store.records_dir().join(".payload-unrelated.tmp");
    fs::write(&unrelated, b"unrelated orphan").unwrap();

    let first = store.stage(51, 4, source(), Cursor::new(b"same")).unwrap();
    let original_record = first.record_path.clone();
    let original_marker = first.marker_path.clone();
    store.stage(51, 4, source(), Cursor::new(b"same")).unwrap();

    let mut record_names = fs::read_dir(store.records_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    record_names.sort();
    assert_eq!(
        record_names,
        vec![
            unrelated.file_name().unwrap().to_owned(),
            original_record.file_name().unwrap().to_owned(),
        ]
    );
    assert!(unrelated.exists(), "unrelated orphan must stay retained");
    assert!(original_record.exists());
    assert!(original_marker.exists());
    assert_eq!(store.recover().unwrap().len(), 1);
    assert!(store
        .stage(51, 4, source(), Cursor::new(b"conflict"))
        .is_err());
    assert!(original_record.exists());
    assert!(original_marker.exists());
}

#[test]
fn retry_after_marker_publish_fault_keeps_prior_marker_orphan_only() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path(), StageFailurePoint::PublishMarker);
    assert!(store.stage(52, 4, source(), Cursor::new(b"same")).is_err());
    let mut prior_marker_orphans = fs::read_dir(store.markers_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".marker-"))
        })
        .collect::<Vec<_>>();
    assert_eq!(prior_marker_orphans.len(), 1);
    let prior_marker_orphan = prior_marker_orphans.pop().unwrap();

    let receipt = store.stage(52, 4, source(), Cursor::new(b"same")).unwrap();
    let record_names = fs::read_dir(store.records_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        record_names,
        vec![receipt.record_path.file_name().unwrap().to_owned()]
    );
    assert!(
        prior_marker_orphan.exists(),
        "first uncertain marker orphan must stay"
    );
    assert!(receipt.marker_path.exists());
    assert_eq!(store.recover().unwrap().len(), 1);
}
