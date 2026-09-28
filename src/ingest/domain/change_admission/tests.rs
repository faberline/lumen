use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::committed_stage::{
    SourceIdentity, SourceKind, StageFailureInjector, StageFailurePoint, StageStore,
};
use crate::ingest::domain::change_admission::{CommittedRecord, RewindablePayload, StagePayload};
use crate::ingest::domain::change_budget::ChangeBudget;

struct FailOnce(Mutex<Option<StageFailurePoint>>);

impl StageFailureInjector for FailOnce {
    fn check(&self, point: StageFailurePoint) -> io::Result<()> {
        let mut wanted = self.0.lock().unwrap();
        if *wanted == Some(point) {
            *wanted = None;
            return Err(io::Error::other("injected stage failure"));
        }
        Ok(())
    }
}

struct DropReader {
    bytes: Cursor<Vec<u8>>,
    drops: Arc<AtomicUsize>,
}

/// Deliberately lacks `Read` and `Seek`: a production `Record` can stream
/// its serializer directly into staging without an encoded-record buffer.
struct WriterOnlyPayload(Vec<u8>);

impl StagePayload for WriterOnlyPayload {
    fn write_stage(&mut self, output: &mut dyn Write) -> io::Result<()> {
        output.write_all(&self.0)
    }
}

impl Read for DropReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(output)
    }
}

impl Seek for DropReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.bytes.seek(position)
    }
}

impl Drop for DropReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct DropChecksCharge {
    bytes: Cursor<Vec<u8>>,
    budget: ChangeBudget,
    observed_reserved: Arc<AtomicUsize>,
}

impl Read for DropChecksCharge {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(output)
    }
}

impl Seek for DropChecksCharge {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.bytes.seek(position)
    }
}

impl Drop for DropChecksCharge {
    fn drop(&mut self) {
        self.observed_reserved
            .store(self.budget.snapshot().reserved, Ordering::SeqCst);
    }
}

fn source(name: &str) -> SourceIdentity {
    SourceIdentity::new(SourceKind::External, name).unwrap()
}

#[test]
fn checkpoint_never_releases_committed_records_waiting_for_apply() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::with_hard_limit(256);
    let owner = budget.owner();
    let record = CommittedRecord::new(
        owner.try_reserve(240).unwrap(),
        RewindablePayload(Cursor::new(vec![7; 240])),
        2,
        1,
        source("orders"),
    )
    .unwrap();
    let _applied = owner.try_reserve(16).unwrap().commit().unwrap();
    let frozen = owner.freeze().unwrap();
    assert_eq!(
        owner.publish_through(&frozen).unwrap(),
        16,
        "checkpoint can release only applied journal data"
    );
    assert_eq!(
        budget.snapshot().total,
        240,
        "committed pre-apply RAM must survive unrelated checkpoint publication"
    );
    assert!(owner.try_reserve(64).is_err());
    record.stage(&store).unwrap();
    assert_eq!(budget.snapshot().total, 0);
    assert!(owner.try_reserve(64).is_ok());
}

#[test]
fn retired_admission_returns_the_committed_payload_to_its_owner() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let reservation = owner.try_reserve(7).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    owner.retire();
    let refusal = CommittedRecord::new(
        reservation,
        RewindablePayload(DropReader {
            bytes: Cursor::new(b"payload".to_vec()),
            drops: drops.clone(),
        }),
        2,
        1,
        source("orders"),
    );
    assert!(refusal.is_err());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "admission failure must return ownership of the committed payload"
    );
    drop(refusal);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn stage_fault_retains_payload_and_charge_for_repeat_attempt() {
    let root = tempfile::tempdir().unwrap();
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let record = CommittedRecord::new(
        owner.try_reserve(7).unwrap(),
        RewindablePayload(Cursor::new(b"payload".to_vec())),
        4,
        2,
        source("orders"),
    )
    .unwrap();
    let store = StageStore::with_injector(
        root.path(),
        Arc::new(FailOnce(Mutex::new(Some(StageFailurePoint::SyncMarker)))),
    )
    .unwrap();
    let failed = match record.stage(&store) {
        Ok(_) => panic!("injected durability fault unexpectedly staged the payload"),
        Err(failed) => failed,
    };
    assert_eq!(failed.error().kind(), io::ErrorKind::Other);
    assert_eq!(budget.snapshot().reserved, 7);
    let staged = failed.into_record().stage(&store).unwrap();
    assert_eq!(staged.stage().sequence(), 4);
    assert_eq!(budget.snapshot().reserved, 0);
}

#[test]
fn dropped_carrier_cannot_cancel_its_committed_charge() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let drops = Arc::new(AtomicUsize::new(0));
    let record = CommittedRecord::new(
        owner.try_reserve(7).unwrap(),
        RewindablePayload(DropReader {
            bytes: Cursor::new(b"payload".to_vec()),
            drops: drops.clone(),
        }),
        5,
        2,
        source("orders"),
    )
    .unwrap();
    drop(record);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.snapshot().reserved, 7);
}

#[test]
fn durable_success_drops_ram_after_exact_recovery_and_releases_charge() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let drops = Arc::new(AtomicUsize::new(0));
    let record = CommittedRecord::new(
        owner.try_reserve(7).unwrap(),
        RewindablePayload(DropReader {
            bytes: Cursor::new(b"payload".to_vec()),
            drops: drops.clone(),
        }),
        6,
        3,
        source("orders"),
    )
    .unwrap();
    let staged = record.stage(&store).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut recovered = Vec::new();
    staged
        .stage()
        .open()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, b"payload");
    assert_eq!(budget.snapshot().reserved, 0);
}

#[test]
fn durable_stage_drops_payload_before_releasing_its_ram_charge() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let observed_reserved = Arc::new(AtomicUsize::new(usize::MAX));
    CommittedRecord::new(
        owner.try_reserve(7).unwrap(),
        RewindablePayload(DropChecksCharge {
            bytes: Cursor::new(b"payload".to_vec()),
            budget: budget.clone(),
            observed_reserved: observed_reserved.clone(),
        }),
        8,
        3,
        source("orders"),
    )
    .unwrap()
    .stage(&store)
    .unwrap();
    assert_eq!(observed_reserved.load(Ordering::SeqCst), 7);
    assert_eq!(budget.snapshot().reserved, 0);
}

#[test]
fn foreign_epoch_source_and_proof_refuse_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let staged = CommittedRecord::new(
        owner.try_reserve(7).unwrap(),
        RewindablePayload(Cursor::new(b"payload".to_vec())),
        7,
        4,
        source("orders"),
    )
    .unwrap()
    .stage(&store)
    .unwrap();
    let stage = staged.into_stage();
    assert!(store
        .remove_after_verified_watermark(stage, store.cleanup_proof(source("other"), 4, 7))
        .is_err());
    let stage = store.recover().unwrap().pop().unwrap();
    assert!(store
        .remove_after_verified_watermark(stage, store.cleanup_proof(source("orders"), 5, 7))
        .is_err());
}

#[test]
fn durable_stage_frees_ram_for_a_later_reservation_without_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::with_hard_limit(256);
    let owner = budget.owner();
    let record = CommittedRecord::new(
        owner.try_reserve(240).unwrap(),
        RewindablePayload(Cursor::new(vec![7; 240])),
        9,
        1,
        source("orders"),
    )
    .unwrap();
    assert!(owner.try_reserve(64).is_err());
    record.stage(&store).unwrap();
    assert_eq!(budget.snapshot().reserved, 0);
    assert!(owner.try_reserve(64).is_ok());
}

#[test]
fn serializer_callback_payload_needs_neither_read_nor_seek() {
    let root = tempfile::tempdir().unwrap();
    let store = StageStore::new(root.path()).unwrap();
    let budget = ChangeBudget::with_hard_limit(32);
    let owner = budget.owner();
    let staged = CommittedRecord::new(
        owner.try_reserve(9).unwrap(),
        WriterOnlyPayload(b"serializer".to_vec()),
        10,
        1,
        source("orders"),
    )
    .unwrap()
    .stage(&store)
    .unwrap();
    let mut recovered = Vec::new();
    staged
        .stage()
        .open()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, b"serializer");
    assert_eq!(budget.snapshot().reserved, 0);
}
