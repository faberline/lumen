use std::sync::{Arc, Mutex};

use futures::StreamExt;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::domain::wal_record::{WalRecord, WAL_FORMAT_VERSION};
use crate::ingest::infrastructure::wal::delivery::WalDelivery;
use crate::ingest::infrastructure::wal::mem_wal::{MemWal, MemWalSlot};
use crate::ingest::infrastructure::wal::tests::{create_entry, index_entry, source_retention};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;
use crate::storage::Engine;
use crate::wal_source_stage::WalSourceStager;

// Append inside `src/wal.rs`'s existing `#[cfg(test)] mod tests`.
// The injected-fault test needs this test-only helper in `wal_source_stage.rs`:
//
// #[cfg(test)] pub(crate) fn for_mem_wal_with_injector(
//     injector: Arc<dyn StageFailureInjector>,
// ) -> io::Result<Self>
//
// It must create the normal private directory and StageStore::with_injector;
// do not expose it outside cfg(test).

#[tokio::test]
async fn admitted_locator_observes_exact_record_after_its_source_is_staged() {
    let wal = MemWal::new();
    let mut stream = wal.subscribe_admitted(0).await.unwrap();
    wal.publish(WalRecord::new(index_entry("orders", "a", "email", "a@x")))
        .await
        .unwrap();
    let (sequence, delivery) = stream.next().await.unwrap().unwrap();
    let WalDelivery::Deferred(locator) = delivery else {
        panic!("MemWal must deliver source locator");
    };
    assert_eq!(locator.sequence(), sequence);
    assert_eq!(
        wal.stage_source(sequence)
            .await
            .unwrap()
            .unwrap()
            .sequence(),
        sequence
    );
    // Staging changes the owned representation. Reprice its decode bound
    // after the source replacement and before reading it.
    let admitted = locator.decoded_owned_bytes().unwrap() + locator.read_scratch_bytes();
    let decoded = locator.read(admitted).unwrap();
    let RaftLogEntry::Index { collection_id, req } = decoded.entry else {
        panic!("expected Index");
    };
    assert_eq!(collection_id, "orders");
    assert!(matches!(&req.items[0].value, FieldValue::String(value) if value == "a@x"));
}

#[tokio::test]
async fn staged_locator_pins_source_after_subscription_truncates_it() {
    let wal = MemWal::new();
    let mut stream = wal.subscribe_admitted(0).await.unwrap();
    wal.publish(WalRecord::new(create_entry("one")))
        .await
        .unwrap();
    let (sequence, delivery) = stream.next().await.unwrap().unwrap();
    let WalDelivery::Deferred(locator) = delivery else {
        panic!("expected locator");
    };
    wal.stage_source(sequence).await.unwrap();
    wal.publish(WalRecord::new(create_entry("two")))
        .await
        .unwrap();
    let _ = stream.next().await.unwrap().unwrap();
    assert_eq!(wal.shared.lock().unwrap().base, sequence);
    // Staging changes the owned representation. Reprice its decode bound
    // after the source replacement and before reading it.
    let admitted = locator.decoded_owned_bytes().unwrap() + locator.read_scratch_bytes();
    assert!(
        matches!(locator.read(admitted).unwrap().entry, RaftLogEntry::CreateCollection { collection_id, .. } if collection_id == "one")
    );
}

#[tokio::test]
async fn admission_is_checked_for_resident_and_staged_sources() {
    let wal = MemWal::new();
    let mut stream = wal.subscribe_admitted(0).await.unwrap();
    wal.publish(WalRecord::new(create_entry("orders")))
        .await
        .unwrap();
    let (sequence, delivery) = stream.next().await.unwrap().unwrap();
    let WalDelivery::Deferred(resident) = delivery else {
        panic!("expected locator");
    };
    let raw = resident.decoded_owned_bytes().unwrap();
    assert!(resident.read(raw - 1).is_err());
    let (_, delivery) = wal
        .subscribe_admitted(0)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let WalDelivery::Deferred(staged) = delivery else {
        panic!("expected locator");
    };
    wal.stage_source(sequence).await.unwrap();
    assert!(
        staged.read(raw).is_err(),
        "stage scratch must also be admitted"
    );
}

#[tokio::test]
async fn stage_proof_follows_slot_replacement_and_is_repeatable_per_source() {
    let wal = MemWal::new();
    wal.publish(WalRecord::new(create_entry("orders")))
        .await
        .unwrap();
    let first = wal.stage_source(1).await.unwrap().unwrap();
    assert_eq!(first.sequence(), 1);
    assert!(matches!(
        &*wal.shared.lock().unwrap().records[0].lock().unwrap(),
        MemWalSlot::Staged(_)
    ));
    assert_eq!(wal.stage_source(1).await.unwrap().unwrap().sequence(), 1);
    wal.publish(WalRecord::new(create_entry("other")))
        .await
        .unwrap();
    assert_eq!(wal.stage_source(2).await.unwrap().unwrap().sequence(), 2);
}

#[tokio::test]
async fn compatibility_subscribe_replays_values_after_staging() {
    let wal = MemWal::new();
    wal.publish(WalRecord::new(index_entry("orders", "a", "email", "a@x")))
        .await
        .unwrap();
    wal.stage_source(1).await.unwrap();
    let mut stream = wal.subscribe(0).await.unwrap();
    let (sequence, record) = stream.next().await.unwrap().unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(record.version, WAL_FORMAT_VERSION);
    assert!(matches!(record.entry, RaftLogEntry::Index { .. }));
}

// Injected fault contract: after adding the cfg(test) stager factory above,
// replace `wal.source_stager` with the injected stager before calling
// `stage_source`. Assert it returns Err, the slot remains Resident, and no
// WalSourceRelease is produced. This needs a small MemWal cfg(test) setter
// because the source stager is intentionally lazy and private.

#[test]
fn owned_delivery_rejects_missing_admission() {
    let record = WalRecord::new(create_entry("orders"));
    let raw = Engine::record_owned_bytes(&record.entry).unwrap();
    assert!(
        WalDelivery::Resident(record).read(raw - 1).is_err(),
        "owned delivery needs raw memory admission"
    );
}

#[tokio::test]
async fn staged_descriptor_rejects_a_different_sequence() {
    let wal = MemWal::new();
    wal.publish(WalRecord::new(create_entry("orders")))
        .await
        .unwrap();
    let mut stream = wal.subscribe_admitted(0).await.unwrap();
    let (_, delivery) = stream.next().await.unwrap().unwrap();
    let WalDelivery::Deferred(mut locator) = delivery else {
        panic!("expected locator");
    };
    wal.stage_source(1).await.unwrap();
    let admitted = locator.decoded_owned_bytes().unwrap() + locator.read_scratch_bytes();
    locator.sequence = 2;
    assert!(
        locator.read(admitted).is_err(),
        "staged descriptor must match its source sequence"
    );
}

#[tokio::test]
async fn failed_source_stage_keeps_resident_until_a_durable_retry() {
    use crate::committed_stage::{StageFailureInjector, StageFailurePoint};
    struct FailOnce(Mutex<Option<StageFailurePoint>>);
    impl StageFailureInjector for FailOnce {
        fn check(&self, point: StageFailurePoint) -> std::io::Result<()> {
            let mut fail = self.0.lock().unwrap();
            if fail.as_ref() == Some(&point) {
                *fail = None;
                return Err(std::io::Error::other(
                    "injected native source staging fault",
                ));
            }
            Ok(())
        }
    }
    for point in [
        StageFailurePoint::SyncPayload,
        StageFailurePoint::RenamePayload,
        StageFailurePoint::SyncPayloadDirectory,
        StageFailurePoint::SyncMarker,
        StageFailurePoint::PublishMarker,
        StageFailurePoint::SyncMarkerDirectory,
    ] {
        let wal = MemWal::new();
        *wal.source_stager.lock().unwrap() = Some(
            WalSourceStager::for_mem_wal_with_injector(Arc::new(FailOnce(Mutex::new(Some(point)))))
                .unwrap(),
        );
        let original = WalRecord::new(index_entry("orders", "a", "email", "a@x"));
        let expected = original.encode().unwrap();
        wal.publish(original).await.unwrap();
        assert!(wal.stage_source(1).await.is_err(), "{point:?}");
        {
            let state = wal.shared.lock().unwrap();
            let slot = state.records[0].lock().unwrap();
            let MemWalSlot::Resident(record, _) = &*slot else {
                panic!("failed stage discarded resident at {point:?}");
            };
            assert_eq!(record.encode().unwrap(), expected, "{point:?}");
        }
        assert_eq!(wal.stage_source(1).await.unwrap().unwrap().sequence(), 1);
        let (_, delivered) = wal
            .subscribe(0)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.encode().unwrap(), expected, "{point:?}");
    }
}

#[tokio::test]
async fn resident_source_retention_outlives_delivery_and_first_subscriber_poll() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wal = MemWal::new();
    let mut first = wal.subscribe_admitted(0).await.unwrap();
    let mut second = wal.subscribe_admitted(0).await.unwrap();
    wal.publish(WalRecord::new(create_entry("one")))
        .await
        .unwrap();

    let (sequence, delivery) = first.next().await.unwrap().unwrap();
    assert_eq!(sequence, 1);
    let retention = source_retention(&budget);
    delivery.retain_source(retention.clone()).unwrap();
    drop(retention);
    drop(delivery);
    assert_eq!(
        budget.snapshot().total,
        7,
        "resident slot owns the source charge"
    );

    // The first subscriber has released its owned delivery and polls again,
    // but the second subscriber has not yet released this native source.
    wal.publish(WalRecord::new(create_entry("two")))
        .await
        .unwrap();
    assert_eq!(first.next().await.unwrap().unwrap().0, 2);
    assert_eq!(
        budget.snapshot().total,
        7,
        "second source pin still owns charge"
    );

    // Polling second through sequence two lets MemWal truncate sequence one.
    // The resident slot's final drop, rather than any timer or idle poll,
    // must release the charge.
    assert_eq!(second.next().await.unwrap().unwrap().0, 1);
    assert_eq!(second.next().await.unwrap().unwrap().0, 2);
    assert_eq!(budget.snapshot().total, 0);
}

#[tokio::test]
async fn verified_stage_replacement_releases_resident_source_retention_while_pinned() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wal = MemWal::new();
    let mut subscriber = wal.subscribe_admitted(0).await.unwrap();
    wal.publish(WalRecord::new(create_entry("one")))
        .await
        .unwrap();

    let (sequence, delivery) = subscriber.next().await.unwrap().unwrap();
    let retention = source_retention(&budget);
    delivery.retain_source(retention.clone()).unwrap();
    drop(retention);
    assert_eq!(budget.snapshot().total, 7);

    assert_eq!(
        wal.stage_source(sequence)
            .await
            .unwrap()
            .unwrap()
            .sequence(),
        sequence
    );
    assert_eq!(
        budget.snapshot().total,
        0,
        "verified replacement releases only the resident-source charge"
    );

    // `subscriber` still pins the source slot and `delivery` still owns a
    // descriptor. The replacement must not make the staged record unreadable.
    let admitted = delivery.decoded_owned_bytes().unwrap() + delivery.read_scratch_bytes();
    assert!(matches!(
        delivery.read(admitted).unwrap().entry,
        RaftLogEntry::CreateCollection { collection_id, .. } if collection_id == "one"
    ));
}
