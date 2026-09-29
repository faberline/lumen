use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;

use crate::ingest::application::write_coordinator::tests::{
    admitted_index_entry, keyword_schema, wait_for_reserved, ControlledWal,
};
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::{WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::storage::{ApplyOutcome, Engine};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_local_reservation_cannot_starve_earlier_external_record() {
    struct DeferredWal {
        inner: MemWal,
        released: Arc<std::sync::atomic::AtomicBool>,
        changed: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl WalLog for DeferredWal {
        async fn publish(&self, record: WalRecord) -> Result<u64> {
            self.inner.publish(record).await
        }
        async fn latest_seq(&self) -> Result<u64> {
            self.inner.latest_seq().await
        }
        async fn stage_source(
            &self,
            seq: u64,
        ) -> Result<Option<crate::ingest::infrastructure::wal::delivery::WalSourceRelease>>
        {
            self.inner.stage_source(seq).await
        }
        async fn subscribe_admitted(
            &self,
            from: u64,
        ) -> Result<crate::ingest::domain::wal_log::WalAdmissionStream> {
            let stream = self.inner.subscribe_admitted(from).await?;
            let state = (stream, self.released.clone(), self.changed.clone());
            Ok(Box::pin(futures::stream::unfold(
                state,
                |(mut stream, released, changed)| async move {
                    loop {
                        let wake = changed.notified();
                        if released.load(Ordering::Acquire) {
                            break;
                        }
                        wake.await;
                    }
                    stream
                        .next()
                        .await
                        .map(|record| (record, (stream, released, changed)))
                },
            )))
        }
        async fn subscribe(&self, from: u64) -> Result<WalStream> {
            let stream = self.inner.subscribe(from).await?;
            let state = (stream, self.released.clone(), self.changed.clone());
            Ok(Box::pin(futures::stream::unfold(
                state,
                |(mut stream, released, changed)| async move {
                    loop {
                        let wake = changed.notified();
                        if released.load(Ordering::Acquire) {
                            break;
                        }
                        wake.await;
                    }
                    stream
                        .next()
                        .await
                        .map(|record| (record, (stream, released, changed)))
                },
            )))
        }
    }
    let entry = |id: &str| RaftLogEntry::Index {
        collection_id: "u".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: id.into(),
                field: "email".into(),
                value: FieldValue::String(format!("{id}-{}", "x".repeat(8192))),
                version: None,
            }],
            request_id: None,
        },
    };
    let external = entry("external");
    let local = entry("local");
    let calibration = Engine::with_change_budget(ChangeBudget::with_hard_limit(1024 * 1024));
    calibration
        .create_collection("u", keyword_schema())
        .unwrap();
    let local_raw = Engine::record_owned_bytes(&local).unwrap();
    let local_price = calibration
        .try_reserve_record(&local, local_raw * 2)
        .unwrap()
        .bytes();
    let external_raw = Engine::record_owned_bytes(&external).unwrap() * 3;
    let external_price = calibration
        .try_reserve_record(&external, external_raw / 3 * 2)
        .unwrap()
        .bytes();
    let hard = local_price + external_raw - 1;
    assert!(external_price < hard && local_price < hard);
    let budget = ChangeBudget::with_hard_limit(hard);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = Arc::new(DeferredWal {
        inner: MemWal::new(),
        released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        changed: Arc::new(tokio::sync::Notify::new()),
    });
    assert_eq!(wal.publish(WalRecord::new(external)).await.unwrap(), 1);
    let coord = WriteCoordinator::start(wal.clone(), engine.clone());
    let mut submitted = tokio::spawn({
        let coord = coord.clone();
        async move { coord.submit(local).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if wal.latest_seq().await.unwrap() >= 2 || submitted.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the local publication attempt must finish before delivery starts");
    let committed_local = wal.latest_seq().await.unwrap() == 2;
    assert_eq!(coord.applied_seq(), 0);

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(
            directory.path(),
        )
        .unwrap(),
    );
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let checkpoint = tokio::spawn({
        let stop = stop.clone();
        let engine = engine.clone();
        let coord = coord.clone();
        let store = store.clone();
        async move {
            while !stop.load(Ordering::Acquire) {
                let engine = engine.clone();
                let store = store.clone();
                let seq = coord.applied_seq();
                tokio::task::spawn_blocking(move || store.save(&engine, seq))
                    .await
                    .unwrap()
                    .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    });
    wal.released.store(true, Ordering::Release);
    wal.changed.notify_waiters();
    let mut delivered_outcome = None;
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        delivered_outcome = Some((&mut submitted).await);
        let expected = if committed_local { 2 } else { 1 };
        while coord.applied_seq() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let completed_without_reservation_discard = result.is_ok();
    let outcome = match result {
        Ok(()) => delivered_outcome.take().expect("local result returned"),
        Err(_) => {
            // Teardown only: unblock a broken implementation so the Tokio
            // runtime does not wait forever for its native capacity waiter.
            // The acceptance boolean was captured before this intervention.
            drop(coord.local_reservations.lock().await.remove(&2));
            match delivered_outcome.take() {
                Some(outcome) => outcome,
                None => tokio::time::timeout(std::time::Duration::from_secs(3), &mut submitted)
                    .await
                    .expect("failure teardown must release the stalled reservation"),
            }
        }
    };
    stop.store(true, Ordering::Release);
    checkpoint.await.unwrap();
    if committed_local {
        outcome.unwrap().unwrap();
        assert_eq!(coord.applied_seq(), 2);
        assert_eq!(engine.stats("u").unwrap().documents_indexed, 2);
    } else {
        let error = outcome.unwrap().unwrap_err();
        assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
        assert_eq!(coord.applied_seq(), 1);
        assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
    }
    assert!(budget.high_water_bytes() <= hard);
    assert!(
        completed_without_reservation_discard,
        "future local reservation blocked the earlier external record despite a running checkpoint worker"
    );
}

#[tokio::test]
async fn local_admission_composite_drops_both_reservation_halves() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let coord = WriteCoordinator::start(Arc::new(MemWal::new()), engine);
    let baseline = budget.snapshot().total;

    let local = coord
        .try_admit_local_record(&admitted_index_entry())
        .unwrap()
        .expect("bounded local record must reserve before WAL publication");
    assert!(local.transient.is_some());
    assert_eq!(budget.snapshot().reserved, local.bytes());
    drop(local);
    assert_eq!(
        budget.snapshot().total,
        baseline,
        "dropping a capacity-relief map entry must release apply and transient halves"
    );
}

#[tokio::test]
async fn cancelled_local_submit_keeps_its_reservation_until_apply_finishes() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = ControlledWal::paused(ControlledWal::PAUSE_DELIVERY);
    let coord = WriteCoordinator::start(wal.clone(), engine);
    let submit = tokio::spawn({
        let coord = coord.clone();
        async move { coord.submit(admitted_index_entry()).await }
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        wal.observed_publish.notified(),
    )
    .await
    .expect("submit must publish after pre-admission");
    wait_for_reserved(&budget).await;
    {
        let ledger = coord.local_reservations.lock().await;
        let local = ledger
            .get(&1)
            .expect("cancelled local submit must retain its composite reservation");
        assert!(
            local.transient.is_some(),
            "local admission must retain transport/AOF ownership with apply ownership"
        );
        assert_eq!(
            local.bytes(),
            budget.snapshot().reserved,
            "the sequence map must retain both reservation halves"
        );
    }
    submit.abort();
    assert!(submit.await.unwrap_err().is_cancelled());
    assert!(budget.snapshot().reserved > 0);
    assert_eq!(coord.applied_seq(), 0);

    wal.release_delivery.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while coord.applied_seq() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("apply must consume the cancelled caller's sequence reservation");
    assert_eq!(budget.snapshot().reserved, 0);
    assert!(budget.snapshot().active > 0);
}

#[tokio::test]
async fn publication_lookup_race_cannot_apply_before_reservation_installation() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = ControlledWal::paused(ControlledWal::PAUSE_PUBLISH);
    let coord = WriteCoordinator::start(wal.clone(), engine);
    let submit = tokio::spawn({
        let coord = coord.clone();
        async move { coord.submit(admitted_index_entry()).await }
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), wal.delivered.notified())
        .await
        .expect("subscriber must observe the record while publish is paused");
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert_eq!(
        coord.applied_seq(),
        0,
        "apply must wait for the sequence reservation installed after publish returns"
    );

    wal.release_publish.notify_one();
    assert!(matches!(
        submit.await.unwrap().unwrap(),
        ApplyOutcome::Indexed(_)
    ));
    assert_eq!(coord.applied_seq(), 1);
}
