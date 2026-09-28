use std::collections::BTreeMap;

use futures::StreamExt;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::CreateCollectionRequest,
};

fn create_entry(coll: &str) -> RaftLogEntry {
    RaftLogEntry::CreateCollection {
        collection_id: coll.into(),
        req: CreateCollectionRequest {
            fields: BTreeMap::new(),
        },
    }
}

fn index_entry(coll: &str, eid: &str, field: &str, val: &str) -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: coll.into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: eid.into(),
                field: field.into(),
                value: FieldValue::String(val.into()),
                version: None,
            }],
            request_id: None,
        },
    }
}

fn source_retention(
    budget: &ChangeBudget,
) -> crate::ingest::domain::change_budget::SourceRetention {
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(7).unwrap();
    let retention = reservation.source_retention();
    drop(reservation);
    retention
}

#[tokio::test]
async fn mem_publish_assigns_increasing_seq() {
    let wal = MemWal::new();
    let s1 = wal
        .publish(WalRecord::new(create_entry("a")))
        .await
        .unwrap();
    let s2 = wal
        .publish(WalRecord::new(create_entry("b")))
        .await
        .unwrap();
    assert_eq!(s1, 1);
    assert_eq!(s2, 2);
    assert_eq!(wal.latest_seq().await.unwrap(), 2);
}

/// #1486 R1: a `MemWal` seeded from a restored watermark assigns its
/// first fresh sequence strictly above that watermark — required so the
/// coordinator's `applied` watermark (also seeded from the same
/// restore) never sees a fresh write land at or below it.
#[tokio::test]
async fn mem_starting_at_assigns_seq_above_base() {
    let wal = MemWal::starting_at(5);
    assert_eq!(wal.latest_seq().await.unwrap(), 5);
    let s1 = wal
        .publish(WalRecord::new(create_entry("a")))
        .await
        .unwrap();
    let s2 = wal
        .publish(WalRecord::new(create_entry("b")))
        .await
        .unwrap();
    assert_eq!(s1, 6);
    assert_eq!(s2, 7);
    assert_eq!(wal.latest_seq().await.unwrap(), 7);
}

/// #1486 R1: a subscriber tailing from exactly the restored watermark
/// (mirrors the apply loop's `wal.subscribe(applied)` on cold start)
/// receives every fresh record published after `starting_at`, in order
/// — the exact delivery path the original bug broke.
#[tokio::test]
async fn mem_starting_at_subscribe_from_watermark_delivers_fresh_writes() {
    let wal = MemWal::starting_at(5);
    let mut sub = wal.subscribe(5).await.unwrap();
    let seq = wal
        .publish(WalRecord::new(create_entry("a")))
        .await
        .unwrap();
    assert_eq!(seq, 6);
    let (delivered_seq, _rec) = sub.next().await.unwrap().unwrap();
    assert_eq!(
        delivered_seq, 6,
        "first fresh write after a watermark restore must be delivered promptly"
    );
}

#[tokio::test]
async fn mem_subscribe_replays_backlog_then_tails() {
    let wal = MemWal::new();
    wal.publish(WalRecord::new(index_entry("c", "u1", "e", "a@x")))
        .await
        .unwrap();
    wal.publish(WalRecord::new(index_entry("c", "u2", "e", "b@x")))
        .await
        .unwrap();

    let mut sub = wal.subscribe(0).await.unwrap();
    // Backlog.
    let (seq1, _) = sub.next().await.unwrap().unwrap();
    let (seq2, _) = sub.next().await.unwrap().unwrap();
    assert_eq!((seq1, seq2), (1, 2));

    // Live tail: publish after subscribing, the stream must deliver it.
    let wal2 = wal.clone();
    tokio::spawn(async move {
        wal2.publish(WalRecord::new(index_entry("c", "u3", "e", "c@x")))
            .await
            .unwrap();
    });
    let (seq3, _) = sub.next().await.unwrap().unwrap();
    assert_eq!(seq3, 3);
}

#[tokio::test]
async fn mem_subscribe_from_offset_skips_backlog() {
    let wal = MemWal::new();
    for i in 0..5 {
        wal.publish(WalRecord::new(create_entry(&format!("c{i}"))))
            .await
            .unwrap();
    }
    // Subscribe from seq 3 → first delivered is seq 4.
    let mut sub = wal.subscribe(3).await.unwrap();
    let (seq, _) = sub.next().await.unwrap().unwrap();
    assert_eq!(seq, 4);
}

#[tokio::test]
async fn mem_truncates_behind_a_caught_up_subscriber() {
    // The single-subscriber steady-state contract: a subscriber that
    // keeps up lets the log drop everything it has consumed, so the
    // retained record count stays bounded no matter how much is
    // published.
    let wal = MemWal::new();
    let mut sub = wal.subscribe(0).await.unwrap();
    for i in 0..200u32 {
        wal.publish(WalRecord::new(create_entry(&format!("c{i}"))))
            .await
            .unwrap();
        // Consume each as it arrives — stays caught up.
        let (seq, _) = sub.next().await.unwrap().unwrap();
        assert_eq!(seq, i as u64 + 1);
    }
    // latest_seq keeps climbing (stable, monotonic) ...
    assert_eq!(wal.latest_seq().await.unwrap(), 200);
    // ... but retained records are bounded near zero, not 200.
    let retained = wal.shared.lock().unwrap().records.len();
    assert!(
        retained <= 1,
        "log should truncate behind the consumer, retained={retained}"
    );
}

#[tokio::test]
async fn mem_keeps_delivered_record_until_the_consumer_polls_again() {
    let wal = MemWal::new();
    let mut first = wal.subscribe(0).await.unwrap();

    wal.publish(WalRecord::new(create_entry("one")))
        .await
        .unwrap();
    let (first_seq, _) = first.next().await.unwrap().unwrap();
    assert_eq!(first_seq, 1);

    // The first consumer still owns the returned record. A publisher
    // must not discard it before another subscriber can install a replay
    // cursor for that record.
    wal.publish(WalRecord::new(create_entry("two")))
        .await
        .unwrap();
    let mut replay = wal.subscribe(0).await.unwrap();
    let (replayed_seq, _) = replay.next().await.unwrap().unwrap();
    assert_eq!(replayed_seq, 1, "the held record must remain replayable");

    // Once the first consumer asks for its next record, its previous
    // record is safe to truncate. Drop the replay cursor first so it no
    // longer pins record 1.
    drop(replay);
    let (second_seq, _) = first.next().await.unwrap().unwrap();
    assert_eq!(second_seq, 2);
    let state = wal.shared.lock().unwrap();
    assert_eq!(state.base, 1, "polling forward must advance truncation");
    assert_eq!(state.records.len(), 1);
}

#[tokio::test]
async fn mem_no_subscriber_retains_for_future_replay() {
    // With no subscribers, nothing is dropped — a late subscriber can
    // still replay from the beginning.
    let wal = MemWal::new();
    for i in 0..10u32 {
        wal.publish(WalRecord::new(create_entry(&format!("c{i}"))))
            .await
            .unwrap();
    }
    let mut sub = wal.subscribe(0).await.unwrap();
    let (first, _) = sub.next().await.unwrap().unwrap();
    assert_eq!(first, 1, "late subscriber must still replay from seq 1");
}

mod codec;

mod source_stage;
