use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::change_journal::ChangeJournal;

struct NotClone {
    drops: Arc<AtomicUsize>,
    value: u32,
}

impl Drop for NotClone {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn value(drops: &Arc<AtomicUsize>, value: u32) -> Arc<NotClone> {
    Arc::new(NotClone {
        drops: drops.clone(),
        value,
    })
}

struct ChargeObservedPayload {
    budget: ChangeBudget,
    observed_total: Arc<AtomicUsize>,
}

impl Drop for ChargeObservedPayload {
    fn drop(&mut self) {
        self.observed_total
            .store(self.budget.snapshot().total, Ordering::SeqCst);
    }
}

#[test]
fn replacing_a_file_owner_can_defer_its_destruction_until_after_apply() {
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record("vector".into(), "id".into(), 1, Some(value(&drops, 1)));
    let previous = journal
        .replace_charged(
            "vector".into(),
            "id".into(),
            2,
            Some(value(&drops, 2)),
            None,
        )
        .expect("existing payload owner returned");
    assert_eq!(
        drops.load(Ordering::Relaxed),
        0,
        "replacement must not destroy the old file owner inside apply"
    );
    let frozen = journal.freeze();
    assert_eq!(
        frozen.row("vector", "id").unwrap().value().unwrap().value,
        2
    );
    assert_eq!(previous.value().unwrap().value, 1);
    drop(previous);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn freeze_moves_non_clone_value_without_copying_it() {
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record("f".into(), "b".into(), 1, Some(value(&drops, 7)));
    let frozen = journal.freeze();
    let row = frozen.row("f", "b").unwrap();
    assert_eq!(row.value().unwrap().value, 7);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(frozen);
    assert_eq!(
        drops.load(Ordering::Relaxed),
        0,
        "journal retains failed capture"
    );
}

#[test]
fn overwrite_and_delete_after_freeze_keep_old_capture() {
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record("f".into(), "id".into(), 1, Some(value(&drops, 1)));
    let frozen = journal.freeze();
    journal.record("f".into(), "id".into(), 2, Some(value(&drops, 2)));
    journal.record("f".into(), "gone".into(), 3, None);
    assert_eq!(frozen.row("f", "id").unwrap().value().unwrap().value, 1);
    assert!(journal.active_revision("f", "id").is_some_and(|r| r == 2));
    assert!(journal.active_revision("f", "gone").is_some_and(|r| r == 3));
}

#[test]
fn acknowledgement_releases_frozen_only_and_keeps_later_active() {
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record("f".into(), "id".into(), 1, Some(value(&drops, 1)));
    let frozen = journal.freeze();
    journal.record("f".into(), "id".into(), 2, Some(value(&drops, 2)));
    assert!(journal.acknowledge(&frozen));
    assert_eq!(journal.active_revision("f", "id"), Some(2));
    assert_eq!(journal.active_len(), 1);
    assert_eq!(frozen.row("f", "id").unwrap().revision(), 1);
}

#[test]
fn cloned_frozen_handles_share_one_non_clone_payload() {
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record("f".into(), "id".into(), 1, Some(value(&drops, 9)));
    let one = journal.freeze();
    let two = one.clone();
    assert_eq!(one.id(), two.id());
    assert_eq!(one.row("f", "id").unwrap().value().unwrap().value, 9);
    assert!(journal.acknowledge(&one));
    drop(one);
    assert_eq!(
        drops.load(Ordering::Relaxed),
        0,
        "second retry handle owns payload"
    );
    drop(two);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn charged_non_clone_payload_moves_to_frozen_capture_without_copying() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let drops = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(value(&drops, 7)),
        Some(owner.try_reserve(7).unwrap().commit_retained().unwrap()),
    );
    let frozen = journal.freeze();
    assert_eq!(frozen.row("f", "id").unwrap().value().unwrap().value, 7);
    assert_eq!(budget.snapshot().total, 7);
    assert!(journal.acknowledge(&frozen));
    drop(frozen);
    assert!(drops.load(Ordering::Relaxed) > 0);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn acknowledge_keeps_one_charged_capture_until_all_retry_handles_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(Arc::new(1)),
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let one = journal.freeze();
    let two = one.clone();
    assert!(journal.acknowledge(&one));
    drop(one);
    assert_eq!(budget.snapshot().total, 5);
    drop(two);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn overwrite_after_freeze_leaves_the_later_active_charge_intact() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(Arc::new(1)),
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let frozen = journal.freeze();
    let _budget_frozen = owner.freeze().unwrap();
    journal.record_charged(
        "f".into(),
        "id".into(),
        2,
        Some(Arc::new(2)),
        Some(owner.try_reserve(6).unwrap().commit_retained().unwrap()),
    );
    assert_eq!(frozen.row("f", "id").unwrap().revision(), 1);
    assert_eq!(journal.active_revision("f", "id"), Some(2));
    assert_eq!(budget.snapshot().active, 6);
    assert_eq!(budget.snapshot().frozen, 5);
    assert!(journal.acknowledge(&frozen));
    drop(frozen);
    assert_eq!(budget.snapshot().active, 6);
    assert_eq!(budget.snapshot().total, 6);
}

#[test]
fn frozen_capture_keeps_its_charge_after_the_journal_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(Arc::new(1)),
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let frozen = journal.freeze();
    drop(journal);
    assert_eq!(budget.snapshot().total, 5);
    drop(frozen);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn row_drops_payload_before_its_retained_charge() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let observed_total = Arc::new(AtomicUsize::new(0));
    let journal = ChangeJournal::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(Arc::new(ChargeObservedPayload {
            budget: budget.clone(),
            observed_total: observed_total.clone(),
        })),
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let frozen = journal.freeze();
    assert!(journal.acknowledge(&frozen));
    drop(journal);
    drop(frozen);
    assert_eq!(observed_total.load(Ordering::SeqCst), 5);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn rows_from_one_record_share_one_retained_charge() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    let charge = owner.try_reserve(5).unwrap().commit_retained().unwrap();
    journal.record_charged(
        "a".into(),
        "id".into(),
        1,
        Some(Arc::new(1)),
        Some(charge.clone()),
    );
    journal.record_charged("b".into(), "id".into(), 1, Some(Arc::new(2)), Some(charge));
    assert_eq!(budget.snapshot().total, 5);
    let frozen = journal.freeze();
    assert!(journal.acknowledge(&frozen));
    drop(frozen);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn escaped_frozen_value_keeps_its_charge_after_journal_and_capture_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        Some(Arc::new(1)),
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let frozen = journal.freeze();
    let escaped = frozen.row("f", "id").unwrap().value().unwrap().clone();
    assert!(escaped.same_identity(frozen.row("f", "id").unwrap().value().unwrap()));
    assert!(journal.acknowledge(&frozen));
    drop(journal);
    drop(frozen);
    assert_eq!(budget.snapshot().total, 5);
    drop(escaped);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn tombstone_charge_lives_until_the_last_frozen_capture_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = ChangeJournal::<u8>::new();
    journal.record_charged(
        "f".into(),
        "id".into(),
        1,
        None,
        Some(owner.try_reserve(5).unwrap().commit_retained().unwrap()),
    );
    let one = journal.freeze();
    let two = one.clone();
    assert!(one.row("f", "id").unwrap().is_delete());
    assert!(journal.acknowledge(&one));
    drop(journal);
    drop(one);
    assert_eq!(budget.snapshot().total, 5);
    drop(two);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn rows_are_field_and_stable_id_ordered_and_deletes_are_explicit() {
    let journal = ChangeJournal::<u8>::new();
    journal.record("z".into(), "b".into(), 1, Some(Arc::new(1)));
    journal.record("a".into(), "z".into(), 2, None);
    journal.record("a".into(), "a".into(), 3, Some(Arc::new(2)));
    let frozen = journal.freeze();
    let got: Vec<_> = frozen
        .rows()
        .map(|(f, id, row)| (f, id, row.revision(), row.is_delete()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("a", "a", 3, false),
            ("a", "z", 2, true),
            ("z", "b", 1, false)
        ]
    );
}
