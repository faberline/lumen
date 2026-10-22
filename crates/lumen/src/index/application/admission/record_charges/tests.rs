use crate::index::application::admission::record_charges::RecordChargeJournal;
use crate::ingest::domain::change_budget::ChangeBudget;

#[test]
fn acknowledged_cut_keeps_one_record_charge_until_its_last_capture_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = RecordChargeJournal::new();
    journal.retain(owner.try_reserve(7).unwrap().commit_retained().unwrap());
    let one = journal.freeze();
    let two = one.clone();
    assert!(journal.acknowledge_through(&one));
    drop(one);
    assert_eq!(budget.snapshot().total, 7);
    drop(two);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn restore_detachment_keeps_old_frozen_credit_until_last_capture_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let journal = RecordChargeJournal::new();
    journal.retain(owner.try_reserve(7).unwrap().commit_retained().unwrap());
    let frozen = journal.freeze();
    let stale_clone = frozen.clone();

    let detached = journal.discard_for_restore();
    assert!(
        !journal.acknowledge_through(&frozen),
        "a detached cut no longer belongs to the live journal"
    );
    drop(frozen);
    assert_eq!(budget.snapshot().total, 7);
    drop(detached);
    assert_eq!(
        budget.snapshot().total,
        7,
        "the stale retry handle still owns the actual charge"
    );
    drop(stale_clone);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn stale_restore_capture_cannot_acknowledge_a_newer_live_record() {
    let budget = ChangeBudget::with_hard_limit(32);
    let owner = budget.owner();
    let journal = RecordChargeJournal::new();
    journal.retain(owner.try_reserve(5).unwrap().commit_retained().unwrap());
    let stale = journal.freeze();
    let detached = journal.discard_for_restore();

    journal.retain(owner.try_reserve(7).unwrap().commit_retained().unwrap());
    let current = journal.freeze();
    let current_clone = current.clone();
    assert!(
        !journal.acknowledge_through(&stale),
        "a stale cut must not remove the new record batch"
    );
    drop(current);
    drop(detached);
    drop(stale);
    assert_eq!(
        budget.snapshot().total,
        7,
        "the live journal still owns its newer record charge"
    );
    assert!(journal.acknowledge_through(&current_clone));
    drop(current_clone);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn foreign_journal_cannot_acknowledge_a_detached_capture() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let source = RecordChargeJournal::new();
    let foreign = RecordChargeJournal::new();
    source.retain(owner.try_reserve(7).unwrap().commit_retained().unwrap());
    let capture = source.freeze();
    let detached = source.discard_for_restore();

    assert!(!foreign.acknowledge_through(&capture));
    drop(capture);
    assert_eq!(budget.snapshot().total, 7);
    drop(detached);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn adopted_candidate_metadata_is_released_by_live_cut_after_last_capture_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let candidate_owner = budget.owner();
    let candidate = RecordChargeJournal::new();
    candidate.retain(
        candidate_owner
            .try_reserve(7)
            .unwrap()
            .commit_retained()
            .unwrap(),
    );
    let candidate_capture = candidate.freeze();
    let detached = candidate.discard_for_restore();

    let live = RecordChargeJournal::new();
    live.adopt_for_restore(detached);
    let live_capture = live.freeze();
    let live_clone = live_capture.clone();
    assert!(live.acknowledge_through(&live_capture));
    drop(live_capture);
    drop(live_clone);
    assert_eq!(
        budget.snapshot().total,
        7,
        "the candidate retry handle remains the last payload owner"
    );
    drop(candidate_capture);
    assert_eq!(budget.snapshot().total, 0);
}
