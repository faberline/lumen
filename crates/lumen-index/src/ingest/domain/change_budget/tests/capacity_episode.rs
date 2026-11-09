use crate::ingest::domain::change_budget::{ActiveCharge, ChangeBudget, Owner};

fn cohort(owner: &Owner, episode: u64) -> ActiveCharge {
    let mut reservation = owner.try_reserve(1).unwrap();
    reservation.set_capacity_episode(episode);
    reservation.commit().unwrap()
}

#[test]
fn served_cohort_keeps_its_charge_without_creating_new_pressure_work() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let _charge = cohort(&owner, 7);
    let raw = owner.capacity_state().unwrap();
    assert!(raw.work_revision > 0);
    assert_eq!(owner.capacity_schedule_state().unwrap(), raw);
    owner.record_capacity_relief(Some(7));
    assert_eq!(owner.capacity_state().unwrap(), raw);
    assert_eq!(owner.capacity_schedule_state().unwrap().work_revision, 0);
    assert_eq!(
        budget.snapshot().total,
        1,
        "coalescing never removes a charge"
    );
}

#[test]
fn interleaved_independent_work_survives_cohort_completion() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let _first = owner.try_reserve(1).unwrap().commit().unwrap();
    let _waiting = cohort(&owner, 7);
    let _fresh = owner.try_reserve(1).unwrap().commit().unwrap();
    let independent = owner.capacity_state().unwrap().work_revision;
    let _late = cohort(&owner, 7);
    owner.record_capacity_relief(Some(7));
    assert!(owner.capacity_state().unwrap().work_revision > independent);
    assert_eq!(
        owner.capacity_schedule_state().unwrap().work_revision,
        independent
    );
    let _later = owner.try_reserve(1).unwrap().commit().unwrap();
    assert!(owner.capacity_schedule_state().unwrap().work_revision > independent);
    assert_eq!(budget.snapshot().total, 5);
}

#[test]
fn new_episode_stays_eligible_and_old_late_arrivals_do_not_accumulate() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let _old = cohort(&owner, 7);
    let _new = cohort(&owner, 8);
    let newest = owner.capacity_state().unwrap().work_revision;
    owner.record_capacity_relief(Some(7));
    assert_eq!(
        owner.capacity_schedule_state().unwrap().work_revision,
        newest
    );
    owner.record_capacity_relief(Some(8));
    let _late = cohort(&owner, 7);
    assert_eq!(owner.capacity_schedule_state().unwrap().work_revision, 0);
    assert!(budget.0.state.lock().unwrap().owners[&owner.id]
        .capacity_cohort_work
        .is_empty());
    assert_eq!(budget.snapshot().total, 3);
}

#[test]
fn transport_split_preserves_only_the_apply_episode() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let mut apply = owner.try_reserve(2).unwrap();
    apply.set_capacity_episode(7);
    let transport = apply.split_off(1);
    assert_eq!(apply.capacity_episode, Some(7));
    assert_eq!(transport.capacity_episode, None);
    owner.record_capacity_relief(Some(7));
    let _charge = apply.commit().unwrap();
    drop(transport);
    assert_eq!(owner.capacity_schedule_state().unwrap().work_revision, 0);
    assert_eq!(budget.snapshot().total, 1);
}

#[test]
fn retained_work_keeps_episode_through_repricing() {
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(2).unwrap();
    reservation.set_capacity_episode(7);
    reservation.try_grow_to(3).unwrap();
    reservation.shrink_to(2).unwrap();
    let _retained = reservation.commit_retained().unwrap();
    assert!(owner.capacity_schedule_state().unwrap().work_revision > 0);
    owner.record_capacity_relief(Some(7));
    assert_eq!(owner.capacity_schedule_state().unwrap().work_revision, 0);
    assert_eq!(budget.snapshot().total, 2);
}
