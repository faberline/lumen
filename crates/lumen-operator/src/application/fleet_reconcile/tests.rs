use std::collections::BTreeSet;
use std::time::Duration;

use super::schedule::*;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// A leader whose first full repair has already run at time zero.
fn leader(repair: Option<Duration>) -> Scheduler {
    let mut sched = Scheduler::new(repair);
    sched.set_leader(true, Duration::ZERO);
    let _ = sched.poll(Duration::ZERO);
    sched
}

fn names(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn a_fleet_change_queues_that_fleet() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::FleetChanged("a".into()));
    let batch = sched.poll(secs(1));
    assert_eq!(batch.fleets, names(&["a"]));
    assert!(!batch.full_repair);
}

#[test]
fn a_fleet_delete_queues_that_fleet() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::FleetDeleted("a".into()));
    let batch = sched.poll(secs(1));
    assert_eq!(batch.fleets, names(&["a"]));
    assert!(!batch.full_repair);
}

#[test]
fn a_child_change_or_delete_queues_its_owning_fleet() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::ChildChanged(Some("a".into())));
    sched.observe(FleetEvent::ChildDeleted(Some("b".into())));
    assert_eq!(sched.poll(secs(1)).fleets, names(&["a", "b"]));
}

#[test]
fn a_child_without_the_fleet_label_is_ignored() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::ChildChanged(None));
    sched.observe(FleetEvent::ChildDeleted(None));
    assert!(sched.poll(secs(1)).is_empty());
}

#[test]
fn a_second_poll_returns_nothing_until_new_events() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::FleetChanged("a".into()));
    let _ = sched.poll(secs(1));
    assert!(sched.poll(secs(2)).is_empty());
}

/// Steady state: no event and no repair is due, so no work and no full scan.
#[test]
fn no_events_means_no_work_before_the_repair_interval() {
    let mut sched = leader(Some(secs(600)));
    for t in [1, 30, 599] {
        assert!(sched.poll(secs(t)).is_empty(), "at {t}s");
    }
}

#[test]
fn repair_fires_once_when_the_interval_is_due() {
    let mut sched = leader(Some(secs(600)));
    assert!(sched.poll(secs(600)).full_repair);
    assert!(!sched.poll(secs(601)).full_repair);
    assert!(sched.poll(secs(1200)).full_repair);
}

/// A child `Lumen` of fleet `a` is deleted but its event is lost. Nothing
/// observes it; only the repair pass converges `a` again.
#[test]
fn a_lost_child_delete_event_is_repaired() {
    let mut sched = leader(Some(secs(600)));
    assert!(sched.poll(secs(599)).is_empty());
    assert!(sched.poll(secs(600)).full_repair);
}

/// Negative control: with repair off, the lost-event scenario above is never
/// recovered. If `poll` ignored `repair_every`, `a_lost_child_delete_event_is_repaired`
/// would still pass here and this test would fail.
#[test]
fn without_repair_a_lost_event_is_never_recovered() {
    let mut sched = leader(None);
    for t in [600, 6000, 600_000] {
        let batch = sched.poll(secs(t));
        assert!(!batch.full_repair, "at {t}s");
        assert!(batch.fleets.is_empty(), "at {t}s");
    }
}

#[test]
fn a_non_leader_converges_nothing() {
    let mut sched = Scheduler::new(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::FleetChanged("a".into()));
    assert!(sched.poll(secs(10_000)).is_empty());
}

#[test]
fn gaining_leadership_runs_one_full_repair_at_once() {
    let mut sched = Scheduler::new(Some(FLEET_REPAIR_INTERVAL));
    sched.set_leader(true, secs(50));
    assert!(sched.poll(secs(50)).full_repair);
    assert!(sched.poll(secs(51)).is_empty());
}

#[test]
fn losing_leadership_drops_queued_work() {
    let mut sched = leader(Some(FLEET_REPAIR_INTERVAL));
    sched.observe(FleetEvent::FleetChanged("a".into()));
    sched.set_leader(false, secs(5));
    sched.set_leader(true, secs(6));
    let batch = sched.poll(secs(6));
    assert!(batch.full_repair);
    assert!(batch.fleets.is_empty());
}
