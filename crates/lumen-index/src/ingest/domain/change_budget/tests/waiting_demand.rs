use super::*;

fn until(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::yield_now();
    }
    true
}

#[test]
fn blocked_reservation_rearms_after_a_publication_that_does_not_free_enough() {
    let budget = ChangeBudget::with_hard_limit(100);
    let owner = Arc::new(budget.owner());
    let _first = owner.try_reserve(10).unwrap().commit().unwrap();
    let captured = owner.freeze().unwrap();
    let _later = owner.try_reserve(70).unwrap().commit().unwrap();
    let served = owner.request_checkpoint_revision().unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let owner = owner.clone();
        std::thread::spawn(move || {
            let reservation = owner.wait_reserve(40).unwrap();
            drop(reservation);
            done_tx.send(()).unwrap();
        })
    };
    assert!(until(|| budget.has_capacity_waiters()));
    owner.publish_through(&captured).unwrap();
    owner.consume_checkpoint_request(Some(served));
    let rearmed = until(|| {
        budget
            .snapshot()
            .checkpoint_request_revision
            .is_some_and(|revision| revision > served)
    });

    // Always release the real remaining work and join the waiter, including
    // the before-fix failure path. A failed oracle must not leak a thread.
    let remaining = owner.freeze().unwrap();
    owner.publish_through(&remaining).unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert!(
        rearmed,
        "a still-blocked reservation must request actual remaining work after the earlier request is consumed"
    );
}

#[test]
fn blocked_growth_rearms_after_a_publication_that_does_not_free_enough() {
    let budget = ChangeBudget::with_hard_limit(100);
    let owner = Arc::new(budget.owner());
    let _first = owner.try_reserve(10).unwrap().commit().unwrap();
    let captured = owner.freeze().unwrap();
    let _later = owner.try_reserve(60).unwrap().commit().unwrap();
    let mut reservation = owner.try_reserve(10).unwrap();
    let served = owner.request_checkpoint_revision().unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        reservation.wait_grow_to(50).unwrap();
        drop(reservation);
        done_tx.send(()).unwrap();
    });
    assert!(until(|| budget.has_capacity_waiters()));
    owner.publish_through(&captured).unwrap();
    owner.consume_checkpoint_request(Some(served));
    let rearmed = until(|| {
        budget
            .snapshot()
            .checkpoint_request_revision
            .is_some_and(|revision| revision > served)
    });
    let remaining = owner.freeze().unwrap();
    owner.publish_through(&remaining).unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert!(rearmed, "still-blocked growth needs a new relief request");
}

#[test]
fn successful_blocking_admission_and_growth_do_not_request_a_checkpoint() {
    let budget = ChangeBudget::with_hard_limit(100);
    let owner = budget.owner();
    let mut reservation = owner.wait_reserve(10).unwrap();
    reservation.wait_grow_to(20).unwrap();
    assert_eq!(budget.snapshot().checkpoint_request_revision, None);
    assert!(!budget.has_capacity_waiters());
    assert_eq!(budget.snapshot().reserved, 20);
}

#[test]
fn reservations_without_publishable_work_do_not_request_empty_checkpoints() {
    let budget = ChangeBudget::with_hard_limit(100);
    let owner = Arc::new(budget.owner());
    let held = owner.try_reserve(90).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let owner = owner.clone();
        std::thread::spawn(move || {
            let reservation = owner.wait_reserve(20).unwrap();
            drop(reservation);
            done_tx.send(()).unwrap();
        })
    };
    let waiting = until(|| budget.has_capacity_waiters());
    let requested = budget.snapshot().checkpoint_request_revision;
    drop(held);
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert!(waiting);
    assert_eq!(requested, None);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn another_owner_can_relieve_a_wait_without_claiming_work_on_the_empty_owner() {
    let budget = ChangeBudget::with_hard_limit(100);
    let producer = budget.owner();
    let waiter_owner = Arc::new(budget.owner());
    let _charge = producer.try_reserve(90).unwrap().commit().unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let owner = waiter_owner.clone();
        std::thread::spawn(move || {
            let reservation = owner.wait_reserve(20).unwrap();
            drop(reservation);
            done_tx.send(()).unwrap();
        })
    };
    let waiting = until(|| budget.has_capacity_waiters());
    let request = budget.snapshot().checkpoint_request_revision;
    let empty_request = waiter_owner
        .capacity_state()
        .unwrap()
        .checkpoint_request_revision;
    let captured = producer.freeze().unwrap();
    producer.publish_through(&captured).unwrap();
    producer.consume_checkpoint_request(request);
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert!(waiting);
    assert!(request.is_some());
    assert_eq!(empty_request, None);
    assert_eq!(budget.snapshot().checkpoint_request_revision, None);
    assert_eq!(budget.snapshot().total, 0);
}
