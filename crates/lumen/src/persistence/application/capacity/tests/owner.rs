use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use crate::persistence::application::capacity::tests::{engine, sink, FailSync, HoldSync, Release};
use crate::persistence::application::capacity::worker::Owner;
use crate::persistence::application::capacity::Fallback;
use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[test]
fn failed_checkpoint_keeps_its_layer_slot_until_successful_retry() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailSync(AtomicBool::new(true))),
    )
    .unwrap();
    assert_eq!(engine.layer_maintenance.append_limit(), 16);
    assert!(store
        .save(&engine, 1)
        .unwrap_err()
        .to_string()
        .contains("activate segment generation"));
    assert_eq!(
        engine.layer_maintenance.append_limit(),
        15,
        "failed frozen ownership must still reserve the publication slot"
    );
    store.save(&engine, 1).unwrap();
    assert_eq!(
        engine.layer_maintenance.append_limit(),
        16,
        "successful publication and binding release the slot"
    );
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(cold.stats("docs").unwrap().documents_indexed, 1);
}

#[test]
fn fallback_attaches_relay_to_existing_configured_owner() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .expect("configured owner");
    let mut fallback = None;
    Fallback::ensure(&mut fallback, &engine, None).unwrap();
    let fallback = fallback.expect("existing owner must receive a relay");
    assert!(fallback.owner.is_none());
    assert!(fallback.relay.is_some());
    drop(fallback);
    let mut owner = owner;
    owner.join().unwrap();
}

#[test]
fn configured_owner_fences_detached_temporary_save_before_current() {
    let engine = engine();
    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Release(Some(release_tx));
    let old_store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            old_dir.path(),
            Arc::new(HoldSync {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(release_rx),
            }),
        )
        .unwrap(),
    );
    let mut old = Owner::start(sink(engine.clone(), old_store.clone()), false)
        .unwrap()
        .unwrap();
    let fenced = old_store
        .as_ref()
        .clone()
        .with_publication_fence(old.fence());
    let saving_engine = engine.clone();
    let saving = std::thread::spawn(move || fenced.save(&saving_engine, 1));
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let new_store = Arc::new(SegmentRdbStore::new(new_dir.path()).unwrap());
    let mut new = Owner::start(sink(engine.clone(), new_store.clone()), true)
        .unwrap()
        .unwrap();
    assert!(old.fence().acquire().is_err());
    assert!(
        Owner::start(sink(engine.clone(), old_store.clone()), false)
            .unwrap()
            .is_none(),
        "temporary owner cannot displace configured owner"
    );
    drop(release);
    let error = saving.join().unwrap().unwrap_err();
    assert!(
        format!("{error:#}").contains("superseded before CURRENT"),
        "{error:#}"
    );
    assert!(
        old_store.load_latest().unwrap().is_none(),
        "superseded output must not change CURRENT"
    );
    assert!(
        engine.capture_barrier.capture(0).is_ok(),
        "pre-publication supersession is not an uncertain commit"
    );
    new_store
        .as_ref()
        .clone()
        .with_publication_fence(new.fence())
        .save(&engine, 1)
        .unwrap();
    assert_eq!(new_store.load_latest().unwrap().unwrap().1, 1);
    old.join().unwrap();
    new.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_checkpoint_keeps_capacity_relief_on_its_publication_owner() {
    for configured in [false, true] {
        let engine = engine();
        let dir = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release = Release(Some(release_tx));
        let store = Arc::new(
            SegmentRdbStore::new_with_failure_injector(
                dir.path(),
                Arc::new(HoldSync {
                    entered: Mutex::new(Some(entered_tx)),
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        let sink = sink(engine.clone(), store.clone());
        let mut configured_owner =
            configured.then(|| Owner::start(sink.clone(), true).unwrap().unwrap());
        let prior = engine.layer_maintenance.owner();
        let saving =
            tokio::spawn(async move { CheckpointSink::checkpoint_now(sink.as_ref()).await });
        let entered =
            tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)))
                .await
                .unwrap();
        let during = engine.layer_maintenance.owner();
        let mut fallback = None;
        if entered.is_ok() && during.is_some() {
            Fallback::ensure(&mut fallback, &engine, None).unwrap();
        }
        let reused = fallback
            .as_ref()
            .is_some_and(|fallback| fallback.owner.is_none() && fallback.relay.is_some());
        drop(release);
        let result = saving.await.unwrap();
        drop(fallback);
        if let Some(owner) = &mut configured_owner {
            owner.join().unwrap();
        }
        entered.expect("manual checkpoint reached file sync");
        assert!(result.unwrap());
        assert!(
            during.is_some(),
            "capacity refusal during manual publication must reuse its owner"
        );
        assert!(
            reused,
            "capacity relief must not open a second checkpoint root"
        );
        if let Some(prior) = prior {
            assert!(
                Arc::ptr_eq(&prior, during.as_ref().unwrap()),
                "manual publication must preserve the configured driver's owner"
            );
        }
        assert_eq!(
            store
                .load_latest()
                .unwrap()
                .unwrap()
                .0
                .stats("docs")
                .unwrap()
                .documents_indexed,
            1
        );
    }
}

#[test]
fn replacement_owner_waits_until_current_and_live_binding_permit_releases() {
    let engine = engine();
    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let old_store = Arc::new(SegmentRdbStore::new(old_dir.path()).unwrap());
    let mut old = Owner::start(sink(engine.clone(), old_store), false)
        .unwrap()
        .unwrap();
    let publication = old.fence().acquire().unwrap();
    let new_sink = sink(
        engine.clone(),
        Arc::new(SegmentRdbStore::new(new_dir.path()).unwrap()),
    );
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let replacement = std::thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        let mut new = Owner::start(new_sink, true).unwrap().unwrap();
        done_tx.send(()).unwrap();
        new.join().unwrap();
    });
    attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = done_rx.recv_timeout(Duration::from_millis(50)).is_ok();
    drop(publication);
    if !early {
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    replacement.join().unwrap();
    old.join().unwrap();
    assert!(
        !early,
        "configured replacement crossed an active durable/live publication permit"
    );
}
