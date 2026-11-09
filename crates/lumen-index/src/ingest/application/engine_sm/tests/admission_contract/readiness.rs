use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_leader_readiness_does_not_append_apply_or_reserve() {
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("docs", keyword_schema()).unwrap();
    let before = budget.snapshot();
    let (_dir, host, sm) = single_node(engine);

    let ready = wait_for_raft_leader(&host, HostConfig::default().propose_timeout).await;
    let is_leader = host.is_leader().await;
    let leader = host.leader().await;
    let last_index = raft_last_index(&host).await;
    let applied = sm.applied_index();
    let after = budget.snapshot();
    host.shutdown().await.unwrap();

    assert!(
        ready,
        "readiness must observe the real host becoming leader"
    );
    assert!(is_leader);
    assert_eq!(leader, Some(0));
    assert_eq!(last_index, 0, "setup must not prime the log with a write");
    assert_eq!(applied, 0);
    assert_eq!(before.reserved, 0);
    assert_eq!(after.reserved, 0);
    assert_eq!(after.active, before.active);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_learner_cannot_satisfy_leader_readiness_or_admit_work() {
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("docs", keyword_schema()).unwrap();
    let before = budget.snapshot();
    let sm = EngineSm::new(engine, 0);
    let dir = tempfile::tempdir().unwrap();
    let host = RaftHost::spawn(
        0,
        Membership {
            voters: vec![1],
            learners: vec![0],
        },
        HashMap::new(),
        RaftStore::open(
            dir.path().to_str().unwrap(),
            0,
            raft_runtime::FsyncPolicy::Os,
        )
        .unwrap(),
        sm.clone() as Arc<dyn RaftStateMachine>,
        HostConfig::default(),
    );

    let ready = wait_for_raft_leader(&host, TEST_LIMIT).await;
    let is_leader = host.is_leader().await;
    let last_index = raft_last_index(&host).await;
    let applied = sm.applied_index();
    let after = budget.snapshot();
    host.shutdown().await.unwrap();

    assert!(
        !ready,
        "a real unready learner must time out, never pass setup"
    );
    assert!(!is_leader);
    assert_eq!(last_index, 0);
    assert_eq!(applied, 0);
    assert_eq!(before.reserved, 0);
    assert_eq!(after.reserved, 0);
    assert_eq!(after.active, before.active);
}
