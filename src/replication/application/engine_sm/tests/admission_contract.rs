use std::collections::{BTreeMap, HashMap};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use raft_runtime::{
    AdmissionPermit, HostConfig, Index, Membership, RaftHost, RaftStateMachine, RaftStore,
};
use tower::ServiceExt;

use crate::coordinator::WriteSink;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::{ChangeBudget, HARD_LIMIT};
use crate::ingest::domain::wal_record::WalRecord;
use crate::replication::application::engine_sm::write_sink::RaftWriteSink;
use crate::replication::application::engine_sm::EngineSm;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
};
use crate::storage::Engine;

// Draft bytes for src/raft_sm.rs's existing `#[cfg(test)] mod tests`.
// The host status is observed through its public router after apply cleanup.

const TEST_LIMIT: Duration = Duration::from_secs(2);

fn keyword_field() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn keyword_schema() -> CreateCollectionRequest {
    CreateCollectionRequest {
        fields: BTreeMap::from([("email".to_string(), keyword_field())]),
    }
}

fn index_entry(id: &str) -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: id.into(),
                field: "email".into(),
                value: FieldValue::String(format!("{id}@example.test")),
                version: None,
            }],
            request_id: None,
        },
    }
}

fn single_node(engine: Arc<Engine>) -> (tempfile::TempDir, Arc<RaftHost>, Arc<EngineSm>) {
    let dir = tempfile::tempdir().unwrap();
    let sm = EngineSm::new(engine, 0);
    let host = Arc::new(RaftHost::spawn(
        0,
        Membership {
            voters: vec![0],
            learners: vec![],
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
    ));
    (dir, host, sm)
}

async fn raft_last_index(host: &RaftHost) -> u64 {
    let response = host
        .router()
        .oneshot(
            axum::http::Request::builder()
                .uri("/raftz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["last_index"]
        .as_u64()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_full_local_admission_refuses_before_append_or_apply() {
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("docs", keyword_schema()).unwrap();
    let used = budget.snapshot().total;
    let blocker = budget.owner().try_reserve(HARD_LIMIT - used).unwrap();
    let (_dir, host, sm) = single_node(engine.clone());
    let sink = RaftWriteSink::new(host.clone(), sm.clone());
    let before = engine.metrics().segment_backpressure_total.get();

    let mut submit = tokio::spawn(async move { sink.submit(index_entry("full")).await });
    let initial = tokio::time::timeout(TEST_LIMIT, &mut submit).await;
    let finished_before_release = initial.is_ok();

    // Always clear the synthetic Full state before inspecting status or joining
    // a delayed task. A correct pre-publication refusal has already returned.
    drop(blocker);
    let after_release = if finished_before_release {
        None
    } else {
        let joined = tokio::time::timeout(TEST_LIMIT, &mut submit).await;
        if joined.is_err() {
            submit.abort();
            let _ = tokio::time::timeout(TEST_LIMIT, &mut submit).await;
        }
        Some(joined)
    };
    let applied = sm.applied_index();
    // The submit task has returned or has been bounded and aborted. Query the
    // public router only now; never query it while a gated callback holds the
    // host node lock.
    let appended = raft_last_index(host.as_ref()).await;
    host.shutdown().await.unwrap();

    assert!(
        finished_before_release,
        "pre-publication Full must return before capacity is released"
    );
    assert!(after_release.is_none());
    let error = initial.unwrap().unwrap().unwrap_err();
    let is_capacity = error.downcast_ref::<PendingChangeCapacity>().is_some();
    let response = axum::response::IntoResponse::into_response(crate::api::ApiErr::from(error));
    assert!(is_capacity);
    assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    assert_eq!(
        engine.metrics().segment_backpressure_total.get(),
        before + 1,
        "one final raft pre-publication refusal increments once"
    );
    assert_eq!(appended, 0, "429 refusal must precede Raft append");
    assert_eq!(applied, 0, "429 refusal must precede state-machine apply");
    assert_eq!(budget.snapshot().reserved, 0);
}

fn wait_for_capacity_waiter(budget: &ChangeBudget) -> bool {
    let deadline = Instant::now() + TEST_LIMIT;
    while Instant::now() < deadline {
        if budget.has_capacity_waiters() {
            return true;
        }
        std::thread::yield_now();
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_raft_head_waits_without_holding_capture_barrier() {
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("docs", keyword_schema()).unwrap();
    // The generic committed wait below must be reserved-only. Otherwise
    // the new owner can correctly publish this schema while the test is
    // checking its capture-barrier behavior.
    let schema_dir = tempfile::tempdir().unwrap();
    let schema_store = crate::segment_rdb::SegmentRdbStore::new(schema_dir.path()).unwrap();
    schema_store.save(&engine, 0).unwrap();
    assert_eq!(
        budget.snapshot().active,
        0,
        "schema checkpoint must freeze fixture work"
    );
    let used = budget.snapshot().total;
    let blocker = budget.owner().try_reserve(HARD_LIMIT - used).unwrap();
    let sm = EngineSm::new(engine.clone(), 0);
    let command = WalRecord::new(index_entry("committed")).encode().unwrap();

    let applying = {
        let sm = sm.clone();
        tokio::task::spawn_blocking(move || sm.apply(1, &command))
    };
    let waiter_seen = wait_for_capacity_waiter(&budget);
    let owner_started = engine.layer_maintenance.owner().is_some();

    // This probe is the lease assertion. It must complete before capacity is
    // released; a wait entered after CaptureBarrier::apply would block it.
    let (capture_tx, capture_rx) = mpsc::channel();
    let probe_engine = engine.clone();
    let probe = std::thread::spawn(move || {
        let stamp = probe_engine
            .capture_barrier
            .capture(0)
            .map(|lease| lease.stamp().sequence);
        let _ = capture_tx.send(stamp);
    });
    let capture_before_release = capture_rx.recv_timeout(TEST_LIMIT);
    let applied_before_release = sm.applied_index();

    // Release and join before assertions. A correct implementation must wake,
    // consume the original committed command, and finish the blocking worker.
    drop(blocker);
    let apply_result = tokio::time::timeout(TEST_LIMIT, applying).await;
    probe.join().unwrap();
    let applied_after_release = sm.applied_index();
    let capture_cut = engine.capture_barrier.capture(0).unwrap().stamp().sequence;

    assert!(
        waiter_seen,
        "committed source must register a capacity wait"
    );
    assert!(
        owner_started,
        "committed Raft admission must start its independent capacity owner before waiting"
    );
    assert_eq!(
        capture_before_release.unwrap().unwrap(),
        0,
        "capture must complete while committed admission waits"
    );
    assert_eq!(applied_before_release, 0);
    assert!(apply_result.unwrap().unwrap().is_ok());
    assert_eq!(applied_after_release, 1);
    assert_eq!(
        capture_cut, 1,
        "record state and watermark share one interval"
    );
    assert!(budget.high_water_bytes() <= HARD_LIMIT);
}

#[derive(Default)]
struct ApplyGate {
    state: Mutex<(bool, bool)>, // (entered, released)
    changed: Condvar,
}

impl ApplyGate {
    fn block_apply(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn wait_entered(&self) -> bool {
        let deadline = Instant::now() + TEST_LIMIT;
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timed) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timed.timed_out() {
                return state.0;
            }
        }
        true
    }
    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

/// A real Raft state-machine wrapper. It stops only after the host has invoked
/// apply, which is after the host allocated/appended the index. It does not
/// manufacture an apply result or poll host state while the node lock is held.
struct GatedSm {
    inner: Arc<EngineSm>,
    gate: Arc<ApplyGate>,
}

impl RaftStateMachine for GatedSm {
    fn admit_proposal(&self, command: &[u8]) -> anyhow::Result<Option<AdmissionPermit>> {
        self.inner.admit_proposal(command)
    }
    fn apply_admitted(
        &self,
        index: Index,
        command: &[u8],
        permit: Option<AdmissionPermit>,
    ) -> anyhow::Result<()> {
        self.gate.block_apply();
        self.inner.apply_admitted(index, command, permit)
    }
    fn apply(&self, index: Index, command: &[u8]) -> anyhow::Result<()> {
        self.apply_admitted(index, command, None)
    }
    fn snapshot(&self, writer: &mut dyn std::io::Write) -> anyhow::Result<()> {
        self.inner.snapshot(writer)
    }
    fn restore(&self, reader: &mut dyn std::io::Read) -> anyhow::Result<()> {
        self.inner.restore(reader)
    }
    fn applied_index(&self) -> Index {
        self.inner.applied_index()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_raft_submit_keeps_reservation_through_actual_apply() {
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("docs", keyword_schema()).unwrap();
    let baseline = budget.snapshot().total;
    let inner = EngineSm::new(engine.clone(), 0);
    let gate = Arc::new(ApplyGate::default());
    let dir = tempfile::tempdir().unwrap();
    let host = Arc::new(RaftHost::spawn(
        0,
        Membership {
            voters: vec![0],
            learners: vec![],
        },
        HashMap::new(),
        RaftStore::open(
            dir.path().to_str().unwrap(),
            0,
            raft_runtime::FsyncPolicy::Os,
        )
        .unwrap(),
        Arc::new(GatedSm {
            inner: inner.clone(),
            gate: gate.clone(),
        }),
        HostConfig::default(),
    ));
    let sink = Arc::new(RaftWriteSink::new(host.clone(), inner.clone()));

    let mut submit = tokio::spawn({
        let sink = sink.clone();
        async move { sink.submit(index_entry("cancelled")).await }
    });
    let entered = gate.wait_entered();
    let reserved_before_cancel = budget.snapshot().reserved;
    submit.abort();
    let reserved_after_cancel = budget.snapshot().reserved;

    // Keep the gate closed while the caller is cancelled. Then release it and
    // bounded-join both the caller and the real host apply before assertions.
    gate.release();
    let submit_joined = tokio::time::timeout(TEST_LIMIT, &mut submit).await;
    let applied = tokio::time::timeout(TEST_LIMIT, async {
        while inner.applied_index() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let reserved_after_apply = budget.snapshot().reserved;
    host.shutdown().await.unwrap();

    assert!(
        entered,
        "gate proves append reached the real host apply callback"
    );
    assert!(
        reserved_before_cancel > 0,
        "caller cancellation must not release post-append reservation"
    );
    assert!(
        reserved_after_cancel > 0,
        "reservation must survive cancellation while the real host apply is gated"
    );
    assert!(
        submit_joined.is_ok(),
        "cancelled caller task must finish after gate cleanup"
    );
    assert!(applied.is_ok());
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 1);
    assert_eq!(reserved_after_apply, 0);
    assert!(budget.snapshot().active >= baseline);
}

// Required commands after root places these bytes in raft_sm.rs:
// cargo test -p lumen --lib raft_full_local_admission_refuses_before_append_or_apply
// cargo test -p lumen --lib committed_raft_head_waits_without_holding_capture_barrier
// cargo test -p lumen --lib cancelled_raft_submit_keeps_reservation_through_actual_apply
// cargo test -p lumen --lib
// cargo test -p lumen
