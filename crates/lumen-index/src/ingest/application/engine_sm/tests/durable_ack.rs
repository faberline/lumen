//! Durable acknowledgement for the Raft backend (#63, slice 2).
//!
//! `RaftWriteSink::submit` returns `Ok` only after `RaftHost::propose` returned
//! a committed and applied index. These tests use a real `RaftHost`, a real
//! `RaftStore` on disk and the real `EngineSm`. They need no peer network.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use raft_runtime::{FsyncPolicy, HostConfig, Membership, RaftHost, RaftStateMachine, RaftStore};

use crate::index::application::engine::Engine;
use crate::ingest::application::engine_sm::write_sink::RaftWriteSink;
use crate::ingest::application::engine_sm::EngineSm;
use crate::ingest::application::write_coordinator::WriteSink;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
};

fn create_entry() -> RaftLogEntry {
    RaftLogEntry::CreateCollection {
        collection_id: "docs".into(),
        req: CreateCollectionRequest {
            fields: BTreeMap::from([(
                "email".to_string(),
                FieldSpec {
                    field_type: FieldType::Keyword,
                    analyzer: None,
                    multi: None,
                    dim: None,
                    metric: None,
                    backend: None,
                    quantize: None,
                },
            )]),
        },
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

/// Open (or reopen) a Raft node on `dir`. Reopening replays the committed log
/// into the fresh `engine` before `spawn` returns.
fn open_node(
    dir: &std::path::Path,
    voters: Vec<u64>,
    learners: Vec<u64>,
    propose_timeout: Duration,
) -> (Arc<Engine>, Arc<RaftHost>, Arc<EngineSm>) {
    let engine = Arc::new(Engine::new());
    let sm = EngineSm::new(engine.clone(), 0);
    let host = Arc::new(RaftHost::spawn(
        0,
        Membership { voters, learners },
        HashMap::new(),
        RaftStore::open(dir.to_str().unwrap(), 0, FsyncPolicy::Always).unwrap(),
        sm.clone() as Arc<dyn RaftStateMachine>,
        HostConfig {
            propose_timeout,
            ..HostConfig::default()
        },
    ));
    (engine, host, sm)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_acknowledged_write_is_applied_and_survives_a_cold_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, host, sm) = open_node(dir.path(), vec![0], vec![], Duration::from_secs(10));
    let sink = RaftWriteSink::new(host.clone(), sm.clone());

    sink.submit(create_entry()).await.unwrap();
    sink.submit(index_entry("u1")).await.unwrap();
    sink.submit(index_entry("u2")).await.unwrap();

    // Acknowledged means committed and applied before submit returned.
    assert_eq!(sm.applied_index(), 3);
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 2);

    host.shutdown().await.unwrap();
    drop(sink);
    drop(host);

    let (restarted, host, restarted_sm) =
        open_node(dir.path(), vec![0], vec![], Duration::from_secs(10));
    assert_eq!(restarted_sm.applied_index(), 3);
    assert_eq!(restarted.stats("docs").unwrap().documents_indexed, 2);
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_uncommitted_write_is_rejected_and_absent_after_a_cold_restart() {
    let dir = tempfile::tempdir().unwrap();
    // Node 0 is only a learner, so no leader can ever commit this entry.
    let (engine, host, sm) = open_node(dir.path(), vec![1], vec![0], Duration::from_millis(300));
    let sink = RaftWriteSink::new(host.clone(), sm.clone());

    let error = sink.submit(index_entry("lost")).await.unwrap_err();
    assert!(!error.to_string().is_empty());
    assert_eq!(sm.applied_index(), 0);
    assert!(engine.stats("docs").is_err());

    host.shutdown().await.unwrap();
    drop(sink);
    drop(host);

    let (restarted, host, restarted_sm) =
        open_node(dir.path(), vec![1], vec![0], Duration::from_millis(300));
    assert_eq!(restarted_sm.applied_index(), 0);
    assert!(restarted.stats("docs").is_err());
    host.shutdown().await.unwrap();
}
