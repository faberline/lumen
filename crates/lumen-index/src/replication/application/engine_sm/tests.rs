use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use raft_runtime::{HostConfig, Membership, RaftHost, RaftStateMachine, RaftStore};

use crate::index::application::engine::{raft_dispatch::ApplyOutcome, Engine};
use crate::ingest::domain::wal_record::WalRecord;
use crate::replication::application::engine_sm::EngineSm;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
};

fn number_field() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Number,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

/// lumen's real `Engine`, driven through the shared `RaftHost`, applies
/// committed commands and returns the rich `ApplyOutcome` (read-your-write).
#[tokio::test]
async fn engine_applies_through_the_shared_host() {
    let tmp = std::env::temp_dir().join(format!("lumen-enginesm-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let engine = Arc::new(Engine::new());
    let sm = EngineSm::new(engine.clone(), 0);
    let host = RaftHost::spawn(
        0,
        Membership {
            voters: vec![0],
            learners: vec![],
        },
        HashMap::new(),
        RaftStore::open(tmp.to_str().unwrap(), 0, raft_runtime::FsyncPolicy::Os).unwrap(),
        sm.clone() as Arc<dyn RaftStateMachine>,
        HostConfig::default(),
    );

    // create a collection through consensus → rich Created outcome.
    let mut fields = BTreeMap::new();
    fields.insert("n".to_string(), number_field());
    let cmd = WalRecord::new(RaftLogEntry::CreateCollection {
        collection_id: "docs".into(),
        req: CreateCollectionRequest { fields },
    })
    .encode()
    .unwrap();
    let idx = host.propose(cmd).await.unwrap();
    assert_eq!(idx, 1);
    assert!(matches!(sm.take_outcome(1), Ok(ApplyOutcome::Created(_))));

    // index a doc → rich Indexed outcome, applied to the real engine.
    let cmd = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "d1".into(),
                field: "n".into(),
                value: FieldValue::Number(42.0),
                version: None,
            }],
            request_id: None,
        },
    })
    .encode()
    .unwrap();
    let idx = host.propose(cmd).await.unwrap();
    assert_eq!(idx, 2);
    match sm.take_outcome(2) {
        Ok(ApplyOutcome::Indexed(r)) => assert_eq!(r.indexed, 1),
        other => panic!("expected Indexed, got {other:?}"),
    }
    // RYW: the engine reflects the applied write immediately.
    assert_eq!(sm.applied_index(), 2);
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn truncate_control_record_applies_and_survives_a_raft_snapshot() {
    let engine = Arc::new(Engine::new());
    let sm = EngineSm::new(engine.clone(), 0);
    let mut fields = BTreeMap::new();
    fields.insert("n".to_string(), number_field());
    for (index, entry) in [
        RaftLogEntry::CreateCollection {
            collection_id: "docs".into(),
            req: CreateCollectionRequest { fields },
        },
        RaftLogEntry::Index {
            collection_id: "docs".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "old".into(),
                    field: "n".into(),
                    value: FieldValue::Number(42.0),
                    version: None,
                }],
                request_id: None,
            },
        },
        RaftLogEntry::TruncateDocs {
            collection_id: "docs".into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        sm.apply((index + 1) as u64, &WalRecord::new(entry).encode().unwrap())
            .unwrap();
    }
    assert!(matches!(
        sm.take_outcome(3),
        Ok(ApplyOutcome::DocsTruncated)
    ));
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);

    let mut bytes = Vec::new();
    sm.snapshot(&mut bytes).unwrap();
    let restored_engine = Arc::new(Engine::new());
    let restored = EngineSm::new(restored_engine.clone(), 0);
    restored.restore(&mut bytes.as_slice()).unwrap();
    assert_eq!(restored.applied_index(), 3);
    assert_eq!(restored_engine.stats("docs").unwrap().documents_indexed, 0);
}

#[test]
fn unindex_control_record_applies_and_survives_a_raft_snapshot() {
    let engine = Arc::new(Engine::new());
    let sm = EngineSm::new(engine.clone(), 0);
    let mut fields = BTreeMap::new();
    fields.insert("n".to_string(), number_field());
    for (index, entry) in [
        RaftLogEntry::CreateCollection {
            collection_id: "docs".into(),
            req: CreateCollectionRequest { fields },
        },
        RaftLogEntry::Index {
            collection_id: "docs".into(),
            req: IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: "remove".into(),
                        field: "n".into(),
                        value: FieldValue::Number(1.0),
                        version: None,
                    },
                    IndexItem {
                        external_id: "keep".into(),
                        field: "n".into(),
                        value: FieldValue::Number(2.0),
                        version: None,
                    },
                ],
                request_id: None,
            },
        },
        RaftLogEntry::UnindexDocs {
            collection_id: "docs".into(),
            req: BatchUnindexDocsRequest {
                external_ids: vec!["remove".into()],
            },
        },
    ]
    .into_iter()
    .enumerate()
    {
        sm.apply((index + 1) as u64, &WalRecord::new(entry).encode().unwrap())
            .unwrap();
    }
    assert!(matches!(
        sm.take_outcome(3),
        Ok(ApplyOutcome::DocsUnindexed)
    ));
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 1);

    let mut bytes = Vec::new();
    sm.snapshot(&mut bytes).unwrap();
    let restored_engine = Arc::new(Engine::new());
    let restored = EngineSm::new(restored_engine.clone(), 0);
    restored.restore(&mut bytes.as_slice()).unwrap();
    assert_eq!(restored.applied_index(), 3);
    assert_eq!(restored_engine.stats("docs").unwrap().documents_indexed, 1);
}

mod admission_contract;
