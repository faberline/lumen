use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use storage_durable::{CommitStep, FailureInjector, FailurePoint};

use crate::api::RestoreSink;
use crate::index::application::engine::Engine;
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::ingest::application::write_coordinator::{SharedAof, WriteCoordinator};
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::persistence::application::restore::{RestorePublicationObserver, SegmentRestoreSink};
use crate::persistence::infrastructure::aof::replay::AofReader;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::schema::CreateCollectionRequest;

const WATERMARK: u64 = 7;

#[derive(Clone, Copy)]
enum InjectedAction {
    Error(io::ErrorKind),
    Panic,
}

#[derive(Default)]
struct FailAt(Mutex<Option<(CommitStep, InjectedAction)>>);

impl FailAt {
    fn arm(&self, step: CommitStep, kind: io::ErrorKind) {
        *self.0.lock().unwrap() = Some((step, InjectedAction::Error(kind)));
    }

    fn panic_at(&self, step: CommitStep) {
        *self.0.lock().unwrap() = Some((step, InjectedAction::Panic));
    }
}

impl FailureInjector for FailAt {
    fn check(&self, point: &FailurePoint) -> io::Result<()> {
        let action = self
            .0
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(step, _)| *step == point.step)
            .map(|(_, action)| *action);
        match action {
            Some(InjectedAction::Error(kind)) => Err(io::Error::from(kind)),
            Some(InjectedAction::Panic) => panic!("injected save panic"),
            None => Ok(()),
        }
    }
}

struct PauseAfterPublication {
    published: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl RestorePublicationObserver for PauseAfterPublication {
    async fn after_candidate_current_published(&self, _generation: String, _sequence: u64) {
        self.published.notify_one();
        self.resume.notified().await;
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    live: Arc<Engine>,
    store: Arc<SegmentRdbStore>,
    wal: Arc<MemWal>,
    writer: Arc<WriteCoordinator>,
    aof: SharedAof,
    old_name: storage_durable::GenerationName,
    old_current: Vec<u8>,
}

fn schema() -> CreateCollectionRequest {
    serde_json::from_value(serde_json::json!({
        "fields": { "value": { "type": "keyword" } }
    }))
    .expect("valid keyword schema")
}

fn engine_with(collection: &str) -> Arc<Engine> {
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(collection, schema())
        .expect("create fixture collection");
    engine
}

fn replacement_snapshot() -> SnapshotV1 {
    engine_with("restored")
        .snapshot()
        .expect("replacement snapshot")
}

fn assert_collections(engine: &Engine, expected: &[&str]) {
    let mut actual = engine.list_collections().expect("list collections");
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
}

fn current_bytes(root: &Path) -> Vec<u8> {
    std::fs::read(root.join("CURRENT")).expect("read CURRENT")
}

fn setup(injector: Option<Arc<dyn FailureInjector>>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(match injector {
        Some(i) => SegmentRdbStore::new_with_failure_injector(dir.path(), i).unwrap(),
        None => SegmentRdbStore::new(dir.path()).unwrap(),
    });
    let live = engine_with("old");
    let old_name = store
        .save_required(&live, WATERMARK)
        .expect("seed old CURRENT");
    let old_current = current_bytes(dir.path());
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(
            dir.path().join("aof.log"),
        )
        .unwrap(),
    ));
    let wal = Arc::new(MemWal::starting_at(WATERMARK));
    let writer = WriteCoordinator::start_from_with_aof(
        wal.clone(),
        Arc::clone(&live),
        WATERMARK,
        Arc::clone(&aof),
    );
    Fixture {
        dir,
        live,
        store,
        wal,
        writer,
        aof,
        old_name,
        old_current,
    }
}

#[tokio::test]
async fn success_commits_exact_watermark_and_next_write_advances() {
    let fixture = setup(None);
    let sink = SegmentRestoreSink::new(
        Arc::clone(&fixture.live),
        Arc::clone(&fixture.store),
        fixture.writer.clone(),
        fixture.aof.clone(),
    )
    .unwrap();

    sink.restore(replacement_snapshot()).await.unwrap();

    assert_collections(fixture.live.as_ref(), &["restored"]);
    let loaded = fixture
        .store
        .load_current_generation()
        .unwrap()
        .expect("restored CURRENT");
    assert_eq!(loaded.sequence, WATERMARK);
    assert_ne!(loaded.name, fixture.old_name);
    assert_collections(loaded.engine.as_ref(), &["restored"]);

    fixture
        .writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "after".to_string(),
            req: schema(),
        })
        .await
        .expect("first post-restore write");
    assert_eq!(fixture.writer.applied_seq(), WATERMARK + 1);
    assert_collections(fixture.live.as_ref(), &["after", "restored"]);
}

#[tokio::test]
async fn external_wal_record_waits_for_restore_activation_after_current_publication() {
    let fixture = setup(None);
    let observer = Arc::new(PauseAfterPublication {
        published: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store.clone(),
        fixture.writer.clone(),
        fixture.aof.clone(),
    )
    .unwrap()
    .with_publication_observer(observer.clone());

    let published = observer.published.notified();
    let restore = tokio::spawn(async move { sink.restore(replacement_snapshot()).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), published)
        .await
        .expect("restore never published its candidate CURRENT");

    let sequence = fixture
        .wal
        .publish(WalRecord::new(RaftLogEntry::CreateCollection {
            collection_id: "external".to_owned(),
            req: schema(),
        }))
        .await
        .unwrap();
    assert_eq!(sequence, WATERMARK + 1);
    for _ in 0..32 {
        tokio::task::yield_now().await;
        assert_eq!(
            fixture.writer.applied_seq(),
            WATERMARK,
            "external record applied while the restore still held activation"
        );
    }

    observer.resume.notify_one();
    restore.await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while fixture.writer.applied_seq() < sequence {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("external committed record must apply after restore activation");
    assert_eq!(fixture.writer.applied_seq(), sequence);
    assert_collections(fixture.live.as_ref(), &["external", "restored"]);
    let mut persisted = Vec::new();
    AofReader::replay(&fixture.dir.path().join("aof.log"), WATERMARK, |seq, _| {
        persisted.push(seq)
    })
    .unwrap();
    assert_eq!(persisted, vec![sequence]);
}

mod failures;
