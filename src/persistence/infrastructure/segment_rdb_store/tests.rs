use crate::index::application::engine::Engine;
use crate::persistence::domain::generation_manifest::SegmentGenerationManifest;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    sync_directory, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, GENERATION_MANIFEST_SCHEMA_VERSION,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use storage_durable::{CurrentTarget, GenerationName};

struct DiagnosticEnvironment {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
}

impl DiagnosticEnvironment {
    fn set(enabled: bool) -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var_os("LUMEN_PERF_DIAGNOSTIC");
        if enabled {
            std::env::set_var("LUMEN_PERF_DIAGNOSTIC", "1");
        } else {
            std::env::remove_var("LUMEN_PERF_DIAGNOSTIC");
        }
        Self {
            _lock: lock,
            previous,
        }
    }
}

impl Drop for DiagnosticEnvironment {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            std::env::set_var("LUMEN_PERF_DIAGNOSTIC", previous);
        } else {
            std::env::remove_var("LUMEN_PERF_DIAGNOSTIC");
        }
    }
}

#[derive(Clone, Default)]
struct DiagnosticTraceWriter(Arc<Mutex<Vec<u8>>>);

struct DiagnosticTraceWriterGuard(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for DiagnosticTraceWriterGuard {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for DiagnosticTraceWriter {
    type Writer = DiagnosticTraceWriterGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        DiagnosticTraceWriterGuard(self.0.clone())
    }
}

impl DiagnosticTraceWriter {
    fn records(&self) -> Vec<serde_json::Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    query::{QueryNode, TermQuery},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
    search::SearchRequest,
};
use std::collections::BTreeMap;

fn kw_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
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
    );
    CreateCollectionRequest { fields }
}

fn index_kw(e: &Engine, eid: &str, v: &str) {
    index_kw_in(e, "u", eid, v);
}

fn index_kw_in(e: &Engine, collection: &str, eid: &str, v: &str) {
    e.index(
        collection,
        IndexRequest {
            items: vec![IndexItem {
                external_id: eid.into(),
                field: "email".into(),
                value: FieldValue::String(v.into()),
                version: None,
            }],
            request_id: None,
        },
    )
    .unwrap();
}

fn has_keyword(engine: &Engine, value: &str) -> bool {
    !engine
        .search(
            "u",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String(value.into()),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .is_empty()
}

fn current_generation(store: &SegmentRdbStore) -> GenerationName {
    match store.generations.read_current().unwrap() {
        CurrentTarget::Generation(name) => name,
        CurrentTarget::Empty => panic!("expected an active generation"),
    }
}

fn install_unpointed_generation(
    store: &SegmentRdbStore,
    engine: &Arc<Engine>,
    sequence: u64,
    previous: Option<&GenerationName>,
) -> GenerationName {
    let (revision, staged) = store.begin_next_generation(sequence).unwrap();
    let staging_path = staged.path().to_path_buf();
    engine.flush_to_segments(&staging_path, sequence).unwrap();
    write_generation_manifest(
        &staging_path,
        &SegmentGenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            checkpoint_sequence: sequence,
            revision,
            previous: previous.map(|name| name.as_str().to_owned()),
            next_collection_generation: 1,
            collections: Vec::new(),
        },
    )
    .unwrap();
    let name = staged.generation().clone();
    let target = store.generations.generation_path(&name);
    drop(staged);
    std::fs::rename(staging_path, &target).unwrap();
    sync_directory(&store.root).unwrap();
    name
}

fn first_segment_file(root: &Path) -> PathBuf {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("lseg") {
                return path;
            }
        }
    }
    panic!("expected a segment file under {}", root.display());
}

fn text_schema() -> CreateCollectionRequest {
    let mut schema = kw_schema();
    schema.fields.get_mut("email").unwrap().field_type = FieldType::Text;
    schema
}

/// Index one Text value the way a committed WAL record does: the value is
/// staged into its own row reader before apply, so it lands in
/// `TextIndex::staged_rows` instead of the in-RAM token map.
fn committed_text(engine: &Arc<Engine>, external_id: &str, value: &str, sequence: u64) {
    let bytes = crate::ingest::domain::wal_record::WalRecord::new(
        crate::shared_kernel::log_entry::RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: external_id.into(),
                    field: "email".into(),
                    value: FieldValue::String(value.into()),
                    version: None,
                }],
                request_id: None,
            },
        },
    )
    .encode()
    .unwrap();
    let scanner =
        crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner::parse(&bytes)
            .unwrap();
    let mut completed = None;
    assert!(engine
        .try_apply_committed_index(&scanner, sequence, |apply, outcome| {
            apply.advance_sequence(sequence);
            completed = Some(outcome);
        })
        .unwrap());
    completed.expect("committed Index must complete").unwrap();
}

mod background;

mod checkpoint_locks;

mod concurrency;

mod diagnostic;

mod field_deltas;

mod flat_layout;

mod generations;

mod integrity;

mod manifest_catalog;

mod merge_selection;

mod pending_retry;

mod root_inventory;

mod staged_validation;

mod telemetry;

mod unique_terms;
