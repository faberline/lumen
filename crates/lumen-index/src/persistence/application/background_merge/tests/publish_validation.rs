use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::tests::published_merge_jobs;
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::CHECKSUM_BYTES;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::{
    MergeObserver, MergePhase, SegmentRdbStore,
};
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{
    CreateCollectionRequest, FieldSpec, FieldType, VectorBackend, VectorMetric,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage_durable::CurrentTarget;

/// Payload bytes hashed between a merge's `BeforePublish` and
/// `AfterPublish`, one entry per published job. Both phases are observed on
/// the merge worker, which is the thread `CHECKSUM_BYTES` counts on.
#[derive(Default)]
struct PublishChecksumObserver {
    published: Mutex<Vec<u64>>,
}

impl MergeObserver for PublishChecksumObserver {
    fn observe(&self, phase: MergePhase) -> std::io::Result<()> {
        match phase {
            MergePhase::BeforePublish => CHECKSUM_BYTES.with(|bytes| bytes.set(0)),
            MergePhase::AfterPublish => self
                .published
                .lock()
                .unwrap()
                .push(CHECKSUM_BYTES.with(|bytes| bytes.get())),
            MergePhase::BeforeEncode => {}
        }
        Ok(())
    }
}

fn mixed_schema(backend: VectorBackend) -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "k".to_owned(),
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
    fields.insert(
        "v".to_owned(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(4),
            metric: Some(VectorMetric::L2),
            backend: Some(backend),
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn put_mixed_row(engine: &Engine, id: &str, sequence: u64) {
    let value = sequence as f32;
    engine
        .index(
            "hot",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: id.into(),
                        field: "k".into(),
                        value: FieldValue::String(format!("k-{sequence}")),
                        version: None,
                    },
                    IndexItem {
                        external_id: id.into(),
                        field: "v".into(),
                        value: FieldValue::Vector(vec![value, value + 1.0, 0.5, 0.25]),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
}

/// A merge's outputs are verified against their catalog entries in its
/// private scratch tree before it takes the save gate. Rehashing the same
/// hard-linked files while holding the gate made every checkpoint behind
/// the merge wait for work proportional to the merged field, so publication
/// must read no payload again. Cold recovery still verifies every payload.
fn assert_merge_publication_rehashes_nothing(backend: VectorBackend) {
    let directory = tempfile::tempdir().unwrap();
    let observer = Arc::new(PublishChecksumObserver::default());
    let store = SegmentRdbStore::with_merge_observer(directory.path(), observer.clone()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("hot", mixed_schema(backend))
        .unwrap();
    for row in 0..64 {
        put_mixed_row(&engine, &format!("seed-{row}"), 1);
    }
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=4u64 {
        put_mixed_row(&engine, &format!("hot-{sequence}"), sequence);
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(120))
        .expect("background merge worker must drain");

    let published = observer.published.lock().unwrap().clone();
    assert!(
        published_merge_jobs(&store) >= 1 && !published.is_empty(),
        "the fixture must publish a merge job"
    );
    assert!(
        published.iter().all(|bytes| *bytes == 0),
        "merge publication rehashed payload already verified in scratch: {published:?}"
    );

    let CurrentTarget::Generation(name) = store.generations.read_current().unwrap() else {
        panic!("the merge fixture must publish CURRENT")
    };
    let record = store.record_for_name(name).unwrap();
    CHECKSUM_BYTES.with(|bytes| bytes.set(0));
    validate_generation_layout(&record).expect("the published generation must stay valid");
    assert!(
        CHECKSUM_BYTES.with(|bytes| bytes.get()) > 0,
        "validation without a verified predecessor must hash every payload"
    );
    let (_cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 4);
}

#[test]
fn flat_cpu_merge_publication_rehashes_no_scratch_verified_output() {
    assert_merge_publication_rehashes_nothing(VectorBackend::FlatCpu);
}

#[test]
fn hnsw_merge_publication_rehashes_no_scratch_verified_output() {
    assert_merge_publication_rehashes_nothing(VectorBackend::HnswCpu);
}
