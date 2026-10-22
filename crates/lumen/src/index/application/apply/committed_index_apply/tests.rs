use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;

use crate::index::application::apply::committed_index_apply::scalar_bytes;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest, IndexResponse};
use crate::shared_kernel::types::schema::CreateCollectionRequest;

fn engine() -> Arc<Engine> {
    let engine = Arc::new(Engine::with_change_budget(
        crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(32 * 1024 * 1024),
    ));
    engine
        .create_collection_inner(
            "docs",
            CreateCollectionRequest {
                fields: serde_json::from_value(serde_json::json!({
                    "kw": {"type":"keyword"}, "n":{"type":"number"}, "tags":{"type":"set"}, "body":{"type":"text", "analyzer":"whitespace_lower"}
                }))
                .unwrap(),
            },
        )
        .unwrap();
    engine
}
fn item(id: &str, field: &str, value: FieldValue, version: Option<u64>) -> IndexItem {
    IndexItem {
        external_id: id.into(),
        field: field.into(),
        value,
        version,
    }
}
fn request(items: Vec<IndexItem>, id: Option<&str>) -> IndexRequest {
    IndexRequest {
        items,
        request_id: id.map(str::to_owned),
    }
}
fn borrowed(engine: &Engine, req: &IndexRequest, sequence: u64) -> Result<IndexResponse> {
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: req.clone(),
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let mut outcome = None;
    assert!(engine
        .try_apply_committed_index(&scanner, sequence, |apply, result| {
            apply.advance_sequence(sequence);
            outcome = Some(result);
        })
        .unwrap());
    match outcome.expect("record must complete")? {
        ApplyOutcome::Indexed(response) => Ok(response),
        _ => panic!("Index result expected"),
    }
}
fn state_fingerprint(engine: &Engine) -> serde_json::Value {
    let state = engine.state.read().unwrap();
    let coll = &state.collections["docs"];
    let rows: Vec<_> = coll
        .interner
        .to_eid
        .iter()
        .enumerate()
        .map(|(id, eid)| {
            let id = id as u32;
            let kw = match &coll.fields["kw"] {
                FieldIndex::Keyword(k) => k.keyword_at(id),
                _ => unreachable!(),
            };
            let n = match &coll.fields["n"] {
                FieldIndex::Number(n) => n.number_at(id).map(SortableF64::to_f64),
                _ => unreachable!(),
            };
            let tags = match &coll.fields["tags"] {
                FieldIndex::Set(s) => s.set_members(id),
                _ => unreachable!(),
            };
            let coverage: Vec<_> = ["kw", "n", "tags"]
                .into_iter()
                .filter(|field| {
                    coll.eid_fields
                        .get(&id)
                        .is_some_and(|set| set.contains(field))
                })
                .collect();
            serde_json::json!([eid, kw, n, tags, coverage, coll.cell_versions.get(&id)])
        })
        .collect();
    let sizes: BTreeMap<_, _> = ["kw", "n", "tags"]
        .into_iter()
        .map(|name| (name.to_owned(), scalar_bytes(&coll.fields[name])))
        .collect();
    serde_json::json!({"rows":rows,"bytes":sizes,"data_version":coll.data_version,
        "last_indexed":coll.last_indexed_at.is_some(),
        "requests":coll.seen_requests.iter().map(|(id, _)|id).collect::<Vec<_>>()})
}
fn compare_apply(actual: &Engine, reference: &Engine, req: &IndexRequest, sequence: u64) {
    let got = borrowed(actual, req, sequence);
    let wanted = reference.index_inner("docs", req.clone(), None, None);
    match (got, wanted) {
        (Ok(got), Ok(wanted)) => assert_eq!(
            serde_json::to_value(got).unwrap(),
            serde_json::to_value(wanted).unwrap()
        ),
        (Err(got), Err(wanted)) => assert_eq!(got.to_string(), wanted.to_string()),
        (got, wanted) => panic!("borrowed/owned outcome differs: {got:?} / {wanted:?}"),
    }
    assert_eq!(state_fingerprint(actual), state_fingerprint(reference));
}

mod borrowed_vector_apply_tests;
mod hash;
mod scalar;
mod text;
