use crate::index::application::engine::Engine;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal_source_stage::tests::stager;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    FieldValue, IndexItem, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};

// These cases intentionally use tightly
// sized source vectors so any decoder growth is visible in the final estimate.
fn tight<T>(mut values: Vec<T>) -> Vec<T> {
    values.shrink_to_fit();
    values
}

fn staged_bound(record: WalRecord) -> (usize, usize) {
    let stager = stager();
    let mut record = record;
    let staged = stager.stage(1, &mut record).unwrap();
    let bound = staged.decoded_owned_bytes();
    let decoded = staged.read(bound + staged.read_scratch_bytes()).unwrap();
    (bound, Engine::record_owned_bytes(&decoded.entry).unwrap())
}

#[test]
fn staged_decode_never_exceeds_source_bound_for_sequence_shapes() {
    for count in [1, 33, 5001] {
        let items = tight(
            (0..count)
                .map(|n| IndexItem {
                    external_id: format!("id-{n}"),
                    field: "v".into(),
                    value: FieldValue::String("x".into()),
                    version: None,
                })
                .collect(),
        );
        let (bound, decoded) = staged_bound(WalRecord::new(RaftLogEntry::Index {
            collection_id: "orders".into(),
            req: IndexRequest {
                items,
                request_id: None,
            },
        }));
        assert!(
            decoded <= bound,
            "Index items={count}: decoded={decoded} bound={bound}"
        );

        let values = tight((0..count).map(|n| format!("value-{n}")).collect());
        let (bound, decoded) = staged_bound(WalRecord::new(RaftLogEntry::Index {
            collection_id: "orders".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "id".into(),
                    field: "set".into(),
                    value: FieldValue::StringList(values),
                    version: None,
                }],
                request_id: None,
            },
        }));
        assert!(
            decoded <= bound,
            "StringList values={count}: decoded={decoded} bound={bound}"
        );

        let values = tight((0..count).map(|n| n as f32).collect());
        let (bound, decoded) = staged_bound(WalRecord::new(RaftLogEntry::Index {
            collection_id: "orders".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "id".into(),
                    field: "vector".into(),
                    value: FieldValue::Vector(values),
                    version: None,
                }],
                request_id: None,
            },
        }));
        assert!(
            decoded <= bound,
            "Vector values={count}: decoded={decoded} bound={bound}"
        );
    }
}

#[test]
fn staged_decode_never_exceeds_source_bound_for_sparse_replacement_maps() {
    for count in [1, 33, 5001] {
        let fields = (0..count)
            .map(|n| (format!("field-{n}"), FieldValue::String("x".into())))
            .collect();
        let docs = tight(vec![ReplaceDocItem {
            external_id: "id".into(),
            version: None,
            fields,
        }]);
        let (bound, decoded) = staged_bound(WalRecord::new(RaftLogEntry::ReplaceDocs {
            collection_id: "orders".into(),
            req: ReplaceDocsRequest { docs },
        }));
        assert!(
            decoded <= bound,
            "Replace sparse fields={count}: decoded={decoded} bound={bound}"
        );
    }
}
