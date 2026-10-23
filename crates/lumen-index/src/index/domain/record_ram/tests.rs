use super::*;
use crate::shared_kernel::types::schema::FieldType;

#[test]
fn staged_numeric_vector_bound_includes_serde_content_and_output_together() {
    let values = 10_000;
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "id".into(),
                field: "v".into(),
                value: FieldValue::Vector(vec![0.5; values]),
                version: None,
            }],
            request_id: None,
        },
    };
    // serde1.0.228 buffers each element in Content before attempting the
    // Vec<f32> alternative. Content has a Vec/String-sized variant plus
    // a discriminant, while the final f32 allocation is also live.
    let unavoidable_peak = values * (4 * size_of::<usize>() + size_of::<f32>());
    let bound = estimate_record_decode_peak(&entry).unwrap();
    assert!(
        bound >= unavoidable_peak,
        "staged decoder bound {bound} omits buffered Content; minimum {unavoidable_peak}"
    );
}
fn keyword() -> FieldSpec {
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

#[test]
fn raw_hash_string_counts_even_when_normalized_hash_is_tiny() {
    let mut raw = String::with_capacity(65_536);
    raw.push_str("0000000000000001");
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "id".into(),
                field: "hash".into(),
                value: FieldValue::String(raw),
                version: None,
            }],
            request_id: None,
        },
    };
    assert!(estimate_record_ram(&entry).unwrap() >= 65_536);
}
#[test]
fn spare_vector_and_string_capacities_are_charged() {
    let mut id = String::with_capacity(1024);
    id.push('x');
    let mut values = Vec::with_capacity(128);
    values.push(1.0);
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: id,
                field: "vector".into(),
                value: FieldValue::Vector(values),
                version: None,
            }],
            request_id: None,
        },
    };
    assert!(estimate_record_ram(&entry).unwrap() >= 1024 + 128 * size_of::<f32>());
}
#[test]
fn index_replace_unindex_and_schema_variants_walk_owned_heaps() {
    let index = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![],
            request_id: Some("r".into()),
        },
    };
    let replace = RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "id".into(),
                version: None,
                fields: BTreeMap::from([("tag".into(), FieldValue::String("v".into()))]),
            }],
        },
    };
    let unindex = RaftLogEntry::UnindexDocs {
        collection_id: "c".into(),
        req: BatchUnindexDocsRequest {
            external_ids: vec!["id".into()],
        },
    };
    let schema = RaftLogEntry::CreateCollection {
        collection_id: "c".into(),
        req: CreateCollectionRequest {
            fields: BTreeMap::from([("tag".into(), keyword())]),
        },
    };
    for entry in [&index, &replace, &unindex, &schema] {
        assert!(estimate_record_ram(entry).unwrap() > size_of::<RaftLogEntry>());
    }
}

#[test]
fn every_log_variant_counts_its_owned_strings_or_containers() {
    let create = RaftLogEntry::CreateCollection {
        collection_id: "create".into(),
        req: CreateCollectionRequest {
            fields: BTreeMap::from([("tag".into(), keyword())]),
        },
    };
    let index = RaftLogEntry::Index {
        collection_id: "index".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "id".into(),
                field: "tag".into(),
                value: FieldValue::StringList(vec!["one".into(), "two".into()]),
                version: None,
            }],
            request_id: Some("request".into()),
        },
    };
    let replace = RaftLogEntry::ReplaceDocs {
        collection_id: "replace".into(),
        req: ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "id".into(),
                version: None,
                fields: BTreeMap::from([("tag".into(), FieldValue::String("value".into()))]),
            }],
        },
    };
    let truncate = RaftLogEntry::TruncateDocs {
        collection_id: "truncate".into(),
    };
    let unindex = RaftLogEntry::UnindexDocs {
        collection_id: "unindex".into(),
        req: BatchUnindexDocsRequest {
            external_ids: vec!["id".into()],
        },
    };
    let delete = RaftLogEntry::Delete {
        collection_id: "delete".into(),
        external_id: "id".into(),
        field: Some("tag".into()),
    };
    let drop_collection = RaftLogEntry::DropCollection {
        collection_id: "drop".into(),
        force: false,
    };
    let add_field = RaftLogEntry::AddField {
        collection_id: "add".into(),
        field_name: "tag".into(),
        spec: keyword(),
    };
    let drop_field = RaftLogEntry::DropField {
        collection_id: "drop-field".into(),
        field_name: "tag".into(),
    };
    for entry in [
        &create,
        &index,
        &replace,
        &truncate,
        &unindex,
        &delete,
        &drop_collection,
        &add_field,
        &drop_field,
    ] {
        assert!(estimate_record_ram(entry).unwrap() > size_of::<RaftLogEntry>());
    }
}
#[test]
fn one_entry_map_charges_a_full_sparse_node_not_only_its_live_pair() {
    let fields = BTreeMap::from([("tag".to_owned(), FieldValue::String("v".into()))]);
    assert!(
        map(&fields, |_, _| Ok(0)).unwrap() >= full_node_bytes::<String, FieldValue>().unwrap()
    );
}
#[test]
fn checked_helpers_refuse_overflow() {
    assert_eq!(add(usize::MAX, 1), Err(RecordRamError::Overflow));
    assert_eq!(mul(usize::MAX, 2), Err(RecordRamError::Overflow));
}
