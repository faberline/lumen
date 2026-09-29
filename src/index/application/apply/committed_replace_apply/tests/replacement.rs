use std::collections::BTreeMap;

use crate::index::application::apply::committed_replace_apply::tests::{
    assert_visible_parity, borrowed, doc, engine, owned, parity, state_fingerprint, wire,
};
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_record::WalRecord;
use crate::shared_kernel::types::document::{FieldValue, ReplaceDocItem, ReplaceDocsRequest};
use crate::shared_kernel::types::schema::CreateCollectionRequest;

#[test]
fn accepted_version_survives_unversioned_duplicate_and_stale_schema_is_dropped() {
    let actual = engine();
    let reference = engine();
    parity(
        &actual,
        &reference,
        ReplaceDocsRequest {
            docs: vec![
                doc("id", Some(30), [("kw", FieldValue::String("first".into()))]),
                doc("id", None, [("kw", FieldValue::String("second".into()))]),
                doc("id", Some(20), [("missing", FieldValue::Number(0.))]),
            ],
        },
        1,
    );
    let state = actual.state.read().unwrap();
    let coll = &state.collections["docs"];
    let id = coll.interner.id("id").unwrap();
    assert_eq!(coll.doc_versions.get(&id), Some(&30));
}

#[test]
fn index_then_equal_text_vector_replace_writes_once_then_skips() {
    let actual = engine();
    let reference = engine();
    let req = crate::shared_kernel::types::document::IndexRequest {
        request_id: None,
        items: vec![
            crate::shared_kernel::types::document::IndexItem {
                external_id: "id".into(),
                field: "body".into(),
                version: Some(7),
                value: FieldValue::String("same same".into()),
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: "id".into(),
                field: "v".into(),
                version: Some(7),
                value: FieldValue::Vector(vec![0., 1.]),
            },
        ],
    };
    actual.index_inner("docs", req.clone(), None, None).unwrap();
    reference.index_inner("docs", req, None, None).unwrap();
    let replacement = ReplaceDocsRequest {
        docs: vec![doc(
            "id",
            None,
            [
                ("body", FieldValue::String("same same".into())),
                ("v", FieldValue::Vector(vec![0., 1.])),
            ],
        )],
    };
    let first = borrowed(&actual, replacement.clone(), 1).unwrap();
    let first_owned = owned(&reference, replacement.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&first_owned).unwrap()
    );
    assert!(matches!(
        first.results[0],
        crate::shared_kernel::types::document::ReplaceDocResult::Ok {
            fields_written: 2,
            fields_skipped: 0
        }
    ));
    assert_visible_parity(&actual, &reference);
    let second = borrowed(&actual, replacement.clone(), 2).unwrap();
    let second_owned = owned(&reference, replacement).unwrap();
    assert_eq!(
        serde_json::to_value(&second).unwrap(),
        serde_json::to_value(&second_owned).unwrap()
    );
    assert!(matches!(
        second.results[0],
        crate::shared_kernel::types::document::ReplaceDocResult::Ok {
            fields_written: 0,
            fields_skipped: 2
        }
    ));
    assert_visible_parity(&actual, &reference);
}

#[test]
fn thirty_two_documents_can_replace_more_than_one_thousand_fields() {
    let fields: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> = (0..33)
        .map(|i| {
            (
                format!("f{i:02}"),
                serde_json::from_value(serde_json::json!({"type":"keyword"})).unwrap(),
            )
        })
        .collect();
    let make = || {
        let engine = Engine::with_change_budget(ChangeBudget::with_hard_limit(64 * 1024 * 1024));
        engine
            .create_collection_inner(
                "docs",
                CreateCollectionRequest {
                    fields: fields.clone(),
                },
            )
            .unwrap();
        engine
    };
    let actual = make();
    let reference = make();
    let req = ReplaceDocsRequest {
        docs: (0..32)
            .map(|i| ReplaceDocItem {
                external_id: format!("id-{i}"),
                version: Some(1),
                fields: fields
                    .keys()
                    .map(|name| (name.clone(), FieldValue::String(format!("value-{i}"))))
                    .collect(),
            })
            .collect(),
    };
    parity(&actual, &reference, req, 1);
    assert_eq!(actual.stats("docs").unwrap().documents_indexed, 32);
}

#[test]
fn malformed_wire_is_not_a_stale_noop_and_fragmented_fallback_releases_reservation() {
    let actual = engine();
    borrowed(
        &actual,
        ReplaceDocsRequest {
            docs: vec![doc(
                "id",
                Some(30),
                [("kw", FieldValue::String("old".into()))],
            )],
        },
        1,
    )
    .unwrap();
    let baseline = state_fingerprint(&actual);
    let before = actual.changes.budget.snapshot();
    let mut malformed = wire(ReplaceDocsRequest {
        docs: vec![doc(
            "id",
            Some(20),
            [("missing", FieldValue::String("bad".into()))],
        )],
    });
    malformed.pop();
    let mut completed = false;
    assert!(actual
        .try_apply_committed_replace_with_capacity_owner(&malformed, 2, &mut || Ok(()), |_, _| {
            completed = true
        })
        .is_err());
    assert!(!completed);
    assert_eq!(state_fingerprint(&actual), baseline);
    assert_eq!(actual.changes.budget.snapshot().total, before.total);

    let raw = wire(ReplaceDocsRequest {
        docs: vec![doc(
            "id",
            None,
            [("kw", FieldValue::String("fallback-token".into()))],
        )],
    });
    let needle = b"fallback-token";
    let pos = raw
        .windows(needle.len())
        .position(|window| window == needle)
        .unwrap();
    assert_eq!(raw[pos - 1], 0x60 + needle.len() as u8);
    let mut fragmented = raw[..pos - 1].to_vec();
    fragmented.push(0x7f);
    fragmented.extend_from_slice(&raw[pos - 1..pos + needle.len()]);
    fragmented.push(0xff);
    fragmented.extend_from_slice(&raw[pos + needle.len()..]);
    assert!(
        WalRecord::decode(&fragmented).is_ok(),
        "fallback fixture is valid legacy-compatible CBOR"
    );
    assert!(!actual
        .try_apply_committed_replace_with_capacity_owner(&fragmented, 2, &mut || Ok(()), |_, _| {
            completed = true
        })
        .unwrap());
    assert!(!completed);
    assert_eq!(actual.changes.budget.snapshot().total, before.total);
    assert_eq!(state_fingerprint(&actual), baseline);
}
