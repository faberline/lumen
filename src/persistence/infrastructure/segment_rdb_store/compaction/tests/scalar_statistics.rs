use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentKind, SegmentRole,
};
use crate::persistence::infrastructure::segment::eid_writer::write_eid_segment;
use crate::persistence::infrastructure::segment::hash_writer::write_hash_segment;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::set_writer::write_set_segment;
use crate::persistence::infrastructure::segment::sparse_rows::{
    decode_sparse_local_rows, encode_sparse_local_rows,
};
use crate::persistence::infrastructure::segment::text_writer::write_text_segment;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::compaction::tests::{inputs, reference};
use crate::persistence::infrastructure::segment_rdb_store::compaction::write_compacted_field;
use crate::storage::Postings;
use std::collections::BTreeMap;

#[test]
fn scalar_compaction_preserves_tombstones_empty_values_and_text_statistics() {
    for includes_base in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("75");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = serde_json::json!({
            "n": {"type":"number"}, "s": {"type":"set"},
            "h": {"type":"hash"}, "t": {"type":"text","analyzer":"whitespace_lower"}
        });
        std::fs::write(
            dir.join("_schema.json"),
            serde_json::to_vec(&serde_json::json!({"version":1,"fields":schema})).unwrap(),
        )
        .unwrap();
        write_eid_segment(&dir.join("_collection.lmeta.lseg"), 1, &["m", "z"]).unwrap();
        let mut collection = CollectionCatalog {
            collection_id: "u".into(),
            collection_generation: 1,
            schema_version: 1,
            data_version: 3,
            schema,
            segments: vec![reference(
                SegmentRole::CollectionEids,
                None,
                SegmentKind::Base,
                0,
                "75/_collection.lmeta.lseg",
                None,
            )],
        };
        for field in ["n", "s", "h", "t"] {
            for ordinal in 0..=2 {
                let (ids, numbers, members, hashes, texts) = match ordinal {
                    0 => (
                        ["m", "z"],
                        [Some(1.), Some(2.)],
                        [Some("old"), Some("old")],
                        [Some(22), Some(44)],
                        [Some(("old", 1)), Some(("old", 1))],
                    ),
                    1 => (
                        ["a", "z"],
                        [Some(7.), None],
                        [Some("first"), None],
                        [Some(11), None],
                        [Some(("first", 1)), None],
                    ),
                    _ => (
                        ["m", "a"],
                        [Some(3.), Some(7.)],
                        [Some("new"), Some("")],
                        [Some(33), Some(11)],
                        [Some(("new", 2)), Some(("", 0))],
                    ),
                };
                let path = if ordinal == 0 {
                    format!("75/{field}.lseg")
                } else {
                    format!("75/__delta/{field}/{ordinal}.lseg")
                };
                let target = root.path().join(&path);
                std::fs::create_dir_all(target.parent().unwrap()).unwrap();
                match field {
                    "n" => write_number_segment(&target, 1, &numbers).unwrap(),
                    "h" => write_hash_segment(&target, 1, &hashes).unwrap(),
                    "s" => {
                        let values: Vec<Option<Vec<String>>> = members
                            .iter()
                            .map(|value| {
                                value.map(|member| {
                                    if member.is_empty() {
                                        vec![]
                                    } else {
                                        vec![member.to_owned()]
                                    }
                                })
                            })
                            .collect();
                        let mut postings = BTreeMap::<String, roaring::RoaringBitmap>::new();
                        for (row, members) in values.iter().enumerate() {
                            for member in members.iter().flatten() {
                                postings
                                    .entry(member.clone())
                                    .or_default()
                                    .insert(row as u32);
                            }
                        }
                        let borrowed: Vec<_> =
                            values.iter().map(|value| value.as_deref()).collect();
                        write_set_segment(&target, 1, &borrowed, &postings).unwrap();
                    }
                    "t" => {
                        let mut postings = BTreeMap::<String, Postings>::new();
                        let mut lens = [0; 2];
                        let present = texts.map(|text| text.is_some());
                        for (row, text) in texts.iter().enumerate() {
                            if let Some((token, count)) = text {
                                lens[row] = *count;
                                if *count != 0 {
                                    postings
                                        .entry((*token).to_owned())
                                        .or_default()
                                        .upsert(row as u32, *count);
                                }
                            }
                        }
                        write_text_segment(
                            &target,
                            1,
                            &postings,
                            &lens,
                            &present,
                            present.iter().filter(|value| **value).count() as u64,
                            lens.iter().map(|value| *value as u64).sum(),
                        )
                        .unwrap();
                    }
                    _ => unreachable!(),
                }
                let rows_path = format!("75/__delta/{field}/{ordinal}.rows.cbor");
                if ordinal != 0 {
                    encode_sparse_local_rows(
                        &root.path().join(&rows_path),
                        &ids.map(str::to_owned),
                    )
                    .unwrap();
                }
                collection.segments.push(reference(
                    SegmentRole::Field,
                    Some(field),
                    if ordinal == 0 {
                        SegmentKind::Base
                    } else {
                        SegmentKind::Delta
                    },
                    ordinal,
                    &path,
                    (ordinal != 0).then_some((rows_path.as_str(), 2)),
                ));
            }
        }
        for field in ["n", "s", "h", "t"] {
            let selected: Vec<_> = inputs(&collection, field)
                .into_iter()
                .filter(|input| includes_base || matches!(input.kind, SegmentKind::Delta))
                .collect();
            let output =
                write_compacted_field(root.path(), 9, &collection, field, &selected, includes_base)
                    .unwrap();
            let local = output.output.local_rows.as_ref().unwrap();
            let rows =
                decode_sparse_local_rows(&root.path().join(&local.path), local.count).unwrap();
            assert_eq!(
                (0..rows.len())
                    .map(|row| rows.external_id(row).unwrap())
                    .collect::<Vec<_>>(),
                ["a", "m", "z"],
                "compaction retains stable IDs including the deleted row"
            );
            let reader = SegmentReader::open(&root.path().join(output.output.path)).unwrap();
            match field {
                "n" => {
                    assert_eq!(
                        (
                            reader.number_at(0),
                            reader.number_at(1),
                            reader.number_at(2)
                        ),
                        (Some(7.), Some(3.), None)
                    );
                }
                "h" => {
                    assert_eq!(
                        (reader.hash_at(0), reader.hash_at(1), reader.hash_at(2)),
                        (Some(11), Some(33), None)
                    );
                }
                "s" => {
                    assert_eq!(reader.set_at(0), Some(vec![]));
                    assert_eq!(reader.set_at(1), Some(vec!["new".into()]));
                    assert_eq!(reader.set_at(2), None);
                    assert_eq!(reader.set_postings("old"), None);
                    assert_eq!(reader.set_postings("first"), None);
                    assert_eq!(
                        reader.set_postings("new"),
                        Some(roaring::RoaringBitmap::from_iter([1]))
                    );
                }
                "t" => {
                    assert!(reader.text_is_present(0));
                    assert_eq!(reader.text_doc_len(0), 0);
                    assert!(!reader.text_is_present(2));
                    assert_eq!(reader.text_postings("old"), None);
                    assert_eq!(reader.text_postings("first"), None);
                    assert_eq!(reader.text_postings("new"), Some((vec![1], vec![2])));
                    assert_eq!(reader.text_doc_count(), 2);
                    assert_eq!(reader.text_total_doc_len(), 2);
                }
                _ => unreachable!(),
            }
        }
    }
}
