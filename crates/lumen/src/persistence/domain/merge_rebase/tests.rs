use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, LocalRowsReference, SegmentFormat, SegmentGenerationManifest, SegmentKind,
    SegmentReference, SegmentRole,
};
use crate::persistence::domain::merge_rebase::{
    rebase_manifest, MergeSelection, RebaseRefusal, VerifiedOutput,
};

fn r(field: &str, kind: SegmentKind, path: &str, ordinal: u32) -> SegmentReference {
    SegmentReference {
        role: SegmentRole::Field,
        field: Some(field.into()),
        ordinal,
        kind,
        format: SegmentFormat::LsegV1,
        path: path.into(),
        local_rows: Some(LocalRowsReference {
            format: "rows".into(),
            path: format!("{path}.rows"),
            count: 1,
        }),
        applied_seq: Some(7),
        payload_sha256: Some(path.into()),
    }
}
fn manifest() -> SegmentGenerationManifest {
    SegmentGenerationManifest {
        schema_version: 2,
        checkpoint_sequence: 99,
        revision: 4,
        previous: Some("old".into()),
        next_collection_generation: 9,
        collections: vec![CollectionCatalog {
            collection_id: "c".into(),
            collection_generation: 7,
            schema_version: 3,
            data_version: 41,
            schema: serde_json::json!({"x":1}),
            segments: vec![
                r("f", SegmentKind::Base, "base", 0),
                r("other", SegmentKind::Delta, "other", 1),
                r("f", SegmentKind::Delta, "d1", 1),
                r("f", SegmentKind::Delta, "d2", 2),
                r("f", SegmentKind::Delta, "later", 3),
            ],
        }],
    }
}
fn select(m: &SegmentGenerationManifest) -> MergeSelection {
    MergeSelection {
        collection_id: "c".into(),
        collection_generation: 7,
        schema_version: 3,
        schema: serde_json::json!({"x":1}),
        field: "f".into(),
        inputs: vec![
            m.collections[0].segments[2].clone(),
            m.collections[0].segments[3].clone(),
        ],
        base: None,
        vector_sidecar: None,
    }
}
#[test]
fn interleaved_preserves_newer_and_data_version() {
    let m = manifest();
    let n = rebase_manifest(
        &m,
        &select(&m),
        VerifiedOutput {
            field: r("f", SegmentKind::Delta, "merged", 2),
            vector_sidecar: None,
        },
    )
    .unwrap();
    assert_eq!(n.checkpoint_sequence, 99);
    assert_eq!(n.revision, 4);
    assert_eq!(n.collections[0].data_version, 41);
    assert_eq!(
        n.collections[0]
            .segments
            .iter()
            .map(|r| r.path.as_str())
            .collect::<Vec<_>>(),
        vec!["base", "other", "merged", "later"]
    );
}
#[test]
fn strict_actual_identity_and_output_kind_refuse() {
    let m = manifest();
    let mut s = select(&m);
    s.inputs[1].local_rows.as_mut().unwrap().count = 2;
    assert_eq!(
        rebase_manifest(
            &m,
            &s,
            VerifiedOutput {
                field: r("f", SegmentKind::Delta, "m", 1),
                vector_sidecar: None
            }
        ),
        Err(RebaseRefusal::InputIdentityChanged)
    );
    assert_eq!(
        rebase_manifest(
            &m,
            &select(&m),
            VerifiedOutput {
                field: r("f", SegmentKind::Base, "m", 0),
                vector_sidecar: None
            }
        ),
        Err(RebaseRefusal::OutputShape)
    );
}

#[test]
fn output_ordinal_and_paths_cannot_replace_unselected_data() {
    let m = manifest();
    let s = select(&m);
    let wrong_ordinal = VerifiedOutput {
        field: r("f", SegmentKind::Delta, "merged", 9),
        vector_sidecar: None,
    };
    assert_eq!(
        rebase_manifest(&m, &s, wrong_ordinal),
        Err(RebaseRefusal::OutputShape)
    );
    let colliding = VerifiedOutput {
        field: r("f", SegmentKind::Delta, "other", 2),
        vector_sidecar: None,
    };
    assert_eq!(
        rebase_manifest(&m, &s, colliding),
        Err(RebaseRefusal::OutputShape)
    );
    let mut colliding_rows = r("f", SegmentKind::Delta, "merged", 2);
    colliding_rows.local_rows.as_mut().unwrap().path = "later".into();
    assert_eq!(
        rebase_manifest(
            &m,
            &s,
            VerifiedOutput {
                field: colliding_rows,
                vector_sidecar: None
            }
        ),
        Err(RebaseRefusal::OutputShape)
    );
}

#[test]
fn actual_input_identity_includes_sequence_checksum_and_row_path() {
    let m = manifest();
    for altered in 0..3 {
        let mut s = select(&m);
        match altered {
            0 => s.inputs[0].applied_seq = Some(6),
            1 => s.inputs[0].payload_sha256 = Some("another-checksum".into()),
            _ => s.inputs[0].local_rows.as_mut().unwrap().path = "another.rows".into(),
        }
        assert_eq!(
            rebase_manifest(
                &m,
                &s,
                VerifiedOutput {
                    field: r("f", SegmentKind::Delta, "merged", 2),
                    vector_sidecar: None
                }
            ),
            Err(RebaseRefusal::InputIdentityChanged)
        );
    }
}

#[test]
fn field_interleaving_is_valid_but_an_inserted_input_layer_is_stale() {
    let mut m = manifest();
    let s = select(&m);
    m.collections[0]
        .segments
        .insert(3, r("g", SegmentKind::Delta, "g", 1));
    let output = || VerifiedOutput {
        field: r("f", SegmentKind::Delta, "merged", 2),
        vector_sidecar: None,
    };
    let n = rebase_manifest(&m, &s, output()).unwrap();
    assert!(n.collections[0]
        .segments
        .contains(&m.collections[0].segments[3]));
    assert!(n.collections[0]
        .segments
        .contains(m.collections[0].segments.last().unwrap()));
    m.collections[0]
        .segments
        .insert(3, r("f", SegmentKind::Delta, "inserted", 1));
    assert_eq!(
        rebase_manifest(&m, &s, output()),
        Err(RebaseRefusal::InputIdentityChanged)
    );
}

#[test]
fn base_merge_requires_complete_older_prefix_and_exact_vector_sidecar() {
    let mut m = manifest();
    let mut sidecar = r("f", SegmentKind::Base, "vector-eids", 0);
    sidecar.role = SegmentRole::VectorEids;
    sidecar.local_rows = None;
    m.collections[0].segments.push(sidecar.clone());
    let mut s = select(&m);
    s.base = Some(m.collections[0].segments[0].clone());
    s.vector_sidecar = Some(sidecar.clone());
    let mut new_sidecar = sidecar.clone();
    new_sidecar.path = "merged-eids".into();
    new_sidecar.payload_sha256 = Some("merged-eids-checksum".into());
    let output = || VerifiedOutput {
        field: r("f", SegmentKind::Base, "merged-base", 0),
        vector_sidecar: Some(new_sidecar.clone()),
    };
    m.collections[0].data_version = 123;
    m.checkpoint_sequence = 120;
    let n = rebase_manifest(&m, &s, output()).unwrap();
    assert_eq!(n.checkpoint_sequence, 120);
    assert_eq!(n.collections[0].data_version, 123);
    assert!(n.collections[0].segments.contains(&new_sidecar));
    assert!(n.collections[0]
        .segments
        .contains(&m.collections[0].segments[4]));
    assert!(!n.collections[0].segments.contains(&sidecar));
    assert_eq!(
        rebase_manifest(
            &m,
            &s,
            VerifiedOutput {
                field: r("f", SegmentKind::Base, "merged-base", 0),
                vector_sidecar: None
            }
        ),
        Err(RebaseRefusal::OutputShape)
    );
    s.inputs.remove(0);
    assert_eq!(
        rebase_manifest(&m, &s, output()),
        Err(RebaseRefusal::InputIdentityChanged)
    );
}

#[test]
fn replacement_refuses_changed_schema_generation_and_missing_collection() {
    let m = manifest();
    let output = || VerifiedOutput {
        field: r("f", SegmentKind::Delta, "merged", 2),
        vector_sidecar: None,
    };
    for altered in 0..3 {
        let mut s = select(&m);
        match altered {
            0 => s.collection_generation += 1,
            1 => s.schema_version += 1,
            _ => s.schema = serde_json::json!({"x": 2}),
        }
        assert_eq!(
            rebase_manifest(&m, &s, output()),
            Err(RebaseRefusal::CollectionIdentityChanged)
        );
    }
    let mut missing = select(&m);
    missing.collection_id = "gone".into();
    assert_eq!(
        rebase_manifest(&m, &missing, output()),
        Err(RebaseRefusal::MissingCollection)
    );
}
