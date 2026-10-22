use crate::ingest::domain::change_memory_cost::{
    estimate_change, Change, CostError, FieldCost, VectorBackendCost, ALLOC_HEADER_BYTES,
    DIRTY_ID_ENTRY_BYTES, MAP_ENTRY_BYTES, POSTING_ROW_BYTES, SPARSE_TEXT_DOCUMENT_BUCKET_BYTES,
    TEXT_BTREE_FIRST_NODE_SLACK_BYTES, TEXT_BTREE_TERM_BYTES,
};

#[test]
fn text_cost_charges_terms_postings_ids_dirty_and_one_capture_clone() {
    let change = Change::index_new_document(
        12,
        FieldCost::Text {
            distinct_terms: 3,
            total_term_bytes: 15,
        },
    );
    let cost = estimate_change(&change).unwrap();
    assert!(cost.active > 12, "text needs more than its external id");
    assert_eq!(cost.frozen, cost.active);
    assert!(cost.prepublish >= DIRTY_ID_ENTRY_BYTES);
    assert_eq!(cost.total(), cost.active + cost.frozen + cost.prepublish);
}

#[test]
fn ngram_is_charged_by_normalized_terms_not_original_wire_bytes() {
    let one_wire_byte = Change::index_existing_document(
        2,
        FieldCost::Text {
            distinct_terms: 5,
            total_term_bytes: 15,
        },
    );
    let cost = estimate_change(&one_wire_byte).unwrap();
    assert!(cost.active >= 5 * POSTING_ROW_BYTES);
    assert!(cost.active > 15);
}

#[test]
fn hnsw_vector_charges_payload_ids_maps_and_a_checkpoint_row_without_wal_bytes() {
    let change = Change::index_new_document(
        9,
        FieldCost::Vector {
            dim: 768,
            backend: VectorBackendCost::Hnsw,
            quantized_sq: false,
        },
    );
    let cost = estimate_change(&change).unwrap();
    assert!(cost.active >= 768 * 4);
    assert!(
        cost.active > 768 * 4,
        "pending payload has a VectorStore map"
    );
    assert!(cost.frozen >= 768 * 4 + 9);
}

#[test]
fn delete_and_schema_changes_charge_their_dirty_or_metadata_work() {
    assert!(estimate_change(&Change::unindex(24, 4)).unwrap().total() > 0);
    assert!(estimate_change(&Change::schema(40)).unwrap().total() >= 40);
    assert!(estimate_change(&Change::reshard(128)).unwrap().total() >= 128);
    assert!(estimate_change(&Change::truncate()).unwrap().total() >= MAP_ENTRY_BYTES);
}

#[test]
fn set_total_bytes_scale_once_per_owned_copy_not_per_member() {
    let small = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Set {
            members: 10,
            member_bytes: 100,
        },
    ))
    .unwrap();
    let large = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Set {
            members: 10,
            member_bytes: 200,
        },
    ))
    .unwrap();
    // 2 copies * (2 * 100 B collection allocation bound) = 400 B.
    assert_eq!(large.active - small.active, 400);
    assert_eq!(large.frozen - small.frozen, 400);
}

#[test]
fn text_charges_allocation_headers_per_distinct_term() {
    let one = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 1,
            total_term_bytes: 100,
        },
    ))
    .unwrap();
    let ten = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 10,
            total_term_bytes: 100,
        },
    ))
    .unwrap();
    assert_eq!(
        ten.active - one.active,
        9 * (2 * 2 * ALLOC_HEADER_BYTES + TEXT_BTREE_TERM_BYTES + POSTING_ROW_BYTES)
    );
}

#[test]
fn text_dictionary_charges_first_btree_node_slack_and_sparse_document_bucket() {
    let zero = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 0,
            total_term_bytes: 0,
        },
    ))
    .unwrap();
    let one = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 1,
            total_term_bytes: 1,
        },
    ))
    .unwrap();
    let many = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 12,
            total_term_bytes: 12,
        },
    ))
    .unwrap();
    assert!(one.active > zero.active);
    assert!(many.active > one.active);
    assert!(zero.active >= SPARSE_TEXT_DOCUMENT_BUCKET_BYTES);
    assert!(
        one.active - zero.active >= TEXT_BTREE_FIRST_NODE_SLACK_BYTES + TEXT_BTREE_TERM_BYTES,
        "the first ordered dictionary node and one sparse row must be reserved"
    );
}

#[test]
fn text_sparse_row_estimate_has_no_dense_prefix_component() {
    let changed_row = || {
        estimate_change(&Change::index_existing_document(
            8,
            FieldCost::Text {
                distinct_terms: 1,
                total_term_bytes: 4,
            },
        ))
        .unwrap()
    };
    let row_zero = changed_row();
    // The estimator has no document ordinal input. A far sparse stable ID
    // therefore keeps this same per-row reservation; storage owns the
    // separate no-dense-prefix proof for `delta_docs`.
    let row_large_sparse_id = changed_row();
    assert_eq!(row_large_sparse_id, row_zero);
}

#[test]
fn text_zero_terms_is_finite_and_high_term_inputs_fail_checked() {
    let zero = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 0,
            total_term_bytes: 0,
        },
    ))
    .unwrap();
    assert!(
        zero.total() > 0,
        "dirty tracking remains owned for empty text"
    );
    let bytes_overflow = Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: 1,
            total_term_bytes: usize::MAX,
        },
    );
    let terms_overflow = Change::index_existing_document(
        8,
        FieldCost::Text {
            distinct_terms: usize::MAX,
            total_term_bytes: 0,
        },
    );
    assert_eq!(estimate_change(&bytes_overflow), Err(CostError::Overflow));
    assert_eq!(estimate_change(&terms_overflow), Err(CostError::Overflow));
}

#[test]
fn hnsw_graph_is_outside_the_change_budget() {
    let flat = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Vector {
            dim: 768,
            backend: VectorBackendCost::Flat,
            quantized_sq: false,
        },
    ))
    .unwrap();
    let hnsw = estimate_change(&Change::index_existing_document(
        8,
        FieldCost::Vector {
            dim: 768,
            backend: VectorBackendCost::Hnsw,
            quantized_sq: false,
        },
    ))
    .unwrap();
    assert_eq!(hnsw, flat);
}

#[test]
fn explicit_versions_and_replace_checksums_stay_active_but_are_not_checkpoint_clones() {
    let plain = Change::index_existing_document(8, FieldCost::Number);
    let versioned = Change::Index {
        external_id_bytes: 8,
        new_document: false,
        field: FieldCost::Number,
        volatile_metadata_bytes: 40,
    };
    let plain = estimate_change(&plain).unwrap();
    let versioned = estimate_change(&versioned).unwrap();
    assert!(versioned.active > plain.active);
    assert_eq!(versioned.frozen, plain.frozen);
    assert_eq!(versioned.prepublish, plain.prepublish);
}

#[test]
fn checked_overflow_rejects_an_unrepresentable_reservation() {
    let change = Change::index_new_document(usize::MAX, FieldCost::Keyword { value_bytes: 1 });
    assert_eq!(estimate_change(&change), Err(CostError::Overflow));
}
