//! Collection DDL: a new collection's version, the reserved `:` in its id, a
//! create over a dropped collection's tombstone, and the sweep of a tombstone
//! nobody recreates.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::index::application::engine::collections::DropOutcome;
use crate::index::application::engine::tests::{build_users_schema, index_kw, kw_only_schema};
use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn create_collection_returns_version_one() {
    let e = Engine::new();
    let r = e.create_collection("users", build_users_schema()).unwrap();
    assert_eq!(r.collection_id, "users");
    assert_eq!(r.version, 1);
    assert_eq!(r.fields_count, 4);
}

// #1271: `:` is reserved for custom-method routes (`POST
// /collections:search`) so it must never be a valid collection id.
#[test]
fn create_collection_rejects_colon_in_collection_id() {
    let e = Engine::new();
    let err = e
        .create_collection("users:search", build_users_schema())
        .unwrap_err();
    let se = err
        .downcast_ref::<StorageError>()
        .expect("StorageError variant");
    assert!(
        matches!(se, StorageError::InvalidCollectionName(id) if id == "users:search"),
        "expected InvalidCollectionName, got {se:?}"
    );
}

// ---- #3953: create-over-tombstone supersede ------------------------

/// #3953(a) at the `Engine` layer: `create_collection` for a
/// soft-deleted id must not stay wedged behind `check_live` — it must
/// supersede the tombstone with a fresh, empty collection whose schema
/// comes from the new request alone (not merged with the deleted
/// predecessor's), and the pre-delete doc must not survive.
///
/// Everything above is about CONTENTS. `version` is the one thing that
/// does cross the tombstone, because it is not content — it is the
/// number the caller keys cached schema state on, and a supersede that
/// re-answered 1 would move it backwards. See the supersede branch's own
/// comment, and `tests/it/collection_version_never_moves_backwards.rs`.
#[test]
fn create_collection_supersedes_tombstone_with_fresh_empty_collection() {
    let e = Engine::new();
    e.create_collection("a", kw_only_schema()).unwrap();
    index_kw(&e, "a", "old1");
    assert_eq!(e.drop_collection("a", false).unwrap(), DropOutcome::Marked);

    // Live routes must still see the tombstone as 410 Gone before the
    // recreate — this fix must not touch that behavior.
    let stats_err = e.stats("a").unwrap_err();
    assert!(
        matches!(
            stats_err.downcast_ref::<StorageError>(),
            Some(StorageError::Gone(id)) if id == "a"
        ),
        "a soft-deleted collection must still answer 410 Gone on reads \
             right up until it is recreated: {stats_err:?}"
    );

    let resp = e.create_collection("a", kw_only_schema()).unwrap();
    assert_eq!(
        resp.version, 2,
        "a superseding create continues the id's version line from the \
             tombstone rather than restarting it — the predecessor reached 1, \
             so the supersede answers 2"
    );

    // The recreated collection is genuinely empty: the pre-delete doc
    // must not be visible, and stats must report zero docs.
    assert_eq!(e.stats("a").unwrap().documents_indexed, 0);
    let hits = e
        .search(
            "a",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String("old1@x.com".into()),
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
        .unwrap();
    assert_eq!(
        hits.total, 0,
        "the recreated collection must not surface the deleted \
             predecessor's documents"
    );
}

/// #3953(a): a superseding create must not additive-merge the new
/// request's schema onto the deleted predecessor's — a field the
/// deleted collection had but the new request omits must be gone, not
/// carried forward.
#[test]
fn create_collection_supersede_does_not_inherit_deleted_schema() {
    let e = Engine::new();
    let mut wide_fields = BTreeMap::new();
    wide_fields.insert(
        "email".into(),
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
    wide_fields.insert(
        "extra".into(),
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
    e.create_collection(
        "a",
        CreateCollectionRequest {
            fields: wide_fields,
        },
    )
    .unwrap();
    e.drop_collection("a", false).unwrap();

    let resp = e.create_collection("a", kw_only_schema()).unwrap();
    assert_eq!(
        resp.fields_count, 1,
        "a superseding create must start from the new request's schema \
             alone, not merge in the deleted predecessor's extra field"
    );
}

/// #3953: `sweep_deleted`'s grace-window path must keep working for a
/// collection that is deleted and never recreated — this fix only
/// changes what an explicit `create_collection` (PUT) call does to a
/// tombstone; it must not remove or shortcut the background sweep.
#[test]
fn sweep_deleted_still_reclaims_a_tombstone_nobody_recreates() {
    let e = Engine::new();
    e.create_collection("a", kw_only_schema()).unwrap();
    assert_eq!(e.drop_collection("a", false).unwrap(), DropOutcome::Marked);

    // Immediately: the grace window has not elapsed, sweep does nothing.
    assert_eq!(e.sweep_deleted(Duration::from_secs(3600)).unwrap(), 0);
    assert!(e.stats("a").is_err(), "still tombstoned, still 410");

    // A zero grace window sweeps it away right away — proving the sweep
    // path itself, independent of any PUT, still physically reclaims a
    // tombstone that nobody recreated.
    assert_eq!(e.sweep_deleted(Duration::from_secs(0)).unwrap(), 1);
    assert!(
        e.stats("a").is_err(),
        "physically removed collection must still report not-found, \
             not silently reappear as live"
    );
}
