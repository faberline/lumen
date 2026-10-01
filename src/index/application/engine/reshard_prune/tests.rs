//! #1467 R1/R2/R4: prune-chunk accumulator hardening

use std::collections::BTreeMap;

use crate::index::application::engine::reshard_prune::ReshardPruneOutcome;
use crate::index::application::engine::reshard_prune::{
    PRUNE_ACCUM_MAX_AGE_TICKS, PRUNE_ACCUM_MAX_ENTRIES, PRUNE_ACCUM_MAX_TOTAL_CHUNKS,
};
use crate::index::application::engine::tests::item;
use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn prune_test_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
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
    CreateCollectionRequest { fields }
}

fn prune_index_user(e: &Engine, collection: &str, eid: &str) {
    e.index(
        collection,
        IndexRequest {
            items: vec![item(
                eid,
                "email",
                FieldValue::String(format!("{eid}@x.com")),
            )],
            request_id: None,
        },
    )
    .unwrap();
}

fn prune_has_doc(e: &Engine, collection: &str, eid: &str) -> bool {
    let resp = e
        .search(
            collection,
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String(format!("{eid}@x.com")),
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
    resp.total == 1
}

fn prune_bucket_of(collection_id: &str, external_id: &str) -> u32 {
    VirtualBucketShardMap::balanced(0, 4, 1)
        .unwrap()
        .route_document(collection_id, None, external_id)
        .bucket
}

fn prune_chunk(
    to_map_version: u64,
    bucket: u32,
    collection_id: &str,
    chunk_index: u32,
    total_chunks: u32,
    keep_ids: &[&str],
) -> crate::sharding::domain::prune_chunk::ReshardPruneChunk {
    crate::sharding::domain::prune_chunk::ReshardPruneChunk {
        to_map_version,
        bucket,
        virtual_bucket_count: 4,
        collection_id: collection_id.to_string(),
        chunk_index,
        total_chunks,
        keep_ids: keep_ids.iter().map(|s| s.to_string()).collect(),
    }
}

/// #1467 R1/AC1: two chunks that BOTH complete the same group (a
/// duplicate final chunk racing its original, e.g. a client retry
/// in flight concurrently with the original request) must never both
/// observe "ready" and neither may ever prune against an empty keep
/// set — the fixed readiness-check-and-removal is one critical section,
/// so exactly one call drains the group with the correct, fully-unioned
/// keep set and every later racer starts a fresh, empty accumulation.
#[test]
fn apply_reshard_prune_chunk_concurrent_completions_never_use_empty_keep_set() {
    let e = std::sync::Arc::new(Engine::new());
    e.create_collection("u", prune_test_schema()).unwrap();
    prune_index_user(&e, "u", "kept-1");
    prune_index_user(&e, "u", "kept-2");
    prune_index_user(&e, "u", "dropped");
    // The scope only prunes documents that route to `bucket` — derive it
    // from "dropped" itself so the doc under test is actually in scope
    // regardless of where "kept-1"/"kept-2" happen to hash (they are
    // safe either way: present in `keep_ids`, so kept if co-bucketed,
    // and untouched if not).
    let bucket = prune_bucket_of("u", "dropped");

    // Two threads race the SAME final chunk of a 1-chunk group — the
    // keep set never includes "dropped", so a correct outcome always
    // prunes exactly it, exactly once (a re-run against already-pruned
    // state prunes 0 more), and never wipes "kept-1"/"kept-2".
    let mut handles = Vec::new();
    for _ in 0..8 {
        let e = e.clone();
        handles.push(std::thread::spawn(move || {
            e.apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 0, 1, &["kept-1", "kept-2"]))
                .unwrap()
        }));
    }
    let outcomes: Vec<ReshardPruneOutcome> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();

    assert!(
        outcomes.iter().all(|o| o.complete),
        "every racer completes its own 1-chunk group: {outcomes:?}"
    );
    let total_pruned: u32 = outcomes.iter().map(|o| o.documents_pruned).sum();
    assert_eq!(
        total_pruned, 1,
        "the dropped doc must be pruned exactly once across every racer, \
             never an empty-keep-set full-bucket wipe: {outcomes:?}"
    );
    assert!(prune_has_doc(&e, "u", "kept-1"));
    assert!(prune_has_doc(&e, "u", "kept-2"));
    assert!(!prune_has_doc(&e, "u", "dropped"));
}

/// #1467 R2/AC2: an abandoned pass that only sent chunk 0 of a
/// multi-chunk group (driver crash/restart mid-pass) leaves a stale
/// partial accumulation. A retried pass's fresh `chunk_index == 0` must
/// reset it rather than union into it — otherwise a keep_id carried
/// over from the abandoned attempt could resurrect a doc the retried
/// pass's own keep set actually drops.
#[test]
fn apply_reshard_prune_chunk_chunk_index_zero_resets_stale_partial() {
    let e = Engine::new();
    e.create_collection("u", prune_test_schema()).unwrap();
    prune_index_user(&e, "u", "kept");
    prune_index_user(&e, "u", "stale-only");
    // The scope only prunes documents that route to `bucket` — derive it
    // from "stale-only" itself so the doc under test is actually in
    // scope. "kept" is safe either way: it is always in the retried
    // pass's `keep_ids`, so it survives whether or not it shares a
    // bucket with "stale-only".
    let bucket = prune_bucket_of("u", "stale-only");

    // Abandoned first pass: chunk 0 of 2 lands, keeping "stale-only"
    // (as if the retried pass's keep set will differ); chunk 1 never
    // arrives.
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 0, 2, &["stale-only"]))
        .unwrap();
    assert!(!out.complete);

    // Retried pass restarts from chunk 0 with the corrected keep set
    // (drops "stale-only", keeps "kept"), then completes with chunk 1.
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 0, 2, &["kept"]))
        .unwrap();
    assert!(!out.complete);
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 1, 2, &[]))
        .unwrap();
    assert!(out.complete);

    assert!(
        prune_has_doc(&e, "u", "kept"),
        "the retried pass's own keep set must be honored"
    );
    assert!(
        !prune_has_doc(&e, "u", "stale-only"),
        "the abandoned pass's stale chunk-0 keep_id must not have survived \
             the chunk_index==0 reset"
    );
}

/// #1467 R4/AC4: `total_chunks == 0` and `total_chunks` beyond the sanity
/// cap are both rejected before ever touching the accumulator.
#[test]
fn apply_reshard_prune_chunk_rejects_invalid_total_chunks() {
    let e = Engine::new();
    e.create_collection("u", prune_test_schema()).unwrap();

    for total_chunks in [0, PRUNE_ACCUM_MAX_TOTAL_CHUNKS + 1] {
        let err = e
            .apply_reshard_prune_chunk(prune_chunk(1, 0, "u", 0, total_chunks, &[]))
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::InvalidPruneChunk { .. })
            ),
            "total_chunks={total_chunks} must be rejected as InvalidPruneChunk: {err:?}"
        );
    }
}

/// #1467 R4/AC4: once [`PRUNE_ACCUM_MAX_ENTRIES`] distinct incomplete
/// groups are already held, a brand-new key is rejected rather than
/// growing the accumulator without bound; an already-tracked key may
/// still make progress.
#[test]
fn apply_reshard_prune_chunk_rejects_new_key_once_accumulator_is_full() {
    let e = Engine::new();
    e.create_collection("u", prune_test_schema()).unwrap();

    // Every filler key uses a 3-chunk group and only ever receives chunk
    // 0, so all `PRUNE_ACCUM_MAX_ENTRIES` entries stay held (incomplete,
    // never removed) at once.
    for v in 0..PRUNE_ACCUM_MAX_ENTRIES as u64 {
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(v, 0, "u", 0, 3, &[]))
            .unwrap();
        assert!(!out.complete);
    }

    // An already-tracked key still makes progress without completing
    // (2 of 3 chunks received) — the accumulator count must not drop,
    // so the capacity check below still holds.
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(0, 0, "u", 1, 3, &[]))
        .unwrap();
    assert!(!out.complete);

    // A brand-new key is rejected: the accumulator is at capacity.
    let err = e
        .apply_reshard_prune_chunk(prune_chunk(
            PRUNE_ACCUM_MAX_ENTRIES as u64,
            0,
            "u",
            0,
            3,
            &[],
        ))
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<StorageError>(),
            Some(StorageError::PruneAccumulatorFull { .. })
        ),
        "a new key beyond PRUNE_ACCUM_MAX_ENTRIES must be rejected: {err:?}"
    );
}

/// #1467 R4/AC4: an incomplete group older than
/// [`PRUNE_ACCUM_MAX_AGE_TICKS`] is age-GC'd on a later call — its
/// earlier chunks are gone, so a later chunk for the same key starts a
/// fresh (still-incomplete) accumulation instead of completing.
#[test]
fn apply_reshard_prune_chunk_gc_evicts_stale_incomplete_groups_by_age() {
    let e = Engine::new();
    e.create_collection("u", prune_test_schema()).unwrap();

    // Key under test: only chunk 0 of 2 ever lands.
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(999, 0, "u", 0, 2, &[]))
        .unwrap();
    assert!(!out.complete);

    // Advance the tick well past PRUNE_ACCUM_MAX_AGE_TICKS via distinct,
    // SELF-COMPLETING single-chunk (total_chunks=1) groups — each is
    // removed from the accumulator the moment it lands, so this loop
    // advances the tick counter without ever growing the accumulator
    // past `PRUNE_ACCUM_MAX_ENTRIES` (which would otherwise reject
    // long before the age budget is reached, since
    // `PRUNE_ACCUM_MAX_AGE_TICKS` far exceeds `PRUNE_ACCUM_MAX_ENTRIES`).
    for v in 0..(PRUNE_ACCUM_MAX_AGE_TICKS + 2) {
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(2_000_000 + v, 0, "u", 0, 1, &[]))
            .unwrap();
        assert!(out.complete);
    }

    // The key under test's chunk 0 must have been age-GC'd: its
    // "final" chunk 1 now starts a fresh, still-incomplete group rather
    // than completing.
    let out = e
        .apply_reshard_prune_chunk(prune_chunk(999, 0, "u", 1, 2, &[]))
        .unwrap();
    assert!(
        !out.complete,
        "chunk 0 of the aged-out group must have been GC'd, so chunk 1 alone \
             cannot complete a fresh 2-chunk group: {out:?}"
    );
}
