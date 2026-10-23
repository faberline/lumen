//! An update and a delete across checkpoints: a sealed keyword's update
//! survives the re-seal and a cold reopen, and a document deleted across a
//! checkpoint is collected, not resurrected.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::index::application::engine::checkpoint_flush::tests::{
    battery, index_doc, knn, run, seed, set_of,
};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::QueryNode;
use crate::shared_kernel::types::query::TermQuery;

/// A keyword replacement of a sealed doc is a live overlay, not a delete.
/// The base id stays tombstoned so old postings stay hidden until re-seal,
/// while `keyword_at` must fall through to the overlay to persist the new
/// value into the next checkpoint.
#[test]
fn sealed_keyword_update_survives_reseal_and_cold_reopen() {
    let persisted = Arc::new(Engine::new());
    seed(&persisted);
    index_doc(
        &persisted,
        "alpha",
        "d0",
        1.0,
        "sealed-before",
        "red",
        true,
        0,
        &[0.1, 0.2, 0.3, 0.4],
    );
    let dir = tempfile::tempdir().unwrap();
    persisted.flush_to_segments(dir.path(), 4).unwrap();

    // d0 is a sealed base id. Replacing its keyword must preserve the base
    // tombstone and write the new value into the live forward overlay.
    index_doc(
        &persisted,
        "alpha",
        "d0",
        1.0,
        "updated",
        "red",
        true,
        0,
        &[0.1, 0.2, 0.3, 0.4],
    );
    persisted.delete("alpha", "d1", None).unwrap();

    // A second checkpoint must carry the replacement, and it must not
    // resurrect a true delete whose base value no longer has live coverage.
    persisted.flush_to_segments(dir.path(), 6).unwrap();

    let reopened = Arc::new(Engine::new());
    assert_eq!(reopened.reopen_from_segment_dir(dir.path()).unwrap(), 6);

    let term = |value: &str| {
        set_of(&run(
            &reopened,
            "alpha",
            QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String(value.into()),
            }),
            100,
        ))
    };
    assert_eq!(term("updated"), BTreeSet::from(["d0".to_string()]));
    assert!(
        term("sealed-before").is_empty(),
        "old keyword must stay absent"
    );
    assert!(term("b").is_empty(), "true delete must not resurrect");
    assert_eq!(
        set_of(&run(
            &reopened,
            "alpha",
            QueryNode::Exists(crate::shared_kernel::types::query::ExistsQuery {
                field: "kw".into()
            }),
            100,
        )),
        BTreeSet::from(["d0".to_string(), "d2".to_string(), "d3".to_string()]),
        "the updated document still exists while the true delete is absent"
    );
}

// ----- (c) TOMBSTONE GC ACROSS A CHECKPOINT (Phase 2g-A) -----------------
//
// THE CRUX. A base doc DELETED after the first checkpoint must be GC'd by the
// second checkpoint and stay absent on reopen — never resurrected, never an
// inflated BM25 corpus. Sequence: seed → flush S1 (base docs' forward dropped,
// values now ONLY on the immutable segment) → DELETE several base docs (in the
// sealed range) + index a live tail → flush S2 (re-seal) → reopen fresh.
//
// The oracle is a PURE-LIVE engine that ran the identical op sequence but NEVER
// flushed (no segments at all). The reopened-from-disk engine must match it on
// every leg: result SETs, byte-identical f32 BM25 scores (corpus scalars must
// exclude the deleted docs), ordered kNN (deleted vectors absent), retrieved
// values, and doc_count. Plus a direct assertion that a deleted eid is gone.
//
// Without the liveness-aware gather, flush S2's `(0..n_docs).map(number_at)`
// re-reads the deleted base doc's STALE value off the prior segment and writes
// it back, so reopen RESURRECTS the doc and the BM25 corpus is inflated.
#[test]
fn delete_across_checkpoint_is_gc_not_resurrected() {
    // Persisted engine: seed (d0..d3), checkpoint, delete, tail, checkpoint.
    let persisted = Arc::new(Engine::new());
    seed(&persisted);
    let dir = tempfile::tempdir().unwrap();
    persisted.flush_to_segments(dir.path(), 4).unwrap(); // S1: base forward dropped

    // Delete BASE docs that live entirely in the sealed segment range. d0
    // (tok=true) is in the BM25 corpus, so deleting it MUST shrink doc_count /
    // total_doc_len; d2 (tok=false) exercises a non-text-bearing delete. On
    // beta, delete bd1 (tok=true) so both collections are GC-tested.
    let deletes_alpha = ["d0", "d2"];
    let deletes_beta = ["bd1"];
    for eid in deletes_alpha {
        persisted.delete("alpha", eid, None).unwrap();
    }
    for eid in deletes_beta {
        persisted.delete("beta", eid, None).unwrap();
    }

    // Live tail (docids > sealed n_docs), some sharing the deleted docs' terms
    // so a resurrected base doc would be detectable as an extra set member.
    let tail = [
        (
            "d4",
            2.5,
            "a",
            "red",
            true,
            0u64,
            [0.11f32, 0.22, 0.33, 0.44],
        ),
        ("d5", 6.5, "b", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
    ];
    for (eid, n, kw, tag, tok, sig, emb) in tail {
        index_doc(&persisted, "alpha", eid, n, kw, tag, tok, sig, &emb);
        index_doc(
            &persisted,
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        );
    }
    persisted.flush_to_segments(dir.path(), 6).unwrap(); // S2: re-seal must GC deletes

    // Fresh reopen ONLY from the checkpoint dir.
    let reopened = Arc::new(Engine::new());
    let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
    assert_eq!(seq, 6, "second checkpoint's seq must win");

    // Oracle: pure-live engine, identical op sequence, NEVER flushed.
    let pure = Arc::new(Engine::new());
    seed(&pure);
    for eid in deletes_alpha {
        pure.delete("alpha", eid, None).unwrap();
    }
    for eid in deletes_beta {
        pure.delete("beta", eid, None).unwrap();
    }
    for (eid, n, kw, tag, tok, sig, emb) in tail {
        index_doc(&pure, "alpha", eid, n, kw, tag, tok, sig, &emb);
        index_doc(
            &pure,
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        );
    }

    let qa = [0.15f32, 0.25, 0.35, 0.45];
    // Every leg of BOTH collections must match the pure-live oracle, SETS and
    // byte-identical f32 BM25 scores. The BM25 leg of `battery` is the corpus
    // teeth: if a deleted tok=true doc were resurrected, doc_count/avgdl shift
    // and EVERY surviving doc's BM25 score changes — a byte diff.
    assert_eq!(
        battery(&reopened, "alpha"),
        battery(&pure, "alpha"),
        "alpha legs diverged after delete+checkpoint"
    );
    assert_eq!(
        battery(&reopened, "beta"),
        battery(&pure, "beta"),
        "beta legs diverged after delete+checkpoint"
    );
    // Ordered kNN: a resurrected vector row would re-enter the scan and reorder.
    assert_eq!(
        knn(&reopened, "alpha", &qa),
        knn(&pure, "alpha", &qa),
        "alpha kNN diverged after delete+checkpoint"
    );
    assert_eq!(
        knn(&reopened, "beta", &qa),
        knn(&pure, "beta", &qa),
        "beta kNN diverged after delete+checkpoint"
    );

    // doc_count: base 4 - 2 deleted + 2 tail = 4 (alpha); 4 - 1 + 2 = 5 (beta).
    assert_eq!(
        reopened.stats("alpha").unwrap().documents_indexed,
        4,
        "alpha doc_count inflated by resurrected docs"
    );
    assert_eq!(
        reopened.stats("beta").unwrap().documents_indexed,
        5,
        "beta doc_count inflated by resurrected docs"
    );

    // DIRECT GC assertion: every deleted eid is absent from every leg AND from
    // direct value retrieval after reopen — it was GC'd, not resurrected.
    let alpha_hits: BTreeSet<String> = {
        // A broad query that would surface a resurrected doc on any field.
        let mut s = BTreeSet::new();
        for kwv in ["a", "b", "c", "d"] {
            let r = run(
                &reopened,
                "alpha",
                QueryNode::Term(TermQuery {
                    field: "kw".into(),
                    value: FieldValue::String(kwv.into()),
                }),
                100_000,
            );
            s.extend(r.into_iter().map(|(e, _)| e));
        }
        s
    };
    for eid in deletes_alpha {
        assert!(
            !alpha_hits.contains(eid),
            "deleted alpha eid `{eid}` RESURRECTED after reopen"
        );
    }
    // And the deleted eid resolves to NO value through the segment-aware
    // accessors (its interner slot is a tombstone, excluded by eid_fields).
    {
        let state = reopened.state.read().unwrap();
        let coll = state.collections.get("alpha").unwrap();
        for eid in deletes_alpha {
            // A deleted eid is still interned (positionally stable docids), but
            // it must carry NO live field coverage.
            if let Some(id) = coll.interner.id(eid) {
                assert!(
                    coll.eid_fields.get(&id).is_none_or(|fs| fs.is_empty()),
                    "deleted alpha eid `{eid}` (id {id}) still has live field coverage after reopen",
                );
            }
        }
    }
}
