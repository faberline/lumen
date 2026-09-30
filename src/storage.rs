// CODEGEN-BEGIN
//! In-memory storage and query execution.
//!
//! The engine is `BTreeMap`-backed inverted indexes per field,
//! constructed by [`Engine::new`]. Single-pod, single-shard; durability
//! comes from the CBOR RDB snapshot path and (when segment persistence is
//! selected) the columnar mmap segment tier — not from this module.
//!
//! The shape below maps 1:1 to the field-type table in the README:
//!
//! | FieldType | Index                                  |
//! |-----------|----------------------------------------|
//! | `text`    | `BTreeMap<token, BTreeSet<eid>>`       |
//! | `keyword` | `BTreeMap<value, BTreeSet<eid>>`       |
//! | `number`  | `BTreeMap<SortableF64, BTreeSet<eid>>` |
//! | `set`     | `BTreeMap<element, BTreeSet<eid>>`     |
//!
//! Every field also carries a per-`external_id` "forward" map so
//! re-indexing the same `(eid, field)` cleanly evicts the old postings
//! before appending the new ones.

// Moved to the index domain, application and infrastructure; re-exported
// until storage.rs becomes the compat facade, so lumen::storage keeps its
// public surface.
pub use crate::index::application::engine::collections::DropOutcome;
pub use crate::index::application::engine::index::MAX_INDEX_ITEMS;
pub use crate::index::application::engine::raft_dispatch::ApplyOutcome;
pub use crate::index::application::engine::reshard_apply::{
    ReshardApplyOutcome, ReshardEvictOutcome,
};
pub use crate::index::application::engine::reshard_prune::ReshardPruneOutcome;
pub use crate::index::application::engine::Engine;
pub use crate::index::domain::collection::coverage::{FieldNotAudited, ReindexNeeded};
pub use crate::index::domain::query::sort::MAX_SORT_KEYS;
pub use crate::index::domain::query::validate_query;
pub use crate::index::domain::sortable_f64::SortableF64;
pub use crate::index::domain::storage_error::StorageError;
pub use crate::index::infrastructure::collection_retirement::{
    collection_reclaimer_snapshot, CollectionReclaimerSnapshot,
};
pub use crate::index::infrastructure::snapshot_v1::{
    CollectionSnapshot, FieldIndexSnapshot, LegacyInvertedIndex, SnapshotV1,
};

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use roaring::RoaringBitmap;

#[cfg(test)]
use crate::index::application::checkpoint_capture::CheckpointCollectionIdentity;
#[cfg(test)]
use crate::index::domain::analysis::tokenize;
#[cfg(test)]
use crate::index::domain::field_index::FieldIndex;
#[cfg(test)]
use crate::index::domain::hash_index::HashIndex;
#[cfg(test)]
use crate::index::domain::keyword_index::KeywordIndex;
#[cfg(test)]
use crate::index::domain::number_index::NumberIndex;
#[cfg(test)]
use crate::index::domain::postings::Postings;
#[cfg(test)]
use crate::index::domain::query::clause::{clause_matches, eval_filter_bitmap};
#[cfg(test)]
use crate::index::domain::query::knn::eval_hamming;
#[cfg(test)]
use crate::index::domain::query::page_cursor::make_cursor;
#[cfg(test)]
use crate::index::domain::query::selectivity::{
    estimate_selectivity, is_exact_hamming, is_predicable, plan_filter_candidates,
    SPARSE_CANDIDATE_MAX,
};
#[cfg(test)]
use crate::index::domain::set_index::SetIndex;
#[cfg(test)]
use crate::index::domain::sortable_f64::MISSING_SORTABLE_F64_BITS;
#[cfg(test)]
use crate::index::domain::text_index::TextIndex;
#[cfg(test)]
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
#[cfg(test)]
use crate::shared_kernel::types::document::ReplaceDocItem;
#[cfg(test)]
use crate::shared_kernel::types::query::{
    HammingQuery, HasChildQuery, KnnQuery, MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery,
    SortMissing, SortOrder, SortSpec, TermQuery, TermsQuery,
};
#[cfg(test)]
use crate::shared_kernel::types::schema::{Analyzer, FieldSpec};
#[cfg(test)]
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};
#[cfg(test)]
use crate::shared_kernel::types::{
    document::{FieldValue, IndexRequest, ReplaceDocsRequest},
    schema::{CreateCollectionRequest, FieldType},
};

// ---------------------------------------------------------------------------
// Exact hamming as an AND filter (#4246): `and[hamming(max_distance 0), match]`
// must plan as filter-driven (bitmap driver / per-doc predicate) and return
// the SAME hit set with byte-identical scores as the materialize-and-intersect
// fallback, which a fuzzy hamming (`max_distance > 0`) still takes.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod exact_hamming_filter_tests {
    use super::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `sig` (Hash) + `body` (Text, whitespace-lower).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        CreateCollectionRequest { fields }
    }

    /// Even-weight code `(i << 1) | parity(i)`: every pair of hashes sits at
    /// Hamming distance ≥ 2, so `max_distance 1` (fallback path) and
    /// `max_distance 0` (filter path) select exactly the same docs.
    fn sig(i: u32) -> u64 {
        ((i as u64) << 1) | (i.count_ones() & 1) as u64
    }

    /// Every doc shares most tokens (like the durable workload's ngram text)
    /// and carries one unique token, so a full-text `match … op=and` selects
    /// exactly one doc while every token's posting spans the corpus.
    fn body(i: u32) -> String {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        format!("doc {i} shared token stream alpha beta gamma {parity}")
    }

    fn index(e: &Engine, eid: &str, hash: u64, text: &str) {
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "sig".into(),
                        value: FieldValue::String(format!("{hash:016x}")),
                        version: None,
                    },
                    crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "body".into(),
                        value: FieldValue::String(text.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn seed(n: u32) -> Arc<Engine> {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for i in 0..n {
            index(&e, &format!("d{i}"), sig(i), &body(i));
        }
        e
    }

    fn hamming_raw(hash: u64, max: u32) -> QueryNode {
        QueryNode::Hamming(HammingQuery {
            field: "sig".into(),
            hash: format!("{hash:016x}"),
            max_distance: max,
        })
    }

    fn hamming(i: u32, max: u32) -> QueryNode {
        hamming_raw(sig(i), max)
    }

    fn matchq(text: &str) -> QueryNode {
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: text.into(),
            op: MatchOp::And,
        })
    }

    /// (external_id, score_bits) — order-independent, byte-exact.
    fn run(e: &Engine, query: QueryNode) -> BTreeMap<String, u32> {
        e.search(
            "c",
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
    }

    #[test]
    fn exact_hamming_is_a_filter_and_fuzzy_is_not() {
        assert!(is_exact_hamming(&hamming(3, 0)));
        assert!(is_predicable(&hamming(3, 0)));
        assert!(!is_exact_hamming(&hamming(3, 1)));
        assert!(!is_predicable(&hamming(3, 1)));

        let e = seed(8);
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        assert_eq!(estimate_selectivity(coll, &hamming(3, 0)), 1);
        assert_eq!(estimate_selectivity(coll, &hamming(3, 1)), u64::MAX);
        // The exact hamming drives the AND ahead of a corpus-wide match, so
        // the match is scored over the hash hits, never materialized.
        assert!(
            estimate_selectivity(coll, &hamming(3, 0))
                <= estimate_selectivity(coll, &matchq("shared token")),
            "exact hamming must be the cheaper driver"
        );
    }

    #[test]
    fn exact_hamming_bitmap_and_predicate_match_eval_hamming() {
        let e = seed(16);
        // Seal the hash field so `hash_at` serves ids from the segment, then
        // add a live-tail doc: the filter reads must cover both sources.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            e.__seal_hash_field_to_segment("c", "sig", dir.path())
                .unwrap(),
            16
        );
        index(&e, "d16", sig(16), &body(16));

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        for i in [0u32, 5, 15, 16, 40] {
            let q = hamming(i, 0);
            let QueryNode::Hamming(hq) = &q else {
                unreachable!()
            };
            let want: RoaringBitmap = eval_hamming(coll, hq).unwrap().into_keys().collect();
            assert_eq!(want.len(), u64::from(i < 17), "sig({i}) hit count");
            let got = eval_filter_bitmap(coll, &q).unwrap();
            assert_eq!(got, want, "bitmap for sig({i})");
            for id in 0..17u32 {
                let pred = clause_matches(coll, &q, id).unwrap();
                assert_eq!(
                    pred,
                    want.contains(id).then_some(1.0),
                    "predicate sig({i}) on id {id}"
                );
            }
        }
    }

    #[test]
    fn exact_hamming_and_match_scores_are_byte_identical_to_fallback() {
        let e = seed(24);
        for i in [0u32, 7, 23] {
            let full = body(i);
            // Fallback: a fuzzy hamming is never predicable, and the pairwise
            // distance ≥ 2 makes `max_distance 1` select the same single doc.
            let fallback = run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&full)]));
            assert_eq!(fallback.len(), 1, "sig({i}) selects exactly its doc");
            assert!(fallback.contains_key(&format!("d{i}")));

            // Filter path, top-k entry (`eval_predicable_and_topk`).
            let topk = run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&full)]));
            assert_eq!(topk, fallback, "top-k filter path vs fallback for d{i}");

            // Filter path, general `eval_query` AND branch (reached through a
            // single-child `or`), with the conjuncts in the other order.
            let general = run(
                &e,
                QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 0)])]),
            );
            let general_fallback = run(
                &e,
                QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 1)])]),
            );
            assert_eq!(
                general, general_fallback,
                "eval_query filter path vs fallback for d{i}"
            );
            assert_eq!(
                general, fallback,
                "conjunct order must not change the score for d{i}"
            );

            // A wrong-field text under the same hash misses on every path.
            let wrong = format!("readback mismatch window {i} body");
            assert!(run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&wrong)])).is_empty());
            assert!(run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&wrong)])).is_empty());
        }
    }

    #[test]
    fn fuzzy_hamming_keeps_its_graded_score_on_the_fallback() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index(&e, "near", 0, "tok");
        index(&e, "far", 1, "tok"); // distance 1 from the query hash 0

        let bm25 = run(&e, matchq("tok"));
        let got = run(&e, QueryNode::And(vec![hamming_raw(0, 1), matchq("tok")]));
        assert_eq!(got.len(), 2);
        assert_eq!(
            got["near"],
            (1.0f32 + f32::from_bits(bm25["near"])).to_bits()
        );
        assert_eq!(
            got["far"],
            ((64 - 1) as f32 / 64.0 + f32::from_bits(bm25["far"])).to_bits()
        );
        // And the exact form drops the distance-1 doc.
        let exact = run(&e, QueryNode::And(vec![hamming_raw(0, 0), matchq("tok")]));
        assert_eq!(exact.len(), 1);
        assert_eq!(exact["near"], got["near"]);
    }

    fn sparse_layered_match_does_not_materialize_for_planning(general: bool, analyzer: Analyzer) {
        use crate::persistence::infrastructure::composed_segment::TextPostingAt;

        let e = Arc::new(Engine::new());
        let mut schema = schema();
        schema.fields.get_mut("body").unwrap().analyzer = Some(analyzer);
        e.create_collection("c", schema).unwrap();
        let text_for = |i| match analyzer {
            Analyzer::Ngram => format!(
                "ngram document {i} slot 0 {}",
                "durable search token ".repeat(12)
            ),
            _ => body(i),
        };
        for i in 0..128 {
            index(&e, &format!("d{i}"), sig(i), &text_for(i));
        }
        let text = text_for(7);
        let query = QueryNode::And(vec![hamming(7, 0), matchq(&text)]);
        let query = if general {
            QueryNode::Or(vec![query])
        } else {
            query
        };
        let expected = run(&e, query.clone());
        assert_eq!(expected.len(), 1);
        assert!(expected.contains_key("d7"));
        let wrong = matchq(&format!("{text} neverindexedpredicate"));
        let wrong_query = QueryNode::And(vec![hamming(7, 0), wrong.clone()]);
        let not_query = QueryNode::And(vec![hamming(7, 0), QueryNode::Not(Box::new(wrong))]);
        assert!(run(&e, wrong_query.clone()).is_empty());
        let expected_not = run(&e, not_query.clone());

        let dir = tempfile::tempdir().unwrap();
        e.__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();
        // A real replacement layer prevents the dense-base df shortcut.
        // It carries the same row, so the in-memory result remains the oracle.
        let tokens = tokenize::tokenize(&text, analyzer);
        let mut postings: BTreeMap<String, Postings> = BTreeMap::new();
        for token in &tokens {
            let posting = postings.entry(token.clone()).or_default();
            posting.upsert(0, posting.tf(0).unwrap_or(0) + 1);
        }
        let layer_path = dir.path().join("replacement.lseg");
        crate::persistence::infrastructure::segment::text_writer::write_text_segment(
            &layer_path,
            1,
            &postings,
            &[tokens.len() as u32],
            &[true],
            1,
            tokens.len() as u64,
        )
        .unwrap();
        let reader = Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&layer_path).unwrap(),
        );
        let segment = {
            let mut state = e.state.write().unwrap();
            let coll = state.collections.get_mut("c").unwrap();
            // The fixture swaps the segment directly instead of publishing
            // through the normal write path. Do not reuse its live oracle.
            coll.clear_search_cache();
            let FieldIndex::Text { idx, .. } = coll.fields.get_mut("body").unwrap() else {
                unreachable!()
            };
            let segment = Arc::new(
                idx.segment
                    .as_ref()
                    .unwrap()
                    .with_delta(reader, vec![7])
                    .unwrap(),
            );
            idx.segment = Some(segment.clone());
            segment
        };
        assert!(matches!(
            segment.text_posting_at(&tokens[0], &[7], |_| false),
            Some(TextPostingAt::Sparse { .. })
        ));

        crate::persistence::infrastructure::composed_segment::reset_text_term_probes();
        assert_eq!(run(&e, query), expected, "layered BM25 score bits");
        let probes = crate::persistence::infrastructure::composed_segment::text_term_probes();
        let distinct = tokens.iter().collect::<BTreeSet<_>>().len() as u64;
        assert!(probes > 0, "the query must read the layered index");
        assert!(
            probes <= distinct,
            "planning probed {probes} postings for {distinct} distinct terms"
        );
        assert!(
            matches!(
                segment.text_posting_at(&tokens[0], &[7], |_| false),
                Some(TextPostingAt::Sparse { .. })
            ),
            "a one-document filter must not fill the whole-posting cache just to plan its match"
        );
        assert!(run(&e, wrong_query).is_empty());
        assert_eq!(run(&e, not_query), expected_not);
    }

    #[test]
    fn sparse_layered_topk_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::WhitespaceLower);
    }

    #[test]
    fn sparse_layered_general_and_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::WhitespaceLower);
    }

    #[test]
    fn sparse_layered_ngram_topk_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::Ngram);
    }

    #[test]
    fn sparse_layered_ngram_general_and_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::Ngram);
    }

    #[test]
    fn sparse_filter_planning_checks_actual_hash_collision_count() {
        for n in [SPARSE_CANDIDATE_MAX as u32, SPARSE_CANDIDATE_MAX as u32 + 1] {
            let e = seed(n);
            for i in 0..n {
                index(&e, &format!("d{i}"), sig(0), &body(i));
            }
            let filter = hamming(0, 0);
            // The absent term has estimate zero. Only a truly bounded set may
            // skip this estimate; above the bound the original match driver wins.
            let absent = matchq("neverindexedpredicate");
            let state = e.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let plan = plan_filter_candidates(coll, &[&filter], &[], &[&absent]).unwrap();
            if u64::from(n) <= SPARSE_CANDIDATE_MAX {
                let ids = plan
                    .expect("bounded actual candidates")
                    .resolve(coll, &[&filter], &[])
                    .unwrap();
                assert_eq!(ids.len(), u64::from(n));
            } else {
                assert!(
                    plan.is_none(),
                    "large collisions must retain the original text estimate"
                );
            }
            drop(state);
            assert!(run(&e, QueryNode::And(vec![filter.clone(), absent])).is_empty());
            let text = matchq("7");
            let exact = run(&e, QueryNode::And(vec![filter, text.clone()]));
            let fallback = run(&e, QueryNode::And(vec![hamming(0, 1), text]));
            assert_eq!(exact, fallback);
            assert_eq!(exact.len(), 1);
            assert!(exact.contains_key("d7"));
        }
    }
}

// ---------------------------------------------------------------------------
// Engine-level checkpoint (Stage 2 Phase 2f-2): the disk engine as the running
// binary's persistence. Two contracts:
//   (a) ENGINE REOPEN — a multi-collection, all-field-type engine, flushed to a
//       checkpoint dir and reopened into a FRESH engine, answers every query leg
//       identically (the disk engine IS a faithful persistence).
//   (b) IDEMPOTENT DOUBLE-FLUSH — flush, index MORE docs, flush again, reopen
//       yields ALL docs (base + tail) identical to a pure-live engine. This is
//       the re-seal-after-drop proof: the second flush reads base docs from the
//       prior segment (their live forward was dropped), not the empty forward map.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod checkpoint_engine_tests {
    use super::*;
    use crate::shared_kernel::types::{
        query::{KnnQuery, MatchOp, MatchQuery, RangeQuery, TermQuery, TermsQuery},
        schema::{VectorBackend, VectorMetric},
    };
    use std::sync::Arc;

    const DIM: usize = 4;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn vec_fieldspec() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(DIM as u32),
            metric: Some(VectorMetric::L2),
            backend: Some(VectorBackend::FlatCpu),
            quantize: None,
        }
    }

    /// Multi-field corpus matching the triple-path schema: num (Number), kw
    /// (Keyword), tags (Set), body (Text), sig (Hash), emb (Vector).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("num".into(), fieldspec(FieldType::Number, None));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert("tags".into(), fieldspec(FieldType::Set, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
        fields.insert("emb".into(), vec_fieldspec());
        CreateCollectionRequest { fields }
    }

    fn index_doc(
        e: &Engine,
        coll: &str,
        eid: &str,
        n: f64,
        kw: &str,
        tag: &str,
        tok: bool,
        sig: u64,
        emb: &[f32],
    ) {
        let items = vec![
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "num".into(),
                value: FieldValue::Number(n),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(vec![tag.into()]),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "body".into(),
                value: FieldValue::String(if tok {
                    "tok filler".into()
                } else {
                    "filler".into()
                }),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "sig".into(),
                value: FieldValue::String(format!("{sig:016x}")),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "emb".into(),
                value: FieldValue::Vector(emb.to_vec()),
                version: None,
            },
        ];
        e.index(
            coll,
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    fn req(query: QueryNode, limit: u32) -> SearchRequest {
        SearchRequest {
            query,
            limit,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, coll: &str, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
        e.search(coll, req(query, limit))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score.to_bits()))
            .collect()
    }

    fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }
    fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
    }

    fn driven(extra: QueryNode) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            extra,
        ])
    }

    /// The full query battery for one collection (predicate legs go through the
    /// segment-aware per-doc accessors; kNN/hamming/bm25 through the segment scan).
    fn battery(e: &Engine, coll: &str) -> Vec<(BTreeSet<String>, BTreeMap<String, u32>)> {
        let legs = vec![
            driven(QueryNode::Range(RangeQuery {
                field: "num".into(),
                gt: None,
                gte: Some(RangeBound::Number(2.0)),
                lt: Some(RangeBound::Number(8.0)),
                lte: None,
            })),
            driven(QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String("a".into()),
            })),
            driven(QueryNode::Terms(TermsQuery {
                field: "tags".into(),
                values: vec![FieldValue::String("red".into())],
            })),
            QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String("b".into()),
            }),
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
                field: "sig".into(),
                hash: format!("{:016x}", 0u64),
                max_distance: 8,
            }),
        ];
        legs.into_iter()
            .map(|q| {
                let r = run(e, coll, q, 100_000);
                (set_of(&r), scores_of(&r))
            })
            .collect()
    }

    fn knn(e: &Engine, coll: &str, q: &[f32]) -> Vec<(String, u32)> {
        run(
            e,
            coll,
            QueryNode::Knn(KnnQuery {
                field: "emb".into(),
                vector: q.to_vec(),
                k: 8,
            }),
            8,
        )
    }

    // Some fixed multi-collection corpus. Two collections, all field types.
    fn seed(e: &Engine) {
        e.create_collection("alpha", schema()).unwrap();
        e.create_collection("beta", schema()).unwrap();
        let docs = [
            ("d0", 1.0, "a", "red", true, 0u64, [0.1f32, 0.2, 0.3, 0.4]),
            ("d1", 3.0, "b", "blue", true, 3, [0.9, 0.8, 0.7, 0.6]),
            ("d2", 5.0, "a", "red", false, 7, [0.5, 0.5, 0.5, 0.5]),
            ("d3", 7.0, "c", "green", true, 1, [0.2, 0.4, 0.6, 0.8]),
        ];
        for (eid, n, kw, tag, tok, sig, emb) in docs {
            index_doc(e, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                e,
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
    }

    // ----- (a) ENGINE REOPEN -------------------------------------------------
    #[test]
    fn flush_then_reopen_into_fresh_engine_is_identical() {
        let live = Arc::new(Engine::new());
        seed(&live);

        let qa = [0.15f32, 0.25, 0.35, 0.45];
        let live_battery_alpha = battery(&live, "alpha");
        let live_battery_beta = battery(&live, "beta");
        let live_knn_alpha = knn(&live, "alpha", &qa);

        let dir = tempfile::tempdir().unwrap();
        live.flush_to_segments(dir.path(), 11).unwrap();

        // Fresh engine reopened ONLY from the checkpoint dir (no CBOR, no log).
        let reopened = Arc::new(Engine::new());
        let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
        assert_eq!(
            seq, 11,
            "applied_seq must round-trip through the checkpoint"
        );

        assert_eq!(
            reopened.list_collections().unwrap().len(),
            2,
            "both collections reopened"
        );
        assert_eq!(
            battery(&reopened, "alpha"),
            live_battery_alpha,
            "alpha legs diverged after reopen"
        );
        assert_eq!(
            battery(&reopened, "beta"),
            live_battery_beta,
            "beta legs diverged after reopen"
        );
        assert_eq!(
            knn(&reopened, "alpha", &qa),
            live_knn_alpha,
            "alpha kNN diverged after reopen"
        );
    }

    // ----- (b) IDEMPOTENT DOUBLE-FLUSH (re-seal-after-drop proof) -------------
    #[test]
    fn double_flush_with_tail_matches_pure_live() {
        // The persisted engine: seed, FLUSH (drops forward for base docs), index
        // MORE docs (the live tail), FLUSH AGAIN, reopen.
        let persisted = Arc::new(Engine::new());
        seed(&persisted);
        let dir = tempfile::tempdir().unwrap();
        persisted.flush_to_segments(dir.path(), 4).unwrap(); // first checkpoint

        // After the first flush the base docs' forward maps are dropped. Add a
        // tail of new docs whose docids are > the sealed n_docs.
        let tail = [
            (
                "d4",
                2.5,
                "b",
                "red",
                true,
                0u64,
                [0.11f32, 0.22, 0.33, 0.44],
            ),
            ("d5", 6.5, "a", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
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
        // SECOND flush: this RE-SEALS. Base docs must be gathered from the prior
        // segment (their forward is empty), the tail from the live forward. If the
        // gather read raw `forward`, the base docs would seal as ABSENT here.
        persisted.flush_to_segments(dir.path(), 6).unwrap();

        let reopened = Arc::new(Engine::new());
        let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
        assert_eq!(seq, 6, "second checkpoint's seq must win");

        // The oracle: a pure-live engine that NEVER flushed, with the SAME docs.
        let pure = Arc::new(Engine::new());
        seed(&pure);
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
        // EVERY leg of BOTH collections must match the pure-live oracle — base AND
        // tail docs (sets AND byte-identical scores). This is the re-seal-after-drop
        // correctness proof: the second flush gathered base docs from the prior
        // segment (their forward was dropped) and the tail from the live state.
        assert_eq!(
            battery(&reopened, "alpha"),
            battery(&pure, "alpha"),
            "alpha legs diverged after double-flush"
        );
        assert_eq!(
            battery(&reopened, "beta"),
            battery(&pure, "beta"),
            "beta legs diverged after double-flush"
        );
        assert_eq!(
            knn(&reopened, "alpha", &qa),
            knn(&pure, "alpha", &qa),
            "alpha kNN diverged after double-flush"
        );
        assert_eq!(
            knn(&reopened, "beta", &qa),
            knn(&pure, "beta", &qa),
            "beta kNN diverged after double-flush"
        );

        // And direct doc-count parity (base 4 + tail 2 = 6 per collection).
        assert_eq!(reopened.stats("alpha").unwrap().documents_indexed, 6);
        assert_eq!(reopened.stats("beta").unwrap().documents_indexed, 6);
    }

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
}

// ---------------------------------------------------------------------------
// #179: an offset cursor combined with `sort` must be REJECTED (400), never
// silently fall through to score ranking and ignore the sort. Sequential
// sorted paging uses the keyset cursor handed back in the response; native
// `offset` provides direct page jumps without client over-fetch.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod offset_sort_guard_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword));
        fields.insert("code".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    /// Five docs d0..d4 with prices 10,20,30,40,50, all `cat = "x"`.
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for (i, p) in [10.0_f64, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "price".into(),
                            value: FieldValue::Number(*p),
                            version: None,
                        },
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "cat".into(),
                            value: FieldValue::String("x".into()),
                            version: None,
                        },
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "code".into(),
                            value: FieldValue::String(format!("k{i}")),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        }
        e
    }

    fn cat_x() -> QueryNode {
        QueryNode::Term(TermQuery {
            field: "cat".into(),
            value: FieldValue::String("x".into()),
        })
    }

    fn base() -> SearchRequest {
        SearchRequest {
            query: cat_x(),
            limit: 2,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn sort_price_asc() -> Vec<SortSpec> {
        vec![SortSpec {
            field: "price".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]
    }

    fn ids(resp: &SearchResponse) -> Vec<String> {
        resp.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: an offset cursor (N>0) + a non-empty sort → 400 UnsupportedSort,
    /// instead of silently mis-ordering by score.
    #[test]
    fn offset_cursor_with_sort_is_rejected() {
        let e = seed();
        let mut r = base();
        r.sort = Some(sort_price_asc());
        r.cursor = Some(make_cursor(2)); // {"offset":2}
        let err = e.search("c", r).unwrap_err();
        let se = err
            .downcast_ref::<StorageError>()
            .expect("offset+sort error must be a StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "offset+sort must map to UnsupportedSort (HTTP 400), got {se:?}"
        );
    }

    /// R2: an offset cursor WITHOUT sort still paginates relevance/constant
    /// results — the guard must not regress the unsorted offset path.
    #[test]
    fn offset_cursor_without_sort_paginates() {
        let e = seed();
        let mut r = base();
        r.cursor = Some(make_cursor(2));
        let resp = e
            .search("c", r)
            .expect("offset cursor without sort must succeed");
        assert_eq!(resp.total, 5, "exact total across the unsorted match set");
        assert!(resp.hits.len() <= 2, "page honors the limit");
    }

    #[test]
    fn native_offset_applies_after_numeric_sort() {
        let e = seed();
        let mut r = base();
        r.sort = Some(sort_price_asc());
        r.offset = 2;
        let resp = e.search("c", r).expect("native sorted offset succeeds");
        assert_eq!(ids(&resp), ["d2", "d3"]);
        assert_eq!(resp.total, 5);
        assert!(
            resp.cursor.is_none(),
            "an offset jump does not emit a cursor"
        );
    }

    #[test]
    fn native_offset_applies_after_keyword_and_composite_sort() {
        let e = seed();
        let mut r = base();
        r.offset = 1;
        r.sort = Some(vec![
            SortSpec {
                field: "cat".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            },
            SortSpec {
                field: "code".into(),
                order: SortOrder::Desc,
                missing: SortMissing::Exclude,
            },
        ]);
        let resp = e
            .search("c", r)
            .expect("native keyword/composite offset succeeds");
        assert_eq!(ids(&resp), ["d3", "d2"]);
    }

    #[test]
    fn native_offset_applies_after_score_ordering() {
        let e = seed();
        let mut r = base();
        r.offset = 2;
        let resp = e.search("c", r).expect("native score offset succeeds");
        assert_eq!(ids(&resp), ["d2", "d3"]);
    }

    #[test]
    fn nonzero_native_offset_and_cursor_are_rejected() {
        let e = seed();
        let mut r = base();
        r.offset = 1;
        r.cursor = Some(make_cursor(1));
        let err = e.search("c", r).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<StorageError>(),
            Some(StorageError::InvalidPagination(_))
        ));
    }

    /// R3: a keyset cursor combined with sort paginates correctly (page 1 with
    /// no cursor hands back a keyset cursor; following it yields the next page).
    #[test]
    fn keyset_cursor_with_sort_paginates() {
        let e = seed();

        let mut p1 = base();
        p1.sort = Some(sort_price_asc());
        let r1 = e.search("c", p1).expect("sorted page 1 must succeed");
        assert_eq!(ids(&r1), vec!["d0".to_string(), "d1".to_string()]);
        let cursor = r1
            .cursor
            .expect("a full sorted page must hand back a keyset cursor");

        let mut p2 = base();
        p2.sort = Some(sort_price_asc());
        p2.cursor = Some(cursor);
        let r2 = e
            .search("c", p2)
            .expect("keyset + sort page 2 must succeed");
        assert_eq!(ids(&r2), vec!["d2".to_string(), "d3".to_string()]);
    }
}

// ---------------------------------------------------------------------------
// #184: external-version last-write-wins. An IndexItem may carry an optional
// `version`; lumen keeps the highest version per (external_id, field) and drops
// strictly-older writes. Absent version = arrival order (today's behavior).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod external_version_lww_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "price".into(),
            FieldSpec {
                field_type: FieldType::Number,
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

    fn setup() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        e
    }

    fn write(e: &Engine, eid: &str, price: f64, version: Option<u64>) {
        e.index(
            "c",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: eid.into(),
                    field: "price".into(),
                    value: FieldValue::Number(price),
                    version,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }

    /// external_ids whose `price` equals `price`.
    fn matches_price(e: &Engine, price: f64) -> Vec<String> {
        let req = SearchRequest {
            query: QueryNode::Term(TermQuery {
                field: "price".into(),
                value: FieldValue::Number(price),
            }),
            limit: 100,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        };
        e.search("c", req)
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect()
    }

    /// R1: a versioned write older than the stored version is dropped.
    #[test]
    fn stale_versioned_write_is_dropped() {
        let e = setup();
        write(&e, "d0", 10.0, Some(5));
        write(&e, "d0", 20.0, Some(3)); // stale: 3 < stored 5
        assert_eq!(
            matches_price(&e, 10.0),
            vec!["d0".to_string()],
            "value must remain at the v5 write"
        );
        assert!(
            matches_price(&e, 20.0).is_empty(),
            "the stale v3 write must not apply"
        );
    }

    /// R2: a newer versioned write advances the cell.
    #[test]
    fn newer_versioned_write_wins() {
        let e = setup();
        write(&e, "d0", 10.0, Some(5));
        write(&e, "d0", 20.0, Some(6)); // newer: 6 > stored 5
        assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
        assert!(matches_price(&e, 10.0).is_empty());
    }

    /// R3: writes without a version apply in arrival order (last wins) —
    /// unchanged from today.
    #[test]
    fn unversioned_writes_keep_arrival_order() {
        let e = setup();
        write(&e, "d0", 10.0, None);
        write(&e, "d0", 20.0, None);
        assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
        assert!(matches_price(&e, 10.0).is_empty());
    }
}

// ---------------------------------------------------------------------------
// #180: opt-in `missing: first|last|exclude` on a sort key. exclude (default)
// drops rows lacking the value (today's behavior); first/last keep them, placed
// before/after the present rows, and count them in an exact total.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod sort_missing_tests {
    use super::*;
    use crate::index::domain::query::sort_missing::{
        MATERIALIZED_SORT_COMPARISONS, MATERIALIZED_SORT_RETAINED_HIGH_WATER,
    };
    use crate::shared_kernel::types::{
        document::IndexItem,
        query::{ExistsQuery, SortMissing},
    };

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    fn idx(e: &Engine, eid: &str, price: Option<f64>) {
        let mut items = vec![IndexItem {
            external_id: eid.into(),
            field: "cat".into(),
            value: FieldValue::String("x".into()),
            version: None,
        }];
        if let Some(p) = price {
            items.push(IndexItem {
                external_id: eid.into(),
                field: "price".into(),
                value: FieldValue::Number(p),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// d0=10, d1=20 have a price; d2 has none (all share cat="x").
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", Some(10.0));
        idx(&e, "d1", Some(20.0));
        idx(&e, "d2", None);
        e
    }

    fn search(
        e: &Engine,
        missing: SortMissing,
        limit: u32,
        cursor: Option<String>,
    ) -> SearchResponse {
        e.search(
            "c",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "cat".into(),
                    value: FieldValue::String("x".into()),
                }),
                limit,
                offset: 0,
                cursor,
                routing_key: None,
                sort: Some(vec![SortSpec {
                    field: "price".into(),
                    order: SortOrder::Asc,
                    missing,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn ids(r: &SearchResponse) -> Vec<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: missing:last places the value-less row after present rows, counted.
    #[test]
    fn missing_last_placed_after_and_counted() {
        let e = seed();
        let r = search(&e, SortMissing::Last, 100, None);
        assert_eq!(ids(&r), vec!["d0", "d1", "d2"]);
        assert_eq!(r.total, 3);
    }

    /// R2: missing:first places the value-less row before present rows.
    #[test]
    fn missing_first_placed_before() {
        let e = seed();
        let r = search(&e, SortMissing::First, 100, None);
        assert_eq!(ids(&r), vec!["d2", "d0", "d1"]);
        assert_eq!(r.total, 3);
    }

    /// R3: default exclude drops the value-less row from results and total.
    #[test]
    fn exclude_default_drops_missing() {
        let e = seed();
        let r = search(&e, SortMissing::Exclude, 100, None);
        assert_eq!(ids(&r), vec!["d0", "d1"]);
        assert_eq!(r.total, 2);
    }

    /// R4: the missing-inclusive order paginates, each row once, exact total.
    #[test]
    fn missing_paginates_each_once() {
        let e = seed();
        let p1 = search(&e, SortMissing::Last, 2, None);
        assert_eq!(ids(&p1), vec!["d0", "d1"]);
        assert_eq!(p1.total, 3);
        let cursor = p1.cursor.expect("a full page hands back a cursor");
        let p2 = search(&e, SortMissing::Last, 2, Some(cursor));
        assert_eq!(ids(&p2), vec!["d2"]);
        assert_eq!(p2.total, 3);
    }

    /// #3997: a single high-cardinality keyword key with `missing:last` must
    /// not send a small page through the generic full tuple-sort fallback.
    /// Values are deliberately permuted so the old all-row sort needs many
    /// comparisons; the new keyword planner streams dictionary buckets.
    #[test]
    fn keyword_missing_last_small_page_bypasses_full_tuple_sort() {
        const DOCS: usize = 1_024;
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for i in 0..DOCS {
            let mut items = vec![IndexItem {
                external_id: format!("d{i:04}"),
                field: "cat".into(),
                value: FieldValue::String("x".into()),
                version: None,
            }];
            if i % 8 != 0 {
                items.push(IndexItem {
                    external_id: format!("d{i:04}"),
                    field: "kw".into(),
                    value: FieldValue::String(format!("k{:04}", DOCS - i)),
                    version: None,
                });
            }
            e.index(
                "c",
                IndexRequest {
                    items,
                    request_id: None,
                },
            )
            .unwrap();
        }

        let segment_dir = tempfile::tempdir().expect("keyword segment tempdir");
        e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
            .expect("seal high-cardinality keyword field");
        e.__seal_keyword_field_to_segment("c", "cat", segment_dir.path())
            .expect("seal exact exists filter field");

        MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.set(0));
        let response = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "kw".into(),
                        order: SortOrder::Asc,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(response.total, DOCS as u64);
        assert!(
            response.hits.iter().all(|hit| hit.score == 1.0),
            "keyword-present rows retain the constant filter score"
        );
        assert_eq!(
            ids(&response),
            (0..DOCS)
                .rev()
                .filter(|i| i % 8 != 0)
                .take(10)
                .map(|i| format!("d{i:04}"))
                .collect::<Vec<_>>()
        );
        assert!(
            MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.get()) <= DOCS as u64,
            "small-page keyword sort must not comparison-sort all {DOCS} matches"
        );
    }

    /// A sealed ordinal stream must merge tail-only values, an equal tail value,
    /// and a post-seal tombstone without materializing either dictionary.
    #[test]
    fn sealed_keyword_bucket_walk_merges_tombstone_and_live_tail_both_orders() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        let write = |eid: &str, keyword: &str| {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: eid.into(),
                            field: "cat".into(),
                            value: FieldValue::String("x".into()),
                            version: None,
                        },
                        IndexItem {
                            external_id: eid.into(),
                            field: "kw".into(),
                            value: FieldValue::String(keyword.into()),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        };
        write("base-a", "a");
        write("base-b", "b");
        write("base-c", "c");
        let segment_dir = tempfile::tempdir().unwrap();
        e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
            .unwrap();
        e.delete("c", "base-b", None).unwrap();
        write("tail-aa", "aa");
        write("tail-c", "c");
        write("tail-z", "z");

        let run = |order| {
            e.search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 100,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "kw".into(),
                        order,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap()
        };
        let asc = run(SortOrder::Asc);
        assert_eq!(
            ids(&asc),
            ["base-a", "tail-aa", "base-c", "tail-c", "tail-z"]
        );
        assert!(asc.hits.iter().all(|hit| hit.score == 1.0));
        let desc = run(SortOrder::Desc);
        assert_eq!(
            ids(&desc),
            ["tail-z", "base-c", "tail-c", "tail-aa", "base-a"]
        );
        assert!(desc.hits.iter().all(|hit| hit.score == 1.0));
    }

    /// Non-keyword/multi-key missing sorts use the exact bounded fallback.
    /// Counting may scan all matches, but retained tuples must never exceed the
    /// requested native prefix.
    #[test]
    fn missing_sort_fallback_retains_at_most_offset_plus_limit() {
        const DOCS: usize = 1_024;
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for i in 0..DOCS {
            idx(
                &e,
                &format!("d{i:04}"),
                (i % 5 != 0).then_some((DOCS - i) as f64),
            );
        }
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));
        let response = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 13,
                    offset: 7,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "price".into(),
                        order: SortOrder::Asc,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(response.total, DOCS as u64);
        assert_eq!(response.hits.len(), 13);
        assert!(
            MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()) <= 20,
            "fallback retained more than offset + limit tuples"
        );
    }
}

// ---------------------------------------------------------------------------
// #181: a has_child query may be combined with sort. It resolves to a parent
// bitmap via the materialized path, which is then sorted by a parent field.
// knn/rrf/hamming + sort stay rejected.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod has_child_sort_tests {
    use super::*;
    use crate::index::domain::query::sort_missing::MATERIALIZED_SORT_RETAINED_HIGH_WATER;
    use crate::shared_kernel::types::document::IndexItem;

    fn kw() -> FieldSpec {
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
    fn num() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Number,
            ..kw()
        }
    }

    fn order(e: &Engine, eid: &str, ts: f64, status: &str) {
        e.index(
            "orders",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: eid.into(),
                        field: "ts".into(),
                        value: FieldValue::Number(ts),
                        version: None,
                    },
                    IndexItem {
                        external_id: eid.into(),
                        field: "status".into(),
                        value: FieldValue::String(status.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn child(e: &Engine, parent: &str, sku: &str) {
        e.index(
            "items",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: format!("{parent}#0"),
                        field: "parent".into(),
                        value: FieldValue::String(parent.into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: format!("{parent}#0"),
                        field: "sku".into(),
                        value: FieldValue::String(sku.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    /// orders o1(ts100,open) o2(ts200,closed) o3(ts300,open); items link each
    /// order; o1,o2 have sku=S0, o3 has sku=X.
    fn setup() -> Engine {
        let e = Engine::new();
        let mut pf = BTreeMap::new();
        pf.insert("status".into(), kw());
        pf.insert("rank".into(), kw());
        pf.insert("ts".into(), num());
        e.create_collection("orders", CreateCollectionRequest { fields: pf })
            .unwrap();
        let mut cf = BTreeMap::new();
        cf.insert("parent".into(), kw());
        cf.insert("sku".into(), kw());
        e.create_collection("items", CreateCollectionRequest { fields: cf })
            .unwrap();
        order(&e, "o1", 100.0, "open");
        order(&e, "o2", 200.0, "closed");
        order(&e, "o3", 300.0, "open");
        child(&e, "o1", "S0");
        child(&e, "o2", "S0");
        child(&e, "o3", "X");
        e
    }

    fn has_child_s0() -> QueryNode {
        QueryNode::HasChild(HasChildQuery {
            collection: "items".into(),
            field: "parent".into(),
            query: Box::new(QueryNode::Term(TermQuery {
                field: "sku".into(),
                value: FieldValue::String("S0".into()),
            })),
        })
    }

    fn sort_ts_desc() -> Option<Vec<SortSpec>> {
        Some(vec![SortSpec {
            field: "ts".into(),
            order: SortOrder::Desc,
            missing: SortMissing::Exclude,
        }])
    }

    fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
        e.search(
            "orders",
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn ids(r: &SearchResponse) -> Vec<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: has_child + sort returns matching parents ordered by the parent field.
    #[test]
    fn has_child_sort_orders_parents() {
        let e = setup();
        let r = run(&e, has_child_s0(), sort_ts_desc());
        assert_eq!(ids(&r), vec!["o2", "o1"]); // ts 200, 100 desc
        assert_eq!(r.total, 2);
    }

    /// R2: has_child AND a parent-field filter, sorted, intersect + exact total.
    #[test]
    fn has_child_sort_composes_with_filter() {
        let e = setup();
        let q = QueryNode::And(vec![
            has_child_s0(),
            QueryNode::Term(TermQuery {
                field: "status".into(),
                value: FieldValue::String("open".into()),
            }),
        ]);
        let r = run(&e, q, sort_ts_desc());
        assert_eq!(ids(&r), vec!["o1"]); // o2 is closed
        assert_eq!(r.total, 1);
    }

    /// #3997: a child query keeps the exact bounded materialized fallback,
    /// even when its one sort key could otherwise use the keyword stream.
    #[test]
    fn has_child_missing_keyword_sort_keeps_materialized_fallback() {
        let e = setup();
        e.index(
            "orders",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "o1".into(),
                    field: "rank".into(),
                    value: FieldValue::String("a".into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));

        let r = run(
            &e,
            has_child_s0(),
            Some(vec![SortSpec {
                field: "rank".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Last,
            }]),
        );

        assert_eq!(ids(&r), vec!["o1", "o2"]);
        assert_eq!(r.total, 2);
        assert_eq!(
            MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()),
            2,
            "has_child must retain its bounded materialized fallback"
        );
    }

    /// R3: sort + knn is still rejected (400 UnsupportedSort).
    #[test]
    fn knn_sort_still_rejected() {
        let e = setup();
        let err = e
            .search(
                "orders",
                SearchRequest {
                    query: QueryNode::Knn(KnnQuery {
                        field: "v".into(),
                        vector: vec![0.1, 0.2],
                        k: 5,
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: sort_ts_desc(),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap_err();
        let se = err.downcast_ref::<StorageError>().expect("StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "knn + sort must stay rejected, got {se:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// #182: native `ids` query — filter by a set of external_ids, resolved through
// the interner. Constant-scored, predicable, composes under and/or/not + sort.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod ids_query_tests {
    use super::*;
    use crate::shared_kernel::types::{document::IndexItem, query::IdsQuery};

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("status".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    /// d0(10,open) d1(20,closed) d2(30,open).
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for (eid, price, status) in [
            ("d0", 10.0, "open"),
            ("d1", 20.0, "closed"),
            ("d2", 30.0, "open"),
        ] {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: eid.into(),
                            field: "price".into(),
                            value: FieldValue::Number(price),
                            version: None,
                        },
                        IndexItem {
                            external_id: eid.into(),
                            field: "status".into(),
                            value: FieldValue::String(status.into()),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        }
        e
    }

    fn ids_q(vals: &[&str]) -> QueryNode {
        QueryNode::Ids(IdsQuery {
            values: vals.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
        e.search(
            "c",
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn id_set(r: &SearchResponse) -> BTreeSet<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: returns exactly the named existing ids, skipping unknown ones.
    #[test]
    fn ids_returns_named_set_skips_unknown() {
        let e = seed();
        let r = run(&e, ids_q(&["d0", "d2", "does-not-exist"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect()
        );
        assert_eq!(r.total, 2);
    }

    /// R2: composes under a boolean AND with another clause.
    #[test]
    fn ids_composes_under_and() {
        let e = seed();
        let q = QueryNode::And(vec![
            ids_q(&["d0", "d1", "d2"]),
            QueryNode::Term(TermQuery {
                field: "status".into(),
                value: FieldValue::String("open".into()),
            }),
        ]);
        let r = run(&e, q, None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect(),
            "d1 is closed, so the AND drops it"
        );
    }

    /// R3: combines with sort (ids is a predicable filter).
    #[test]
    fn ids_combines_with_sort() {
        let e = seed();
        let r = run(
            &e,
            ids_q(&["d2", "d0"]),
            Some(vec![SortSpec {
                field: "price".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            }]),
        );
        let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
        assert_eq!(ordered, vec!["d0".to_string(), "d2".to_string()]); // 10 then 30
    }

    /// #1487/R1: a fully-deleted doc (all fields removed) must not match an
    /// `ids` query, consistent with `term`/`terms` on the same state.
    #[test]
    fn ids_excludes_fully_deleted_doc() {
        let e = seed();
        e.delete("c", "d1", None).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect(),
            "d1 was fully deleted and must not be a hit"
        );
        assert_eq!(r.total, 2);

        // Same doc-state, term query on the surviving docs' field agrees.
        let term_r = run(
            &e,
            QueryNode::Terms(TermsQuery {
                field: "status".into(),
                values: vec![
                    FieldValue::String("open".into()),
                    FieldValue::String("closed".into()),
                ],
            }),
            None,
        );
        assert_eq!(
            id_set(&term_r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect()
        );
    }

    /// #1487: mixed batch — a request naming live and deleted ids together
    /// returns only the live subset.
    #[test]
    fn ids_mixed_batch_returns_only_live_subset() {
        let e = seed();
        e.delete("c", "d0", None).unwrap();
        e.delete("c", "d2", None).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2", "does-not-exist"]), None);
        assert_eq!(
            id_set(&r),
            ["d1".to_string()].into_iter().collect(),
            "only the still-live doc survives, deleted + unknown ids drop out"
        );
        assert_eq!(r.total, 1);
    }

    /// #1487: partial-field deletion — a doc with SOME fields deleted but at
    /// least one field still live stays a hit under `ids` (matches the
    /// engine's liveness definition used by `term`: live iff any field
    /// lives).
    #[test]
    fn ids_matches_doc_with_partial_field_deletion() {
        let e = seed();
        // Delete only the `price` field on d0 — `status` is still live.
        e.delete("c", "d0", Some("price")).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d1".to_string(), "d2".to_string()]
                .into_iter()
                .collect(),
            "d0 still has a live field (status), so it remains a hit"
        );
        assert_eq!(r.total, 3);

        // Now delete the remaining field too — d0 becomes fully dead.
        e.delete("c", "d0", Some("status")).unwrap();
        let r2 = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r2),
            ["d1".to_string(), "d2".to_string()].into_iter().collect(),
            "d0 has no live fields left, so it drops out"
        );
    }
}

// ---------------------------------------------------------------------------
// #183: multi-key sort cap raised to MAX_SORT_KEYS (4). The generic plan
// compares every key in priority order; > 4 keys is rejected.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod multikey_sort_cap_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn schema() -> CreateCollectionRequest {
        let num = || FieldSpec {
            field_type: FieldType::Number,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        };
        let mut fields = BTreeMap::new();
        fields.insert("a".into(), num());
        fields.insert("b".into(), num());
        fields.insert("c".into(), num());
        CreateCollectionRequest { fields }
    }

    fn idx(e: &Engine, eid: &str, a: f64, b: f64, c: f64) {
        let item = |field: &str, v: f64| IndexItem {
            external_id: eid.into(),
            field: field.into(),
            value: FieldValue::Number(v),
            version: None,
        };
        e.index(
            "c",
            IndexRequest {
                items: vec![item("a", a), item("b", b), item("c", c)],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn sort_asc(fields: &[&str]) -> Vec<SortSpec> {
        fields
            .iter()
            .map(|f| SortSpec {
                field: (*f).into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            })
            .collect()
    }

    fn all() -> QueryNode {
        QueryNode::Range(RangeQuery {
            field: "a".into(),
            gte: None,
            gt: None,
            lte: None,
            lt: None,
        })
    }

    /// R1: a 3-key sort orders by each key in priority.
    #[test]
    fn three_key_sort_orders() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", 1.0, 1.0, 1.0);
        idx(&e, "d1", 1.0, 1.0, 2.0);
        idx(&e, "d2", 1.0, 2.0, 1.0);
        idx(&e, "d3", 2.0, 1.0, 1.0);
        let r = e
            .search(
                "c",
                SearchRequest {
                    query: all(),
                    limit: 100,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(sort_asc(&["a", "b", "c"])),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
        assert_eq!(ordered, vec!["d0", "d1", "d2", "d3"]);
        assert_eq!(r.total, 4);
    }

    /// R2: more than MAX_SORT_KEYS keys is rejected with UnsupportedSort.
    #[test]
    fn over_four_keys_rejected() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", 1.0, 1.0, 1.0);
        let err = e
            .search(
                "c",
                SearchRequest {
                    query: all(),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(sort_asc(&["a", "b", "c", "a", "b"])), // 5 keys
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap_err();
        let se = err.downcast_ref::<StorageError>().expect("StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "more than {MAX_SORT_KEYS} keys must be rejected, got {se:?}"
        );
    }
}
#[cfg(test)]
mod sparse_scalar_overlay_tests {
    use super::*;

    #[test]
    fn sealed_number_reads_replacement_overlay_and_then_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number.lseg");
        crate::persistence::infrastructure::segment::number_writer::write_number_segment(
            &path,
            1,
            &[Some(1.0)],
        )
        .unwrap();
        let mut number = NumberIndex::default();
        number.segment = Some(Arc::new(ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap(),
        ))));
        number.tombstones.insert(0);
        number.forward.insert(0, SortableF64::new(2.0).unwrap());
        assert_eq!(number.live_number_at(0).map(|v| v.to_f64()), Some(2.0));
        number.forward.remove(&0);
        assert_eq!(number.live_number_at(0), None);
    }

    #[test]
    fn sparse_high_delta_does_not_hide_middle_live_scalar_overlays() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.lseg");
        let delta = dir.path().join("delta.lseg");

        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &base,
            1,
            &[Some("base")],
            &BTreeMap::new(),
        )
        .unwrap();
        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &delta,
            1,
            &[Some("delta")],
            &BTreeMap::new(),
        )
        .unwrap();
        let keyword_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut keyword = KeywordIndex::default();
        keyword.segment = Some(Arc::new(keyword_view));
        keyword.forward.insert(2, "middle".into());
        assert_eq!(keyword.keyword_at(2).as_deref(), Some("middle"));

        let base_set = ["base".to_string()];
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &base,
            1,
            &[Some(base_set.as_slice())],
            &BTreeMap::new(),
        )
        .unwrap();
        let delta_set = ["delta".to_string()];
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &delta,
            1,
            &[Some(delta_set.as_slice())],
            &BTreeMap::new(),
        )
        .unwrap();
        let set_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut set = SetIndex::default();
        set.segment = Some(Arc::new(set_view));
        set.forward
            .insert(2, ["middle".to_string()].into_iter().collect());
        assert!(set.set_contains(2, "middle"));
        assert_eq!(
            set.set_members(2),
            Some(["middle".to_string()].into_iter().collect())
        );

        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &base,
            1,
            &[Some(1)],
        )
        .unwrap();
        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &delta,
            1,
            &[Some(3)],
        )
        .unwrap();
        let hash_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut hash = HashIndex::default();
        hash.segment = Some(Arc::new(hash_view));
        hash.forward.insert(2, 2);
        assert_eq!(hash.hash_at(2), Some(2));
    }
}

#[cfg(test)]
mod scalar_checkpoint_cut_tests {
    use super::*;

    #[test]
    fn checkpoint_capture_marks_unsealed_scalar_fields_with_empty_cuts() {
        let mut fields = BTreeMap::new();
        for (name, field_type) in [
            ("keyword", FieldType::Keyword),
            ("number", FieldType::Number),
            ("set", FieldType::Set),
        ] {
            fields.insert(
                name.into(),
                FieldSpec {
                    field_type,
                    analyzer: None,
                    multi: None,
                    dim: None,
                    metric: None,
                    backend: None,
                    quantize: None,
                },
            );
        }
        let engine = Engine::new();
        engine
            .create_collection("c", CreateCollectionRequest { fields })
            .unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let cuts = frozen.capture.scalar_cuts.get("c").unwrap();
        assert_eq!(cuts.len(), 3);
        for field in ["keyword", "number", "set"] {
            assert!(cuts.contains_key(field), "{field} must get an empty cut");
        }
    }

    #[test]
    fn scalar_checkpoint_retirement_keeps_a_mutation_after_preparation() {
        let engine = Engine::with_change_budget(
            crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(32 * 1024 * 1024),
        );
        engine
            .create_collection_inner(
                "c",
                CreateCollectionRequest {
                    fields: serde_json::from_value(serde_json::json!({
                        "keyword":{"type":"keyword"}, "number":{"type":"number"}
                    }))
                    .unwrap(),
                },
            )
            .unwrap();
        let write = |value: &str, include_number: bool| {
            let mut items = vec![crate::shared_kernel::types::document::IndexItem {
                external_id: "e".into(),
                field: "keyword".into(),
                value: FieldValue::String(value.into()),
                version: None,
            }];
            if include_number {
                items.push(crate::shared_kernel::types::document::IndexItem {
                    external_id: "e".into(),
                    field: "number".into(),
                    value: FieldValue::Number(3.0),
                    version: None,
                });
            }
            engine
                .index_inner(
                    "c",
                    IndexRequest {
                        items,
                        request_id: None,
                    },
                    None,
                    None,
                )
                .unwrap();
        };
        write("captured", true);
        // Select the full-base branch so both ordinary overlay retirement and
        // a newer ordinary overlay cross the real Engine publication seam.
        engine
            .state
            .write()
            .unwrap()
            .collections
            .get_mut("c")
            .unwrap()
            .requires_full_checkpoint = true;
        let root = tempfile::tempdir().unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let mut capture = frozen.write(root.path(), 0).unwrap();
        engine
            .prepare_scalar_checkpoint_publications(&mut capture)
            .unwrap();
        write("later", false);
        engine
            .bind_checkpoint_origins(root.path(), &mut capture)
            .unwrap();
        let state = engine.state.read().unwrap();
        let coll = &state.collections["c"];
        let id = coll.interner.id("e").unwrap();
        let FieldIndex::Keyword(keyword) = &coll.fields["keyword"] else {
            unreachable!()
        };
        assert_eq!(
            keyword.keyword_at(id).as_deref(),
            Some("later"),
            "publication must preserve the ordinary write made after preparation"
        );
        assert!(
            keyword
                .dense_forward
                .get(id as usize)
                .and_then(Option::as_ref)
                .is_some()
                || keyword.forward.contains_key(&id)
        );
        let FieldIndex::Number(number) = &coll.fields["number"] else {
            unreachable!()
        };
        assert_eq!(number.number_at(id).unwrap().to_f64(), 3.0);
        assert!(
            number.forward.is_empty(),
            "unchanged captured overlays must be retired"
        );
        assert!(number
            .dense_forward
            .get(id as usize)
            .is_none_or(|value| *value == MISSING_SORTABLE_F64_BITS));
        assert!(number.segment.is_some());
    }
}
// CODEGEN-END

#[cfg(test)]
mod batch_unindex_docs_tests {
    use super::*;
    use crate::shared_kernel::types::document::{BatchUnindexDocsRequest, IndexItem};

    #[test]
    fn batch_unindex_removes_a_known_document() {
        let engine = Engine::new();
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".to_string(),
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
        engine
            .create_collection("docs", CreateCollectionRequest { fields })
            .unwrap();
        engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("old@example.com".to_string()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();

        engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: vec!["old".to_string()],
                },
            )
            .unwrap();
        assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
    }

    #[test]
    fn batch_unindex_clears_lww_and_replace_side_state_before_rewrite() {
        let engine = Engine::new();
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".to_string(),
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
        engine
            .create_collection("docs", CreateCollectionRequest { fields })
            .unwrap();
        engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("old@example.com".to_string()),
                        version: Some(4),
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        engine
            .replace_docs(
                "docs",
                ReplaceDocsRequest {
                    docs: vec![ReplaceDocItem {
                        external_id: "old".to_string(),
                        version: Some(9),
                        fields: BTreeMap::from([(
                            "email".to_string(),
                            FieldValue::String("replace@example.com".to_string()),
                        )]),
                    }],
                },
            )
            .unwrap();

        engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: vec!["old".to_string()],
                },
            )
            .unwrap();
        let state = engine.state.read().unwrap();
        let coll = state.collections.get("docs").unwrap();
        let id = coll.interner.id("old").expect("append-only interner entry");
        assert!(!coll.eid_fields.contains_key(&id));
        assert!(!coll.cell_versions.contains_key(&id));
        assert!(!coll.doc_versions.contains_key(&id));
        assert!(!coll.field_checksums.contains_key(&id));
        drop(state);

        // No unindex tombstone is retained.  An older external version can
        // become the first version of the rewritten row.
        let rewritten = engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("rewritten@example.com".to_string()),
                        version: Some(1),
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        assert_eq!(rewritten.indexed, 1);
    }

    #[test]
    fn batch_unindex_revalidates_before_taking_any_mutation_path() {
        let engine = Engine::new();
        engine
            .create_collection(
                "docs",
                CreateCollectionRequest {
                    fields: BTreeMap::new(),
                },
            )
            .unwrap();
        let err = engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: Vec::new(),
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("at least one"), "got: {err}");
        assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
    }

    #[test]
    fn background_merge_capture_and_bind_preserve_live_dirty_ownership() {
        let engine = Engine::new();
        engine
            .create_collection(
                "docs",
                serde_json::from_value(serde_json::json!({"fields":{"email":{"type":"keyword"}}}))
                    .unwrap(),
            )
            .unwrap();
        engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "one".into(),
                        field: "email".into(),
                        value: FieldValue::String("old".into()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        let dirty = engine.checkpoint_dirty_fields().unwrap();
        let (generation, schema, fields) = dirty["docs"].clone();
        assert_eq!(fields, BTreeSet::from(["email".to_owned()]));
        let expected = BTreeMap::from([(
            "docs".to_owned(),
            CheckpointCollectionIdentity {
                generation,
                schema_version: schema,
                data_version: 0,
            },
        )]);
        let mut capture = engine.capture_background_merge(expected).unwrap();
        assert!(capture.field_dirty.is_empty());
        assert!(capture.frozen_changes.is_empty());
        assert!(capture.field_deltas.is_empty());
        {
            let mut state = engine.state.write().unwrap();
            state
                .collections
                .get_mut("docs")
                .unwrap()
                .requires_full_checkpoint = true;
        }
        let root = tempfile::tempdir().unwrap();
        engine
            .bind_background_merge(root.path(), &mut capture)
            .unwrap();
        assert!(engine.checkpoint_dirty_fields().unwrap()["docs"]
            .2
            .contains("email"));
        assert!(engine.state.read().unwrap().collections["docs"].requires_full_checkpoint);
    }
}

/// `/stats` cost with un-absorbed staged Text rows present (#4246). A
/// committed-WAL Text value stays in `TextIndex::staged_rows` until a
/// checkpoint publication absorbs it, so `unique_terms` must stay linear in
/// live terms plus staged tokens instead of scanning every staged row once
/// per term.
#[cfg(test)]
mod staged_text_stats_tests {
    use super::*;
    use crate::index::domain::text_index::{reset_staged_term_probes, staged_term_probes};
    use crate::index::infrastructure::staging::staged_text_row;
    use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;

    fn staged_row(input: &str) -> Arc<staged_text_row::StagedTextRow> {
        Arc::new(
            staged_text_row::StagedTextRow::stage(
                input,
                Analyzer::WhitespaceLower,
                TextRowStageOptions::minimum_scratch_bytes() + 4096,
                |_| Ok(()),
            )
            .expect("stage one Text row"),
        )
    }

    /// `tail` distinct live-tail tokens on doc 0, then `rows` staged rows each
    /// carrying one token shared with every other row plus one of its own.
    fn index_with_staged_rows(tail: u32, rows: u32) -> TextIndex {
        let mut idx = TextIndex {
            doc_count: 1,
            total_doc_len: u64::from(tail),
            ..Default::default()
        };
        idx.lens.push(tail);
        for n in 0..tail {
            let mut posting = Postings::default();
            posting.upsert(0, 1);
            idx.tokens.insert(format!("tail{n}"), posting);
        }
        for row in 0..rows {
            idx.staged_rows
                .insert(row + 1, staged_row(&format!("shared only{row}")));
            idx.doc_count += 1;
        }
        idx
    }

    #[test]
    fn live_unique_tokens_counts_every_staged_and_tail_token_exactly_once() {
        let idx = index_with_staged_rows(30, 50);
        // 30 tail tokens + the one token every staged row shares + 50
        // row-private tokens.
        assert_eq!(idx.live_unique_tokens(), 30 + 1 + 50);
    }

    #[test]
    fn live_unique_tokens_cost_stays_linear_in_the_staged_row_count() {
        let small = index_with_staged_rows(30, 25);
        reset_staged_term_probes();
        assert_eq!(small.live_unique_tokens(), 30 + 1 + 25);
        let small_probes = staged_term_probes();

        let large = index_with_staged_rows(30, 100);
        reset_staged_term_probes();
        assert_eq!(large.live_unique_tokens(), 30 + 1 + 100);
        let large_probes = staged_term_probes();

        // Reading each staged row's dictionary once costs its 2 tokens and
        // nothing per live tail term: 2 x rows probes. Asking `tok_postings`
        // per union term instead costs (tail + 1 + rows) x rows, which is
        // 13_100 here and is what made `/stats` superlinear in document count.
        assert!(
            large_probes <= 4 * (30 + 2 * 100),
            "counting staged tokens must cost O(live terms + staged tokens), \
             observed {large_probes} probes"
        );
        // Four times the rows may cost at most four times the probes.
        assert!(
            large_probes <= 4 * small_probes + 16,
            "staged-row cost must grow linearly, not quadratically: \
             {small_probes} probes at 25 rows, {large_probes} at 100"
        );
    }
}

#[cfg(test)]
mod checkpoint_publish_releases_retained_charge_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn text_field(analyzer: Analyzer) -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Text,
            analyzer: Some(analyzer),
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// Admit N committed rows that each retain a real
    /// [`crate::ingest::domain::change_budget::RetainedCharge`], run one real checkpoint
    /// freeze + write + publish through the exact Engine code path, and
    /// assert the process-wide budget's `active + frozen` (its `total`) after
    /// publication. If publication does not release every payload, this must
    /// fail before any fix and pass after.
    #[test]
    fn checkpoint_publish_releases_every_committed_row_charge() {
        let budget =
            crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(8 * 1024 * 1024);
        let engine = Engine::with_change_budget(budget.clone());
        let mut fields = BTreeMap::new();
        fields.insert("body".to_string(), text_field(Analyzer::WhitespaceLower));
        engine
            .create_collection_inner("c", CreateCollectionRequest { fields })
            .unwrap();

        let mut charges = Vec::new();
        for i in 0..50 {
            let charge = engine
                .changes
                .owner
                .try_reserve(1024)
                .unwrap()
                .commit_retained()
                .unwrap();
            engine
                .index_inner(
                    "c",
                    IndexRequest {
                        items: vec![IndexItem {
                            external_id: format!("doc{i}"),
                            field: "body".to_string(),
                            value: FieldValue::String("hello world from lumen".to_string()),
                            version: None,
                        }],
                        request_id: None,
                    },
                    Some(&charge),
                    None,
                )
                .unwrap();
            charges.push(charge);
        }
        // The caller's own handles drop here; the journal rows still hold
        // their own clones of the same retained charges.
        drop(charges);
        let before = budget.snapshot();
        assert!(
            before.total > 0,
            "committed rows must remain charged before any checkpoint runs"
        );

        let root = tempfile::tempdir().unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let mut capture = frozen.write(root.path(), 0).unwrap();
        engine
            .bind_checkpoint_origins(root.path(), &mut capture)
            .unwrap();
        engine.acknowledge_record_charges(&capture).unwrap();
        drop(capture);
        // Mirrors the real driver: `PendingFrozenLease::disarm` drops the
        // original `FrozenCheckpoint` only after publication and live
        // binding both succeed (`segment_rdb.rs`'s `pending.disarm()`).
        drop(frozen);

        let after = budget.snapshot();
        assert_eq!(
            after.total, 0,
            "publishing a checkpoint that captured every committed row must \
             release each row's retained charge: active={} frozen={} reserved={}",
            after.active, after.frozen, after.reserved
        );
    }
}
