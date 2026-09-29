//! The similarity leaves: kNN over a vector field, on its own or within an
//! allowed doc set, Hamming distance over a hash field, and reciprocal rank
//! fusion of sub-queries' rankings.

use std::collections::{BTreeSet, HashMap};

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::query::eval::eval_query;
use crate::index::domain::query::ScoredHits;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::{HammingQuery, KnnQuery, RrfQuery};
use crate::storage::EngineState;

pub(super) fn eval_knn(coll: &Collection, q: &KnnQuery) -> Result<ScoredHits> {
    eval_knn_inner(coll, q, None)
}

/// Reciprocal Rank Fusion: run each sub-query, rank its hits by score
/// descending (ties broken by docid for determinism), and fuse by rank —
/// `score(d) = Σ_i 1/(k + rank_i(d))`, 1-based rank, over the sub-queries that
/// returned `d`. Rank-based, so BM25 and cosine scales need no normalisation.
/// Filters belong inside each leg (`knn AND <filter>`), where the kNN stays
/// filter-correct via the And-node allow-list.
pub(super) fn eval_rrf(
    coll: &Collection,
    collection_id: &str,
    r: &RrfQuery,
    universe: &BTreeSet<u32>,
    state: &EngineState,
) -> Result<ScoredHits> {
    let k = r.k.max(1) as f32;
    let mut fused: ScoredHits = HashMap::new();
    for sub in &r.queries {
        let hits = eval_query(coll, collection_id, sub, universe, state)?;
        // Rank by score desc; break ties by docid asc so fusion is deterministic.
        let mut ranked: Vec<(u32, f32)> = hits.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        for (rank0, (id, _)) in ranked.into_iter().enumerate() {
            let contrib = 1.0 / (k + (rank0 as f32 + 1.0));
            *fused.entry(id).or_insert(0.0) += contrib;
        }
    }
    Ok(fused)
}

/// kNN constrained to an `allow` set of dense doc-ids — returns the nearest
/// `k` *within the filtered set*. Drives `VectorIndex::search_knn_filtered`,
/// so a selective filter never collapses recall the way a post-filter over the
/// global top-k does (the pgvector failure mode).
pub(super) fn eval_knn_filtered(
    coll: &Collection,
    q: &KnnQuery,
    allow: &RoaringBitmap,
) -> Result<ScoredHits> {
    eval_knn_inner(coll, q, Some(allow))
}

fn eval_knn_inner(
    coll: &Collection,
    q: &KnnQuery,
    allow: Option<&RoaringBitmap>,
) -> Result<ScoredHits> {
    let fi = coll
        .fields
        .get(&q.field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: q.field.clone(),
        })?;
    let FieldIndex::Vector { spec, idx, .. } = fi else {
        bail!(
            "knn query is only valid on vector fields (field `{}`)",
            q.field
        );
    };
    if q.vector.len() as u32 != spec.dim {
        bail!(
            "knn query on field `{}` declared dim={} but got vector of length {}",
            q.field,
            spec.dim,
            q.vector.len()
        );
    }
    if q.k == 0 {
        return Ok(HashMap::new());
    }
    // The vector index keeps its own String-keyed id space; map results to the
    // collection's dense doc-ids for the unified scored-hits space. When an
    // allow-set is given, the bits gate the graph search itself (translated
    // back to external_ids via the interner) rather than filtering afterwards.
    let pairs = match allow {
        None => idx.search_knn(&q.vector, q.k as usize)?,
        Some(bm) => {
            let allow_fn = |eid: &str| coll.interner.id(eid).map_or(false, |id| bm.contains(id));
            idx.search_knn_filtered(&q.vector, q.k as usize, &allow_fn)?
        }
    };
    Ok(pairs
        .into_iter()
        .filter_map(|(eid, score)| coll.interner.id(&eid).map(|id| (id, score)))
        .collect())
}

/// Hamming near-duplicate search: every doc whose 64-bit hash is within
/// `max_distance` bits of the query hash, scored by similarity (closer →
/// higher). Brute-force scan over the field's forward map.
pub(crate) fn eval_hamming(coll: &Collection, q: &HammingQuery) -> Result<ScoredHits> {
    let fi = coll
        .fields
        .get(&q.field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: q.field.clone(),
        })?;
    let FieldIndex::Hash(h) = fi else {
        bail!(
            "hamming query is only valid on hash fields (field `{}`)",
            q.field
        );
    };
    let query = parse_hash(&q.hash)?;
    let max = q.max_distance.min(64);
    let mut hits = ScoredHits::new();
    // The per-doc hash read routes through `hash_at`, so a sealed segment
    // serves ids `[0..n_docs)`. We must scan the union of the segment's covered
    // id range and the live `forward` tail; with no segment the union is
    // exactly `forward`'s keys, so this is byte-for-byte the live scan.
    let mut scan = |id: u32, doc: u64| {
        let dist = (doc ^ query).count_ones();
        if dist <= max {
            hits.insert(id, (64 - dist) as f32 / 64.0);
        }
    };
    if let Some(seg) = &h.segment {
        // Sealed range, served zero-copy from the segment (absent ids skipped).
        for id in 0..seg.n_docs() {
            if let Some(doc) = h.hash_at(id) {
                scan(id, doc);
            }
        }
        // Live tail: ids the segment does not cover.
        for (&id, &doc) in &h.forward {
            if id >= seg.n_docs() {
                scan(id, doc);
            }
        }
        return Ok(hits);
    }
    for (&id, &doc) in &h.forward {
        scan(id, h.hash_at(id).unwrap_or(doc));
    }
    Ok(hits)
}
