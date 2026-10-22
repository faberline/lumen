//! Distance helpers: squared L2, dot and cosine over f32 slices, and the
//! zero-safe unit normalization cosine uses.

use crate::shared_kernel::types::schema::VectorMetric;

// ---------------------------------------------------------------------------
// Distance helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub(super) fn distance(metric: VectorMetric, a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    match metric {
        VectorMetric::L2 => l2_squared(a, b).sqrt(),
        VectorMetric::Cosine => 1.0 - cosine_similarity(a, b),
        // For dot product we store *negative* dot as distance so that
        // smaller = closer = higher similarity, matching the HNSW
        // ordering contract.
        VectorMetric::Dot => -dot(a, b),
    }
}

#[allow(dead_code)]
pub(super) fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Scale `v` to *just under* unit norm (‖·‖ = 1 − 1e-6) for the DistDot cosine
/// path. Scaling both the stored and the query vector by a constant is exactly
/// rank-preserving for cosine (the cosine angle is scale-invariant), so recall
/// is unaffected. Staying a hair under unit keeps `dot(v,v) < 1`, which dodges
/// anndists `scalar_dot_f32`'s `assert!(1 - dot >= 0)` — that fires on
/// near-duplicate clustered vectors when float rounding pushes a unit self-dot
/// just above 1. A zero vector passes through unchanged (its dot is 0 →
/// distance 1, identical to what DistCosine yields for a zero-norm input).
pub(in crate::index) fn normalize_unit_safe(v: &[f32]) -> Vec<f32> {
    let norm = dot(v, v).sqrt();
    if norm == 0.0 {
        return v.to_vec();
    }
    let inv = (1.0 - 1e-6) / norm;
    v.iter().map(|x| x * inv).collect()
}

#[allow(dead_code)]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let d = dot(a, b);
    let na = dot(a, a).sqrt();
    let nb = dot(b, b).sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        d / (na * nb)
    }
}
