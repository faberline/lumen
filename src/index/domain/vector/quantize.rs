//! Scalar quantization: the codebook a field learns from the vectors it is
//! given, and the linear f32-to-u8 codec it encodes them with.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Scalar quantization
// ---------------------------------------------------------------------------

/// Codebook for the linear f32→u8 scalar quantizer. The codebook is
/// learned at insert time: every `add()` widens `(min, max)` if needed.
///
/// Re-quantizing already-stored vectors on codebook growth is a v2
/// nice-to-have; v1 simply accepts that earlier inserts will saturate
/// at the codebook's edges. In practice this is fine because callers
/// L2-normalize embeddings before insertion, which bounds the input
/// range tightly.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScalarCodebook {
    pub min: f32,
    pub max: f32,
    pub dim: usize,
}

impl ScalarCodebook {
    /// Empty codebook — the next `widen` call defines the range.
    pub fn empty(dim: usize) -> Self {
        Self {
            min: f32::INFINITY,
            max: f32::NEG_INFINITY,
            dim,
        }
    }

    /// Grow the codebook to cover `vec`. No-op when the vector already
    /// fits.
    pub fn widen(&mut self, vec: &[f32]) {
        for &v in vec {
            if v < self.min {
                self.min = v;
            }
            if v > self.max {
                self.max = v;
            }
        }
        // Degenerate fallback when only one value was ever seen — give
        // the codec a 1-unit window so the divisor isn't zero.
        if self.min == self.max {
            self.max = self.min + 1.0;
        }
    }

    fn range(&self) -> f32 {
        (self.max - self.min).max(f32::MIN_POSITIVE)
    }
}

/// Encode a vector to one byte per dimension using `cb`. Out-of-range
/// values saturate at `0` / `255`.
pub fn encode_sq(vec: &[f32], cb: &ScalarCodebook) -> Vec<u8> {
    let span = cb.range();
    vec.iter()
        .map(|&v| {
            let t = ((v - cb.min) / span).clamp(0.0, 1.0);
            (t * 255.0).round() as u8
        })
        .collect()
}

/// Decode a u8-encoded vector back to f32 using `cb`.
pub fn decode_sq(bytes: &[u8], cb: &ScalarCodebook) -> Vec<f32> {
    let span = cb.range();
    bytes
        .iter()
        .map(|&b| cb.min + (b as f32 / 255.0) * span)
        .collect()
}
