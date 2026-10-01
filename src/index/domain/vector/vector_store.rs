//! The in-memory vector store both backends keep: raw f32 vectors, or
//! SQ-encoded bytes with the codebook they were encoded with.

use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::index::domain::vector::quantize::{decode_sq, encode_sq, ScalarCodebook};
#[cfg(test)]
use crate::index::domain::vector::VECTOR_STORE_OWNED_SCANS;
use crate::shared_kernel::types::schema::{VectorQuantize, VectorSpec};

// ---------------------------------------------------------------------------
// Quantization-aware storage helper
// ---------------------------------------------------------------------------

/// In-memory store of either raw f32 vectors or SQ-encoded bytes plus
/// a learned codebook. Both backends below share this.
#[derive(Debug)]
pub(in crate::index) struct VectorStore {
    pub(in crate::index) spec: VectorSpec,
    pub(super) raw: HashMap<String, Vec<f32>>,
    pub(super) encoded: HashMap<String, Vec<u8>>,
    pub(in crate::index) codebook: Option<ScalarCodebook>,
}

impl VectorStore {
    pub(in crate::index) fn new(spec: VectorSpec) -> Self {
        let codebook = match spec.quantize {
            Some(VectorQuantize::Sq) => Some(ScalarCodebook::empty(spec.dim as usize)),
            _ => None,
        };
        Self {
            spec,
            raw: HashMap::new(),
            encoded: HashMap::new(),
            codebook,
        }
    }

    pub(in crate::index) fn put(&mut self, eid: &str, vec: &[f32]) -> Result<()> {
        if vec.len() != self.spec.dim as usize {
            bail!(
                "vector dim mismatch: expected {}, got {}",
                self.spec.dim,
                vec.len()
            );
        }
        match self.spec.quantize {
            Some(VectorQuantize::Sq) => {
                let cb = self
                    .codebook
                    .as_mut()
                    .expect("codebook present when SQ enabled");
                cb.widen(vec);
                let bytes = encode_sq(vec, cb);
                self.encoded.insert(eid.to_string(), bytes);
            }
            _ => {
                self.raw.insert(eid.to_string(), vec.to_vec());
            }
        }
        Ok(())
    }

    pub(super) fn drop(&mut self, eid: &str) -> bool {
        self.raw.remove(eid).is_some() | self.encoded.remove(eid).is_some()
    }

    pub(in crate::index) fn len(&self) -> usize {
        if self.spec.quantize.is_some() {
            self.encoded.len()
        } else {
            self.raw.len()
        }
    }

    /// Materialize the f32 view of every stored vector. Decoded on the
    /// fly when SQ is on.
    pub(in crate::index) fn iter_decoded(
        &self,
    ) -> Box<dyn Iterator<Item = (String, Vec<f32>)> + '_> {
        #[cfg(test)]
        VECTOR_STORE_OWNED_SCANS.with(|scans| scans.set(scans.get() + 1));
        if let Some(cb) = self.codebook.as_ref() {
            Box::new(
                self.encoded
                    .iter()
                    .map(move |(k, b)| (k.clone(), decode_sq(b, cb))),
            )
        } else {
            Box::new(self.raw.iter().map(|(k, v)| (k.clone(), v.clone())))
        }
    }

    /// Decode a single eid's vector.
    #[allow(dead_code)]
    pub(in crate::index) fn get_decoded(&self, eid: &str) -> Option<Vec<f32>> {
        if let Some(cb) = self.codebook.as_ref() {
            self.encoded.get(eid).map(|b| decode_sq(b, cb))
        } else {
            self.raw.get(eid).cloned()
        }
    }
}
