//! The fixed-size table that counts a default-ngram field's distinct terms
//! without allocating.

use crate::ingest::domain::change_record_cost::text_upper_bound::{
    add, text_upper_bound, AnalyzerKind, NormalizeError, TextUpperBound,
};

/// One fixed table, reserved before exact pricing starts. The table never owns
/// input or heap tokens. All other analyzers keep their allocation-free bound.
pub(crate) const DEFAULT_NGRAM_COST_WORKSPACE_BYTES: usize =
    std::mem::size_of::<NgramDistinctTable>();

pub(super) const DEFAULT_NGRAM_DISTINCT_CAP: usize = 256;

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct NgramSlot {
    pub(super) occupied: bool,
    hash: u64,
    pub(super) len: u8,
    pub(super) bytes: [u8; 12],
}

pub(super) struct NgramDistinctTable {
    pub(super) slots: [NgramSlot; DEFAULT_NGRAM_DISTINCT_CAP],
    pub(super) full: bool,
}

impl NgramDistinctTable {
    pub(super) fn new() -> Self {
        Self {
            slots: [NgramSlot {
                occupied: false,
                hash: 0,
                len: 0,
                bytes: [0; 12],
            }; DEFAULT_NGRAM_DISTINCT_CAP],
            full: false,
        }
    }

    fn add(&mut self, token: &str) {
        let hash = token
            .as_bytes()
            .iter()
            .fold(14695981039346656037_u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(1099511628211)
            });
        self.add_with_hash(token, hash);
    }

    pub(super) fn add_with_hash(&mut self, token: &str, hash: u64) {
        if self.full {
            return;
        }
        let bytes = token.as_bytes();
        if bytes.len() > 12 {
            self.full = true;
            return;
        }
        let mut index = (hash as usize) % DEFAULT_NGRAM_DISTINCT_CAP;
        for _ in 0..DEFAULT_NGRAM_DISTINCT_CAP {
            let slot = &mut self.slots[index];
            if !slot.occupied {
                slot.occupied = true;
                slot.hash = hash;
                slot.len = bytes.len() as u8;
                slot.bytes[..bytes.len()].copy_from_slice(bytes);
                return;
            }
            if slot.hash == hash
                && usize::from(slot.len) == bytes.len()
                && slot.bytes[..bytes.len()] == *bytes
            {
                return;
            }
            index = (index + 1) % DEFAULT_NGRAM_DISTINCT_CAP;
        }
        self.full = true;
    }

    pub(super) fn bound(&mut self, input: &str) -> Result<TextUpperBound, NormalizeError> {
        // Each Text cell creates its own TokenSet during apply. Do not carry a
        // previous field, document, or discarded validation prefix into its cost.
        for slot in &mut self.slots {
            slot.occupied = false;
        }
        self.full = false;
        let conservative = text_upper_bound(
            input,
            AnalyzerKind::Ngram,
            crate::tokenize::DEFAULT_NGRAM_MIN,
            crate::tokenize::DEFAULT_NGRAM_MAX,
        )?;
        let streamed = crate::ngram_stream::stream_default_ngrams(input, |token| {
            self.add(token);
            if self.full {
                Err(())
            } else {
                Ok(())
            }
        });
        match streamed {
            Ok(_) => (),
            Err(crate::ngram_stream::NgramStreamError::Callback(())) => return Ok(conservative),
            Err(crate::ngram_stream::NgramStreamError::TokenCountOverflow) => {
                return Err(NormalizeError::Overflow);
            }
        }
        let mut bound = TextUpperBound::default();
        for slot in self.slots.iter().filter(|slot| slot.occupied) {
            bound.terms = add(bound.terms, 1)?;
            bound.total_utf8_bytes = add(bound.total_utf8_bytes, usize::from(slot.len))?;
        }
        Ok(bound)
    }
}
