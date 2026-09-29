//! A text row's distinct tokens: inline for a small row, promoted to a hash set
//! once it outgrows eight, moving each token's allocation rather than cloning
//! it.

use std::collections::BTreeSet;

use smallvec::SmallVec;

use crate::index::domain::fast_hash::FastHashSet;

/// Small rows avoid a hash allocation. Large rows own each distinct token in
/// a hash set, so normalization does not scan all preceding terms per token.
#[derive(Debug, Clone)]
pub(crate) enum TokenSet {
    Inline(SmallVec<[String; 8]>),
    Indexed(FastHashSet<String>),
}

impl Default for TokenSet {
    fn default() -> Self {
        Self::Inline(SmallVec::new())
    }
}

impl TokenSet {
    pub(crate) fn insert_str(&mut self, token: &str) -> bool {
        match self {
            Self::Inline(tokens) => {
                if tokens.iter().any(|seen| seen == token) {
                    return false;
                }
                if tokens.len() < 8 {
                    tokens.push(token.to_owned());
                } else {
                    // Move each String into its bucket. Keep no second token
                    // list and do not clone payloads during this transition.
                    let mut indexed: FastHashSet<String> = tokens.drain(..).collect();
                    indexed.insert(token.to_owned());
                    *self = Self::Indexed(indexed);
                }
                true
            }
            Self::Indexed(tokens) => {
                if tokens.contains(token) {
                    false
                } else {
                    tokens.insert(token.to_owned());
                    true
                }
            }
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &String> {
        let (inline, indexed) = match self {
            Self::Inline(tokens) => (Some(tokens), None),
            Self::Indexed(tokens) => (None, Some(tokens)),
        };
        inline
            .into_iter()
            .flatten()
            .chain(indexed.into_iter().flatten())
    }

    pub(super) fn from_btree_set(set: BTreeSet<String>) -> Self {
        if set.len() <= 8 {
            Self::Inline(set.into_iter().collect())
        } else {
            Self::Indexed(set.into_iter().collect())
        }
    }
}

#[cfg(test)]
mod tests;
