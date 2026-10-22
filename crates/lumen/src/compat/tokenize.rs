//! `crate::tokenize` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::index::domain::analysis::tokenize::{
    tokenize, DEFAULT_NGRAM_MAX, DEFAULT_NGRAM_MIN,
};
