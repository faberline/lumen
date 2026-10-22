//! Text analysis: the tokenizer each schema analyzer selects, and the streamed
//! forms of the default n-gram and the feature-off Jieba fallback, which emit
//! tokens one at a time instead of collecting them.

#[cfg(not(feature = "jieba"))]
pub(in crate::index) mod jieba_fallback_stream;
pub mod ngram_stream;
pub mod tokenize;
