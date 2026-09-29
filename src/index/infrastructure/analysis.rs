//! Analysis that spills to disk: Jieba's file-backed dynamic-programming route,
//! and the bounded streaming lowercase of one long whitespace token.

#[cfg(feature = "jieba")]
pub(crate) mod jieba_disk_route;
pub(crate) mod unicode_lower_stream;
