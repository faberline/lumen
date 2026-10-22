//! Analysis that spills to disk: Jieba's file-backed dynamic-programming route,
//! and the bounded streaming lowercase of one long whitespace token.

#[cfg(feature = "jieba")]
pub(in crate::index) mod jieba_disk_route;
pub(super) mod unicode_lower_stream;
