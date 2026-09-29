//! Rows staged to private files before apply: a Text row as a one-row segment
//! (with the bounded path for long whitespace tokens) and a Vector row as
//! aligned mmap data. Each receipt owns only its final file; the caller
//! reserves the input, the workspace and the reader separately.

pub(in crate::index) mod large_text_row;
pub(crate) mod staged_text_row;
pub(in crate::index) mod staged_vector_row;
