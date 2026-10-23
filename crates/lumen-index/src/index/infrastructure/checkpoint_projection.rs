//! Checkpoint rows written as segments without rehydrating a staged field: the
//! scalar projection merges reader dictionaries with the owned tail, and the
//! Text projection merges prepared rows and live postings one term at a time.

pub mod scalar_projection;
pub mod text_projection;
