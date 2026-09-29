//! What persistence writes and reads on disk: the columnar segment file format,
//! its writers and its zero-copy reader, and the composed view that reads one
//! base segment through its ordered sparse deltas.

pub(crate) mod composed_segment;
pub(crate) mod segment;
