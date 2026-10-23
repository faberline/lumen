//! lumen-storage — the data lumen keeps on disk: the columnar segment file
//! format, its writers and its zero-copy reader, and the composed reader that
//! reads one base segment through its ordered sparse deltas.
//!
//! It depends on no other lumen crate. `lumen-index` seals and reopens each
//! field's segments through it, and its persistence context publishes them as
//! checkpoint generations.

pub mod infrastructure;
