//! The seam's diff tests: a field read served from a sealed segment (plus the
//! live tail and the delete tombstones) must match the in-RAM read, one module
//! per field kind, and the triple-path test adds a collection reopened cold
//! from its segments.

mod hash;
mod keyword;
mod keyword_inverted;
mod number_range;
mod predicate;
mod set;
mod set_inverted;
mod text;
mod triple_path;
mod vector;
