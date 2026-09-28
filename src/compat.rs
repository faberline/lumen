//! Module paths from before the DDD split.
//!
//! Each child re-exports the public items of a module the split broke up, so
//! callers outside the crate keep compiling. They go once those callers use the
//! new paths.

pub mod auth;
