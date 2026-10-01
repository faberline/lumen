//! `crate::dx` before the DDD split. Re-exports its public items from their new
//! homes so callers outside the crate keep compiling.

pub use crate::app::spec::dx::{field_catalog, llm_protocol, render_llm};
