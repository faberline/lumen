//! `crate::vector_index` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
pub use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
pub use crate::index::domain::vector::quantize::{decode_sq, encode_sq, ScalarCodebook};
pub use crate::index::domain::vector::{open_backend, VectorIndex};
