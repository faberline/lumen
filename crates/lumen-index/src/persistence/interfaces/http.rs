//! The admin HTTP handlers that export, restore and checkpoint an engine's
//! state, and seal the HNSW cache before a planned restart. Each requires
//! `Role::Admin`.

pub mod backup;
pub mod checkpoint;
