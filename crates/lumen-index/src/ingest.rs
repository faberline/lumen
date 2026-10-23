//! Ingest: the write path. WAL records, the budget and cost of changes not yet
//! checkpointed, durable staging, and the write coordinator's apply loop.

pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod interfaces;
