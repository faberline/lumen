//! lumen-index — the search engine and the contexts that are still one
//! dependency cycle: index, ingest, persistence, sharding, and the `app`
//! composition root.
//!
//! Replication's cluster state is `lumen-replication`. The HTTP layer reads it
//! through the kernel's `ports::cluster_status::ClusterStatus` port. The raft
//! state machine over the engine (`EngineSm`) is ingest's raft write path, at
//! `ingest::application::engine_sm`.
//!
//! These contexts call each other in both directions today, so they share one
//! crate. The `lumen` crate re-exports them under their old module paths.

use lumen_access as access;
use lumen_shared_kernel as shared_kernel;

pub mod app;
pub mod index;
pub mod ingest;
pub mod persistence;
pub mod sharding;
