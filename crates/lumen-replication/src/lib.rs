//! lumen-replication — a shard's raft group, this pod's live role in it, and
//! the StatefulSet topology (`ClusterConfig`) it comes from.
//!
//! It depends only on the kernel and `raft-runtime`. `lumen-index` reads this
//! pod's cluster state through the kernel's `ports::cluster_status` port. The
//! raft state machine over the engine stays in `lumen-index` at
//! `ingest::application::engine_sm`, because it drives the engine's internals.

use lumen_shared_kernel as shared_kernel;

pub mod application;
pub mod domain;
