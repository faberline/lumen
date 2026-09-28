//! The write path's use case: the write coordinator publishes each mutation to
//! the log, waits for this node's apply loop to fold it in, and hands back the
//! outcome.

pub(crate) mod write_coordinator;
