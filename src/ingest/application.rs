//! The write path's use case: the write coordinator publishes each mutation to
//! the log, waits for this node's apply loop to fold it in, and hands back the
//! outcome; and the write port the HTTP handlers call, with its local
//! implementation over the write sink.

pub(crate) mod ports;
pub(crate) mod write_coordinator;
