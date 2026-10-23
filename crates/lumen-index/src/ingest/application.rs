//! The write path's use case: the write coordinator publishes each mutation to
//! the log, waits for this node's apply loop to fold it in, and hands back the
//! outcome; and the write port the HTTP handlers call, with its local
//! implementation over the write sink. `request_arrival` carries the time the
//! data-plane router received the request, which admission measures its relief
//! deadline from.

pub mod ports;
pub mod request_arrival;
pub mod write_coordinator;
