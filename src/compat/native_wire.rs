//! `crate::native_wire` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::index::interfaces::native_wire::codec::{
    encode_range_frame, encode_search_frame, encode_term_frame, encode_term_range_frame,
};
pub use crate::index::interfaces::native_wire::server::serve_search;
pub use crate::index::interfaces::native_wire::{
    search_prepared, NativeSearchRequest, NativeSearchResponse,
};

#[cfg(unix)]
pub use crate::index::interfaces::native_wire::server::serve_unix_search;
