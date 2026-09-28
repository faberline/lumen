//! `crate::operator::render` before the DDD split. Re-exports its public items
//! from their new homes so callers outside the crate keep compiling.

pub use crate::operator::application::render::identity::{
    auth_delegator_binding, auth_delegator_binding_name, auth_delegator_labels,
};
pub use crate::operator::application::render::{
    prunes, render, render_with_profile, serving_dns_names, PEER_TLS_KEYS, SERVING_TLS_KEYS,
};
