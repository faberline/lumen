//! `crate::auth` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::access::application::auth_config::{AuthConfig, AuthProfile};
pub use crate::access::application::authorization::{verb, AuthContext, AuthErr, AuthTarget, Role};
pub use crate::access::application::control_plane_token::{
    control_plane_token_file, CONTROL_PLANE_TOKEN_FILE, CONTROL_PLANE_TOKEN_MOUNT,
    CONTROL_PLANE_TOKEN_VOLUME,
};
pub use crate::access::domain::identity::{
    ADMIN_RESOURCE, API_GROUP, AUDIENCE, COLLECTIONS_RESOURCE,
};
pub use crate::access::infrastructure::lumen_verifier::LumenVerifier;
pub use crate::access::interfaces::http::auth_middleware;
