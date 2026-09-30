//! The HTTP surface the composition root assembles: the router and the state
//! its handlers share, the guards they run, the error they answer with, the
//! admin probes and the OpenAPI document. The handlers are in each context's
//! interfaces/http.

pub(crate) mod api_err;
pub(crate) mod app_state;
pub(crate) mod guards;
pub(crate) mod openapi;
pub(crate) mod probes;
pub(crate) mod router;
pub(crate) mod write_fence;
