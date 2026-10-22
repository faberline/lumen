//! The HTTP surface the composition root assembles: the router and the state
//! its handlers share, the guards they run, the error they answer with, the
//! admin probes and the OpenAPI document. The handlers are in each context's
//! interfaces/http.

pub mod api_err;
pub mod app_state;
pub mod guards;
pub mod openapi;
mod probes;
pub mod router;
pub mod write_fence;
