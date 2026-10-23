//! Wire types for the public HTTP API.
//!
//! These structs serialize to and from the JSON shapes documented in
//! `README.md`. They power the live router and the OpenAPI schema served at
//! `GET /openapi.json` — so they are the single source of truth consumers
//! integrate against.

pub mod api_error;
pub mod cluster_state_view;
pub mod document;
pub mod peer_addr;
pub mod query;
pub mod raft_role;
pub mod schema;
pub mod search;
pub mod stats;

#[cfg(test)]
mod tests;
