//! Wire types for the public HTTP API.
//!
//! These structs serialize to and from the JSON shapes documented in
//! `README.md`. They power the live router and the OpenAPI schema served at
//! `GET /openapi.json` — so they are the single source of truth consumers
//! integrate against.

pub(crate) mod api_error;
pub(crate) mod document;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod search;
pub(crate) mod stats;

#[cfg(test)]
mod tests;
