//! Access: who a caller is and what they may do.
//!
//! Callers are Kubernetes ServiceAccounts. `TokenReview` says who they are and
//! `SubjectAccessReview` says what they may do, so Lumen's access policy lives
//! in the cluster's RoleBindings. This context also loads the TLS material for
//! the serving and peer ports.

pub(crate) mod application;
pub(crate) mod domain;
pub(crate) mod infrastructure;
pub(crate) mod interfaces;

#[cfg(test)]
mod tests;
