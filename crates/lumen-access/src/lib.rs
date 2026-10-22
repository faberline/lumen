//! Access: who a caller is and what they may do.
//!
//! Callers are Kubernetes ServiceAccounts. `TokenReview` says who they are and
//! `SubjectAccessReview` says what they may do, so Lumen's access policy lives
//! in the cluster's RoleBindings. This context also loads the TLS material for
//! the serving and peer ports.

use lumen_shared_kernel as shared_kernel;

pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod interfaces;

#[cfg(test)]
mod tests;
