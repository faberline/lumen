//! Request auth for the serving API: Kubernetes ServiceAccount identities only.
//!
//! Lumen holds no credentials. A caller presents a short-lived, audience-bound
//! Kubernetes ServiceAccount token; `TokenReview` says who they are and
//! `SubjectAccessReview` says what they may do. Both questions are answered by
//! kube-apiserver, so the only place Lumen's access policy lives is
//! `RoleBinding`s in the cluster — there is no registry file, no token Secret,
//! and nothing for the operator to project (#2869).
//!
//! ## What this deliberately refuses
//!
//! A GKE authenticator will happily verify `alice@example.com` or
//! `svc@project.iam.gserviceaccount.com`, and `TokenReview` will report
//! `authenticated: true` for both. Lumen rejects them. Those principals
//! authenticate to *kube-apiserver*, and Kubernetes RBAC decides whether they
//! may request a token for a named client ServiceAccount; they never
//! authenticate to Lumen directly. Accepting them here would quietly make
//! Lumen a second identity provider for whatever the cluster's authenticator
//! happens to verify. The check is [`service_auth::k8s`]'s: the username must
//! strictly parse as `system:serviceaccount:<namespace>:<name>`.
//!
//! ## The resource mapping
//!
//! Lumen owns exactly this much of the shared mechanism — the translation from
//! a domain operation to the attributes a `SubjectAccessReview` asks about:
//!
//! | Operation | group | resource | name | verb |
//! |---|---|---|---|---|
//! | read a collection | `lumen.axiom.dev` | `lumencollections` | collection id | `get` |
//! | write a collection | `lumen.axiom.dev` | `lumencollections` | collection id | `update` |
//! | administer a collection | `lumen.axiom.dev` | `lumencollections` | collection id | `delete` |
//! | instance admin | `lumen.axiom.dev` | `lumenadmin` | — | per role |
//!
//! Instance-level endpoints (`/admin/*`) are a *separate resource*, not
//! wildcard access to every collection. A grant that lets an operator take a
//! backup should not thereby let them read every document in the fleet, and a
//! grant on one collection should never reach the admin surface.
//!
//! The namespace in every check is the serving instance's own — the one this
//! process runs in — so a caller from another namespace needs a RoleBinding
//! *here*, and holding one where it lives proves nothing.
//!
//! ## Configuration
//!
//! Env (read by [`AuthConfig::from_env`]):
//!
//! - `LUMEN_AUTH=off|disabled|required|in-cluster` — default `off`.
//!   `off`/`disabled` serve without authentication and make **no** Kubernetes
//!   call at all. `required` keeps Managed's private `lumen.axiom.dev`
//!   audience. `in-cluster` accepts the default KSA token Kubernetes mounts.
//! - `LUMEN_AUTH_NAMESPACE` — the namespace every `SubjectAccessReview` is
//!   scoped to. Defaults to `POD_NAMESPACE`, then to the in-cluster
//!   ServiceAccount namespace file. Both required profiles refuse to start
//!   without one: an unscoped check asks a different question than intended.
//!
//! ## Role precedence
//!
//! There is none any more, and that is the point. `admin ⊇ write ⊇ read` was a
//! property of a local role map. Here each role maps to a distinct Kubernetes
//! verb and the cluster's RBAC answers each independently: a RoleBinding
//! granting `delete` does not imply `get` unless it says so.

use std::sync::Arc;

use anyhow::Result;
use axum::http::StatusCode;
use service_auth::k8s::{
    DelegatedAuthError, DelegatedAuthenticator, ResourceAttributes, ServiceAccountPrincipal,
};

use crate::access::domain::identity::{ADMIN_RESOURCE, API_GROUP, COLLECTIONS_RESOURCE};

pub use service_auth::Role;

/// What a handler is asking about — one collection, or the instance itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthTarget<'a> {
    Collection(&'a str),
    Admin,
}

impl AuthTarget<'_> {
    /// The `SubjectAccessReview` attributes for this target at `needed`.
    pub fn attributes(&self, namespace: &str, needed: Role) -> ResourceAttributes {
        match self {
            AuthTarget::Collection(id) => ResourceAttributes::new(
                API_GROUP,
                namespace,
                COLLECTIONS_RESOURCE,
                Some((*id).to_string()),
                verb(needed),
            ),
            AuthTarget::Admin => {
                ResourceAttributes::new(API_GROUP, namespace, ADMIN_RESOURCE, None, verb(needed))
            }
        }
    }

    /// How this target is named in a denial message and an audit line.
    fn describe(&self) -> String {
        match self {
            AuthTarget::Collection(id) => (*id).to_string(),
            AuthTarget::Admin => ADMIN_RESOURCE.to_string(),
        }
    }
}

/// The Kubernetes verb a Lumen role maps to.
///
/// These are ordinary RBAC verbs, so a grant is expressible in a plain
/// `Role`/`ClusterRole` with no Lumen-specific vocabulary.
pub fn verb(role: Role) -> &'static str {
    match role {
        Role::Read => "get",
        Role::Write => "update",
        Role::Admin => "delete",
    }
}

/// Resolved auth state attached to every request as an axum extension.
///
/// Authentication happened once, in the middleware. Authorization happens per
/// operation, in the handler, because "may they touch *this* collection?" is a
/// different question per handler and the route alone does not answer it.
#[derive(Clone)]
pub enum AuthContext {
    /// Auth is off. Passes every check, and makes no Kubernetes call.
    Open,
    Delegated {
        principal: Arc<ServiceAccountPrincipal>,
        /// The `system:serviceaccount:<ns>:<name>` rendering, resolved once so
        /// the access log and every denial share one string.
        subject: Arc<str>,
        authenticator: Arc<DelegatedAuthenticator>,
        namespace: Arc<str>,
    },
}

impl std::fmt::Debug for AuthContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthContext::Open => f.write_str("AuthContext::Open"),
            AuthContext::Delegated {
                subject, namespace, ..
            } => f
                .debug_struct("AuthContext::Delegated")
                .field("subject", subject)
                .field("namespace", namespace)
                .finish(),
        }
    }
}

impl AuthContext {
    /// Authorize `needed` on one collection.
    pub async fn ensure(&self, collection_id: &str, needed: Role) -> Result<(), AuthErr> {
        self.authorize(AuthTarget::Collection(collection_id), needed)
            .await
    }

    /// Authorize `needed` on the instance-wide admin surface. A grant on every
    /// collection in the namespace still does not reach here.
    pub async fn ensure_admin(&self, needed: Role) -> Result<(), AuthErr> {
        self.authorize(AuthTarget::Admin, needed).await
    }

    async fn authorize(&self, target: AuthTarget<'_>, needed: Role) -> Result<(), AuthErr> {
        let AuthContext::Delegated {
            principal,
            subject,
            authenticator,
            namespace,
        } = self
        else {
            return Ok(());
        };
        let attributes = target.attributes(namespace, needed);
        authenticator
            .authorize(principal, &attributes)
            .await
            .map_err(|e| AuthErr::new(subject.to_string(), needed, target.describe(), e))
    }

    pub fn subject(&self) -> Option<&str> {
        match self {
            AuthContext::Open => None,
            AuthContext::Delegated { subject, .. } => Some(subject),
        }
    }
}

/// A failed authorization.
///
/// The two variants stay apart all the way to the status line because they are
/// different facts about the cluster. `Forbidden` means the apiserver answered
/// and the answer was no — retrying will not help, and the fix is a
/// RoleBinding. `Unavailable` means nobody answered — the request may well be
/// permitted, and calling it a denial sends an operator to fix a policy that
/// was never the problem.
#[derive(Debug)]
pub enum AuthErr {
    Forbidden {
        subject: String,
        needed: Role,
        resource: String,
    },
    Unavailable {
        subject: String,
        needed: Role,
        resource: String,
        /// A stable classification (`transport`, `malformed_response`,
        /// `not_delegated`) — never the credential that was presented.
        reason: &'static str,
    },
}

impl AuthErr {
    fn new(subject: String, needed: Role, resource: String, e: DelegatedAuthError) -> Self {
        match e {
            // `authorize` cannot report an authentication failure — the
            // middleware already resolved a principal — but the shared error
            // type carries the variant, and refusing the request is the only
            // safe rendering of a principal that stopped being one.
            DelegatedAuthError::Unauthenticated(_) | DelegatedAuthError::Denied(_) => {
                AuthErr::Forbidden {
                    subject,
                    needed,
                    resource,
                }
            }
            DelegatedAuthError::Unavailable(ref err) => AuthErr::Unavailable {
                subject,
                needed,
                resource,
                reason: err.reason(),
            },
        }
    }

    /// The wire code and message, shared by the HTTP response and the per-item
    /// batch envelope so the two can never drift.
    pub fn wire(&self) -> (StatusCode, &'static str, String) {
        match self {
            AuthErr::Forbidden {
                subject,
                needed,
                resource,
            } => (
                StatusCode::FORBIDDEN,
                "forbidden",
                format!("subject `{subject}` lacks {needed:?} on `{resource}`"),
            ),
            AuthErr::Unavailable {
                needed,
                resource,
                reason,
                ..
            } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "authorization_unavailable",
                format!(
                    "could not authorize {needed:?} on `{resource}`: kube-apiserver did not \
                     answer ({reason})"
                ),
            ),
        }
    }
}
