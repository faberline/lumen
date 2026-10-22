use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use axum::http::HeaderMap;
use service_auth::k8s::{
    AccessReviewOutcome, ResourceAttributes, ReviewBackend, ReviewError, ReviewedIdentity,
    TokenReviewOutcome,
};
use service_auth::{AsyncVerifier, Role};

use crate::access::application::authorization::{verb, AuthContext, AuthTarget};
use crate::access::domain::identity::{AUDIENCE, COLLECTIONS_RESOURCE};
use crate::access::infrastructure::lumen_verifier::LumenVerifier;

/// Which half of the apiserver is unreachable, so an outage in
/// authentication and an outage in authorization can be proven separately.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reachable {
    Both,
    Neither,
    /// TokenReview answers; SubjectAccessReview does not.
    AuthenticationOnly,
}

/// A cluster under the test's control: one authentication answer plus an
/// explicit list of `(user, resource, name, verb)` grants, so every
/// assertion below names the exact RoleBinding it is modelling.
struct Cluster {
    username: String,
    audiences: Vec<String>,
    grants: Vec<(String, String, Option<String>, String)>,
    reachable: Reachable,
    token_calls: Mutex<Vec<Vec<String>>>,
    asked: Mutex<Vec<ResourceAttributes>>,
}

impl Cluster {
    fn with_ksa(namespace: &str, name: &str) -> Self {
        Self {
            username: format!("system:serviceaccount:{namespace}:{name}"),
            audiences: vec![AUDIENCE.to_string()],
            grants: Vec::new(),
            reachable: Reachable::Both,
            token_calls: Mutex::new(Vec::new()),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn as_user(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self
    }

    fn with_audiences(mut self, audiences: &[&str]) -> Self {
        self.audiences = audiences.iter().map(|a| a.to_string()).collect();
        self
    }

    fn granting(mut self, resource: &str, name: Option<&str>, verb: &str) -> Self {
        self.grants.push((
            self.username.clone(),
            resource.to_string(),
            name.map(|n| n.to_string()),
            verb.to_string(),
        ));
        self
    }

    fn reachable(mut self, reachable: Reachable) -> Self {
        self.reachable = reachable;
        self
    }

    fn asked(&self) -> Vec<ResourceAttributes> {
        self.asked.lock().unwrap().clone()
    }

    fn token_calls(&self) -> Vec<Vec<String>> {
        self.token_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ReviewBackend for Cluster {
    async fn review_token(
        &self,
        _token: &str,
        audiences: &[String],
    ) -> Result<TokenReviewOutcome, ReviewError> {
        self.token_calls.lock().unwrap().push(audiences.to_vec());
        if self.reachable == Reachable::Neither {
            return Err(ReviewError::Transport("apiserver unreachable".into()));
        }
        Ok(TokenReviewOutcome {
            authenticated: true,
            identity: ReviewedIdentity {
                username: self.username.clone(),
                uid: "uid-1".into(),
                groups: vec!["system:serviceaccounts".into()],
                ..Default::default()
            },
            audiences: self.audiences.clone(),
            error: None,
        })
    }

    async fn review_access(
        &self,
        identity: &ReviewedIdentity,
        attributes: &ResourceAttributes,
    ) -> Result<AccessReviewOutcome, ReviewError> {
        if self.reachable != Reachable::Both {
            return Err(ReviewError::Transport("apiserver unreachable".into()));
        }
        self.asked.lock().unwrap().push(attributes.clone());
        let held = self.grants.iter().any(|(user, resource, name, verb)| {
            user == &identity.username
                && resource == &attributes.resource
                && name == &attributes.name
                && verb == &attributes.verb
        });
        Ok(if held {
            AccessReviewOutcome::allow()
        } else {
            AccessReviewOutcome::deny("no RoleBinding grants this")
        })
    }
}

fn verifier(cluster: Cluster) -> (Arc<Cluster>, LumenVerifier) {
    let cluster = Arc::new(cluster);
    let verifier = LumenVerifier::delegated("serving", cluster.clone()).unwrap();
    (cluster, verifier)
}

fn in_cluster_verifier(cluster: Cluster) -> (Arc<Cluster>, LumenVerifier) {
    let cluster = Arc::new(cluster);
    let verifier = LumenVerifier::delegated_in_cluster("serving", cluster.clone()).unwrap();
    (cluster, verifier)
}

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    headers
}

async fn context(cluster: Cluster) -> (Arc<Cluster>, AuthContext) {
    let (cluster, verifier) = verifier(cluster);
    let ctx = verifier.authenticate_async(&bearer("t")).await.unwrap();
    (cluster, ctx)
}

async fn in_cluster_context(cluster: Cluster) -> (Arc<Cluster>, AuthContext) {
    let (cluster, verifier) = in_cluster_verifier(cluster);
    let ctx = verifier
        .authenticate_async(&bearer("default-ksa"))
        .await
        .unwrap();
    (cluster, ctx)
}

// ---- the mapping ----------------------------------------------------

/// R5: every field of the check a collection read produces, named.
#[test]
fn a_collection_read_asks_about_that_collection_by_name() {
    let attributes = AuthTarget::Collection("orders").attributes("serving", Role::Read);
    assert_eq!(attributes.group, "lumen.axiom.dev");
    assert_eq!(attributes.namespace, "serving");
    assert_eq!(attributes.resource, "lumencollections");
    assert_eq!(attributes.name.as_deref(), Some("orders"));
    assert_eq!(attributes.verb, "get");
}

/// R5: the three roles are three distinct RBAC verbs, so a grant of one
/// cannot be read as a grant of another.
#[test]
fn each_role_is_its_own_kubernetes_verb() {
    assert_eq!(verb(Role::Read), "get");
    assert_eq!(verb(Role::Write), "update");
    assert_eq!(verb(Role::Admin), "delete");
}

/// R6: the admin surface is a different resource with no resource name —
/// not `lumencollections` with a wildcard.
#[test]
fn the_admin_surface_is_a_separate_resource_not_a_wildcard_collection() {
    let attributes = AuthTarget::Admin.attributes("serving", Role::Admin);
    assert_eq!(attributes.resource, "lumenadmin");
    assert_eq!(attributes.name, None);
    assert_ne!(attributes.resource, COLLECTIONS_RESOURCE);
}

mod authentication;

mod authorization;
