// CODEGEN-BEGIN
//! Authorization matrix (TEST-STRATEGY security gate) over delegated auth (#2869).
//!
//! Every check runs through the real router with a scripted `ReviewBackend`
//! standing in for kube-apiserver, so what is under test is the whole path a
//! request actually takes: middleware authenticates once, the handler asks
//! `SubjectAccessReview` about the operation it is about to perform.
//!
//! The matrix has three columns, and each one catches a different hole:
//!
//! - **401** — no credential, and a credential kube-apiserver verified but that
//!   is not a ServiceAccount (a Google user, a GSA). Those principals
//!   authenticate to the apiserver, never to Lumen.
//! - **403** — an authenticated ServiceAccount without the grant for *that*
//!   operation. This column is per-verb on purpose: `delete` does not imply
//!   `get`, because RBAC has no such hierarchy and neither does Lumen.
//! - **2xx** — the same endpoint with exactly its own grant, so the 403 column
//!   cannot pass by accident on a route that is simply broken.
//!
//! Plus the two cases that are neither: a collection listing is *filtered*, not
//! denied, and an apiserver that cannot answer yields 503 — never 200, never a
//! 403 that would look like a settled policy decision.
//!
//! ## Contracts inherited from the retired EC shells
//!
//! These 2 sentences were the whole of the `// Contract:` comment in 2 AW-EC shells
//! under `e2e/`, each of which ran `cargo test -p lumen --test
//! authz_matrix_e2e` in a subprocess and asserted the child's exit status. `cargo test
//! -p lumen` already runs this target directly, so the shells added a second, nested
//! run and nothing else. They were deleted on 2026-08-20 with the EC machinery they
//! belonged to, and the sentence is the only thing they held that nothing else did.
//! Each line below is prefixed with the EC id its shell was filed under.
//!
//! - `lumen-claim-security-rbac-matrix` — Every serving route is exercised twice - it
//!   answers on an open server and returns 401 under LUMEN_AUTH=required - so a route
//!   that silently disappeared cannot pass as a protected one; index size and result
//!   pages stay bounded. (#2871 removed the role dimension; it returns with
//!   SubjectAccessReview.)
//! - `lumen-security-hardening-access-control` — FILTERING: under LUMEN_AUTH=required a
//!   search over any collection is refused with 401 and returns no rows, so no result
//!   reaches an unidentified caller. (#2871 removed the 403 read-scope column; it
//!   returns with SubjectAccessReview.)

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum_test::TestServer;
use serde_json::{json, Value};

use lumen::api::{router, AppState, ANONYMOUS_ROUTES};
use lumen::auth::{
    AuthConfig, LumenVerifier, ADMIN_RESOURCE, COLLECTIONS_RESOURCE, RUNTIMES_RESOURCE,
    RUNTIME_VERB,
};
use lumen::storage::Engine;
use service_auth::k8s::{
    AccessReviewOutcome, ResourceAttributes, ReviewBackend, ReviewError, ReviewedIdentity,
    TokenReviewOutcome,
};

const NAMESPACE: &str = "serving";
const COLLECTION: &str = "users";
const RUNTIME: &str = "lumen-serving";
const OTHER_RUNTIME: &str = "lumen-other";

const OPERATOR: &str = "system:serviceaccount:serving:lumen-operator";
const WRONG_RUNTIME_CALLER: &str = "system:serviceaccount:serving:wrong-runtime-caller";
const LEGACY_CALLER: &str = "system:serviceaccount:serving:legacy-caller";
/// A ServiceAccount kube-apiserver knows and Lumen grants nothing to.
const STRANGER: &str = "system:serviceaccount:serving:lumen-stranger";
/// A human kube-apiserver authenticates. Never a Lumen caller.
const GOOGLE_USER: &str = "alice@example.com";

/// A scripted apiserver: a token table, an explicit grant list, and a switch
/// for the outage case.
struct Cluster {
    tokens: HashMap<&'static str, &'static str>,
    /// `(username, resource, name, verb)` — the exact tuple a
    /// `SubjectAccessReview` would be asked about.
    grants: HashSet<(String, String, Option<String>, String)>,
    access_reachable: bool,
    asked: Mutex<Vec<ResourceAttributes>>,
}

impl Cluster {
    fn new() -> Self {
        let tokens = HashMap::from([
            ("t-use", OPERATOR),
            ("t-wrong-runtime", WRONG_RUNTIME_CALLER),
            ("t-legacy", LEGACY_CALLER),
            ("t-stranger", STRANGER),
            ("t-google", GOOGLE_USER),
        ]);
        let mut cluster = Self {
            tokens,
            grants: HashSet::new(),
            access_reachable: true,
            asked: Mutex::new(Vec::new()),
        };
        // t-use holds lumenruntimes/use on RUNTIME.
        cluster.grant(OPERATOR, RUNTIMES_RESOURCE, Some(RUNTIME), RUNTIME_VERB);
        // t-wrong-runtime holds lumenruntimes/use on a different runtime name.
        cluster.grant(
            WRONG_RUNTIME_CALLER,
            RUNTIMES_RESOURCE,
            Some(OTHER_RUNTIME),
            RUNTIME_VERB,
        );
        // t-legacy holds only legacy per-collection and admin grants.
        cluster.grant(LEGACY_CALLER, COLLECTIONS_RESOURCE, Some(COLLECTION), "get");
        cluster.grant(
            LEGACY_CALLER,
            COLLECTIONS_RESOURCE,
            Some(COLLECTION),
            "update",
        );
        cluster.grant(
            LEGACY_CALLER,
            COLLECTIONS_RESOURCE,
            Some(COLLECTION),
            "delete",
        );
        cluster.grant(LEGACY_CALLER, ADMIN_RESOURCE, None, "delete");
        cluster
    }

    fn grant(&mut self, user: &str, resource: &str, name: Option<&str>, verb: &str) {
        self.grants.insert((
            user.to_string(),
            resource.to_string(),
            name.map(str::to_string),
            verb.to_string(),
        ));
    }

    fn unreachable_authorizer(mut self) -> Self {
        self.access_reachable = false;
        self
    }

    fn asked(&self) -> Vec<ResourceAttributes> {
        self.asked.lock().expect("asked").clone()
    }
}

#[async_trait]
impl ReviewBackend for Cluster {
    async fn review_token(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<TokenReviewOutcome, ReviewError> {
        Ok(match self.tokens.get(token) {
            Some(username) => TokenReviewOutcome {
                authenticated: true,
                identity: ReviewedIdentity {
                    username: (*username).to_string(),
                    ..Default::default()
                },
                audiences: audiences.to_vec(),
                error: None,
            },
            None => TokenReviewOutcome {
                authenticated: false,
                identity: ReviewedIdentity::default(),
                audiences: Vec::new(),
                error: Some("unknown token".into()),
            },
        })
    }

    async fn review_access(
        &self,
        identity: &ReviewedIdentity,
        attributes: &ResourceAttributes,
    ) -> Result<AccessReviewOutcome, ReviewError> {
        self.asked.lock().expect("asked").push(attributes.clone());
        if !self.access_reachable {
            return Err(ReviewError::Transport("apiserver unreachable".into()));
        }
        let key = (
            identity.username.clone(),
            attributes.resource.clone(),
            attributes.name.clone(),
            attributes.verb.clone(),
        );
        Ok(if self.grants.contains(&key) {
            AccessReviewOutcome::allow()
        } else {
            AccessReviewOutcome::deny("no RoleBinding grants this")
        })
    }
}

fn delegated_server(cluster: Arc<Cluster>) -> TestServer {
    let engine = Arc::new(Engine::new());
    let verifier =
        Arc::new(LumenVerifier::delegated(NAMESPACE, cluster).expect("delegated verifier"));
    let auth = Arc::new(AuthConfig::required_in(NAMESPACE).with_runtime(RUNTIME));
    let state = AppState::new(engine, auth).with_verifier(verifier);
    TestServer::new(router(state)).expect("server")
}

/// One row of the matrix: the endpoint, and the single identity whose grant
/// covers it.
struct Row {
    method: &'static str,
    path: &'static str,
    body: Option<Value>,
    /// The token that must succeed. Every other ServiceAccount must be 403.
    allowed: &'static str,
}

/// Ordered on purpose: the collection is created, written, read, then dropped,
/// so every row runs against a server where the row before it succeeded.
fn rows() -> Vec<Row> {
    let schema = json!({ "fields": { "email": { "type": "keyword" } } });
    let index = json!({ "items": [{ "external_id": "u1", "field": "email", "value": "a@x.com" }] });
    let search =
        json!({ "query": { "term": { "field": "email", "value": "a@x.com" } }, "limit": 5 });
    vec![
        Row {
            method: "PUT",
            path: "/collections/users",
            body: Some(schema),
            allowed: "t-use",
        },
        Row {
            method: "POST",
            path: "/collections/users/index",
            body: Some(index),
            allowed: "t-use",
        },
        Row {
            method: "POST",
            path: "/collections/users/search",
            body: Some(search),
            allowed: "t-use",
        },
        Row {
            method: "GET",
            path: "/collections/users/stats",
            body: None,
            allowed: "t-use",
        },
        Row {
            method: "GET",
            path: "/debug/cluster",
            body: None,
            allowed: "t-use",
        },
        Row {
            method: "GET",
            path: "/openapi.json",
            body: None,
            allowed: "t-use",
        },
        Row {
            method: "GET",
            path: "/docs",
            body: None,
            allowed: "t-use",
        },
        Row {
            method: "DELETE",
            path: "/collections/users",
            body: None,
            allowed: "t-use",
        },
    ]
}

async fn status(s: &TestServer, row: &Row, token: Option<&str>) -> u16 {
    let mut r = match row.method {
        "GET" => s.get(row.path),
        "PUT" => s.put(row.path),
        "POST" => s.post(row.path),
        "DELETE" => s.delete(row.path),
        other => panic!("unhandled method {other}"),
    };
    if let Some(b) = &row.body {
        r = r.json(b);
    }
    if let Some(t) = token {
        r = r.add_header("authorization", format!("Bearer {t}"));
    }
    r.await.status_code().as_u16()
}

fn delegated_metric(body: &str, name: &str) -> u64 {
    let prefix = format!("{name} ");
    let samples: Vec<_> = body
        .lines()
        .filter(|line| line.starts_with(&prefix))
        .collect();
    assert_eq!(
        samples.len(),
        1,
        "{name} must have exactly one bare sample in:\n{body}"
    );
    assert!(
        !body
            .lines()
            .any(|line| line.starts_with(&format!("{name}{{"))),
        "{name} must not be labeled in:\n{body}"
    );
    samples[0]
        .strip_prefix(&prefix)
        .expect("sample has the metric prefix")
        .parse()
        .unwrap_or_else(|error| panic!("{name} sample is not an integer: {error}; body:\n{body}"))
}

/// Delegated counters must use the same probe router as the public `/metrics`
/// endpoint. This checks the zero samples before requests and both authorization
/// outcomes after requests, so it fails if the router drops verifier metrics.
#[tokio::test]
async fn delegated_metrics_are_exposed_by_the_actual_router() {
    let s = delegated_server(Arc::new(Cluster::new()));
    let before = s.get("/metrics").await;
    before.assert_status_ok();
    let before = before.text();
    for name in [
        "delegated_auth_token_reviews_total",
        "delegated_auth_access_reviews_total",
        "delegated_auth_allowed_total",
        "delegated_auth_denied_total",
    ] {
        assert_eq!(
            delegated_metric(&before, name),
            0,
            "{name} must start at zero"
        );
    }

    assert_eq!(
        s.get("/collections/users/stats")
            .add_header("authorization", "Bearer t-use")
            .await
            .status_code()
            .as_u16(),
        404,
        "the granted caller reaches the handler even when the collection is absent"
    );
    assert_eq!(
        s.get("/collections/users/stats")
            .add_header("authorization", "Bearer t-stranger")
            .await
            .status_code()
            .as_u16(),
        403,
        "the ungranted ServiceAccount is denied by the real router"
    );

    let after = s.get("/metrics").await;
    after.assert_status_ok();
    let after = after.text();
    for (name, expected) in [
        ("delegated_auth_token_reviews_total", 2),
        ("delegated_auth_access_reviews_total", 2),
        ("delegated_auth_allowed_total", 1),
        ("delegated_auth_denied_total", 1),
    ] {
        assert_eq!(
            delegated_metric(&after, name),
            expected,
            "{name} must reflect the routed allowed and denied requests"
        );
    }
}

/// The whole matrix in one pass: for every endpoint, callers without the
/// exact whole-runtime grant (strangers, wrong runtime name, legacy grants)
/// are denied with 403, and only `t-use` holding `lumenruntimes/use` on this
/// runtime name gets through with 2xx.
#[tokio::test]
async fn each_endpoint_admits_exact_runtime_grant_and_denies_unauthorized() {
    let cluster = Arc::new(Cluster::new());
    let s = delegated_server(cluster.clone());

    for row in rows() {
        for token in ["t-stranger", "t-wrong-runtime", "t-legacy"] {
            assert_eq!(
                status(&s, &row, Some(token)).await,
                403,
                "{} {} must deny {token}",
                row.method,
                row.path
            );
        }
        let code = status(&s, &row, Some(row.allowed)).await;
        assert!(
            (200..300).contains(&code),
            "{} {} with lumenruntimes/use returned {code}",
            row.method,
            row.path
        );
    }
}

/// The 401 column. Both cases are "not an accepted identity", and keeping them
/// distinct from 403 matters: a 403 would tell a caller their credential was
/// accepted and merely under-privileged.
#[tokio::test]
async fn unauthenticated_and_non_serviceaccount_callers_are_401_not_403() {
    let cluster = Arc::new(Cluster::new());
    let s = delegated_server(cluster.clone());

    for row in rows() {
        assert_eq!(
            status(&s, &row, None).await,
            401,
            "{} {} with no credential",
            row.method,
            row.path
        );
        assert_eq!(
            status(&s, &row, Some("t-unknown")).await,
            401,
            "{} {} with a token the apiserver does not know",
            row.method,
            row.path
        );
        assert_eq!(
            status(&s, &row, Some("t-google")).await,
            401,
            "{} {} with a Google user the apiserver *did* authenticate",
            row.method,
            row.path
        );
    }

    assert!(
        cluster.asked().is_empty(),
        "a rejected identity must never reach SubjectAccessReview: {:?}",
        cluster.asked()
    );
}

/// Whole-runtime grant permits collection listing; ungranted callers are denied with 403.
#[tokio::test]
async fn the_collection_listing_is_authorized_per_runtime() {
    let cluster = Arc::new(Cluster::new());
    let s = delegated_server(cluster.clone());

    s.put("/collections/users")
        .add_header("authorization", "Bearer t-use")
        .json(&json!({ "fields": { "email": { "type": "keyword" } } }))
        .await
        .assert_status_ok();

    let listed: Value = s
        .get("/collections")
        .add_header("authorization", "Bearer t-use")
        .await
        .json();
    assert_eq!(
        listed,
        json!(["users"]),
        "a caller with lumenruntimes/use sees the collections"
    );

    for token in ["t-stranger", "t-wrong-runtime", "t-legacy"] {
        assert_eq!(
            s.get("/collections")
                .add_header("authorization", format!("Bearer {token}"))
                .await
                .status_code()
                .as_u16(),
            403,
            "GET /collections without whole-runtime grant must deny with 403"
        );
    }
}

/// An apiserver that cannot answer is 503. Never 200 — that would be a
/// fail-open — and never 403, which would report a policy decision nobody made.
#[tokio::test]
async fn an_authorization_outage_is_503() {
    let cluster = Arc::new(Cluster::new().unreachable_authorizer());
    let s = delegated_server(cluster);

    for row in rows() {
        assert_eq!(
            status(&s, &row, Some(row.allowed)).await,
            503,
            "{} {} during an apiserver outage",
            row.method,
            row.path
        );
    }
}

/// AC: A test lists the anonymous routes and fails if one is added or removed.
#[test]
fn anonymous_routes_exact_set() {
    let mut actual: Vec<&str> = ANONYMOUS_ROUTES.to_vec();
    actual.sort();
    let expected = vec!["/healthz", "/metrics", "/readyz", "/version"];
    assert_eq!(
        actual, expected,
        "the anonymous routes set must be exactly /healthz, /readyz, /metrics, /version"
    );
}

/// AC: Anonymous routes serve without a token, while protected routes require auth.
#[tokio::test]
async fn anonymous_routes_serve_without_token() {
    let cluster = Arc::new(Cluster::new());
    let s = delegated_server(cluster);

    for path in ANONYMOUS_ROUTES {
        let code = s.get(path).await.status_code().as_u16();
        assert!(
            (200..300).contains(&code),
            "anonymous route {path} returned {code} without token"
        );
    }

    // /debug/cluster is protected, so without a token it returns 401
    assert_eq!(
        s.get("/debug/cluster").await.status_code().as_u16(),
        401,
        "/debug/cluster must require authentication"
    );
}
// CODEGEN-END
