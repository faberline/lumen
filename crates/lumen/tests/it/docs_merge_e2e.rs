//! #113 child 1: `POST /collections/{id}/docs:merge` is the canonical partial
//! field merge. It runs the same logic as the legacy `POST .../index`, which
//! stays wire-compatible. These tests pin that equivalence, the partial-merge
//! semantics, the Write grant, and the published OpenAPI and client surface.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum_test::TestServer;
use openapi_codegen::{generate, GenOptions, HttpClient, Lang};
use serde_json::{json, Value};

use lumen::api::{router, AppState};
use lumen::auth::{AuthConfig, LumenVerifier, COLLECTIONS_RESOURCE};
use lumen::storage::Engine;
use service_auth::k8s::{
    AccessReviewOutcome, ResourceAttributes, ReviewBackend, ReviewError, ReviewedIdentity,
    TokenReviewOutcome,
};

const NAMESPACE: &str = "serving";
const COLLECTION: &str = "documents";
const WRITER: &str = "system:serviceaccount:serving:lumen-writer";
const READER: &str = "system:serviceaccount:serving:lumen-reader";
const ADMIN: &str = "system:serviceaccount:serving:lumen-admin";
const STRANGER: &str = "system:serviceaccount:serving:lumen-stranger";

const MERGE: &str = "/collections/documents/docs:merge";
const INDEX: &str = "/collections/documents/index";

fn open_server() -> TestServer {
    let engine = Arc::new(Engine::new());
    TestServer::new(router(AppState::open(engine))).expect("open test server")
}

async fn create_documents(server: &TestServer) {
    server
        .put("/collections/documents")
        .json(&json!({ "fields": {
            "title": { "type": "text" },
            "state": { "type": "keyword" }
        }}))
        .await
        .assert_status_ok();
}

/// A small delegated-auth oracle. It grants only the exact Kubernetes-style
/// access tuple the requested route needs.
struct AuthzOracle {
    tokens: HashMap<&'static str, &'static str>,
    grants: HashSet<(String, String, Option<String>, String)>,
}

impl AuthzOracle {
    fn new() -> Self {
        let tokens = HashMap::from([
            ("writer", WRITER),
            ("reader", READER),
            ("admin", ADMIN),
            ("stranger", STRANGER),
        ]);
        let grants = HashSet::from([
            (
                WRITER.to_string(),
                COLLECTIONS_RESOURCE.to_string(),
                Some(COLLECTION.to_string()),
                "update".to_string(),
            ),
            (
                READER.to_string(),
                COLLECTIONS_RESOURCE.to_string(),
                Some(COLLECTION.to_string()),
                "get".to_string(),
            ),
            (
                ADMIN.to_string(),
                COLLECTIONS_RESOURCE.to_string(),
                Some(COLLECTION.to_string()),
                "delete".to_string(),
            ),
        ]);
        Self { tokens, grants }
    }
}

#[async_trait]
impl ReviewBackend for AuthzOracle {
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
                error: Some("unknown token".to_string()),
            },
        })
    }

    async fn review_access(
        &self,
        identity: &ReviewedIdentity,
        attributes: &ResourceAttributes,
    ) -> Result<AccessReviewOutcome, ReviewError> {
        let grant = (
            identity.username.clone(),
            attributes.resource.clone(),
            attributes.name.clone(),
            attributes.verb.clone(),
        );
        Ok(if self.grants.contains(&grant) {
            AccessReviewOutcome::allow()
        } else {
            AccessReviewOutcome::deny("no matching RoleBinding")
        })
    }
}

fn delegated_server() -> TestServer {
    let engine = Arc::new(Engine::new());
    let verifier = Arc::new(
        LumenVerifier::delegated(NAMESPACE, Arc::new(AuthzOracle::new()))
            .expect("delegated verifier"),
    );
    let state =
        AppState::new(engine, Arc::new(AuthConfig::required_in(NAMESPACE))).with_verifier(verifier);
    TestServer::new(router(state)).expect("delegated test server")
}

fn partial_batch() -> Value {
    json!({ "items": [
        { "external_id": "a", "field": "title", "value": "alpha one" },
        { "external_id": "a", "field": "state", "value": "open" },
        { "external_id": "b", "field": "title", "value": "beta two" }
    ]})
}

async fn search_ids(server: &TestServer, field: &str, value: &str) -> Value {
    server
        .post("/collections/documents/search")
        .json(&json!({
            "query": { "term": { "field": field, "value": value } },
            "limit": 10
        }))
        .await
        .json::<Value>()
}

async fn search_match(server: &TestServer, field: &str, text: &str) -> Value {
    server
        .post("/collections/documents/search")
        .json(&json!({
            "query": { "match": { "field": field, "text": text } },
            "limit": 10
        }))
        .await
        .json::<Value>()
}

#[tokio::test]
async fn docs_merge_matches_index_for_the_same_partial_request() {
    let legacy = open_server();
    create_documents(&legacy).await;
    let canonical = open_server();
    create_documents(&canonical).await;

    let via_index = legacy.post(INDEX).json(&partial_batch()).await;
    let via_merge = canonical.post(MERGE).json(&partial_batch()).await;
    via_index.assert_status_ok();
    via_merge.assert_status_ok();

    let index_body: Value = via_index.json();
    let merge_body: Value = via_merge.json();
    assert_eq!(index_body["indexed"], 3);
    assert_eq!(merge_body["indexed"], index_body["indexed"]);
    let keys = |v: &Value| -> Vec<String> {
        let mut k: Vec<String> = v["bytes_written"]
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        k.sort();
        k
    };
    assert_eq!(keys(&merge_body), keys(&index_body));

    for server in [&legacy, &canonical] {
        let found = search_ids(server, "state", "open").await;
        assert_eq!(found["total"], 1);
        assert_eq!(found["hits"][0]["external_id"], "a");
    }
}

#[tokio::test]
async fn docs_merge_keeps_omitted_fields_but_docs_replace_deletes_them() {
    let server = open_server();
    create_documents(&server).await;

    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "a", "field": "title", "value": "alpha one" },
            { "external_id": "a", "field": "state", "value": "open" }
        ]}))
        .await
        .assert_status_ok();
    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "a", "field": "title", "value": "alpha two" }
        ]}))
        .await
        .assert_status_ok();

    let kept = search_ids(&server, "state", "open").await;
    assert_eq!(kept["total"], 1, "merge must keep the omitted state field");

    server
        .put("/collections/documents/docs:replace")
        .json(&json!({ "docs": [
            { "external_id": "a", "fields": { "title": "alpha three" } }
        ]}))
        .await
        .assert_status_ok();

    let replaced = search_ids(&server, "state", "open").await;
    assert_eq!(
        replaced["total"], 0,
        "replace must delete the omitted state field"
    );
}

#[tokio::test]
async fn docs_merge_field_version_is_scoped_to_external_id_and_field() {
    let server = open_server();
    create_documents(&server).await;

    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "a", "field": "title", "value": "new", "version": 5 }
        ]}))
        .await
        .assert_status_ok();
    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "a", "field": "title", "value": "old", "version": 3 }
        ]}))
        .await
        .assert_status_ok();

    let old = search_match(&server, "title", "old").await;
    assert_eq!(old["total"], 0, "a lower field version must not overwrite");
    let new = search_match(&server, "title", "new").await;
    assert_eq!(new["total"], 1);

    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "a", "field": "state", "value": "open", "version": 1 }
        ]}))
        .await
        .assert_status_ok();
    let open = search_ids(&server, "state", "open").await;
    assert_eq!(
        open["total"], 1,
        "a version on one field must not block another field"
    );
}

#[tokio::test]
async fn docs_merge_errors_match_index() {
    let server = open_server();
    create_documents(&server).await;

    let oversized: Vec<Value> = (0..1001)
        .map(|i| json!({ "external_id": format!("id-{i}"), "field": "title", "value": "x" }))
        .collect();
    let type_mismatch = json!({ "items": [
        { "external_id": "a", "field": "state", "value": 3.5 }
    ]});

    for route in [INDEX, MERGE] {
        server
            .post(route)
            .json(&json!({ "items": oversized }))
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        server
            .post(&route.replace("documents", "missing"))
            .json(&partial_batch())
            .await
            .assert_status(StatusCode::NOT_FOUND);
        server
            .post(route)
            .json(&type_mismatch)
            .await
            .assert_status(StatusCode::UNPROCESSABLE_ENTITY);
    }
}

#[tokio::test]
async fn docs_merge_requires_write_and_denies_read_grants() {
    let server = delegated_server();
    server
        .put("/collections/documents")
        .add_header("authorization", "Bearer admin")
        .json(&json!({ "fields": { "state": { "type": "keyword" } } }))
        .await
        .assert_status_ok();

    for token in ["reader", "stranger"] {
        server
            .post(MERGE)
            .add_header("authorization", format!("Bearer {token}"))
            .json(&json!({ "items": [
                { "external_id": "row-1", "field": "state", "value": "live" }
            ]}))
            .await
            .assert_status(StatusCode::FORBIDDEN);
    }
    server
        .post(MERGE)
        .json(&json!({ "items": [
            { "external_id": "row-1", "field": "state", "value": "live" }
        ]}))
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    server
        .post(MERGE)
        .add_header("authorization", "Bearer writer")
        .json(&json!({ "items": [
            { "external_id": "row-1", "field": "state", "value": "live" }
        ]}))
        .await
        .assert_status_ok();
}

#[test]
fn openapi_publishes_docs_merge_and_keeps_the_index_alias() {
    let spec: Value = serde_json::from_str(&lumen::spec::openapi_json()).expect("OpenAPI parses");
    let merge = &spec["paths"]["/collections/{collection_id}/docs:merge"]["post"];
    assert!(
        merge.is_object(),
        "docs:merge POST missing from OpenAPI: {merge}"
    );
    assert_eq!(merge["operationId"], "merge_docs");
    assert_ne!(
        merge["deprecated"], true,
        "docs:merge must not be deprecated: {merge}"
    );

    let legacy = &spec["paths"]["/collections/{collection_id}/index"]["post"];
    assert!(
        legacy.is_object(),
        "legacy /index POST must remain published"
    );
    assert_eq!(
        legacy["deprecated"], true,
        "legacy /index must stay deprecated"
    );
}

fn generated_client_options(lang: Lang) -> GenOptions {
    GenOptions {
        lang,
        target: None,
        spec_path: Default::default(),
        out_dir: Default::default(),
        client_name: "createLumenClient".to_string(),
        http_client: HttpClient::Fetch,
        emit_types: true,
        emit_client: true,
        emit_hooks: lang == Lang::Ts,
    }
}

#[test]
fn generated_clients_expose_merge_docs_and_index() {
    for (lang, merge, legacy_index) in [
        (Lang::Py, "def merge_docs(", "def index("),
        (Lang::Ts, "mergeDocs(", "index("),
        (Lang::Rust, "pub fn merge_docs(", "pub fn index("),
    ] {
        let generated = generate(
            &lumen::spec::openapi_json(),
            &generated_client_options(lang),
        )
        .unwrap_or_else(|error| panic!("generate {lang:?} client: {error}"));
        let output = generated
            .files
            .into_iter()
            .map(|file| file.contents)
            .collect::<Vec<_>>()
            .join("\n");
        for operation in [merge, legacy_index] {
            assert!(
                output.contains(operation),
                "{lang:?} generated client must expose {operation:?}"
            );
        }
    }
}
