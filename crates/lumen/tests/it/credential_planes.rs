//! #11: a Google-style bearer never authorizes a request on the serving API,
//! and the refusal logs no token. The KSA token on the same fixture is the
//! control, so a refusal cannot come from a broken fixture.

use std::io;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt as _;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::prelude::*;

use lumen::api::{router, AppState};
use lumen::auth::{AuthConfig, LumenVerifier};
use lumen::storage::Engine;
use service_auth::k8s::{
    AccessReviewOutcome, ResourceAttributes, ReviewBackend, ReviewError, ReviewedIdentity,
    TokenReviewOutcome,
};

const NAMESPACE: &str = "serving";
const KSA_TOKEN: &str = "ksa-plane-token-0123456789";
const KSA_USER: &str = "system:serviceaccount:serving:lumen-reader";
/// A JWT whose `iss` is Google's account issuer. Only the header and
/// payload matter here; the signature is never checked by the review.
const GOOGLE: &str =
    "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20ifQ.c2lnbmF0dXJl";

/// A review backend that knows exactly one token, grants nothing.
struct Reviewer;

#[async_trait]
impl ReviewBackend for Reviewer {
    async fn review_token(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<TokenReviewOutcome, ReviewError> {
        Ok(if token == KSA_TOKEN {
            TokenReviewOutcome {
                authenticated: true,
                identity: ReviewedIdentity {
                    username: KSA_USER.into(),
                    ..Default::default()
                },
                audiences: audiences.to_vec(),
                error: None,
            }
        } else {
            TokenReviewOutcome {
                authenticated: false,
                identity: ReviewedIdentity::default(),
                audiences: Vec::new(),
                error: Some("unknown token".into()),
            }
        })
    }

    async fn review_access(
        &self,
        _identity: &ReviewedIdentity,
        _attributes: &ResourceAttributes,
    ) -> Result<AccessReviewOutcome, ReviewError> {
        Ok(AccessReviewOutcome::deny("no grant"))
    }
}

#[derive(Clone, Default)]
struct SharedWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct SharedWriterGuard {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedWriterGuard {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for SharedWriter {
    type Writer = SharedWriterGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedWriterGuard {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

impl SharedWriter {
    fn output(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}

async fn get_collections(app: &axum::Router, bearer: &str) -> u16 {
    let request = Request::builder()
        .method("GET")
        .uri("/collections")
        .header("authorization", format!("Bearer {bearer}"))
        .body(Body::empty())
        .unwrap();
    app.clone()
        .oneshot(request)
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn google_style_bearer_is_refused_and_never_logged() {
    let writer = SharedWriter::default();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(writer.clone());
    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(tracing_subscriber::filter::EnvFilter::new("debug"));
    let _guard = tracing::subscriber::set_default(subscriber);

    let verifier = Arc::new(
        LumenVerifier::delegated(NAMESPACE, Arc::new(Reviewer)).expect("delegated verifier"),
    );
    let state = AppState::new(
        Arc::new(Engine::new()),
        Arc::new(AuthConfig::required_in(NAMESPACE)),
    )
    .with_verifier(verifier);
    let app = router(state);

    assert_eq!(
        get_collections(&app, GOOGLE).await,
        401,
        "a Google-style bearer must be refused on the serving API"
    );
    assert_eq!(
        get_collections(&app, KSA_TOKEN).await,
        200,
        "the KSA token on the same fixture must still succeed"
    );

    let output = writer.output();
    for secret in [KSA_TOKEN, GOOGLE, "-----BEGIN"] {
        assert!(
            !output.contains(secret),
            "the refusal path logged credential material"
        );
    }
}
