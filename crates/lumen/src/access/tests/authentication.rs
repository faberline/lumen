use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use service_auth::{AsyncVerifier, AuthError as ServiceAuthError, Role};

use crate::access::application::auth_config::AuthConfig;
use crate::access::application::authorization::AuthContext;
use crate::access::domain::identity::{AUDIENCE, COLLECTIONS_RESOURCE};
use crate::access::infrastructure::lumen_verifier::LumenVerifier;
use crate::access::tests::{
    bearer, context, in_cluster_context, in_cluster_verifier, verifier, Cluster, Reachable,
};

// ---- authentication -------------------------------------------------

/// AC1: a Lumen-audience KSA token authenticates.
#[tokio::test]
async fn a_lumen_audience_ksa_token_authenticates() {
    let (_, ctx) = context(Cluster::with_ksa("clients", "reader")).await;
    assert_eq!(ctx.subject(), Some("system:serviceaccount:clients:reader"));
}

/// AC1: the token every pod already has — bound only to the apiserver's
/// own audience — does not open Lumen.
#[tokio::test]
async fn the_default_pod_token_audience_is_not_accepted() {
    let (_, verifier) = verifier(
        Cluster::with_ksa("clients", "reader").with_audiences(&["https://kubernetes.default.svc"]),
    );
    assert!(matches!(
        verifier.authenticate_async(&bearer("t")).await.unwrap_err(),
        ServiceAuthError::Unauthenticated
    ));
}

/// AC2: `authenticated=true` is not enough. A Google user email is
/// rejected at the Lumen boundary, before any SubjectAccessReview.
#[tokio::test]
async fn an_authenticated_google_user_is_rejected_before_authorization() {
    let (cluster, verifier) =
        verifier(Cluster::with_ksa("clients", "reader").as_user("alice@example.com"));
    assert!(matches!(
        verifier.authenticate_async(&bearer("t")).await.unwrap_err(),
        ServiceAuthError::Unauthenticated
    ));
    assert!(
        cluster.asked().is_empty(),
        "a rejected principal must never reach SubjectAccessReview"
    );
}

/// AC2: the same for a Google service account — the identity a workload
/// using ADC would present.
#[tokio::test]
async fn an_authenticated_gsa_is_rejected_before_authorization() {
    let (cluster, verifier) = verifier(
        Cluster::with_ksa("clients", "reader").as_user("svc@project.iam.gserviceaccount.com"),
    );
    assert!(verifier.authenticate_async(&bearer("t")).await.is_err());
    assert!(cluster.asked().is_empty());
}

/// AC2: a value wearing the reserved prefix without the right shape is
/// malformed — not a ServiceAccount named `reader:extra`.
#[tokio::test]
async fn a_malformed_service_account_username_is_rejected() {
    let (_, verifier) = verifier(
        Cluster::with_ksa("clients", "reader")
            .as_user("system:serviceaccount:clients:reader:extra"),
    );
    assert!(verifier.authenticate_async(&bearer("t")).await.is_err());
}

/// R10 / AC7: no credential at all is a 401, and costs the apiserver
/// nothing.
#[tokio::test]
async fn a_request_without_a_credential_is_rejected_without_a_review() {
    let (cluster, verifier) = verifier(Cluster::with_ksa("clients", "reader"));
    assert!(matches!(
        verifier
            .authenticate_async(&HeaderMap::new())
            .await
            .unwrap_err(),
        ServiceAuthError::Unauthenticated
    ));
    assert!(cluster.asked().is_empty());
}

#[tokio::test]
async fn managed_and_standalone_request_distinct_tokenreview_audiences() {
    let (managed_cluster, managed) = verifier(Cluster::with_ksa("clients", "reader"));
    managed
        .authenticate_async(&bearer("managed"))
        .await
        .unwrap();
    assert_eq!(
        managed_cluster.token_calls(),
        vec![vec![AUDIENCE.to_string()]],
        "Managed must keep its private audience"
    );

    let (standalone_cluster, standalone) =
        in_cluster_verifier(Cluster::with_ksa("clients", "reader").with_audiences(&[]));
    standalone
        .authenticate_async(&bearer("default-ksa"))
        .await
        .unwrap();
    assert_eq!(
        standalone_cluster.token_calls(),
        vec![Vec::<String>::new()],
        "Standalone must intentionally use TokenReview's Kubernetes-default audiences"
    );
}

#[tokio::test]
async fn standalone_default_ksa_can_use_collections_but_not_lumenadmin() {
    let (cluster, ctx) = in_cluster_context(
        Cluster::with_ksa("apps", "api")
            .with_audiences(&[])
            .granting(COLLECTIONS_RESOURCE, Some("orders"), "get")
            .granting(COLLECTIONS_RESOURCE, Some("orders"), "update")
            .granting(COLLECTIONS_RESOURCE, Some("orders"), "delete"),
    )
    .await;
    for role in [Role::Read, Role::Write, Role::Admin] {
        ctx.ensure("orders", role).await.unwrap();
    }
    let error = ctx.ensure_admin(Role::Admin).await.unwrap_err();
    assert_eq!(error.wire().0, StatusCode::FORBIDDEN);
    assert_eq!(cluster.token_calls(), vec![Vec::<String>::new()]);
}

#[tokio::test]
async fn standalone_default_profile_preserves_401_and_503_failure_classes() {
    let (_, missing) = in_cluster_verifier(Cluster::with_ksa("apps", "api").with_audiences(&[]));
    assert!(matches!(
        missing
            .authenticate_async(&HeaderMap::new())
            .await
            .unwrap_err(),
        ServiceAuthError::Unauthenticated
    ));

    let (_, wrong_identity) = in_cluster_verifier(
        Cluster::with_ksa("apps", "api")
            .with_audiences(&[])
            .as_user("alice@example.com"),
    );
    assert!(matches!(
        wrong_identity
            .authenticate_async(&bearer("bad-identity"))
            .await
            .unwrap_err(),
        ServiceAuthError::Unauthenticated
    ));

    let (_, unavailable) = in_cluster_verifier(
        Cluster::with_ksa("apps", "api")
            .with_audiences(&[])
            .reachable(Reachable::Neither),
    );
    assert!(matches!(
        unavailable
            .authenticate_async(&bearer("default-ksa"))
            .await
            .unwrap_err(),
        ServiceAuthError::Unavailable(_)
    ));

    let (_, ctx) = in_cluster_context(
        Cluster::with_ksa("apps", "api")
            .with_audiences(&[])
            .reachable(Reachable::AuthenticationOnly),
    )
    .await;
    let error = ctx.ensure("orders", Role::Read).await.unwrap_err();
    assert_eq!(error.wire().0, StatusCode::SERVICE_UNAVAILABLE);
}

/// AC7: an outage during authentication is a rejection, not an open
/// principal.
#[tokio::test]
async fn an_authentication_outage_rejects_rather_than_admitting() {
    let (_, verifier) =
        verifier(Cluster::with_ksa("clients", "reader").reachable(Reachable::Neither));
    assert!(verifier.authenticate_async(&bearer("t")).await.is_err());
}

// ---- modes ----------------------------------------------------------

/// R9: auth off asks Kubernetes nothing — not to authenticate, and not to
/// authorize.
#[tokio::test]
async fn auth_off_never_asks_kubernetes_anything() {
    let verifier = LumenVerifier::new(Arc::new(AuthConfig::open()));
    let ctx = verifier
        .authenticate_async(&HeaderMap::new())
        .await
        .unwrap();
    assert!(matches!(ctx, AuthContext::Open));
    assert!(ctx.ensure("any", Role::Admin).await.is_ok());
    assert!(ctx.ensure_admin(Role::Admin).await.is_ok());
    assert_eq!(ctx.subject(), None);
    assert!(!AsyncVerifier::required(&verifier));
    assert!(verifier.render_metrics().is_empty());
}

/// Auth off still refuses a presented credential. Serving it as anonymous
/// would tell a stale client its token was accepted by a process that
/// owns no way to check one — the exact silent fallback #2871 pinned.
#[tokio::test]
async fn auth_off_rejects_a_presented_credential_rather_than_ignoring_it() {
    let verifier = LumenVerifier::new(Arc::new(AuthConfig::open()));
    assert!(verifier
        .authenticate_async(&bearer("looks-like-a-token"))
        .await
        .is_err());
}

/// A required config with no review backend rejects every request. It
/// never degrades to the open principal, with or without a credential.
#[tokio::test]
async fn a_required_but_unwired_verifier_rejects_every_request() {
    let verifier = LumenVerifier::new(Arc::new(AuthConfig::required_in("serving")));
    assert!(AsyncVerifier::required(&verifier));
    assert!(verifier
        .authenticate_async(&HeaderMap::new())
        .await
        .is_err());
    assert!(verifier
        .authenticate_async(&bearer("anything"))
        .await
        .is_err());
}

/// R8: the delegated counters are rendered, and name no credential.
#[tokio::test]
async fn the_delegated_counters_are_exported_without_credentials() {
    let (_, verifier) = verifier(Cluster::with_ksa("clients", "reader"));
    let ctx = verifier
        .authenticate_async(&bearer("super-secret"))
        .await
        .unwrap();
    let _ = ctx.ensure("orders", Role::Read).await;
    let rendered = verifier.render_metrics();
    assert!(
        rendered.contains("delegated_auth_token_reviews_total"),
        "{rendered}"
    );
    assert!(
        rendered.contains("delegated_auth_denied_total"),
        "{rendered}"
    );
    assert!(!rendered.contains("super-secret"), "{rendered}");
}
