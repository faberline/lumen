use axum::http::StatusCode;
use service_auth::Role;

use crate::access::application::authorization::{AuthContext, AuthErr};
use crate::access::domain::identity::{ADMIN_RESOURCE, COLLECTIONS_RESOURCE};
use crate::access::tests::{context, Cluster, Reachable};

// ---- authorization --------------------------------------------------

/// AC3: `get` on `lumencollections/orders` reads `orders` — and nothing
/// else. Each negative is a distinct way that grant could be over-read.
#[tokio::test]
async fn a_grant_on_one_collection_reaches_exactly_that_collection_and_verb() {
    let (_, ctx) = context(Cluster::with_ksa("clients", "reader").granting(
        COLLECTIONS_RESOURCE,
        Some("orders"),
        "get",
    ))
    .await;

    assert!(ctx.ensure("orders", Role::Read).await.is_ok());
    // ...not a write of the same collection,
    assert!(ctx.ensure("orders", Role::Write).await.is_err());
    // ...not another collection,
    assert!(ctx.ensure("invoices", Role::Read).await.is_err());
    // ...and not the admin surface.
    assert!(ctx.ensure_admin(Role::Admin).await.is_err());
}

/// AC4: the write and admin verbs are separately grantable, and holding
/// one is not holding another.
#[tokio::test]
async fn write_and_admin_verbs_are_granted_independently() {
    let (_, ctx) = context(Cluster::with_ksa("clients", "writer").granting(
        COLLECTIONS_RESOURCE,
        Some("orders"),
        "update",
    ))
    .await;
    assert!(ctx.ensure("orders", Role::Write).await.is_ok());
    assert!(ctx.ensure("orders", Role::Admin).await.is_err());
    assert!(ctx.ensure("orders", Role::Read).await.is_err());
}

/// AC4 / R6: `lumenadmin` is granted on its own, and holding it says
/// nothing about any collection.
#[tokio::test]
async fn the_admin_resource_is_granted_on_its_own() {
    let (_, ctx) =
        context(Cluster::with_ksa("ops", "backup").granting(ADMIN_RESOURCE, None, "delete")).await;
    assert!(ctx.ensure_admin(Role::Admin).await.is_ok());
    assert!(ctx.ensure("orders", Role::Read).await.is_err());
}

/// AC3: every check is scoped to the serving instance's namespace, so a
/// RoleBinding in the caller's own namespace does not carry over.
#[tokio::test]
async fn every_check_is_scoped_to_the_serving_namespace() {
    let (cluster, ctx) = context(Cluster::with_ksa("elsewhere", "reader")).await;
    let _ = ctx.ensure("orders", Role::Read).await;
    let asked = cluster.asked();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].namespace, "serving");
}

/// R4: the whole reviewed identity — not just the username — survives
/// into the authorization question, so RBAC can bind by group.
#[tokio::test]
async fn the_reviewed_identity_survives_into_the_authorization_question() {
    let (_, ctx) = context(Cluster::with_ksa("clients", "reader")).await;
    let AuthContext::Delegated { principal, .. } = &ctx else {
        panic!("a delegated verifier yields a delegated context");
    };
    assert_eq!(principal.identity.uid, "uid-1");
    assert!(principal
        .identity
        .groups
        .contains(&"system:serviceaccounts".to_string()));
}

/// R10: a denial is a 403 naming the resource, and carries no credential.
#[tokio::test]
async fn a_denial_is_a_403_naming_the_resource_and_no_credential() {
    let (_, ctx) = context(Cluster::with_ksa("clients", "reader")).await;
    let err = ctx.ensure("orders", Role::Read).await.unwrap_err();
    let (status, code, message) = err.wire();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(code, "forbidden");
    assert!(message.contains("orders"), "{message}");
    assert!(
        message.contains("system:serviceaccount:clients:reader"),
        "{message}"
    );
    assert!(!message.contains("Bearer"), "{message}");
}

/// AC7 / R10: an apiserver outage during authorization is a 503, not a
/// 403 and never an allow. Reporting it as a denial sends an operator to
/// fix a RoleBinding that was never wrong; reporting it as an allow is the
/// failure this whole design exists to prevent.
#[tokio::test]
async fn an_authorization_outage_is_unavailable_never_denied_and_never_allowed() {
    let (_, ctx) = context(
        Cluster::with_ksa("clients", "reader")
            .granting(COLLECTIONS_RESOURCE, Some("orders"), "get")
            .reachable(Reachable::AuthenticationOnly),
    )
    .await;
    // The caller genuinely holds this grant; the apiserver simply cannot
    // say so. The answer is still not "yes".
    let err = ctx.ensure("orders", Role::Read).await.unwrap_err();
    assert!(matches!(err, AuthErr::Unavailable { .. }), "{err:?}");
    let (status, code, _) = err.wire();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(code, "authorization_unavailable");
}

/// AC7: the outage classification reaches the wire with a stable reason
/// and no credential.
#[test]
fn an_unavailable_authorization_renders_with_a_stable_reason() {
    let err = AuthErr::Unavailable {
        subject: "system:serviceaccount:clients:reader".into(),
        needed: Role::Read,
        resource: "orders".into(),
        reason: "transport",
    };
    let (status, code, message) = err.wire();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(code, "authorization_unavailable");
    assert!(message.contains("transport"), "{message}");
}
