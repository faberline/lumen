//! `LumenVerifier`: authenticates each request's bearer token through
//! `TokenReview`, or refuses every credential when auth is off.

use std::sync::Arc;

use anyhow::Result;
use axum::http::HeaderMap;
use service_auth::k8s::{DelegatedAuthConfig, DelegatedAuthenticator, ReviewBackend};
use service_auth::{bearer_token, AsyncVerifier, AuthError as ServiceAuthError};

use crate::access::application::auth_config::AuthConfig;
use crate::access::application::authorization::AuthContext;
use crate::access::domain::identity::AUDIENCE;

#[cfg(feature = "delegated-auth")]
use service_auth::Role;

#[cfg(feature = "delegated-auth")]
use crate::access::application::authorization::AuthTarget;

/// How this process answers "who is calling?".
enum VerifierMode {
    /// Auth is off. Every request resolves to [`AuthContext::Open`] without
    /// touching the network (#2869 R9).
    Open,
    /// Auth is required but no review backend is wired — a state reachable
    /// only by building [`AuthConfig`] by hand. Every request is rejected;
    /// there is no degradation to `Open`.
    Unwired,
    /// Auth is required and delegated to kube-apiserver.
    Delegated {
        authenticator: Arc<DelegatedAuthenticator>,
        namespace: Arc<str>,
    },
}

/// Lumen's verifier for the shared async auth middleware.
pub struct LumenVerifier(VerifierMode);

impl std::fmt::Debug for LumenVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = match &self.0 {
            VerifierMode::Open => "open",
            VerifierMode::Unwired => "required-unwired",
            VerifierMode::Delegated { .. } => "delegated",
        };
        f.debug_tuple("LumenVerifier").field(&mode).finish()
    }
}

impl LumenVerifier {
    pub fn new(cfg: Arc<AuthConfig>) -> Self {
        Self(if cfg.required {
            VerifierMode::Unwired
        } else {
            VerifierMode::Open
        })
    }

    /// Delegate to an arbitrary review backend. The serving binary passes a
    /// live kube client; tests pass a scripted one, which is what lets the
    /// whole domain mapping be proven without a cluster.
    pub fn delegated(namespace: &str, backend: Arc<dyn ReviewBackend>) -> Result<Self> {
        let config = DelegatedAuthConfig::new(vec![AUDIENCE.to_string()])
            .map_err(|e| anyhow::anyhow!("lumen's delegated-auth audience is missing: {e}"))?;
        Ok(Self::delegated_with_config(namespace, backend, config))
    }

    /// Standalone's verifier for Kubernetes default ServiceAccount tokens.
    pub fn delegated_in_cluster(namespace: &str, backend: Arc<dyn ReviewBackend>) -> Result<Self> {
        Ok(Self::delegated_with_config(
            namespace,
            backend,
            DelegatedAuthConfig::kubernetes_default(),
        ))
    }

    fn delegated_with_config(
        namespace: &str,
        backend: Arc<dyn ReviewBackend>,
        config: DelegatedAuthConfig,
    ) -> Self {
        Self::with_authenticator(
            namespace,
            Arc::new(DelegatedAuthenticator::new(backend, config)),
        )
    }

    /// Delegate to an already-built authenticator — the seam a deterministic
    /// cache/TTL test uses to install its own clock.
    pub fn with_authenticator(namespace: &str, authenticator: Arc<DelegatedAuthenticator>) -> Self {
        Self(VerifierMode::Delegated {
            authenticator,
            namespace: Arc::from(namespace),
        })
    }

    /// Build the in-cluster verifier, proving both delegation grants before
    /// returning (#2869 R9).
    ///
    /// Every failure here is a startup failure. A process that cannot reach an
    /// apiserver, or whose ServiceAccount lacks `system:auth-delegator`, cannot
    /// authenticate anyone — and discovering that on the first request means
    /// serving 503s while looking healthy.
    #[cfg(feature = "delegated-auth")]
    pub async fn connect(cfg: &AuthConfig) -> Result<Self> {
        use service_auth::k8s::KubeReviewBackend;

        let backend = KubeReviewBackend::in_cluster().await.map_err(|e| {
            anyhow::anyhow!(
                "LUMEN_AUTH={} could not reach kube-apiserver: {e}",
                cfg.profile.env_value()
            )
        })?;
        let delegated = cfg.delegated_config()?;
        // A rejected probe is a successful probe: what is under test is
        // whether the apiserver will answer these two questions at all.
        backend
            .probe_delegation(
                delegated.audiences(),
                &AuthTarget::Admin.attributes(&cfg.namespace, Role::Read),
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "LUMEN_AUTH={} but this instance's ServiceAccount cannot create \
                     TokenReview/SubjectAccessReview for namespace `{}` ({e}). Bind it to \
                     `system:auth-delegator`. Refusing to start.",
                    cfg.profile.env_value(),
                    cfg.namespace
                )
            })?;
        Ok(Self::delegated_with_config(
            &cfg.namespace,
            Arc::new(backend),
            delegated,
        ))
    }

    /// The delegated counters in Prometheus text format — empty when auth is
    /// off, because nothing was measured.
    pub fn render_metrics(&self) -> String {
        match &self.0 {
            VerifierMode::Delegated { authenticator, .. } => authenticator.metrics().render(),
            _ => String::new(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncVerifier for LumenVerifier {
    type Principal = AuthContext;

    async fn authenticate_async(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthContext, ServiceAuthError> {
        match &self.0 {
            // A server that verifies nothing must never tell a caller its
            // credential was accepted. Absent header: anonymous, served.
            // Present header: rejected, because there is nothing here that
            // could have checked it (#2871).
            VerifierMode::Open => match bearer_token(headers) {
                Some(_) => Err(ServiceAuthError::Unauthenticated),
                None => Ok(AuthContext::Open),
            },
            VerifierMode::Unwired => Err(ServiceAuthError::Unauthenticated),
            VerifierMode::Delegated {
                authenticator,
                namespace,
            } => {
                let token = bearer_token(headers).ok_or(ServiceAuthError::Unauthenticated)?;
                let principal = authenticator.authenticate(token).await?;
                Ok(AuthContext::Delegated {
                    subject: Arc::from(principal.username().as_str()),
                    principal: Arc::new(principal),
                    authenticator: Arc::clone(authenticator),
                    namespace: Arc::clone(namespace),
                })
            }
        }
    }

    fn required(&self) -> bool {
        !matches!(self.0, VerifierMode::Open)
    }
}
