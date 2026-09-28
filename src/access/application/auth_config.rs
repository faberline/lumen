//! The auth profile an instance serves under, read from `LUMEN_AUTH`, and the
//! namespace every check is scoped to.

use anyhow::{bail, Result};

#[cfg(feature = "delegated-auth")]
use service_auth::k8s::DelegatedAuthConfig;

#[cfg(feature = "delegated-auth")]
use crate::access::domain::identity::AUDIENCE;

/// The in-cluster file every pod with a mounted ServiceAccount token carries.
/// Lumen needs such a token to call `TokenReview` at all, so this is readable
/// exactly when delegation can work.
const SERVICE_ACCOUNT_NAMESPACE_FILE: &str =
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// The credential profile selected by `LUMEN_AUTH`.
///
/// Managed and Standalone are separate on purpose. A default pod token must
/// open only a Standalone instance that explicitly chose `in-cluster`; it
/// must never become an alternate credential for Managed's private audience.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthProfile {
    Off,
    ManagedAudience,
    KubernetesDefault,
}

impl AuthProfile {
    pub fn env_value(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::ManagedAudience => "required",
            Self::KubernetesDefault => "in-cluster",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether a caller must present a verifiable identity. `false` makes no
    /// Kubernetes call on any request path.
    pub required: bool,
    /// The namespace every `SubjectAccessReview` is scoped to — the serving
    /// instance's own. Empty is only valid when `required` is `false`.
    pub namespace: String,
    pub(in crate::access) profile: AuthProfile,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::open()
    }
}

impl AuthConfig {
    pub fn open() -> Self {
        Self {
            required: false,
            namespace: String::new(),
            profile: AuthProfile::Off,
        }
    }

    /// A required config scoped to `namespace`. The verifier still has to be
    /// wired to a review backend before it can authenticate anyone; until it
    /// is, it rejects every request rather than falling back to an open one.
    pub fn required_in(namespace: impl Into<String>) -> Self {
        Self {
            required: true,
            namespace: namespace.into(),
            profile: AuthProfile::ManagedAudience,
        }
    }

    /// Standalone's explicit default-KSA profile.
    pub fn in_cluster(namespace: impl Into<String>) -> Self {
        Self {
            required: true,
            namespace: namespace.into(),
            profile: AuthProfile::KubernetesDefault,
        }
    }

    pub fn profile(&self) -> AuthProfile {
        self.profile
    }

    pub fn from_env() -> Result<Self> {
        let profile = match std::env::var("LUMEN_AUTH") {
            Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
                "required" => AuthProfile::ManagedAudience,
                "in-cluster" => AuthProfile::KubernetesDefault,
                "off" | "disabled" => AuthProfile::Off,
                other => {
                    bail!(
                        "LUMEN_AUTH must be `off`, `disabled`, `required`, or `in-cluster`; got `{other}`"
                    )
                }
            },
            Err(std::env::VarError::NotPresent) => AuthProfile::Off,
            Err(e) => bail!("LUMEN_AUTH must be valid UTF-8: {e}"),
        };
        let required = profile != AuthProfile::Off;

        let namespace = namespace_from_env();

        if required && namespace.is_empty() {
            // Fail closed. A namespace-less SubjectAccessReview asks about a
            // different resource than the one the request touched, and the
            // safest wrong answer is still a wrong answer.
            bail!(
                "LUMEN_AUTH={} needs the serving namespace to scope every \
                 SubjectAccessReview to, and neither LUMEN_AUTH_NAMESPACE nor POD_NAMESPACE is \
                 set and {SERVICE_ACCOUNT_NAMESPACE_FILE} is unreadable. Refusing to start \
                 rather than authorize against an unscoped resource.",
                profile.env_value()
            );
        }

        Ok(Self {
            required,
            namespace,
            profile,
        })
    }

    #[cfg(feature = "delegated-auth")]
    pub(in crate::access) fn delegated_config(&self) -> Result<DelegatedAuthConfig> {
        match self.profile {
            AuthProfile::ManagedAudience => DelegatedAuthConfig::new(vec![AUDIENCE.to_string()])
                .map_err(|e| anyhow::anyhow!("lumen's delegated-auth audience is missing: {e}")),
            AuthProfile::KubernetesDefault => Ok(DelegatedAuthConfig::kubernetes_default()),
            AuthProfile::Off => bail!("auth=off has no delegated token profile"),
        }
    }
}

/// The serving namespace, from the explicit override, the downward-API env, or
/// the mounted ServiceAccount. Empty when none of the three answers.
fn namespace_from_env() -> String {
    for key in ["LUMEN_AUTH_NAMESPACE", "POD_NAMESPACE"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    std::fs::read_to_string(SERVICE_ACCOUNT_NAMESPACE_FILE)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
