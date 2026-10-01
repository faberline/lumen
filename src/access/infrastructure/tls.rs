//! mTLS configuration for the peer (`:8082`) transport.
//!
//! v1 ships the configuration surface — paths to cert / key / CA bundle
//! and an `is_required` flag — so deployments can declare their TLS
//! posture today. The rustls binding is wired in alongside the raft_core-backed
//! peer transport.
//!
//! ## Env contract
//!
//! - `LUMEN_PEER_TLS_CERT` — path to this pod's PEM cert chain.
//! - `LUMEN_PEER_TLS_KEY`  — path to its private key.
//! - `LUMEN_PEER_TLS_CA`   — path to the CA bundle peers are verified against.
//! - `LUMEN_PEER_MTLS=on|off` — when `on`, non-mTLS peer connections are rejected.
//!
//! The presence of all three paths + `LUMEN_PEER_MTLS=on` enables mTLS;
//! any other combination falls back to plain HTTP/2 (with a warning).
//!
//! The PEM loading, rustls server/client config builders, and the
//! Once-guarded crypto-provider install are generic across every service
//! with a peer/replication port and live in `libs/peer-tls` (#971); this
//! module is a thin adapter over it that keeps lumen's `LUMEN_PEER_TLS_*`/
//! `LUMEN_PEER_MTLS` env names and pub API unchanged.

use std::path::PathBuf;

use anyhow::Result;

/// The prefix passed to `peer_tls::PeerTlsConfig::from_env`: derives
/// `LUMEN_PEER_TLS_CERT` / `LUMEN_PEER_TLS_KEY` / `LUMEN_PEER_TLS_CA` /
/// `LUMEN_PEER_MTLS`, preserving lumen's env contract byte-for-byte.
const ENV_PREFIX: &str = "LUMEN_PEER";

#[derive(Debug, Clone)]
pub struct PeerTlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub required: bool,
}

impl From<peer_tls::PeerTlsConfig> for PeerTlsConfig {
    fn from(cfg: peer_tls::PeerTlsConfig) -> Self {
        Self {
            cert: cfg.cert,
            key: cfg.key,
            ca: cfg.ca,
            required: cfg.required,
        }
    }
}

impl From<PeerTlsConfig> for peer_tls::PeerTlsConfig {
    fn from(cfg: PeerTlsConfig) -> Self {
        Self {
            cert: cfg.cert,
            key: cfg.key,
            ca: cfg.ca,
            required: cfg.required,
        }
    }
}

impl PeerTlsConfig {
    /// Load from env. Returns `Ok(None)` when no TLS material is
    /// configured (plain-HTTP peer transport).
    pub fn from_env() -> Result<Option<Self>> {
        Ok(peer_tls::PeerTlsConfig::from_env(ENV_PREFIX)?.map(Self::from))
    }

    /// Build a rustls server config for the peer transport.
    pub fn rustls_server_config(&self) -> Result<rustls::ServerConfig> {
        peer_tls::PeerTlsConfig::from(self.clone()).rustls_server_config()
    }

    /// Build a rustls client config for dialing peer transports.
    pub fn rustls_client_config(&self) -> Result<rustls::ClientConfig> {
        peer_tls::PeerTlsConfig::from(self.clone()).rustls_client_config()
    }

    /// Construct the shared reloadable Raft peer transport. Lumen owns only
    /// env naming; TLS connection, identity, and reload semantics stay in
    /// `raft-runtime`.
    pub fn peer_transport(&self) -> Result<raft_runtime::PeerTransport> {
        let config = peer_tls::PeerTlsConfig::from(self.clone());
        raft_runtime::PeerTransport::from_config(&config)
    }

    /// Bind this member's projected peer material to the shared reloadable
    /// seam (#3112 R2).
    ///
    /// Lumen contributes the only two things the library cannot know — where
    /// the Secret is projected, and which identity *this* member must present —
    /// and nothing else. Validation, last-known-good retention, trust overlap
    /// during issuer rotation, and atomic activation all stay in
    /// [`peer_tls::reload`]; there is deliberately no Lumen-side reload engine
    /// for them to diverge from.
    ///
    /// Fails when no valid material exists at startup, which is the intended
    /// posture: a member that cannot prove who it is has no business joining
    /// the group.
    pub fn reloadable(
        &self,
        dns_names: impl IntoIterator<Item = String>,
        spiffe_uris: impl IntoIterator<Item = String>,
    ) -> Result<peer_tls::ReloadableTls> {
        peer_tls::ReloadableTls::required(
            peer_tls::TlsRuntimeProfile::peer(dns_names, spiffe_uris),
            std::sync::Arc::new(peer_tls::FileMaterialSource::new(
                &self.cert, &self.key, &self.ca,
            )),
        )
        .map_err(anyhow::Error::from)
    }
}

/// The switch that turns the client port into a TLS listener (#3113 R1).
const SERVING_TLS_ENV: &str = "LUMEN_TLS";

/// Where the serving leaf is projected, for the client port on `:7373`.
///
/// A separate type from [`PeerTlsConfig`] and not a mode of it. The peer port's
/// question is "is the dialer a member of this Raft group", answered by a
/// client certificate; the client port's question is "is the server the Service
/// I asked for", answered by this leaf while the *caller* proves itself with a
/// short-lived ServiceAccount token. There is no `required` flag here because
/// there is no mutual half to make optional.
///
/// ## Env contract
///
/// - `LUMEN_TLS=on` — serve TLS. Anything else leaves the port h2c.
/// - `LUMEN_TLS_CERT` / `LUMEN_TLS_KEY` / `LUMEN_TLS_CA` — the projected leaf,
///   its key, and the anchor callers are told to trust.
/// - `LUMEN_TLS_SERVER_NAMES` — comma-separated Service DNS names the leaf must
///   answer to. Optional; when absent the leaf is accepted for whatever names
///   it carries.
#[derive(Debug, Clone)]
pub struct ServingTlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    /// The Kubernetes Service DNS names callers dial. Checked against the leaf
    /// at load, so a certificate issued for some *other* Service fails here
    /// once instead of at every client in turn.
    pub dns_names: Vec<String>,
}

impl ServingTlsConfig {
    /// Load from env. `Ok(None)` means the deployment asked for h2c — the
    /// local/kind posture, and the only way to get cleartext on this port.
    ///
    /// Half a configuration is an error rather than a silent downgrade in
    /// either direction: paths without the switch would serve cleartext from a
    /// deployment that projected a certificate, and the switch without paths
    /// would come up with nothing to present. Both are the same mistake seen
    /// from opposite sides, and both are worth failing startup over.
    pub fn from_env() -> Result<Option<Self>> {
        let on = std::env::var(SERVING_TLS_ENV)
            .map(|v| v.eq_ignore_ascii_case("on"))
            .unwrap_or(false);
        let cert = std::env::var("LUMEN_TLS_CERT").ok().map(PathBuf::from);
        let key = std::env::var("LUMEN_TLS_KEY").ok().map(PathBuf::from);
        let ca = std::env::var("LUMEN_TLS_CA").ok().map(PathBuf::from);
        let dns_names = std::env::var("LUMEN_TLS_SERVER_NAMES")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect();
        match (on, cert, key, ca) {
            (false, None, None, None) => Ok(None),
            (true, Some(cert), Some(key), Some(ca)) => Ok(Some(Self {
                cert,
                key,
                ca,
                dns_names,
            })),
            (true, ..) => Err(anyhow::anyhow!(
                "{SERVING_TLS_ENV}=on but LUMEN_TLS_CERT / LUMEN_TLS_KEY / LUMEN_TLS_CA are not all set"
            )),
            (false, ..) => Err(anyhow::anyhow!(
                "LUMEN_TLS_CERT / LUMEN_TLS_KEY / LUMEN_TLS_CA are set but {SERVING_TLS_ENV} is not `on`; \
                 the client port would serve cleartext against a projected certificate"
            )),
        }
    }

    /// Bind the projected material to the shared reloadable seam (#3113 R1/R9).
    ///
    /// `required`, so a pod with no valid leaf never reaches the accept loop.
    /// Rotation after that point is [`peer_tls::reload`]'s business — the
    /// listener re-reads this on every accept, which is what makes a renewal
    /// cost no restart.
    pub fn reloadable(&self) -> Result<peer_tls::ReloadableTls> {
        peer_tls::ReloadableTls::required(
            peer_tls::TlsRuntimeProfile::serving(self.dns_names.clone()),
            std::sync::Arc::new(peer_tls::FileMaterialSource::new(
                &self.cert, &self.key, &self.ca,
            )),
        )
        .map_err(anyhow::Error::from)
    }
}

pub use peer_tls::install_default_crypto_provider;

#[cfg(test)]
mod tests;
