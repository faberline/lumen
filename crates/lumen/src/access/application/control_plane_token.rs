//! The credential Lumen's own control-plane workloads — the operator's reshard
//! driver, the backup runner — present to a serving instance (#2877).

use crate::access::domain::identity::AUDIENCE;

/// Where a Lumen *control-plane* workload — the operator's reshard driver, the
/// backup runner — finds the token it presents to a serving instance (#2877).
///
/// Deliberately not `/var/run/secrets/kubernetes.io/serviceaccount`: that is
/// the kubelet's default projection, minted for the apiserver's audience, and
/// the whole point is that the two are different credentials. Keeping them in
/// different directories means a wiring mistake produces a missing file rather
/// than a token Lumen will reject for reasons that look like RBAC.
pub const CONTROL_PLANE_TOKEN_MOUNT: &str = "/var/run/secrets/lumen.axiom.dev";

/// Pod volume name for the projection at [`CONTROL_PLANE_TOKEN_MOUNT`].
pub const CONTROL_PLANE_TOKEN_VOLUME: &str = "lumen-admin-token";

/// The file itself.
pub const CONTROL_PLANE_TOKEN_FILE: &str = "/var/run/secrets/lumen.axiom.dev/token";

/// The credential a control-plane caller presents, read fresh per call.
///
/// Lumen owns *which* workloads need one and *what audience* it must carry;
/// the reading, the rotation contract, and the redaction rules are
/// [`service_auth::k8s::ProjectedTokenFile`]'s.
pub fn control_plane_token_file() -> service_auth::k8s::ProjectedTokenFile {
    service_auth::k8s::ProjectedTokenFile::new(CONTROL_PLANE_TOKEN_FILE, AUDIENCE)
}
