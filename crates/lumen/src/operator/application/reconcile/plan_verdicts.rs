//! What the plan hook found — a missing Raft peer identity (#2890) or a refused
//! auth-delegator binding (#2876) — and the reconcile-context keys that carry
//! each verdict to the condition projection.

use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;

/// Why this replicated instance has no usable Raft peer identity, if it has
/// none (#2890 R4).
///
/// `Some(_)` is a fail-closed verdict: a `replicasPerShard > 1` instance whose
/// `spec.peerTlsSecret` is unset.
///
/// Single-replica instances run no consensus link and are always `None`: there
/// is no peer to authenticate.
pub(super) fn check_peer_identity(lumen: &Lumen) -> Option<String> {
    if !lumen.spec.peer_identity_required() {
        return None;
    }
    if lumen.spec.peer_tls_secret.is_none() {
        return Some(format!(
            "replicasPerShard={} requires spec.peerTlsSecret naming a Secret with {}; \
             replicated Raft traffic has no plaintext fallback",
            lumen.spec.replicas_per_shard,
            render::PEER_TLS_KEYS.join(", ")
        ));
    }
    None
}

/// The reconcile-context key carrying [`apply_auth_delegator_binding`]'s
/// verdict from the plan hook to the condition projection (#2876).
///
/// [`apply_auth_delegator_binding`]: crate::operator::application::reconcile::auth_delegator::apply_auth_delegator_binding
pub(super) const AUTH_DELEGATION_CONTEXT_KEY: &str = "authDelegationError";

/// The same channel for [`check_peer_identity`]'s verdict (#2890). Same reason:
/// the check is a Secret read, and `observe` is I/O-free by contract.
///
/// Public so the render-gate tests project a condition through the key the
/// reconcile loop actually writes, rather than a string literal that would keep
/// passing after a rename.
pub const PEER_IDENTITY_CONTEXT_KEY: &str = "peerIdentityError";

/// Read that verdict back out. Absent key = peer identity is satisfied (or not
/// required).
pub(super) fn peer_identity_error(context: &serde_json::Value) -> Option<String> {
    context
        .get(PEER_IDENTITY_CONTEXT_KEY)?
        .as_str()
        .map(str::to_string)
}

/// Read that verdict back out. Absent key = the binding applied.
pub(super) fn auth_delegation_error(context: &serde_json::Value) -> Option<String> {
    context
        .get(AUTH_DELEGATION_CONTEXT_KEY)?
        .as_str()
        .map(str::to_string)
}
