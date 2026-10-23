//! The audience a Managed token is bound to, and the API group and resources
//! every authorization check names.

/// The audience every Managed token must be bound to, and the only audience
/// Managed Lumen requests in `TokenReview`.
///
/// A token minted for the apiserver's own audience — which is what every pod's
/// default ServiceAccount token is — must not open Managed Lumen. Requesting
/// this audience explicitly is what keeps Managed separate. Standalone's
/// explicit `in-cluster` profile uses Kubernetes' default audience instead.
pub const AUDIENCE: &str = "lumen.axiom.dev";

/// The API group every authorization check is asked under.
pub const API_GROUP: &str = "lumen.axiom.dev";

/// The resource a per-collection check names. The collection id is the
/// resource *name*, so a RoleBinding can grant one collection by
/// `resourceNames` instead of a wildcard.
pub const COLLECTIONS_RESOURCE: &str = "lumencollections";

/// The resource instance-wide administration is checked against — backups,
/// restores, resharding, checkpoints. Deliberately not `lumencollections/*`.
pub const ADMIN_RESOURCE: &str = "lumenadmin";
