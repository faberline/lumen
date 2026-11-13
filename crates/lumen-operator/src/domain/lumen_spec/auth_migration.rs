//! The auth migration policy for a Managed `Lumen` spec. Before `0.23.0` a
//! deprecated shape yields warnings; from `0.23.0` it yields refusals. This
//! module is pure: it is not wired into validation, reconcile or status.

use crate::domain::lumen_spec::serving::AuthMode;
use crate::domain::lumen_spec::LumenSpec;

/// A release version, compared by major then minor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Release {
    pub major: u32,
    pub minor: u32,
}

/// The first release that refuses the deprecated shapes.
pub const REFUSAL_RELEASE: Release = Release {
    major: 0,
    minor: 23,
};

/// Which deprecated shape a notice reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationFinding {
    MissingAccess,
    DisabledAuth,
    PerCollectionGrants,
}

/// One warning or refusal with its exact message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationNotice {
    pub finding: MigrationFinding,
    pub message: String,
}

/// The findings for one spec at one release. Each finding is in exactly one
/// of the two vectors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReport {
    pub warnings: Vec<MigrationNotice>,
    pub refusals: Vec<MigrationNotice>,
}

impl MigrationReport {
    /// True when there is neither a warning nor a refusal.
    pub fn is_clean(&self) -> bool {
        self.warnings.is_empty() && self.refusals.is_empty()
    }
}

impl LumenSpec {
    /// Classify the auth-migration shapes of this spec for `release`.
    /// `per_collection_grants` is true when the caller found per-collection
    /// RBAC grants for this Lumen.
    pub fn auth_migration(&self, release: Release, per_collection_grants: bool) -> MigrationReport {
        // (finding, warning text before REFUSAL_RELEASE, refusal text from it)
        let mut found: Vec<(MigrationFinding, &str, &str)> = Vec::new();
        if self.access.is_none() {
            found.push((
                MigrationFinding::MissingAccess,
                "spec.access is absent: the current external RBAC behavior is kept and is deprecated; set spec.access.allowedServiceAccounts (an empty list denies all) before 0.23.0",
                "spec.access is required from 0.23.0: set spec.access.allowedServiceAccounts (an empty list denies all)",
            ));
        }
        if self.auth == AuthMode::Off {
            found.push((
                MigrationFinding::DisabledAuth,
                "spec.auth: disabled is deprecated for a Managed Lumen; remove it or set required before 0.23.0",
                "spec.auth: disabled is refused for a Managed Lumen from 0.23.0; remove it or set required",
            ));
        }
        if per_collection_grants {
            found.push((
                MigrationFinding::PerCollectionGrants,
                "per-collection access grants are deprecated; move to spec.access.allowedServiceAccounts before 0.23.0",
                "per-collection access grants are removed in 0.23.0; use spec.access.allowedServiceAccounts, the only access source",
            ));
        }

        let refuse = release >= REFUSAL_RELEASE;
        let mut warnings = Vec::new();
        let mut refusals = Vec::new();
        for (finding, warning, refusal) in found {
            let message = if refuse { refusal } else { warning };
            let notice = MigrationNotice {
                finding,
                message: message.into(),
            };
            if refuse {
                refusals.push(notice);
            } else {
                warnings.push(notice);
            }
        }
        MigrationReport { warnings, refusals }
    }
}

#[cfg(test)]
mod tests;
