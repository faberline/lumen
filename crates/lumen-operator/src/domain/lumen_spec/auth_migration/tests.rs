use std::collections::BTreeMap;

use crate::domain::lumen_spec::access::{AccessSpec, ServiceAccountSubject};
use crate::domain::lumen_spec::serving::{AuthMode, LogFormat, ServingSpec};
use crate::domain::lumen_spec::topology::{ReshardPolicy, ShardMapSpec};
use crate::domain::lumen_spec::{LumenSpec, PlacementSpec};

use super::{MigrationFinding, MigrationNotice, Release, REFUSAL_RELEASE};

const BEFORE: Release = Release {
    major: 0,
    minor: 22,
};
const FROM: Release = Release {
    major: 0,
    minor: 23,
};

fn test_spec() -> LumenSpec {
    LumenSpec {
        image: "lumen:latest".into(),
        image_pull_policy: None,
        image_pull_secrets: Vec::new(),
        placement: PlacementSpec::default(),
        shard_count: 1,
        shard_map: ShardMapSpec::default(),
        replicas_per_shard: 1,
        voter_count: 1,
        log_format: LogFormat::Pretty,
        log_level: None,
        auth: AuthMode::Off,
        serving: ServingSpec::default(),
        reshard_policy: ReshardPolicy::default(),
        observability: false,
        network_policy: false,
        admission: None,
        service_account_name: None,
        service_account_annotations: BTreeMap::new(),
        peer_tls_secret: None,
        serving_tls_secret: None,
        body_limit_bytes: None,
        access: None,
        runtime: None,
        extra_env: Vec::new(),
        extra_args: Vec::new(),
    }
}

fn spec(access: Option<AccessSpec>, auth: AuthMode) -> LumenSpec {
    let mut spec = test_spec();
    spec.access = access;
    spec.auth = auth;
    spec
}

fn subject(namespace: &str, name: &str) -> ServiceAccountSubject {
    ServiceAccountSubject {
        namespace: namespace.into(),
        name: name.into(),
    }
}

fn empty_access() -> Option<AccessSpec> {
    Some(AccessSpec {
        allowed_service_accounts: Vec::new(),
    })
}

fn one_subject_access() -> Option<AccessSpec> {
    Some(AccessSpec {
        allowed_service_accounts: vec![subject("apps", "web")],
    })
}

fn notice(finding: MigrationFinding, message: &str) -> MigrationNotice {
    MigrationNotice {
        finding,
        message: message.into(),
    }
}

#[test]
fn missing_access_warns_before_0_23_and_is_refused_from_0_23() {
    let s = spec(None, AuthMode::Required);

    let before = s.auth_migration(BEFORE, false);
    assert_eq!(
        before.warnings,
        vec![notice(
            MigrationFinding::MissingAccess,
            "spec.access is absent: the current external RBAC behavior is kept and is deprecated; set spec.access.allowedServiceAccounts (an empty list denies all) before 0.23.0",
        )]
    );
    assert!(before.refusals.is_empty());

    let from = s.auth_migration(FROM, false);
    assert_eq!(
        from.refusals,
        vec![notice(
            MigrationFinding::MissingAccess,
            "spec.access is required from 0.23.0: set spec.access.allowedServiceAccounts (an empty list denies all)",
        )]
    );
    assert!(from.warnings.is_empty());
}

#[test]
fn empty_access_is_explicit_deny_all_and_never_a_finding() {
    let s = spec(empty_access(), AuthMode::Required);
    assert!(s.auth_migration(BEFORE, false).is_clean());
    assert!(s.auth_migration(FROM, false).is_clean());
}

#[test]
fn access_with_subjects_is_clean() {
    let s = spec(one_subject_access(), AuthMode::Required);
    assert!(s.auth_migration(BEFORE, false).is_clean());
    assert!(s.auth_migration(FROM, false).is_clean());
}

#[test]
fn disabled_auth_warns_before_and_is_refused_from_0_23() {
    let s = spec(empty_access(), AuthMode::Off);

    let before = s.auth_migration(BEFORE, false);
    assert_eq!(
        before.warnings,
        vec![notice(
            MigrationFinding::DisabledAuth,
            "spec.auth: disabled is deprecated for a Managed Lumen; remove it or set required before 0.23.0",
        )]
    );
    assert!(before.refusals.is_empty());

    let from = s.auth_migration(FROM, false);
    assert_eq!(
        from.refusals,
        vec![notice(
            MigrationFinding::DisabledAuth,
            "spec.auth: disabled is refused for a Managed Lumen from 0.23.0; remove it or set required",
        )]
    );
    assert!(from.warnings.is_empty());
}

#[test]
fn per_collection_grants_flag_adds_its_finding_without_a_second_access_source() {
    let s = spec(one_subject_access(), AuthMode::Required);

    let before = s.auth_migration(BEFORE, true);
    assert_eq!(
        before.warnings,
        vec![notice(
            MigrationFinding::PerCollectionGrants,
            "per-collection access grants are deprecated; move to spec.access.allowedServiceAccounts before 0.23.0",
        )]
    );
    assert!(before.refusals.is_empty());
    assert!(before.warnings[0]
        .message
        .contains("spec.access.allowedServiceAccounts"));

    let from = s.auth_migration(FROM, true);
    assert_eq!(
        from.refusals,
        vec![notice(
            MigrationFinding::PerCollectionGrants,
            "per-collection access grants are removed in 0.23.0; use spec.access.allowedServiceAccounts, the only access source",
        )]
    );
    assert!(from.warnings.is_empty());
    assert!(from.refusals[0]
        .message
        .contains("spec.access.allowedServiceAccounts"));
}

#[test]
fn all_three_findings_are_reported_in_order() {
    let s = spec(None, AuthMode::Off);
    let order = |notices: &[MigrationNotice]| -> Vec<MigrationFinding> {
        notices.iter().map(|n| n.finding).collect()
    };

    let before = s.auth_migration(BEFORE, true);
    assert_eq!(
        order(&before.warnings),
        vec![
            MigrationFinding::MissingAccess,
            MigrationFinding::DisabledAuth,
            MigrationFinding::PerCollectionGrants,
        ]
    );
    assert!(before.refusals.is_empty());

    let from = s.auth_migration(FROM, true);
    assert_eq!(
        order(&from.refusals),
        vec![
            MigrationFinding::MissingAccess,
            MigrationFinding::DisabledAuth,
            MigrationFinding::PerCollectionGrants,
        ]
    );
    assert!(from.warnings.is_empty());
}

#[test]
fn release_ordering_uses_major_then_minor() {
    assert!(BEFORE < REFUSAL_RELEASE);
    assert!(FROM >= REFUSAL_RELEASE);
    assert!(Release { major: 1, minor: 0 } >= REFUSAL_RELEASE);
}

#[test]
fn refusal_is_never_downgraded_to_a_warning_from_0_23() {
    // Negative control: a comparison that is inverted or removed would move
    // these findings into `warnings`.
    let s = spec(None, AuthMode::Off);
    for release in [
        Release {
            major: 0,
            minor: 23,
        },
        Release {
            major: 0,
            minor: 24,
        },
        Release { major: 1, minor: 0 },
    ] {
        let report = s.auth_migration(release, true);
        assert!(report.warnings.is_empty(), "no warnings at {release:?}");
        assert_eq!(report.refusals.len(), 3, "three refusals at {release:?}");
    }
}

#[test]
fn messages_never_carry_secret_material() {
    let s = spec(None, AuthMode::Off);
    for release in [BEFORE, FROM] {
        let report = s.auth_migration(release, true);
        for n in report.warnings.iter().chain(report.refusals.iter()) {
            for forbidden in ["Bearer", "password", "BEGIN", "token"] {
                assert!(
                    !n.message.contains(forbidden),
                    "message at {release:?} contains {forbidden:?}: {}",
                    n.message
                );
            }
        }
    }
}
