use std::collections::HashMap;

use service_k8s::{ManagedService, ReadyFacts};

use crate::application::reconcile::member_versions::{now_secs, record_member_reports};
use crate::application::reconcile::shard_usage::cache_key;
use crate::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::domain::lumen_spec::member_versions::MemberVersionReport;

fn fresh(name: &str, compat: &str) -> MemberVersionReport {
    MemberVersionReport {
        name: name.to_string(),
        version: "0.7.0".to_string(),
        capabilities: vec!["search".to_string()],
        compatibility_version: compat.to_string(),
        observed_at_secs: now_secs(),
    }
}

#[test]
fn status_patch_lists_members_and_effective_version() {
    let lumen = hpa_test_lumen("search", "acme-member-status", 2, 1);
    record_member_reports(
        &cache_key(&lumen),
        vec![fresh("search-0", "0.2"), fresh("search-1", "0.1")],
    );

    let patch = lumen.status_patch(&ReadyFacts::new(HashMap::new()));
    let members = patch["status"]["members"]
        .as_array()
        .expect("status.members must be present once reports are cached");
    assert_eq!(members.len(), 2, "{patch}");
    assert_eq!(patch["status"]["effectiveCompatibilityVersion"], "0.1");
}

#[test]
fn status_patch_has_no_member_fields_without_reports() {
    let lumen = hpa_test_lumen("search", "acme-member-none", 2, 1);

    let patch = lumen.status_patch(&ReadyFacts::new(HashMap::new()));
    assert!(patch["status"].get("members").is_none(), "{patch}");
    assert!(
        patch["status"]
            .get("effectiveCompatibilityVersion")
            .is_none(),
        "{patch}"
    );
}
