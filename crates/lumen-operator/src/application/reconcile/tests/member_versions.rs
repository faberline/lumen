use std::collections::HashMap;

use service_k8s::{ManagedService, ReadyFacts};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::application::reconcile::member_versions::{
    collect_member_reports, fetch_member_report, now_secs, pod_version_urls, record_member_reports,
};
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

const VERSION_BODY: &str = r#"{"version":"0.7.0","git_sha":"x","built_at":"y","binary_capabilities":["search_facets_v1"],"active_capabilities":[],"effective_compatibility_version":"0.2"}"#;

#[tokio::test]
async fn fetch_member_report_reads_version_and_capabilities() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(VERSION_BODY))
        .mount(&server)
        .await;
    let url = format!("{}/version", server.uri());

    let report = fetch_member_report(&reqwest::Client::new(), "search-0", &url, 1234)
        .await
        .expect("a 200 with the version body is a report");

    assert_eq!(report.name, "search-0");
    assert_eq!(report.version, "0.7.0");
    assert_eq!(report.capabilities, vec!["search_facets_v1".to_string()]);
    assert_eq!(report.compatibility_version, "0.2");
    assert_eq!(report.observed_at_secs, 1234);
}

#[tokio::test]
async fn fetch_member_report_unreachable_or_bad_body_is_none() {
    let http = reqwest::Client::new();
    assert!(
        fetch_member_report(&http, "search-0", "http://127.0.0.1:1/version", 1)
            .await
            .is_none()
    );

    let not_json = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&not_json)
        .await;
    let url = format!("{}/version", not_json.uri());
    assert!(fetch_member_report(&http, "search-0", &url, 1)
        .await
        .is_none());

    let server_error = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server_error)
        .await;
    let url = format!("{}/version", server_error.uri());
    assert!(fetch_member_report(&http, "search-0", &url, 1)
        .await
        .is_none());
}

#[tokio::test]
async fn collect_member_reports_keeps_the_previous_report_when_a_pod_is_unreachable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string(VERSION_BODY))
        .mount(&server)
        .await;
    let pods = vec![
        ("search-0".to_string(), format!("{}/version", server.uri())),
        (
            "search-1".to_string(),
            "http://127.0.0.1:1/version".to_string(),
        ),
    ];
    let previous = vec![MemberVersionReport {
        name: "search-1".to_string(),
        version: "0.6.0".to_string(),
        capabilities: Vec::new(),
        compatibility_version: "0.1".to_string(),
        observed_at_secs: 10,
    }];

    let reports = collect_member_reports(&reqwest::Client::new(), &pods, &previous, 500).await;

    assert_eq!(reports.len(), 2, "{reports:?}");
    let kept = reports.iter().find(|r| r.name == "search-1").unwrap();
    assert_eq!(
        kept.observed_at_secs, 10,
        "an unreachable pod keeps its last report"
    );
    let fresh = reports.iter().find(|r| r.name == "search-0").unwrap();
    assert_eq!(fresh.observed_at_secs, 500);
    assert_eq!(fresh.compatibility_version, "0.2");
}

#[test]
fn pod_version_urls_follow_the_statefulset_pod_names() {
    let lumen = hpa_test_lumen("search", "ns1", 2, 2);
    let urls = pod_version_urls(&lumen);
    assert_eq!(urls.len(), 4);
    assert_eq!(
        urls[0],
        (
            "search-0".to_string(),
            "http://search-0.search-headless.ns1.svc.cluster.local:7373/version".to_string()
        )
    );
}
