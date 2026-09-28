use std::collections::BTreeMap;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::operator::application::reconcile::shard_usage::{
    aggregate_shard_usage, parse_metric, pod_metrics_urls, pod_storage_bytes,
};
use crate::operator::domain::lumen_spec::Lumen;

fn body_with(bytes: u64) -> String {
    format!(
        "# HELP lumen_storage_bytes docs\n\
             # TYPE lumen_storage_bytes gauge\n\
             lumen_storage_bytes {bytes}\n"
    )
}

#[test]
fn parse_metric_reads_matching_gauge_line() {
    let body = body_with(2048);
    assert_eq!(parse_metric(&body, "lumen_storage_bytes"), Some(2048));
}

#[test]
fn parse_metric_missing_metric_is_none() {
    let body = "lumen_docs_total 3\n";
    assert_eq!(parse_metric(body, "lumen_storage_bytes"), None);
}

#[tokio::test]
async fn pod_storage_bytes_fetches_and_parses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body_with(4096)))
        .mount(&server)
        .await;
    let http = reqwest::Client::new();
    let url = format!("{}/metrics", server.uri());
    assert_eq!(pod_storage_bytes(&http, &url).await, Some(4096));
}

#[tokio::test]
async fn pod_storage_bytes_unreachable_pod_is_none() {
    let http = reqwest::Client::new();
    // No listener on this port; connection should fail promptly.
    let url = "http://127.0.0.1:1/metrics";
    assert_eq!(pod_storage_bytes(&http, url).await, None);
}

#[tokio::test]
async fn aggregate_shard_usage_takes_max_within_a_shard() {
    // shard 0 has two replicas (2048, 8192 bytes); max, not sum, wins.
    let a = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body_with(2048)))
        .mount(&a)
        .await;
    let b = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body_with(8192)))
        .mount(&b)
        .await;
    let http = reqwest::Client::new();
    let urls = vec![
        (0u32, format!("{}/metrics", a.uri())),
        (0u32, format!("{}/metrics", b.uri())),
    ];
    let usage = aggregate_shard_usage(&http, &urls).await;
    assert_eq!(usage.get(&0), Some(&8192));
}

#[tokio::test]
async fn aggregate_shard_usage_skips_unreachable_pods() {
    let a = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body_with(1024)))
        .mount(&a)
        .await;
    let http = reqwest::Client::new();
    let urls = vec![
        (0u32, "http://127.0.0.1:1/metrics".to_string()),
        (1u32, format!("{}/metrics", a.uri())),
    ];
    let usage = aggregate_shard_usage(&http, &urls).await;
    assert_eq!(usage.get(&0), None);
    assert_eq!(usage.get(&1), Some(&1024));
}

#[test]
fn pod_metrics_urls_covers_every_storage_pod_by_headless_dns() {
    use crate::operator::domain::lumen_spec::{
        serving::ServingSpec, topology::ShardMapSpec, LumenSpec,
    };
    let spec = LumenSpec {
        image: "lumen:latest".into(),
        image_pull_policy: None,
        placement: Default::default(),
        shard_count: 2,
        shard_map: ShardMapSpec::default(),
        replicas_per_shard: 3,
        voter_count: 3,
        log_format: Default::default(),
        log_level: None,
        auth: Default::default(),
        serving: ServingSpec::default(),
        reshard_policy: Default::default(),
        observability: false,
        network_policy: false,
        admission: None,
        service_account_name: None,
        service_account_annotations: BTreeMap::new(),
        peer_tls_secret: None,
        serving_tls_secret: None,
        body_limit_bytes: None,
    };
    let mut lumen = Lumen::new("search", spec);
    lumen.metadata.namespace = Some("acme".to_string());

    let urls = pod_metrics_urls(&lumen);
    assert_eq!(urls.len(), 6);
    assert!(urls.contains(&(
        0,
        "http://search-0.search-headless.acme.svc.cluster.local:7373/metrics".to_string()
    )));
    // ordinal 3 = replica_index 1, shard_index 1 (3 % 2 == 1).
    assert!(urls.contains(&(
        1,
        "http://search-3.search-headless.acme.svc.cluster.local:7373/metrics".to_string()
    )));
}
