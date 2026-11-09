// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-long-running-stability-resource-leak
// @capability operations-observability
// @claim long-running-stability
// @contract search-stability-resource-leak
// @category stability
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- stability_lumen_long_running_stability_resource_leak::lumen_long_running_stability_resource_leak --exact --ignored
// AW-EC-END

// Contract: fd_leak resolves the unique Lumen listener PID, runs sustained bounded-keyspace index and search work, records independent fd/socket/thread counts before and after, requires zero request failures, and gates every after count at <= 1.20 * before + 16.
// Contract: soak_rss_plateau warms a bounded keyspace, runs two mixed workload windows with zero request failures, measures rss_w1/rss_w2 from the live Lumen PID, and requires rss_w2 <= 1.10 * rss_w1.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_resource_leak() {
    crate::support::native_contracts::scenarios(&[
        "endurance/fd_leak",
        "endurance/soak_rss_plateau",
    ]);
}
// CODEGEN-END
