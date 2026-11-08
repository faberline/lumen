// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-long-running-stability-latency-drift
// @capability operations-observability
// @claim long-running-stability
// @contract search-stability-latency-drift
// @category stability
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- stability_lumen_long_running_stability_latency_drift::lumen_long_running_stability_latency_drift --exact --ignored
// AW-EC-END

// Contract: soak_p99_drift warms a bounded keyspace, records p99_w1/p99_w2/p99_w3 over three 500-search windows with zero failures, and requires each adjacent window plus window 3 versus window 1 to stay <= 1.10x + 6ms.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_latency_drift() {
    crate::support::native_contracts::scenarios(&["endurance/soak_p99_drift"]);
}
// CODEGEN-END
