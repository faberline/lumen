// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-long-running-stability-query-resilience
// @capability operations-observability
// @claim long-running-stability
// @contract search-stability-fault-resilience
// @category stability
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- stability_lumen_long_running_stability_query_resilience::lumen_long_running_stability_query_resilience --exact --ignored
// AW-EC-END

// Contract: packet_loss_p99 routes baseline and fault search samples through toxiproxy with downstream timeout toxicity 0.05, requires 0 < loss_fail <= 30 and loss_p99 <= 2 * baseline_p99 + 20ms, then removes the toxic, records loss_recovered_recovered_secs <= 10, requires loss_recovery_fail == 0, and requires loss_recovery_p99 <= 2 * baseline_p99 + 1ms.
// Contract: partition_recovery applies a full downstream partition, requires partition_fail > 0, records recovered_recovered_secs <= 10, and requires recovery_p99 <= 2 * baseline_p99 + 1ms after the toxic is removed.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_query_resilience() {
    crate::support::native_contracts::scenarios(&[
        "resilience/packet_loss_p99",
        "resilience/partition_recovery",
    ]);
}
// CODEGEN-END
