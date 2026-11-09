// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-long-running-stability-resilience-survival
// @capability operations-observability
// @claim long-running-stability
// @contract search-p99-survives-fault-and-recovers
// @category stability
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- stability_lumen_long_running_stability_resilience_survival::lumen_long_running_stability_resilience_survival --exact --ignored
// AW-EC-END

// Contract: packet_loss_p99 applies downstream toxiproxy timeout toxicity 0.05, requires 0 < loss_fail <= 30 and loss_p99 <= 2 * baseline_p99 + 20ms, then removes the toxic, records loss_recovered_recovered_secs <= 10, requires loss_recovery_fail == 0, and requires loss_recovery_p99 <= 2 * baseline_p99 + 1ms.
// Contract: partition_recovery requires partition_fail > 0 under a full downstream partition, records recovered_recovered_secs <= 10 after toxic removal, and requires recovery_p99 <= 2 * baseline_p99 + 1ms.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_resilience_survival() {
    crate::support::native_contracts::scenarios(&[
        "resilience/packet_loss_p99",
        "resilience/partition_recovery",
    ]);
}
// CODEGEN-END
