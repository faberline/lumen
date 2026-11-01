// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-long-running-stability-overload-backpressure
// @capability operations-observability
// @claim long-running-stability
// @contract search-stability-backpressure
// @category stability
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- stability_lumen_long_running_stability_overload_backpressure::lumen_long_running_stability_overload_backpressure --exact --ignored
// AW-EC-END

// Contract: backpressure_overload drives the live service at 600 offered QPS with 16 workers for 30s after warmup (3x the 200-QPS steady baseline), measures error_rate/p99_ms/achieved_qps, enforces error_rate <= 0.05 and p99_ms <= 250ms, and uses Rig's >=95% load-honesty gate so a crash, OOM, or collapsed request schedule cannot pass.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_overload_backpressure() {
    crate::support::native_contracts::scenarios(&["load/backpressure_overload"]);
}
// CODEGEN-END
