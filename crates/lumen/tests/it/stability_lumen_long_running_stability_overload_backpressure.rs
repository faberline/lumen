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

// Contract: retain the whole old rig-load directory. Keep steady search at 200 QPS,
// overload at 600 QPS, their original workers/windows/pins and >=95% honesty gate,
// plus the complete HTTP cursor browse oracle and depth bound. Ratchets use the
// pinned Rig same-host baseline and explicit NoBaseline/RIG_STRICT semantics.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_long_running_stability_overload_backpressure() {
    crate::support::native_contracts::scenarios(&[
        "load/backpressure_overload",
        "load/data_table_browse",
        "load/search_qps",
    ]);
}
// CODEGEN-END
