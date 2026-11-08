// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-competitor-performance-competitive
// @capability querying
// @claim query-quality
// @contract search-efficiency-filtering-ranking-pagination
// @category efficiency
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- benchmark_lumen_competitor_performance_competitive::lumen_competitor_performance_competitive --exact --ignored
// AW-EC-END

// Contract: Lumen-only default gate holds per-cell e2e/engine latency floors from perf-baseline.json without provisioning pg/OpenSearch.
// Contract: Retained pg/OpenSearch ratios remain the calibrated competitive evidence; explicit compare runners refresh them only when cells or peer configs change.
// Contract: FILTERING/RANKING/PAGINATION/SORT cells still execute through the same release-mode Lumen search path and qps pin.
// Contract: Peer floors are ratcheted (perf-baseline.json, 0.8) and btree point-lookup cells stay EXEMPT unless LUMEN_GATE_COMPARE_PEERS=1 is explicitly set.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_competitor_performance_competitive() {
    crate::support::native_contracts::test_binary(
        "LUMEN_CONTRACT_COMPETITIVE_TEST_BIN",
        "competitive_perf_gate",
    );
}
// CODEGEN-END
