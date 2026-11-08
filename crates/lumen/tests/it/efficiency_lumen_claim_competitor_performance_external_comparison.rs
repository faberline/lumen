// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-competitor-performance-external-comparison
// @capability querying
// @claim query-quality
// @contract competitor-performance-external-comparison
// @category efficiency
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- efficiency_lumen_claim_competitor_performance_external_comparison::lumen_claim_competitor_performance_external_comparison --exact --ignored
// AW-EC-END

// Contract: The vat efficiency runner executes the Lumen-only regression path against retained Postgres/OpenSearch-calibrated floors; explicit calibration runners refresh peers only on demand.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_competitor_performance_external_comparison() {
    crate::support::native_contracts::test_binary(
        "LUMEN_CONTRACT_COMPETITIVE_TEST_BIN",
        "competitive_perf_gate",
    );
}
// CODEGEN-END
