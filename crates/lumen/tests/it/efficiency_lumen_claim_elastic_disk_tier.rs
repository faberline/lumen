// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-elastic-disk-tier
// @capability scaling-availability
// @claim elastic-segment-tier
// @contract elastic-disk-tier
// @category efficiency
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- efficiency_lumen_claim_elastic_disk_tier::lumen_claim_elastic_disk_tier --exact --ignored
// AW-EC-END

// Contract: The disk-scale proof keeps the full corpus on disk-backed segments while bounded hot state remains in memory.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_elastic_disk_tier() {
    crate::support::native_contracts::test_binary(
        "LUMEN_CONTRACT_SCALE_TEST_BIN",
        "scale_proof_reopen_rss_is_bounded",
    );
}
// CODEGEN-END
