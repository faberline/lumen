// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-cli-connect-query-workflow
// @capability api-cli-agent-integration
// @claim standard-cli-and-agent-interface
// @contract cli-connect-query-workflow
// @category behavior
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- behavior_lumen_claim_cli_connect_query_workflow::lumen_claim_cli_connect_query_workflow --exact --ignored
// AW-EC-END

// Contract: The connect and query command groups parse and the query adapter emits the published request shape.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_cli_connect_query_workflow() {
    crate::support::native_contracts::cli_help();
    crate::support::native_contracts::test_binary(
        "LUMEN_CONTRACT_CLI_TEST_BIN",
        "query::tests::build_index_body_matches_published_index_shape",
    );
}
// CODEGEN-END
