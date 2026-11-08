// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-developer-interactive-tooling
// @capability api-cli-agent-integration
// @claim standard-cli-and-agent-interface
// @contract developer-interactive-tooling
// @category behavior
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- behavior_lumen_claim_developer_interactive_tooling::lumen_claim_developer_interactive_tooling --exact --ignored
// AW-EC-END

// Contract: The installed CLI exposes discoverable connect and query tooling for agents and operators.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_developer_interactive_tooling() {
    crate::support::native_contracts::cli_help();
}
// CODEGEN-END
