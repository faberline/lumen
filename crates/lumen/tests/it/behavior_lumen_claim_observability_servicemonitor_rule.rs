// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-observability-servicemonitor-rule
// @capability operations-observability
// @claim control-plane-observability
// @contract observability-servicemonitor-rule
// @category behavior
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- behavior_lumen_claim_observability_servicemonitor_rule::lumen_claim_observability_servicemonitor_rule --exact --ignored
// AW-EC-END

// Contract: The production overlay renders the ServiceMonitor and PrometheusRule bundle.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_observability_servicemonitor_rule() {
    crate::support::native_contracts::kustomize(&["k8s/overlays/prod"], true);
}
// CODEGEN-END
