// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-long-running-kustomize-base
// @capability operations-observability
// @claim long-running-stability
// @contract long-running-kustomize-base-overlays
// @category behavior
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- behavior_lumen_claim_long_running_kustomize_base::lumen_claim_long_running_kustomize_base --exact --ignored
// AW-EC-END

// Contract: The base, dev, staging, prod, and operator kustomize surfaces render valid Kubernetes manifests.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_long_running_kustomize_base() {
    crate::support::native_contracts::kustomize(
        &[
            "k8s/base",
            "k8s/overlays/dev",
            "k8s/overlays/staging",
            "k8s/overlays/prod",
            "k8s/operator",
        ],
        false,
    );
}
// CODEGEN-END
