// CODEGEN-BEGIN
// AW-EC-BEGIN
// @ec lumen-claim-k8s-kustomize-base
// @capability kubernetes-native-deployment
// @claim layered-deployment-artifacts
// @contract k8s-kustomize-base-overlays
// @category behavior
// @required_for_production true
// @command cargo test --release --locked -p lumen --test it -- behavior_lumen_claim_k8s_kustomize_base::lumen_claim_k8s_kustomize_base --exact --ignored
// AW-EC-END

// Contract: The Kubernetes base, overlays, HPA/PDB, and operator manifests render successfully.
#[test]
#[ignore = "native hosted contract owner; exact roster is required by development CI"]
fn lumen_claim_k8s_kustomize_base() {
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
