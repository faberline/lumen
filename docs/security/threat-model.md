# Lumen threat model (1.0.0)

This model covers the HTTP API, the operator and the release image. It names
what Lumen protects, who may call it, and which control enforces each threat.
Each mitigation names the repository file where it is written down or enforced.

## Assets

- Indexed documents and vectors on disk: the WAL, the segments and the checkpoints.
- Backup artifacts that carry index data out of the runtime.
- Credentials and tokens: Kubernetes ServiceAccount tokens and TLS keys.
- The operator's Kubernetes RBAC, which decides what the operator may change.
- The container image and the release artifacts (binary, SBOM, signatures).

## Trust boundaries

### HTTP API

Callers are application workloads and developers' tools. In Managed and
in-cluster Standalone mode, a caller is trusted only after TokenReview accepts
its ServiceAccount token. SubjectAccessReview and RBAC then decide each
operation. Reachability (ClusterIP and NetworkPolicy) is not trusted as
authentication. Local Standalone mode (`LUMEN_AUTH=off`) listens on loopback
and trusts the local network path.

### Operator

The operator runs in its own namespace with a ClusterRole. It is trusted to
reconcile the `lumen.dev` custom resources and the child kinds it renders. It
is not a caller of normal search requests. A human developer does not give a
kubeconfig identity to Lumen; they mint a short-lived token for an allowed client
ServiceAccount.

### Image

The release image is built in CI from a published release binary, not from a
developer's machine. The runtime is distroless with no shell and runs as
nonroot. The image is trusted only when its keyless cosign signature and its
attestations verify. `cargo deny` is covered by #307 and #325.

## Threats

| ID | Surface | Threat | Mitigation ID |
|---|---|---|---|
| T1 | HTTP API | A caller without a valid token reads or writes indexed data. | M1 |
| T2 | HTTP API | A caller with a valid ServiceAccount token exceeds its RBAC rights. | M2 |
| T3 | HTTP API | An oversized request body exhausts memory. | M3 |
| T4 | HTTP API | An unauthenticated caller reads data through a route left anonymous. | M4 |
| T5 | Operator | The operator ServiceAccount is abused to read or change objects outside its scope. | M5 |
| T6 | Operator | A client workload with a token reads another tenant's indexes. | M6 |
| T7 | Operator | The reconcile loop grants broader RBAC than the custom resource asks for. | M5 |
| T8 | Image | A tampered release binary is baked into the image. | M7 |
| T9 | Image | An image is published or replaced without a matching signature. | M8 |
| T10 | Image | The shipped image has no SBOM that records its contents. | M9 |
| T11 | Image | The container runs with a shell or root privileges that help an attacker pivot. | M10 |

## Mitigations

| ID | Mitigation | Where it is enforced |
|---|---|---|
| M1 | Protected routes require a Kubernetes ServiceAccount token checked by TokenReview. | docs/authentication.md |
| M2 | SubjectAccessReview and RBAC check each protected operation. | docs/authentication.md |
| M3 | The request body limit (default 8 MiB) returns a structured 413. | crates/lumen/tests/body_limit_configurable.rs |
| M4 | Anonymous paths are listed in one place; the planned contract shrinks that list. | docs/authentication.md |
| M5 | The operator ClusterRole is limited to the `lumen.dev` resources and the child kinds it renders. | k8s/operator/rbac.yaml |
| M6 | Clients reach only their own collections, with a private ClusterIP and NetworkPolicy; SubjectAccessReview is the authorization boundary. | docs/authentication.md |
| M7 | The release binary's SHA-256 is checked before it enters the image, and the base images are pinned by digest. | Dockerfile.release |
| M8 | The root image index carries a keyless cosign signature, and `cosign verify` runs before promotion. | .github/workflows/lumen-release-candidate.yml, scripts/verify-release-candidate.sh |
| M9 | Each platform has an SPDX SBOM attestation and provenance attestation, checked with `gh attestation verify` in scripts/verify-release-candidate.sh; lumen-release.yml runs scripts/verify-release-artifacts.sh before promotion. | .github/workflows/lumen-release-candidate.yml, scripts/verify-release-candidate.sh, .github/workflows/lumen-release.yml |
| M10 | The runtime is distroless static with no shell, and it runs as the nonroot user (uid 65532). | Dockerfile.release |
