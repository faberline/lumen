#!/usr/bin/env bash
# Deploy-only kind run of one cluster e2e suite (faberline/aw
# docs/standards/rust-tests.md: Rust decides pass or fail, a script only
# deploys, waits, and collects logs).
#
# It gives the suite the same shape a run on the shared GKE test cluster gets
# from aw's gke-test run.sh: a fresh k8s/overlays/gke-test with the image under
# test, the ConfigMap gke-test-env, and the suite's test Job
# (k8s/gke-test-job/<suite>), which runs
# `lumen-it --ignored --test-threads=1 e2e::<suite>::` on the client node. This
# script checks nothing about lumen. It passes only when Job/lumen-e2e
# completes, and it prints the Job's log either way.
#
# Usage:  scripts/kind-cluster.sh overlay|operator
#
# Inputs:
#   E2E_EXPECTED_IMAGE           ghcr.io/faberline/lumen@sha256:<digest>, the root
#                                index to deploy
#   E2E_EXPECTED_RUNTIME_DIGEST  its child for this machine's architecture
#   E2E_EXPECTED_VERSION         the Cargo version the image reports
#   E2E_EXPECTED_GIT_SHA         the 8-hex commit the image reports
#   E2E_TEST_CLIENT_IMAGE        a local Dockerfile.test `test-client` image for
#                                this architecture, by tag; it is loaded into the
#                                nodes instead of pulled
#   E2E_REGISTRY_CONFIG          optional absolute docker config.json with GHCR
#                                credentials: every kubelet reads it, and it
#                                becomes the Secret ghcr-pull the manifests name
#   LUMEN_KIND_KEEP=1            keep the cluster after the run
#
# The four E2E_EXPECTED_* values go to the Job verbatim; tests/it/e2e/identity.rs
# checks them.
#
# Requirements: kind, kubectl, docker.

set -euo pipefail

die() {
  echo "!! $*" >&2
  exit 1
}

SUITE="${1:-}"
case "$SUITE" in
  overlay | operator) ;;
  *) die "usage: scripts/kind-cluster.sh overlay|operator" ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="lumen-${SUITE}"
NAMESPACE=lumen
IMAGE_REPO=ghcr.io/faberline/lumen
for name in E2E_EXPECTED_IMAGE E2E_EXPECTED_RUNTIME_DIGEST E2E_EXPECTED_VERSION E2E_EXPECTED_GIT_SHA E2E_TEST_CLIENT_IMAGE; do
  [[ -n "${!name:-}" ]] || die "missing $name"
done
IMAGE="$E2E_EXPECTED_IMAGE"
CLIENT_IMAGE="$E2E_TEST_CLIENT_IMAGE"
REGISTRY_CONFIG="${E2E_REGISTRY_CONFIG:-}"
[[ "$IMAGE" == "${IMAGE_REPO}@sha256:"* ]] || die "E2E_EXPECTED_IMAGE must be ${IMAGE_REPO}@sha256:<digest>, got: $IMAGE"
if [[ -n "$REGISTRY_CONFIG" ]]; then
  [[ "$REGISTRY_CONFIG" == /* && -r "$REGISTRY_CONFIG" ]] || die "E2E_REGISTRY_CONFIG must be a readable absolute path"
fi

WORK="$(mktemp -d -t lumen-kind.XXXXXX)"
cleanup() {
  local status=$?
  if [[ "${LUMEN_KIND_KEEP:-}" != 1 ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT

say() {
  echo ">> [kind ${SUITE}] $*"
}

# One control plane, three pool-lumen-server workers, one pool-lumen-client
# worker. The overlay holds three server pods (Deployment lumen, Deployment
# lumen-operator, and the operator's lumen-op-0), and every pod of the
# namespace takes a node of its own (aw's one-per-node rule), so each needs its
# own node, and so does the test Job. The nodes carry the GKE pool labels but
# not the pool taints: kind's local-path provisioner would not tolerate them.
node() {
  printf '  - role: %s\n' "$1"
  if [[ -n "$2" ]]; then
    printf '    labels:\n      faberline.dev/pool: %s\n' "$2"
  fi
  if [[ -n "$REGISTRY_CONFIG" ]]; then
    printf '    extraMounts:\n      - containerPath: /var/lib/kubelet/config.json\n        hostPath: %s\n' "$REGISTRY_CONFIG"
  fi
}
{
  printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n'
  node control-plane ""
  node worker lumen-server
  node worker lumen-server
  node worker lumen-server
  node worker lumen-client
} >"$WORK/kind.yaml"

say "create cluster $CLUSTER"
kind create cluster --name "$CLUSTER" --wait 180s --config "$WORK/kind.yaml"
kubectl get nodes -L faberline.dev/pool
kind load docker-image "$CLIENT_IMAGE" --name "$CLUSTER"

kubectl create namespace "$NAMESPACE"
if [[ -n "$REGISTRY_CONFIG" ]]; then
  kubectl -n "$NAMESPACE" create secret generic ghcr-pull \
    --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson="$REGISTRY_CONFIG"
fi

# aw's run.sh runs `kustomize edit set image ghcr.io/faberline/lumen=<IMAGE>`
# on the overlay. Write the same digest into a copy, so the checkout is not
# edited and kustomize itself is not needed.
say "deploy k8s/overlays/gke-test with $IMAGE"
cp -R "$ROOT/k8s" "$WORK/k8s"
server="$WORK/k8s/overlays/gke-test/kustomization.yaml"
[[ "$(grep -c '^    newTag: REPLACE_ME$' "$server")" == 1 ]] || die "k8s/overlays/gke-test must hold one newTag: REPLACE_ME"
sed -i.orig "s|^    newTag: REPLACE_ME\$|    digest: ${IMAGE#*@}|" "$server"
# The overlay holds Lumen CRs, so their CRD must be served first.
kubectl apply -f "$ROOT/k8s/operator/crd.yaml"
kubectl wait --for=condition=established crd/lumens.lumen.dev --timeout=60s
kubectl apply -k "$WORK/k8s/overlays/gke-test"
for deploy in $(kubectl -n "$NAMESPACE" get deploy -o name); do
  kubectl -n "$NAMESPACE" rollout status "$deploy" --timeout=600s
done

kubectl -n "$NAMESPACE" create configmap gke-test-env \
  --from-literal=E2E_EXPECTED_IMAGE="$IMAGE" \
  --from-literal=E2E_EXPECTED_RUNTIME_DIGEST="$E2E_EXPECTED_RUNTIME_DIGEST" \
  --from-literal=E2E_EXPECTED_VERSION="$E2E_EXPECTED_VERSION" \
  --from-literal=E2E_EXPECTED_GIT_SHA="$E2E_EXPECTED_GIT_SHA"

say "run Job/lumen-e2e from k8s/gke-test-job/$SUITE with $CLIENT_IMAGE"
kubectl kustomize "$ROOT/k8s/gke-test-job/$SUITE" >"$WORK/job.yaml"
[[ "$(grep -c '^ *image: lumen-test-client$' "$WORK/job.yaml")" == 1 ]] || die "k8s/gke-test-job/$SUITE must name image lumen-test-client once"
sed -i.orig "s|image: lumen-test-client\$|image: ${CLIENT_IMAGE}|" "$WORK/job.yaml"
kubectl apply -f "$WORK/job.yaml"

# The Job's activeDeadlineSeconds (70m) fails it first; this only bounds a Job
# that never reports.
deadline=$((SECONDS + 4500))
state=""
while ((SECONDS < deadline)); do
  state="$(kubectl -n "$NAMESPACE" get job/lumen-e2e \
    -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type}{" "}{end}')"
  case " $state " in
    *" Complete "* | *" Failed "*) break ;;
  esac
  sleep 10
done

say "Job/lumen-e2e log"
kubectl -n "$NAMESPACE" logs job/lumen-e2e --tail=-1 || echo "!! no log from Job/lumen-e2e"
if [[ " $state " != *" Complete "* ]]; then
  kubectl -n "$NAMESPACE" get pods -o wide || true
  kubectl -n "$NAMESPACE" describe job/lumen-e2e || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp || true
fi
[[ " $state " == *" Complete "* ]] || die "Job/lumen-e2e did not complete (conditions: ${state:-none})"
say "Job/lumen-e2e completed"
