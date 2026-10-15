#!/usr/bin/env bash
# lumen — image identity check after the GKE e2e Job, on the runner.
#
# faberline/aw's reusable gke-test workflow deploys k8s/overlays/gke-test with
# IMAGE, runs one suite of k8s/gke-test-job (Job/lumen-e2e: the suite's deploy
# path, the 100k-doc fixture, search, duplicates, and the durable restart
# oracle, all from pool-lumen-client through the Services), and runs this
# script once that Job has completed. The Job cannot know which image it is testing, so this script
# proves the pods it exercised were that image:
#
#   - Deployment/lumen, Deployment/lumen-operator, Lumen/lumen-op, and the
#     operator's StatefulSet/lumen-op all ask for IMAGE;
#   - every serving and operator pod runs IMAGE's root index or its
#     linux/amd64 child digest, and sits on a pool-lumen-server node;
#   - each mode's /version answers in the Job's log, before and after the
#     restart, report EXPECTED_VERSION and EXPECTED_GIT_SHA;
#   - the Job's log ends each mode it ran with a pass result, and it ran
#     exactly the suite's mode.
#
# It reads the cluster and the Job's log only. No API traffic leaves the
# cluster, so nothing here uses a port-forward. Cluster-scoped objects the
# operator made for this namespace carry faberline.dev/owner-namespace, which
# aw's teardown and reaper delete.
#
# Inputs (from the aw workflow):
#   NAMESPACE      namespace the overlay and the Job were applied to
#   IMAGE          ghcr.io/faberline/lumen@sha256:<64 hex> root index digest
#   EVIDENCE_DIR   directory aw uploads; this script writes under verify/
#   RUN_LABEL      gha-<run_id>-<attempt>
#   KUBECONFIG     credentials for the shared cluster
# Expected identity (set by .github/workflows/gke-test.yml):
#   EXPECTED_VERSION          must equal Cargo.toml's version
#   EXPECTED_GIT_SHA          8 lowercase hex, as /version reports it
#   EXPECTED_RUNTIME_DIGEST   the linux/amd64 child digest of IMAGE
#   EXPECTED_SUITE            overlay or operator, the suite's mode
#
# Requirements: kubectl, jq.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LUMEN_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

die() {
  echo "!! $*" >&2
  exit 1
}

: "${NAMESPACE:?NAMESPACE is required}"
: "${IMAGE:?IMAGE is required}"
: "${EVIDENCE_DIR:?EVIDENCE_DIR is required}"
: "${RUN_LABEL:?RUN_LABEL is required}"
: "${KUBECONFIG:?KUBECONFIG is required}"

JOB_NAME="lumen-e2e"
LUMEN_CR_NAME="lumen-op"
OPERATOR_DEPLOYMENT="lumen-operator"
OPERATOR_LABEL="app.kubernetes.io/name=lumen-operator"
OVERLAY_LABEL="app=lumen,role=server"
OPERATOR_SERVING_LABEL="app.kubernetes.io/name=lumen,app.kubernetes.io/instance=${LUMEN_CR_NAME},app.kubernetes.io/component=server"
POOL_KEY="faberline.dev/pool"
POOL_VALUE="lumen-server"

[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "invalid NAMESPACE: $NAMESPACE"
[[ "$RUN_LABEL" =~ ^[A-Za-z0-9][-A-Za-z0-9]*$ ]] || die "invalid RUN_LABEL: $RUN_LABEL"
[[ "$IMAGE" =~ ^ghcr\.io/faberline/lumen@sha256:[0-9a-f]{64}$ ]] || die "invalid IMAGE: expected ghcr.io/faberline/lumen@sha256:<64 hex>"
cargo_ver="$(grep '^version = ' "$LUMEN_DIR/Cargo.toml" | head -n1 | cut -d'"' -f2)"
EXPECTED_VER="${EXPECTED_VERSION:-}"
[[ -n "$EXPECTED_VER" && "$EXPECTED_VER" == "$cargo_ver" ]] || die "EXPECTED_VERSION mismatch (expected $cargo_ver)"
EXPECTED_GIT_SHA="${EXPECTED_GIT_SHA:-}"
[[ "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{8}$ ]] || die "invalid EXPECTED_GIT_SHA: expected 8 lowercase hex"
EXPECTED_RUNTIME_DIGEST="${EXPECTED_RUNTIME_DIGEST:-}"
[[ "$EXPECTED_RUNTIME_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || die "invalid EXPECTED_RUNTIME_DIGEST: expected sha256:<64 hex>"
EXPECTED_SUITE="${EXPECTED_SUITE:-}"
[[ "$EXPECTED_SUITE" == "overlay" || "$EXPECTED_SUITE" == "operator" ]] || die "invalid EXPECTED_SUITE: expected overlay or operator"
ROOT_DIGEST="${IMAGE##*@}"
[[ "$EXPECTED_RUNTIME_DIGEST" != "$ROOT_DIGEST" ]] || die "runtime child digest must differ from root index digest"

VERIFY_DIR="${EVIDENCE_DIR}/verify"
mkdir -p "$VERIFY_DIR"
JOB_LOG="${VERIFY_DIR}/job.log"
EVIDENCE_JSON="${VERIFY_DIR}/job-evidence.json"
MODES='[]'

summary() {
  local ec=$?
  set +e
  [[ -s "$EVIDENCE_JSON" ]] || echo '[]' > "$EVIDENCE_JSON"
  jq -n --arg run_label "$RUN_LABEL" --arg namespace "$NAMESPACE" --arg image "$IMAGE" \
    --arg version "$EXPECTED_VER" --arg git_sha "$EXPECTED_GIT_SHA" \
    --arg runtime_digest "$EXPECTED_RUNTIME_DIGEST" --arg suite "$EXPECTED_SUITE" --argjson modes "$MODES" \
    --argjson exit_code "$ec" --slurpfile evidence "$EVIDENCE_JSON" \
    '{schema:"faberline.lumen.gke-test-verify.v3",run_label:$run_label,namespace:$namespace,image:$image,
      suite:$suite,expected:{version:$version,git_sha:$git_sha,runtime_digest:$runtime_digest},
      modes:$modes,job_evidence:$evidence[0],
      result:(if $exit_code == 0 then "pass" else "fail" end),exit_code:$exit_code}' \
    > "${VERIFY_DIR}/summary.json"
  exit "$ec"
}
trap summary EXIT

# ---------------------------------------------------------------------------
# The Job's own report
# ---------------------------------------------------------------------------

# k8s/gke-test-job/base/e2e.sh prints `LUMEN_E2E_EVIDENCE<TAB>mode<TAB>name<TAB>json`.
# Keep those lines as one JSON array and refuse a log that is not a complete
# pass.
read_job_evidence() {
  kubectl -n "$NAMESPACE" get job/"$JOB_NAME" -o json \
    | jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' >/dev/null \
    || die "Job/${JOB_NAME} has not completed"
  kubectl -n "$NAMESPACE" logs job/"$JOB_NAME" --tail=-1 > "$JOB_LOG"
  jq -R -s '
    split("\n") | map(select(startswith("LUMEN_E2E_EVIDENCE\t")) | split("\t")) |
    map(if length == 4 then {mode: .[1], name: .[2], data: (.[3] | fromjson)}
        else error("malformed evidence line") end)
  ' "$JOB_LOG" > "$EVIDENCE_JSON" || die "Job/${JOB_NAME} log holds a malformed evidence line"
  MODES="$(jq -c '[.[] | select(.mode == "setup" and .name == "result" and .data.result == "pass") | .data.modes[]] | unique' "$EVIDENCE_JSON")"
  jq -e 'length > 0' <<<"$MODES" >/dev/null || die "Job/${JOB_NAME} log has no final pass result"
  jq -e --arg suite "$EXPECTED_SUITE" '. == [$suite]' <<<"$MODES" >/dev/null \
    || die "Job/${JOB_NAME} ran modes $(jq -c . <<<"$MODES"), want only the suite ${EXPECTED_SUITE}"
  local mode
  for mode in $(jq -r '.[]' <<<"$MODES"); do
    jq -e --arg mode "$mode" 'any(.[]; .mode == $mode and .name == "result" and .data.result == "pass")' "$EVIDENCE_JSON" >/dev/null \
      || die "Job/${JOB_NAME} log has no pass result for mode $mode"
  done
  echo "   Job/${JOB_NAME} completed and passed: $(jq -r 'join(" ")' <<<"$MODES")"
}

# Every /version the Job saw, before and after each restart.
assert_version_identity() {
  local mode
  for mode in $(jq -r '.[]' <<<"$MODES"); do
    jq -e --arg mode "$mode" --arg version "$EXPECTED_VER" --arg sha "$EXPECTED_GIT_SHA" '
      [.[] | select(.mode == $mode and (.name | startswith("version-")))] as $v |
      ($v | map(.name) | sort) == ["version-after-restart", "version-before-restart"] and
      all($v[]; .data.version == $version and .data.git_sha == $sha)
    ' "$EVIDENCE_JSON" >/dev/null \
      || die "$mode: /version before and after restart must report version=$EXPECTED_VER git_sha=$EXPECTED_GIT_SHA"
  done
  echo "   /version reported version=$EXPECTED_VER git_sha=$EXPECTED_GIT_SHA in every mode, before and after restart"
}

# ---------------------------------------------------------------------------
# Desired and running images
# ---------------------------------------------------------------------------

normalize_runtime_image_id() {
  local raw="$1"
  if [[ "$raw" =~ ^(docker-pullable://)?ghcr\.io/faberline/lumen@(sha256:[0-9a-f]{64})$ ]]; then
    echo "${BASH_REMATCH[2]}"
  elif [[ "$raw" =~ ^(containerd|cri-o|docker)://(sha256:[0-9a-f]{64})$ ]]; then
    echo "${BASH_REMATCH[2]}"
  else
    return 1
  fi
}

# assert_template_image <kind/name> <container>: the workload asks for IMAGE.
assert_template_image() {
  local ref="$1" container="$2"
  kubectl -n "$NAMESPACE" get "$ref" -o json \
    | jq -e --arg name "$container" --arg image "$IMAGE" '
        [.spec.template.spec.containers[] | select(.name == $name)] as $c |
        ($c | length) == 1 and $c[0].image == $image
      ' >/dev/null || die "$ref must run one $container container at $IMAGE"
}

# assert_pods <label> <container> <replicas>: the pods run IMAGE on pool-lumen-server.
assert_pods() {
  local label="$1" container="$2" desired="$3" pods runtime_id normalized node
  pods="$(kubectl -n "$NAMESPACE" get pods -l "$label" -o json)"
  jq -e --arg name "$container" --arg image "$IMAGE" --argjson desired "$desired" '
    (.items | length) == $desired and
    all(.items[];
      ([.spec.containers[] | select(.name == $name and .image == $image)] | length) == 1 and
      ([(.status.containerStatuses // [])[] | select(.name == $name and .ready == true and (.imageID // "") != "")] | length) == 1)
  ' <<<"$pods" >/dev/null || die "$label: want $desired Ready pod(s) with one $container container at $IMAGE"
  while IFS= read -r runtime_id; do
    normalized="$(normalize_runtime_image_id "$runtime_id")" || die "unrecognized $container runtime imageID: $runtime_id"
    [[ "$normalized" == "$ROOT_DIGEST" || "$normalized" == "$EXPECTED_RUNTIME_DIGEST" ]] \
      || die "$container runtime imageID $runtime_id is neither the root digest $ROOT_DIGEST nor the platform child digest $EXPECTED_RUNTIME_DIGEST"
  done < <(jq -r --arg name "$container" '.items[].status.containerStatuses[] | select(.name == $name) | .imageID' <<<"$pods")
  while IFS= read -r node; do
    [[ "$(kubectl get node "$node" -o json | jq -r --arg key "$POOL_KEY" '.metadata.labels[$key] // ""')" == "$POOL_VALUE" ]] \
      || die "$container pod runs on node $node outside pool-${POOL_VALUE}"
  done < <(jq -r '.items[].spec.nodeName' <<<"$pods")
  jq '[.items[] | {name:.metadata.name,node:.spec.nodeName,images:[.status.containerStatuses[]? | {name,image,imageID}]}]' \
    <<<"$pods" > "${VERIFY_DIR}/pods-${container}.json"
}

replicas_of() {
  kubectl -n "$NAMESPACE" get "$1" -o json \
    | jq -er '.spec.replicas | select(type == "number" and . > 0 and . == floor)' \
    || die "$1 replicas must be a positive integer"
}

assert_overlay_identity() {
  local replicas
  replicas="$(replicas_of deploy/lumen)"
  assert_template_image deploy/lumen lumen
  assert_pods "$OVERLAY_LABEL" lumen "$replicas"
  echo "   overlay: Deployment/lumen and its pods run $IMAGE on pool-${POOL_VALUE}"
}

assert_operator_identity() {
  local cr_img replicas
  replicas="$(replicas_of deploy/"$OPERATOR_DEPLOYMENT")"
  assert_template_image deploy/"$OPERATOR_DEPLOYMENT" operator
  assert_pods "$OPERATOR_LABEL" operator "$replicas"
  cr_img="$(kubectl -n "$NAMESPACE" get lumen/"${LUMEN_CR_NAME}" -o jsonpath='{.spec.image}')"
  [[ "$cr_img" == "$IMAGE" ]] || die "Lumen CR spec.image $cr_img != $IMAGE"
  replicas="$(replicas_of statefulset/"$LUMEN_CR_NAME")"
  assert_template_image statefulset/"$LUMEN_CR_NAME" server
  assert_pods "$OPERATOR_SERVING_LABEL" server "$replicas"
  echo "   operator: the operator, Lumen/${LUMEN_CR_NAME}, and its StatefulSet run $IMAGE on pool-${POOL_VALUE}"
}

echo ">> 1. read Job/${JOB_NAME}'s report"
read_job_evidence
echo ">> 2. /version identity"
assert_version_identity
echo ">> 3. desired and running images"
for mode in $(jq -r '.[]' <<<"$MODES"); do
  case "$mode" in
    overlay) assert_overlay_identity ;;
    operator) assert_operator_identity ;;
    *) die "unknown mode in the Job's report: $mode" ;;
  esac
done
echo ">> gke-test identity PASS: version=$EXPECTED_VER git_sha=$EXPECTED_GIT_SHA runtime_digest=$EXPECTED_RUNTIME_DIGEST"
