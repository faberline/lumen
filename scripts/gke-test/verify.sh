#!/usr/bin/env bash
# lumen — end-to-end check on Faberline's shared GKE test cluster.
#
# faberline/aw's reusable gke-test workflow renders k8s/overlays/gke-test with
# IMAGE, applies it to NAMESPACE on pool-lumen, waits for the rollouts it can
# see, and then runs this script. It runs kind-e2e.sh's checks for both deploy
# paths against that one namespace, in order:
#
#   overlay   the direct-install Deployment `lumen` (labels app=lumen,role=server)
#   operator  the Lumen CR `lumen-op`, reconciled by the shipped operator, plus
#             `lumen-op-legacy-missing-catalog`, which must fail on the missing
#             capacity catalog
#
# For each path: identity of every pod and of /version, PUT a collection, index
# the 100k-doc fixture, search, duplicates, then the durable restart oracle
# (checkpoint one document, replace the serving pod, read it back before any
# new write, and prove the replacement accepts a write).
#
# The API is reached with `kubectl port-forward` to one Ready serving pod. A
# forward can stall on successive large POSTs (kind-e2e.sh's header says why it
# avoids one), so every request restarts the forward and retries after a
# transport failure. An HTTP error status is never retried.
#
# Inputs (from the aw workflow):
#   NAMESPACE      namespace the overlay was applied to (the overlay pins lumen)
#   IMAGE          ghcr.io/faberline/lumen@sha256:<64 hex> root index digest
#   EVIDENCE_DIR   directory aw uploads; this script writes under verify/
#   RUN_LABEL      gha-<run_id>-<attempt>, used in the oracle document ids
#   KUBECONFIG     credentials for the shared cluster
# Expected identity (set by .github/workflows/gke-test.yml):
#   EXPECTED_VERSION          must equal Cargo.toml's version
#   EXPECTED_GIT_SHA          8 lowercase hex, as /version reports it
#   EXPECTED_RUNTIME_DIGEST   the linux/amd64 child digest of IMAGE
# Optional:
#   LUMEN_GKE_DOC_COUNT       fixture size (default 100000)
#   LUMEN_GKE_MODES           "overlay operator" (default) or one of them
#   LUMEN_GKE_LOCAL_PORT      local port for the forward (default 17373)
#
# Requirements: kubectl, curl, jq, python3.

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

LUMEN_CR_NAME="lumen-op"
LEGACY_CR_NAME="lumen-op-legacy-missing-catalog"
OPERATOR_DEPLOYMENT="lumen-operator"
OPERATOR_LABEL="app.kubernetes.io/name=lumen-operator"
POOL_KEY="faberline.dev/pool"
POOL_VALUE="lumen"
PULL_SECRET="ghcr-pull"
EXPECTED_STORAGE_PODS=1
PORT_REMOTE=7373
PORT_LOCAL="${LUMEN_GKE_LOCAL_PORT:-17373}"
DOC_COUNT="${LUMEN_GKE_DOC_COUNT:-100000}"
BATCH_SIZE=1000
MODES="${LUMEN_GKE_MODES:-overlay operator}"
IMAGE_TAG="$IMAGE"

[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "invalid NAMESPACE: $NAMESPACE"
[[ "$RUN_LABEL" =~ ^[A-Za-z0-9][-A-Za-z0-9]*$ ]] || die "invalid RUN_LABEL: $RUN_LABEL"
[[ ! "$IMAGE_TAG" =~ [[:space:][:cntrl:]] ]] || die "IMAGE contains whitespace or control characters"
[[ "$IMAGE_TAG" =~ ^ghcr\.io/faberline/lumen@sha256:[0-9a-f]{64}$ ]] || die "invalid IMAGE: expected ghcr.io/faberline/lumen@sha256:<64 hex>"
cargo_ver="$(grep '^version = ' "$LUMEN_DIR/Cargo.toml" | head -n1 | cut -d'"' -f2)"
EXPECTED_VER="${EXPECTED_VERSION:-}"
[[ -n "$EXPECTED_VER" && "$EXPECTED_VER" == "$cargo_ver" ]] || die "EXPECTED_VERSION mismatch (expected $cargo_ver)"
EXPECTED_GIT_SHA="${EXPECTED_GIT_SHA:-}"
[[ "$EXPECTED_GIT_SHA" =~ ^[0-9a-f]{8}$ ]] || die "invalid EXPECTED_GIT_SHA: expected 8 lowercase hex"
EXPECTED_RUNTIME_DIGEST="${EXPECTED_RUNTIME_DIGEST:-}"
[[ "$EXPECTED_RUNTIME_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || die "invalid EXPECTED_RUNTIME_DIGEST: expected sha256:<64 hex>"
ROOT_DIGEST="${IMAGE_TAG##*@}"
[[ "$EXPECTED_RUNTIME_DIGEST" != "$ROOT_DIGEST" ]] || die "runtime child digest must differ from root index digest"
[[ "$DOC_COUNT" =~ ^[1-9][0-9]*$ ]] || die "invalid LUMEN_GKE_DOC_COUNT: $DOC_COUNT"
for mode in $MODES; do
  [[ "$mode" == "overlay" || "$mode" == "operator" ]] || die "invalid LUMEN_GKE_MODES entry: $mode"
done

VERIFY_DIR="${EVIDENCE_DIR}/verify"
mkdir -p "$VERIFY_DIR"
TIMINGS="${VERIFY_DIR}/timings.tsv"
printf 'mode\tstep\tseconds\n' > "$TIMINGS"

E2E_MODE=""
APP_LABEL=""
MODE_DIR=""
PF_PID_FILE="$(mktemp -t lumen-gke-pf.XXXXXX)"
PF_LOG="${VERIFY_DIR}/port-forward.log"
FIXTURE_FILE=""
INDEX_BODIES=()
RESULTS='[]'

# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

step() {
  local label="$1"
  shift
  local start end
  start=$(date +%s)
  echo ">> [${E2E_MODE:-setup}] $label"
  "$@"
  end=$(date +%s)
  echo "   ($label finished in $((end - start))s)"
  printf '%s\t%s\t%s\n' "${E2E_MODE:-setup}" "$label" "$((end - start))" >> "$TIMINGS"
}

# ---------------------------------------------------------------------------
# Cleanup on exit
# ---------------------------------------------------------------------------

# The operator binds `lumen.<namespace>.<instance>.auth-delegator`, a
# ClusterRoleBinding, for every Lumen it reconciles. aw's teardown removes the
# namespace and the CRDs but not that binding, so it would outlive this run on
# the shared cluster. Delete the CRs first (nothing recreates the binding once
# the operator has no Lumen to reconcile), stop the operator, then delete the
# bindings this namespace owns.
release_cluster_scoped() {
  kubectl -n "$NAMESPACE" delete lumens.lumen.dev --all --ignore-not-found \
    --wait=true --timeout=180s || true
  kubectl -n "$NAMESPACE" scale deploy/"$OPERATOR_DEPLOYMENT" --replicas=0 || true
  kubectl -n "$NAMESPACE" wait --for=delete pod -l "$OPERATOR_LABEL" --timeout=120s || true
  kubectl delete clusterrolebinding --ignore-not-found \
    -l "app.kubernetes.io/component=auth-delegation,lumen.dev/owner-namespace=${NAMESPACE}" || true
}

cleanup() {
  local ec=$?
  set +e
  pf_stop
  rm -f "$PF_PID_FILE"
  if [[ -n "$FIXTURE_FILE" ]]; then
    rm -f "$FIXTURE_FILE" "${FIXTURE_FILE%.json}".req.*.json
  fi
  if [[ "$ec" -ne 0 ]]; then
    {
      kubectl -n "$NAMESPACE" get lumens.lumen.dev -o yaml
      kubectl -n "$NAMESPACE" get statefulsets,deployments,pods,pvc -o wide
    } > "${VERIFY_DIR}/failure-state.txt" 2>&1
    kubectl -n "$NAMESPACE" logs deploy/"$OPERATOR_DEPLOYMENT" --tail=400 \
      > "${VERIFY_DIR}/failure-operator.log" 2>&1
  fi
  release_cluster_scoped > "${VERIFY_DIR}/release-cluster-scoped.log" 2>&1
  jq -n --arg run_label "$RUN_LABEL" --arg namespace "$NAMESPACE" --arg image "$IMAGE_TAG" \
    --arg version "$EXPECTED_VER" --arg git_sha "$EXPECTED_GIT_SHA" \
    --arg runtime_digest "$EXPECTED_RUNTIME_DIGEST" --argjson doc_count "$DOC_COUNT" \
    --argjson exit_code "$ec" --argjson modes "$RESULTS" \
    '{schema:"faberline.lumen.gke-test-verify.v1",run_label:$run_label,namespace:$namespace,image:$image,
      expected:{version:$version,git_sha:$git_sha,runtime_digest:$runtime_digest},
      doc_count:$doc_count,modes:$modes,result:(if $exit_code == 0 then "pass" else "fail" end),exit_code:$exit_code}' \
    > "${VERIFY_DIR}/summary.json"
  exit "$ec"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Mode selection
# ---------------------------------------------------------------------------

enter_mode() {
  E2E_MODE="$1"
  if [[ "$E2E_MODE" == "operator" ]]; then
    # The instance label keeps the legacy CR and the overlay pod out of it.
    APP_LABEL="app.kubernetes.io/name=lumen,app.kubernetes.io/instance=${LUMEN_CR_NAME},app.kubernetes.io/component=server"
  else
    APP_LABEL="app=lumen,role=server"
  fi
  MODE_DIR="${VERIFY_DIR}/${E2E_MODE}"
  mkdir -p "$MODE_DIR"
  PF_LOG="${MODE_DIR}/port-forward.log"
}

# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------

# Poll until enough pods match the label, so `kubectl wait` does not fail with
# "no matching resources" before the controller has created them.
wait_pods_exist() {
  local label="$1" expected="${2:-1}" timeout="${3:-60}" deadline
  deadline=$(( $(date +%s) + timeout ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if [[ "$(kubectl -n "$NAMESPACE" get pod -l "$label" --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  kubectl -n "$NAMESPACE" get pod -l "$label" >&2 || true
  die "expected at least ${expected} pod(s) for ${label} within ${timeout}s"
}

wait_lumen_ready() {
  local timeout="${1:-180}"
  echo "   waiting up to ${timeout}s for serving pods ($APP_LABEL) Ready"
  wait_pods_exist "$APP_LABEL" "$EXPECTED_STORAGE_PODS" "$timeout"
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l "$APP_LABEL" \
    --timeout="${timeout}s"
}

ready_pod() {
  kubectl -n "$NAMESPACE" get pods -l "$APP_LABEL" -o json | jq -r '
    [.items[]
      | select(.metadata.deletionTimestamp == null)
      | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
    ][0].metadata.name // empty'
}

# ---------------------------------------------------------------------------
# Port-forward
# ---------------------------------------------------------------------------

# The forward's pid lives in a file, not a variable: `api` restarts the forward
# from inside `$(...)` subshells, and the next request must still find it.
pf_stop() {
  local pid
  pid="$(cat "$PF_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
  fi
  : > "$PF_PID_FILE"
}

# Forward PORT_LOCAL to one Ready serving pod and wait for /healthz. Pinning a
# pod (not the Service) keeps create/index/search on one storage owner.
pf_start() {
  local pod pid attempt
  for attempt in 1 2 3 4 5; do
    pf_stop
    pod="$(ready_pod)"
    if [[ -z "$pod" ]]; then
      echo "   no Ready pod for $APP_LABEL yet (attempt $attempt)" >&2
      sleep 5
      continue
    fi
    echo "   port-forward pod/${pod} ${PORT_LOCAL}:${PORT_REMOTE} (attempt $attempt)" >&2
    kubectl -n "$NAMESPACE" port-forward --address 127.0.0.1 "pod/${pod}" \
      "${PORT_LOCAL}:${PORT_REMOTE}" >> "$PF_LOG" 2>&1 < /dev/null &
    pid=$!
    echo "$pid" > "$PF_PID_FILE"
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      if curl -fsS --max-time 5 "$(base_url)/healthz" >/dev/null 2>&1; then
        return 0
      fi
      sleep 1
    done
    sleep 2
  done
  pf_stop
  die "lumen API never became reachable through port-forward on :${PORT_LOCAL}"
}

base_url() {
  echo "http://127.0.0.1:${PORT_LOCAL}"
}

# curl through the forward. A transport failure (connection refused or reset,
# empty reply, timeout) restarts the forward and repeats the request; curl's
# exit 22 is an HTTP error status from lumen and fails at once. Every request
# made through here is safe to repeat: index items carry external ids.
api() {
  local attempt rc out
  for attempt in 1 2 3 4; do
    rc=0
    out="$(curl -fsS --connect-timeout 10 "$@")" || rc=$?
    if [[ "$rc" -eq 0 ]]; then
      printf '%s\n' "$out"
      return 0
    fi
    if [[ "$rc" -eq 22 ]]; then
      echo "!! HTTP error status from: curl $*" >&2
      return "$rc"
    fi
    echo "   transport failure (curl exit $rc, attempt $attempt); restarting port-forward" >&2
    pf_start
  done
  echo "!! request still failing after $attempt attempts: curl $*" >&2
  return "$rc"
}

# ---------------------------------------------------------------------------
# API calls (same requests as kind-e2e.sh)
# ---------------------------------------------------------------------------

api_put_collection() {
  api --max-time 30 -X PUT "$(base_url)/collections/users" \
    -H 'content-type: application/json' \
    -d '{"fields": {"bio": {"type": "text"}, "email": {"type": "keyword"}}}'
}

api_index_batch() {
  local body_file="$1"
  api --max-time 120 -X POST "$(base_url)/collections/users/index" \
    -H 'content-type: application/json' \
    --data-binary "@${body_file}"
}

api_search() {
  api --max-time 30 -X POST "$(base_url)/collections/users/search" \
    -H 'content-type: application/json' \
    -d '{"query": {"match": {"field": "bio", "text": "engineer"}}, "limit": 20}'
}

api_duplicates() {
  api --max-time 30 -X POST "$(base_url)/collections/users/duplicates" \
    -H 'content-type: application/json' \
    -d '{"field": "email", "min_group_size": 2, "limit": 100}'
}

api_index_exact() {
  local external_id="$1" value="$2"
  local body
  body="$(jq -nc --arg id "$external_id" --arg value "$value" \
    '{items:[{external_id:$id,field:"email",value:$value}]}')"
  api --max-time 30 -X POST "$(base_url)/collections/users/index" \
    -H 'content-type: application/json' -d "$body"
}

api_search_exact() {
  local value="$1"
  local body
  body="$(jq -nc --arg value "$value" '{query:{term:{field:"email",value:$value}},limit:5}')"
  api --max-time 30 -X POST "$(base_url)/collections/users/search" \
    -H 'content-type: application/json' -d "$body"
}

api_checkpoint() {
  api --max-time 120 -X POST "$(base_url)/admin/checkpoint" \
    -H 'content-type: application/json' -d '{}'
}

# ---------------------------------------------------------------------------
# Image identity
# ---------------------------------------------------------------------------

normalize_runtime_image_id() {
  local raw="$1"
  if [[ "$raw" =~ ^ghcr\.io/faberline/lumen@(sha256:[0-9a-f]{64})$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "$raw" =~ ^docker-pullable://ghcr\.io/faberline/lumen@(sha256:[0-9a-f]{64})$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "$raw" =~ ^(containerd|cri-o|docker)://(sha256:[0-9a-f]{64})$ ]]; then
    echo "${BASH_REMATCH[2]}"
  else
    return 1
  fi
}

runtime_digest_is_expected() {
  local digest="$1"
  [[ "$digest" == "$ROOT_DIGEST" || "$digest" == "$EXPECTED_RUNTIME_DIGEST" ]]
}

assert_named_pods() {
  local label="$1" container="$2" desired="$3"
  local pods pod_count named_specs desired_specs named_statuses image_ids runtime_id normalized
  pods="$(kubectl -n "$NAMESPACE" get pods -l "$label" -o json)"
  pod_count="$(jq -er '.items | length' <<<"$pods")"
  named_specs="$(jq --arg name "$container" '[.items[].spec.containers[] | select(.name == $name)] | length' <<<"$pods")"
  desired_specs="$(jq --arg name "$container" --arg image "$IMAGE_TAG" '[.items[].spec.containers[] | select(.name == $name and .image == $image)] | length' <<<"$pods")"
  named_statuses="$(jq --arg name "$container" '[.items[] | (.status.containerStatuses // [])[] | select(.name == $name)] | length' <<<"$pods")"
  image_ids="$(jq --arg name "$container" '[.items[] | (.status.containerStatuses // [])[] | select(.name == $name and (.imageID | type) == "string" and (.imageID | length) > 0)] | length' <<<"$pods")"
  [[ "$pod_count" -eq "$desired" ]] || die "$container pod count $pod_count != desired replicas $desired"
  [[ "$named_specs" -eq "$desired" && "$desired_specs" -eq "$desired" ]] || die "$container desired-image container count mismatch"
  [[ "$named_statuses" -eq "$desired" && "$image_ids" -eq "$desired" ]] || die "$container runtime imageID count mismatch"
  while IFS= read -r runtime_id; do
    normalized="$(normalize_runtime_image_id "$runtime_id")" || die "unrecognized $container runtime imageID: $runtime_id"
    runtime_digest_is_expected "$normalized" || \
      die "$container runtime imageID $runtime_id is neither the pinned root digest $ROOT_DIGEST nor the expected platform child digest $EXPECTED_RUNTIME_DIGEST"
  done < <(jq -r --arg name "$container" '.items[] | .status.containerStatuses[] | select(.name == $name) | .imageID' <<<"$pods")
  jq '[.items[] | {name:.metadata.name,node:.spec.nodeName,images:[.status.containerStatuses[]? | {name,image,imageID}]}]' \
    <<<"$pods" > "${MODE_DIR}/pods-${container}.json"
}

assert_cluster_identity() {
  echo "   verifying desired state and runtime image identities"
  if [[ "$E2E_MODE" == "overlay" ]]; then
    assert_overlay_identity
  else
    assert_operator_identity
  fi
  assert_version_identity
}

assert_overlay_identity() {
  local deploy_json deploy_replicas deploy_named deploy_image
  deploy_json="$(kubectl -n "$NAMESPACE" get deploy/lumen -o json)"
  deploy_replicas="$(jq -er '.spec.replicas | select(type == "number" and . > 0 and . == floor)' <<<"$deploy_json")" || die "serving Deployment replicas must be a positive integer"
  deploy_named="$(jq '[.spec.template.spec.containers[] | select(.name == "lumen")] | length' <<<"$deploy_json")"
  deploy_image="$(jq --arg image "$IMAGE_TAG" '[.spec.template.spec.containers[] | select(.name == "lumen" and .image == $image)] | length' <<<"$deploy_json")"
  [[ "$deploy_named" -eq 1 && "$deploy_image" -eq 1 ]] || die "serving Deployment template must contain one named lumen container at $IMAGE_TAG"
  assert_named_pods "$APP_LABEL" lumen "$deploy_replicas"
}

assert_operator_identity() {
  local op_json op_replicas op_named op_image cr_img sset_json sset_replicas sset_named sset_image
  op_json="$(kubectl -n "$NAMESPACE" get deploy/"$OPERATOR_DEPLOYMENT" -o json)"
  op_replicas="$(jq -er '.spec.replicas | select(type == "number" and . > 0 and . == floor)' <<<"$op_json")" || die "operator replicas must be a positive integer"
  op_named="$(jq '[.spec.template.spec.containers[] | select(.name == "operator")] | length' <<<"$op_json")"
  op_image="$(jq --arg image "$IMAGE_TAG" '[.spec.template.spec.containers[] | select(.name == "operator" and .image == $image)] | length' <<<"$op_json")"
  [[ "$op_named" -eq 1 && "$op_image" -eq 1 ]] || die "operator Deployment template must contain one named operator container at $IMAGE_TAG"
  assert_named_pods "$OPERATOR_LABEL" operator "$op_replicas"
  cr_img="$(kubectl -n "$NAMESPACE" get lumen/"${LUMEN_CR_NAME}" -o jsonpath='{.spec.image}')"
  [[ "$cr_img" == "$IMAGE_TAG" ]] || die "Lumen CR spec.image $cr_img != $IMAGE_TAG"
  sset_json="$(kubectl -n "$NAMESPACE" get statefulset/"${LUMEN_CR_NAME}" -o json)"
  sset_replicas="$(jq -er '.spec.replicas | select(type == "number" and . > 0 and . == floor)' <<<"$sset_json")" || die "serving replicas must be a positive integer"
  sset_named="$(jq '[.spec.template.spec.containers[] | select(.name == "server")] | length' <<<"$sset_json")"
  sset_image="$(jq --arg image "$IMAGE_TAG" '[.spec.template.spec.containers[] | select(.name == "server" and .image == $image)] | length' <<<"$sset_json")"
  [[ "$sset_named" -eq 1 && "$sset_image" -eq 1 ]] || die "StatefulSet template must contain one named server container at $IMAGE_TAG"
  assert_named_pods "$APP_LABEL" server "$sset_replicas"
}

assert_version_identity() {
  local ver_json
  ver_json="$(api --max-time 10 "$(base_url)/version")"
  printf '%s\n' "$ver_json" > "${MODE_DIR}/version.json"
  jq -e --arg version "$EXPECTED_VER" --arg sha "$EXPECTED_GIT_SHA" '
    (.version | type) == "string" and (.git_sha | type) == "string" and
    .version != "unknown" and .git_sha != "unknown" and
    .version == $version and .git_sha == $sha
  ' <<<"$ver_json" >/dev/null || die "/version identity is missing, non-string, unknown, or mismatched"
  echo "   identity verified: version=$EXPECTED_VER git_sha=$EXPECTED_GIT_SHA runtime_digest=$EXPECTED_RUNTIME_DIGEST"
}

# ---------------------------------------------------------------------------
# Overlay deploy path
# ---------------------------------------------------------------------------

# The overlay's Deployment carries the dev overlay's shape plus a data PVC at
# the image's LUMEN_DATA_DIR. Check that the render reached the cluster as
# written, since nothing here patches it after the fact.
assert_overlay_shape() {
  local deploy_json
  deploy_json="$(kubectl -n "$NAMESPACE" get deploy/lumen -o json)"
  printf '%s\n' "$deploy_json" > "${MODE_DIR}/deployment.json"
  jq -e '
    ([.spec.template.spec.containers[] | select(.name == "lumen") | .env[] | select(.name == "LUMEN_WAL" and .value == "embedded")] | length) == 1 and
    ([.spec.template.spec.containers[] | select(.name == "lumen") | .volumeMounts[] | select(.name == "data" and .mountPath == "/var/lib/lumen/data")] | length) == 1 and
    ([.spec.template.spec.volumes[] | select(.name == "data" and .persistentVolumeClaim.claimName == "lumen-data")] | length) == 1
  ' <<<"$deploy_json" >/dev/null || die "overlay Deployment lost LUMEN_WAL=embedded or the lumen-data mount"
  assert_pool_placement "Deployment/lumen" "$deploy_json"
  kubectl -n "$NAMESPACE" get pvc/lumen-data -o json | jq -e '.status.phase == "Bound"' >/dev/null \
    || die "lumen-data PVC is not bound"
  echo "   overlay Deployment runs LUMEN_WAL=embedded on the bound lumen-data PVC in pool-lumen"
}

# A workload's pod template must schedule onto pool-lumen and pull from GHCR.
assert_pool_placement() {
  local what="$1" workload_json="$2"
  jq -e --arg key "$POOL_KEY" --arg value "$POOL_VALUE" --arg secret "$PULL_SECRET" '
    .spec.template.spec as $s |
    ($s.nodeSelector[$key] // "") == $value and
    any($s.tolerations[]?; .key == $key and .operator == "Equal" and .value == $value and .effect == "NoSchedule") and
    any($s.imagePullSecrets[]?; .name == $secret)
  ' <<<"$workload_json" >/dev/null || die "$what is missing the ${POOL_KEY}=${POOL_VALUE} selector, toleration, or ${PULL_SECRET} pull secret"
}

assert_pods_on_pool() {
  local pods node
  pods="$(kubectl -n "$NAMESPACE" get pods -l "$APP_LABEL" -o json)"
  while IFS= read -r node; do
    [[ -n "$node" ]] || die "serving pod is not scheduled"
    [[ "$(kubectl get node "$node" -o json | jq -r --arg key "$POOL_KEY" '.metadata.labels[$key] // ""')" == "$POOL_VALUE" ]] \
      || die "serving pod runs on node $node outside pool-${POOL_VALUE}"
  done < <(jq -r '.items[].spec.nodeName // ""' <<<"$pods")
  echo "   serving pods run on pool-${POOL_VALUE} nodes"
}

# ---------------------------------------------------------------------------
# Operator deploy path
# ---------------------------------------------------------------------------

wait_operator_statefulset() {
  kubectl wait --for=condition=established crd/lumens.lumen.dev --timeout=60s
  kubectl -n "$NAMESPACE" rollout status deploy/"$OPERATOR_DEPLOYMENT" --timeout=300s
  local deadline=$(( $(date +%s) + 180 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if kubectl -n "$NAMESPACE" get statefulset/"${LUMEN_CR_NAME}" >/dev/null 2>&1; then
      echo "   operator reconciled StatefulSet/${LUMEN_CR_NAME}"
      return 0
    fi
    sleep 2
  done
  kubectl -n "$NAMESPACE" logs deploy/"$OPERATOR_DEPLOYMENT" --tail=60 >&2 || true
  die "operator did not render StatefulSet/${LUMEN_CR_NAME} within 180s"
}

# Native placement: the CR's exact selector, toleration, and pull secret reach
# the StatefulSet without a capacity catalog.
assert_native_placement() {
  local sset_json
  sset_json="$(kubectl -n "$NAMESPACE" get statefulset/"${LUMEN_CR_NAME}" -o json)"
  printf '%s\n' "$sset_json" > "${MODE_DIR}/statefulset.json"
  assert_pool_placement "StatefulSet/${LUMEN_CR_NAME}" "$sset_json"
  if kubectl -n "$NAMESPACE" get configmap/lumen-capacity-catalog >/dev/null 2>&1; then
    die "native placement unexpectedly depends on a fake capacity catalog"
  fi
  echo "   native placement preserved ${POOL_KEY}=${POOL_VALUE} and ${PULL_SECRET} without a capacity catalog"
}

assert_legacy_missing_catalog_event() {
  local deadline=$(( $(date +%s) + 120 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local message
    message="$(kubectl -n "$NAMESPACE" get events \
      --field-selector "involvedObject.name=${LEGACY_CR_NAME},reason=ReconcileFailed" \
      -o json | jq -r '[.items[] | (.message // .note // "")] | join("\n")')"
    if [[ "$message" == *"capacity catalog"* ]]; then
      printf '%s\n' "$message" > "${MODE_DIR}/legacy-reconcile-failed.txt"
      echo "   legacy empty-selector failure Event names missing capacity catalog"
      return 0
    fi
    sleep 2
  done
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp >&2 || true
  die "legacy empty-selector CR did not publish a namespaced ReconcileFailed Event naming the capacity catalog"
}

assert_operator_topology() {
  local stateful_json actual_replicas
  stateful_json="$(kubectl -n "$NAMESPACE" get statefulset/"${LUMEN_CR_NAME}" -o json)"
  actual_replicas="$(jq '.spec.replicas' <<<"$stateful_json")"
  echo "   StatefulSet replicas: ${actual_replicas} (expected ${EXPECTED_STORAGE_PODS})"
  [[ "$actual_replicas" -eq "$EXPECTED_STORAGE_PODS" ]] \
    || die "expected StatefulSet/${LUMEN_CR_NAME} replicas=${EXPECTED_STORAGE_PODS}, got ${actual_replicas}"
  jq -e '
    (.spec.volumeClaimTemplates | length) == 1 and
    .spec.volumeClaimTemplates[0].metadata.name == "raft" and
    ([.spec.template.spec.containers[] | select(.name == "server")] | length) == 1
  ' <<<"$stateful_json" >/dev/null \
    || die "operator StatefulSet must render exactly one raft PVC and one server container"
  jq -e '
    ([.spec.template.spec.containers[] | select(.name == "server") | .volumeMounts[] | select(.name == "raft")] |
      map(if has("readOnly") then . else . + {"readOnly": false} end)) == [
      {"mountPath":"/var/lib/lumen","name":"raft","readOnly":false},
      {"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":false,"subPath":"data"}
    ] and
    ([.spec.template.spec.containers[] | select(.name == "server") | .env[] | select(.name == "LUMEN_DATA_DIR" and .value == "/var/lib/lumen/data")] | length) == 1 and
    ([.spec.template.spec.containers[] | select(.name == "server") | .env[] | select(.name == "LUMEN_PERSISTENCE" and .value == "segment")] | length) == 1
  ' <<<"$stateful_json" >/dev/null \
    || die "single-replica operator StatefulSet lost the exact raft parent/data child mount contract"
  local cm_json cm_shards cm_bucket_count cm_map_version
  cm_json="$(kubectl -n "$NAMESPACE" get cm/"${LUMEN_CR_NAME}-config" -o json)"
  cm_shards="$(jq -r '.data.SHARD_COUNT' <<<"$cm_json")"
  cm_bucket_count="$(jq -r '.data.VIRTUAL_BUCKET_COUNT' <<<"$cm_json")"
  cm_map_version="$(jq -r '.data.SHARD_MAP_VERSION' <<<"$cm_json")"
  echo "   ConfigMap topology: SHARD_COUNT=${cm_shards} VIRTUAL_BUCKET_COUNT=${cm_bucket_count} SHARD_MAP_VERSION=${cm_map_version}"
  [[ "$cm_shards" == "1" && "$cm_bucket_count" != "null" && "$cm_map_version" != "null" ]] \
    || die "ConfigMap topology keys are missing or wrong"
}

assert_operator_storage_live() {
  local pod_json pvc_json
  pod_json="$(kubectl -n "$NAMESPACE" get pod/"${LUMEN_CR_NAME}-0" -o json)"
  pvc_json="$(kubectl -n "$NAMESPACE" get pvc/"raft-${LUMEN_CR_NAME}-0" -o json)"
  jq -e --arg claim "raft-${LUMEN_CR_NAME}-0" '
    ([.spec.containers[] | select(.name == "server") | .volumeMounts[] | select(.name == "raft")] |
      map(if has("readOnly") then . else . + {"readOnly": false} end)) == [
      {"mountPath":"/var/lib/lumen","name":"raft","readOnly":false},
      {"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":false,"subPath":"data"}
    ] and
    ([.spec.volumes[] | select(.name == "raft" and .persistentVolumeClaim.claimName == $claim)] | length) == 1
  ' <<<"$pod_json" >/dev/null || die "live serving pod lost the exact raft PVC parent/data child mounts"
  jq -e --arg claim "raft-${LUMEN_CR_NAME}-0" '
    .metadata.name == $claim and
    .status.phase == "Bound" and
    (.spec.volumeName | type) == "string" and
    (.spec.volumeName | length) > 0
  ' <<<"$pvc_json" >/dev/null || die "raft-${LUMEN_CR_NAME}-0 PVC is not bound"
  echo "   live pod mounts the bound raft-${LUMEN_CR_NAME}-0 PVC at parent and exact data child paths"
}

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

# Each doc emits 2 IndexItems (bio + email); split the request bodies so no
# single POST exceeds the public HTTP cap (MAX_INDEX_BATCH_SIZE=1000).
generate_fixture() {
  FIXTURE_FILE="$(mktemp -t lumen-fixture.XXXXXX.json)"
  python3 "${LUMEN_DIR}/scripts/load-fixture.py" \
    --count "$DOC_COUNT" \
    --items-per-batch "$BATCH_SIZE" \
    --output "$FIXTURE_FILE"
  shopt -s nullglob
  INDEX_BODIES=( "${FIXTURE_FILE%.json}".req.*.json )
  shopt -u nullglob
  [[ ${#INDEX_BODIES[@]} -gt 0 ]] || die "fixture script emitted no request bodies"
}

index_all_batches() {
  local n=0 body
  for body in "${INDEX_BODIES[@]}"; do
    n=$((n + 1))
    if (( n % 20 == 1 || n == ${#INDEX_BODIES[@]} )); then
      echo "   batch ${n}/${#INDEX_BODIES[@]}: $(basename "$body")"
    fi
    api_index_batch "$body" >/dev/null
  done
}

assert_search_and_duplicates() {
  local search_resp search_hits dup_resp dup_groups
  search_resp="$(api_search)"
  printf '%s\n' "$search_resp" > "${MODE_DIR}/search.json"
  search_hits="$(jq '.hits | length' <<<"$search_resp")"
  echo "   search hits: $search_hits"
  [[ "$search_hits" -gt 0 ]] || die "expected search hits > 0, got $search_hits"
  # One shard, so duplicates is answered locally (kind-e2e.sh explains the
  # routed refusal that a multi-shard topology would assert instead).
  dup_resp="$(api_duplicates)"
  printf '%s\n' "$dup_resp" > "${MODE_DIR}/duplicates.json"
  dup_groups="$(jq '.groups | length' <<<"$dup_resp")"
  echo "   duplicate groups: $dup_groups"
  [[ "$dup_groups" -gt 0 ]] || die "expected at least one duplicate group, got $dup_groups"
}

# ---------------------------------------------------------------------------
# Durable restart oracle
# ---------------------------------------------------------------------------

# Same order as kind-e2e.sh's oracle. The forward is re-opened to the
# replacement pod where kind-e2e.sh re-exposes its NodePort.
durable_restart_oracle() {
  local pre_id="pre-restart-${RUN_LABEL}-${E2E_MODE}-$$"
  local pre_value="${pre_id}@example.invalid"
  local post_id="post-restart-${RUN_LABEL}-${E2E_MODE}-$$"
  local post_value="${post_id}@example.invalid"
  local checkpoint old_hits post_hits
  api_index_exact "$pre_id" "$pre_value"
  checkpoint="$(api_checkpoint)"
  jq -e '.persisted == true' <<<"$checkpoint" >/dev/null || die "/admin/checkpoint did not return persisted=true"
  pf_stop
  kubectl -n "$NAMESPACE" delete pod -l "$APP_LABEL" --wait=true
  wait_lumen_ready 600
  pf_start
  assert_cluster_identity
  old_hits="$(api_search_exact "$pre_value" | jq --arg id "$pre_id" '[.hits[] | select(.external_id == $id)] | length')"
  [[ "$old_hits" -eq 1 ]] || die "pre-restart document was not readable before any new write"
  api_index_exact "$post_id" "$post_value"
  post_hits="$(api_search_exact "$post_value" | jq --arg id "$post_id" '[.hits[] | select(.external_id == $id)] | length')"
  [[ "$post_hits" -eq 1 ]] || die "replacement pod did not accept the post-restart write"
  jq -n --arg pre_id "$pre_id" --arg post_id "$post_id" --argjson checkpoint "$checkpoint" \
    '{pre_id:$pre_id,post_id:$post_id,checkpoint:$checkpoint,pre_restart_hits:1,post_restart_hits:1}' \
    > "${MODE_DIR}/durable-restart.json"
  echo "   durable restart preserved $pre_id and accepted $post_id"
}

# ---------------------------------------------------------------------------
# One deploy path, end to end
# ---------------------------------------------------------------------------

run_mode() {
  enter_mode "$1"
  if [[ "$E2E_MODE" == "operator" ]]; then
    step "2. wait for operator StatefulSet/${LUMEN_CR_NAME}" wait_operator_statefulset
    step "2a. assert native placement" assert_native_placement
    step "2b. assert legacy missing-catalog Event" assert_legacy_missing_catalog_event
    step "2c. assert operator storage topology" assert_operator_topology
  else
    step "2. assert overlay Deployment shape" assert_overlay_shape
  fi
  step "3. wait for serving pods Ready" wait_lumen_ready 600
  step "3a. assert serving pods on pool-${POOL_VALUE}" assert_pods_on_pool
  if [[ "$E2E_MODE" == "operator" ]]; then
    step "3b. assert live operator PVC and mounts" assert_operator_storage_live
  fi
  step "4a. port-forward to a Ready serving pod" pf_start
  step "4a2. assert cluster identity and /version" assert_cluster_identity
  step "4b. PUT /collections/users" api_put_collection
  step "4d. POST /collections/users/index (${DOC_COUNT} docs, ${#INDEX_BODIES[@]} batches)" index_all_batches
  step "4e. search and duplicates" assert_search_and_duplicates
  step "5. checkpoint, replace serving pod, and prove durable recovery" durable_restart_oracle
  pf_stop
  RESULTS="$(jq -c --arg mode "$E2E_MODE" '. + [{mode:$mode,result:"pass"}]' <<<"$RESULTS")"
  echo ">> [${E2E_MODE}] PASS"
}

E2E_MODE=""
step "1. generate ${DOC_COUNT}-doc fixture (batched ≤${BATCH_SIZE} items/req)" generate_fixture
for mode in $MODES; do
  run_mode "$mode"
done

echo ">> gke-test verify PASS (${MODES})"
