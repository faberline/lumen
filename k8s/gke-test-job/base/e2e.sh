#!/usr/bin/env bash
# lumen — end-to-end check, run as the in-cluster test Job on pool-lumen-client
# of Faberline's shared GKE test cluster (faberline/aw
# docs/standards/test-cluster.md).
#
# aw's run.sh applies k8s/overlays/gke-test, waits for its rollouts, then
# applies one suite folder (../overlay or ../operator) and waits for the Job.
# This script runs kind-e2e.sh's checks for the deploy paths in
# LUMEN_E2E_MODES, in the Job's namespace, in order:
#
#   overlay   the direct-install Deployment `lumen` (labels app=lumen,role=server),
#             reached at http://lumen:7373
#   operator  the Lumen CR `lumen-op`, reconciled by the shipped operator,
#             reached at http://lumen-op:7373, plus
#             `lumen-op-legacy-missing-catalog`, which must fail on the missing
#             capacity catalog
#
# For each path: deploy shape and placement, PUT a collection, index the
# 100k-doc fixture, search, duplicates, then the durable restart oracle
# (checkpoint one document, replace the serving pod, read it back before any
# new write, and prove the replacement accepts a write).
#
# The Job cannot know the image under test, so image identity is checked on
# the runner afterwards by scripts/gke-test/verify.sh. This script hands it the
# API's answers as evidence lines on stdout:
#
#   LUMEN_E2E_EVIDENCE<TAB><mode><TAB><name><TAB><compact JSON>
#
# Inputs (from the Job's env):
#   NAMESPACE             the Job's namespace (downward API)
#   POD_NAME              the Job pod's name, used in the oracle document ids
#   LUMEN_E2E_DOC_COUNT   fixture size (default 100000)
#   LUMEN_E2E_MODES       the suite: "overlay" or "operator"; default both
#
# Requirements: kubectl (in-cluster ServiceAccount), curl, jq.

set -euo pipefail

die() {
  echo "!! $*" >&2
  exit 1
}

: "${NAMESPACE:?NAMESPACE is required}"
: "${POD_NAME:?POD_NAME is required}"

LUMEN_CR_NAME="lumen-op"
LEGACY_CR_NAME="lumen-op-legacy-missing-catalog"
OPERATOR_DEPLOYMENT="lumen-operator"
POOL_KEY="faberline.dev/pool"
POOL_VALUE="lumen-server"
PULL_SECRET="ghcr-pull"
EXPECTED_STORAGE_PODS=1
PORT=7373
DOC_COUNT="${LUMEN_E2E_DOC_COUNT:-100000}"
# Each doc is 2 IndexItems (bio + email); the public HTTP cap is
# MAX_INDEX_BATCH_SIZE=1000 items per request.
DOCS_PER_BATCH=500
MODES="${LUMEN_E2E_MODES:-overlay operator}"
WORK_DIR="${WORK_DIR:-/work}"

[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "invalid NAMESPACE: $NAMESPACE"
[[ "$POD_NAME" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "invalid POD_NAME: $POD_NAME"
[[ "$DOC_COUNT" =~ ^[1-9][0-9]*$ ]] || die "invalid LUMEN_E2E_DOC_COUNT: $DOC_COUNT"
for mode in $MODES; do
  [[ "$mode" == "overlay" || "$mode" == "operator" ]] || die "invalid LUMEN_E2E_MODES entry: $mode"
done

E2E_MODE=""
APP_LABEL=""
BASE_URL=""
BODIES_FILE="${WORK_DIR}/index-bodies.ndjson"
BATCH_COUNT=0

# ---------------------------------------------------------------------------
# Timing and evidence
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
  evidence timing "$(jq -nc --arg step "$label" --argjson seconds "$((end - start))" '{step:$step,seconds:$seconds}')"
}

# evidence <name> <json>: one line scripts/gke-test/verify.sh collects.
evidence() {
  printf 'LUMEN_E2E_EVIDENCE\t%s\t%s\t%s\n' "${E2E_MODE:-setup}" "$1" "$(jq -c . <<<"$2")"
}

# On failure, print what the runner would otherwise have to go and look at.
# aw keeps this pod's log, every pod's log, and `describe pods` as evidence.
on_exit() {
  local ec=$?
  set +e
  if [[ "$ec" -ne 0 ]]; then
    echo "!! [${E2E_MODE:-setup}] FAIL (exit $ec); cluster state follows"
    kubectl -n "$NAMESPACE" get lumens.lumen.dev -o yaml
    kubectl -n "$NAMESPACE" get statefulsets,deployments,pods,pvc,endpoints -o wide
    kubectl -n "$NAMESPACE" logs deploy/"$OPERATOR_DEPLOYMENT" --tail=400
  fi
  exit "$ec"
}
trap on_exit EXIT
trap 'exit 143' INT TERM

# ---------------------------------------------------------------------------
# Mode selection
# ---------------------------------------------------------------------------

enter_mode() {
  E2E_MODE="$1"
  if [[ "$E2E_MODE" == "operator" ]]; then
    # The instance label keeps the legacy CR and the overlay pod out of it.
    APP_LABEL="app.kubernetes.io/name=lumen,app.kubernetes.io/instance=${LUMEN_CR_NAME},app.kubernetes.io/component=server"
    BASE_URL="http://${LUMEN_CR_NAME}:${PORT}"
  else
    APP_LABEL="app=lumen,role=server"
    BASE_URL="http://lumen:${PORT}"
  fi
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

# The Service only routes to Ready pods, and a replaced pod's endpoint takes a
# moment to appear, so wait for /healthz through the Service itself.
wait_service() {
  local deadline=$(( $(date +%s) + 180 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if curl -fsS --max-time 5 "${BASE_URL}/healthz" >/dev/null 2>&1; then
      echo "   ${BASE_URL} answers /healthz"
      return 0
    fi
    sleep 2
  done
  die "${BASE_URL}/healthz did not answer within 180s"
}

# ---------------------------------------------------------------------------
# API calls (same requests as kind-e2e.sh)
# ---------------------------------------------------------------------------

# curl through the Service. A transport failure (connection refused or reset,
# empty reply, timeout) waits and repeats the request; curl's exit 22 is an
# HTTP error status from lumen and fails at once. Every request made through
# here is safe to repeat: index items carry external ids.
api() {
  local attempt rc out
  for attempt in 1 2 3 4 5; do
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
    echo "   transport failure (curl exit $rc, attempt $attempt); retrying" >&2
    sleep $((attempt * 3))
  done
  echo "!! request still failing after $attempt attempts: curl $*" >&2
  return "$rc"
}

api_version() {
  api --max-time 10 "${BASE_URL}/version"
}

api_put_collection() {
  api --max-time 30 -X PUT "${BASE_URL}/collections/users" \
    -H 'content-type: application/json' \
    -d '{"fields": {"bio": {"type": "text"}, "email": {"type": "keyword"}}}'
}

api_index_body() {
  api --max-time 120 -X POST "${BASE_URL}/collections/users/index" \
    -H 'content-type: application/json' \
    --data-binary @-
}

api_search() {
  api --max-time 30 -X POST "${BASE_URL}/collections/users/search" \
    -H 'content-type: application/json' \
    -d '{"query": {"match": {"field": "bio", "text": "engineer"}}, "limit": 20}'
}

api_duplicates() {
  api --max-time 30 -X POST "${BASE_URL}/collections/users/duplicates" \
    -H 'content-type: application/json' \
    -d '{"field": "email", "min_group_size": 2, "limit": 100}'
}

api_index_exact() {
  local external_id="$1" value="$2"
  local body
  body="$(jq -nc --arg id "$external_id" --arg value "$value" \
    '{items:[{external_id:$id,field:"email",value:$value}]}')"
  api --max-time 30 -X POST "${BASE_URL}/collections/users/index" \
    -H 'content-type: application/json' -d "$body"
}

api_search_exact() {
  local value="$1"
  local body
  body="$(jq -nc --arg value "$value" '{query:{term:{field:"email",value:$value}},limit:5}')"
  api --max-time 30 -X POST "${BASE_URL}/collections/users/search" \
    -H 'content-type: application/json' -d "$body"
}

api_checkpoint() {
  api --max-time 120 -X POST "${BASE_URL}/admin/checkpoint" \
    -H 'content-type: application/json' -d '{}'
}

# /version as the serving pod reports it; the runner compares it with the
# expected version and git sha.
record_version() {
  local phase="$1" ver_json
  ver_json="$(api_version)"
  jq -e '(.version | type) == "string" and (.git_sha | type) == "string"' <<<"$ver_json" >/dev/null \
    || die "/version did not return string version and git_sha: $ver_json"
  evidence "version-${phase}" "$ver_json"
  echo "   /version (${phase}): $(jq -c '{version,git_sha}' <<<"$ver_json")"
}

# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------

# A workload's pod template must schedule onto pool-lumen-server and pull from GHCR.
assert_pool_placement() {
  local what="$1" workload_json="$2"
  jq -e --arg key "$POOL_KEY" --arg value "$POOL_VALUE" --arg secret "$PULL_SECRET" '
    .spec.template.spec as $s |
    ($s.nodeSelector[$key] // "") == $value and
    any($s.tolerations[]?; .key == $key and .operator == "Equal" and .value == $value and .effect == "NoSchedule") and
    any($s.imagePullSecrets[]?; .name == $secret)
  ' <<<"$workload_json" >/dev/null || die "$what is missing the ${POOL_KEY}=${POOL_VALUE} selector, toleration, or ${PULL_SECRET} pull secret"
}

# Nodes are cluster-scoped and this Job's Role is not, so check what put each
# pod where it is: the selector on the scheduled pod. The node pool's taint
# keeps every pod without the matching toleration off it, and the runner
# checks the node labels afterwards.
assert_pods_on_pool() {
  local pods
  pods="$(kubectl -n "$NAMESPACE" get pods -l "$APP_LABEL" -o json)"
  jq -e --arg key "$POOL_KEY" --arg value "$POOL_VALUE" '
    (.items | length) > 0 and
    all(.items[]; (.spec.nodeName // "") != "" and (.spec.nodeSelector[$key] // "") == $value)
  ' <<<"$pods" >/dev/null || die "a serving pod is unscheduled or lacks ${POOL_KEY}=${POOL_VALUE}"
  evidence pods "$(jq '[.items[] | {name:.metadata.name,node:.spec.nodeName}]' <<<"$pods")"
  echo "   serving pods scheduled with ${POOL_KEY}=${POOL_VALUE}"
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
  jq -e '
    ([.spec.template.spec.containers[] | select(.name == "lumen") | .env[] | select(.name == "LUMEN_WAL" and .value == "embedded")] | length) == 1 and
    ([.spec.template.spec.containers[] | select(.name == "lumen") | .volumeMounts[] | select(.name == "data" and .mountPath == "/var/lib/lumen/data")] | length) == 1 and
    ([.spec.template.spec.volumes[] | select(.name == "data" and .persistentVolumeClaim.claimName == "lumen-data")] | length) == 1
  ' <<<"$deploy_json" >/dev/null || die "overlay Deployment lost LUMEN_WAL=embedded or the lumen-data mount"
  assert_pool_placement "Deployment/lumen" "$deploy_json"
  kubectl -n "$NAMESPACE" get pvc/lumen-data -o json | jq -e '.status.phase == "Bound"' >/dev/null \
    || die "lumen-data PVC is not bound"
  echo "   overlay Deployment runs LUMEN_WAL=embedded on the bound lumen-data PVC in pool-lumen-server"
}

# ---------------------------------------------------------------------------
# Operator deploy path
# ---------------------------------------------------------------------------

wait_operator_statefulset() {
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
      evidence legacy-reconcile-failed "$(jq -n --arg message "$message" '{message:$message}')"
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

# The same shape as scripts/load-fixture.py, which kustomize cannot load from
# outside this folder: per doc one `bio` item from the same four word lists and
# one `email` item, about 5% of them from a pool of 50 shared addresses so the
# duplicates endpoint has groups to return. A fixed hash of the doc number
# replaces the seeded RNG, so every run indexes the same bytes. One request
# body per line, at most DOCS_PER_BATCH docs each, with a request_id unique
# per batch (the server dedups a repeated id as an idempotent retry).
generate_fixture() {
  mkdir -p "$WORK_DIR"
  jq -nc --argjson count "$DOC_COUNT" --argjson per "$DOCS_PER_BATCH" '
    ["fast","quiet","fierce","kind","bold","calm","eager","proud",
     "happy","curious","brave","gentle","lucky","wise","young","old"] as $adj |
    ["engineer","designer","writer","doctor","teacher","musician",
     "scientist","explorer","chef","pilot","farmer","architect"] as $noun |
    ["rust","python","go","typescript","kotlin","swift","java",
     "c++","haskell","ocaml","elixir","ruby"] as $lang |
    ["hiking","cycling","running","reading","gaming","cooking",
     "painting","writing","climbing","skiing","surfing"] as $hobby |
    def h($i; $k): (($i + 1) * $k) % 1000003;
    def pick($a; $i; $k): $a[h($i; $k) % ($a | length)];
    def doc($i):
      ("u" + ("\($i)" | (("0" * (7 - length)) + .))) as $id |
      [{external_id: $id, field: "bio",
        value: "a \(pick($adj; $i; 7919)) \(pick($noun; $i; 104729)) who codes in \(pick($lang; $i; 1299709)) and loves \(pick($hobby; $i; 15485863))"},
       {external_id: $id, field: "email",
        value: (if h($i; 32452843) % 20 == 0
                then "shared\(h($i; 49979687) % 50)@example.com"
                else "user\($i)@example.com" end)}];
    range(0; ($count + $per - 1) / $per | floor) as $b |
    {items: [range($b * $per; [($b + 1) * $per, $count] | min) | doc(.)[]],
     request_id: "e2e-gke-\($count)-\($b)"}
  ' > "$BODIES_FILE"
  BATCH_COUNT="$(wc -l < "$BODIES_FILE" | tr -d ' ')"
  [[ "$BATCH_COUNT" -gt 0 ]] || die "fixture generator emitted no request bodies"
  jq -e -s --argjson count "$DOC_COUNT" '(map(.items | length) | add) == 2 * $count' "$BODIES_FILE" >/dev/null \
    || die "fixture does not hold 2 items for each of ${DOC_COUNT} docs"
  echo "   ${DOC_COUNT} docs in ${BATCH_COUNT} request bodies ($(wc -c < "$BODIES_FILE" | tr -d ' ') bytes)"
}

index_all_batches() {
  local n=0 body
  while IFS= read -r body; do
    n=$((n + 1))
    if (( n % 20 == 1 || n == BATCH_COUNT )); then
      echo "   batch ${n}/${BATCH_COUNT}"
    fi
    printf '%s' "$body" | api_index_body >/dev/null
  done < "$BODIES_FILE"
  [[ "$n" -eq "$BATCH_COUNT" ]] || die "indexed $n of $BATCH_COUNT batches"
}

assert_search_and_duplicates() {
  local search_resp search_hits dup_resp dup_groups
  search_resp="$(api_search)"
  search_hits="$(jq '.hits | length' <<<"$search_resp")"
  evidence search "$(jq '{hits: (.hits | length), first: (.hits[0] // null)}' <<<"$search_resp")"
  echo "   search hits: $search_hits"
  [[ "$search_hits" -gt 0 ]] || die "expected search hits > 0, got $search_hits"
  # One shard, so duplicates is answered locally (kind-e2e.sh explains the
  # routed refusal that a multi-shard topology would assert instead).
  dup_resp="$(api_duplicates)"
  dup_groups="$(jq '.groups | length' <<<"$dup_resp")"
  evidence duplicates "$(jq '{groups: (.groups | length), first: (.groups[0] // null)}' <<<"$dup_resp")"
  echo "   duplicate groups: $dup_groups"
  [[ "$dup_groups" -gt 0 ]] || die "expected at least one duplicate group, got $dup_groups"
}

# ---------------------------------------------------------------------------
# Durable restart oracle
# ---------------------------------------------------------------------------

# Same order as kind-e2e.sh's oracle. The Service, not a NodePort, finds the
# replacement pod once it is Ready.
durable_restart_oracle() {
  local pre_id="pre-restart-${POD_NAME}-${E2E_MODE}"
  local pre_value="${pre_id}@example.invalid"
  local post_id="post-restart-${POD_NAME}-${E2E_MODE}"
  local post_value="${post_id}@example.invalid"
  local checkpoint old_hits post_hits before after
  api_index_exact "$pre_id" "$pre_value" >/dev/null
  checkpoint="$(api_checkpoint)"
  jq -e '.persisted == true' <<<"$checkpoint" >/dev/null || die "/admin/checkpoint did not return persisted=true"
  before="$(kubectl -n "$NAMESPACE" get pods -l "$APP_LABEL" -o json | jq -c '[.items[].metadata.uid] | sort')"
  kubectl -n "$NAMESPACE" delete pod -l "$APP_LABEL" --wait=true
  wait_lumen_ready 600
  after="$(kubectl -n "$NAMESPACE" get pods -l "$APP_LABEL" -o json | jq -c '[.items[].metadata.uid] | sort')"
  jq -e -n --argjson before "$before" --argjson after "$after" \
    '($before - $after) == $before' >/dev/null || die "a serving pod survived the restart: before=$before after=$after"
  wait_service
  assert_pods_on_pool
  record_version after-restart
  old_hits="$(api_search_exact "$pre_value" | jq --arg id "$pre_id" '[.hits[] | select(.external_id == $id)] | length')"
  [[ "$old_hits" -eq 1 ]] || die "pre-restart document was not readable before any new write"
  api_index_exact "$post_id" "$post_value" >/dev/null
  post_hits="$(api_search_exact "$post_value" | jq --arg id "$post_id" '[.hits[] | select(.external_id == $id)] | length')"
  [[ "$post_hits" -eq 1 ]] || die "replacement pod did not accept the post-restart write"
  evidence durable-restart "$(jq -n --arg pre_id "$pre_id" --arg post_id "$post_id" --argjson checkpoint "$checkpoint" \
    '{pre_id:$pre_id,post_id:$post_id,checkpoint:$checkpoint,pre_restart_hits:1,post_restart_hits:1}')"
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
  step "4a. wait for ${BASE_URL}" wait_service
  step "4a2. record /version" record_version before-restart
  step "4b. PUT /collections/users" api_put_collection
  step "4d. POST /collections/users/index (${DOC_COUNT} docs, ${BATCH_COUNT} batches)" index_all_batches
  step "4e. search and duplicates" assert_search_and_duplicates
  step "5. checkpoint, replace serving pod, and prove durable recovery" durable_restart_oracle
  evidence result '{"result":"pass"}'
  echo ">> [${E2E_MODE}] PASS"
}

E2E_MODE=""
step "1. generate ${DOC_COUNT}-doc fixture (≤${DOCS_PER_BATCH} docs/req)" generate_fixture
for mode in $MODES; do
  run_mode "$mode"
done

E2E_MODE=""
evidence result "$(jq -n --arg modes "$MODES" '{result:"pass",modes:($modes | split(" "))}')"
echo ">> gke-test e2e PASS (${MODES})"
