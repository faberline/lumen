#!/usr/bin/env bash
# Kind acceptance gate (#570, #571): one run-scoped Kind cluster, one image
# build, the durable single-node overlay and the operator overlay, the shared
# functional runner with a real restart in each mode, one combined report and
# a guaranteed teardown.
#
#   scripts/kind-acceptance.sh [--mode overlay|operator|all] [--artifact-dir <dir>]
#   scripts/kind-acceptance.sh --combine-only <overlay.json> <operator.json> --artifact-dir <dir>
#
# --mode all (the default) runs the overlay mode, then the operator mode on the
# same cluster, and writes <artifact-dir>/report.json. The script exits 1 when
# a mode fails or the combined report is not passed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
PORT=7373

die() {
  echo "kind-acceptance: $*" >&2
  exit 2
}

fail() {
  echo "kind-acceptance: $*" >&2
  exit 1
}

phase() {
  local name="$1"
  shift
  echo "phase $name $*" | tee -a "$OUT/phases.log"
}

# Starts the port-forward in the background. Its output goes to files, so the
# runner, which reads the restart command through pipes, does not hang on it.
start_forward() {
  if [[ -f "$TMP/port-forward.pid" ]]; then
    kill "$(cat "$TMP/port-forward.pid")" 2>/dev/null || true
  fi
  nohup kubectl -n lumen port-forward svc/lumen "$PORT:7373" >"$OUT/port-forward.log" 2>&1 </dev/null &
  echo $! >"$TMP/port-forward.pid"
  sleep 2
}

# Runs the shared functional runner against the endpoint. Sets RUNNER_RC.
# $3 is the restart command word that the runner passes back to this script.
run_functional() {
  local out_dir="$1" environment="$2" restart_word="$3"
  RUNNER_RC=0
  (cd "$REPO_ROOT" && cargo run -p lumen --features acceptance --bin lumen-acceptance -- functional \
    --endpoint "http://127.0.0.1:$PORT" \
    --artifact-dir "$out_dir" \
    --image-digest "$IMAGE_ID" \
    --environment "$environment" \
    --restart-cmd "$SCRIPT_DIR/kind-acceptance.sh $restart_word") >"$out_dir/functional.log" 2>&1 || RUNNER_RC=$?
}

# The two checks of a runner report: every phase passed, and the required
# phases are present. Used by both modes.
verify_report() {
  local report="$1" runner_rc="$2"
  [[ -f "$report" ]] || fail "no report at $report"
  [[ "$runner_rc" == 0 ]] || fail "runner exited $runner_rc; see $(dirname "$report")/functional.log"
  jq -e '[.phases[] | select(.status != "passed")] | length == 0' "$report" >/dev/null \
    || fail "a phase is not passed in $report"
  jq -e '[.phases[].name] | (index("restart_persistence") != null) and (index("checkpoint_and_merge") != null)' "$report" >/dev/null \
    || fail "report lacks restart_persistence or checkpoint_and_merge in $report"
}

# Writes the combined report with one jq call. A missing or null mode report is
# null, so a skipped mode fails the report. Arguments: overlay report, operator
# report, topology file, output file, image identity, cleanup_complete (true or false).
combine_reports() {
  local overlay_file="$1" operator_file="$2" topology_file="$3" out_file="$4" image="$5" cleanup="$6"
  local overlay_text="null" operator_text="null" topology_text="null"
  [[ -f "$overlay_file" ]] && overlay_text="$(cat "$overlay_file")"
  [[ -f "$operator_file" ]] && operator_text="$(cat "$operator_file")"
  [[ -f "$topology_file" ]] && topology_text="$(cat "$topology_file")"
  jq -n \
    --argjson overlay "$overlay_text" \
    --argjson operator "$operator_text" \
    --argjson topology "$topology_text" \
    --arg image "$image" \
    --argjson cleanup "$cleanup" \
    '
    def mode_ok($r):
      $r != null
      and (($r.phases // []) | length) > 0
      and ([($r.phases // [])[] | select(.status == "passed")] | length) == (($r.phases // []) | length)
      and ([($r.phases // [])[].name] | (index("restart_persistence") != null) and (index("checkpoint_and_merge") != null));
    ($operator | if . == null then null else . + {topology: $topology} end) as $op
    | {
        image_identity: $image,
        modes: {overlay: $overlay, operator: $op},
        passed: (
          mode_ok($overlay) and mode_ok($operator)
          and ($overlay.image_digest == $image) and ($operator.image_digest == $image)
        ),
        cleanup_complete: $cleanup
      }
    ' >"$out_file.tmp"
  mv "$out_file.tmp" "$out_file"
}

# Restart command of the runner: it runs as `sh -c`, so it re-enters this
# script to reach start_forward.
if [[ "${1:-}" == "__restart" ]]; then
  : "${RUN_ID:?}" "${TMP:?}" "${KUBECONFIG:?}" "${PORT:?}"
  OUT="${OUT:-$TMP/restart}"
  mkdir -p "$OUT"
  export OUT
  kubectl -n lumen rollout restart deployment/lumen
  kubectl -n lumen rollout status deployment/lumen --timeout=300s
  start_forward
  exit 0
fi

# Restart command of the operator mode: the one serving pod of the CR.
if [[ "${1:-}" == "__restart_operator" ]]; then
  : "${RUN_ID:?}" "${TMP:?}" "${KUBECONFIG:?}" "${PORT:?}"
  OUT="${OUT:-$TMP/restart}"
  mkdir -p "$OUT"
  export OUT
  kubectl -n lumen delete pod lumen-0 --wait=true
  kubectl -n lumen rollout status statefulset/lumen --timeout=300s
  start_forward
  exit 0
fi

MODE="all"
ARTIFACT_DIR=""
COMBINE_ONLY=0
COMBINE_OVERLAY=""
COMBINE_OPERATOR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --artifact-dir)
      ARTIFACT_DIR="${2:-}"
      shift 2
      ;;
    --combine-only)
      COMBINE_OVERLAY="${2:-}"
      COMBINE_OPERATOR="${3:-}"
      [[ -n "$COMBINE_OVERLAY" && -n "$COMBINE_OPERATOR" ]] \
        || die "usage: kind-acceptance.sh --combine-only <overlay.json> <operator.json> --artifact-dir <dir>"
      COMBINE_ONLY=1
      shift 3
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

# The combined report of two existing runner reports. Needs only jq; no
# cluster work and no tool check of the cluster.
if [[ "$COMBINE_ONLY" == 1 ]]; then
  [[ -n "$ARTIFACT_DIR" ]] || die "--combine-only needs --artifact-dir"
  command -v jq >/dev/null 2>&1 || die "missing tool: jq"
  mkdir -p "$ARTIFACT_DIR"
  IMAGE=""
  if [[ -f "$COMBINE_OVERLAY" ]]; then
    IMAGE="$(jq -r '.image_digest? // ""' "$COMBINE_OVERLAY" 2>/dev/null || true)"
  fi
  combine_reports "$COMBINE_OVERLAY" "$COMBINE_OPERATOR" "$ARTIFACT_DIR/operator/topology.json" \
    "$ARTIFACT_DIR/report.json" "$IMAGE" true
  cat "$ARTIFACT_DIR/report.json"
  if jq -e '.passed == true' "$ARTIFACT_DIR/report.json" >/dev/null; then
    exit 0
  fi
  exit 1
fi

case "$MODE" in
  overlay | operator | all) ;;
  *) die "mode '$MODE' is not supported (use overlay, operator or all)" ;;
esac

for tool in docker kind kubectl kustomize jq cargo; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done

RUN_ID="lumen-accept-$(date -u +%Y%m%dt%H%M%Sz)-$$"
ARTIFACT_DIR="${ARTIFACT_DIR:-$REPO_ROOT/target/kind-acceptance/$RUN_ID}"
mkdir -p "$ARTIFACT_DIR"
ARTIFACT_DIR="$(cd "$ARTIFACT_DIR" && pwd -P)"
OUT_OVERLAY="$ARTIFACT_DIR/overlay"
OUT_OPERATOR="$ARTIFACT_DIR/operator"
case "$MODE" in
  overlay)
    mkdir -p "$OUT_OVERLAY"
    OUT="$OUT_OVERLAY"
    ;;
  operator)
    mkdir -p "$OUT_OPERATOR"
    OUT="$OUT_OPERATOR"
    ;;
  all)
    mkdir -p "$OUT_OVERLAY" "$OUT_OPERATOR"
    OUT="$OUT_OVERLAY"
    ;;
esac
OUT="$(cd "$OUT" && pwd -P)"

TMP="$(mktemp -d)"
export KUBECONFIG="$TMP/kubeconfig"

cleanup() {
  local rc=$?
  phase teardown "kind delete cluster $RUN_ID"
  if [[ -f "$TMP/port-forward.pid" ]]; then
    kill "$(cat "$TMP/port-forward.pid")" 2>/dev/null || true
  fi
  kind delete cluster --name "$RUN_ID" >"$OUT/teardown.log" 2>&1 || true
  rm -rf "$TMP"
  if kind get clusters 2>/dev/null | grep -qx "$RUN_ID"; then
    echo "kind-acceptance: cluster $RUN_ID still exists after teardown" >&2
    exit 1
  fi
  if [[ -f "$ARTIFACT_DIR/report.json" ]]; then
    jq '.cleanup_complete = true' "$ARTIFACT_DIR/report.json" >"$ARTIFACT_DIR/report.json.tmp" \
      && mv "$ARTIFACT_DIR/report.json.tmp" "$ARTIFACT_DIR/report.json" || true
  fi
  exit "$rc"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export RUN_ID TMP PORT OUT

phase build "image lumen-accept:$RUN_ID"
docker build -t "lumen-accept:$RUN_ID" "$REPO_ROOT" >"$OUT/build.log" 2>&1
IMAGE_ID="$(docker image inspect --format '{{.Id}}' "lumen-accept:$RUN_ID")"

phase cluster "kind create cluster $RUN_ID"
kind create cluster --name "$RUN_ID" --kubeconfig "$KUBECONFIG" --wait 120s >"$OUT/cluster.log" 2>&1

phase load "lumen-accept:$RUN_ID into $RUN_ID"
kind load docker-image "lumen-accept:$RUN_ID" --name "$RUN_ID" >"$OUT/load.log" 2>&1

cp -R "$REPO_ROOT/k8s" "$TMP/k8s"

overlay_mode() {
  OUT="$OUT_OVERLAY"
  export OUT

  phase deploy "overlay k8s/overlays/kind-acceptance"
  (cd "$TMP/k8s/overlays/kind-acceptance" && kustomize edit set image "ghcr.io/faberline/lumen=lumen-accept:$RUN_ID") >"$OUT/deploy.log" 2>&1
  kubectl apply -k "$TMP/k8s/overlays/kind-acceptance" >>"$OUT/deploy.log" 2>&1

  phase rollout "deployment/lumen"
  kubectl -n lumen rollout status deployment/lumen --timeout=300s >"$OUT/rollout.log" 2>&1

  POD_IMAGE_ID="$(kubectl -n lumen get pod -l app=lumen,role=server -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')"
  if [[ "$POD_IMAGE_ID" != *"${IMAGE_ID#sha256:}"* ]]; then
    fail "pod image $POD_IMAGE_ID does not match loaded image $IMAGE_ID"
  fi

  phase forward "svc/lumen on 127.0.0.1:$PORT"
  start_forward

  phase functional "lumen-acceptance functional"
  run_functional "$OUT" kind-overlay __restart

  phase verify "$OUT/report.json"
  verify_report "$OUT/report.json" "$RUNNER_RC"
}

operator_mode() {
  OUT="$OUT_OPERATOR"
  export OUT
  local op_dir="$TMP/k8s/overlays/kind-acceptance-operator"

  if [[ "$MODE" == all ]]; then
    # The overlay and the CR share the Service name lumen: remove the overlay first.
    phase operator-deploy "delete overlay k8s/overlays/kind-acceptance"
    kubectl delete -k "$TMP/k8s/overlays/kind-acceptance" --wait=true --ignore-not-found >"$OUT/deploy.log" 2>&1
    kubectl -n lumen wait --for=delete deployment/lumen --timeout=120s >>"$OUT/deploy.log" 2>&1
  fi

  phase operator-deploy "overlay k8s/overlays/kind-acceptance-operator and the Lumen CR"
  (cd "$op_dir" && kustomize edit set image "ghcr.io/faberline/lumen=lumen-accept:$RUN_ID") >>"$OUT/deploy.log" 2>&1
  kubectl apply -k "$op_dir" >>"$OUT/deploy.log" 2>&1
  kubectl -n lumen rollout status deployment/lumen-operator --timeout=300s >>"$OUT/deploy.log" 2>&1
  kubectl wait --for=condition=Established crd/lumens.lumen.dev --timeout=60s >>"$OUT/deploy.log" 2>&1
  sed -i.bak "s|REPLACE_ME|lumen-accept:$RUN_ID|" "$op_dir/lumen-cr.yaml"
  kubectl apply -f "$op_dir/lumen-cr.yaml" >>"$OUT/deploy.log" 2>&1

  phase operator-ready "statefulset lumen"
  for _ in $(seq 1 60); do
    kubectl -n lumen get statefulset lumen >/dev/null 2>&1 && break
    sleep 2
  done
  kubectl -n lumen get statefulset lumen >/dev/null 2>&1 || fail "statefulset lumen did not appear within 120 s"
  kubectl -n lumen wait --for=jsonpath='{.status.phase}'=Ready lumen/lumen --timeout=300s >"$OUT/rollout.log" 2>&1
  kubectl -n lumen rollout status statefulset/lumen --timeout=300s >>"$OUT/rollout.log" 2>&1

  POD_IMAGE_ID="$(kubectl -n lumen get pod -l app.kubernetes.io/name=lumen,app.kubernetes.io/instance=lumen,app.kubernetes.io/component=server -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')"
  if [[ "$POD_IMAGE_ID" != *"${IMAGE_ID#sha256:}"* ]]; then
    fail "operator pod image $POD_IMAGE_ID does not match loaded image $IMAGE_ID"
  fi

  phase operator-topology "statefulset, pvc and CR of lumen"
  kubectl -n lumen get statefulset lumen -o json >"$OUT/statefulset.json"
  kubectl -n lumen get pvc -o json >"$OUT/pvc.json"
  kubectl -n lumen get lumen lumen -o json >"$OUT/cr.json"
  jq -n \
    --argjson sts "$(cat "$OUT/statefulset.json")" \
    --argjson pvc "$(cat "$OUT/pvc.json")" \
    --argjson cr "$(cat "$OUT/cr.json")" \
    '{
      statefulset: $sts.metadata.name,
      replicas: $sts.spec.replicas,
      ready_replicas: $sts.status.readyReplicas,
      pvcs: [$pvc.items[].metadata.name],
      cr_phase: $cr.status.phase,
      cr_ready_replicas: $cr.status.servingReadyReplicas
    }' >"$OUT/topology.json"
  jq -e '.statefulset == "lumen" and .replicas == 1 and .ready_replicas == 1
    and (.pvcs | length) == 1 and .cr_phase == "Ready" and .cr_ready_replicas == 1' "$OUT/topology.json" >/dev/null \
    || fail "operator topology is not as expected in $OUT/topology.json"

  phase forward "svc/lumen on 127.0.0.1:$PORT"
  start_forward

  phase functional "lumen-acceptance functional (operator)"
  run_functional "$OUT" kind-operator __restart_operator

  phase verify "$OUT/report.json"
  verify_report "$OUT/report.json" "$RUNNER_RC"
}

# Each mode runs in its own subshell with errexit on, so a failure in one mode
# does not stop the other mode or the combined report. The EXIT trap is not
# inherited by a subshell.
MODE_RC=0
set +e
if [[ "$MODE" == overlay || "$MODE" == all ]]; then
  (set -e; overlay_mode)
  RC=$?
  [[ "$RC" == 0 ]] || MODE_RC="$RC"
fi
if [[ "$MODE" == operator || "$MODE" == all ]]; then
  (set -e; operator_mode)
  RC=$?
  [[ "$RC" == 0 ]] || MODE_RC="$RC"
fi
set -e

if [[ "$MODE" == all ]]; then
  combine_reports "$OUT_OVERLAY/report.json" "$OUT_OPERATOR/report.json" "$OUT_OPERATOR/topology.json" \
    "$ARTIFACT_DIR/report.json" "$IMAGE_ID" false
  if ! jq -e '.passed == true' "$ARTIFACT_DIR/report.json" >/dev/null; then
    echo "kind-acceptance: combined report is not passed: $ARTIFACT_DIR/report.json" >&2
    MODE_RC=1
  fi
fi

echo "artifact directory: $ARTIFACT_DIR"
exit "$MODE_RC"
