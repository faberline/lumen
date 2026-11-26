#!/usr/bin/env bash
# Kind acceptance gate (#570): one run-scoped Kind cluster, one image build,
# the durable single-node overlay, the shared functional runner with a real
# restart, and a guaranteed teardown.
#
#   scripts/kind-acceptance.sh --mode overlay [--artifact-dir <dir>]
#
# Operator mode is not supported yet (#571).

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

MODE=""
ARTIFACT_DIR=""
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
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$MODE" ]] || die "usage: kind-acceptance.sh --mode overlay [--artifact-dir <dir>]"
case "$MODE" in
  overlay) ;;
  *) die "mode '$MODE' is not supported yet" ;;
esac

for tool in docker kind kubectl kustomize jq cargo; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done

RUN_ID="lumen-accept-$(date -u +%Y%m%dt%H%M%Sz)-$$"
ARTIFACT_DIR="${ARTIFACT_DIR:-$REPO_ROOT/target/kind-acceptance/$RUN_ID}"
OUT="$ARTIFACT_DIR/overlay"
mkdir -p "$OUT"
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

phase deploy "overlay k8s/overlays/kind-acceptance"
cp -R "$REPO_ROOT/k8s" "$TMP/k8s"
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
RUNNER_RC=0
(cd "$REPO_ROOT" && cargo run -p lumen --features acceptance --bin lumen-acceptance -- functional \
  --endpoint "http://127.0.0.1:$PORT" \
  --artifact-dir "$OUT" \
  --image-digest "$IMAGE_ID" \
  --environment kind-overlay \
  --restart-cmd "$SCRIPT_DIR/kind-acceptance.sh __restart") >"$OUT/functional.log" 2>&1 || RUNNER_RC=$?

phase verify "$OUT/report.json"
REPORT="$OUT/report.json"
[[ -f "$REPORT" ]] || fail "no report at $REPORT"
[[ "$RUNNER_RC" == 0 ]] || fail "runner exited $RUNNER_RC; see $OUT/functional.log"
jq -e '[.phases[] | select(.status != "passed")] | length == 0' "$REPORT" >/dev/null \
  || fail "a phase is not passed in $REPORT"
jq -e '[.phases[].name] | (index("restart_persistence") != null) and (index("checkpoint_and_merge") != null)' "$REPORT" >/dev/null \
  || fail "report lacks restart_persistence or checkpoint_and_merge in $REPORT"

echo "artifact directory: $ARTIFACT_DIR"
