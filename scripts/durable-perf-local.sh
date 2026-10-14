#!/usr/bin/env bash
# Local driver for the durable performance matrix.
#
# Wraps the existing contract of tests/it/perf_gate.rs
# (perf_gate::durable_workload::approved_30_minute_durable_workload) so a whole
# local run is one command that leaves receipts behind. `build`, `cell`,
# `matrix`, and `status` are diagnostic only. `qualify` and `qualify-matrix`
# write schema-v1 qualifying receipts for one candidate run: they require the
# candidate registry digest (LUMEN_PERF_IMAGE=repo@sha256:...) and a clean
# checkout at the candidate commit, and verify-durable-perf.py judges the
# sixteen receipts. Every mode refuses CARGO_PROFILE_* overrides, so the load
# client always uses the repository [profile.release].
#
# Usage:
#   scripts/durable-perf-local.sh build
#       docker build Dockerfile into a fresh build dir; refuses the
#       image when any tracked or untracked source changed during the build.
#   scripts/durable-perf-local.sh cell <endpoint> <batch> <backend>
#       run one diagnostic cell against the latest (or $LUMEN_PERF_BUILD_DIR)
#       build; exit code is the test's exit code.
#   scripts/durable-perf-local.sh matrix
#       run every cell of the qualifying matrix in order, skipping cells whose
#       receipt already records exit 0, continuing past failures; exit 1 when
#       any cell is red.
#   scripts/durable-perf-local.sh status
#       print the cell receipts of the selected build as a table.
#   scripts/durable-perf-local.sh qualify <endpoint> <batch> <backend>
#       run one qualifying cell. Requires LUMEN_PERF_IMAGE (the candidate
#       ghcr.io/faberline/lumen@sha256:<root digest>), LUMEN_PERF_RUN_ID,
#       LUMEN_PERF_RUN_ATTEMPT, and LUMEN_PERF_COMMIT (HEAD of a clean
#       checkout); LUMEN_PERF_REPOSITORY defaults to faberline/lumen. The
#       harness writes the receipt only when every gate passes.
#   scripts/durable-perf-local.sh qualify-matrix
#       run every qualifying cell that has no receipt yet, in order, continuing
#       past failures, then run verify-durable-perf.py once all sixteen exist.
#
# Layout: $LUMEN_PERF_LOCAL_ROOT (default /tmp/lumen-durable-perf-local)
#   <ts>/source.sha256 build.log image-id.txt image-inspect.json
#   <ts>/cells/<cell>.log <cell>.json  and failure bundles (TMPDIR is pointed
#   here so perf_gate's lumen-perf-failure-* directories land next to the log)
#   <ts>/summary.json          written by `matrix`
#   latest -> <ts>             updated by `build`
#   qualify/<run_id>-<attempt>/perf/<cell>.json   qualifying receipts
#   qualify/<run_id>-<attempt>/logs/<cell>.log    and failure bundles
#   qualify/<run_id>-<attempt>/durable-perf-summary.json(.sha256)
#       written by verify-durable-perf.py; this directory is the
#       --durable-perf-dir of the release verifier.
#
# Signals for a monitor: every finished cell prints one line
#   CELL <cell> exit=<n> duration_s=<n> bundle=<path|->
# and `matrix` ends with MATRIX exit=<n> red=<k> summary=<path>.
# Qualifying cells print QUALIFY <cell> exit=<n> duration_s=<n> bundle=<path|->,
# and `qualify-matrix` ends with QUALIFY-MATRIX exit=<n> red=<k> dir=<path>
# after EVIDENCE dir=<path> summary_sha256=<64hex> when all sixteen pass.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="${LUMEN_PERF_LOCAL_ROOT:-/tmp/lumen-durable-perf-local}"
GIT=(git -c core.fsmonitor=false)
WORKLOAD_TEST="perf_gate::durable_workload::approved_30_minute_durable_workload"
MATRIX_CELLS=(
  index-1-flat-cpu index-1-hnsw-cpu
  index-100-flat-cpu index-100-hnsw-cpu
  index-1000-flat-cpu index-1000-hnsw-cpu
  unindex-1-flat-cpu unindex-1-hnsw-cpu
  unindex-100-flat-cpu unindex-100-hnsw-cpu
  unindex-1000-flat-cpu unindex-1000-hnsw-cpu
  replace-1-flat-cpu replace-1-hnsw-cpu
  replace-32-flat-cpu replace-32-hnsw-cpu
)

die() { printf 'refused: %s\n' "$*" >&2; exit 2; }

release_profile_only() {
  local overrides
  overrides="$(env | cut -d= -f1 | grep '^CARGO_PROFILE_' || true)"
  [ -z "$overrides" ] || die "CARGO_PROFILE_* overrides are not allowed: $(printf '%s' "$overrides" | tr '\n' ' ')"
}
now_utc() { date -u +%Y%m%dT%H%M%SZ; }
epoch() { date +%s; }

source_digest() {
  # One digest over every tracked and untracked (non-ignored) file: path + content.
  (cd "$REPO_ROOT" && "${GIT[@]}" ls-files -z --cached --others --exclude-standard \
    | python3 -c '
import hashlib, os, sys
names = sorted(set(sys.stdin.buffer.read().split(b"\0")) - {b""})
total = hashlib.sha256()
count = 0
for name in names:
    path = os.fsdecode(name)
    if os.path.islink(path):
        digest = hashlib.sha256(b"symlink:" + os.fsencode(os.readlink(path))).hexdigest()
    elif os.path.isfile(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        digest = h.hexdigest()
    else:
        continue
    total.update(name + b"\0" + digest.encode() + b"\n")
    count += 1
print(f"{total.hexdigest()} {count}")
')
}

lane_is_idle() {
  if pgrep -f 'it-[0-9a-f]+ .*perf_gate::' >/dev/null 2>&1; then
    die "another perf_gate test process is running; one Cargo/Docker lane at a time"
  fi
  local containers
  containers="$(docker ps -q --filter name=lumen-perf- 2>/dev/null || true)"
  [ -z "$containers" ] || die "lumen-perf-* container(s) still running: $containers"
}

build_dir() {
  local dir="${LUMEN_PERF_BUILD_DIR:-$ROOT/latest}"
  [ -d "$dir" ] || die "no build dir at $dir; run '$0 build' first or set LUMEN_PERF_BUILD_DIR"
  [ -s "$dir/image-id.txt" ] || die "$dir has no accepted image-id.txt"
  (cd "$dir" && pwd -P)
}

cmd_build() {
  lane_is_idle
  release_profile_only
  command -v docker >/dev/null || die "docker is required"
  local ts dir tag before after code image
  ts="$(now_utc)"
  dir="$ROOT/$ts"
  mkdir -p "$dir"
  tag="lumen-durable-perf-local:$(printf '%s' "$ts" | tr '[:upper:]' '[:lower:]')"
  before="$(source_digest)"
  printf '%s\n' "$before" >"$dir/source.sha256"
  printf 'build %s image %s source %s\n' "$dir" "$tag" "$before"
  set +e
  # --provenance=false --sbom=false: with the containerd image store the
  # inspected Id is the OCI index digest, and a provenance attestation
  # carries build timestamps, so two builds of identical bytes would get
  # two ids. Without attestations the id is content-addressed and the
  # build-twice-same-id self-check means what it says.
  docker build --progress plain --provenance=false --sbom=false --file "$REPO_ROOT/Dockerfile" --tag "$tag" "$REPO_ROOT" >"$dir/build.log" 2>&1
  code=$?
  set -e
  printf '%s\n' "$code" >"$dir/build.exit"
  after="$(source_digest)"
  if [ "$before" != "$after" ]; then
    printf 'source changed during the build (%s -> %s); image not accepted\n' "$before" "$after" >&2
    exit 1
  fi
  if [ "$code" -ne 0 ]; then
    printf 'docker build failed with exit %s; see %s\n' "$code" "$dir/build.log" >&2
    exit "$code"
  fi
  docker image inspect "$tag" >"$dir/image-inspect.json"
  image="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[0]["Id"])' "$dir/image-inspect.json")"
  [[ "$image" =~ ^sha256:[0-9a-f]{64}$ ]] || die "unexpected image id $image"
  printf '%s\n' "$image" >"$dir/image-id.txt"
  ln -sfn "$ts" "$ROOT/latest"
  printf 'BUILD dir=%s image=%s source=%s\n' "$dir" "$image" "${before%% *}"
}

run_cell() {
  # run_cell <build_dir> <endpoint> <batch> <backend>
  local dir="$1" endpoint="$2" batch="$3" backend="$4"
  local cell="$endpoint-$batch-$backend"
  local cells="$dir/cells" log json image source started ended code bundle
  mkdir -p "$cells"
  log="$cells/$cell.log"
  json="$cells/$cell.json"
  image="$(cat "$dir/image-id.txt")"
  source="$(cut -d' ' -f1 "$dir/source.sha256")"
  started="$(epoch)"
  set +e
  (
    cd "$REPO_ROOT" && \
    env -u LUMEN_PERF_QUALIFYING_CELL -u LUMEN_PERF_RECEIPT_PATH TMPDIR="$cells" \
      LUMEN_PERF_DIAGNOSTIC=1 \
      LUMEN_PERF_IMAGE="$image" \
      LUMEN_PERF_ENDPOINT="$endpoint" \
      LUMEN_PERF_BATCH="$batch" \
      LUMEN_PERF_BACKEND="$backend" \
      cargo test --release --locked -p lumen --test it -- \
        --ignored --exact "$WORKLOAD_TEST" --test-threads=1 --nocapture
  ) >"$log" 2>&1
  code=$?
  set -e
  ended="$(epoch)"
  bundle="$(grep -o 'PERF_FAILURE_EVIDENCE path=[^ ]*' "$log" | tail -n1 | cut -d= -f2- || true)"
  python3 - "$json" "$cell" "$endpoint" "$batch" "$backend" "$image" "$source" "$started" "$ended" "$code" "${bundle:-}" "$log" <<'PY'
import json, sys
(path, cell, endpoint, batch, backend, image, source, started, ended, code, bundle, log) = sys.argv[1:]
json.dump({
    "cell": cell, "endpoint": endpoint, "batch": int(batch), "backend": backend,
    "mode": "diagnostic", "acceptance": False,
    "image": image, "source_sha256": source,
    "started_unix": int(started), "ended_unix": int(ended),
    "duration_s": int(ended) - int(started), "exit": int(code),
    "failure_bundle": bundle or None, "log": log,
}, open(path, "w"), indent=2, sort_keys=True)
open(path, "a").write("\n")
PY
  printf 'CELL %s exit=%s duration_s=%s bundle=%s\n' "$cell" "$code" "$((ended - started))" "${bundle:--}"
  return "$code"
}

cmd_cell() {
  [ $# -eq 3 ] || die "usage: $0 cell <index|replace|unindex> <batch> <flat-cpu|hnsw-cpu>"
  lane_is_idle
  release_profile_only
  local dir
  dir="$(build_dir)"
  run_cell "$dir" "$1" "$2" "$3"
}

cell_is_green() {
  local json="$1"
  [ -s "$json" ] && python3 -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["exit"] == 0 else 1)' "$json"
}

cmd_matrix() {
  lane_is_idle
  release_profile_only
  local dir cell endpoint batch backend red=0 skipped=0 code
  dir="$(build_dir)"
  for cell in "${MATRIX_CELLS[@]}"; do
    if cell_is_green "$dir/cells/$cell.json"; then
      printf 'SKIP %s already green\n' "$cell"
      skipped=$((skipped + 1))
      continue
    fi
    endpoint="${cell%%-*}"
    backend="${cell#*-*-}"
    batch="${cell#*-}"; batch="${batch%%-*}"
    # `run_cell` re-enables errexit internally, so a bare `set +e` here does
    # not survive the call; an `if` test is the one context where a failing
    # cell cannot abort the loop.
    if run_cell "$dir" "$endpoint" "$batch" "$backend"; then code=0; else code=$?; fi
    [ "$code" -eq 0 ] || red=$((red + 1))
  done
  python3 - "$dir" "$red" "$skipped" <<'PY'
import glob, json, os, sys
directory, red, skipped = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
cells = {}
for path in sorted(glob.glob(os.path.join(directory, "cells", "*.json"))):
    receipt = json.load(open(path))
    cells[receipt["cell"]] = {k: receipt[k] for k in ("exit", "duration_s", "failure_bundle", "log")}
summary = {
    "mode": "diagnostic", "acceptance": False,
    "image": open(os.path.join(directory, "image-id.txt")).read().strip(),
    "source_sha256": open(os.path.join(directory, "source.sha256")).read().split()[0],
    "cells": cells, "green": sum(1 for c in cells.values() if c["exit"] == 0),
    "red_this_run": red, "skipped_this_run": skipped,
}
with open(os.path.join(directory, "summary.json"), "w") as f:
    json.dump(summary, f, indent=2, sort_keys=True); f.write("\n")
PY
  printf 'MATRIX exit=%s red=%s summary=%s\n' "$(( red > 0 ))" "$red" "$dir/summary.json"
  [ "$red" -eq 0 ]
}

qualify_env() {
  release_profile_only
  : "${LUMEN_PERF_REPOSITORY:=faberline/lumen}"
  [[ "${LUMEN_PERF_IMAGE:-}" =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]] \
    || die "LUMEN_PERF_IMAGE must be the candidate repo@sha256:<64hex>"
  [[ "${LUMEN_PERF_RUN_ID:-}" =~ ^[1-9][0-9]*$ ]] || die "LUMEN_PERF_RUN_ID must be the candidate run ID"
  [[ "${LUMEN_PERF_RUN_ATTEMPT:-}" =~ ^[1-9][0-9]*$ ]] || die "LUMEN_PERF_RUN_ATTEMPT must be the candidate run attempt"
  [[ "${LUMEN_PERF_COMMIT:-}" =~ ^[0-9a-f]{40}$ ]] || die "LUMEN_PERF_COMMIT must be a full commit SHA"
  local head dirty
  head="$(cd "$REPO_ROOT" && "${GIT[@]}" rev-parse HEAD)"
  [ "$head" = "$LUMEN_PERF_COMMIT" ] || die "checkout HEAD $head is not LUMEN_PERF_COMMIT $LUMEN_PERF_COMMIT"
  dirty="$(cd "$REPO_ROOT" && "${GIT[@]}" status --porcelain)"
  [ -z "$dirty" ] || die "checkout has local changes; qualifying cells compile the candidate commit only"
  QUALIFY_DIR="$ROOT/qualify/$LUMEN_PERF_RUN_ID-$LUMEN_PERF_RUN_ATTEMPT"
  mkdir -p "$QUALIFY_DIR/perf" "$QUALIFY_DIR/logs"
  QUALIFY_DIR="$(cd "$QUALIFY_DIR" && pwd -P)"
  docker pull --quiet "$LUMEN_PERF_IMAGE" >/dev/null || die "cannot pull $LUMEN_PERF_IMAGE"
}

run_qualifying_cell() {
  # run_qualifying_cell <endpoint> <batch> <backend>
  local endpoint="$1" batch="$2" backend="$3"
  local cell="$endpoint-$batch-$backend" receipt log started ended code bundle
  receipt="$QUALIFY_DIR/perf/$cell.json"
  log="$QUALIFY_DIR/logs/$cell.log"
  [ ! -e "$receipt" ] || die "$receipt already exists"
  started="$(epoch)"
  set +e
  (
    cd "$REPO_ROOT" && \
    env -u LUMEN_PERF_DIAGNOSTIC -u LUMEN_PERF_RECOVERY_OBSERVATION_SECS TMPDIR="$QUALIFY_DIR/logs" \
      LUMEN_PERF_QUALIFYING_CELL=1 \
      LUMEN_RECOVERY_PROFILE=1 \
      LUMEN_PERF_IMAGE="$LUMEN_PERF_IMAGE" \
      LUMEN_PERF_REPOSITORY="$LUMEN_PERF_REPOSITORY" \
      LUMEN_PERF_RUN_ID="$LUMEN_PERF_RUN_ID" \
      LUMEN_PERF_RUN_ATTEMPT="$LUMEN_PERF_RUN_ATTEMPT" \
      LUMEN_PERF_COMMIT="$LUMEN_PERF_COMMIT" \
      LUMEN_PERF_RECEIPT_PATH="$receipt" \
      LUMEN_PERF_ENDPOINT="$endpoint" \
      LUMEN_PERF_BATCH="$batch" \
      LUMEN_PERF_BACKEND="$backend" \
      cargo test --release --locked -p lumen --test it -- \
        --ignored --exact "$WORKLOAD_TEST" --test-threads=1 --nocapture
  ) >"$log" 2>&1
  code=$?
  set -e
  ended="$(epoch)"
  if [ "$code" -eq 0 ] && { [ ! -f "$receipt" ] || [ -L "$receipt" ]; }; then
    printf 'cell %s exited 0 without a regular receipt file\n' "$cell" >&2
    code=1
  fi
  if [ "$code" -ne 0 ] && [ -e "$receipt" ]; then
    mv "$receipt" "$QUALIFY_DIR/logs/$cell.rejected-$(now_utc).json"
  fi
  bundle="$(grep -o 'PERF_FAILURE_EVIDENCE path=[^ ]*' "$log" | tail -n1 | cut -d= -f2- || true)"
  printf 'QUALIFY %s exit=%s duration_s=%s bundle=%s\n' "$cell" "$code" "$((ended - started))" "${bundle:--}"
  return "$code"
}

verify_qualifying_dir() {
  local summary="$QUALIFY_DIR/durable-perf-summary.json"
  [ ! -e "$summary" ] || die "$summary already exists"
  python3 -I "$SCRIPT_DIR/verify-durable-perf.py" \
    --receipts-dir "$QUALIFY_DIR/perf" \
    --repo "$LUMEN_PERF_REPOSITORY" --run-id "$LUMEN_PERF_RUN_ID" --run-attempt "$LUMEN_PERF_RUN_ATTEMPT" \
    --commit "$LUMEN_PERF_COMMIT" --image "$LUMEN_PERF_IMAGE" \
    --output "$summary"
  printf 'EVIDENCE dir=%s summary_sha256=%s\n' "$QUALIFY_DIR" "$(cut -d' ' -f1 "$summary.sha256")"
}

cmd_qualify() {
  [ $# -eq 3 ] || die "usage: $0 qualify <index|replace|unindex> <batch> <flat-cpu|hnsw-cpu>"
  lane_is_idle
  qualify_env
  run_qualifying_cell "$1" "$2" "$3"
}

cmd_qualify_matrix() {
  lane_is_idle
  qualify_env
  local cell endpoint batch backend red=0 code
  for cell in "${MATRIX_CELLS[@]}"; do
    if [ -f "$QUALIFY_DIR/perf/$cell.json" ]; then
      printf 'SKIP %s already has a qualifying receipt\n' "$cell"
      continue
    fi
    endpoint="${cell%%-*}"
    backend="${cell#*-*-}"
    batch="${cell#*-}"; batch="${batch%%-*}"
    if run_qualifying_cell "$endpoint" "$batch" "$backend"; then code=0; else code=$?; fi
    [ "$code" -eq 0 ] || red=$((red + 1))
  done
  if [ "$red" -ne 0 ]; then
    printf 'QUALIFY-MATRIX exit=1 red=%s dir=%s\n' "$red" "$QUALIFY_DIR"
    return 1
  fi
  verify_qualifying_dir
  printf 'QUALIFY-MATRIX exit=0 red=0 dir=%s\n' "$QUALIFY_DIR"
}

cmd_status() {
  local dir
  dir="$(build_dir)"
  printf 'build %s\nimage %s\n' "$dir" "$(cat "$dir/image-id.txt")"
  python3 - "$dir" "${MATRIX_CELLS[@]}" <<'PY'
import json, os, sys
directory, cells = sys.argv[1], sys.argv[2:]
print(f"{'cell':<22}{'exit':>5}{'duration_s':>12}  bundle")
for cell in cells:
    path = os.path.join(directory, "cells", cell + ".json")
    if not os.path.exists(path):
        print(f"{cell:<22}{'-':>5}{'-':>12}  -")
        continue
    r = json.load(open(path))
    print(f"{cell:<22}{r['exit']:>5}{r['duration_s']:>12}  {r['failure_bundle'] or '-'}")
PY
}

main() {
  local cmd="${1:-}"
  [ $# -gt 0 ] && shift
  case "$cmd" in
    build) cmd_build "$@" ;;
    cell) cmd_cell "$@" ;;
    matrix)
      if command -v caffeinate >/dev/null && [ -z "${LUMEN_PERF_NO_CAFFEINATE:-}" ]; then
        exec caffeinate -dimsu env LUMEN_PERF_NO_CAFFEINATE=1 "$0" matrix "$@"
      fi
      cmd_matrix "$@" ;;
    status) cmd_status "$@" ;;
    qualify) cmd_qualify "$@" ;;
    qualify-matrix)
      if command -v caffeinate >/dev/null && [ -z "${LUMEN_PERF_NO_CAFFEINATE:-}" ]; then
        exec caffeinate -dimsu env LUMEN_PERF_NO_CAFFEINATE=1 "$0" qualify-matrix "$@"
      fi
      cmd_qualify_matrix "$@" ;;
    *) sed -n '2,54p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
  esac
}
main "$@"
