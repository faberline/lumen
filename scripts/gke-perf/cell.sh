#!/usr/bin/env bash
# One qualifying durable-performance cell in the client Job of
# k8s/gke-perf-job. It runs the same test with the same environment as
# `scripts/durable-perf-local.sh qualify`, against the attached pod
# (LUMEN_PERF_RUNTIME=kubernetes) instead of a local container.
#
# The pod log is the only thing the aw run keeps, so the outputs go to stdout
# between markers, as base64 lines of 76 characters:
#   LUMEN-PERF-RECEIPT-BEGIN cell=<cell> sha256=<hex> bytes=<n>
#   ...
#   LUMEN-PERF-RECEIPT-END cell=<cell>
# A failed cell prints LUMEN-PERF-FAILURE-BEGIN/END instead, around a tar.gz of
# the harness failure evidence and the rejected receipt, if any. Only a cell
# that exits 0 with a regular receipt file prints a receipt.
# scripts/gke-perf/extract-receipts.py reads the blocks back.
#
# Input (ConfigMap gke-test-env): LUMEN_PERF_CELL, LUMEN_PERF_ENDPOINT,
# LUMEN_PERF_BATCH, LUMEN_PERF_BACKEND, LUMEN_PERF_IMAGE, LUMEN_PERF_REPOSITORY,
# LUMEN_PERF_RUN_ID, LUMEN_PERF_RUN_ATTEMPT, LUMEN_PERF_COMMIT. The Job sets
# LUMEN_PERF_RUNTIME, LUMEN_PERF_KUBE_POD, and LUMEN_PERF_KUBE_NAMESPACE.
set -euo pipefail

WORKLOAD_TEST="perf_gate::durable_workload::approved_30_minute_durable_workload"
TEST_BINARY="${LUMEN_PERF_TEST_BINARY:-/usr/local/bin/lumen-perf-it}"
WORK="${LUMEN_PERF_WORK:-/work}"
# Larger evidence would crowd the 10 MiB container log; it is listed instead.
MAX_FAILURE_BYTES=$((4 * 1024 * 1024))

die() { printf 'refused: %s\n' "$*" >&2; exit 2; }

[[ "${LUMEN_PERF_ENDPOINT:-}" =~ ^(index|unindex|replace)$ ]] || die "LUMEN_PERF_ENDPOINT must be index, unindex, or replace"
[[ "${LUMEN_PERF_BATCH:-}" =~ ^[1-9][0-9]*$ ]] || die "LUMEN_PERF_BATCH must be a positive integer"
[[ "${LUMEN_PERF_BACKEND:-}" =~ ^(flat|hnsw)-cpu$ ]] || die "LUMEN_PERF_BACKEND must be flat-cpu or hnsw-cpu"
cell="$LUMEN_PERF_ENDPOINT-$LUMEN_PERF_BATCH-$LUMEN_PERF_BACKEND"
[ "${LUMEN_PERF_CELL:-}" = "$cell" ] || die "LUMEN_PERF_CELL ${LUMEN_PERF_CELL:-<unset>} is not $cell"
[[ "${LUMEN_PERF_IMAGE:-}" =~ ^[^@[:space:]]+@sha256:[0-9a-f]{64}$ ]] || die "LUMEN_PERF_IMAGE must be the candidate repo@sha256:<64hex>"
[[ "${LUMEN_PERF_REPOSITORY:-}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die "LUMEN_PERF_REPOSITORY must be owner/name"
[[ "${LUMEN_PERF_RUN_ID:-}" =~ ^[1-9][0-9]*$ ]] || die "LUMEN_PERF_RUN_ID must be the candidate run ID"
[[ "${LUMEN_PERF_RUN_ATTEMPT:-}" =~ ^[1-9][0-9]*$ ]] || die "LUMEN_PERF_RUN_ATTEMPT must be the candidate run attempt"
[[ "${LUMEN_PERF_COMMIT:-}" =~ ^[0-9a-f]{40}$ ]] || die "LUMEN_PERF_COMMIT must be a full commit SHA"
[ "${LUMEN_PERF_RUNTIME:-}" = kubernetes ] || die "LUMEN_PERF_RUNTIME must be kubernetes"
[ -n "${LUMEN_PERF_KUBE_NAMESPACE:-}" ] || die "LUMEN_PERF_KUBE_NAMESPACE is not set"
pod="${LUMEN_PERF_KUBE_POD:-lumen-perf-0}"
overrides="$(env | cut -d= -f1 | grep '^CARGO_PROFILE_' || true)"
[ -z "$overrides" ] || die "CARGO_PROFILE_* overrides are not allowed: $(printf '%s' "$overrides" | tr '\n' ' ')"

# emit <RECEIPT|FAILURE> <file>
emit() {
  local kind="$1" file="$2" sha bytes
  sha="$(sha256sum "$file" | cut -d' ' -f1)"
  bytes="$(stat -c %s "$file")"
  printf 'LUMEN-PERF-%s-BEGIN cell=%s sha256=%s bytes=%s\n' "$kind" "$cell" "$sha" "$bytes"
  base64 -w 76 "$file"
  printf 'LUMEN-PERF-%s-END cell=%s\n' "$kind" "$cell"
}

tmp="$WORK/tmp"
receipts="$WORK/perf"
mkdir -p "$tmp" "$receipts"
receipt="$receipts/$cell.json"
[ ! -e "$receipt" ] || die "$receipt already exists"

# The rollout waits for readiness already; this covers a pod that turned
# unready between the rollout and the Job.
kubectl -n "$LUMEN_PERF_KUBE_NAMESPACE" wait --for=condition=Ready --timeout=600s "pod/$pod"

started="$(date +%s)"
set +e
env -u LUMEN_PERF_DIAGNOSTIC -u LUMEN_PERF_RECOVERY_OBSERVATION_SECS -u LUMEN_PERF_CELL \
  TMPDIR="$tmp" \
  LUMEN_PERF_QUALIFYING_CELL=1 \
  LUMEN_RECOVERY_PROFILE=1 \
  LUMEN_PERF_RECEIPT_PATH="$receipt" \
  "$TEST_BINARY" --ignored --exact "$WORKLOAD_TEST" --test-threads=1 --nocapture
code=$?
set -e
ended="$(date +%s)"

if [ "$code" -eq 0 ] && { [ ! -f "$receipt" ] || [ -L "$receipt" ]; }; then
  printf 'cell %s exited 0 without a regular receipt file\n' "$cell" >&2
  code=1
fi

if [ "$code" -eq 0 ]; then
  emit RECEIPT "$receipt"
else
  bundle="$WORK/failure-$cell.tar.gz"
  members=()
  [ ! -e "$receipt" ] || members+=("perf/$cell.json")
  while IFS= read -r dir; do members+=("tmp/$dir"); done \
    < <(find "$tmp" -mindepth 1 -maxdepth 1 -name 'lumen-perf-failure-*' -printf '%f\n' | sort)
  if [ "${#members[@]}" -gt 0 ]; then
    tar -czf "$bundle" -C "$WORK" "${members[@]}"
    if [ "$(stat -c %s "$bundle")" -le "$MAX_FAILURE_BYTES" ]; then
      emit FAILURE "$bundle"
    else
      printf 'failure evidence for %s is %s bytes, over %s; not printed:\n' \
        "$cell" "$(stat -c %s "$bundle")" "$MAX_FAILURE_BYTES" >&2
      tar -tzvf "$bundle" >&2
    fi
  fi
fi

printf 'QUALIFY %s exit=%s duration_s=%s\n' "$cell" "$code" "$((ended - started))"
exit "$code"
