#!/usr/bin/env bash
# Print the GKE suites that a list of changed paths needs (issue #457).
#
# Reads one repository-relative path per line on stdin. Prints one line of
# compact JSON, for example ["overlay","operator"], or [] when every path
# needs no GKE suite. Empty input selects both suites (fail safe).
set -euo pipefail

overlay=0
operator=0
seen=0

while IFS= read -r path || [[ -n "$path" ]]; do
  [[ -n "$path" ]] || continue
  seen=1
  case "$path" in
    # Rule D, checked first so that the selector itself is never ignored.
    scripts/select-gke-suites.sh)
      overlay=1
      operator=1
      ;;
    # Rule A: no GKE suite needed.
    docs/* | clients/* | examples/* | observability/* | terraform/* | release-evidence/* | proptest-regressions/* | k8s/gke-perf-job/* | k8s/overlays/gke-perf/* | *.md | LICENSE | llms.txt | .github/claude-review-cli-supply.json | .github/workflows/gke-perf.yml | .github/workflows/lumen-release.yml | .github/workflows/lumen-release-candidate.yml | scripts/*)
      ;;
    # Rule B: operator suite only.
    crates/lumen-operator/* | k8s/operator/* | k8s/gke-test-job/operator/* | crates/lumen/tests/it/e2e/operator.rs)
      operator=1
      ;;
    # Rule C: overlay suite only.
    k8s/gke-test-job/overlay/* | crates/lumen/tests/it/e2e/overlay.rs)
      overlay=1
      ;;
    # Rule D: everything else needs both suites.
    *)
      overlay=1
      operator=1
      ;;
  esac
done

if (( seen == 0 )); then
  overlay=1
  operator=1
fi

suites=()
if (( overlay == 1 )); then
  suites+=("overlay")
fi
if (( operator == 1 )); then
  suites+=("operator")
fi

if (( ${#suites[@]} == 0 )); then
  echo '[]'
  exit 0
fi

out='['
sep=''
for suite in "${suites[@]}"; do
  out+="${sep}\"${suite}\""
  sep=','
done
echo "${out}]"
