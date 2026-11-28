#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/lumen-tf-check.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT
export TF_DATA_DIR="$tmp_dir/terraform-data"

terraform -chdir="$module_dir" fmt -check -recursive -diff
terraform -chdir="$module_dir" init -backend=false -input=false -lockfile=readonly -no-color >/dev/null
terraform -chdir="$module_dir" validate -no-color
terraform -chdir="$module_dir" test -no-color

check_contract() {
  local root="$1"
  local cluster_file="$root/main.tf"
  has_match() {
    local pattern="$1"
    local file
    while IFS= read -r -d '' file; do
      grep -Eq "$pattern" "$file" && return 0
    done < <(find "$root" -type f -name '*.tf' -print0)
    return 1
  }
  count_matches() {
    local pattern="$1"
    find "$root" -type f -name '*.tf' -exec grep -Eoh "$pattern" {} + | wc -l | tr -d ' '
  }
  for required in \
    'remove_default_node_pool[[:space:]]*=[[:space:]]*true' \
    'initial_node_count[[:space:]]*=[[:space:]]*1' \
    'channel = "REGULAR"' \
    'workload_identity_config' \
    'ip_allocation_policy[[:space:]]*\{\}' \
    'gce_persistent_disk_csi_driver_config' \
    'enable_components = \["SYSTEM_COMPONENTS", "WORKLOADS"\]' \
    'machine_type[[:space:]]*=[[:space:]]*var\.machine_type' \
    'max_node_count[[:space:]]*=[[:space:]]*1' \
    'disk_size_gb[[:space:]]*=[[:space:]]*100' \
    'disk_type[[:space:]]*=[[:space:]]*"pd-balanced"' \
    'deletion_protection[[:space:]]*=[[:space:]]*false' \
    'lumen-ac-\$\{local\.run_hash\}' \
    'lumen-acnp-\$\{local\.run_hash\}' \
    'lumen-acn-\$\{local\.run_hash\}' \
    'lumen-acceptance-\$\{local\.run_hash\}' \
    'oauth_scopes[[:space:]]*=[[:space:]]*\["https://www.googleapis.com/auth/cloud-platform"\]' \
    'disable-legacy-endpoints = "true"' \
    'mode = "GKE_METADATA"'; do
    grep -Eq "$required" "$cluster_file" || { echo "missing contract: $required" >&2; return 1; }
  done
  if has_match 'provider[[:space:]]+"kubernetes"|provider[[:space:]]+"tls"|google_gke_hub_|google_storage_bucket|google_container_registry|google_artifact_registry|network_policy|taint[[:space:]]*\{|credential|access_token|private_key|impersonat|workload_identity_user|roles/iam.workloadIdentityUser|shard|replica|backend[[:space:]]+"|terraform_remote_state'; then
    echo "forbidden provider, resource, backend, credential, auth, or scope found" >&2
    return 1
  fi
  if [ "$(count_matches '^resource ')" -ne 4 ]; then return 1; fi
  if [ "$(count_matches '^resource "google_container_cluster"')" -ne 1 ]; then return 1; fi
  if [ "$(count_matches '^resource "google_container_node_pool"')" -ne 1 ]; then return 1; fi
  if [ "$(count_matches '^resource "google_service_account"')" -ne 1 ]; then return 1; fi
  if [ "$(count_matches '^resource "google_project_iam_member"')" -ne 1 ]; then return 1; fi
  if [ "$(count_matches 'role[[:space:]]*=[[:space:]]*"roles/container.defaultNodeServiceAccount"')" -ne 1 ]; then return 1; fi
  local expected_outputs='project_id gke_zone cluster_name node_pool_name node_service_account_email run_id owner_label'
  if [ "$(count_matches '^output ')" -ne 7 ]; then return 1; fi
  for output in $expected_outputs; do
    has_match "^output \"$output\"" || { echo "missing output: $output" >&2; return 1; }
  done
}

check_contract "$module_dir"

# Prove the rejection oracle without changing repository bytes.
mutation_dir="$tmp_dir/mutated-module"
mkdir -p "$mutation_dir"
cp "$module_dir"/*.tf "$mutation_dir/"
printf '\nresource "google_container_node_pool" "mutation" {}\n' >> "$mutation_dir/main.tf"
if check_contract "$mutation_dir"; then
  echo "negative mutation was not rejected" >&2
  exit 1
fi

# Also prove that removing a required contract token is rejected.
missing_mutation_dir="$tmp_dir/missing-contract-module"
mkdir -p "$missing_mutation_dir"
cp "$module_dir"/*.tf "$missing_mutation_dir/"
sed -i.bak '/channel = "REGULAR"/d' "$missing_mutation_dir/main.tf"
rm -f "$missing_mutation_dir/main.tf.bak"
if check_contract "$missing_mutation_dir"; then
  echo "missing-contract mutation was not rejected" >&2
  exit 1
fi

# Prove that a remote backend in the module is rejected.
backend_mutation_dir="$tmp_dir/backend-module"
mkdir -p "$backend_mutation_dir"
cp "$module_dir"/*.tf "$backend_mutation_dir/"
printf '\nterraform { backend "gcs" {} }\n' >> "$backend_mutation_dir/main.tf"
if check_contract "$backend_mutation_dir"; then
  echo "backend mutation was not rejected" >&2
  exit 1
fi

echo "terraform acceptance GKE checks passed (cloud-free)"
