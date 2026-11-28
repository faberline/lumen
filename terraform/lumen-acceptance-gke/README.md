# Lumen acceptance GKE cluster

This module makes one disposable zonal GKE cluster for one Lumen pod of
2.5 CPU and 16 GiB.

What it makes:

- one zonal cluster `lumen-ac-<run_hash>`;
- one node pool `lumen-acnp-<run_hash>` with one `e2-standard-8` node
  (the `machine_type` variable sets it);
- one node service account `lumen-acn-<run_hash>`;
- one IAM member: `roles/container.defaultNodeServiceAccount` for that account.

`<run_hash>` is the first 10 characters of the SHA-256 of `run_id`.

Every labelable resource carries the label `lumen-owner` with the value
`lumen-acceptance-<run_hash>`.

State is local to the run. The module has no `backend` block and no
`terraform_remote_state`. The controller sets a per-run `TF_DATA_DIR` and
`-state` path.

The cluster costs money while it exists. The controller must always destroy
it, also after a failed run.

Check the module without any cloud access:

```sh
terraform/lumen-acceptance-gke/scripts/check.sh
```

This module has no run record yet. It was not applied to any project.
