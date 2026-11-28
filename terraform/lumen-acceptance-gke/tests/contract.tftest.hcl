mock_provider "google" {}

run "acceptance_cluster_contract" {
  command = plan

  variables {
    project_id = "axiom-test-12345"
    region     = "us-central1"
    gke_zone   = "us-central1-a"
    run_id     = "acceptance-20260828-01"
  }

  assert {
    condition     = startswith(google_container_cluster.acceptance.name, "lumen-ac-")
    error_message = "cluster name must start with lumen-ac-"
  }
  assert {
    condition     = startswith(google_container_node_pool.acceptance.name, "lumen-acnp-")
    error_message = "node pool name must start with lumen-acnp-"
  }
  assert {
    condition     = startswith(google_service_account.nodes.account_id, "lumen-acn-")
    error_message = "node service account id must start with lumen-acn-"
  }
  assert {
    condition     = google_container_cluster.acceptance.name == "lumen-ac-be54da124b"
    error_message = "cluster name must carry the run hash of acceptance-20260828-01"
  }
  assert {
    condition     = google_container_cluster.acceptance.deletion_protection == false
    error_message = "cluster must be disposable"
  }
  assert {
    condition     = google_container_cluster.acceptance.location == "us-central1-a"
    error_message = "cluster must be zonal in gke_zone"
  }
  assert {
    condition     = google_container_node_pool.acceptance.node_count == 1
    error_message = "there must be exactly one node"
  }
  assert {
    condition     = google_container_node_pool.acceptance.autoscaling[0].min_node_count == 1 && google_container_node_pool.acceptance.autoscaling[0].max_node_count == 1
    error_message = "node pool autoscaling must be fixed at one node"
  }
  assert {
    condition     = google_container_node_pool.acceptance.node_config[0].machine_type == "e2-standard-8"
    error_message = "node pool must use the default machine_type e2-standard-8"
  }
  assert {
    condition     = google_container_node_pool.acceptance.node_config[0].disk_size_gb == 100
    error_message = "node disk must be 100 GB"
  }
  assert {
    condition     = google_container_node_pool.acceptance.node_config[0].disk_type == "pd-balanced"
    error_message = "node disk must be pd-balanced"
  }
  assert {
    condition     = google_container_cluster.acceptance.resource_labels["lumen-owner"] == "lumen-acceptance-be54da124b"
    error_message = "cluster ownership label must be the run scoped owner label"
  }
  assert {
    condition     = google_container_node_pool.acceptance.node_config[0].labels["lumen-owner"] == "lumen-acceptance-be54da124b"
    error_message = "node ownership label must be the run scoped owner label"
  }
  assert {
    condition     = output.owner_label == "lumen-acceptance-be54da124b"
    error_message = "owner_label output must carry the run hash"
  }
}

run "run_a_names" {
  command = plan

  variables {
    project_id = "axiom-test-12345"
    region     = "us-central1"
    gke_zone   = "us-central1-a"
    run_id     = "acceptance-a-01"
  }

  assert {
    condition     = google_container_cluster.acceptance.name == "lumen-ac-f639f31134"
    error_message = "run a cluster name must carry its run hash"
  }
}

run "run_b_names_differ" {
  command = plan

  variables {
    project_id = "axiom-test-12345"
    region     = "us-central1"
    gke_zone   = "us-central1-a"
    run_id     = "acceptance-b-01"
  }

  assert {
    condition     = google_container_cluster.acceptance.name == "lumen-ac-7b67459c01"
    error_message = "run b cluster name must carry its run hash"
  }
  assert {
    condition     = google_container_cluster.acceptance.name != "lumen-ac-f639f31134"
    error_message = "two run ids must give two cluster names"
  }
}

run "reject_placeholder_run_id" {
  command = plan

  variables {
    project_id = "axiom-test-12345"
    region     = "us-central1"
    gke_zone   = "us-central1-a"
    run_id     = "run"
  }

  expect_failures = [var.run_id]
}

run "reject_placeholder_project" {
  command = plan

  variables {
    project_id = "replace-with-real-project-id"
    region     = "us-central1"
    gke_zone   = "us-central1-a"
    run_id     = "acceptance-20260828-01"
  }

  expect_failures = [var.project_id]
}
