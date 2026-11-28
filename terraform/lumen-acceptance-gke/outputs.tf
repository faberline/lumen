output "project_id" { value = var.project_id }
output "gke_zone" { value = var.gke_zone }
output "cluster_name" { value = google_container_cluster.acceptance.name }
output "node_pool_name" { value = google_container_node_pool.acceptance.name }
output "node_service_account_email" { value = google_service_account.nodes.email }
output "run_id" { value = var.run_id }
output "owner_label" { value = local.owner_label }
