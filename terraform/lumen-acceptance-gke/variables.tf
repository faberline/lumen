variable "project_id" {
  type        = string
  description = "GCP project used only for this disposable acceptance cluster."
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id)) && !contains(["replace-with-real-project-id", "example-project", "placeholder-project", "your-project-id", "required-project-id"], var.project_id)
    error_message = "project_id must be a real-looking GCP project ID, not a placeholder."
  }
}

variable "region" {
  type = string
  validation {
    condition     = can(regex("^[a-z]+-[a-z0-9]+[0-9]$", var.region))
    error_message = "region must be a valid-looking GCP region."
  }
}

variable "gke_zone" {
  type = string
  validation {
    condition     = can(regex("^[a-z]+-[a-z0-9]+[0-9]-[a-z]$", var.gke_zone)) && startswith(var.gke_zone, "${var.region}-")
    error_message = "gke_zone must be a zonal location in the selected region."
  }
}

variable "run_id" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{0,39}[a-z0-9]$", var.run_id)) && !contains(["run", "test", "placeholder"], var.run_id)
    error_message = "run_id must be a non-placeholder lowercase run identifier."
  }
}

variable "machine_type" {
  type        = string
  default     = "e2-standard-8"
  description = "Node machine type: 8 vCPU and 32 GiB hold one pod of 2.5 CPU and 16 GiB with room for system pods."
  validation {
    condition     = can(regex("^[a-z][a-z0-9]*-[a-z0-9-]+$", var.machine_type))
    error_message = "machine_type must be a valid-looking GCE machine type."
  }
}
