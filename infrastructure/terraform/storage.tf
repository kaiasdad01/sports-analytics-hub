resource "google_storage_bucket" "nfl_raw_data" {
  name          = "nfl-analytics-${var.environment}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
        age = 90
    }
    action {
        type            = "SetStorageClass"
        storage_class   = "COLDLINE"
    }
  }

  labels = {
    environment     = var.environment 
    purpose         = "raw-data-lake"
    managed_by      = "terraform"
  }
}

resource "google_storage_bucket_iam_member" "nfl_analytics_bucket_admin" {
    bucket = google_storage_bucket.nfl_raw_data.name
    role   = "roles/storage.objectAdmin"
    member = "serviceAccount:${google_service_account.nfl_analytics.email}"
}