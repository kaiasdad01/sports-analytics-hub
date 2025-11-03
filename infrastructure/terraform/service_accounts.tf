resource "google_service_account" "nfl_analytics" {
  account_id   = "nfl-analytics-${var.environment}"
  display_name = "NFL Analytics Service Account"
  description  = "Service account for NFL analytics platform"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.nfl_analytics.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.nfl_analytics.email}"
}

resource "google_service_account_key" "nfl_analytics_key" {
  service_account_id = google_service_account.nfl_analytics.name
}

resource "local_file" "service_account_key" {
  content  = base64decode(google_service_account_key.nfl_analytics_key.private_key)
  filename = "${path.module}/../../credentials/gcp-service-account.json"
  file_permission = "0600"
}

# Service account specifically for Composer DAG execution
resource "google_service_account" "composer_worker" {
  account_id   = "composer-worker-${var.environment}"
  display_name = "Composer Worker Service Account"
  description  = "Minimal permissions service account for Cloud Composer DAG tasks"
}

# Only the permissions your DAGs actually need
resource "google_project_iam_member" "composer_storage_object_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"  # Read from GCS bucket
  member  = "serviceAccount:${google_service_account.composer_worker.email}"
}

resource "google_project_iam_member" "composer_bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"  # Write to BigQuery tables
  member  = "serviceAccount:${google_service_account.composer_worker.email}"
}

resource "google_project_iam_member" "composer_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"  # Run BigQuery load jobs
  member  = "serviceAccount:${google_service_account.composer_worker.email}"
}