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

