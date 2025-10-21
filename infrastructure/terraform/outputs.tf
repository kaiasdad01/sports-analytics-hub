output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    raw       = google_bigquery_dataset.nfl_raw.dataset_id
    staging   = google_bigquery_dataset.nfl_staging.dataset_id
    analytics = google_bigquery_dataset.nfl_analytics.dataset_id
    ml        = google_bigquery_dataset.nfl_ml.dataset_id
  }
}

output "service_account_email" {
  description = "Service account email"
  value       = google_service_account.nfl_analytics.email
}

output "credentials_path" {
  description = "Path to service account credentials"
  value       = local_file.service_account_key.filename
}

