resource "google_bigquery_dataset" "nfl_raw" {
  dataset_id                 = "nfl_raw"
  friendly_name              = "NFL Raw Data"
  description                = "Raw data from nflreadpy"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false

  labels = {
    environment = var.environment
    layer       = "raw"
  }
}

resource "google_bigquery_dataset" "nfl_staging" {
  dataset_id                 = "nfl_staging"
  friendly_name              = "NFL Staging"
  description                = "dbt staging models"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false

  labels = {
    environment = var.environment
    layer       = "staging"
  }
}

resource "google_bigquery_dataset" "nfl_analytics" {
  dataset_id                 = "nfl_analytics"
  friendly_name              = "NFL Analytics"
  description                = "dbt analytics marts"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false

  labels = {
    environment = var.environment
    layer       = "analytics"
  }
}

resource "google_bigquery_dataset" "nfl_ml" {
  dataset_id                 = "nfl_ml"
  friendly_name              = "NFL ML Features"
  description                = "Machine learning feature tables"
  location                   = var.bigquery_location
  delete_contents_on_destroy = false

  labels = {
    environment = var.environment
    layer       = "ml"
  }
}

