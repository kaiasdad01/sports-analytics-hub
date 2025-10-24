"""Configuration for data ingestion."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Dataset name constants
DEFAULT_RAW_DATASET = "nfl_raw"
DEFAULT_STAGING_DATASET = "nfl_staging"
DEFAULT_ANALYTICS_DATASET = "nfl_analytics"
DEFAULT_ML_DATASET = "nfl_ml"

# Ingestion configuration constants
DEFAULT_BATCH_SIZE = 10000
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 5


@dataclass
class BigQueryConfig:
    """BigQuery configuration settings."""
    
    project_id: str
    credentials_path: str
    raw_dataset: str
    staging_dataset: str
    analytics_dataset: str
    ml_dataset: str
    location: str = "US"
    
    @classmethod
    def from_env(cls) -> "BigQueryConfig":
        """Load configuration from environment variables."""
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID", ""),
            credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
            raw_dataset=DEFAULT_RAW_DATASET,
            staging_dataset=DEFAULT_STAGING_DATASET,
            analytics_dataset=DEFAULT_ANALYTICS_DATASET,
            ml_dataset=DEFAULT_ML_DATASET,
        )


@dataclass
class IngestionConfig:
    """General ingestion configuration."""
    
    batch_size: int = DEFAULT_BATCH_SIZE
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: int = DEFAULT_RETRY_DELAY
    
    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """Load configuration from environment variables."""
        return cls(
            batch_size=int(os.getenv("INGESTION_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
            max_retries=int(os.getenv("INGESTION_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))),
            retry_delay=int(os.getenv("INGESTION_RETRY_DELAY", str(DEFAULT_RETRY_DELAY))),
        )


def get_bigquery_config() -> BigQueryConfig:
    """Get BigQuery configuration."""
    return BigQueryConfig.from_env()

@dataclass
class GCSConfig:

    project_id: str
    bucket_name: str

    @classmethod
    def from_env(cls) -> "GCSConfig":
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID", ""),
            bucket_name=os.getenv("GCS_RAW_BUCKET", ""),
        )
    
    def get_raw_path(self, source: str, data_type: str, **partition_keys) -> str:
        """
        Build GCS path for raw data with partitioning.

        Args:
            source: Data source name (like 'nflreadpy')
            data_type: Type of data (pbp, schedules, etc.)
            **partition_keys: Partition keys (e.g., season=2025)

        Returns:
            GCS path like: raw/nfl/play_by_play/season=2025
        """
        path_parts = ["raw", source, data_type]

        for key, value in sorted(partition_keys.items()):
            path_parts.append(f"{key}={value}")

        return "/".join(path_parts)
    
def get_gcs_config() -> GCSConfig:
    """Get GCS Config"""
    return GCSConfig.from_env()

def get_ingestion_config() -> IngestionConfig:
    """Get ingestion configuration."""
    return IngestionConfig.from_env()