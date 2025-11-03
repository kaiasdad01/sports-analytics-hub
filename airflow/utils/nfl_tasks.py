"""
Airflow task functions for NFL data pipeline.
"""
import sys
sys.path.insert(0, '/opt/airflow/nfl_v3')

from ingestion.nfl.extractor import NFLExtractor
from ingestion.scrapers.nfl_fines_scraper import NFLFinesScraper
from ingestion.storage.gcs_writer import GCSWriter
from ingestion.config import get_gcs_config


def extract_nfl_data(data_type, seasons=None, **context):
    """
    Extract NFL data from various sources identified in ~/docs/source-data and write to GCS

    Args:
        data_type: Type of NFL data to extract (e.g., 'pbp', 'player_stats', 'rosters')
        seasons: List of season years. If None, uses the default season selection.
        **context: Airflow context dictionary

    Returns:
        str: GCS URI where the data was written
    """
    config = get_gcs_config()
    extractor = NFLExtractor()
    gcs_writer = GCSWriter(
        bucket_name=config.bucket_name,
        project_id=config.project_id
    )

    nfl_df, gcs_uri = extractor.extract_write_gcs(
        data_type=data_type,
        gcs_writer=gcs_writer,
        seasons=seasons
    )
    return gcs_uri


def scrape_fines(**context):
    """
    Run NFL.com fines scraper

    Args:
        **context: Airflow context dictionary

    Returns:
        str: GCS URI where the data was written
    """
    config = get_gcs_config()
    gcs_writer = GCSWriter(
        bucket_name=config.bucket_name,
        project_id=config.project_id
    )

    scraper = NFLFinesScraper()
    gcs_uri = scraper.scrape_and_write(gcs_writer=gcs_writer)
    return gcs_uri

def load_to_bigquery(gcs_uri, table_name, **context):

    from ingestion.storage.gcs_to_bq_loader import GCSToBigQueryLoader
    from ingestion.config import get_bigquery_config

    config = get_gcs_config()
    bq_config = get_bigquery_config()

    bq_loader = GCSToBigQueryLoader(project_id=bq_config.project_id)

    table_id = f"{config.project_id}.{bq_config.raw_dataset}.{table_name}"

    bq_loader.load_from_gcs(
        gcs_uri=gcs_uri,
        table_id=table_id,
        write_mode='replace'
    )


def run_dbt(command='run', select=None, **context):
    """
    Execute dbt command in the dbt_project directory.

    Args:
        command: dbt command to run (default: 'run'). Options: 'run', 'test', 'build', etc.
        select: Optional model selection (e.g., 'marts', 'staging', 'models/staging/nfl/*')
        **context: Airflow context dictionary

    Returns:
        int: Exit code (0 for success)
    """
    import subprocess
    import os

    dbt_project_dir = '/opt/airflow/dbt_project'
    profiles_dir = '/opt/airflow/dbt_project'

    # Build dbt command
    dbt_cmd = ['dbt', command]
    
    # Add selection if provided
    if select:
        dbt_cmd.extend(['--select', select])
    
    # Add project and profiles directory flags
    dbt_cmd.extend(['--project-dir', dbt_project_dir])
    dbt_cmd.extend(['--profiles-dir', profiles_dir])

    # Change to dbt project directory and execute
    result = subprocess.run(
        dbt_cmd,
        cwd=dbt_project_dir,
        env=os.environ.copy(),
        capture_output=True,
        text=True
    )

    # Print output for Airflow logs
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # Raise exception if command failed
    if result.returncode != 0:
        raise Exception(f"dbt {command} failed with exit code {result.returncode}")

    return result.returncode
