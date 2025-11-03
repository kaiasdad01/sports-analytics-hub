from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys

sys.path.insert(0, '/opt/airflow/nfl_v3')
sys.path.insert(0, '/opt/airflow/nfl_v3/airflow')

from utils.nfl_tasks import load_to_bigquery

# Map dataset types to their file extensions
EXTENSION_BY_DATASET = {
    'fines': '.ndjson',
    # All other datasets use .parquet (default)
}

def find_latest_gcs_uri(source: str, data_type: str, season: int | None = None) -> str:
    """
    Find the latest GCS file for a given data type.

    Args:
        source: Data source name (e.g., 'nfl')
        data_type: Type of data (e.g., 'fines', 'schedules', 'pbp')
        season: Optional season year for partitioning

    Returns:
        GCS URI of the latest file matching the expected extension
    """
    # Late import to avoid module path issues in scheduler
    from ingestion.config import get_gcs_config
    from google.cloud import storage

    config = get_gcs_config()
    prefix_kwargs = {'season': season} if season is not None else {}
    prefix = config.get_raw_path(source, data_type, **prefix_kwargs)

    client = storage.Client(project=config.project_id)
    bucket = client.bucket(config.bucket_name)

    # Get expected file extension for this dataset
    expected_ext = EXTENSION_BY_DATASET.get(data_type, '.parquet')

    # List blobs under prefix and pick the newest file with correct extension
    newest = None
    for blob in client.list_blobs(bucket, prefix=prefix):
        if not blob.name.endswith(expected_ext):
            continue
        if newest is None or blob.updated > newest.updated:
            newest = blob

    if not newest:
        raise FileNotFoundError(
            f"No {expected_ext} file found under gs://{config.bucket_name}/{prefix}"
        )

    return f"gs://{config.bucket_name}/{newest.name}"

def load_latest(data_type: str, season: int | None = None, **context):
    gcs_uri = find_latest_gcs_uri(source='nfl', data_type=data_type, season=season)
    return load_to_bigquery(gcs_uri=gcs_uri, table_name=data_type, **context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nfl_data_load',
    default_args=default_args,
    description='Load latest raw files from GCS into BigQuery',
    schedule_interval='0 6 * * 5',  # run after extract; adjust as needed
    catchup=False,
    tags=['nfl', 'load'],
)

# wait_for_extract = ExternalTaskSensor(
#     task_id='wait_for_extract',
#     external_dag_id='nfl_data_pipeline',
#     allowed_states=['success'],
#     mode='reschedule',
#     poke_interval=60,
#     timeout=6 * 60 * 60,
#     dag=dag,
# )

DATASETS = [
    'schedules', 'pbp',
    'rosters', 'rosters_weekly', 'depth_charts', 'trades', 'players', 'teams',
    'player_stats', 'snap_counts', 'nextgen_stats', 'participation',
    'officials', 'combine', 'draft_picks', 'contracts',
    'ff_playerids', 'ff_opportunity', 'ff_rankings',
    'fines',
]

for name in DATASETS:
    t = PythonOperator(
        task_id=f'load_{name}',
        python_callable=load_latest,
        op_kwargs={'data_type': name},
        dag=dag,
    )
    # wait_for_extract >> t
