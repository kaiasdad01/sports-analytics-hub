import logging
import os
import sys

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from ingestion.nfl.extractor import NFLExtractor
from ingestion.storage import GCSWriter, GCSToBigQueryLoader
from ingestion.config import get_bigquery_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# params 
seasons = [2020, 2021, 2022, 2023, 2024, 2025]

# GCS config
GCS_BUCKET = os.getenv("GCS_RAW_BUCKET", "nfl-analytics-dev")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "sports-analytics-475802")

def ingest_all_data():
    """Get key datasets: pbp, schedules, rosters, teams"""
    logger.info("=== Starting Core Data Ingestion ===")

    extractor = NFLExtractor()
    config = get_bigquery_config()

    gcs_writer = GCSWriter(bucket_name=GCS_BUCKET, project_id=PROJECT_ID)
    bq_loader = GCSToBigQueryLoader(project_id=PROJECT_ID)

    datasets = [

        # core game data
        ('pbp', 'play_by_play'),
        ('schedules', 'schedules'),

        # roster data, including context re: injuries, trades, starter changes, etc.
        ('rosters', 'rosters'),
        ('rosters_weekly', 'rosters_weekly'),
        ('depth_charts', 'depth_charts'),
        ('injuries', 'injuries'),
        ('trades', 'trades'),
        ('players', 'players'),

        # player performance data
        ('player_stats', 'player_stats'),
        ('snap_counts', 'snap_counts'),

        # advanced data
        ('nextgen_stats', 'nextgen_stats'),
        ('participation', 'participation'),

        # other stuff
        ('officials', 'officials'),
        ('combine', 'combine'),
        ('draft_picks', 'draft_picks'),
        ('contracts', 'contracts'),

        # fantasy
        ('ff_playerids', 'ff_playerids'),
        ('ff_opportunity', 'ff_opportunity'),
        ('ff_rankings', 'ff_rankings')
    ]

    for data_type, table_name in datasets:
        logger.info(f"Extracting {data_type} for seasons {seasons}")

        try:
            df, gcs_uri = extractor.extract_write_gcs(
                data_type=data_type,
                gcs_writer=gcs_writer,
                seasons=seasons
            )

            if not gcs_uri: 
                logger.warning(f"No data written to GCS for {data_type}, skipping")
                continue

            logger.info(f"Wrote {len(df)} rows to {gcs_uri}")

            logger.info(f"Loading {data_type} to {config.raw_dataset}.{table_name}")
            table_id = f"{PROJECT_ID}.{config.raw_dataset}.{table_name}"
            bq_loader.load_from_gcs(
                gcs_uri=gcs_uri,
                table_id=table_id,
                write_mode='replace'
            )

            # validate
            row_count = bq_loader.get_row_count(table_id)
            logger.info(f"{data_type}: {row_count} rows loaded successfully")
        
        except Exception as e:
            logger.error(f"Failed to load {data_type}: {e}")
    
    # teams data
    logger.info("=== Starting Teams Data Ingestion // NO SEASON PARAM ===")
    try:
        df, gcs_uri = extractor.extract_write_gcs(
            data_type='teams',
            gcs_writer=gcs_writer
        )

        if gcs_uri:
            logger.info(f"Wrote {len(df)} rows to {gcs_uri}")

            logger.info(f"Loading teams to {config.raw_dataset}.teams")
            table_id = f"{PROJECT_ID}.{config.raw_dataset}.teams"
            bq_loader.load_from_gcs(
                gcs_uri=gcs_uri,
                table_id=table_id,
                write_mode='replace'
            )

            # validate
            row_count = bq_loader.get_row_count(table_id)
            logger.info(f"teams: {row_count} rows loaded successfully")
        
    except Exception as e:
        logger.error(f"Failed to load teams: {e}")

    logger.info("=== Ingestion finished ===")

if __name__ == "__main__":
    ingest_all_data()