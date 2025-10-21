import logging
from ingestion.nfl.extractor import NFLExtractor
from ingestion.nfl.loader import BigQueryLoader
from ingestion.config import get_bigquery_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# params 
seasons = [2020, 2021, 2022, 2023, 2024, 2025]

def ingest_all_data():
    """Get key datasets: pbp, schedules, rosters, teams"""
    logger.info("=== Starting Core Data Ingestion ===")

    extractor = NFLExtractor()
    config = get_bigquery_config()
    loader = BigQueryLoader()

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
    ]

    for data_type, table_name in datasets:
        logger.info(f"Extracting {data_type} for seasons {seasons}")

        try:
            df = extractor.extract(data_type, seasons=seasons)

            logger.info(f"Loading {len(df)} rows to {config.raw_dataset}.{table_name}")
            loader.load_dataframe(
                df=df,
                table_name=table_name,
                dataset=config.raw_dataset,
                write_mode='replace'
            )

            # validate
            row_count = loader.get_row_count(table_name, config.raw_dataset)
            logger.info(f"{data_type}: {row_count} rows loaded successfully")
        
        except Exception as e:
            logger.error(f"Failed to load {data_type}: {e}")
    
    # teams data
    logger.info("=== Starting Teams Data Ingestion ===")
    try:
        df = extractor.extract('teams')

        logger.info(f"Loading {len(df)} rows to {config.raw_dataset}.teams")
        loader.load_dataframe(
            df=df,
            table_name='teams',
            dataset=config.raw_dataset,
            write_mode='replace'
        )

        # validate
        row_count = loader.get_row_count('teams', config.raw_dataset)
        logger.info(f"teams: {row_count} rows loaded successfully")
        
    except Exception as e:
        logger.error(f"Failed to load teams: {e}")

    logger.info("=== Ingestion finished ===")

if __name__ == "__main__":
    ingest_all_data()