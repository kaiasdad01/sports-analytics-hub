from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys

sys.path.insert(0, '/opt/airflow/nfl_v3/airflow')

from utils.nfl_tasks import extract_nfl_data, scrape_fines

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'nfl_data_pipeline',  # Eventually, we should replicate this for NBA, CFB, etc.
    default_args=default_args,
    description='Extracting NFL data from various sources and storing in GCS. From GCS - data goes to BigQuery and transformed '
                'via dbt.',
    schedule_interval='0 5 * * 5',  # for now, setting to Friday at 5am after prior week fines assessed.
    # TODO: evaluate frequency of updates for injuries + roster moves, that may warrant more frequent runs
    catchup=False,
    tags=['nfl', 'data-pipeline'],
    doc_md=""
)

season_selection = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
recent_seasons = [2020, 2021, 2022, 2023, 2024, 2025]
limited_seasons = [2020, 2021, 2022, 2023, 2024] # For functions that don't have 2025 yet.

# Game Data
with TaskGroup(group_id='core_game_data', dag=dag) as core_game_group:
    extract_schedules = PythonOperator(
        task_id='extract_schedules',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'schedules',
            'seasons': season_selection},
        dag=dag
    )

    extract_pbp = PythonOperator(
        task_id='extract_pbp',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'pbp',
            'seasons': recent_seasons},
        dag=dag
    )

# Roster & Team Data
with TaskGroup(group_id='roster_team_data', dag=dag) as roster_team_group:
    extract_rosters = PythonOperator(
        task_id='extract_rosters',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'rosters',
            'seasons': season_selection},
        dag=dag
    )

    extract_rosters_weekly = PythonOperator(
        task_id='extract_rosters_weekly',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'rosters_weekly',
            'seasons': season_selection},
        dag=dag
    )

    extract_depth_charts = PythonOperator(
        task_id='extract_depth_charts',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'depth_charts',
            'seasons': season_selection},
        dag=dag
    )

    extract_trades = PythonOperator(
        task_id='extract_trades',
        python_callable=extract_nfl_data,
        op_kwargs={'data_type': 'trades', 'seasons': None},
        dag=dag
    )

    extract_players = PythonOperator(
        task_id='extract_players',
        python_callable=extract_nfl_data,
        op_kwargs={'data_type': 'players', 'seasons': None},
        dag=dag
    )

    extract_teams = PythonOperator(
        task_id='extract_teams',
        python_callable=extract_nfl_data,
        op_kwargs={'data_type': 'teams', 'seasons': None},
        dag=dag
    )

# Player Performance Deets

with TaskGroup(group_id='player_performance_data', dag=dag) as player_performance_group:
    extract_player_stats = PythonOperator(
        task_id='extract_player_stats',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'player_stats',
            'seasons': season_selection},
        dag=dag
    )

    extract_snap_counts = PythonOperator(
        task_id='extract_snap_counts',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'snap_counts',
            'seasons': season_selection},
        dag=dag
    )

    extract_nextgen_stats = PythonOperator(
        task_id='extract_nextgen_stats',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'nextgen_stats',
            'seasons': limited_seasons},
        dag=dag
    )

    extract_participation = PythonOperator(
        task_id='extract_participation',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'participation',
            'seasons': limited_seasons},
        dag=dag
    )

# additional data
with TaskGroup(group_id='additional_context', dag=dag) as additional_context_group:
    extract_officials = PythonOperator(
        task_id='extract_officials',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'officials',
            'seasons': season_selection},
        dag=dag
    )

    extract_combine = PythonOperator(
        task_id='extract_combine',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'combine',
            'seasons': season_selection},
        dag=dag
    )

    extract_draft_picks = PythonOperator(
        task_id='extract_draft_picks',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'draft_picks',
            'seasons': season_selection},
        dag=dag
    )

    extract_contracts = PythonOperator(
        task_id='extract_contracts',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'contracts',
            'seasons': None},
        dag=dag
    )

# Fantasy Data
with TaskGroup(group_id='fantasy_data', dag=dag) as fantasy_group:
    extract_ff_playerids = PythonOperator(
        task_id='extract_ff_playerids',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'ff_playerids',
            'seasons': None},
        dag=dag
    )

    extract_ff_opportunity = PythonOperator(
        task_id='extract_ff_opportunity',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'ff_opportunity',
            'seasons': season_selection},
        dag=dag
    )

    extract_ff_rankings = PythonOperator(
        task_id='extract_ff_rankings',
        python_callable=extract_nfl_data,
        op_kwargs={
            'data_type': 'ff_rankings',
            'seasons': None},
        dag=dag
    )

# Fines Scraper
scrape_fines_task = PythonOperator(
    task_id='scrape_fines',
    python_callable=scrape_fines,
    dag=dag
)
