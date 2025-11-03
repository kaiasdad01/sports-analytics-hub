from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys

sys.path.insert(0, '/opt/airflow/nfl_v3/airflow')

from utils.nfl_tasks import run_dbt

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
    'nfl_dbt_transform',
    default_args=default_args,
    description='Transform raw NFL data in BigQuery using dbt after load DAG completes',
    schedule_interval='0 7 * * 5',  
    catchup=False,
    tags=['nfl', 'dbt', 'transform'],
)


wait_for_load = ExternalTaskSensor(
    task_id='wait_for_load',
    external_dag_id='nfl_data_load',
    allowed_states=['success'],
    mode='reschedule',
    poke_interval=60,  # Check every minute
    timeout=6 * 60 * 60,  # 6 hour timeout
    dag=dag,
)


run_dbt_staging = PythonOperator(
    task_id='run_dbt_staging',
    python_callable=run_dbt,
    op_kwargs={
        'command': 'run',
        'select': 'staging',
    },
    dag=dag,
)


run_dbt_marts = PythonOperator(
    task_id='run_dbt_marts',
    python_callable=run_dbt,
    op_kwargs={
        'command': 'run',
        'select': 'marts',
    },
    dag=dag,
)


run_dbt_tests = PythonOperator(
    task_id='run_dbt_tests',
    python_callable=run_dbt,
    op_kwargs={
        'command': 'test',
    },
    dag=dag,
)


wait_for_load >> run_dbt_staging >> run_dbt_marts >> run_dbt_tests

