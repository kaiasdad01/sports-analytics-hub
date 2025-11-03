import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, '/opt/airflow/nfl_v3/airflow')

from utils.nfl_tasks import run_dbt


def get_latest_external_dag_run_date(logical_date, **kwargs):
    """Returns the execution_date of the most recent successful load DAG run"""
    from airflow.models import DagRun
    from airflow.utils.db import create_session

    external_dag_id = 'nfl_data_load'

    with create_session() as session:
        latest_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == external_dag_id,
                DagRun.state == 'success'
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )

        if latest_run:
            print(f"Found latest successful {external_dag_id} run at {latest_run.execution_date}")
            return latest_run.execution_date

    print(f"No successful {external_dag_id} run found, using logical_date {logical_date}")
    return logical_date

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
    external_task_id=None,
    allowed_states=['success'],
    failed_states=None,
    check_existence=True,
    mode='reschedule',
    poke_interval=60,
    timeout=6 * 60 * 60,
    execution_date_fn=get_latest_external_dag_run_date,
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

