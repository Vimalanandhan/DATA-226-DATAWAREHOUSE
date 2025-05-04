# dbt_run_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = 'C:/Users/vimal/Desktop/Github/sjsu-data226-SP25'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_sequential_run',
    default_args=default_args,
    description='Run dbt models in staging, intermediate, marts and then test',
    start_date=datetime(2025, 5, 1),
    schedule_interval='0 2 * * *',  # daily at 02:00
    catchup=False,
    tags=['dbt'],
) as dag:

    # 1. install dependencies
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps',
    )

    # 2. run staging models
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} '
            '&& dbt run --select staging'
        ),
    )

    # 3. run intermediate enrichment
    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} '
            '&& dbt run --select intermediate'
        ),
    )

    # 4. run marts (dims & facts)
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} '
            '&& dbt run --select marts'
        ),
    )

    # 5. run tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )

    # define ordering
    dbt_deps \
        >> dbt_run_staging \
        >> dbt_run_intermediate \
        >> dbt_run_marts \
        >> dbt_test
