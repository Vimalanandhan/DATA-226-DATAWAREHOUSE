"""
A dbt DAG that runs dependencies, staging, intermediate, marts, tests and snapshot,
loading Snowflake credentials from an Airflow connection.
"""

from pendulum import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Path to your dbt project inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt"
# Absolute path to the dbt executable
DBT_BIN = "/home/airflow/.local/bin/dbt"

# Load Snowflake credentials from Airflow connection
conn = BaseHook.get_connection('snowflake_conn')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'env': {
        'DBT_USER':      conn.login,
        'DBT_PASSWORD':  conn.password,
        'DBT_ACCOUNT':   conn.extra_dejson.get('account'),
        'DBT_DATABASE':  conn.extra_dejson.get('database'),
        'DBT_SCHEMA':    conn.schema,
        'DBT_ROLE':      conn.extra_dejson.get('role'),
        'DBT_WAREHOUSE': conn.extra_dejson.get('warehouse'),
        'DBT_TYPE':      'snowflake'
    }
}

with DAG(
    dag_id='dbt_sequential_run',
    default_args=default_args,
    description='Run dbt: deps → staging → intermediate → marts → test → snapshot',
    start_date=datetime(2025, 5, 1),
    schedule_interval='0 2 * * *',  # daily at 02:00 UTC
    catchup=False,
    tags=['dbt'],
) as dag:

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"{DBT_BIN} deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --select staging",
    )

    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --select intermediate",
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --select marts",
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f"{DBT_BIN} snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    # Define task order
dbt_deps \
        >> dbt_run_staging \
        >> dbt_run_intermediate \
        >> dbt_run_marts \
        >> dbt_test \
        >> dbt_snapshot
