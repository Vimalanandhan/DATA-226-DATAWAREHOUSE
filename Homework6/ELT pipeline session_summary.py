from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    logging.info(f"Running CTAS for table: {table}")
    logging.info(f"SQL for table creation: {select_sql}")

    cur = return_snowflake_conn()

    try:
        
        
        # Create the temp table with select statement
        create_temp_table_sql = f"CREATE TABLE If Not Exists {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(f"Executing: {create_temp_table_sql}")
        cur.execute(create_temp_table_sql)

# Primary key uniqueness check
        if primary_key is not None:
            primary_key_check_sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            logging.info(f"Executing primary key check: {primary_key_check_sql}")
            cur.execute(primary_key_check_sql)
            result = cur.fetchone()
            logging.info(f"Primary key check result: {result}")
            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")
# Duplicate records check
        total_count_sql = f"SELECT COUNT(1) FROM {database}.{schema}.temp_{table};"
        logging.info(f"Executing: {total_count_sql}")
        cur.execute(total_count_sql)
        total_count = cur.fetchone()[0]
        logging.info(f"Total row count in temp_{table}: {total_count}")

        distinct_count_sql = f"SELECT COUNT(1) FROM (SELECT DISTINCT * FROM {database}.{schema}.temp_{table});"
        logging.info(f"Executing: {distinct_count_sql}")
        cur.execute(distinct_count_sql)
        distinct_count = cur.fetchone()[0]
        logging.info(f"Distinct row count in temp_{table}: {distinct_count}")

        # Compare total row count and distinct row count to check for duplicates
        if total_count != distinct_count:
            logging.error(f"Duplicate rows found in table temp_{table}. Total rows: {total_count}, Distinct rows: {distinct_count}")
            raise Exception(f"Duplicate records found in table temp_{table}")
        else:
            logging.info(f"No duplicates found in table temp_{table}")

        # Create the main table if not exists (this step uses the original table name, not the temp table)
        main_table_creation_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
        SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;
        """
        logging.info(f"Executing: {main_table_creation_sql}")
        cur.execute(main_table_creation_sql)

        # Swap the main table with the temp table
        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        logging.info(f"Executing: {swap_sql}")
        cur.execute(swap_sql)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise

        
with DAG(
    dag_id='ELT_summary',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *'
) as dag:

    database = "USER_DB_MACKEREL"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM USER_DB_MACKEREL.raw.user_session_channel u
    JOIN USER_DB_MACKEREL.raw.session_timestamp s ON u.sessionId=s.sessionId
    """
    run_ctas(database, schema, table, select_sql, primary_key='sessionId')
