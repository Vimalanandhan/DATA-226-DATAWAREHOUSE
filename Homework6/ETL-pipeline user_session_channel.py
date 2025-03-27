from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

import json


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_user_session_channel_table(cur,table1):
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table1} (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);""")

@task
def create_session_timestamp_table(cur,table2):
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table2} (
    sessionId varchar(32) primary key,
    ts timestamp  
);""")

@task
def populate_tables():
    cur = return_snowflake_conn()

    # Create the stage
    cur.execute("""
        CREATE OR REPLACE STAGE USER_DB_MACKEREL.raw.blob_stage
        URL = 's3://s3-geospatial/readonly/'
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """)

    # Copy data into user_session_channel
    cur.execute("""
        COPY INTO USER_DB_MACKEREL.raw.user_session_channel
        FROM @USER_DB_MACKEREL.raw.blob_stage/user_session_channel.csv;
    """)

    # Copy data into session_timestamp
    cur.execute("""
        COPY INTO USER_DB_MACKEREL.raw.session_timestamp
        FROM @USER_DB_MACKEREL.raw.blob_stage/session_timestamp.csv;
    """)



with DAG (
    dag_id = 'session_summary_ETL',
    start_date = datetime(2025,2,21),
    catchup=False,
    tags=['ETL_user_activity'],
    schedule = '30 2 * * *'
) as dag:
    table1= "USER_DB_MACKEREL.raw.user_session_channel"
    table2="USER_DB_MACKEREL.raw.session_timestamp"
    cur = return_snowflake_conn()

    create_user_session_channel_table(cur,table1)
    create_session_timestamp_table(cur,table2)
    populate_tables() 
        
   
   















