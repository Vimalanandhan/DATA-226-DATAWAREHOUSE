from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import snowflake.connector

# Define constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
STOCK_SYMBOLS = ["AAPL", "META"]  # Two stock symbols
WAREHOUSE = "MACKEREL_QUERY_WH"
DATABASE = "USER_DB_MACKEREL"
SCHEMA = "RAW"
TARGET_TABLE = f"{DATABASE}.{SCHEMA}.STOCK_PRICE"

# Function to get Snowflake connection using SnowflakeHook
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")
    return conn, cursor  # Return both connection and cursor for proper closing

# Task to extract data from yfinance API
@task
def extract_data():
    data_records = []
    for symbol in STOCK_SYMBOLS:
        stock_data = yf.download(symbol, period="180d")
        for date, row in stock_data.iterrows():
            record = (
                symbol,
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                int(row['Volume']),
                date.strftime('%Y-%m-%d')
            )
            data_records.append(record)
    return data_records

# Task to load transformed data into Snowflake
@task
def load_data(records):
    target_table = "USER_DB_MACKEREL.RAW.STOCK_PRICE"
    conn, cursor = get_snowflake_cursor()  # Get both connection and cursor
    try:
        cursor.execute("BEGIN")
        
        # Ensure the table exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                "SYMBOL" STRING,
                "OPEN" FLOAT,  -- Quote reserved keyword
                "HIGH" FLOAT,
                "LOW" FLOAT,
                "CLOSE" FLOAT,
                "VOLUME" INT,
                "DATE" DATE
            )
        """)
        # Alternative: Use a non-reserved column name
        # cursor.execute(f"""
        #     CREATE TABLE IF NOT EXISTS {target_table} (
        #         "SYMBOL" STRING,
        #         "OPEN_PRICE" FLOAT,  -- Non-reserved name
        #         "HIGH" FLOAT,
        #         "LOW" FLOAT,
        #         "CLOSE" FLOAT,
        #         "VOLUME" INT,
        #         "DATE" DATE
        #     )
        # """)

        for symbol in STOCK_SYMBOLS:
            cursor.execute(f"DELETE FROM {target_table} WHERE SYMBOL = '{symbol}'")  # Delete only relevant stock data

        # Single INSERT with quoted column names
        cursor.executemany(
            f"""INSERT INTO {target_table} ("SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "DATE")
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            records
        )
        # Alternative: If using OPEN_PRICE
        # cursor.executemany(
        #     f"""INSERT INTO {target_table} ("SYMBOL", "OPEN_PRICE", "HIGH", "LOW", "CLOSE", "VOLUME", "DATE")
        #     VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        #     records
        # )

        cursor.execute("COMMIT")
        print(f"Loaded {len(records)} records into {target_table}")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise RuntimeError(f"Load failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()  # Close the connection

with DAG(
    dag_id='stock_data_pipeline',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    schedule='0 2 * * *',
    tags=['ETL', 'Snowflake']
) as dag:
    raw_data = extract_data()
    load_data(raw_data)