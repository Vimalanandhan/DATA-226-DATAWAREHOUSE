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

# Function to get Snowflake connection using SnowflakeHook
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("USE WAREHOUSE compute_wh")
    cursor.execute("USE DATABASE dev")
    cursor.execute("USE SCHEMA raw")
    return cursor

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
    target_table = "dev.raw.stock_price"
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        
        # Ensure the table exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol STRING,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT,
                date DATE
            )
        """)
        
        for symbol in STOCK_SYMBOLS:
            cursor.execute(f"DELETE FROM {target_table} WHERE symbol = '{symbol}'")  # Delete only relevant stock data
        cursor.executemany(
            f"INSERT INTO {target_table} (symbol, open, high, low, close, volume, date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            records
        )
        cursor.execute("COMMIT")
        print(f"Loaded {len(records)} records into {target_table}")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise RuntimeError(f"Load failed: {str(e)}")
    finally:
        cursor.close()


with DAG(
    dag_id='stock_data_pipeline',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    schedule='0 2 * * *',
    tags=['ETL', 'Snowflake']
) as dag:
    raw_data = extract_data()
    load_data(raw_data)
