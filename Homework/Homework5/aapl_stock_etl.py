from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import logging

def return_snowflake_conn():
    """Initialize Snowflake connection using SnowflakeHook."""
    
    # Initialize the SnowflakeHook using the connection ID stored in Airflow
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Return the cursor object
    return hook.get_conn().cursor()

@task
def extract():
    """Extract AAPL stock data from Alpha Vantage API"""
    api_key = Variable.get("ALPHA_VANTAGE_API_KEY")  # Get API key from Airflow Variables
    symbol = "AAPL"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("Time Series (Daily)", {})
        logging.info(f"Extracted {len(data)} records")
        return data  # XCom push
    except Exception as e:
        logging.error(f"Error in extract: {str(e)}")
        raise

@task
def transform(data):
    """Transform extracted stock data into structured format"""
    records = []
    for date, values in data.items():
        records.append([
            date, float(values["1. open"]), float(values["2. high"]),
            float(values["3. low"]), float(values["4. close"]),
            int(values["5. volume"])
        ])
    logging.info(f"Transformed {len(records)} records")
    return records  # XCom push

@task
def load(records):
    """Load transformed data into Snowflake using SnowflakeHook"""
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data.raw.stock_data (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT
            );
        """)
        cur.execute("DELETE FROM stock_data.raw.stock_data;")  # Full refresh

        for record in records:
            # Debug: Print each record before inserting
            print(f"Inserting record: {record}")

            # Check for NULL values and replace them
            cleaned_record = [
                value if value is not None else 0  # Replace NULLs with 0
                for value in record
            ]

            sql = f"""
            INSERT INTO stock_data.raw.stock_data (date, open, high, low, close, volume)
            VALUES ('{cleaned_record[0]}', {cleaned_record[1]}, {cleaned_record[2]}, {cleaned_record[3]}, {cleaned_record[4]}, {cleaned_record[5]});
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
        logging.info("Data successfully loaded into Snowflake")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error in load: {str(e)}")
        raise

# Define the Airflow DAG
with DAG(
    dag_id='AAPL_Stock_ETL',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL', 'Stock Data'],
    schedule_interval='30 2 * * *'  # Run daily at 2:30 AM UTC
) as dag:
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
