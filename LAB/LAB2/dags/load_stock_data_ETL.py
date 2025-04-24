from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_stock_data(**context):
    # List of stock symbols
    symbols = ['GOOGL', 'AAPL', 'MFST', 'TSLA', 'AMZN']  # Add more symbols as needed
    
    all_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    for symbol in symbols:
        # Fetch data from yfinance
        stock = yf.Ticker(symbol)
        df = stock.history(start=start_date, end=end_date)
        
        # Reset index to make date a column
        df = df.reset_index()
        
        # Rename columns to match our schema
        df = df.rename(columns={
            'Date': 'date',
            'Close': 'close',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Volume': 'volume'
        })
        
        # Add symbol column
        df['symbol'] = symbol
        
        # Create symbol_date_key
        df['symbol_date_key'] = df['symbol'] + '_' + df['date'].dt.strftime('%Y-%m-%d')
        
        # Select and reorder columns
        df = df[[
            'symbol_date_key', 'date', 'symbol',
            'open', 'high', 'low', 'close', 'volume'
        ]]
        
        all_data.append(df)
    
    # Combine all stock data
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Store the DataFrame in XCom for the next task
    context['task_instance'].xcom_push(key='stock_data', value=final_df.to_dict(orient='records'))

def load_to_postgres(**context):
    # Get data from XCom
    data = context['task_instance'].xcom_pull(key='stock_data')
    df = pd.DataFrame(data)
    
    # PostgreSQL connection parameters
    db_params = {
        'host': os.getenv('DBT_HOST', 'localhost'),
        'port': os.getenv('DBT_PORT', '5432'),
        'database': os.getenv('DBT_DATABASE', 'stock_analytics'),
        'user': os.getenv('DBT_USER', 'your_username'),
        'password': os.getenv('DBT_PASSWORD', 'your_password')
    }
    
    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    )
    
    # Load data to PostgreSQL
    df.to_sql('stock_input', engine, if_exists='replace', index=False)

with DAG(
    'Load_Stock_Data_ETL',
    default_args=default_args,
    description='Load stock data using yfinance',
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    fetch_data >> load_data    