from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Define constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
TRAIN_INPUT_TABLE = "dev.raw.stock_price"
TRAIN_VIEW = "dev.adhoc.market_data_view"
FORECAST_TABLE = "dev.adhoc.market_data_forecast"
FORECAST_FUNCTION_NAME = "dev.analytics.predict_stock_price"
FINAL_TABLE = "dev.analytics.market_data"
WAREHOUSE = "compute_wh"

# Function to get Snowflake connection
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE};")
    return cursor

@task
def ensure_schema_exists():
    """
    Ensures that the required schemas exist before running queries.
    """
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DEV.ADHOC;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DEV.ANALYTICS;")
        cursor.execute("GRANT USAGE ON SCHEMA DEV.ANALYTICS TO ROLE ACCOUNTADMIN;")
        cursor.execute("GRANT CREATE TABLE ON SCHEMA DEV.ANALYTICS TO ROLE ACCOUNTADMIN;")
    except Exception as e:
        print(f"Schema creation failed: {e}")
        raise
    finally:
        cursor.close()

@task
def train_model():
    """
    Creates a training view and trains the ML model.
    """
    cursor = get_snowflake_cursor()
    create_view_sql = f"""
        CREATE OR REPLACE VIEW {TRAIN_VIEW} AS 
        SELECT date, close, symbol FROM {TRAIN_INPUT_TABLE};
    """
    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {FORECAST_FUNCTION_NAME} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{TRAIN_VIEW}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
    """
    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(f"CALL {FORECAST_FUNCTION_NAME}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(f"Training failed: {e}")
        raise

@task
def predict_stock_prices():
    """
    Generates predictions and stores them in Snowflake.
    """
    cursor = get_snowflake_cursor()
    make_prediction_sql = f"""
        BEGIN
        CALL {FORECAST_FUNCTION_NAME}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {FORECAST_TABLE} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
    """
    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {FINAL_TABLE} AS
        SELECT symbol, date, close AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {TRAIN_INPUT_TABLE}
        UNION ALL
        SELECT replace(series, '"', '') as symbol, ts as date, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {FORECAST_TABLE};
    """
    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
    except Exception as e:
        print(f"Prediction failed: {e}")
        raise

with DAG(
    dag_id='ml_forecasting_pipeline',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    schedule='30 2 * * *',
    tags=['ML', 'Forecasting']
) as dag:
    ensure_schema_exists() >> train_model() >> predict_stock_prices()
