from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# Define constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
TRAIN_INPUT_TABLE = "USER_DB_MACKEREL.RAW.STOCK_PRICE"
TRAIN_VIEW = "USER_DB_MACKEREL.ADHOC.MARKET_DATA_VIEW"
FORECAST_TABLE = "USER_DB_MACKEREL.ADHOC.MARKET_DATA_FORECAST"
FORECAST_FUNCTION_NAME = "USER_DB_MACKEREL.ANALYTICS.PREDICT_STOCK_PRICE"
FINAL_TABLE = "USER_DB_MACKEREL.ANALYTICS.MARKET_DATA"
WAREHOUSE = "MACKEREL_QUERY_WH"

# Function to get Snowflake connection
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE}")
    cursor.execute(f"USE DATABASE USER_DB_MACKEREL")
    return conn, cursor  # Return both connection and cursor for proper closing

@task
def ensure_schema_exists():
    """
    Ensures that the required schemas exist before running queries.
    """
    conn, cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_MACKEREL.ADHOC")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_MACKEREL.ANALYTICS")
        cursor.execute("GRANT USAGE ON SCHEMA USER_DB_MACKEREL.ANALYTICS TO ROLE ACCOUNTADMIN")
        cursor.execute("GRANT CREATE TABLE ON SCHEMA USER_DB_MACKEREL.ANALYTICS TO ROLE ACCOUNTADMIN")
        cursor.execute("COMMIT")
        print("Schemas USER_DB_MACKEREL.ADHOC and USER_DB_MACKEREL.ANALYTICS ensured")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Schema creation failed: {e}")
        raise RuntimeError(f"Schema creation failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@task
def train_model():
    """
    Creates a training view and trains the ML model.
    """
    conn, cursor = get_snowflake_cursor()
    create_view_sql = f"""
        CREATE OR REPLACE VIEW {TRAIN_VIEW} AS 
        SELECT DATE, CLOSE, SYMBOL FROM {TRAIN_INPUT_TABLE};
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
        cursor.execute("BEGIN")
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(f"CALL {FORECAST_FUNCTION_NAME}!SHOW_EVALUATION_METRICS()")
        cursor.execute("COMMIT")
        print(f"Model {FORECAST_FUNCTION_NAME} trained successfully")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Training failed: {e}")
        raise RuntimeError(f"Training failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@task
def predict_stock_prices():
    """
    Generates predictions and stores them in Snowflake.
    """
    conn, cursor = get_snowflake_cursor()
    call_forecast_sql = f"""
        CALL {FORECAST_FUNCTION_NAME}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        )
    """
    create_forecast_table_sql = f"""
        CREATE OR REPLACE TABLE {FORECAST_TABLE} AS 
        SELECT * FROM TABLE(RESULT_SCAN(%s))
    """
    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {FINAL_TABLE} AS
        SELECT * FROM (
            -- Apple actual values
            SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {TRAIN_INPUT_TABLE}
            WHERE SYMBOL = 'AAPL'
            UNION ALL
            -- Apple predicted values
            SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {FORECAST_TABLE}
            WHERE REPLACE(series, '"', '') = 'AAPL'
            UNION ALL
            -- Meta actual values
            SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {TRAIN_INPUT_TABLE}
            WHERE SYMBOL = 'META'
            UNION ALL
            -- Meta predicted values
            SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {FORECAST_TABLE}
            WHERE REPLACE(series, '"', '') = 'META'
        ) ORDER BY SYMBOL, DATE;
    """
    try:
        cursor.execute("BEGIN")
        # Execute the forecast call
        cursor.execute(call_forecast_sql)
        # Retrieve the SQLID of the forecast call
        sql_id = cursor.sfqid
        # Use the SQLID to create the forecast table
        cursor.execute(create_forecast_table_sql, (sql_id,))
        # Create the final table with actual and predicted values
        cursor.execute(create_final_table_sql)
        cursor.execute("COMMIT")
        print(f"Predictions stored in {FINAL_TABLE}")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Prediction failed: {e}")
        raise RuntimeError(f"Prediction failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id='ml_forecasting_pipeline',
    start_date=datetime(2025, 3, 1),
    catchup=False,
    schedule='30 2 * * *',
    tags=['ML', 'Forecasting']
) as dag:
    ensure_schema = ensure_schema_exists()
    train = train_model()
    predict = predict_stock_prices()
    ensure_schema >> train >> predict