from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def train(train_input_table, train_view, forecast_function_name):
    """
    Creates a view that stored relevant historical stock information for the forecast model.
    A forecast model is trained with the created view and can be used to generate forecasts. 
    """
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    cur = return_snowflake_conn()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS adhoc;")
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise
    finally:
        cur.close()


@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    """
     Generate a 7-day prediction and store the results to a table named market_data_forecast.
     The predictions are unioned with historical data, creating a final table complete with both historical and forecasted stock closing prices.
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    cur = return_snowflake_conn()
    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise
    finally:
        cur.close()

with DAG(
    dag_id = 'TrainPredict',
    start_date = datetime(2025,10,3),
    catchup = False,
    tags=['ML', 'ELT'],
    schedule = '10 0 * * *'
) as dag:

    train_input_table = "raw.lab1_market_data"
    train_view = "adhoc.lab1_market_data_view"
    forecast_table = "adhoc.lab1_market_data_forecast"
    forecast_function_name = "analytics.lab1_predict_market_prices"
    final_table = "analytics.lab1_market_data"

    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)
    train_task >> predict_task