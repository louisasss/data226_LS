from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(symbol: str):
    api_key = Variable.get("alphavantage_api")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"
    r = requests.get(url)
    return r.json()


@task
def transform(data: dict):
    """
    Transform Alpha Vantage TIME_SERIES_DAILY JSON into a list of dicts.
    Each dict = one row with: symbol, date, open, close, high, low, volume
    """
    symbol = data['Meta Data']['2. Symbol']
    ts_data = data['Time Series (Daily)']

    rows = []
    for date_str, values in ts_data.items():
        rows.append({
            "symbol": symbol,
            "date": date_str,
            "open": float(values['1. open']),
            "high": float(values['2. high']),
            "low": float(values['3. low']),
            "close": float(values['4. close']),
            "volume": int(values['5. volume'])
        })

    # keep only last 90 days
    return rows[:90]


@task
def load(rows: list, symbol: str):
    """
    Load list of dicts into Snowflake table (full refresh).
    """
    con = return_snowflake_conn()
    target_table = f"USER_DB_PIGEON.RAW.{symbol}"

    try:
        con.execute("BEGIN;")

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            SYMBOL VARCHAR(10),
            DATE DATE,
            OPEN FLOAT,
            CLOSE FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            VOLUME FLOAT,
            PRIMARY KEY(SYMBOL, DATE)
        )
        """
        con.execute(create_sql)

        con.execute(f"DELETE FROM {target_table}")

        for row in rows:
            sql = f"""
            INSERT INTO {target_table} (symbol, date, open, close, high, low, volume)
            VALUES (%(symbol)s, %(date)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s)
            """
            con.execute(sql, row)

        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        raise e


with DAG(
    dag_id='homework5',
    start_date=datetime(2025, 10, 3),
    catchup=False,
    tags=['ETL'],
    schedule_interval='0 0 * * *',  # daily at 12 AM
) as dag:
    symbol = "LLY"
    data = extract(symbol)
    rows = transform(data)
    load(rows, symbol)
