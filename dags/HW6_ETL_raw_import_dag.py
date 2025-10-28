from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# ----------------------------------------
# Function: return Snowflake cursor
# ----------------------------------------
def return_snowflake_conn():
    """
    Create and return a Snowflake cursor using Airflow connection.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# ----------------------------------------
# ETL Tasks
# ----------------------------------------

@task
def create_raw_tables():
    """
    Create raw.user_session_channel and raw.session_timestamp tables.
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32) DEFAULT 'direct'
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId VARCHAR(32) PRIMARY KEY,
                ts TIMESTAMP
            );
        """)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


@task
def create_stage():
    """
    Create or replace the S3 stage for CSV loading.
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("""
            CREATE OR REPLACE STAGE raw.blob_stage
                URL = 's3://s3-geospatial/readonly/'
                FILE_FORMAT = (TYPE = csv, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)
    except Exception as e:
        raise e


@task
def load_raw_data():
    """
    Load both CSVs into Snowflake using COPY INTO inside a transaction.
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        cur.execute("""
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv;
        """)

        cur.execute("""
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv;
        """)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


# ----------------------------------------
# DAG Definition
# ----------------------------------------
with DAG(
    dag_id="etl_raw_import_dag",
    description="ETL DAG to create and load raw tables into Snowflake using transactional COPY",
    start_date=datetime(2025, 10, 20),
    schedule='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=["ETL", "Snowflake"],
) as dag:

    # Define task dependencies
    t1 = create_raw_tables()
    t2 = create_stage()
    t3 = load_raw_data()

    t1 >> t2 >> t3
