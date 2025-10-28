from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta


# ----------------------------------------
# Helper function: Get Snowflake connection
# ----------------------------------------
def return_snowflake_conn():
    """
    Create and return a Snowflake cursor using Airflow connection.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


# ----------------------------------------
# ELT Tasks
# ----------------------------------------

@task
def create_analytics_schema():
    """
    Create the analytics schema if it does not exist.
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS analytics;
        """)
    except Exception as e:
        raise e


@task
def create_session_summary():
    """
    Create or replace analytics.session_summary table by joining the two raw tables.
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE OR REPLACE TABLE analytics.session_summary AS
            SELECT 
                a.userId,
                a.sessionId,
                a.channel,
                b.ts AS session_time,
                DATE_TRUNC('WEEK', b.ts) AS session_week
            FROM raw.user_session_channel a
            JOIN raw.session_timestamp b
                ON a.sessionId = b.sessionId;
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


@task
def check_duplicates():
    """
    Create or replace a duplicate check table listing sessions that appear more than once.
    (Extra +1 point for duplicate detection.)
    """
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""
            CREATE OR REPLACE TABLE analytics.duplicate_sessions AS
            SELECT 
                sessionId, 
                COUNT(*) AS duplicate_count
            FROM analytics.session_summary
            GROUP BY sessionId
            HAVING COUNT(*) > 1;
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e


# ----------------------------------------
# DAG Definition
# ----------------------------------------
with DAG(
    dag_id="elt_session_summary_dag",
    description="ELT DAG to join raw tables into analytics.session_summary and check duplicates",
    start_date=datetime(2025, 10, 20),
    schedule='0 1 * * *',  # Run daily at 1am
    catchup=False,
    tags=["ELT", "Snowflake"],
) as dag:

    # Define DAG flow
    t1 = create_analytics_schema()
    t2 = create_session_summary()
    t3 = check_duplicates()

    t1 >> t2 >> t3
