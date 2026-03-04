from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import sys
sys.path.append("/opt/airflow")

# Import your ETL functions without creating DB engine at top-level
from etl.bronzetosilver import bronze_to_silver
from etl.silvertogold import silver_to_gold

# Internal Docker Postgres URL
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow"

default_args = {
    "owner": "sreekumar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Wrapper functions for Airflow tasks
def run_bronze_to_silver():
    engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
    bronze_to_silver(engine)

def run_silver_to_gold():
    engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
    silver_to_gold(engine)

with DAG(
    dag_id="employee_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    silver_to_gold_task = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
    )

    bronze_to_silver_task >> silver_to_gold_task