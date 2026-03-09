"""
weather_pipeline_dag.py — Apache Airflow DAG
Schedules the Weather ETL Pipeline to run daily at 06:00 UTC.
Place this file inside your Airflow 'dags/' folder.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Make pipeline scripts importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))

from extract import extract_weather
from transform import transform
from load import load

# ── Default arguments applied to every task ───────────────────
default_args = {
    "owner": "your_name",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["you@example.com"],
}

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Daily weather data ETL pipeline — Extract → Transform → Load",
    schedule_interval="0 6 * * *",   # Run every day at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                    # Don't backfill missed runs
    tags=["weather", "etl", "beginner", "automation"],
) as dag:

    # ── Task 1: Extract raw data from API ─────────────────────
    task_extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather,
        op_kwargs={"city": "London"},
    )

    # ── Task 2: Clean and transform the data ──────────────────
    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    # ── Task 3: Load into SQLite database ─────────────────────
    task_load = PythonOperator(
        task_id="load_to_database",
        python_callable=load,
    )

    # ── Set task execution order ──────────────────────────────
    # extract must finish → then transform → then load
    task_extract >> task_transform >> task_load
