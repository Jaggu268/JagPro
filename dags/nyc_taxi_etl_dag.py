"""
nyc_taxi_etl_dag.py
-------------------
Apache Airflow DAG — NYC Yellow Taxi ETL Pipeline

Schedule: Monthly (runs on the 5th of each month to allow TLC data to publish)

Task graph:
  start
    └─► extract_data
          └─► transform_data
                └─► load_data
                      └─► refresh_views
                            └─► notify_success

Environment variables required (set in Airflow UI → Admin → Variables):
  POSTGRES_CONN_STR   : postgresql+psycopg2://user:pass@host:5432/taxi_db
  RAW_DATA_DIR        : /opt/airflow/data/raw
  PROCESSED_DATA_DIR  : /opt/airflow/data/processed
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Adjust sys.path so Airflow can find the src package
import sys
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.extract import download_taxi_data
from src.transform import transform
from src.load import load

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments applied to every task
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "jagadeesh",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# Task functions (called by PythonOperator)
# ---------------------------------------------------------------------------

def _extract(data_interval_start: datetime, **kwargs) -> str:
    """Download the parquet file for the logical month being processed."""
    year  = data_interval_start.year
    month = data_interval_start.month

    raw_dir = Path(Variable.get("RAW_DATA_DIR", default_var="/opt/airflow/data/raw"))
    file_path = download_taxi_data(year, month, output_dir=raw_dir)

    # Push file path to XCom so downstream tasks can pick it up
    kwargs["ti"].xcom_push(key="raw_file_path", value=str(file_path))
    logger.info("Extract complete: %s", file_path)
    return str(file_path)


def _transform(**kwargs) -> str:
    """Clean and enrich the raw parquet, save as processed parquet."""
    raw_path = Path(kwargs["ti"].xcom_pull(task_ids="extract_data", key="raw_file_path"))
    processed_dir = Path(Variable.get("PROCESSED_DATA_DIR", default_var="/opt/airflow/data/processed"))
    processed_dir.mkdir(parents=True, exist_ok=True)

    df = transform(raw_path)

    out_path = processed_dir / raw_path.name
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info("Transform complete: %d rows → %s", len(df), out_path)

    kwargs["ti"].xcom_push(key="processed_file_path", value=str(out_path))
    return str(out_path)


def _load(**kwargs) -> int:
    """Load processed parquet into PostgreSQL."""
    import pandas as pd

    processed_path = Path(kwargs["ti"].xcom_pull(task_ids="transform_data", key="processed_file_path"))
    df = pd.read_parquet(processed_path)

    conn_str = Variable.get("POSTGRES_CONN_STR")
    rows = load(df, connection_string=conn_str)

    logger.info("Load complete: %d rows inserted", rows)
    kwargs["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def _notify(**kwargs):
    """Log a summary — extend this to send Slack/email alerts."""
    rows = kwargs["ti"].xcom_pull(task_ids="load_data", key="rows_loaded")
    run_date = kwargs["data_interval_start"].strftime("%Y-%m")
    logger.info("✅  Pipeline SUCCESS | Period: %s | Rows loaded: %d", run_date, rows or 0)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_etl_pipeline",
    description="Monthly ETL pipeline for NYC Yellow Taxi trip data",
    schedule_interval="0 6 5 * *",   # 06:00 on the 5th of every month
    start_date=datetime(2024, 1, 1),
    catchup=True,                     # backfill historical months automatically
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["data-engineering", "etl", "nyc-taxi"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=_transform,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=_load,
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify,
        trigger_rule="all_success",
    )

    end = EmptyOperator(task_id="end")

    # DAG dependency chain
    start >> extract_data >> transform_data >> load_data >> notify_success >> end
