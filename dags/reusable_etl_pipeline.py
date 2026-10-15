from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

DAGS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(DAGS_DIR))

from scripts.etl_engine import process_all_files
from scripts.load_to_postgres import load_to_postgres


with DAG(
    dag_id="reusable_csv_etl_pipeline",
    description="Clean CSV datasets and load them into PostgreSQL",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ETL", "PostgreSQL", "DataQuality"],
) as dag:

    clean_validate = PythonOperator(
        task_id="discover_clean_validate_csv_files",
        python_callable=process_all_files,
    )

    load_postgres = PythonOperator(
        task_id="load_cleaned_data_to_postgres",
        python_callable=load_to_postgres,
    )

    clean_validate >> load_postgres