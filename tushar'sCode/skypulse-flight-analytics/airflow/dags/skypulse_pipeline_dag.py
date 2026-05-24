from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = "/home/sunbeam/sunbeamLab/SkyPulse/skypulse-flight-analytics"
CONFIG_PATH = f"{PROJECT_ROOT}/configs/pipeline_config.yaml"


default_args = {
    "owner": "skypulse-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="skypulse_flight_fare_pipeline",
    description="Orchestrates SkyPulse ingestion checks, batch refreshes, and ML retraining.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["skypulse", "big-data", "flight-fares"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_dataset = BashOperator(
        task_id="validate_dataset",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python - <<'PY'\n"
            "import pandas as pd\n"
            "from src.common.config import load_config\n"
            "cfg = load_config('configs/pipeline_config.yaml')\n"
            "df = pd.read_csv(cfg['producer']['source_csv_path'])\n"
            "print('rows=', len(df), 'columns=', list(df.columns))\n"
            "PY"
        ),
    )

    producer_dry_run = BashOperator(
        task_id="producer_config_check",
        bash_command=f"cd {PROJECT_ROOT} && python kafka/flight_fare_producer.py --config {CONFIG_PATH} --help",
    )

    gold_refresh_note = BashOperator(
        task_id="gold_refresh_note",
        bash_command="echo 'Run Databricks job/notebook 04_gold_streaming_and_sql for Gold refresh.'",
    )

    ml_retraining_note = BashOperator(
        task_id="ml_retraining_note",
        bash_command="echo 'Run Databricks job/notebook 05_ml_training for scheduled ML retraining.'",
    )

    end = EmptyOperator(task_id="end")

    start >> validate_dataset >> producer_dry_run >> gold_refresh_note >> ml_retraining_note >> end

