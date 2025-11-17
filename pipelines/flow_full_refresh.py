# pipelines/flow_full_refresh.py
import os
import sys
import subprocess
from pathlib import Path

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt_project" / "gpu_telemetry"
DB_PATH = PROJECT_ROOT / "telemetry.db"


@task
def ingest_bronze():
    """
    Run the CSV -> Parquet bronze ingestion script.
    """
    print(">>> Ingesting bronze Parquet files...")
    subprocess.run(
        [sys.executable, "pipelines/ingest_bronze.py"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    print(">>> Bronze ingestion complete.")


def _dbt_env():
    """Base environment for dbt commands, ensuring DUCKDB_PATH is set."""
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(DB_PATH)
    return env


@task
def run_dbt():
    """
    Run dbt models (all) and tests.
    """
    print(">>> Running dbt models...")
    subprocess.run(
        ["dbt", "run"],
        cwd=DBT_DIR,
        check=True,
        env=_dbt_env(),
    )

    print(">>> Running dbt tests...")
    subprocess.run(
        ["dbt", "test"],
        cwd=DBT_DIR,
        check=True,
        env=_dbt_env(),
    )
    print(">>> dbt run + test complete.")


@task
def run_ml_scoring():
    """
    Train the anomaly model and score daily cluster utilization.
    """
    print(">>> Training cluster anomaly model...")
    subprocess.run(
        [sys.executable, "ml/train_cluster_anomaly_model.py"],
        cwd=PROJECT_ROOT,
        check=True,
    )

    print(">>> Scoring daily anomalies...")
    subprocess.run(
        [sys.executable, "ml/score_cluster_anomalies.py"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    print(">>> ML scoring complete.")


@flow(name="gpu_telemetry_full_refresh")
def full_refresh():
    """
    End-to-end pipeline:
    1. CSV -> Parquet (bronze)
    2. dbt run (bronze views, silver, gold)
    3. dbt test
    4. ML train + scoring
    """
    ingest_bronze()
    run_dbt()
    run_ml_scoring()


if __name__ == "__main__":
    full_refresh()
