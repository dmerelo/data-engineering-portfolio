"""Airflow DAG for the incremental customer pipeline.

Task flow:

    read_watermark >> run_incremental_batch >> quality_gate >> publish_target >> commit_watermark

Design choices:
- ``max_active_runs=1``: incremental runs depend on the previous watermark, so two
  runs must never overlap.
- ``catchup=False``: each run reads everything newer than the committed watermark,
  so missed schedules are recovered by the next run instead of by backfill.
- Retries with exponential backoff on every task; all tasks are safe to rerun.
- The watermark is committed in the last task, only after the target was published.

In the AWS version of this pipeline, ``run_incremental_batch`` is a Glue job
(PySpark) started with ``GlueJobOperator`` and the files live in S3; the task
boundaries and the commit rule stay the same.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import orchestration  # noqa: E402

# Read from the environment, not from an Airflow Variable, so parsing the DAG file
# never hits the metadata database.
DATA_DIR = Path(os.environ.get("INCREMENTAL_CUSTOMERS_DATA_DIR", PROJECT_ROOT / "data"))
SOURCE_PATH = DATA_DIR / "incremental_batch.csv"
TARGET_PATH = DATA_DIR / "curated" / "customers.csv"
STATE_PATH = DATA_DIR / "state" / "watermark.json"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="incremental_customers",
    description="Incremental upsert of customer records with watermark state",
    schedule="0 3 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["incremental", "watermark", "aws"],
)
def incremental_customers():
    @task
    def read_watermark() -> str:
        return orchestration.read_watermark(STATE_PATH)

    @task
    def run_incremental_batch(watermark: str, run_id: str | None = None) -> dict:
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id or "manual")
        staging_path = DATA_DIR / "staging" / f"customers_{safe_run_id}.csv"
        metrics = orchestration.run_incremental_batch(
            SOURCE_PATH, TARGET_PATH, staging_path, watermark
        )
        metrics["staging_path"] = str(staging_path)
        return metrics

    @task
    def quality_gate(metrics: dict) -> dict:
        orchestration.quality_gate(Path(metrics["staging_path"]), metrics)
        return metrics

    @task
    def publish_target(metrics: dict) -> dict:
        orchestration.publish_target(Path(metrics["staging_path"]), TARGET_PATH)
        return metrics

    @task
    def commit_watermark(metrics: dict, run_id: str | None = None) -> str:
        orchestration.commit_watermark(STATE_PATH, metrics["candidate_watermark"], run_id)
        return metrics["candidate_watermark"]

    metrics = run_incremental_batch(read_watermark())
    commit_watermark(publish_target(quality_gate(metrics)))


incremental_customers()
