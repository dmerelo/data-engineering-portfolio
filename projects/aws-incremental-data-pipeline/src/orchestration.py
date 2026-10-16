"""Orchestration steps for the incremental pipeline.

Each function maps to one Airflow task in ``dags/incremental_customers_dag.py``.
They are plain Python so they can be unit tested without an Airflow installation.

Execution order and the rule that matters most:

    read watermark -> run incremental batch -> quality gate -> publish target -> commit watermark

The watermark is committed only after the target has been published, so a failure
at any earlier step leaves the state untouched and the run can simply be retried.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict
from pathlib import Path

from incremental_pipeline import DataQualityError, Row, execute, read_rows

DEFAULT_WATERMARK = "1970-01-01T00:00:00+00:00"


def read_watermark(state_path: Path) -> str:
    """Return the last committed watermark, or the default for a first run."""
    if not state_path.exists():
        return DEFAULT_WATERMARK
    return json.loads(state_path.read_text(encoding="utf-8"))["watermark"]


def commit_watermark(state_path: Path, watermark: str, run_id: str) -> None:
    """Persist the new watermark atomically (write to a temp file, then rename)."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps({"watermark": watermark, "committed_by_run": run_id}, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp_path, state_path)


def write_rows(path: Path, rows: list[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row}) if rows else ["customer_id"]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_incremental_batch(
    source_path: Path,
    target_path: Path,
    staging_path: Path,
    watermark: str,
) -> dict[str, object]:
    """Select, deduplicate and merge the batch into a staging copy of the target.

    The published target is not touched here. Writing to staging first keeps a
    failed run from leaving a half-written target behind.
    """
    source = read_rows(source_path)
    target = read_rows(target_path) if target_path.exists() else []
    merged, metrics = execute(source, target, watermark)
    write_rows(staging_path, merged)
    result = asdict(metrics)
    result["target_rows_before_merge"] = len(target)
    return result


def quality_gate(staging_path: Path, metrics: dict[str, object]) -> None:
    """Blocking checks before publication. Raises DataQualityError on failure."""
    staged = read_rows(staging_path)
    keys = [row["customer_id"] for row in staged]

    if len(keys) != len(set(keys)):
        raise DataQualityError("Duplicate customer_id values in staged target")

    if len(staged) != metrics["target_rows_after_merge"]:
        raise DataQualityError(
            f"Staged row count {len(staged)} does not match "
            f"merge result {metrics['target_rows_after_merge']}"
        )

    if len(staged) < int(metrics["target_rows_before_merge"]):
        raise DataQualityError("Target shrank after an upsert-only merge")


def publish_target(staging_path: Path, target_path: Path) -> None:
    """Swap the validated staging file into place in one atomic rename."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging_path, target_path)
