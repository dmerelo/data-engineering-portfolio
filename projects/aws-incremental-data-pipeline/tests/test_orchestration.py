import csv
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

import orchestration  # noqa: E402
from incremental_pipeline import DataQualityError  # noqa: E402

FIELDS = ["customer_id", "amount", "updated_at"]


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


@pytest.fixture
def paths(tmp_path: Path) -> dict[str, Path]:
    source = tmp_path / "incremental_batch.csv"
    target = tmp_path / "curated" / "customers.csv"
    write_csv(
        target,
        [
            {"customer_id": "1", "amount": "100", "updated_at": "2026-09-01T00:00:00+00:00"},
            {"customer_id": "2", "amount": "200", "updated_at": "2026-09-01T00:00:00+00:00"},
        ],
    )
    write_csv(
        source,
        [
            {"customer_id": "2", "amount": "250", "updated_at": "2026-09-07T12:00:00+00:00"},
            {"customer_id": "3", "amount": "300", "updated_at": "2026-09-07T13:00:00+00:00"},
        ],
    )
    return {
        "source": source,
        "target": target,
        "staging": tmp_path / "staging" / "customers_run.csv",
        "state": tmp_path / "state" / "watermark.json",
    }


def run_dag_steps(p: dict[str, Path], run_id: str = "run_1") -> dict:
    """Same order as the Airflow DAG."""
    watermark = orchestration.read_watermark(p["state"])
    metrics = orchestration.run_incremental_batch(p["source"], p["target"], p["staging"], watermark)
    orchestration.quality_gate(p["staging"], metrics)
    orchestration.publish_target(p["staging"], p["target"])
    orchestration.commit_watermark(p["state"], metrics["candidate_watermark"], run_id)
    return metrics


def test_first_run_uses_default_watermark(paths):
    assert orchestration.read_watermark(paths["state"]) == orchestration.DEFAULT_WATERMARK


def test_full_flow_publishes_target_and_commits_watermark(paths):
    metrics = run_dag_steps(paths)

    target = {row["customer_id"]: row["amount"] for row in read_csv(paths["target"])}
    assert target == {"1": "100", "2": "250", "3": "300"}
    assert metrics["rows_selected"] == 2
    assert orchestration.read_watermark(paths["state"]) == "2026-09-07T13:00:00+00:00"
    assert not paths["staging"].exists()


def test_rerun_after_commit_selects_nothing_and_keeps_target(paths):
    run_dag_steps(paths, "run_1")
    before = read_csv(paths["target"])

    metrics = run_dag_steps(paths, "run_2")

    assert metrics["rows_selected"] == 0
    assert read_csv(paths["target"]) == before
    assert orchestration.read_watermark(paths["state"]) == "2026-09-07T13:00:00+00:00"


def test_failed_quality_gate_leaves_target_and_watermark_untouched(paths):
    target_before = paths["target"].read_text(encoding="utf-8")
    metrics = orchestration.run_incremental_batch(
        paths["source"], paths["target"], paths["staging"], orchestration.DEFAULT_WATERMARK
    )
    metrics["target_rows_after_merge"] = 99  # simulate a reconciliation mismatch

    with pytest.raises(DataQualityError):
        orchestration.quality_gate(paths["staging"], metrics)

    assert paths["target"].read_text(encoding="utf-8") == target_before
    assert not paths["state"].exists()


def test_retry_after_failure_before_commit_is_idempotent(paths):
    watermark = orchestration.read_watermark(paths["state"])
    metrics = orchestration.run_incremental_batch(
        paths["source"], paths["target"], paths["staging"], watermark
    )
    orchestration.quality_gate(paths["staging"], metrics)
    orchestration.publish_target(paths["staging"], paths["target"])
    # Worker dies here: target published, watermark not committed.

    run_dag_steps(paths, "retry")

    keys = [row["customer_id"] for row in read_csv(paths["target"])]
    assert keys == ["1", "2", "3"]
    assert orchestration.read_watermark(paths["state"]) == "2026-09-07T13:00:00+00:00"


def test_quality_gate_rejects_duplicate_keys(paths):
    write_csv(
        paths["staging"],
        [
            {"customer_id": "1", "amount": "1", "updated_at": "2026-09-01T00:00:00+00:00"},
            {"customer_id": "1", "amount": "2", "updated_at": "2026-09-02T00:00:00+00:00"},
        ],
    )
    metrics = {"target_rows_after_merge": 2, "target_rows_before_merge": 0}

    with pytest.raises(DataQualityError, match="Duplicate"):
        orchestration.quality_gate(paths["staging"], metrics)
