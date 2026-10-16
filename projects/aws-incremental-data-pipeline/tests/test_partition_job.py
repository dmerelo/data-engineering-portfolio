import importlib.util
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
from pyspark.sql import SparkSession  # noqa: E402

JOB_PATH = Path(__file__).resolve().parents[1] / "glue" / "incremental_partition_job.py"
spec = importlib.util.spec_from_file_location("incremental_partition_job", JOB_PATH)
job = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = job
spec.loader.exec_module(job)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("partition-job-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_source(spark, path: Path, rows: list[tuple]) -> None:
    schema = "event_id string, amount long, updated_at string, dt string"
    spark.createDataFrame(rows, schema).write.mode("append").partitionBy("dt").parquet(str(path))


def read_target(spark, path: Path) -> list[tuple]:
    df = spark.read.parquet(str(path))
    rows = df.select("dt", "event_id", "amount").collect()
    return sorted((str(r.dt), r.event_id, r.amount) for r in rows)


@pytest.fixture
def source(spark, tmp_path):
    path = tmp_path / "raw"
    write_source(
        spark,
        path,
        [
            ("a", 10, "2026-09-05T08:00:00", "2026-09-05"),
            ("b", 20, "2026-09-05T09:00:00", "2026-09-05"),
            ("a", 11, "2026-09-06T08:00:00", "2026-09-06"),
            ("a", 12, "2026-09-06T10:00:00", "2026-09-06"),  # later version of the same key
            ("c", 30, "2026-09-07T08:00:00", "2026-09-07"),
        ],
    )
    return path


def test_partitions_to_process_normal_run():
    available = ["2026-09-05", "2026-09-06", "2026-09-07"]
    assert job.partitions_to_process(available, "2026-09-05") == ["2026-09-06", "2026-09-07"]
    assert job.partitions_to_process(available, None) == available
    assert job.partitions_to_process(available, "2026-09-07") == []


def test_partitions_to_process_reprocess_only_requested():
    available = ["2026-09-05", "2026-09-06"]
    assert job.partitions_to_process(available, "2026-09-06", ["2026-09-05"]) == ["2026-09-05"]
    with pytest.raises(ValueError, match="do not exist"):
        job.partitions_to_process(available, None, ["2026-09-09"])


def test_run_reads_only_new_partitions_and_deduplicates(spark, source, tmp_path):
    target = tmp_path / "curated"

    metrics = job.run(spark, str(source), str(target), last_committed="2026-09-05")

    assert metrics.partitions == ["2026-09-06", "2026-09-07"]
    assert metrics.rows_read == 3
    assert metrics.duplicates_removed == 1
    assert read_target(spark, target) == [("2026-09-06", "a", 12), ("2026-09-07", "c", 30)]


def test_reprocessing_one_day_is_idempotent_and_leaves_other_days(spark, source, tmp_path):
    target = tmp_path / "curated"
    job.run(spark, str(source), str(target), last_committed=None)
    before = read_target(spark, target)

    metrics = job.run(spark, str(source), str(target), None, reprocess=["2026-09-06"])

    assert metrics.partitions == ["2026-09-06"]
    assert read_target(spark, target) == before


def test_nothing_new_writes_nothing(spark, source, tmp_path):
    target = tmp_path / "curated"

    metrics = job.run(spark, str(source), str(target), last_committed="2026-09-07")

    assert metrics.partitions == []
    assert not target.exists()


def test_null_key_blocks_the_write(spark, tmp_path):
    source = tmp_path / "raw_bad"
    target = tmp_path / "curated"
    write_source(spark, source, [(None, 1, "2026-09-08T08:00:00", "2026-09-08")])

    with pytest.raises(job.DataQualityError, match="null"):
        job.run(spark, str(source), str(target), last_committed=None)

    assert not target.exists()
