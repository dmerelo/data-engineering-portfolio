"""PySpark job: incremental processing by date partition (AWS Glue style).

Sanitized recreation of the pattern used to replace a daily full reload. It does
not contain proprietary code or data. Instead of rereading the whole history, each
run processes only the date partitions that are new since the last committed one
(or the ones explicitly requested for reprocessing).

Flow per run:

    choose partitions -> read only those partitions -> deduplicate -> validate
    -> overwrite only those partitions in the curated layer

Reprocessing a day is idempotent: dynamic partition overwrite replaces that day's
partition and leaves every other partition untouched.

Runs locally with plain PySpark and on AWS Glue (``awsglue`` is used when present).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PARTITION_COL = "dt"
KEY_COL = "event_id"
ORDER_COL = "updated_at"


class DataQualityError(ValueError):
    """Raised when a batch violates a blocking quality rule."""


@dataclass(frozen=True)
class JobMetrics:
    partitions: list[str]
    rows_read: int
    rows_written: int
    duplicates_removed: int


def partitions_to_process(
    available: list[str],
    last_committed: str | None,
    reprocess: list[str] | None = None,
) -> list[str]:
    """Pick the date partitions for this run.

    Normal run: every available partition newer than the last committed one.
    Reprocessing: exactly the partitions requested, if they exist.
    """
    available_sorted = sorted(set(available))
    if reprocess:
        missing = sorted(set(reprocess) - set(available_sorted))
        if missing:
            raise ValueError(f"Requested partitions do not exist: {missing}")
        return sorted(set(reprocess))
    if last_committed is None:
        return available_sorted
    return [dt for dt in available_sorted if dt > last_committed]


def list_partitions(spark: SparkSession, path: str) -> list[str]:
    """List available ``dt`` values (only the partition column is selected)."""
    rows = spark.read.parquet(path).select(PARTITION_COL).distinct().collect()
    return sorted(str(row[PARTITION_COL]) for row in rows)


def read_partitions(spark: SparkSession, path: str, dts: list[str]) -> DataFrame:
    """Read only the selected partitions; the filter on ``dt`` enables partition pruning."""
    return spark.read.parquet(path).where(F.col(PARTITION_COL).cast("string").isin(dts))


def deduplicate_latest(df: DataFrame) -> DataFrame:
    """Keep the latest version of each key within its partition."""
    window = Window.partitionBy(PARTITION_COL, KEY_COL).orderBy(F.col(ORDER_COL).desc())
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def validate(df: DataFrame) -> None:
    """Blocking checks before the write."""
    null_keys = df.where(F.col(KEY_COL).isNull() | F.col(ORDER_COL).isNull()).count()
    if null_keys:
        raise DataQualityError(f"{null_keys} rows with null {KEY_COL} or {ORDER_COL}")

    duplicated = df.groupBy(PARTITION_COL, KEY_COL).count().where(F.col("count") > 1).count()
    if duplicated:
        raise DataQualityError(f"{duplicated} keys still duplicated after deduplication")


def write_partitions(df: DataFrame, path: str) -> None:
    """Overwrite only the partitions present in ``df``."""
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(PARTITION_COL)
        .parquet(path)
    )


def run(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    last_committed: str | None,
    reprocess: list[str] | None = None,
) -> JobMetrics:
    dts = partitions_to_process(list_partitions(spark, source_path), last_committed, reprocess)
    if not dts:
        return JobMetrics(partitions=[], rows_read=0, rows_written=0, duplicates_removed=0)

    raw = read_partitions(spark, source_path, dts).cache()
    rows_read = raw.count()

    curated = deduplicate_latest(raw)
    validate(curated)
    rows_written = curated.count()

    write_partitions(curated, target_path)
    raw.unpersist()

    return JobMetrics(
        partitions=dts,
        rows_read=rows_read,
        rows_written=rows_written,
        duplicates_removed=rows_read - rows_written,
    )


def parse_args(argv: list[str]) -> dict[str, str | None]:
    """Use Glue's resolver on AWS Glue, argparse everywhere else."""
    names = ["source_path", "target_path", "last_committed_dt", "reprocess_dts"]
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]

        resolved = getResolvedOptions(argv, ["source_path", "target_path"])
        for optional in names[2:]:
            if f"--{optional}" in argv:
                resolved.update(getResolvedOptions(argv, [optional]))
        return {name: resolved.get(name) or None for name in names}
    except ImportError:
        parser = argparse.ArgumentParser()
        parser.add_argument("--source_path", required=True)
        parser.add_argument("--target_path", required=True)
        parser.add_argument("--last_committed_dt")
        parser.add_argument("--reprocess_dts", help="Comma-separated dates, e.g. 2026-09-07")
        return vars(parser.parse_args(argv[1:]))


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("incremental_partition_job").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    reprocess = args["reprocess_dts"].split(",") if args["reprocess_dts"] else None
    metrics = run(
        spark,
        args["source_path"],
        args["target_path"],
        args["last_committed_dt"],
        reprocess,
    )
    # The orchestrator (Airflow / Step Functions) reads this line and commits the
    # new last_committed_dt only after the job succeeds.
    print(f"JOB_METRICS {metrics}")


if __name__ == "__main__":
    main(sys.argv)
