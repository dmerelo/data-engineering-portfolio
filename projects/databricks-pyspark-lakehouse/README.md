# Databricks + PySpark Lakehouse Reference Project

Reference project focused on scalable transformations with PySpark, Delta Lake and Medallion architecture.

> This project demonstrates study and hands-on portfolio work. It does not claim previous production experience with Databricks.

## Scenario

A company receives daily customer and transaction files. The goal is to build a maintainable Lakehouse pipeline with incremental processing, data quality checks and curated analytical outputs.

## Architecture

```text
Source Files / APIs
       |
       v
Bronze - raw ingestion
       |
       v
Silver - cleaned, deduplicated, standardized
       |
       v
Gold - analytical aggregates / marts
```

## Technologies

- PySpark
- Delta Lake concepts
- Databricks-oriented workflow
- Medallion architecture
- Incremental processing
- MERGE/upsert
- Data quality checks
- Partition-aware processing

## Engineering goals

- Avoid unnecessary full reloads
- Make the pipeline idempotent
- Separate raw, cleaned and business-ready layers
- Reduce shuffle and unnecessary scans
- Handle schema and duplicate records explicitly
- Create reliable outputs for analytics

## Repository contents

- `src/bronze_ingestion.py`
- `src/silver_transform.py`
- `src/gold_aggregate.py`
- `src/incremental_merge.py`
- `tests/test_data_quality.py`
- `docs/performance-notes.md`

## Design considerations

### Incremental processing
A watermark or ingestion date is used to identify the new batch. Only changed data is transformed and merged into the target.

### Idempotency
Reprocessing the same batch should not create duplicates. Business keys and MERGE logic are used to preserve consistency.

### Shuffle control
Filters and projections are applied early. Join strategies should consider dataset size, partitioning and broadcast opportunities.

### Data quality
Checks include null critical keys, duplicates, invalid values and freshness.

### Observability
A production implementation should capture row counts, rejected records, processing duration, freshness and failure context.

## AWS analogy

For an AWS-focused engineer, this architecture can be mapped conceptually to:

- S3 raw/processed layers
- Glue/Spark jobs for transformations
- Athena/Redshift for analytical access
- CloudWatch for monitoring

The core engineering principles remain the same: incremental processing, partitioning, data quality, lineage and reliable reprocessing.

## Portfolio context

This project is a hands-on extension of principles I use in AWS-oriented Data Engineering. For the main production-inspired case with executable incremental logic, watermarking, idempotency, tests and operational recovery, see the **[AWS Incremental Data Pipeline flagship](../aws-incremental-data-pipeline/)**.
