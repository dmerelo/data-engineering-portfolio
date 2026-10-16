# Architecture Notes

## Goal

Design an incremental data pipeline that minimizes unnecessary reprocessing while remaining safe to rerun, observable and easy to recover.

## Logical layers

### 1. Raw / Landing

Purpose:
- preserve source data with minimal transformation;
- make ingestion traceable;
- retain enough metadata to reproduce processing.

Recommended metadata:
- `ingestion_timestamp`
- `source_system`
- `batch_id`
- `source_file` or source offset where applicable

### 2. Processing / Standardization

Responsibilities:
- schema validation;
- type normalization;
- deduplication;
- business-key validation;
- enrichment;
- incremental change selection.

### 3. Curated / Consumption

Responsibilities:
- analytical models;
- business-ready datasets;
- stable schemas for downstream consumers;
- optimized access patterns.

## Watermark lifecycle

A watermark records the last successfully processed change position.

Safe sequence:

```text
Read previous watermark
        |
        v
Read changed source data
        |
        v
Transform + validate
        |
        v
Write / merge target
        |
        v
Validate result
        |
        v
Commit new watermark
```

The watermark should not advance before the target write and validation complete successfully.

## Insert-only vs updates

### Insert-only source

An append strategy can be enough when records are immutable and guaranteed to be new.

### Mutable source

When existing records can change, use a stable key and an upsert/merge strategy.

Example matching key:

```text
customer_id
```

The key identifies the logical record. A timestamp, CDC sequence or source change indicator determines whether the incoming version is newer.

## Failure scenarios

### Failure during extraction
- do not advance watermark;
- retry extraction;
- preserve batch identity.

### Failure during transformation
- log rejected records and validation failures;
- keep raw input available;
- re-run only the failed batch.

### Failure during target write
- use idempotent write semantics;
- do not advance watermark;
- rerun the same logical batch.

### Duplicate delivery
- deduplicate on stable keys and version/change metadata;
- use merge/upsert where appropriate.

## Performance considerations

- filter as early as possible;
- read only required columns;
- prefer columnar formats such as Parquet for analytical files;
- partition based on real query and ingestion patterns;
- avoid excessive small files;
- avoid scanning unchanged history;
- monitor job duration and data volume trends.

## AWS mapping

Possible service mapping for this architecture:

| Responsibility | AWS option |
| --- | --- |
| Object storage / landing | Amazon S3 |
| Metadata/catalog | AWS Glue Data Catalog |
| Transformation | AWS Glue (PySpark) / Python / SQL |
| Event-driven processing | AWS Lambda / EventBridge |
| Analytical query over lake | Amazon Athena |
| Relational serving | Amazon Aurora PostgreSQL |
| Monitoring | Amazon CloudWatch |
| Secrets | AWS Secrets Manager |
| Permissions | AWS IAM |

The exact service choice depends on latency, data volume, transformation complexity and consumption pattern.

## Design questions

Before implementing, answer:

1. What guarantees that a record is new or changed?
2. What happens if the pipeline executes twice?
3. How is the last successful state persisted?
4. Can one failed partition be reprocessed independently?
5. How are duplicates handled?
6. What quality checks block publication?
7. What is the target SLA?
8. What metrics indicate performance degradation?
9. Which layer owns business rules?
10. How will cloud cost evolve as data volume grows?
