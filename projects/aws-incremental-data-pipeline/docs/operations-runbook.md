# Operations Runbook

## Purpose

Define predictable operational behavior for the incremental pipeline when batches succeed, fail, arrive late or need to be reprocessed.

## Success path

1. Read the last committed watermark.
2. Extract only rows newer than that watermark.
3. Validate schema and critical keys.
4. Deduplicate using stable business/technical keys plus change metadata.
5. Transform the incremental slice.
6. Merge/upsert into the target.
7. Run reconciliation and data quality checks.
8. Emit execution metrics.
9. Commit the new watermark only after all blocking validations pass.

## Failure handling

### Extraction failure
- keep the previous watermark unchanged;
- retry with the same logical batch identity;
- alert after the configured retry threshold.

### Transformation failure
- preserve the raw batch;
- capture rejected records and error context;
- fix the transformation or source-data issue;
- rerun only the affected batch.

### Target-write failure
- do not advance the watermark;
- use idempotent merge semantics so the same batch can be retried safely;
- validate target consistency before resuming normal scheduling.

### Data-quality failure
- block publication of the affected batch;
- persist metrics and failed rules;
- separate warning-level rules from blocking rules;
- advance the watermark only after the batch is accepted.

## Late-arriving data

A strict `updated_at > watermark` predicate can miss late events when source timestamps are not monotonic or delivery is delayed. Production designs should consider one of the following:

- CDC sequence/offset when available;
- a configurable lookback window followed by deduplication;
- ingestion timestamp managed by the platform;
- reconciliation against source control totals.

## Reprocessing procedure

1. Identify the logical batch, date or partition.
2. Confirm downstream impact.
3. Preserve current production state and metrics.
4. Reset or override the processing range without corrupting the global watermark.
5. Re-read the raw data for the affected range.
6. Apply deterministic transformation and merge logic.
7. Re-run data quality and reconciliation.
8. Confirm downstream freshness and row counts.
9. Close the incident with root cause and preventive action.

## Operational metrics

Recommended CloudWatch metrics/log fields:

- execution_id
- batch_id
- previous_watermark
- candidate_watermark
- committed_watermark
- input_rows
- incremental_rows
- output_rows
- rejected_rows
- duplicate_rows
- processing_duration_seconds
- data_freshness_seconds
- retry_count
- status

## Alert examples

- pipeline failed after retry threshold;
- no data received when data is expected;
- freshness above SLA;
- duplicate rate above threshold;
- rejected-row rate above threshold;
- runtime materially above recent baseline;
- source/target reconciliation mismatch.

## Core principle

The pipeline should expose enough state that an operator can answer three questions quickly:

1. What was the last successfully committed position?
2. What data did the failed execution attempt to process?
3. Can the exact same batch be replayed without corrupting the target?
