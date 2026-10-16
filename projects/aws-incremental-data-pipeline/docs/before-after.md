# Before vs After — Full Load to Incremental

This document summarizes the engineering reasoning behind replacing a recurring full-load pattern with incremental processing.

> The numbers below are presented as sanitized portfolio context inspired by a real production case. They are intended to explain the decision process without exposing proprietary implementation details.

## Before: daily full load

Characteristics:
- historical data was reprocessed on each execution;
- jobs typically ran for several hours;
- more data was read and transformed than necessary;
- resource demand increased as history grew;
- failures were more expensive because reruns repeated large portions of work.

Observed production context:
- processing windows were approximately 4–6 hours, reaching up to 8 hours at peak;
- the environment supported about 20 pipelines and millions of records.

## After: incremental processing

Characteristics:
- processing focused on new or changed records;
- watermark/state controlled the last successful position;
- failed logical batches could be reprocessed independently;
- target writes were designed to be idempotent;
- partition-aware processing reduced unnecessary scans.

## Result

The production case that inspired this reference project showed an approximate **60% reduction in AWS resource consumption**, based on before/after comparison of:

- processing time;
- resource consumption;
- cloud cost.

The exact internal infrastructure and proprietary implementation are intentionally omitted.

## Why the improvement happens

The core effect is not a single AWS service optimization. It comes from changing the amount of work performed by the system.

```text
Full load
Total work per run ≈ complete historical dataset

Incremental
Total work per run ≈ new + changed data
```

As the ratio of changed data to historical data becomes smaller, the incremental architecture avoids increasing amounts of unnecessary I/O, compute and transformation work.

## Metrics to compare in a real implementation

| Metric | Full load expectation | Incremental expectation |
| --- | --- | --- |
| Rows read | Entire relevant history | New/changed slice |
| Data scanned | High and grows with history | More closely tied to daily change volume |
| Job duration | Tends to grow with history | More stable when change rate is stable |
| Reprocessing cost | High | Isolated by batch/partition |
| Failure blast radius | Larger | Smaller logical batch |
| Cloud cost | More tied to historical size | More tied to actual change volume |

## Important caveat

Incremental processing adds state and operational complexity. It requires reliable change detection, watermark handling, idempotent writes, late-arriving data strategy and explicit recovery behavior. For small datasets, a full load can still be the better engineering choice.
