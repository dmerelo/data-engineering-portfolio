# Data Engineering Portfolio — Douglas Merelo

[![CI](https://github.com/dmerelo/meu-portifolio-de-dados/actions/workflows/ci.yml/badge.svg)](https://github.com/dmerelo/meu-portifolio-de-dados/actions/workflows/ci.yml)

Portfolio focused on **Data Engineering, AWS, Python, SQL, ETL/ELT, data pipelines, performance and cloud architecture**.

My strongest production experience is with **AWS, PySpark, Python, SQL and Airflow**. Projects involving Databricks, Snowflake, dbt and Azure are clearly identified as hands-on/reference labs rather than previous production experience.

## Selected professional impact

- Led the implementation of the V2 incremental-processing architecture for a national-scale telecom platform, replacing daily **full loads** that ran in **4–6 hour windows (up to 8 hours at peak)**.
- Reduced reprocessing, execution time, AWS resource consumption and cloud cost by approximately **60%**.
- Built and maintained approximately **20 production ETL/ELT pipelines** with AWS Glue (PySpark), Python and SQL, orchestrated with Airflow and Step Functions, in a critical environment processing millions of records.

> Proprietary code, datasets and internal architecture are not exposed. Public examples are sanitized or recreated as reference architectures.

## Project map

| Project | Stack / Concepts | What it demonstrates |
|---|---|---|
| [AWS Incremental Data Pipeline](./projects/aws-incremental-data-pipeline/) | AWS, Python, SQL, incremental, watermark, idempotency | **Flagship** — architecture, reprocessing, data quality, observability, performance and cloud cost |
| [AWS Redshift + DMS Modernization](./projects/aws-redshift-dms-modernization/) | Redshift, DMS, CDC, S3, SQL | Full load + CDC, OLTP → OLAP separation, migration validation and cutover thinking |
| [Databricks + PySpark Lakehouse](./projects/databricks-pyspark-lakehouse/) | PySpark, Delta concepts, Medallion | Bronze/Silver/Gold, deduplication, incremental processing, Spark performance and CI |
| [Snowflake + dbt Analytics Engineering](./projects/snowflake-dbt-analytics-engineering/) | Snowflake, dbt, SQL | Staging/intermediate/marts, tests, snapshots, incremental models and lineage-oriented structure |
| [Azure Data Engineering Platform](./projects/azure-data-engineering-platform/) | ADF, ADLS Gen2, Databricks, Key Vault | Transfer of core Data Engineering principles from AWS to Azure |

## Production-inspired flagship

### AWS Incremental Data Pipeline

The flagship is the closest public representation of the type of problem I have handled in production: evolving an expensive full-load pattern into a more efficient incremental architecture.

It includes:
- synthetic reproducible dataset;
- executable Python incremental pipeline;
- input validation and blocking data-quality rules;
- explicit watermark lifecycle;
- deterministic deduplication and idempotent merge/upsert behavior;
- execution metrics;
- automated tests;
- before-vs-after design analysis;
- operational runbook;
- failure and reprocessing strategy;
- architecture and trade-off documentation.

[Open the flagship project](./projects/aws-incremental-data-pipeline/)

## Code quality and repository standards

The repository includes engineering controls beyond example code:

- **pytest** for automated validation;
- **Ruff** for Python linting and formatting checks;
- **GitHub Actions CI** on pushes and pull requests to `main`;
- project-level Python configuration in `pyproject.toml`;
- `.gitignore` for generated data, local environments and development artifacts;
- deterministic outputs where practical to make reruns and tests predictable.

The flagship test suite covers incremental selection, deduplication, idempotent replay, watermark behavior and blocking input-quality failures.

## Core production stack

**AWS:** Glue (PySpark), Redshift, Athena, S3, Lambda, Step Functions, EventBridge, ECS, EC2, API Gateway, CloudWatch, IAM, Secrets Manager, DynamoDB, Aurora PostgreSQL  
**Orchestration:** Apache Airflow, AWS Step Functions  
**Languages:** Python, PySpark, SQL, Shell Script  
**Databases:** PostgreSQL, Aurora PostgreSQL, Oracle, SQL Server, MySQL, DynamoDB  
**Engineering:** ETL/ELT, Data Pipelines, Data Modeling, REST APIs, Docker, Git, CI/CD, monitoring and troubleshooting

## Engineering principles

- Process only what changed when possible
- Design pipelines to be idempotent
- Separate ingestion, transformation and consumption responsibilities
- Make reprocessing predictable
- Validate data before publication
- Monitor failures, latency, freshness and processing volume
- Optimize data layout before scaling compute
- Treat cost as an architectural metric
- Apply least privilege and avoid hard-coded credentials
- Document trade-offs instead of treating tools as interchangeable

## Hands-on expansion

Current practical study focuses on:

`Advanced SQL` · `AWS DMS / CDC` · `Databricks / Delta Lake` · `Snowflake` · `dbt` · `Azure Data Engineering`

## Supporting / historical work

- [Python CSV Transformation](./src/data_processing/tratar_fonte.py)
- [Jupyter study archive](https://github.com/dmerelo/Project_Jupyter_Notebooks)
- [Portfolio website](https://dmerelo.github.io/)

## About

**Douglas Merelo**  
Data Engineer | AWS (Glue · Redshift · Athena · Lambda) | PySpark · Airflow · Python · SQL | ETL/ELT

- [LinkedIn](https://www.linkedin.com/in/douglas-merelo/)
- [GitHub](https://github.com/dmerelo)
