# Retail Store Daily Sales & Inventory Aggregator

> **Level:** Level 1 — Beginner | **Domain:** Retail & Multi-Store Operations  
> **Tech Stack:** Python, SQL, SQLite / PostgreSQL, Pandas / DictReader, Git, Pytest

---

## 🚀 Business Problem & Objectives

undefined

**Primary Data Engineering Deliverables:**
- **Task 1:** Repository Architecture & CSV Ingestion Engine — *Set up the repository structure with dedicated `src/` modules, and implement `ingest_raw_csv` in `pipeline.py` to read transaction records safely with strict type conversions.*
- **Task 2:** Data Cleaning, Deduplication & Quality Enforcer — *Implement `clean_transactions(records)` to remove duplicate transactions (keeping the earliest timestamp or primary record) and filter out business rule violations (quantity <= 0 or unit_price <= 0).*
- **Task 3:** Relational Schema Design & SQL DDL Definition — *Write clean, production-ready SQL DDL statements in `schema.sql` defining `stg_pos_transactions` and `daily_store_sales_summary` with appropriate primary keys, constraints, and indexes.*
- **Task 4:** Idempotent Database Loading & Upsert Logic — *Implement `load_clean_transactions(conn, clean_records)` in `pipeline.py` using idempotent SQL logic (`INSERT OR REPLACE` / `ON CONFLICT DO UPDATE`) to ensure deterministic pipeline execution.*
- **Task 5:** Analytical Summary Transformation & Git Release Milestone — *Implement `generate_daily_summary(conn, target_date)` using SQL aggregation (`DATE(txn_timestamp)`, `SUM(net_amount)`, `COUNT(DISTINCT txn_id)`) to populate `daily_store_sales_summary`, followed by finalizing project documentation and tagging a Git release.*

---

## 🏛️ Pipeline Architecture & Data Flow

```text
Raw Daily POS CSVs → Staging Buffer → Validation & Cleaning Module (Deduplication & Type Enforcer) → Idempotent SQLite Database Loader → Daily Store Summary Mart
```

### Data Layers
1. **Raw / Ingestion Layer:** Ingests raw unvalidated source feeds with strict schema validation.
2. **Staging / Cleansing Layer:** Enforces deduplication, null-handling, type safety, and quarantines corrupted records into Dead Letter Queues (DLQ).
3. **Analytical Warehouse / Mart:** Optimized Star Schema / Medallion models with surrogate keys, surrogate indexes, and high-watermark incremental upserts.
4. **Orchestration & Quality:** Scheduled via Airflow DAGs with retry policies, SLA monitors, and automated data quality test suites.

---

## ⚖️ Engineering Decisions & Trade-Off Matrix

| Component | Technical Decision | Core Justification | Incurred Trade-Off | Scalable Alternative |
| :--- | :--- | :--- | :--- | :--- |
| **Repository Architecture & CSV Ingestion Engine** | Implement explicit schema casting at the ingestion boundary and quarantine or log unparseable rows rather than silent failure. | Failing fast or logging malformed rows at the extraction boundary prevents bad types from propagating into the warehouse. | Row-by-row Python DictReader has moderate throughput for gigantic files (>10GB), where PySpark/DuckDB is preferred. | In enterprise data lakehouses, files are landed in S3/GCS as raw objects and ingested using AWS Glue, Snowflake Snowpipe, or Databricks Auto Loader. |
| **Data Cleaning, Deduplication & Quality Enforcer** | Establish an explicit cleaning stage with a dead-letter quarantine output instead of silently discarding bad records. | Quarantining enables financial audits to reconcile missing revenue without polluting analytical dashboards. | In-memory sets require sufficient memory; for larger datasets, distributed hash partitioning or SQL window functions are used. | Enterprise tools like Great Expectations or dbt tests enforce constraints and route invalid records into Dead Letter Queues (DLQ). |
| **Relational Schema Design & SQL DDL Definition** | Establish a staging table with `txn_id` PK and a summary table with `(report_date, store_id)` composite PK. | Enforcing primary keys guarantees database-level integrity against duplicate ingestion jobs. | Index maintenance incurs slight write overhead during inserts, but accelerates reporting queries significantly. | In Snowflake/BigQuery, traditional indexes are replaced by clustering keys and micro-partitioning. |
| **Idempotent Database Loading & Upsert Logic** | Implement atomic `INSERT ... ON CONFLICT DO UPDATE` inside explicit transaction blocks. | Guarantees data consistency across unexpected job re-executions and historical backfills. | Upserts require checking unique indexes which has slightly higher write latency than append-only tables. | In Big Data lakehouses (Databricks Delta Lake / Apache Iceberg), `MERGE INTO target USING source ON target.id = source.id` is standard. |
| **Analytical Summary Transformation & Git Release Milestone** | Build a pre-aggregated daily summary mart table with composite primary key `(report_date, store_id)`. | Pre-aggregating daily metrics delivers sub-second dashboard response times for executives and inventory managers. | Requires pipeline execution after business close, introducing a slight batch latency compared to streaming. | In modern data stacks, this aggregation is managed as a dbt incremental model or Snowflake Dynamic Table. |

---

## 🛡️ Production Failure Modes & Recovery

This pipeline is engineered to handle real-world edge cases:
- [x] **Duplicate transaction IDs caused by offline POS syncing**
- [x] **Corrupt negative prices and missing customer store IDs**
- [x] **Late-arriving transaction files from previous business dates**
- [x] **Non-idempotent re-runs creating duplicate daily totals**

---

## 📦 How to Run Locally

```bash
# 1. Clone repository
git clone https://github.com/your-username/project-1.git
cd project-1

# 2. Build Docker container
docker build -t project-1:latest .

# 3. Run automated tests and pipeline
docker run --rm project-1:latest
```

*Project created using the Data Engineering Mastery Platform.*
