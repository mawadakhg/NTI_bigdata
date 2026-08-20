# EnerWise ETL Pipeline — Extract, Transform, Load

This document describes the three Spark jobs that move electricity consumption data from raw ingested batches to a dimensional model in Snowflake: `extraction.py`, `transformation.py`, and `loading.py`. All three run on YARN and are orchestrated by an Airflow DAG that triggers them once per day, one batch at a time.

## Pipeline overview

```
Raw zone (HDFS)  --extraction.py-->  Bronze layer (HDFS)
Bronze layer     --transformation.py-->  Silver layer + Gold star schema (HDFS)
Gold layer       --loading.py-->  Snowflake (POWER_DW.GOLD_LAYER)
```

Each layer represents a stage of data quality, following a Medallion architecture:

| Layer  | Format  | Content |
|--------|---------|---------|
| Raw    | JSON    | Ingested batch, as produced by the simulator, untouched |
| Bronze | Parquet | Same records, schema-validated, append-only |
| Silver | Parquet | Cleaned records: typed, deduplicated, nulls handled |
| Gold   | Parquet | Star schema (fact + dimensions), ready to load |
| Snowflake | Table | Permanent, incrementally maintained warehouse tables |

---

## 1. extraction.py — Raw → Bronze

**Purpose:** Validate and move one batch of raw JSON into the Bronze layer.

**Input:** HDFS path to a batch file, passed as a command-line argument by the Airflow DAG (via XCom).
```
spark-submit extraction.py /user/root/datalake/raw1/power_consumption/power_batch_0001.json
```

**Key steps:**
1. Set `HADOOP_USER_NAME=root` so Spark can read/write HDFS with the right permissions.
2. Read `sys.argv[1]` as the batch path; exit with an error if no argument is provided.
3. Enforce an explicit schema (`StructType`) instead of letting Spark infer it — faster and fails loudly on malformed records instead of silently guessing types.
4. Read the batch with `spark.read.schema(schema).json(batch_file)`.
5. If the batch has 0 records, skip the write (avoids empty Parquet files).
6. Otherwise, write to Bronze in `append` mode — Bronze is a historical, append-only layer.
7. Always call `spark.stop()` in a `finally` block to release YARN resources, whether the job succeeded or failed.

**Output:** Parquet files appended to `hdfs://hadoop-namenode:9000/user/root/datalake/bronzeLayer/power_consumption/`.

---

## 2. transformation.py — Bronze → Silver → Gold

**Purpose:** Clean the newly ingested Bronze data and build/update the star schema.

> Note: the Airflow DAG archives processed Bronze files after each run, so on any given run `transformation.py` only sees the current batch's Bronze data — not the full history.

### Cleaning (→ Silver)
1. Read all current Bronze data.
2. Parse `ReadingDate` into a proper date type and derive `date_key` (`yyyyMMdd` as integer).
3. Drop ingestion metadata columns that carry no analytical value: `source`, `batch_id`, `ingested_at`.
4. Deduplicate by `RecordID`.
5. Fill missing values — numeric columns default to `0`, categorical columns default to `"Unknown"`.
6. Re-cast columns to their correct types (`Integer`/`Double`).
7. Write the cleaned data to Silver in `append` mode.

### Star schema (→ Gold)
Built using Kimball dimensional modeling:

- **dim_date** — static dimension covering 2019–2030, generated once via `sequence()` + `explode()`. Written once and skipped on later runs.
- **dim_building_type**, **dim_hvac_system** — SCD Type 0 (no history tracking; values don't change over time). Loaded incrementally: a `left_anti` join against the existing dimension adds only genuinely new values. On the very first run (no existing file), the whole dimension is written directly.
- **fact_energy_consumption** — built by joining cleaned data against the two dimensions to pick up their surrogate keys, then selecting the final fact columns.
- **Surrogate keys** are `MD5` hashes of the natural key (e.g. `md5(BuildingType)`), used instead of the raw string as the join key — standard warehousing practice.

The fact table is written to Gold in `overwrite` mode: because Bronze at that point only contains the current batch, this Gold file represents the current batch's fact rows, acting as a staging area that `loading.py` merges into the permanent Snowflake table.

**Output paths:**
```
hdfs://.../goldLayer/dim_date
hdfs://.../goldLayer/dim_building_type
hdfs://.../goldLayer/dim_hvac_system
hdfs://.../goldLayer/fact_energy_consumption
```

---

## 3. loading.py — Gold → Snowflake

**Purpose:** Load the Gold layer into Snowflake (`POWER_DW.GOLD_LAYER`), using a load strategy suited to each table's size and change pattern.

| Table | Strategy | Why |
|---|---|---|
| `dim_date` | Load once, skip if already populated | Static, never changes |
| `dim_building_type`, `dim_hvac_system` | Full `overwrite` every run | Small tables, overwrite is cheap |
| `fact_energy_consumption` | Staging table + `MERGE INTO` | Large, continuously growing — needs incremental upsert |

### Dimension loading
- `load_dim_once_to_snowflake`: checks row count in Snowflake first (via a pushdown `COUNT(*)` query); skips the write if the table is already populated.
- `load_dimension_to_snowflake`: reads the full accumulated dimension from HDFS Gold and overwrites the Snowflake table directly — acceptable given the small size of these dimensions.

### Fact loading (merge/upsert)
1. Write the current batch's Gold fact data to a temporary staging table (`FACT_ENERGY_CONSUMPTION_STG`), overwriting it each run.
2. Check whether the permanent fact table already exists (`INFORMATION_SCHEMA.TABLES` query).
3. **First run:** table doesn't exist yet → write directly with `overwrite` (creates the table).
4. **Subsequent runs:** run a `MERGE INTO` statement — `UPDATE` matching `RecordID`s, `INSERT` new ones. This avoids rewriting the whole table every day and protects historical data if a run fails partway through.
5. Drop the staging table after the merge (cleanup).

The `MERGE INTO` statement is executed via `spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(...)` — a direct call into the underlying Java connector — because `MERGE` is not exposed as a DataFrame write operation by the Spark–Snowflake connector.

### Snowflake connection
Configured via `sf_options` (URL, user, password, database, schema, warehouse) at the top of the file.

**Security note:** credentials are currently hardcoded in plaintext for this training project. Before sharing or committing this code, move them to environment variables or Airflow Connections/Variables (e.g. `os.environ["SF_PASSWORD"]`).

---

## Running the pipeline manually

```bash
# 1. Extract one batch
spark-submit extraction.py /user/root/datalake/raw1/power_consumption/power_batch_0001.json

# 2. Transform Bronze -> Silver + Gold
spark-submit transformation.py

# 3. Load Gold -> Snowflake
spark-submit loading.py
```

In production, these are chained by the Airflow DAG (`power.py`), which also handles batch resolution, NiFi ingestion, and Bronze archiving between steps.

## Common configuration across all three jobs

- `master("yarn")` — jobs run on the YARN cluster, not in local mode.
- `spark.hadoop.fs.defaultFS` — points to the HDFS namenode.
- `spark.driver.host` / `spark.driver.bindAddress` — fixed driver IP so executors on other containers can reach back to it.
- `spark.executor.memory` / `spark.yarn.am.memory` — set to `512m`, sized for a local development cluster.
