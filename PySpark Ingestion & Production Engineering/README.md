# 🚀 PySpark Production Data Engineering Masterclass: 10-Day Zero-to-Hero
> **A Comprehensive, Production-Grade Curriculum: From Medallion Architecture and Cluster Tuning to Cloud PostgreSQL Ingestion and Custom DAG Orchestration**

[![PySpark](https://img.shields.io/badge/PySpark-3.5%2B%20%2F%204.0%2B-F26522?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-008080?style=for-the-badge)](https://www.databricks.com/glossary/medallion-architecture)
[![Cloud Database](https://img.shields.io/badge/Cloud%20Database-Neon%20Postgres-00E599?style=for-the-badge&logo=postgresql&logoColor=white)](https://neon.tech)
[![Orchestrator](https://img.shields.io/badge/Orchestrator-Custom%20DAG%20with%20Retries-FF6F00?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](#)

---

## 📌 Table of Contents
1. [Overview & Engineering Philosophy](#-overview--engineering-philosophy)
2. [Master 10-Day Curriculum & Learning Roadmap](#-master-10-day-curriculum--learning-roadmap)
3. [Architecture & Design Blueprints](#-architecture--design-blueprints)
   - [Medallion Lakehouse Flow](#1-medallion-lakehouse-flow)
   - [Data Quality Quarantine Pattern](#2-data-quality-quarantine-pattern)
   - [Incremental Ingestion & Watermarking](#3-incremental-ingestion--watermarking)
   - [Spark Query Execution & Optimization](#4-spark-query-execution--optimization)
   - [Analytical Window Function Mechanics](#5-analytical-window-function-mechanics)
   - [Custom DAG Orchestrator Engine](#6-custom-dag-orchestrator-engine)
4. [Day-by-Day Detailed Modules](#-day-by-day-detailed-modules)
   - [Day 01: Medallion Architecture Fundamentals](#day-01-medallion-architecture-fundamentals)
   - [Day 02: Production Data Quality & Validation Framework](#day-02-production-data-quality--validation-framework)
   - [Day 03: Incremental Ingestion & Metadata Tracking](#day-03-incremental-ingestion--metadata-tracking)
   - [Day 04: Spark Performance Optimization & Partitioning](#day-04-spark-performance-optimization--partitioning)
   - [Day 05: Advanced Joins & Data Modeling](#day-05-advanced-joins--data-modeling)
   - [Day 06: Analytical Window Functions Deep Dive](#day-06-analytical-window-functions-deep-dive)
   - [Day 07: Modular Production Pipeline Architecture](#day-07-modular-production-pipeline-architecture)
   - [Day 08: Enterprise Logging, Configuration & Testing](#day-08-enterprise-logging-configuration--testing)
   - [Day 09: Cloud PostgreSQL JDBC & Lakehouse Ingestion](#day-09-cloud-postgresql-jdbc--lakehouse-ingestion)
   - [Day 10: Custom DAG Pipeline Orchestrator with Retries](#day-10-custom-dag-pipeline-orchestrator-with-retries)
5. [Repository Structure](#-repository-structure)
6. [Environment Setup & Installation](#-environment-setup--installation)
   - [Windows WinUtils & Hadoop Configuration](#windows-winutils--hadoop-configuration)
   - [Python Dependencies](#python-dependencies)
   - [PostgreSQL JDBC Driver Setup](#postgresql-jdbc-driver-setup)
7. [Production Best Practices & Interview Guide](#-production-best-practices--interview-guide)

---

## 🧠 Overview & Engineering Philosophy

In modern data engineering, reading a tutorial or writing a simple script is not enough to survive in production. Real-world systems require:
- **Resilient Lakehouse Architecture**: Ensuring immutable raw records while delivering pristine business aggregations.
- **Fail-Safe Data Quality Guardrails**: Quarantining malformed records into dead-letter tables without halting critical pipelines.
- **Incremental Processing**: Eliminating costly full-table re-scans via watermarks and metadata state audit tables.
- **Performance Mastery**: Understanding lazy evaluation, physical file layout, partition pruning, and broadcast joins to prevent memory bottlenecks.
- **Enterprise Engineering Practices**: Centralized config, structured JSON/file logging, SLA threshold alerts, and automated PySpark unit test suites.
- **Hybrid Cloud Ingestion**: Querying cloud relational databases (Neon Serverless PostgreSQL) via JDBC and transforming them distributedly.
- **Fault-Tolerant Orchestration**: Implementing custom Airflow-style DAG dependency engines with automated retries.

This repository provides **two reading formats**:
1. **Unified Interactive Masterclass**: [`PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb`](./PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb)
2. **Modular Day-by-Day Reading Tracks**: [`modules/`](./modules/) containing standalone, step-by-step notebooks for each day.

---

## 🗺️ Master 10-Day Curriculum & Learning Roadmap

```
                          ┌────────────────────────────────────────────────────────┐
                          │          10-DAY PRODUCTION ENGINEERING TRACK           │
                          └──────────────────────────┬─────────────────────────────┘
                                                     │
       ┌───────────────────┬─────────────────────────┼─────────────────────────┬───────────────────┐
       ▼                   ▼                         ▼                         ▼                   ▼
   [Day 01-03]         [Day 04]                  [Day 05-06]               [Day 07-08]         [Day 09-10]
  Medallion &         Performance               Joins, Window             Modular ETL &       Cloud Postgres &
  Incremental        Optimization &             Functions &              Production SLAs     Custom DAG Engine
   Ingestion          Partitioning                Modeling                 w/ Testing            w/ Retries
```

| Day | Module Notebook | Core Focus & Techniques | Output Layer / Artifacts |
|:---:|:---|:---|:---:|
| **01** | [**Day 01: Medallion Architecture**](./modules/Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb) | Explicit `StructType` schema, CSV ingestion, Bronze Parquet, Silver data cleansing, Gold business aggregations | `bronze_orders`, `silver_orders`, `gold_category`, `gold_city` |
| **02** | [**Day 02: Production Data Quality**](./modules/Day_02_Data_Quality_Validation_and_Quarantine.ipynb) | Validation rules, bad record quarantine (`rejected_orders`), rejection reason tagging, audit report, Python logging | `silver_orders_day2`, `rejected_orders_day2` |
| **03** | [**Day 03: Incremental Ingestion**](./modules/Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb) | Change detection, watermarking (`ingestion_timestamp`), metadata state audit log (`metadata/processed_files.csv`) | `bronze_incremental` (Appended Parquet) |
| **04** | [**Day 04: Performance & Partitioning**](./modules/Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb) | `repartition()` vs `coalesce()`, `partitionBy()`, partition pruning, broadcast joins, `.explain()`, caching, benchmarks | `partitioned_orders` |
| **05** | [**Day 05: Joins & Data Modeling**](./modules/Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb) | Star schema modeling, Inner/Left/Right/Full joins, column aliasing (`orders.alias("o")`), `when/otherwise`, multi-metric aggregation | Curated Business Mart |
| **06** | [**Day 06: Analytical Window Functions**](./modules/Day_06_Analytical_Window_Functions_Deep_Dive.ipynb) | `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, running revenue totals, moving average, Top-N ranking | Analytical Customer Mart |
| **07** | [**Day 07: Modular Production ETL**](./modules/Day_07_Modular_Production_ETL_Architecture.ipynb) | Decoupled functions (`ingest`, `validate`, `dedup`, `transform`, `write`), single-responsibility pipeline driver | End-to-End Modular Driver |
| **08** | [**Day 08: Enterprise Logging & Testing**](./modules/Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb) | Central `CONFIG`, file logging (`pipeline.log`), exception isolation, SLA rejection limits, automated PySpark unit test assertions | `pipeline_report.json`, `pipeline.log` |
| **09** | [**Day 09: Cloud PostgreSQL JDBC**](./modules/Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb) | Neon Serverless PostgreSQL, JDBC driver integration, `psycopg2` + Spark bridge, hybrid lakehouse aggregation | Lakehouse Parquet Tables |
| **10** | [**Day 10: Custom DAG Orchestrator**](./modules/Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb) | Custom Airflow-style DAG engine (`SimpleOrchestrator`), task dependency chaining, automated retries, fault injection testing | Orchestration Engine |

---

## 🏗️ Architecture & Design Blueprints

### 1. Medallion Lakehouse Flow
```
  Raw Source Files                Bronze Layer                 Silver Layer                  Gold Layer
  (CSV / JSON / JDBC)           (Immutable Raw)           (Cleaned & Enriched)          (Curated Business)
 ┌───────────────────┐        ┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
 │ orders.csv        │ ────►  │ bronze_orders/   │ ────►  │ silver_orders/   │ ────►  │ gold_category/   │
 │ (External source) │        │ (Parquet copy)   │        │ (Deduplicated,   │        │ gold_city/       │
 └───────────────────┘        └──────────────────┘        │  imputed, typed) │        │ (Aggregated)     │
                                                          └──────────────────┘        └──────────────────┘
```

### 2. Data Quality Quarantine Pattern
```
                                 Incoming Raw Batch
                                         │
                                         ▼
                            ┌─────────────────────────┐
                            │ Schema & DQ Validation  │
                            │ (Null, Type, Bounds)    │
                            └────────────┬────────────┘
                                         │
                     ┌───────────────────┴───────────────────┐
                     ▼                                       ▼
            [Pristine Records]                       [Tainted Records]
                     │                                       │
                     ▼                                       ▼
         ┌───────────────────────┐              ┌────────────────────────┐
         │ silver_orders/        │              │ rejected_orders/       │
         │ (Cleaned Business DB) │              │ (rejection_reason tag) │
         └───────────────────────┘              └────────────────────────┘
```

### 3. Incremental Ingestion & Watermarking
```
 Source Files Directory                   Audit Metadata Table                    Bronze Parquet
 ┌──────────────────────┐               ┌───────────────────────┐            ┌──────────────────────┐
 │ source/              │               │ metadata/             │            │ bronze_incremental/  │
 │ ├── orders_day1.csv  │  ──[Diff]──►  │ processed_files.csv   │ ──Append─► │ ├── part-0001        │
 │ ├── orders_day2.csv  │               │                       │            │ ├── part-0002        │
 │ └── orders_day3.csv  │               │ [file_name, timestamp]│            │ └── ingestion_ts=... │
 └──────────────────────┘               └───────────────────────┘            └──────────────────────┘
```

### 4. Spark Query Execution & Optimization
```
   repartition(N)                                    coalesce(N)
   ---------------------------------                 ---------------------------------
   • Increases or decreases partitions                • ONLY decreases partitions
   • Triggers full cluster network SHUFFLE           • Merges adjacent partitions locally
   • Balances data evenly across cluster             • Avoids network shuffle overhead
   • Use before heavy aggregations/joins             • Use immediately before writing to disk
```

### 5. Analytical Window Function Mechanics
```
  Customer 101 Orders:
  Date       Amount   row_number()   rank()   dense_rank()   lag(1)   Running Total
  ---------------------------------------------------------------------------------
  2026-09-01  55,000       1           1           1          NULL        55,000
  2026-09-02  55,000       2           1           1         55,000      110,000
  2026-09-03  12,000       3           3           2         55,000      122,000
  2026-09-04     800       4           4           3         12,000      122,800
```

### 6. Custom DAG Orchestrator Engine
```
                  ┌──────────────┐
                  │ Ingest Task  │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ Validate DQ  │
                  └──────┬───────┘
                         │
         ┌───────────────┴───────────────┐
    [Success]                        [Failure]
         │                               │
         ▼                               ▼
  ┌──────────────┐             ┌───────────────────┐
  │  Transform   │             │ Retry Attempt (3) │
  └──────┬───────┘             └─────────┬─────────┘
         │                               │
         ▼                     ┌─────────┴─────────┐
  ┌──────────────┐             ▼                   ▼
  │  Write Gold  │        [Retry OK]          [Fatal Halt]
  └──────────────┘             │                   │
                               ▼                   ▼
                         Continue DAG         Abort Pipeline
```

---

## 📚 Day-by-Day Detailed Modules

### Day 01: Medallion Architecture Fundamentals
- **Notebook**: [`Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb`](./modules/Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb)
- **Topics**: Explicit schema definition using `StructType` & `StructField`, CSV loading options, Bronze immutable lakehouse layer, Silver cleansing (`fillna`, `to_date`, casting), derived columns (`revenue = quantity * price`), Gold analytical aggregations, Parquet writing.
- **Key Code Snippet**:
  ```python
  order_schema = StructType([
      StructField("order_id", IntegerType(), False),
      StructField("customer_id", IntegerType(), True),
      StructField("product", StringType(), True),
      StructField("category", StringType(), True),
      StructField("quantity", IntegerType(), True),
      StructField("price", DoubleType(), True),
      StructField("order_date", StringType(), True),
      StructField("city", StringType(), True)
  ])
  bronze_df = spark.read.schema(order_schema).csv("orders.csv", header=True)
  bronze_df.write.mode("overwrite").parquet("bronze_orders")
  ```

---

### Day 02: Production Data Quality & Validation Framework
- **Notebook**: [`Day_02_Data_Quality_Validation_and_Quarantine.ipynb`](./modules/Day_02_Data_Quality_Validation_and_Quarantine.ipynb)
- **Topics**: Dirty data handling, schema validation rules, column-level assertions (`isNull`, `<= 0`), quarantine routing, creating tagged `rejection_reason` columns, automated data reconciliation reporting, standard Python logging.
- **Key Code Snippet**:
  ```python
  invalid_condition = (
      col("customer_id").isNull() |
      col("quantity").isNull() | (col("quantity") <= 0) |
      col("price").isNull() | (col("price") <= 0) |
      col("parsed_date").isNull()
  )
  valid_df = validated_df.filter(~invalid_condition).dropDuplicates(["order_id"])
  rejected_df = validated_df.filter(invalid_condition).withColumn(
      "rejection_reason",
      when(col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
      .when(col("quantity") <= 0, "INVALID_QUANTITY")
      .otherwise("INVALID_DATE_FORMAT")
  )
  ```

---

### Day 03: Incremental Ingestion & Metadata Tracking
- **Notebook**: [`Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb`](./modules/Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb)
- **Topics**: Incremental processing architecture, file globbing, differential comparison against metadata ledger (`metadata/processed_files.csv`), adding ingestion watermarks (`ingestion_timestamp`), atomic Parquet appends.
- **Key Code Snippet**:
  ```python
  source_files = [os.path.basename(f) for f in glob.glob("source/*.csv")]
  processed_files = pd.read_csv("metadata/processed_files.csv")["file_name"].tolist()
  new_files = [f for f in source_files if f not in processed_files]

  if new_files:
      new_paths = [f"source/{f}" for f in new_files]
      new_orders = spark.read.schema(order_schema).csv(new_paths, header=True)
      new_orders = new_orders.withColumn("ingestion_timestamp", current_timestamp())
      new_orders.write.mode("append").parquet("bronze_incremental")
  ```

---

### Day 04: Spark Performance Optimization & Partitioning
- **Notebook**: [`Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb`](./modules/Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb)
- **Topics**: Execution engine internals, default parallelism, `repartition` (wide shuffle) vs `coalesce` (narrow merge), physical `partitionBy("order_date")`, verifying partition pruning with `.explain()`, broadcast joins (`broadcast(small_df)`), memory caching lifecycle (`cache()`, `unpersist()`), empirical query runtime benchmarks.
- **Key Code Snippet**:
  ```python
  # Physical Partitioning & Broadcast Join
  bronze_df.write.mode("overwrite").partitionBy("order_date").parquet("partitioned_orders")
  orders_with_products = large_df.join(broadcast(products), on="product_id", how="inner")
  orders_with_products.explain()
  ```

---

### Day 05: Advanced Joins & Data Modeling
- **Notebook**: [`Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb`](./modules/Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb)
- **Topics**: Star schema data modeling, Orders, Customers & Products joins, column disambiguation via DataFrame aliasing (`orders.alias("o")`), conditional branching with `when/otherwise`, multi-column group-by aggregations.
- **Key Code Snippet**:
  ```python
  joined = orders.alias("o").join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
  final_orders = joined.select(
      col("o.order_id"),
      col("c.customer_name"),
      col("c.city"),
      when(col("o.revenue") > 50000, "HIGH_VALUE").otherwise("STANDARD").alias("segment")
  )
  ```

---

### Day 06: Analytical Window Functions Deep Dive
- **Notebook**: [`Day_06_Analytical_Window_Functions_Deep_Dive.ipynb`](./modules/Day_06_Analytical_Window_Functions_Deep_Dive.ipynb)
- **Topics**: Window specification syntax (`Window.partitionBy()`, `Window.orderBy()`), `row_number()` for latest/first order deduplication, `rank()` vs `dense_rank()`, inter-row calculations (`lag()`, `lead()`), cumulative running totals, moving rolling averages (`rowsBetween(-2, 0)`), Top-N rankings.
- **Key Code Snippet**:
  ```python
  customer_window = Window.partitionBy("customer_id").orderBy("order_date")
  running_window = Window.partitionBy("customer_id").orderBy("order_date") \
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  orders_analytics = orders \
      .withColumn("previous_revenue", lag("revenue", 1).over(customer_window)) \
      .withColumn("running_total", sum("revenue").over(running_window))
  ```

---

### Day 07: Modular Production Pipeline Architecture
- **Notebook**: [`Day_07_Modular_Production_ETL_Architecture.ipynb`](./modules/Day_07_Modular_Production_ETL_Architecture.ipynb)
- **Topics**: Functional decomposition, writing testable Python functions for big data pipelines, standardizing contracts (`ingest_csv`, `add_metadata`, `validate_orders`, `deduplicate_orders`, `transform_orders`, `write_parquet`), master pipeline runner.
- **Key Code Snippet**:
  ```python
  def run_pipeline(spark, source_path):
      raw = ingest_csv(spark, source_path, ORDER_SCHEMA)
      bronze = add_metadata(raw)
      valid, rejected = validate_orders(bronze)
      unique, dups = deduplicate_orders(valid)
      silver = transform_orders(unique)
      write_parquet(silver, "output/silver")
      gold = create_product_report(silver)
      write_parquet(gold, "output/gold")
      return {"total": raw.count(), "valid": silver.count(), "rejected": rejected.count()}
  ```

---

### Day 08: Enterprise Logging, Configuration & Testing
- **Notebook**: [`Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb`](./modules/Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb)
- **Topics**: Centralized configuration management, production file & console logging, failure isolation and structured error reporting, SLA rejection rate thresholds (abort if rejection > 30%), automated PySpark unit test assertions.
- **Key Code Snippet**:
  ```python
  # Automated Production Unit Test Suite
  assert total == valid + rejected, "Reconciliation failed!"
  assert silver_df.filter(col("customer_id").isNull()).count() == 0, "Null customer IDs in Silver!"
  assert silver_df.filter(col("revenue") < 0).count() == 0, "Negative revenue detected!"
  print("✅ All automated pipeline unit tests passed successfully!")
  ```

---

### Day 09: Cloud PostgreSQL JDBC & Lakehouse Ingestion
- **Notebook**: [`Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb`](./modules/Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb)
- **Topics**: Connecting Spark to cloud relational databases (Neon Serverless PostgreSQL), configuring JDBC drivers (`postgresql-42.6.0.jar`), `psycopg2` direct extraction bridged into Spark DataFrames, hybrid cloud lakehouse joins, writing to curated Parquet marts.
- **Key Code Snippet**:
  ```python
  spark = SparkSession.builder \
      .appName("Day9_Neon_Pipeline") \
      .config("spark.jars", "postgresql-42.6.0.jar") \
      .getOrCreate()

  # Bridge Pandas to PySpark for Serverless PostgreSQL
  df_pg = pd.read_sql_query("SELECT * FROM orders;", conn)
  orders_spark = spark.createDataFrame(df_pg)
  orders_spark.show()
  ```

---

### Day 10: Custom DAG Pipeline Orchestrator with Retries
- **Notebook**: [`Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb`](./modules/Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb)
- **Topics**: Building a lightweight Airflow-style DAG workflow engine from scratch (`SimpleOrchestrator`), task dependency chaining, shared execution context, automatic retries with delay backoff, fault injection testing, compute vs orchestration responsibilities.
- **Key Code Snippet**:
  ```python
  orchestrator = SimpleOrchestrator(name="ecommerce_lakehouse_dag")
  orchestrator.add_task("ingest", task_ingest, retries=2)
  orchestrator.add_task("validate", task_validate, retries=2)
  orchestrator.add_task("transform", task_transform, retries=1)
  orchestrator.add_task("export", task_export, retries=3)

  results = orchestrator.run()
  ```

---

## 📁 Repository Structure

```
PySpark Ingestion & Production Engineering/
│
├── PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb  # Unified 10-day interactive masterclass notebook
├── README.md                                                   # Comprehensive documentation & curriculum guide
├── orders.csv                                                  # Day 01 sample batch orders dataset
├── orders_day2.csv                                             # Day 02 sample dirty data batch
├── postgresql-42.6.0.jar                                       # PostgreSQL JDBC driver for Cloud database ingestion
├── PysparkInterview Qs.pdf                                     # Comprehensive PySpark interview questions & answers
│
├── source/                                                     # Incremental ingestion incoming directory
│   ├── orders_day1.csv
│   ├── orders_day2.csv
│   └── orders_day3.csv
│
├── metadata/                                                   # Audit & watermark state ledger
│   └── processed_files.csv
│
└── modules/                                                    # Modular Day-by-Day reading notebooks
    ├── Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb
    ├── Day_02_Data_Quality_Validation_and_Quarantine.ipynb
    ├── Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb
    ├── Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb
    ├── Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb
    ├── Day_06_Analytical_Window_Functions_Deep_Dive.ipynb
    ├── Day_07_Modular_Production_ETL_Architecture.ipynb
    ├── Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb
    ├── Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb
    └── Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb
```

---

## ⚙️ Environment Setup & Installation

### Windows WinUtils & Hadoop Configuration
On Windows, Apache Spark requires `winutils.exe` and `hadoop.dll` to perform local filesystem I/O safely without `NullPointerException` errors.

1. Ensure Python 3.10+ and Java 8 / 11 / 17 are installed (`java -version`).
2. This project includes an automated dummy Hadoop directory (`hadoop_dummy/bin`) or you can configure `HADOOP_HOME`:
   ```bash
   pip install winutils-pyspark
   ```
   Or set the environment variable in Python:
   ```python
   import os
   os.environ["HADOOP_HOME"] = os.path.abspath("./hadoop_dummy")
   ```

### Python Dependencies
Install the required packages using pip:
```bash
pip install pyspark pandas pyarrow psycopg2-binary jupyter
```

### PostgreSQL JDBC Driver Setup
For Day 09, `postgresql-42.6.0.jar` is included directly in the root directory. Spark references it via:
```python
spark = SparkSession.builder \
    .appName("PostgreSQL_Ingestion") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()
```

---

## 💡 Production Best Practices & Interview Guide

### 1. Medallion Architecture vs Traditional Data Warehouses
- **Bronze**: Append-only, raw format preserving original source fidelity. Allows full data reprocessing in case of pipeline bugs.
- **Silver**: Cleansed, deduplicated, enriched single version of truth.
- **Gold**: High-performance domain-specific aggregations optimized for BI queries.

### 2. When to use `repartition()` vs `coalesce()`?
- Use `repartition()` when increasing partitions, mitigating data skew, or before heavy joins/aggregations.
- Use `coalesce()` immediately before saving to disk to combine partitions into fewer files without incurring an expensive network shuffle.

### 3. Avoiding the Small Files Problem
- Partitioning by high-cardinality columns (e.g. `user_id` or timestamp down to the second) creates millions of tiny files, degrading HDFS / S3 read performance and metadata management.
- Always partition by medium-cardinality columns (e.g. `order_date`, `region`, `status`).

### 4. Broadcast Joins vs Shuffle Hash Joins
- Spark's default threshold for broadcasting is `10MB` (`spark.sql.autoBroadcastJoinThreshold`).
- For tables under 100MB, explicit `broadcast(df)` eliminates network shuffling of large tables across executor nodes, yielding up to 10x speedups.

---
⭐ **Authored by Swapnanil Chatterjee** | Distributed Data Engineering & Production Lakehouse Pipelines
