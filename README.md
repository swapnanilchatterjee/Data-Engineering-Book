<div align="center">

# 📖 The Data Engineering Book
### *An Interactive Hands-On Curriculum, Production Lakehouse Guide & Learning Playlist*

[![Author](https://img.shields.io/badge/Author-Swapnanil%20Chatterjee-0052CC?style=for-the-badge&logo=github&logoColor=white)](https://github.com/swapnanilchatterjee)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B%20%2F%204.0%2B-F26522?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Cloud%20Neon-00E599?style=for-the-badge&logo=postgresql&logoColor=white)](https://neon.tech)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-008080?style=for-the-badge)](https://www.databricks.com/glossary/medallion-architecture)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](#)

<p align="center">
  <b>A zero-to-production compendium covering Relational Data Engineering, Distributed Big Data, Medallion Lakehouses, Cluster Optimization, Cloud JDBC Streaming, and Custom DAG Orchestration.</b>
</p>

---

</div>

## 📑 Book Architecture & Playlist Roadmap

This repository is formatted as a progressive, modular **Data Engineering Book & Playlist**. Each chapter builds foundational competencies that compound into enterprise-grade data platform engineering:

```
                            THE DATA ENGINEERING BOOK
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │                                                                                        │
 │  PART I: RELATIONAL FOUNDATIONS                                                        │
 │  └── Chapter 1: Advanced Analytical SQL & Query Engineering (Windowing, CTEs)          │
 │                                                                                        │
 │  PART II: PYTHON DATA TRANSFORMATION                                                   │
 │  └── Chapter 2: Python Data Manipulation, Feature Pipelines & Pandas                   │
 │                                                                                        │
 │  PART III: DISTRIBUTED BIG DATA & APACHE SPARK                                         │
 │  └── Chapter 3: Apache Spark Architecture, SparkSession & PySpark Fundamentals        │
 │                                                                                        │
 │  PART IV: ENTERPRISE DATA INGESTION & LAKEHOUSE ENGINEERING (10-DAY MASTERCLASS) ⚡     │
 │  ├── Day 01: Medallion Architecture Fundamentals (Bronze / Silver / Gold)              │
 │  ├── Day 02: Production Data Quality, Quarantine & Dead-Letter Queue (DLQ)             │
 │  ├── Day 03: Incremental Ingestion, Watermarking & Metadata Audit State                │
 │  ├── Day 04: Spark Performance Optimization, Partition Pruning & Broadcast Joins       │
 │  ├── Day 05: Multi-Entity Dimensional Modeling & Complex Star-Schema Joins             │
 │  ├── Day 06: Analytical Window Functions Deep Dive & Time-Series Deltas                │
 │  ├── Day 07: Modular Software Engineering for Big Data ETL Pipelines                   │
 │  ├── Day 08: Enterprise Logging, SLA Threshold Alerts & Automated Unit Testing         │
 │  ├── Day 09: Cloud Database Ingestion (Neon Serverless PostgreSQL via JDBC)           │
 │  └── Day 10: Custom Airflow-Style DAG Workflow Engine with Automatic Retries           │
 │                                                                                        │
 │  PART V: REAL-WORLD DOMAIN DATA PLATFORMS                                              │
 │  ├── Chapter 5.1: Quick-Commerce Ingestion & Surge Delivery Analytics (Zepto)          │
 │  ├── Chapter 5.2: Omnichannel Retail Inventory & Basket Modeling (Retail)              │
 │  ├── Chapter 5.3: Dimensional Sales Warehousing & Executive BI (Sales Dashboard)       │
 │  ├── Chapter 5.4: Media Streaming Catalog Analytics (Netflix Analysis)                 │
 │  └── Chapter 5.5: Actuarial Healthcare Cost Modeling (Health Expenses)                 │
 └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📚 Book Table of Contents & Playlist Chapters

| Track / Chapter | Module / Directory | Target Level | Stack & Tooling | Core Competencies |
|:---:|:---|:---:|:---|:---|
| **Chapter 1** | [**Practice SQL**](./Practice%20SQL) | Intermediate | ANSI SQL, PostgreSQL, MySQL | Complex window functions (`ROW_NUMBER`, `DENSE_RANK`, `LAG`, `LEAD`), recursive CTEs, grouping sets, analytical subqueries, performance optimization. |
| **Chapter 2** | [**Pandas**](./Pandas) | Intermediate | Python 3, Pandas, NumPy | Vectorized operations, missing data handling, multi-index aggregation, dataset reshaping, and exploratory pipelines. |
| **Chapter 3** | [**PySpark Tutorial**](./PySpark%20Tutorial) | Intermediate | PySpark 4.0+, Apache Spark | Distributed SparkSession initialization, DataFrame APIs, Catalyst optimizer concepts, user-defined functions (UDFs), and last-minute cheat sheet. |
| **Chapter 4** 🔥 | [**PySpark Ingestion & Production Engineering**](./PySpark%20Ingestion%20%26%20Production%20Engineering) | **Advanced / Production** | **PySpark, Parquet, Neon PostgreSQL JDBC, Custom DAG Engine** | **10-Day Production Masterclass: Medallion Architecture, Dead-letter Quarantine, Incremental Watermarking, Cluster Tuning, Unit Testing & Retries.** |
| **Chapter 5.1** | [**Zepto Analytics**](./Zepto) | Real-World Case | SQL, Python, Analytics | Quick-commerce delivery latency analysis, order clustering, SLA tracking, and dynamic pricing metrics. |
| **Chapter 5.2** | [**Retail Analytics**](./Retail) | Real-World Case | Python, SQL, Statistics | Customer cohort retention, store inventory turns, basket cross-sell analytics, and revenue trends. |
| **Chapter 5.3** | [**Sales Dashboard**](./Sales%20Dashboard) | BI Engineering | Power BI, DAX, Python | Executive KPI tracking, star-schema dimensional modeling, customer lifetime value (LTV), regional drilldowns. |
| **Chapter 5.4** | [**Netflix Analysis**](./NetFlix%20Analysis) | Data Science | Python, Seaborn, Pandas | Streaming catalog distributions, release window trends, content genre categorization, runtime analysis. |
| **Chapter 5.5** | [**Health Expenses**](./Health%20Expenses) | Statistical Case | Python, SQL, Power BI | Clinical diabetic dataset cleansing, demographic cost factor modeling, outlier detection, and reporting. |

---

## ⚡ Featured Spotlight: PySpark Ingestion & Production Engineering (10-Day Masterclass)

The core cornerstone of this book is the **10-Day Production Masterclass** located in [`PySpark Ingestion & Production Engineering`](./PySpark%20Ingestion%20%26%20Production%20Engineering/).

It provides both a complete interactive Master Notebook and individual Day-by-Day modular reading notebooks:

👉 **Master Interactive Notebook**: [`PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb`](./PySpark%20Ingestion%20%26%20Production%20Engineering/PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb)  
👉 **Comprehensive Module Guide**: [`PySpark Ingestion & Production Engineering/README.md`](./PySpark%20Ingestion%20%26%20Production%20Engineering/README.md)

### 🗓️ Day-by-Day Modular Reading Curriculum

```
  Raw Source Batches ──► Bronze Layer ──► Quarantine Filter ──► Silver Layer ──► Gold Analytics Mart
  (CSV / JSON / JDBC)     (Immutable)        (Bad Records)       (Clean Data)       (Business KPIs)
```

1. **[Day 01: Medallion Architecture Fundamentals](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb)**
   * Explicit schema enforcement (`StructType`, `StructField`)
   * Bronze immutable raw Parquet storage
   * Silver layer cleansing: null imputation, date type casting, revenue calculation
   * Gold layer business aggregations (category revenue, city metrics)

2. **[Day 02: Production Data Quality & Quarantine Framework](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_02_Data_Quality_Validation_and_Quarantine.ipynb)**
   * Fail-safe validation rules (non-null foreign keys, domain bounds, timestamp validation)
   * Dead-Letter Queue (DLQ) pattern: routing clean records to Silver and corrupt records to `rejected_orders`
   * Rejection reason tagging for root-cause audits
   * Automated Data Quality Reconciliation reports and Python `logging`

3. **[Day 03: Incremental Ingestion & Metadata Tracking](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb)**
   * Eliminating full-table rescans via differential file detection
   * Ingestion state ledger (`metadata/processed_files.csv`)
   * Ingestion lineage watermarking (`ingestion_timestamp`)
   * Atomic incremental Parquet appending

4. **[Day 04: Spark Performance Optimization & Partitioning](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb)**
   * Spark cluster parallelism & Catalyst optimizer execution plans
   * `repartition()` (wide shuffle) vs `coalesce()` (narrow merge)
   * Physical storage partitioning (`partitionBy("order_date")`) and partition pruning
   * Broadcast Hash Joins (`broadcast()`) eliminating network shuffles on dimensional lookups
   * In-memory cache lifecycle management (`cache()`, `unpersist()`) and query benchmarking

5. **[Day 05: Advanced Joins & Dimensional Data Modeling](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb)**
   * Relational star-schema modeling across Orders, Customers, and Products
   * Inner, Left, Right, and Full Outer join mechanics
   * Eliminating ambiguous column name collisions using DataFrame aliasing
   * Business branching logic with `F.when().otherwise()`

6. **[Day 06: Analytical Window Functions Deep Dive](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_06_Analytical_Window_Functions_Deep_Dive.ipynb)**
   * PySpark Window specifications (`partitionBy` + `orderBy`)
   * `row_number()`, `rank()`, and `dense_rank()` with tie-handling mechanics
   * Inter-row time-series metrics using `lag()` and `lead()`
   * Customer financial deltas, cumulative running totals, rolling moving averages, and Top-N queries

7. **[Day 07: Modular Production Pipeline Architecture](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_07_Modular_Production_ETL_Architecture.ipynb)**
   * Functional decomposition: converting scripts into modular, testable Python functions
   * Single-responsibility components (`ingest_csv`, `validate_orders`, `deduplicate_orders`, `transform_orders`, `write_parquet`)
   * Master pipeline execution driver `run_pipeline(spark, source_path)`

8. **[Day 08: Enterprise Logging, Configuration & Testing](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb)**
   * Centralized configuration management (`CONFIG` dictionary)
   * Enterprise dual-handler logging (console + `pipeline.log`)
   * Data Quality SLAs: Automated pipeline abort if rejection rate exceeds threshold (30%)
   * Automated unit test suite with PySpark assertions (reconciliation, schema integrity, revenue validity)

9. **[Day 09: Cloud Database Connectivity & Neon PostgreSQL JDBC](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb)**
   * Hybrid Lakehouse architecture: Cloud OLTP to distributed Parquet lake
   * Spark PostgreSQL JDBC driver integration (`postgresql-42.6.0.jar`)
   * Serverless Neon PostgreSQL database queries bridged directly into Spark DataFrames
   * Distributed cloud joins and curated Gold Parquet export

10. **[Day 10: Custom DAG Pipeline Orchestrator with Retries](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb)**
    * Lightweight Airflow-style Directed Acyclic Graph (DAG) workflow engine (`SimpleOrchestrator`)
    * Dependency resolution, shared inter-task execution context
    * Automated exponential/linear retry engine with delay backoffs
    * Chaos testing: fault injection and verifying downstream failure halt

---

## 🛠️ Getting Started & Local Environment

### 1. Clone the Book Repository
```bash
git clone https://github.com/swapnanilchatterjee/Data-Engineering-Book.git
cd Data-Engineering-Book
```

### 2. Environment Prerequisites
- **Python**: Version 3.10+ recommended
- **Java**: Java 8, 11, or 17 (required for Apache Spark: check via `java -version`)
- **Windows Users**: Apache Spark requires `winutils.exe` and `hadoop.dll`. You can install them via:
  ```bash
  pip install winutils-pyspark
  ```
  Or set `HADOOP_HOME` to a local directory with `bin/winutils.exe`.

### 3. Install Python Dependencies
```bash
pip install pyspark pandas pyarrow psycopg2-binary jupyter matplotlib seaborn
```

### 4. Launch Jupyter Notebook
```bash
jupyter notebook
```
Navigate to any chapter or module notebook to begin reading and executing code interactively!

---

## 🤝 Contributing & Community
Contributions, feedback, and issue discussions are warmly welcomed!
- Found a typo or improvement? Submit a pull request.
- Have a question or idea for a new module? Open an issue.

⭐ **If you find this book valuable for your data engineering career or interview prep, please consider giving the repository a Star!**

---
<div align="center">
  <b>The Data Engineering Book</b> • Maintained by <a href="https://github.com/swapnanilchatterjee">Swapnanil Chatterjee</a>
</div>
