# ⚡ Data Engineering & PySpark: The Ultimate Before-Interview Cheat Sheet
> **A High-Yield Rapid Revision Guide Covering Apache Spark Internals, Cluster Tuning, Lakehouse Architecture, Advanced SQL, and Production System Design.**

[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B%20%2F%204.0%2B-F26522?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![SQL](https://img.shields.io/badge/SQL-Advanced%20Window%20Functions-00758F?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Lakehouse](https://img.shields.io/badge/Lakehouse-Medallion%20Architecture-008080?style=for-the-badge)](https://databricks.com)

---

## 📌 Table of Contents
1. [Core Spark Architecture & Internals](#1-core-spark-architecture--internals)
2. [Transformations vs Actions & Catalyst Optimizer](#2-transformations-vs-actions--catalyst-optimizer)
3. [Spark Performance Tuning & Cluster Optimization](#3-spark-performance-tuning--cluster-optimization)
4. [Medallion Lakehouse & Ingestion Patterns](#4-medallion-lakehouse--ingestion-patterns)
5. [Advanced Analytical SQL & Windowing Cheat Sheet](#5-advanced-analytical-sql--windowing-cheat-sheet)
6. [Partitioning, Shuffling & File Layout](#6-partitioning-shuffling--file-layout)
7. [Cloud Database Ingestion & Hybrid Lakehouses](#7-cloud-database-ingestion--hybrid-lakehouses)
8. [Workflow Orchestration & Fault Tolerance](#8-workflow-orchestration--fault-tolerance)
9. [🔥 Top 30 High-Frequency Technical Interview Questions](#9--top-30-high-frequency-technical-interview-questions)

---

## 1. Core Spark Architecture & Internals

```
                             ┌───────────────────────────────────┐
                             │          Driver Program           │
                             │  SparkSession  │  DAG Scheduler   │
                             └─────────────────┬─────────────────┘
                                               │
                       ┌───────────────────────┴───────────────────────┐
                       ▼                                               ▼
            ┌─────────────────────┐                         ┌─────────────────────┐
            │   Worker Node 1     │                         │   Worker Node 2     │
            │  ┌───────────────┐  │                         │  ┌───────────────┐  │
            │  │  Executor 1   │  │                         │  │  Executor 2   │  │
            │  │ Task 1 │Task 2│  │                         │  │ Task 3 │Task 4│  │
            │  └───────────────┘  │                         │  └───────────────┘  │
            └─────────────────────┘                         └─────────────────────┘
```

- **Driver Program**: Runs the `main()` function, initializes `SparkSession`, translates code into a Directed Acyclic Graph (DAG), schedules stages, and coordinates tasks with the cluster manager (YARN, K8s, Standalone).
- **Cluster Manager**: Allocates resources (CPU, RAM) across nodes.
- **Worker Node**: Physical or virtual machine hosting executor processes.
- **Executor**: JVM process running on worker nodes responsible for executing tasks in parallel and caching data in memory/disk.
- **Task**: Smallest unit of work, executed on a single partition of data on a single core.
- **Stage**: A collection of tasks that can run in parallel without a cluster-wide shuffle.
- **Job**: Triggered whenever an **Action** is called on a DataFrame or RDD.

---

## 2. Transformations vs Actions & Catalyst Optimizer

### Narrow vs Wide Transformations
| Feature | Narrow Transformations | Wide Transformations |
|:---|:---|:---|
| **Definition** | Each input partition contributes to at most one output partition. | Multiple input partitions contribute to output partitions. |
| **Shuffle** | **Zero Network Shuffle**. Computed locally on worker. | **Cluster Network Shuffle Required**. Heavy network I/O. |
| **Examples** | `map()`, `filter()`, `select()`, `withColumn()`, `drop()` | `groupBy()`, `distinct()`, `join()`, `repartition()`, `orderBy()` |
| **Stage Boundary** | Remains in the **same stage**. | **Creates a new Stage boundary**. |

### Lazy Evaluation
Spark does NOT execute transformations immediately. Instead, it records them in a logical execution plan.
- **Benefit**: Catalyst optimizer inspects the entire query graph before execution and performs optimizations (predicate pushdown, projection pruning).
- **Triggers**: Computation only happens when an **Action** is invoked (`count()`, `show()`, `collect()`, `write`, `take()`).

### Catalyst Optimizer Phases
1. **Unresolved Logical Plan**: Validates syntax.
2. **Resolved Logical Plan**: Verifies table names and column types against the Catalog.
3. **Optimized Logical Plan**: Applies rule-based optimizations (e.g., filter pushdown, constant folding).
4. **Physical Plans**: Generates multiple execution strategies and uses Cost-Based Optimization (CBO) to select the fastest physical plan.
5. **Code Generation (Tungsten Engine)**: Emits optimized Java bytecode for CPU execution.

---

## 3. Spark Performance Tuning & Cluster Optimization

### 1. `repartition()` vs `coalesce()`
- `repartition(N)`:
  - Can **increase** or **decrease** partition count.
  - Triggers a **full cluster network shuffle**.
  - Distributes data evenly across nodes to cure **Data Skew**.
- `coalesce(N)`:
  - Can **ONLY decrease** partition count.
  - Avoids a full shuffle by merging adjacent partitions locally.
  - Best practice: Call immediately before writing to storage to reduce file count.

### 2. Broadcast Hash Join (BHJ)
- Standard join shuffles both large tables across cluster nodes based on the join key (expensive).
- When one table is small (< 10MB by default, configurable via `spark.sql.autoBroadcastJoinThreshold`):
  ```python
  from pyspark.sql.functions import broadcast
  orders.join(broadcast(products), "product_id", "inner")
  ```
- **How it works**: Driver copies the small table and broadcasts it to all executor memory pools. Workers join locally without shuffling the large table.

### 3. Data Skew & Salting Technique
- **Symptom**: 99 tasks finish in seconds, but 1 task hangs for minutes because one partition has 90% of the data (e.g., `null` values or popular customer ID).
- **Solutions**:
  1. Filter out `null` keys before joining or handle separately.
  2. **Salting**: Append a random integer (`0` to `N`) to the skewed join key on the large table, and replicate the small table across all salted keys.
  3. Enable Adaptive Query Execution:
     ```python
     spark.conf.set("spark.sql.adaptive.enabled", "true")
     spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
     ```

### 4. Cache vs Persist
- `df.cache()`: Shortcut for `persist(StorageLevel.MEMORY_AND_DISK)`.
- Use when: A DataFrame with expensive transformations is reused across multiple actions.
- Anti-pattern: Caching a DataFrame that is only used once wastes RAM. Always call `df.unpersist()` when done.

---

## 4. Medallion Lakehouse & Ingestion Patterns

```
  Source Files / Streams ──►   Bronze Layer   ──►   Silver Layer   ──►   Gold Layer
  (Raw CSV, JSON, Kafka)     (Immutable Raw)     (Clean & Dedupped)   (Business Marts)
```

- **Bronze Layer (Raw Storage)**:
  - Raw, append-only, immutable copy of source data.
  - Preserves exact source schema and original payload.
  - Allows full pipeline replay if logic bugs are discovered.
- **Silver Layer (Curated & Cleansed)**:
  - Deduplicated, type-casted, schema-enforced, and validated.
  - Business rules applied: invalid records routed to a **Dead-Letter Queue (DLQ) / Quarantine table**.
- **Gold Layer (Aggregated Business Marts)**:
  - Star-schema dimensional modeling (Facts & Dimensions).
  - Pre-computed domain KPIs optimized for BI reporting and analytics queries.

---

## 5. Advanced Analytical SQL & Windowing Cheat Sheet

### Window Specification Syntax
```sql
SELECT 
    customer_id,
    order_date,
    revenue,
    -- 1. Sequential numbering (deduplication)
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date DESC) as rn,
    
    -- 2. Ranking with ties (gaps vs no gaps)
    RANK() OVER(PARTITION BY city ORDER BY revenue DESC) as rank_with_gaps,
    DENSE_RANK() OVER(PARTITION BY city ORDER BY revenue DESC) as dense_rank_no_gaps,
    
    -- 3. Inter-row navigation (previous & next)
    LAG(revenue, 1) OVER(PARTITION BY customer_id ORDER BY order_date) as prev_rev,
    LEAD(revenue, 1) OVER(PARTITION BY customer_id ORDER BY order_date) as next_rev,
    
    -- 4. Cumulative running total
    SUM(revenue) OVER(
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_revenue,
    
    -- 5. Rolling 3-order moving average
    AVG(revenue) OVER(
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as rolling_3_avg
FROM orders;
```

### Top SQL Interview Snippets
1. **Second Highest Salary**:
   ```sql
   WITH Ranked AS (
       SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rnk 
       FROM employees
   )
   SELECT salary FROM Ranked WHERE rnk = 2 LIMIT 1;
   ```
2. **Find Duplicate Records**:
   ```sql
   SELECT email, COUNT(*) 
   FROM users 
   GROUP BY email 
   HAVING COUNT(*) > 1;
   ```
3. **Monthly Active Customers & Retention**:
   ```sql
   SELECT 
       DATE_TRUNC('month', order_date) as order_month, 
       COUNT(DISTINCT customer_id) as active_customers 
   FROM orders 
   GROUP BY 1 
   ORDER BY 1;
   ```

---

## 6. Partitioning, Shuffling & File Layout

- **Physical Storage Partitioning (`partitionBy`)**:
  - Organizes files in directory trees: `orders/order_date=2026-09-29/part-0001.parquet`.
  - **Partition Pruning**: Query engines read only directories matching the filter clause, skipping terabytes of irrelevant data.
- **The Small Files Problem**:
  - High-cardinality partition columns (e.g., partitioning by `user_id` or timestamp to the second) create millions of tiny files.
  - Causes massive metadata overhead, slow object store LIST operations, and JVM memory thrashing.
  - **Rule of thumb**: Files per partition should ideally be between **128 MB and 1 GB**.

---

## 7. Cloud Database Ingestion & Hybrid Lakehouses

- **JDBC Ingestion Mechanics**:
  - Default JDBC read uses a **single thread/core** on the Driver, creating a massive bottleneck.
  - **Parallel JDBC Ingestion**:
    ```python
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "orders") \
        .option("partitionColumn", "order_id") \
        .option("lowerBound", 1) \
        .option("upperBound", 1000000) \
        .option("numPartitions", 10) \
        .load()
    ```
- **Driver Considerations**: Include official JDBC driver (e.g., `postgresql-42.6.0.jar`) in `--jars` or `spark.jars` configuration.

---

## 8. Workflow Orchestration & Fault Tolerance

- **Directed Acyclic Graph (DAG)**:
  - Set of nodes (tasks) and directed edges (dependencies) without circular loops.
- **Idempotency**:
  - Running a pipeline task multiple times with the same input must produce the **exact same output** without duplicate records or side effects.
  - Achieved via: Overwrite modes (`mode("overwrite")`), partition-level swapping, and atomic commits.
- **Airflow vs Spark**:
  - **Spark**: Distributed compute engine for heavy data transformation.
  - **Airflow**: Workflow orchestration engine scheduling when and how tasks execute, managing retries, SLAs, and notifications.

---

## 9. 🔥 Top 30 High-Frequency Technical Interview Questions

#### Q1: What is the difference between an RDD, DataFrame, and Dataset?
- **RDD**: Low-level distributed object collection, compile-time type safety, lacks Catalyst query optimizations.
- **DataFrame**: Distributed untyped row collection organized into named columns, optimized by Catalyst and Tungsten.
- **Dataset**: Strongly typed JVM objects combining RDD type-safety with Catalyst optimization (available in Scala/Java).

#### Q2: What causes an OutOfMemory (OOM) error in PySpark?
- Driver OOM: Calling `.collect()` or `.toPandas()` on massive datasets.
- Executor OOM: Heavy data skew, high broadcast join size exceeding worker memory, or excessive partitions with tiny memory overhead.

#### Q3: How do you handle Data Skew in PySpark joins?
- Salting the skewed join key.
- Using Adaptive Query Execution (`spark.sql.adaptive.skewJoin.enabled = true`).
- Broadcasting the smaller table if eligible.

#### Q4: What is Predicate Pushdown?
- The optimization where filter conditions (`WHERE age > 30`) are pushed down to the storage layer (Parquet/JDBC), reading only qualifying rows into memory.

#### Q5: What is the difference between `rank()` and `dense_rank()`?
- `rank()` leaves gaps in rank values upon ties (1, 2, 2, 4).
- `dense_rank()` maintains consecutive rankings without gaps (1, 2, 2, 3).

#### Q6: How does PySpark execute Python code?
- PySpark uses Py4J on the driver to communicate with the JVM. For Python UDFs, workers serialize data between Python worker processes and the JVM via IPC sockets (incurring serialization overhead, unless using Vectorized Pandas UDFs via Apache Arrow).

#### Q7: Why is Apache Parquet preferred over CSV in Lakehouses?
- Columnar storage (reads only queried columns), snappy compression (up to 80% size reduction), statistics embedded in metadata (min/max for file skipping), and explicit type schemas.

#### Q8: What is the difference between `unboundedPreceding` and `currentRow` in Window frames?
- `unboundedPreceding` starts at the very first row of the partition.
- `currentRow` ends at the current row being processed, enabling cumulative running calculations.

#### Q9: What happens when you do `repartition(1)` before writing?
- All data from all cluster nodes is shuffled into a single partition on one executor, producing a single output file, but risking an executor OOM if data is large.

#### Q10: How do you achieve Idempotency in Big Data pipelines?
- By writing to staging directories and executing atomic partition swaps, or using ACID storage formats (Delta Lake / Apache Iceberg) with partition-overwrite semantics.

---
⭐ **Good luck with your interviews! Review this cheat sheet 30 minutes before your technical round.**
