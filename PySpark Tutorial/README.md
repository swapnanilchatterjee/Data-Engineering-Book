# ⚡ PySpark Tutorial & Last-Minute Cheat Sheet

![PySpark](https://img.shields.io/badge/PySpark-4.0%2B-orange?style=for-the-badge&logo=apachespark)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange?style=for-the-badge&logo=jupyter)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

Welcome to the **PySpark Complete Hands-On Tutorial & Quick Reference Cheat Sheet**! 🚀

This module is designed as an end-to-end practical guide and a revision cheat sheet for data engineers, data analysts, and ML engineers working with **Apache Spark** using Python.

---

## 📌 Table of Contents
1. [Overview & Spark Architecture](#-overview--spark-architecture)
2. [Repository Structure](#-repository-structure)
3. [Key Topics Covered](#-key-topics-covered)
4. [🔥 Last-Minute PySpark Cheat Sheet](#-last-minute-pyspark-cheat-sheet)
   - [1. SparkSession Initialization](#1-sparksession-initialization)
   - [2. DataFrame Creation & Schemas](#2-dataframe-creation--schemas)
   - [3. Ingestion & Inspection](#3-ingestion--inspection)
   - [4. Column Selection & Expressions](#4-column-selection--expressions)
   - [5. Data Filtering & Subsetting](#5-data-filtering--subsetting)
   - [6. Column Manipulations & Conditional Logic](#6-column-manipulations--conditional-logic)
   - [7. Sorting & Ordering](#7-sorting--ordering)
   - [8. Missing Values & Deduplication](#8-missing-values--deduplication)
   - [9. GroupBy & Aggregations](#9-groupby--aggregations)
   - [10. Joins](#10-joins)
   - [11. Spark SQL Engine](#11-spark-sql-engine)
   - [12. User Defined Functions (UDFs)](#12-user-defined-functions-udfs)
   - [13. Date & Timestamp Functions](#13-date--timestamp-functions)
   - [14. String Manipulations](#14-string-manipulations)
   - [15. Advanced Window Functions](#15-advanced-window-functions)
   - [16. Exporting & Interoperability](#16-exporting--interoperability)
5. [🚀 How to Run the Notebook](#-how-to-run-the-notebook)

---

## 🧠 Overview & Spark Architecture

**Apache Spark** is a unified analytics engine for large-scale data processing. PySpark provides the Python bindings to leverage Spark's distributed computing power.

```
       ┌────────────────────────────────────────────────────────┐
       │                 Driver Program                         │
       │  (SparkSession / SparkContext / DAG Scheduler)         │
       └──────────────────────────┬─────────────────────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
  │ Worker Node  │         │ Worker Node  │         │ Worker Node  │
  │ ┌──────────┐ │         │ ┌──────────┐ │         │ ┌──────────┐ │
  │ │ Executor │ │         │ │ Executor │ │         │ │ Executor │ │
  │ └──────────┘ │         │ └──────────┘ │         │ └──────────┘ │
  └──────────────┘         └──────────────┘         └──────────────┘
```

* **Driver**: Manages the application, plans execution DAG (Directed Acyclic Graph), and coordinates tasks across workers.
* **Executors**: Worker processes that execute tasks in parallel and store cached data.
* **Lazy Evaluation**: PySpark builds a logical plan for transformations and only executes computation when an **Action** (e.g., `show()`, `count()`, `collect()`, `write`) is called.

---

## 📁 Repository Structure

```
PySpark Tutorial/
│
├── Pyspark_Tutorial.ipynb       # Fully executed interactive Jupyter Notebook with outputs & markdown
├── popularity_campaign_v2 1.csv # Sample campaign analytics dataset used in demonstrations
└── README.md                     # Comprehensive documentation & PySpark quick reference cheat sheet
```

---

## 📚 Key Topics Covered

| Topic | Description | Key API Methods |
| :--- | :--- | :--- |
| **Session Setup** | Initializing cluster entry point | `SparkSession.builder.getOrCreate()` |
| **Data Ingestion** | Reading structured & semi-structured data | `read.csv()`, `read.parquet()`, `read.json()` |
| **Data Cleaning** | Handling null values & row deduplication | `na.fill()`, `na.drop()`, `dropDuplicates()` |
| **Transformations** | Filtering, projection, derived columns | `select()`, `filter()`, `withColumn()`, `when()` |
| **Aggregations** | Grouping & calculating summary metrics | `groupBy()`, `agg()`, `avg()`, `count()` |
| **Joins** | Relational dataset joins | `join(how='inner'/'left'/'right'/'outer')` |
| **Spark SQL** | Executing ANSI SQL queries on DataFrames | `createOrReplaceTempView()`, `spark.sql()` |
| **UDFs** | Custom Python logic on DataFrame columns | `udf()`, `F.col()` |
| **Windowing** | Advanced analytical window functions | `Window.partitionBy()`, `rank()`, `lag()` |

---

## 🔥 Last-Minute PySpark Cheat Sheet

### 1. SparkSession Initialization
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("PySparkApp") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

### 2. DataFrame Creation & Schemas
```python
# From tuples with custom column names
df = spark.createDataFrame([(1, "Alice", 50000), (2, "Bob", 60000)], ["id", "name", "salary"])

# With explicit StructType schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df_schema = spark.createDataFrame(data, schema=schema)
```

### 3. Ingestion & Inspection
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)

df.show(5, truncate=False) # Preview rows
df.printSchema()          # View column data types
df.count()                # Get total row count
df.columns                # Get list of column names
df.describe().show()      # Summary stats
```

### 4. Column Selection & Expressions
```python
# Basic selection
df.select("user_id", "user_specialty").show()

# Selection with aliasing
df.select(col("score").alias("campaign_score")).show()

# SQL expressions in selection
df.selectExpr("user_id", "score * 100 as score_pct").show()
```

### 5. Data Filtering & Subsetting
```python
# Single condition
df.filter(col("score") > 0.005).show()

# AND condition (&)
df.filter((col("score") > 0.005) & (col("user_specialty") == "Internal Medicine")).show()

# OR condition (|)
df.filter((col("score") > 0.01) | (col("user_specialty") == "ENT")).show()

# Set membership
df.filter(col("user_specialty").isin("Paediatrics", "ENT")).show()
```

### 6. Column Manipulations & Conditional Logic
```python
# Add static column
df = df.withColumn("Country", lit("India"))

# Derived column
df = df.withColumn("final_score", col("score") * 100)

# Conditional column (IF-ELSE logic)
df = df.withColumn("level", when(col("score") > 0.005, "High").otherwise("Low"))

# Rename & Drop
df = df.withColumnRenamed("channel_code", "channel").drop("filter_used")
```

### 7. Sorting & Ordering
```python
df.orderBy("content_id").show()                             # Ascending
df.orderBy(col("content_id").desc()).show()                # Descending
df.orderBy(col("user_specialty").asc(), col("score").desc()).show() # Multi-column
```

### 8. Missing Values & Deduplication
```python
# Deduplication
df.dropDuplicates().show()                  # Entire row deduplication
df.dropDuplicates(["user_id"]).show()        # Subset deduplication

# Null Imputation
df.na.fill(0).show()                        # Fill numbers with 0
df.na.fill("Unknown").show()                # Fill strings with "Unknown"
df.na.fill({"age": 0, "name": "N/A"}).show()# Dictionary imputation
df.na.drop(subset=["user_id"]).show()       # Drop null rows
```

### 9. GroupBy & Aggregations
```python
df.groupBy("department").avg("salary").show()

# Multiple aggregations with .agg()
df.groupBy("department").agg(
    avg("salary").alias("avg_sal"),
    max("salary").alias("max_sal"),
    min("salary").alias("min_sal"),
    count("*").alias("total_emp")
).show()
```

### 10. Joins
```python
# Inner, Left, Right, Outer
emp_df.join(dept_df, on="dept_id", how="inner").show()
emp_df.join(dept_df, on="dept_id", how="left").show()
```

### 11. Spark SQL Engine
```python
df.createOrReplaceTempView("campaigns")
spark.sql("""
    SELECT user_specialty, COUNT(*) as cnt, AVG(score) as avg_score
    FROM campaigns
    WHERE score > 0.005
    GROUP BY user_specialty
    ORDER BY avg_score DESC
""").show()
```

### 12. User Defined Functions (UDFs)
```python
def classify_score(s):
    return "High" if s and s > 0.005 else "Low"

score_udf = udf(classify_score, StringType())
df.withColumn("category", score_udf(col("score"))).show()
```

### 13. Date & Timestamp Functions
```python
df_dates = df.withColumn("date", to_date(col("timestamp_str"))) \
             .withColumn("year", year(col("date"))) \
             .withColumn("month", month(col("date"))) \
             .withColumn("today", current_date()) \
             .withColumn("next_week", date_add(col("date"), 7)) \
             .withColumn("days_diff", datediff(current_date(), col("date")))
```

### 14. String Manipulations
```python
df.select(
    upper(col("user_specialty")).alias("upper_spec"),
    lower(col("user_specialty")).alias("lower_spec"),
    length(col("user_specialty")).alias("spec_len"),
    concat(col("user_specialty"), lit(" - Specialization")).alias("full_spec"),
    substring(col("user_specialty"), 1, 4).alias("short_spec")
).show()
```

### 15. Advanced Window Functions
```python
window_spec = Window.partitionBy("department").orderBy(desc("salary"))
window_global = Window.orderBy("salary")

df_win = df.withColumn("row_num", row_number().over(window_spec)) \
           .withColumn("rank", rank().over(window_spec)) \
           .withColumn("dense_rank", dense_rank().over(window_spec)) \
           .withColumn("prev_sal", lag("salary", 1).over(window_global)) \
           .withColumn("next_sal", lead("salary", 1).over(window_global))
```

### 16. Exporting & Interoperability
```python
# Convert to Pandas
pdf = df.toPandas()

# Export Data
df.write.mode("overwrite").option("header", "true").csv("output_path")
df.write.mode("overwrite").parquet("parquet_path")
```

---

## 🚀 How to Run the Notebook

1. **Prerequisites**:
   Ensure you have Java 8+ and Python 3.10+ installed.

2. **Install Required Libraries**:
   ```bash
   pip install pyspark pandas jupyter
   ```

3. **Launch Jupyter Notebook**:
   ```bash
   jupyter notebook Pyspark_Tutorial.ipynb
   ```

---
Happy Sparking! ⚡
