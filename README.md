# 📚 The Data Engineering Book & Master Learning Playlist
> **A Curated, Hands-On Roadmap for Big Data Engineers, Analytics Engineers, and Modern Data Professionals**

[![Repo Name](https://img.shields.io/badge/Repository-Data--Engineering--Book-FF6F00?style=for-the-badge&logo=git&logoColor=white)](https://github.com/swapnanilchatterjee/Data-Engineering-Book)
[![PySpark](https://img.shields.io/badge/Apache%20Spark-3.5%2B%20%2F%204.0%2B-F26522?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Cloud%20Neon-00E599?style=for-the-badge&logo=postgresql&logoColor=white)](https://neon.tech)
[![Power BI](https://img.shields.io/badge/BI-Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](#)

Welcome to the **Data Engineering Book & Master Learning Playlist**! 🚀

This repository is designed as a structured, progressive curriculum that takes you from foundational SQL queries and data manipulation to distributed computing, Medallion Lakehouse architectures, incremental batch processing, and building fault-tolerant workflow orchestrators.

---

## 🗺️ Master Curriculum Playlist & Learning Tracks

Follow the playlist sequentially or jump directly to specialized modules:

```
                                  DATA ENGINEERING PLAYLIST
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │                                                                                        │
 │  TRACK 1: Relational Foundations ──► [Practice SQL]                                    │
 │                                                                                        │
 │  TRACK 2: Data Manipulation      ──► [Pandas Tutorial]                                 │
 │                                                                                        │
 │  TRACK 3: Apache Spark Basics    ──► [PySpark Tutorial & Cheat Sheet]                  │
 │                                                                                        │
 │  TRACK 4: Enterprise Lakehouse   ──► [PySpark Ingestion & Production Engineering] ⚡   │
 │                                      ├── Day 01-03: Medallion & Incremental            │
 │                                      ├── Day 04-06: Tuning, Joins & Window Functions   │
 │                                      ├── Day 07-08: Modular Pipelines & Testing        │
 │                                      └── Day 09-10: Cloud JDBC & Custom DAG Engine     │
 │                                                                                        │
 │  TRACK 5: Industry Analytics     ──► [Zepto] [Retail] [Sales Dashboard]                │
 │                                      [Netflix Analysis] [Health Expenses]              │
 └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Learning Tracks Directory

| Episode / Track | Folder / Module | Level | Key Technologies | Description & Core Focus |
|:---:|:---|:---:|:---|:---|
| **Episode 1** | [**Practice SQL**](./Practice%20SQL) | Intermediate | ANSI SQL, PostgreSQL, MySQL | Complex window functions, recursive CTEs, grouping sets, running totals, and performance-tuned analytical queries. |
| **Episode 2** | [**Pandas**](./Pandas) | Beginner - Intermediate | Python 3, Pandas, NumPy | Exploratory data analysis (EDA), data wrangling, missing data imputation, joins, and vector manipulations. |
| **Episode 3** | [**PySpark Tutorial**](./PySpark%20Tutorial) | Intermediate | PySpark 4.0+, Spark SQL | Complete foundational interactive tutorial & rapid revision cheat sheet covering SparkSession, DataFrames, and UDFs. |
| **Episode 4** 🔥 | [**PySpark Ingestion & Production Engineering**](./PySpark%20Ingestion%20%26%20Production%20Engineering) | **Advanced / Production** | **PySpark, Parquet, Neon PostgreSQL JDBC, Custom DAG Engine** | **10-Day Intensive Masterclass: Medallion Architecture (Bronze/Silver/Gold), Dead-letter Quarantine, Incremental Watermarking, Cluster Tuning, Unit Testing & Retries.** |
| **Episode 5** | [**Zepto Quick-Commerce Analytics**](./Zepto) | Real-World Project | Python, SQL, Analytics | Delivery time optimization, order volume dynamics, surge patterns, and SLA performance tracking. |
| **Episode 6** | [**Retail Analytics**](./Retail) | Real-World Project | Python, EDA, Statistics | Customer cohort analysis, store-level inventory metrics, basket analysis, and revenue modeling. |
| **Episode 7** | [**Sales Performance Dashboard**](./Sales%20Dashboard) | BI Project | Power BI, DAX, Python | Interactive executive KPI dashboard tracking revenue trends, customer lifetime value, and regional performance. |
| **Episode 8** | [**Netflix Content Analytics**](./NetFlix%20Analysis) | Data Science | Python, Matplotlib, Seaborn | Streaming title distributions, release patterns, genre popularity trends, and movie vs. TV show dynamics. |
| **Episode 9** | [**Healthcare Expense Analytics**](./Health%20Expenses) | Statistical Modeling | Python, SQL, Power BI | Actuarial medical cost analysis, patient risk factors, diabetic dataset cleaning, and BI visual reports. |

---

## ⚡ Featured Track: PySpark Ingestion & Production Engineering (10-Day Masterclass)

The [**PySpark Ingestion & Production Engineering**](./PySpark%20Ingestion%20%26%20Production%20Engineering) module is an end-to-end masterclass featuring both a unified master notebook and 10 modular day-by-day reading guides:

```
  Raw Batch CSVs ──► Bronze Layer ──► Quarantine Filter ──► Silver Layer ──► Gold Analytics Mart
                      (Immutable)        (Bad Records)       (Clean Data)       (Business KPIs)
```

### 🗓️ The 10-Day Syllabus
- **Day 01**: [Medallion Architecture Fundamentals (Bronze/Silver/Gold)](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb)
- **Day 02**: [Production Data Quality & Quarantine Framework](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_02_Data_Quality_Validation_and_Quarantine.ipynb)
- **Day 03**: [Incremental Ingestion, Watermarking & Metadata Tracking](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb)
- **Day 04**: [Spark Performance Optimization, Partitioning & Broadcast Joins](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb)
- **Day 05**: [Advanced Multi-Table Joins & Dimensional Data Modeling](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb)
- **Day 06**: [Analytical Window Functions Deep Dive (`row_number`, `rank`, `lag`, `lead`, running totals)](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_06_Analytical_Window_Functions_Deep_Dive.ipynb)
- **Day 07**: [Modular Production Pipeline Architecture & Reusable Functions](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_07_Modular_Production_ETL_Architecture.ipynb)
- **Day 08**: [Enterprise Logging, Centralized Config & Automated Unit Testing](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb)
- **Day 09**: [Cloud Database Ingestion (Neon Serverless PostgreSQL via JDBC)](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb)
- **Day 10**: [Custom Airflow-Style DAG Pipeline Orchestrator with Retries & Fault Tolerance](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb)

👉 **Interactive Notebook**: [`PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb`](./PySpark%20Ingestion%20%26%20Production%20Engineering/PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb)

---

## 🚀 Quick Start Guide

### 1. Clone the Repository
```bash
git clone https://github.com/swapnanilchatterjee/Data-Engineering-Book.git
cd Data-Engineering-Book
```

### 2. Install Core Dependencies
```bash
pip install pyspark pandas pyarrow psycopg2-binary jupyter matplotlib seaborn
```

### 3. Launch Jupyter & Explore
```bash
jupyter notebook
```

---

## 👨‍💻 Author & Contributions
Maintained with ❤️ by **[Swapnanil Chatterjee](https://github.com/swapnanilchatterjee)**.

If you find this repository helpful for your data engineering journey or interview preparation, don't forget to **⭐ Star** the repo!
