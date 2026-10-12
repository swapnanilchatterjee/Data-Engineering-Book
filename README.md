<div align="center">

# 📖 The Data Engineering Book
### *An End-to-End Hands-On Curriculum, Production Lakehouse Guide & Master Learning Playlist*

[![Author](https://img.shields.io/badge/Author-Swapnanil%20Chatterjee-0052CC?style=for-the-badge&logo=github&logoColor=white)](https://github.com/swapnanilchatterjee)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B%20%2F%204.0%2B-F26522?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Cloud%20Neon-00E599?style=for-the-badge&logo=postgresql&logoColor=white)](https://neon.tech)
[![Power BI](https://img.shields.io/badge/BI-Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](#)
[![Cheat Sheet](https://img.shields.io/badge/Cheat%20Sheet-Before--Interview%20Guide-E0234E?style=for-the-badge)](./BEFORE_INTERVIEW_CHEATSHEET.md)
[![Resume Bullets](https://img.shields.io/badge/Portfolio-Resume%20Project%20Bullets-6C5CE7?style=for-the-badge)](./RESUME_PROJECTS_PORTFOLIO.md)

<p align="center">
  <b>A complete production compendium spanning Relational SQL Engineering, Python Feature Pipelines, Apache Spark Foundations, a 10-Day Production Lakehouse Masterclass, and Real-World Domain Platforms.</b>
</p>

---

</div>

## 📌 Table of Contents
1. [Book Architecture & Playlist Roadmap](#-book-architecture--playlist-roadmap)
2. [Complete Repository Folder Structure](#-complete-repository-folder-structure)
3. [Master Learning Playlist & Project Directory](#-master-learning-playlist--project-directory)
4. [Deep-Dive Project Walkthroughs (Every Single Project in Detail)](#-deep-dive-project-walkthroughs)
   - [Project 1: PySpark Ingestion & Production Engineering (10-Day Masterclass)](#1-pyspark-ingestion--production-engineering-10-day-masterclass)
   - [Project 2: PySpark Tutorial & Quick Reference Cheat Sheet](#2-pyspark-tutorial--quick-reference-cheat-sheet)
   - [Project 3: Practice SQL (Query Engineering & 15 Top Interview Problems)](#3-practice-sql-query-engineering--15-top-interview-problems)
   - [Project 4: Pandas (Data Wrangling & Exploratory Feature Pipelines)](#4-pandas-data-wrangling--exploratory-feature-pipelines)
   - [Project 5: Zepto (Quick-Commerce Delivery & SKU Analytics)](#5-zepto-quick-commerce-delivery--sku-analytics)
   - [Project 6: Retail (Omnichannel Transactional Analytics)](#6-retail-omnichannel-transactional-analytics)
   - [Project 7: Sales Dashboard (Executive BI & Dimensional Warehousing)](#7-sales-dashboard-executive-bi--dimensional-warehousing)
   - [Project 8: NetFlix Analysis (Entertainment Catalog & Popularity EDA)](#8-netflix-analysis-entertainment-catalog--popularity-eda)
   - [Project 9: Health Expenses (Clinical Analytics & Actuarial Modeling)](#9-health-expenses-clinical-analytics--actuarial-modeling)
5. [⚡ Before-Interview Rapid Revision Cheat Sheet](./BEFORE_INTERVIEW_CHEATSHEET.md)
6. [💼 Resume Project Portfolio (Bullets, Impact & URLs)](./RESUME_PROJECTS_PORTFOLIO.md)
7. [Local Environment Setup & Installation](#-local-environment-setup--installation)
8. [Contributing & Community](#-contributing--community)

---

## 🗺️ Book Architecture & Playlist Roadmap

```
                            THE DATA ENGINEERING BOOK
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
 │  TRACK 5: Industry Platforms     ──► [Zepto] [Retail] [Sales Dashboard]                │
 │                                      [Netflix Analysis] [Health Expenses]              │
 └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Complete Repository Folder Structure

Below is the verified, complete directory tree of the repository:

```
Data-Engineering-Book/
│
├── 📂 PySpark Ingestion & Production Engineering/               # 10-Day Production Masterclass Track
│   ├── PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb # Unified 10-day masterclass notebook
│   ├── README.md                                                 # Masterclass curriculum & architecture guide
│   ├── orders.csv                                                # Day 01 clean batch orders dataset
│   ├── orders_day2.csv                                           # Day 02 dirty data batch (DQ testing)
│   ├── postgresql-42.6.0.jar                                     # PostgreSQL JDBC driver for cloud ingestion
│   ├── PysparkInterview Qs.pdf                                   # PySpark interview questions & answers
│   ├── 📂 metadata/                                              # State audit table for watermarking
│   │   └── processed_files.csv
│   ├── 📂 source/                                                # Incremental daily batch files
│   │   ├── orders_2026_09_27.csv
│   │   ├── orders_2026_09_28.csv
│   │   ├── orders_2026_09_29.csv
│   │   └── orders_2026_09_30.csv
│   └── 📂 modules/                                               # 10 Standalone modular day-by-day notebooks
│       ├── Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb
│       ├── Day_02_Data_Quality_Validation_and_Quarantine.ipynb
│       ├── Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb
│       ├── Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb
│       ├── Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb
│       ├── Day_06_Analytical_Window_Functions_Deep_Dive.ipynb
│       ├── Day_07_Modular_Production_ETL_Architecture.ipynb
│       ├── Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb
│       ├── Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb
│       └── Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb
│
├── 📂 PySpark Tutorial/                                         # Foundational PySpark & Spark SQL
│   ├── Pyspark_Tutorial.ipynb                                    # Hands-on interactive tutorial notebook
│   ├── popularity_campaign_v2 1.csv                              # Campaign analytics practice dataset
│   └── README.md                                                 # PySpark quick reference cheat sheet
│
├── 📂 Practice SQL/                                             # Relational Query Engineering
│   ├── Practice Common 15.sql                                    # Top 15 SQL interview queries with answers
│   ├── Common 15 Data.sql                                        # DDL tables & DML seed inserts
│   ├── Practice all types.sql                                    # Joins, CTEs, subqueries, and windowing
│   └── README.md                                                 # Query guide & concepts overview
│
├── 📂 Pandas/                                                   # Python Feature Pipelines & Data Wrangling
│   ├── Pandas_Tutorial.ipynb                                     # Comprehensive 67-cell interactive notebook
│   └── README.md                                                 # Indexing, missing values & aggregation guide
│
├── 📂 Zepto/                                                    # Quick-Commerce Ingestion & SKU Analytics
│   ├── Zepto-Dataset.csv                                         # SKU catalog dataset with MRPs & stock
│   ├── zepto.sql                                                 # Analytical SQL pipeline & margin analysis
│   └── README.md                                                 # Business case study & query documentation
│
├── 📂 Retail/                                                   # Omnichannel Retail Transaction Analytics
│   ├── Retail.csv                                                # Sales logs with COGS & customer metadata
│   ├── Retail.sql                                                # Peak hours, profit margins, and cohort SQL
│   └── README.md                                                 # Retail metrics & documentation
│
├── 📂 Sales Dashboard/                                          # Executive BI & Dimensional Warehousing
│   ├── My First Dashboard.pbix                                   # Interactive Power BI report
│   ├── customers.csv                                             # Dimension table: customer profiles
│   ├── orders.csv                                                # Fact table: transactional sales
│   └── README.md                                                 # Schema dictionary & usage instructions
│
├── 📂 NetFlix Analysis/                                         # Media Streaming Catalog & Popularity EDA
│   ├── mymoviedb.csv                                             # 10,000+ movie titles with ratings & genres
│   ├── Netflix_Analysis.ipynb                                    # 46-cell exploratory analysis & visualizations
│   └── README.md                                                 # Data insights & visual methodology
│
├── 📂 Health Expenses/                                          # Clinical & Actuarial Hospital Analytics
│   ├── Diabetic_data.csv                                         # Raw 100K+ inpatient records (19MB)
│   ├── cleaned_diabetic_data.csv                                 # Cleaned, standardized records (17.5MB)
│   ├── Health cleaning.sql                                       # SQL standardization & data hygiene
│   ├── Health expenses.pbix                                      # Power BI readmission & cost report
│   ├── Health_Expenses.ipynb                                     # Exploratory demographics & cost factors
│   └── README.md                                                 # Clinical analysis documentation
│
├── 📄 BEFORE_INTERVIEW_CHEATSHEET.md                             # High-yield 30-min pre-interview revision guide
├── 📄 RESUME_PROJECTS_PORTFOLIO.md                                # Copy-paste resume bullets & interview elevator pitches
└── README.md                                                    # Main repository guide (This File)
```

---

## 📺 Master Learning Playlist & Project Directory

| Episode | Project Module | Domain / Level | Core Stack | Key Highlights |
|:---:|:---|:---:|:---|:---|
| **01** | [**Practice SQL**](./Practice%20SQL) | Relational SQL / Interm. | PostgreSQL, MySQL, ANSI SQL | Top 15 interview queries, recursive CTEs, window ranking, salary deltas, and multi-table joins. |
| **02** | [**Pandas**](./Pandas) | Python Wrangling / Interm. | Python 3, Pandas, NumPy | `.loc` vs `.iloc`, missing value imputation, multi-level group-by, merging, reshaping. |
| **03** | [**PySpark Tutorial**](./PySpark%20Tutorial) | Big Data Core / Interm. | PySpark 4.0+, Spark SQL | SparkSession setup, Catalyst optimizer, UDFs, TempViews, windowing, and rapid cheat sheet. |
| **04** 🔥 | [**PySpark Ingestion & Production Engineering**](./PySpark%20Ingestion%20%26%20Production%20Engineering) | **Enterprise / Advanced** | **PySpark, Parquet, Neon PostgreSQL, Custom DAG** | **10-Day Masterclass: Medallion Architecture, Dead-letter DLQ, Incremental Watermarking, Cluster Tuning, Testing, Cloud JDBC & Retrying DAG Engine.** |
| **05** | [**Zepto**](./Zepto) | Quick-Commerce / Case | SQL, Analytics, Python | FMCG SKU catalog analysis, out-of-stock monitoring, discount spreads, and delivery bag weight constraints. |
| **06** | [**Retail**](./Retail) | Omnichannel / Case | SQL, Statistics, Python | Profit margin calculation (`total_sale - cogs`), peak shopping hours, demographic age cohorts. |
| **07** | [**Sales Dashboard**](./Sales%20Dashboard) | BI Engineering / Exec | Power BI, DAX, Python | Interactive star-schema report, regional revenue maps, customer score distribution, KPI tiles. |
| **08** | [**NetFlix Analysis**](./NetFlix%20Analysis) | Media Analytics / EDA | Python, Matplotlib, Seaborn | Exploding genre arrays, rating distributions, popularity vs. vote correlation, release timeline. |
| **09** | [**Health Expenses**](./Health%20Expenses) | Clinical & Costs / Case | SQL, Power BI, Python | 100,000+ patient records, diabetic readmission factors, hospital stay duration, and cost drivers. |

---

## 🔍 Deep-Dive Project Walkthroughs

### 1. PySpark Ingestion & Production Engineering (10-Day Masterclass)
- **Directory**: [`PySpark Ingestion & Production Engineering/`](./PySpark%20Ingestion%20%26%20Production%20Engineering/)
- **Unified Master Notebook**: [`PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb`](./PySpark%20Ingestion%20%26%20Production%20Engineering/PySpark_Zero_to_Hero_Production_Pipeline_Masterclass.ipynb)
- **Core Technologies**: PySpark 3.5+/4.0+, Apache Parquet, Neon Serverless PostgreSQL, Python Logging, Custom DAG Engine.

```
  Raw Source Files                Bronze Layer                 Silver Layer                  Gold Layer
  (CSV / JSON / JDBC)           (Immutable Raw)           (Cleaned & Enriched)          (Curated Business)
 ┌───────────────────┐        ┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
 │ orders.csv        │ ────►  │ bronze_orders/   │ ────►  │ silver_orders/   │ ────►  │ gold_category/   │
 │ (External source) │        │ (Parquet copy)   │        │ (Deduplicated,   │        │ gold_city/       │
 └───────────────────┘        └──────────────────┘        │  imputed, typed) │        │ (Aggregated)     │
                                                          └──────────────────┘        └──────────────────┘
```

#### The 10-Day Progressive Curriculum:
- **[Day 01: Medallion Architecture Fundamentals](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_01_Medallion_Architecture_Bronze_Silver_Gold.ipynb)**: Explicit `StructType` schema definition, raw CSV ingestion, Bronze append-only storage, Silver cleansing (imputing nulls, casting dates, calculating revenue), Gold aggregation marts.
- **[Day 02: Production Data Quality & Validation Framework](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_02_Data_Quality_Validation_and_Quarantine.ipynb)**: Building a Dead-Letter Queue (DLQ) quarantine routing clean data to `silver_orders_day2` and corrupt rows to `rejected_orders_day2` with tagged `rejection_reason` causes.
- **[Day 03: Incremental Ingestion & Metadata Tracking](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_03_Incremental_Ingestion_and_Metadata_Audit.ipynb)**: Differential file arrival detection, updating the metadata state store (`metadata/processed_files.csv`), appending lineage watermarks (`ingestion_timestamp`), atomic Parquet appends.
- **[Day 04: Spark Performance Optimization & Partitioning](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_04_Performance_Tuning_Partitioning_and_Broadcast.ipynb)**: Deep dive into `repartition` (shuffle) vs `coalesce` (narrow merge), physical `partitionBy("order_date")`, verifying partition pruning with `.explain()`, Broadcast joins (`broadcast()`), in-memory caching lifecycle.
- **[Day 05: Advanced Joins & Dimensional Data Modeling](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_05_Advanced_Joins_Transformations_and_Data_Modeling.ipynb)**: Star schema modeling with Orders, Customers, and Products; inner/left/right/full joins; column aliasing (`orders.alias("o")`); conditional branching with `F.when().otherwise()`.
- **[Day 06: Analytical Window Functions Deep Dive](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_06_Analytical_Window_Functions_Deep_Dive.ipynb)**: `Window.partitionBy()`, `row_number()`, `rank()` vs `dense_rank()`, inter-row metrics with `lag()` and `lead()`, cumulative running revenue totals, rolling 3-row moving averages, Top-N ranking.
- **[Day 07: Modular Production Pipeline Architecture](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_07_Modular_Production_ETL_Architecture.ipynb)**: Converting monolithic scripts into modular, testable Python functions (`ingest_csv`, `validate_orders`, `deduplicate_orders`, `transform_orders`, `write_parquet`) orchestrated by `run_pipeline()`.
- **[Day 08: Enterprise Logging, Configuration & Testing](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_08_Enterprise_Logging_Config_and_Automated_Testing.ipynb)**: Centralized configuration management (`CONFIG`), dual file/stream logging (`pipeline.log`), SLA rejection limits (abort if rejection > 30%), automated PySpark assertion unit tests.
- **[Day 09: Cloud Database Connectivity & Neon PostgreSQL JDBC](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_09_Cloud_PostgreSQL_JDBC_and_Lakehouse_Ingestion.ipynb)**: Serverless Neon PostgreSQL connection, `postgresql-42.6.0.jar` driver integration, `psycopg2` + Spark bridge, hybrid cloud joins, writing to local Parquet Gold tables.
- **[Day 10: Custom DAG Pipeline Orchestrator with Retries](./PySpark%20Ingestion%20%26%20Production%20Engineering/modules/Day_10_Custom_DAG_Pipeline_Orchestrator.ipynb)**: Building an Airflow-style DAG workflow engine (`SimpleOrchestrator`) from scratch with task dependency chaining, automated retry backoff, and fault injection tests.

---

### 2. PySpark Tutorial & Quick Reference Cheat Sheet
- **Directory**: [`PySpark Tutorial/`](./PySpark%20Tutorial/)
- **Core Files**: [`Pyspark_Tutorial.ipynb`](./PySpark%20Tutorial/Pyspark_Tutorial.ipynb), [`popularity_campaign_v2 1.csv`](./PySpark%20Tutorial/popularity_campaign_v2%201.csv), [`README.md`](./PySpark%20Tutorial/README.md)
- **Overview**: An end-to-end interactive tutorial and last-minute revision guide for Apache Spark using Python.
- **Key Concepts Covered**:
  - Spark cluster architecture: Driver, Worker Nodes, Executors, DAG Scheduler.
  - SparkSession entry point and configuration.
  - Data ingestion with explicit `StructType` schemas vs schema inference.
  - Filtering, projection, column transformations (`select`, `filter`, `withColumn`, `when/otherwise`).
  - Missing value imputation (`na.fill()`, `na.drop()`) and deduplication (`dropDuplicates()`).
  - Spark SQL TempViews and running ANSI SQL queries directly on DataFrames.
  - User-Defined Functions (UDFs) for custom Python transformation logic.
  - Analytical windowing operations (`Window.partitionBy()`, `row_number()`, `lag()`).

---

### 3. Practice SQL (Query Engineering & 15 Top Interview Problems)
- **Directory**: [`Practice SQL/`](./Practice%20SQL/)
- **Core Files**: [`Practice Common 15.sql`](./Practice%20SQL/Practice%20Common%2015.sql), [`Common 15 Data.sql`](./Practice%20SQL/Common%2015%20Data.sql), [`Practice all types.sql`](./Practice%20SQL/Practice%20all%20types.sql), [`README.md`](./Practice%20SQL/README.md)
- **Overview**: A masterclass in relational SQL query engineering, covering the 15 most frequent queries asked in Big Data and Data Engineering interviews.
- **Problem Highlights**:
  1. **Monthly Sales Extraction**: Using `EXTRACT(MONTH FROM order_date)` and `TO_CHAR()` with aggregation.
  2. **Active Customer Filtering**: Identifying customers who placed orders after a cutoff date using inner joins.
  3. **Duplicate Record Detection**: Grouping by natural keys and filtering with `HAVING COUNT(*) > 1`.
  4. **Running Sales Totals**: Computing cumulative revenue with `SUM(order_amount) OVER (ORDER BY order_date)`.
  5. **Second Highest Salary**: Solving using both subquery `MAX()` and `DENSE_RANK() OVER (ORDER BY salary DESC)`.
  6. **Departmental Top Earners**: Partitioning salaries by department with window ranking.
  7. **Correlated Subqueries & Exists**: Matching customer order existence against master catalogs.
  8. **Common Table Expressions (CTEs)**: Modularizing complex multi-step pipelines for readability and performance.
  9. **DML Setup Script**: Complete `CREATE TABLE` and `INSERT INTO` statements for `customers`, `orders`, and `employees`.

---

### 4. Pandas (Data Wrangling & Exploratory Feature Pipelines)
- **Directory**: [`Pandas/`](./Pandas/)
- **Core Files**: [`Pandas_Tutorial.ipynb`](./Pandas/Pandas_Tutorial.ipynb), [`README.md`](./Pandas/README.md)
- **Overview**: A 67-cell comprehensive notebook that covers data wrangling, exploratory data analysis, and feature engineering.
- **Key Concepts Covered**:
  - Series and DataFrame instantiation from Python dictionaries, lists, and external CSVs.
  - Indexing mastery: Label-based `.loc[]` vs integer position `.iloc[]`.
  - Missing data detection and handling: `.isna().sum()`, `.dropna()`, `.fillna()`, and forward/backward interpolation.
  - Boolean indexing, compound conditions (`&`, `|`), `.isin()`, and string pattern matching.
  - Multi-level group-by operations, custom aggregation dictionaries, and pivot tables.
  - Relational operations: Inner, outer, left, and right joins via `.merge()`, and dataset concatenation (`pd.concat()`).

---

### 5. Zepto (Quick-Commerce Delivery & SKU Analytics)
- **Directory**: [`Zepto/`](./Zepto/)
- **Core Files**: [`Zepto-Dataset.csv`](./Zepto/Zepto-Dataset.csv), [`zepto.sql`](./Zepto/zepto.sql), [`README.md`](./Zepto/README.md)
- **Overview**: Real-world e-grocery analytics investigating SKU pricing strategies, stockout rates, and product weight constraints in quick-commerce.
- **Analytical Highlights**:
  - **DDL Schema**: Creating the `zepto` table with strict typing for `sku_id`, `category`, `mrp`, `discountPercent`, `discountedSellingPrice`, and `outOfStock`.
  - **Stockout Analysis**: Computing out-of-stock percentages across FMCG categories to detect supply-chain friction.
  - **Discount Spread**: Segmenting catalog items by discount depth (> 20% vs regular price) to evaluate margin erosion.
  - **Weight Capacity Constraints**: Analyzing package weights in grams to optimize delivery rider bag loading and routing.

---

### 6. Retail (Omnichannel Transactional Analytics)
- **Directory**: [`Retail/`](./Retail/)
- **Core Files**: [`Retail.csv`](./Retail/Retail.csv), [`Retail.sql`](./Retail/Retail.sql), [`README.md`](./Retail/README.md)
- **Overview**: Transactional analytics pipeline examining sales logs, customer age cohorts, peak shopping hours, and net profit margins.
- **Analytical Highlights**:
  - **Schema Setup**: Ingesting `transaction_id`, `sale_date`, `sale_time`, `customer_id`, `gender`, `age`, `category`, `quantity`, `price_per_unit`, `cogs`, and `total_sale`.
  - **Profitability Modeling**: Computing net gross profit per transaction (`total_sale - cogs`) across departments.
  - **Peak Hour Analysis**: Extracting `sale_time` to identify store traffic rushes (morning vs. lunch vs. evening spikes).
  - **Demographic Cohort Analysis**: Cross-tabulating customer age bands (Youth, Middle-Aged, Senior) with category preference.
  - **Top Spending Customers**: Using window ranking functions to isolate top 10% highest-value shoppers.

---

### 7. Sales Dashboard (Executive BI & Dimensional Warehousing)
- **Directory**: [`Sales Dashboard/`](./Sales%20Dashboard/)
- **Core Files**: [`My First Dashboard.pbix`](./Sales%20Dashboard/My%20First%20Dashboard.pbix), [`customers.csv`](./Sales%20Dashboard/customers.csv), [`orders.csv`](./Sales%20Dashboard/orders.csv), [`README.md`](./Sales%20Dashboard/README.md)
- **Overview**: An interactive Power BI executive dashboard built on a relational star schema.
- **Analytical Highlights**:
  - **Data Modeling**: One-to-many relationship linking `customers.csv` (`customer_id`, location, score) to `orders.csv` (`order_id`, date, product, quantity, sales).
  - **Executive KPIs**: Cards displaying Total Revenue, Units Sold, Unique Customers, and Average Customer Score.
  - **Geographic Mapping**: Regional revenue drilldowns across states and cities.
  - **Visualizations**: Category-wise revenue breakdown, monthly order volume trends, and interactive slicers.

---

### 8. NetFlix Analysis (Entertainment Catalog & Popularity EDA)
- **Directory**: [`NetFlix Analysis/`](./NetFlix%20Analysis/)
- **Core Files**: [`mymoviedb.csv`](./NetFlix%20Analysis/mymoviedb.csv), [`Netflix_Analysis.ipynb`](./NetFlix%20Analysis/Netflix_Analysis.ipynb), [`README.md`](./NetFlix%20Analysis/README.md)
- **Overview**: A 46-cell exploratory data science study analyzing global streaming titles, rating patterns, and genre popularity.
- **Analytical Highlights**:
  - **Catalog Ingestion & Cleaning**: Handling missing values in movie overviews and formatting release dates.
  - **Genre Exploding**: Splitting comma-separated genre strings into individual records to identify top produced genres (Drama, Comedy, Action).
  - **Popularity vs. Rating Correlation**: Measuring whether high viewer popularity correlates with critical vote averages.
  - **Timeline Dynamics**: Tracking the volume of releases over historical decades to visualize the streaming boom.
  - **Visualizations**: Seaborn countplots, distribution histograms, and correlation heatmaps.

---

### 9. Health Expenses (Clinical Analytics & Actuarial Modeling)
- **Directory**: [`Health Expenses/`](./Health%20Expenses/)
- **Core Files**: [`Diabetic_data.csv`](./Health%20Expenses/Diabetic_data.csv), [`cleaned_diabetic_data.csv`](./Health%20Expenses/cleaned_diabetic_data.csv), [`Health cleaning.sql`](./Health%20Expenses/Health%20cleaning.sql), [`Health expenses.pbix`](./Health%20Expenses/Health%20expenses.pbix), [`Health_Expenses.ipynb`](./Health%20Expenses/Health_Expenses.ipynb), [`README.md`](./Health%20Expenses/README.md)
- **Overview**: High-scale healthcare analytics evaluating over 100,000 inpatient records across 50 clinical attributes to identify cost drivers and hospital readmission risk factors.
- **Analytical Highlights**:
  - **Data Scale**: Raw dataset of 19MB (`Diabetic_data.csv`) and sanitized version of 17.5MB (`cleaned_diabetic_data.csv`).
  - **SQL Data Cleaning**: Standardizing admission types, discharge dispositions, diagnosis codes, and filtering null specialties.
  - **Length of Stay Correlation**: Modeling patient time in hospital against medication counts and number of lab procedures.
  - **Readmission Risk Evaluation**: Analyzing 30-day hospital readmission rates to support actuarial healthcare models.
  - **Power BI Dashboard**: Interactive clinical report (`Health expenses.pbix`) visualizing patient demographics and hospital KPIs.

---

## ⚙️ Local Environment Setup & Installation

### 1. Clone the Book Repository
```bash
git clone https://github.com/swapnanilchatterjee/Data-Engineering-Book.git
cd Data-Engineering-Book
```

### 2. Environment Prerequisites
- **Python**: Version 3.10+ recommended
- **Java**: Java 8, 11, or 17 (required for Apache Spark: verify via `java -version`)
- **Windows Users**: Apache Spark requires `winutils.exe` and `hadoop.dll`. You can install them via:
  ```bash
  pip install winutils-pyspark
  ```
  Or set `HADOOP_HOME` to a local directory containing `bin/winutils.exe`.

### 3. Install Python Dependencies
```bash
pip install pyspark pandas pyarrow psycopg2-binary jupyter matplotlib seaborn
```

### 4. Launch Jupyter Notebook
```bash
jupyter notebook
```
Navigate to any project folder to run notebooks interactively!

---

## 🤝 Contributing & Community
Contributions, improvements, and discussions are warmly welcomed!
- Found a bug or typo? Submit a pull request.
- Have an idea for a new data engineering module? Open an issue.

⭐ **If you find this book valuable for your data engineering journey or interview preparation, please consider giving the repository a Star!**

---
<div align="center">
  <b>The Data Engineering Book</b> • Maintained with ❤️ by <a href="https://github.com/swapnanilchatterjee">Swapnanil Chatterjee</a>
</div>
