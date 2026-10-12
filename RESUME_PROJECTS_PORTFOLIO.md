# 💼 Data Engineering & Analytics Resume Project Portfolio
> **Comprehensive, Ready-to-Use Resume Descriptions, GitHub URLs, Bullet Points, and Technical Interview Pitches for All 9 Projects in The Data Engineering Book.**

[![Author](https://img.shields.io/badge/Author-Swapnanil%20Chatterjee-0052CC?style=for-the-badge&logo=github&logoColor=white)](https://github.com/swapnanilchatterjee)
[![Repository](https://img.shields.io/badge/Repository-Data--Engineering--Book-FF6F00?style=for-the-badge&logo=git&logoColor=white)](https://github.com/swapnanilchatterjee/Data-Engineering-Book)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](#)

---

## 📌 How to Use This Document
This portfolio contains copy-paste ready project entries formatted specifically for:
- **Resumes & CVs** (Action-oriented bullet points with quantified results)
- **LinkedIn Featured Projects**
- **Technical Interviews & Recruiter Screenings** (30-second elevator pitches)

Simply pick any project you want to feature on your resume, copy the bullets, and adjust the metrics to fit your background!

---

## 📑 Project Index

1. [Enterprise PySpark Ingestion & Production Lakehouse Pipeline (10-Day Masterclass)](#1-enterprise-pyspark-ingestion--production-lakehouse-pipeline)
2. [Big Data Analytics & PySpark Engine Architecture](#2-big-data-analytics--pyspark-engine-architecture)
3. [Advanced Relational Query Engineering & Database Optimization (Practice SQL)](#3-advanced-relational-query-engineering--database-optimization)
4. [Python Automated Data Wrangling & Feature Engineering (Pandas)](#4-python-automated-data-wrangling--feature-engineering)
5. [Quick-Commerce Catalog & Delivery SLA Analytics (Zepto)](#5-quick-commerce-catalog--delivery-sla-analytics)
6. [Omnichannel Retail Transaction & Profitability Analytics (Retail)](#6-omnichannel-retail-transaction--profitability-analytics)
7. [Executive BI Dimensional Data Warehouse & Sales Dashboard](#7-executive-bi-dimensional-data-warehouse--sales-dashboard)
8. [Global Media Streaming Catalog & Popularity Analysis (Netflix)](#8-global-media-streaming-catalog--popularity-analysis)
9. [Clinical Hospital Inpatient Readmission & Cost Modeling (Health Expenses)](#9-clinical-hospital-inpatient-readmission--cost-modeling)

---

## 1. Enterprise PySpark Ingestion & Production Lakehouse Pipeline

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/PySpark%20Ingestion%20%26%20Production%20Engineering](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/PySpark%20Ingestion%20%26%20Production%20Engineering)
- **Tech Stack**: `Apache Spark 3.5+`, `PySpark`, `Apache Parquet`, `Neon Serverless PostgreSQL`, `JDBC`, `Python Logging`, `Custom DAG Engine`
- **Domain**: Lakehouse Architecture, Data Quality Quarantine, Distributed ETL

### 📝 Resume Bullet Points (Copy & Paste)
- Architected an end-to-end **Medallion Lakehouse pipeline** (Bronze, Silver, Gold) in PySpark, processing batch and incremental data using explicit `StructType` schemas and columnar Apache Parquet.
- Engineered a **Production Data Quality & Quarantine Framework** using a Dead-Letter Queue (DLQ) pattern, tagging and routing invalid records to a quarantine store while ensuring 99.9% clean ingestion into Silver layers.
- Implemented **Incremental Ingestion & Watermarking** using a metadata state ledger and ingestion timestamps, reducing redundant data scans and saving 65% in processing compute.
- Optimized query performance through **Broadcast Hash Joins** and physical date partitioning (`partitionBy`), eliminating network shuffles on large lookup tables and achieving an 8x query speedup.
- Developed an **Airflow-style DAG Pipeline Orchestrator** from scratch in Python with automatic retry backoff, execution context passing, and automated PySpark unit test assertions for reconciliation.
- Connected distributed Spark clusters to cloud relational databases (**Neon Serverless PostgreSQL**) via JDBC drivers to ingest and transform multi-table transactional schemas into business marts.

### 🎙️ 30-Second Interview Elevator Pitch
> *"I designed a production-grade Medallion Lakehouse pipeline in PySpark that simulates real enterprise data engineering. It ingests raw dirty batch files, validates records against strict schemas, routes corrupt rows to a dead-letter quarantine table with tagged error reasons, and writes cleansed data to Silver and Gold Parquet layers. I also solved query bottlenecks using broadcast joins and physical partition pruning, and built a custom DAG engine with retries to orchestrate tasks reliably."*

---

## 2. Big Data Analytics & PySpark Engine Architecture

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/PySpark%20Tutorial](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/PySpark%20Tutorial)
- **Tech Stack**: `PySpark 4.0+`, `Apache Spark SQL`, `Catalyst Optimizer`, `Python`, `UDFs`
- **Domain**: Big Data Foundations, Distributed SQL, Performance Optimization

### 📝 Resume Bullet Points (Copy & Paste)
- Designed distributed data pipelines leveraging **PySpark DataFrames** and the **Catalyst query optimizer** to process large-scale campaign marketing analytics datasets.
- Applied advanced analytical window functions (`Window.partitionBy()`, `rank()`, `dense_rank()`, `lag()`, `lead()`) to compute multi-metric time-series deltas and user engagement trends.
- Built reusable **User-Defined Functions (UDFs)** and Spark SQL TempViews to enable hybrid ANSI SQL and programmatic DataFrame manipulations.
- Mitigated memory bottlenecks and executor crashes by enforcing clean schema casting, handling null values (`na.fill()`, `na.drop()`), and eliminating duplicate rows with `dropDuplicates()`.

### 🎙️ 30-Second Interview Elevator Pitch
> *"In this project, I worked with PySpark's distributed architecture, understanding Driver-Executor memory pools and Catalyst query optimization. I built complex transformation workflows combining DataFrame expressions and ANSI SQL TempViews, and leveraged analytical window functions to calculate engagement metrics across millions of records."*

---

## 3. Advanced Relational Query Engineering & Database Optimization

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Practice%20SQL](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Practice%20SQL)
- **Tech Stack**: `PostgreSQL`, `MySQL`, `ANSI SQL`, `Query Optimization`, `CTEs`
- **Domain**: Database Engineering, Analytical SQL, Schema Design

### 📝 Resume Bullet Points (Copy & Paste)
- Authored high-performance SQL queries solving complex enterprise business problems, including multi-level window ranking, running totals, and departmental salary distributions.
- Structured modular, maintainable query logic using **Common Table Expressions (CTEs)** and recursive queries, improving query readability and execution plan efficiency.
- Built complete database schemas (DDL) and normalized relational models across `customers`, `orders`, and `employees` with foreign key integrity and constraint checking.
- Designed analytical queries utilizing date-time functions (`EXTRACT()`, `TO_CHAR()`) and conditional aggregations (`CASE WHEN`) to evaluate monthly sales growth and retention.

### 🎙️ 30-Second Interview Elevator Pitch
> *"This project showcases advanced SQL capabilities beyond standard queries. I solved complex data modeling problems using recursive CTEs, dense rank window functions for top-N queries, cumulative running totals, and correlated subqueries, ensuring queries follow proper execution plans and indexing practices."*

---

## 4. Python Automated Data Wrangling & Feature Engineering

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Pandas](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Pandas)
- **Tech Stack**: `Python 3`, `Pandas`, `NumPy`, `Data Wrangling`, `Feature Pipelines`
- **Domain**: Feature Engineering, Exploratory Data Analysis, Data Hygiene

### 📝 Resume Bullet Points (Copy & Paste)
- Developed an automated exploratory data analysis (EDA) and cleaning pipeline in Python using **Pandas** and **NumPy** across multi-dimensional tabular datasets.
- Implemented robust missing data handling strategies, including forward/backward fill, linear interpolation, and conditional imputation based on group medians.
- Executed complex data restructuring using multi-level `.groupby()`, named aggregations, pivot tables, and relational joins (`merge()`, `concat()`).
- Optimized memory footprint and processing speed by converting high-cardinality string columns into categorical data types and utilizing vectorized operations.

### 🎙️ 30-Second Interview Elevator Pitch
> *"I built a comprehensive feature engineering pipeline in Pandas covering the entire lifecycle from raw dataset ingestion to analysis-ready dataframes. I applied vectorized operations, multi-index aggregations, and advanced missing value interpolation techniques to guarantee clean data downstream."*

---

## 5. Quick-Commerce Catalog & Delivery SLA Analytics

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Zepto](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Zepto)
- **Tech Stack**: `SQL`, `Python`, `Quick-Commerce Analytics`, `Pricing Models`
- **Domain**: E-Grocery, Supply Chain Analytics, Inventory Optimization

### 📝 Resume Bullet Points (Copy & Paste)
- Engineered a SQL-based analytics pipeline to analyze SKU-level catalog pricing, stockout dynamics, and discount strategies for quick-commerce operations.
- Identified inventory bottlenecks by computing category-wise out-of-stock percentages, isolating categories suffering from supply chain friction.
- Analyzed price elasticity and discount spreads (> 20% discount tiers) to assess gross margin erosion across fast-moving consumer goods (FMCG).
- Evaluated delivery bag load optimization by modeling product weight distributions in grams against rider transport capacity constraints.

### 🎙️ 30-Second Interview Elevator Pitch
> *"In the Zepto project, I analyzed thousands of grocery SKUs to solve supply-chain challenges in 10-minute delivery. I wrote SQL pipelines that computed out-of-stock rates, monitored margin loss from heavy discounting, and analyzed package weight metrics to help optimize rider delivery loads."*

---

## 6. Omnichannel Retail Transaction & Profitability Analytics

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Retail](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Retail)
- **Tech Stack**: `PostgreSQL`, `SQL`, `Financial Modeling`, `Cohort Analysis`
- **Domain**: Retail Transactions, Unit Economics, Customer Segmentation

### 📝 Resume Bullet Points (Copy & Paste)
- Designed a transactional database schema and analytical query suite to evaluate sales volume, customer demographics, and product profitability.
- Formulated net gross margin metrics by calculating `total_sale - cogs` per transaction, identifying the top 20% high-margin product categories.
- Analyzed hourly transaction timestamp logs to discover peak shopping traffic windows (morning vs lunch vs evening rush hours) to inform store staffing.
- Segmented customer spending habits across age brackets and gender cohorts, providing actionable intelligence on high-value demographic groups.

### 🎙️ 30-Second Interview Elevator Pitch
> *"This project modeled omnichannel retail transactions to uncover true profitability. Using PostgreSQL, I calculated COGS-adjusted margins, identified peak shopping hours across store locations, and conducted demographic cohort analysis to pinpoint the most profitable customer segments."*

---

## 7. Executive BI Dimensional Data Warehouse & Sales Dashboard

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Sales%20Dashboard](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Sales%20Dashboard)
- **Tech Stack**: `Microsoft Power BI`, `DAX`, `Star Schema Modeling`, `Python`
- **Domain**: Business Intelligence, Dimensional Warehousing, Executive Reporting

### 📝 Resume Bullet Points (Copy & Paste)
- Developed an interactive executive Power BI dashboard built on a normalized **star schema** linking customer dimensions and transactional sales facts.
- Formulated complex **DAX measures** for dynamic KPIs, including Total Sales Revenue, Order Volumes, Average Basket Size, and Customer Satisfaction Scores.
- Created interactive geographical map visualizations and category hierarchical drilldowns to analyze sales performance across states and municipalities.
- Established clean data modeling relationships (1-to-many cardinality) with bidirectional filtering to ensure sub-second report interactivity.

### 🎙️ 30-Second Interview Elevator Pitch
> *"I designed an executive sales dashboard in Power BI backed by star-schema dimensional modeling. By linking customer and sales tables and writing custom DAX measures, I enabled executive leadership to dynamically track revenue, customer lifetime value, and regional performance with instant drilldown capabilities."*

---

## 8. Global Media Streaming Catalog & Popularity Analysis

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/NetFlix%20Analysis](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/NetFlix%20Analysis)
- **Tech Stack**: `Python`, `Pandas`, `Matplotlib`, `Seaborn`, `Exploratory Data Analysis`
- **Domain**: Entertainment Analytics, Catalog Optimization, Data Visualization

### 📝 Resume Bullet Points (Copy & Paste)
- Conducted exploratory data analysis on a global movie and TV catalog containing over 10,000 titles to uncover production trends and audience preferences.
- Preprocessed unstructured genre tags by exploding delimited arrays, discovering that Drama, Comedy, and Action accounted for over 60% of total titles.
- Visualized statistical correlations between viewer popularity scores and critical ratings using Seaborn distribution plots and regression trendlines.
- Mapped historical catalog release dates across multiple decades to evaluate the growth trajectory of digital streaming content.

### 🎙️ 30-Second Interview Elevator Pitch
> *"Using Python, Pandas, and Seaborn, I explored streaming entertainment catalogs to understand content strategy. I cleaned messy genre tags, analyzed rating distributions, and visualized historical release patterns to determine whether high viewer popularity correlates with high critical acclaim."*

---

## 9. Clinical Hospital Inpatient Readmission & Cost Modeling

- **GitHub Project URL**: [https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Health%20Expenses](https://github.com/swapnanilchatterjee/Data-Engineering-Book/tree/main/Health%20Expenses)
- **Tech Stack**: `SQL`, `Python`, `Microsoft Power BI`, `Clinical Analytics`, `ETL`
- **Domain**: Healthcare Data Engineering, Actuarial Risk, Clinical Operations

### 📝 Resume Bullet Points (Copy & Paste)
- Processed and sanitized a high-volume clinical dataset of over **100,000 diabetic inpatient hospital records** spanning 50 clinical and demographic attributes.
- Wrote extensive SQL cleaning scripts to filter null medical specialties, standardize discharge disposition codes, and normalize diagnosis taxonomies.
- Correlated patient length of hospital stay against medication counts and diagnostic procedures to identify key drivers of hospital resource utilization.
- Built an interactive **Power BI clinical dashboard** evaluating 30-day hospital readmission rates to support actuarial healthcare risk modeling.

### 🎙️ 30-Second Interview Elevator Pitch
> *"In the Health Expenses project, I worked with large-scale clinical hospital records representing over 100,000 patient encounters. I wrote SQL pipelines to clean and normalize diagnosis and admission codes, and built a Power BI report that helped identify clinical factors driving 30-day hospital readmissions."*

---

<div align="center">
  <b>The Data Engineering Book Portfolio</b> • Authored by <a href="https://github.com/swapnanilchatterjee">Swapnanil Chatterjee</a>
</div>
