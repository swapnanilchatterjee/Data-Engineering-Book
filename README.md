# 📊 Data Analytics Projects

Welcome to the **Data Analytics Projects** repository! 🚀

This repository serves as a centralized collection of hands-on data analytics, big data engineering, data science, and business intelligence projects.

---

## 📂 Repository Contents

| Directory | Topic / Focus | Key Technologies |
| :--- | :--- | :--- |
| [**PySpark Tutorial**](./PySpark%20Tutorial) | Complete Hands-On PySpark Tutorial & Last-Minute Revision Cheat Sheet | PySpark 4.0+, Apache Spark, Python, SQL, Window Functions |
| [**Pandas**](./Pandas) | Data manipulation, transformation, and exploratory data analysis | Python, Pandas, NumPy |
| [**Practice SQL**](./Practice%20SQL) | Complex SQL queries, window functions, CTEs, and relational database analytics | ANSI SQL, PostgreSQL / MySQL |
| [**NetFlix Analysis**](./NetFlix%20Analysis) | Content analysis, rating distributions, and trend visualization | Python, Matplotlib, Seaborn, Pandas |
| [**Retail**](./Retail) | Customer purchase analysis, inventory tracking, and retail analytics | Python, EDA, Data Visualization |
| [**Sales Dashboard**](./Sales%20Dashboard) | Sales performance tracking, KPIs, and BI dashboards | Power BI / Tableau / Python |
| [**Health Expenses**](./Health%20Expenses) | Medical cost analysis, insurance modeling, and statistical insights | Python, Statistical Modeling |
| [**Zepto**](./Zepto) | Quick-commerce analytics, delivery time analysis, and product insights | Python, Data Analytics |

---

## ⚡ Feature Spotlight: PySpark Hands-On Tutorial & Cheat Sheet

The [**PySpark Tutorial**](./PySpark%20Tutorial) folder contains a full end-to-end interactive guide and quick reference guide covering:
* **SparkSession & Architecture**: Cluster connection, driver/executors, lazy evaluation.
* **Data Ingestion & Schemas**: CSV, Parquet, JSON, and explicit `StructType` schemas.
* **Transformations & Filtering**: `select`, `filter`, `withColumn`, `when/otherwise`, `drop`.
* **Missing Value Handling**: `na.fill()`, `na.drop()`, row deduplication (`dropDuplicates`).
* **Aggregations & GroupBy**: Summary statistics, `.agg()`, `avg`, `max`, `min`, `count`.
* **Relational Joins**: Inner, Left, Right, Full Outer Joins.
* **Spark SQL Engine**: TempViews & running ANSI SQL directly on DataFrames.
* **User Defined Functions (UDFs)**: Custom Python functions in Spark pipelines.
* **Date & String Manipulations**: `to_timestamp`, `year`, `month`, `datediff`, `upper`, `concat`.
* **Window Functions**: `Window.partitionBy()`, `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`.
* **Pandas Interoperability**: `.toPandas()` and dataset exporting.

---

## 🚀 Getting Started

To explore any project locally:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/swapnanilchatterjee/Data-Analytics-Projects.git
   cd Data-Analytics-Projects
   ```

2. **Navigate to Desired Project**:
   ```bash
   cd "PySpark Tutorial"
   ```

3. **Install Dependencies & Run**:
   ```bash
   pip install -r requirements.txt # or pip install pyspark pandas jupyter
   jupyter notebook Pyspark_Tutorial.ipynb
   ```

---
Happy Analyzing! 📈
