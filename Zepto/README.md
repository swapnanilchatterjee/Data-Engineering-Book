# 🛒 Zepto Quick-Commerce Delivery & SKU Analytics

Welcome to the **Zepto Analytics** case study in **The Data Engineering Book**! ⚡

This project investigates product pricing, stock availability, category distribution, and delivery dynamics for instant-delivery quick-commerce platforms.

---

## 📂 Module Files
- **`Zepto-Dataset.csv`**: Granular product catalog dataset with SKUs, MRPs, discount percentages, stock availability, and package weights.
- **`zepto.sql`**: SQL pipeline containing table DDL schema, exploratory queries, discount distribution analysis, and out-of-stock monitoring.

---

## 📊 Analytical Highlights
1. **Catalog Schema Definition**: Strict typing for SKU IDs, numerical price bounds, and stock flags.
2. **Discount Distribution**: Segmenting products with high discounts (> 20%) vs regular catalog items.
3. **Stockout Rate Monitoring**: Calculating out-of-stock percentages across FMCG categories.
4. **Volume & Weight Profiling**: Analyzing weight in grams to assess delivery bag load constraints.

---

## 🚀 How to Run
Import `Zepto-Dataset.csv` into your relational database and execute queries in `zepto.sql`.
