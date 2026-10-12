# 🛍️ Omnichannel Retail Transaction Analytics

Welcome to the **Retail Analytics** module in **The Data Engineering Book**! 📈

This project models brick-and-mortar and digital retail sales transactions to extract revenue insights, customer demographics, and margin profitability.

---

## 📂 Module Files
- **`Retail.csv`**: Transactional records detailing `transaction_id`, `sale_date`, `sale_time`, `customer_id`, `gender`, `age`, `category`, `quantity`, `price_per_unit`, `cogs`, and `total_sale`.
- **`Retail.sql`**: Production SQL script with data ingestion schema, data hygiene checks, peak hour analysis, customer age segmentation, and net profit calculations.

---

## 🎯 Key Metrics & Queries
- **Profit Margin Calculation**: `SUM(total_sale - cogs)` by product category.
- **Peak Hour Analysis**: Extracting `sale_time` to identify store traffic spikes (morning vs afternoon vs evening).
- **Demographic Segmentation**: Age band grouping (Youth, Adults, Seniors) crossed with product categories.
- **Top Spenders**: Ranking customers by total monetary spend using window ranking.

---

## 🚀 How to Run
Run `Retail.sql` in PostgreSQL or MySQL after loading `Retail.csv`.
