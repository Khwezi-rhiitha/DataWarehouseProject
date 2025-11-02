# Retail_Store_Sales
This is an SQL project, implementing a Retail Sales Data Warehouse with a layered architecture (Medallion Architecture). Focuses on data cleaning, ETL processes, and generating analytical-ready datasets for reporting, dashboards, and advanced analytics.

## Dataset

The Dirty Retail Store Sales dataset contains 12,575 rows of synthetic data representing sales transactions from a retail store. The dataset includes eight product categories with 25 items per category, each having static prices.
The data for this project is sourced from the Kaggle dataset:
- **Dataset Link:** [Retail Store Sales Dataset](https://www.kaggle.com/datasets/ahmedmohamed2003/retail-store-sales-dirty-for-data-cleaning)

## Project Overview

### Objectives
1. **Centralize and standardize transactional data**
- Store all retail transactions in a structured format.
- Clean missing or inconsistent values.
- Compute derived metrics like discount value and discount rate.
2. **Ensure data quality and traceability**
- Add RecordStatus and DataQualityStatus indicators to highlight incomplete or inconsistent data.
- Maintain a historical record of data processing timestamps.
3. **Provide analytical-ready Gold layer views**
- Generate aggregated sales and customer insights.
- Enable business users to analyze trends by location, category, payment method, and customer.
- Serve as a foundation for dashboards, reporting, and KPI tracking.

### Architecture
**The warehouse follows a layered medallion architecture:**
1. **Bronze Layer**
- Stores unprocessed transactional data from source systems.
2. **Silver Layer**
- Cleansed, standardized, and enriched data.
- Includes computed fields (CalculatedDiscountValue, CalculatedDiscountRate) and data quality indicators (RecordStatus, DataQualityStatus).
3. **Gold Layer**
- Analytical views summarizing sales, categories, and total bought by customers .
- Aggregations suitable for dashboards, reporting, and BI tools.

### Key Features
1. Data Cleaning & Standardization: Handles missing or inconsistent values.
2. Data Quality Metrics: Automatically flags incomplete or missing records.
3. Derived Metrics: Calculates discounts, total spend, and revenue per transaction.
4. Customer Insights: Aggregated metrics per customer, including total spend, average discount, and data completeness.
5. Analytical Views: Ready-to-use Gold layer views for business reporting and decision-making.

## About Me
I am an aspiring data analyst who is proficient in R, SQL, and Excel. This project forms part of my portfolio and highlights the SQL skills required for data analyst roles. 
