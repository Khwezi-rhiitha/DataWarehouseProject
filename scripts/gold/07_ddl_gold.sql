/*
===============================================================================
Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.
===============================================================================
*/


-- Create schema if it doesn’t exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- Drop existing views (if they already exist)
IF OBJECT_ID('gold.vw_sales_summary', 'V') IS NOT NULL
    DROP VIEW gold.vw_sales_summary;
GO

IF OBJECT_ID('gold.vw_category_performance', 'V') IS NOT NULL
    DROP VIEW gold.vw_category_performance;
GO

IF OBJECT_ID('gold.vw_data_quality_summary', 'V') IS NOT NULL
    DROP VIEW gold.vw_data_quality_summary;
GO

IF OBJECT_ID('gold.vw_customers_summary', 'V') IS NOT NULL
    DROP VIEW gold.vw_customers_summary;
GO

/* =============================================================
   View 1: vw_sales_summary
   Description: Aggregated summary of sales by Location and Payment Method
   ============================================================= */
CREATE VIEW gold.vw_sales_summary AS
SELECT
    c.Location,
    c.PaymentMethod,
    COUNT(DISTINCT c.TransactionalID) AS TotalTransactions,
    SUM(c.Quantity) AS TotalQuantity,
    SUM(c.TotalSpent) AS TotalRevenue,
    ROUND(AVG(TRY_CAST(c.CalculatedDiscountRate AS DECIMAL(10,2))), 2) AS AverageDiscountRate,
    ROUND(SUM(c.TotalSpent) / NULLIF(COUNT(DISTINCT c.TransactionalID), 0), 2) AS AverageSpendPerTxn,
    CAST(GETDATE() AS DATE) AS ReportDate
FROM silver.retail_store_sales AS c
GROUP BY c.Location, c.PaymentMethod;
GO

/* =============================================================
   View 2: vw_category_performance
   Description: Monthly sales and discount performance by category
   ============================================================= */
CREATE VIEW gold.vw_category_performance AS
SELECT
    c.Category,
    FORMAT(c.TransactionDate, 'yyyy-MM') AS Month,
    SUM(c.Quantity) AS TotalQuantity,
    SUM(c.TotalSpent) AS TotalRevenue,
    ROUND(AVG(TRY_CAST(c.CalculatedDiscountRate AS DECIMAL(10,2))), 2) AS AvgDiscountRate,
    CAST(GETDATE() AS DATE) AS ReportDate
FROM silver.retail_store_sales AS c
GROUP BY c.Category, FORMAT(c.TransactionDate, 'yyyy-MM');
GO

/* =============================================================
   View 3: vw_data_quality_summary
   Description: Summarizes data completeness and quality
   ============================================================= */
CREATE VIEW gold.vw_data_quality_summary AS
SELECT
    RecordStatus,
    DataQualityStatus,
    COUNT(*) AS TotalRecords,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS PercentageOfTotal,
    CAST(GETDATE() AS DATE) AS ReportDate
FROM silver.retail_store_sales AS c
GROUP BY 
    RecordStatus,
    DataQualityStatus
GO

/* =============================================================
   View 4: vw_customers_summary
   Description: Includes spending, and quantity
   ============================================================= */
CREATE VIEW gold.vw_customers_summary AS
SELECT
    c.CustomerID,
    -- Total transactions for this customer
    COUNT(c.TransactionalID) AS TotalTransactions,
    
    -- Total quantity purchased
    SUM(c.Quantity) AS TotalQuantity,
    
    -- Total amount spent
    SUM(c.TotalSpent) AS TotalSpent
FROM silver.retail_store_sales AS c
GROUP BY c.CustomerID;
GO

/*select * from gold.vw_sales_summary
select * from gold.vw_category_performance
select * from gold.vw_data_quality_summary
select * from gold.vw_customers_summary*/
