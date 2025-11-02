/*
=============================================================
Create Tables
=============================================================
Script Purpose:
    -This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
*/

--Create a Table
IF OBJECT_ID ('silver.retail_store_sales', 'U') IS NOT NULL
    DROP TABLE silver.retail_store_sales;
GO
CREATE TABLE silver.retail_store_sales (
    TransactionalID VARCHAR(100),
    CustomerID VARCHAR(100),
    Category VARCHAR(150),
    Item VARCHAR(100),
    PricePerUnit DECIMAL,
    Quantity INT,
    TotalSpent DECIMAL,
    PaymentMethod VARCHAR(100),
    Location VARCHAR(100),
    TransactionDate DATE,
    DiscountApplied VARCHAR(50),
    dwh_create_data DATETIME2 DEFAULT GETDATE()
)
GO
