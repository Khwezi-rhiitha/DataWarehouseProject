/*
=============================================================
Add Columns
=============================================================
Script Purpose: Add calculated discount and status columns to silver.retail_store_sales
*/


-- Loading silver.retail_store_sales
PRINT '>> Truncating Table: silver.retail_store_sales';
TRUNCATE TABLE silver.retail_store_sales;
PRINT '>> Inserting Data Into: retail_store_sales';

PRINT '>> Ensuring new calculated columns exist in silver.retail_store_sales';
IF COL_LENGTH('silver.retail_store_sales', 'CalculatedDiscountValue') IS NULL
BEGIN
    ALTER TABLE silver.retail_store_sales
    ADD CalculatedDiscountValue DECIMAL(18,2),
        CalculatedDiscountRate DECIMAL(5,2),
        RecordStatus VARCHAR(50),
        DataQualityStatus VARCHAR(50);
    PRINT '>> Columns CalculatedDiscountValue, CalculatedDiscountRate, RecordStatus, and DataQualityStatus added successfully';
END
ELSE
BEGIN
    PRINT '>> Columns already exist, skipping ALTER TABLE';
END
