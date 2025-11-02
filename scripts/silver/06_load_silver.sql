/*
=============================================================
Load Data - Using Stored Procedure
=============================================================
Script Purpose:
    -This stored procedure performs the ETL (Extract, Transform, Load) process to 
		populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
	- Truncates Silver tables.
	- Inserts transformed and cleansed data from Bronze into Silver tables.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
	
		--INSERT TABLE
		INSERT INTO silver.retail_store_sales (
			TransactionalID, 
			CustomerID, 
			Category, 
			Item, 
			CalculatedDiscountValue,
			CalculatedDiscountRate,
			PricePerUnit, 
			Quantity,
			TotalSpent,
			PaymentMethod,
			Location,
			TransactionDate,
			DiscountApplied,
			RecordStatus,
			DataQualityStatus
		)
		SELECT
			-- 1. TransactionalID
			TransactionalID, 

			-- 2. CustomerID
			CustomerID,
			
			-- 3. Category
			Category, 

			-- 4. Item (If Item IS NULL replace with 'Unknown')
			ISNULL(Item, 'Unknown') AS Item,

			-- 5. Compute Actual Discount Value if possible
			CASE 
				WHEN PricePerUnit IS NOT NULL 
					 AND Quantity IS NOT NULL 
					 AND TotalSpent IS NOT NULL
				THEN (PricePerUnit * Quantity) - TotalSpent
				ELSE NULL
			END AS CalculatedDiscountValue,

			-- 6. Compute Discount Rate (%)
			CASE 
				WHEN PricePerUnit IS NOT NULL 
					 AND Quantity IS NOT NULL 
					 AND (PricePerUnit * Quantity) > 0
				THEN ROUND(((PricePerUnit * Quantity) - TotalSpent) / (PricePerUnit * Quantity) * 100, 2)
				ELSE NULL
			END AS CalculatedDiscountRate,

			-- 7. Adjust PricePerUnit if missing (use TotalSpent / Quantity, factoring discount)
			CASE
				WHEN (PricePerUnit IS NULL OR PricePerUnit = 0)
					 AND Item IS NOT NULL
					 AND Item <> 'Unknown'
					 AND Quantity IS NOT NULL
					 AND Quantity <> 0
					 AND TotalSpent IS NOT NULL THEN
					ROUND(TotalSpent / Quantity, 2)  -- actual unit price based on total spent
				ELSE ISNULL(PricePerUnit, 0)
			END AS PricePerUnit,

			-- 8. Quantity (Checks if Quantity IS NULL, If Null replace it 0)
			ISNULL(Quantity, 0) AS Quantity,

			-- 9. TotalSpent (recheck to ensure consistency with PricePerUnit * Quantity - DiscountApplied)
			CASE
				WHEN PricePerUnit IS NOT NULL AND Quantity IS NOT NULL THEN
					CASE 
						WHEN DiscountApplied IS NULL THEN PricePerUnit * Quantity
						ELSE TotalSpent  -- keep TotalSpent if discount is explicitly recorded
					END
				ELSE ISNULL(TotalSpent, 0)
			END AS TotalSpent,

			-- 10. PaymentMethod
			PaymentMethod,

			-- 11. Location
			Location,

			-- 12 TransactionDate
			ISNULL(CAST(TransactionDate AS DATETIME), '1900-01-01 00:00:00') AS TransactionDate,

			--13. Fix the states of the by appling this formula ((PricePerUnit * Quantity) - TotalSpent = 0) if True=0 and False=0 else N/A
			CASE 
				WHEN (PricePerUnit * Quantity) - TotalSpent = 0 THEN 'False'
				WHEN (PricePerUnit * Quantity) - TotalSpent != 0 THEN 'True'
				ELSE 'N/A'
			END AS DiscountApplied,

			-- 14. RecordStatus
			CASE 
				WHEN Item IS NULL AND PricePerUnit IS NULL THEN 'Incomplete'
				ELSE 'Complete'
			END AS RecordStatus,

			-- 15. DataQualityStatus
			CASE
				WHEN Item IS NULL AND PricePerUnit IS NULL THEN 'Missing Item and Price'
				WHEN Quantity IS NULL AND TotalSpent IS NULL THEN 'Missing Quantity and Spend'
				WHEN TotalSpent IS NULL THEN 'Missing Spend'
				WHEN PricePerUnit IS NULL THEN 'Missing Price'
				WHEN Quantity IS NULL THEN 'Missing Quantity'
				WHEN DiscountApplied IS NULL THEN 'Missing Discount Info'
				ELSE 'Complete'
			END AS DataQualityStatus

		FROM bronze.retail_store_sales;
		 
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
    
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading curated data Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING CURATED DATA'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
		PRINT '=========================================='
	END CATCH
END


EXEC silver.load_silver

/*select * from silver.retail_store_sales;

SELECT COUNT(*) FROM silver.retail_store_sales;*/
