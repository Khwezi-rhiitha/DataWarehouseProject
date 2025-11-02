/*
=============================================================
Load Data - Using Stored Procedure
=============================================================
Script Purpose:
    -This stored procedure loads data into the 'bronze' schema from external CSV files. 
    -Truncates the table before loading data. It removes everything in the table . 
    -Loads new entries using Bulk Insert command.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading bronze';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.retail_store_sales';
		TRUNCATE TABLE bronze.retail_store_sales;
		PRINT '>> Inserting Data Into: bronze.retail_store_sales';
		--Bulk INSERT
		BULK INSERT bronze.retail_store_sales
		FROM 'C:\Users\nomak\OneDrive\CERTIFICATES\MicrosoftSQLServer\DataWarehouseProject\retail_store_sales.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
 
		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading raw data Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

EXEC bronze.load_bronze

select * from bronze.retail_store_sales


