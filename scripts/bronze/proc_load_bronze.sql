/* STORED PROCEDURE: load bronze layer(source - bronze)*/CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
--If the procedure does not exist, it creates it.
--If the procedure already exists, it alters/updates it.

BEGIN 
	DECLARE @start_time DATETIME,@end_time DATETIME ,@batch_start_time DATETIME,@batch_end_time DATETIME
	--DECLARE creates two variables: @start_time and @end_time.
	--Both are of type DATETIME, meaning they can store date and time values.
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '=============================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=============================================';

		SET @start_time = GETDATE()
		-- STORES THE CURRENT TIME 
		PRINT '---------------------------------------------';
		PRINT 'LOADING CRM TABLE';
		PRINT '---------------------------------------------';
		TRUNCATE TABLE bronze.crm_cust_info --quickly remove all rows from a table.

		BULK INSERT bronze.crm_cust_info
		--FILE LOCATION
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		WITH(
			FIRSTROW = 2, --FROM WHERE DOES THE DATA STARTS
			FIELDTERMINATOR = ',',  --DATA SEPRETOR
			TABLOCK --requests a table-level lock instead of row-level or page-level locks.
		);
		SET @end_time = GETDATE()
		PRINT 'LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'

		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT  bronze.crm_prd_info
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
			);

		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
			);

		PRINT '---------------------------------------------';
		PRINT 'LOADING ERP TABLE';
		PRINT '---------------------------------------------';

		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
			);

		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
			);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM "C:\Users\Abhay\OneDrive\Desktop\SQL dataset\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
			);
	SET @batch_end_time = GETDATE()
	PRINT 'LOAD DURATION WHOLE BATCH: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
	END TRY
	BEGIN CATCH 
		PRINT '########################################'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE '+ ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE '+ CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'ERROR MESSAGE '+ CAST(ERROR_MESSAGE()AS NVARCHAR);
		PRINT '########################################'
	END CATCH 
END
	
SELECT * FROM Bronze.crm_cust_info
SELECT COUNT(*) TABLE_COUNT FROM Bronze.crm_cust_info 

EXECUTE bronze.load_bronze
