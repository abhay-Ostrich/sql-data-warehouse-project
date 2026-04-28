
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
    PRINT('>>>>> TUNICATING TABLE: Silver.crm_cust_info <<<<<')
    TRUNCATE TABLE Silver.crm_cust_info
    PRINT('>>>>> INSERTING DATA: Silver.crm_cust_info <<<<<')
    INSERT INTO Silver.crm_cust_info (
	       [cst_id]
          ,[cst_key]
          ,[cst_firstname]
          ,[cst_lastname]
          ,[cst_marital_status]
          ,[cst_gndr]
          ,[cst_create_date])

    SELECT 
	       [cst_id]
          ,[cst_key]
          ,TRIM([cst_firstname]) cst_firstname
          ,TRIM([cst_lastname]) cst_lastname
          ,CASE WHEN UPPER(TRIM( [cst_marital_status] ))= 'S' THEN 'Single' 
			    WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Married' 
			    ELSE 'Unknown'
			    END [cst_marital_status]
          ,CASE WHEN UPPER(TRIM( [cst_gndr] ))= 'M' THEN 'Male' 
			    WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female' 
			    ELSE 'Unknown'
			    END [cst_gndr]
          ,[cst_create_date]
    FROM (
	    SELECT 
		    *,
		    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY	cst_create_date DESC) RANKED
	    FROM bronze.crm_cust_info
	    WHERE cst_id IS NOT NULL) T
    WHERE RANKED = 1 ;

    PRINT('>>>>> TUNICATING TABLE: [Silver].[crm_prd_info]<<<<<')
    TRUNCATE TABLE [Silver].[crm_prd_info]
    PRINT('>>>>> INSERTING DATA: [Silver].[crm_prd_info] <<<<<')
    INSERT INTO [Silver].[crm_prd_info](
           [prd_id]
          ,[cat_id]
          ,[prd_key]
          ,[prd_nm]
          ,[prd_cost]
          ,[prd_line]
          ,[prd_start_dt]
          ,[prd_end_dt]
          ,[dwh_create_date])

    SELECT [prd_id]
          ,REPLACE(SUBSTRING([prd_key],1,5),'-','_') cat_id   --extract category ID
          ,SUBSTRING (prd_key,7,LEN(prd_key)) AS [prd_key]    --extract product key
          ,[prd_nm]
          ,ISNULL([prd_cost],0) [prd_cost]
          , CASE UPPER(TRIM([prd_line]))
                  WHEN 'M' THEN 'Mountain'
                  WHEN 'R' THEN 'Road'
                  WHEN 'S' THEN 'Other Sales'
                  WHEN 'T' THEN 'Touring'
                 ELSE 'N/A'
            END AS [prd_line]        -- map product key value to descriptive values
          ,CAST([prd_start_dt]AS DATE) AS [prd_start_dt],
           CAST(DATEADD(DAY,-1,LEAD([prd_start_dt]) OVER(PARTITION BY [prd_key] ORDER BY [prd_start_dt])) AS DATE)[prd_end_dt]
          ,GETDATE() AS [dwh_create_date]       -- calculate end date as one day before the next start date 
      FROM [Bronze].[crm_prd_info];

    PRINT('>>>>> TUNICATING TABLE: silver.crm_sales_details <<<<<')
    TRUNCATE TABLE silver.crm_sales_details
    PRINT('>>>>> INSERTING DATA: silver.crm_sales_details <<<<<')
    INSERT INTO silver.crm_sales_details ([sls_ord_num]
          ,[sls_prd_key]
          ,[sls_cust_id]
          ,[sls_order_dt]
          ,[sls_ship_dt]
          ,[sls_due_dt]
          ,[sls_sales]
          ,[sls_quantity]
          ,[sls_price])

    SELECT [sls_ord_num]
          ,[sls_prd_key]
          ,[sls_cust_id]
          ,CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) != 8 THEN NULL
                ELSE CAST(CAST([sls_order_dt] AS varchar) AS DATE)     --   can not cast int to date directly 
            END AS [sls_order_dt]
          ,CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) != 8 THEN NULL
                ELSE CAST(CAST([sls_ship_dt] AS varchar) AS DATE)
            END AS [sls_ship_dt]
          ,CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) != 8 THEN NULL
                ELSE CAST(CAST([sls_due_dt] AS varchar) AS DATE)
            END AS [sls_due_dt]
          ,CASE WHEN [sls_sales] IS NULL OR [sls_sales] <= 0 OR [sls_sales] != [sls_quantity] * ABS([sls_price])
                    THEN [sls_quantity] * ABS([sls_price])    -- ABS: returns the absolute (non-negative) value
                ELSE [sls_sales]
            END [sls_sales]
          ,[sls_quantity]
          ,CASE WHEN [sls_price] IS NULL OR [sls_price] <= 0
                    THEN [sls_sales] / NULLIF([sls_quantity],0)
                ELSE [sls_price]
            END [sls_price]
      FROM [Bronze].[crm_sales_details];

    PRINT('>>>>> TUNICATING TABLE: Silver.erp_cust_az12 <<<<<')
    TRUNCATE TABLE Silver.erp_cust_az12
    PRINT('>>>>> INSERTING DATA: Silver.erp_cust_az12 <<<<<')
    INSERT INTO Silver.erp_cust_az12(cid,bdate,gen)
	    SELECT 
	    CASE    -- REMOVE 'NAS' PREFIX IF PRESENT
		    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		    ELSE cid
	    END AS cid,
	    CASE   -- SET VERY OLD AND FUTURE BDATE TO NULL
		    WHEN bdate < '1924-01-01' OR bdate > GETDATE() THEN NULL
		    ELSE bdate
	    END AS bdate,
	    CASE   -- NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
		    WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		    ELSE 'N/A'
	    END AS gen
    FROM Bronze.erp_cust_az12;

    PRINT('>>>>> TUNICATING TABLE: Silver.erp_loc_a101 <<<<<')
    TRUNCATE TABLE Silver.erp_loc_a101
    PRINT('>>>>> INSERTING DATA: Silver.erp_loc_a101 <<<<<')
    INSERT INTO Silver.erp_loc_a101
    (cid,cntry)
    SELECT 
    REPLACE(cid, '-','') cid,
    CASE 
	    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	    ELSE TRIM(cntry)
    END AS cntry      --NORMALIZE AND HANDLE MISSING OR BLANK COUNTRY CODES  
    FROM Bronze.erp_loc_a101;


    PRINT('>>>>> TUNICATING TABLE: Silver.erp_px_cat_g1v2 <<<<<')
    TRUNCATE TABLE Silver.erp_px_cat_g1v2  -- QUICKLY DELETE THE TABLE 
    PRINT('>>>>> INSERTING DATA: Silver.erp_px_cat_g1v2 <<<<<')
    INSERT INTO Silver.erp_px_cat_g1v2
    ([id],[cat],[subcat],[maintenance])
    SELECT  [id]
          ,[cat]
          ,[subcat]
          ,[maintenance]
      FROM [Bronze].[erp_px_cat_g1v2];
END
 
