/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- CHECKES FOR NULLS OR DUPLICATES IN PRIMARY KEY 
-- EXECPTION : NO RESULT

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

/*
-ALL THE DUPLICATES AND NULL IN cst_id
	SELECT * FROM (
	SELECT 
		cst_id,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY	cst_create_date DESC) FL
	FROM bronze.crm_cust_info) T
	WHERE FL >1

-REMOVED NULLS AND DUPLICATES AND MOVE DATA INTO TESTTABLE
	SELECT * 
	INTO TESTTABLE
	FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY	cst_create_date DESC) RANKED
		FROM bronze.crm_cust_info) T
	WHERE RANKED = 1 AND cst_id IS NOT NULL

DROP TABLE TESTTABLE
SELECT * FROM TESTTABLE 

SELECT 
	cst_id,
	COUNT(*)
FROM 
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
*/

--REMOVED DUPLICATES AND NULLS 
SELECT * 
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY	cst_create_date DESC) RANKED
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) T
WHERE RANKED = 1  


-- CHECK FOR UNWANTED SPACES IN STRING VALUES 
-- EXECPTION : NO RESULT

SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
/*
--CHECKING COUNT OF BAD STRING VALUES 
SELECT 
	COUNT (CASE
			WHEN cst_firstname = TRIM(cst_firstname)
			THEN 1
			END) GOOD_STRING,
	COUNT (CASE
			WHEN cst_firstname != TRIM(cst_firstname)
			THEN 1
			END) BAD_STRING
FROM bronze.crm_cust_info

*/

--QUALITY CHECK 
	--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS

SELECT DISTINCT [cst_marital_status]
FROM bronze.crm_cust_info

SELECT DISTINCT * FROM(
SELECT 
	CASE WHEN UPPER(TRIM( [cst_marital_status] ))= 'S' THEN 'Single' 
		WHEN UPPER(TRIM([cst_marital_status])) = 'M' THEN 'Married' 
		END [cst_marital_status]
FROM bronze.crm_cust_info)T
SELECT * FROM Silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
SELECT [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [Bronze].[crm_prd_info]

-- CHECKES FOR NULLS OR DUPLICATES IN PRIMARY KEY 
    SELECT 
	    [prd_id],
	    COUNT(*)
    FROM bronze.[crm_prd_info]
    GROUP BY [prd_id]
    HAVING COUNT(*) > 1 OR [prd_id] IS NULL;

--SUBSTRING
--EXTRACTS A SPECIFIC PART OF STRING VALUE 
    SELECT
    SUBSTRING([prd_key],1,5)
    FROM bronze.[crm_prd_info]

    SELECT 
        [prd_key],
        REPLACE(SUBSTRING([prd_key],1,5),'-','_')
    FROM bronze.[crm_prd_info]
    WHERE REPLACE(SUBSTRING([prd_key],1,5),'-','_') NOT IN
    (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)
    /*
    -- REPLACED - TO _ 
    -- CHECKED KEY WHICH IS NOT PRESENT INT bronze.erp_px_cat_g1v2
    */

-- CHECK FOR UNWANTED SPACES IN STRING VALUES 
    SELECT 
	    [prd_nm]
    FROM [Bronze].[crm_prd_info]
    WHERE [prd_nm] != TRIM([prd_nm])

-- CHECK FOR NULLS & NEGATIVE 
    SELECT 
	    [prd_cost]
    FROM [Bronze].[crm_prd_info]
    WHERE [prd_cost] < 0 OR [prd_cost] IS NULL

--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT [prd_line]
FROM [Bronze].[crm_prd_info]
SELECT 
    [prd_line],
    CASE WHEN UPPER(TRIM([prd_line])) ='M' THEN 'Mountain'
         WHEN UPPER(TRIM([prd_line])) ='R' THEN 'Road'
         WHEN UPPER(TRIM([prd_line])) ='S' THEN 'Other Sales'
         WHEN UPPER(TRIM([prd_line])) ='T' THEN 'Touring'
         ELSE 'N/A'
    END AS [prd_line]
FROM [Bronze].[crm_prd_info]

--CHECK FOR INVALID DATE ORDERS
   -- END DATE MUST NOT BE EARLIER THAN THE START DATE 
SELECT * 
FROM [Bronze].[crm_prd_info]
WHERE prd_end_dt < prd_start_dt 
 
SELECT 
    prd_start_dt,
    CAST([prd_end_dt]AS DATE),
    CAST(DATEADD(DAY,-1,LEAD([prd_start_dt]) OVER(PARTITION BY [prd_key] ORDER BY [prd_start_dt])) AS DATE)
FROM [Bronze].[crm_prd_info]

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

/*
--BUSINESS RULE 
  Sales = Quantity * Price 
  Nagetive ,Zero ,Null  are not allowed 
*/

-- CHECK FOR INVALID prd_key & cust_id
    SELECT
           [sls_prd_key]
          ,[sls_cust_id]
    FROM Bronze.crm_sales_details
    WHERE [sls_prd_key] NOT IN (SELECT [sls_prd_key] FROM silver.crm_prd_info)

    SELECT
           [sls_prd_key]
          ,[sls_cust_id]
    FROM Bronze.crm_sales_details
    WHERE [sls_cust_id] NOT IN (SELECT [sls_cust_id] FROM silver.crm_prd_info)

--CHECK FOR INVALID DATE 
SELECT
    [sls_order_dt],
    NULLIF([sls_order_dt],0)     -- making all the zero null 
FROM [Bronze].[crm_sales_details]
WHERE [sls_order_dt] <= 0     -- checking is there any 0 or negetive number 
      OR LEN([sls_order_dt]) != 8     --date should be in eight number
      OR [sls_order_dt] > 20500101    --setting boundreis: not bigger then that date 
      OR [sls_order_dt] < 19000101    --setting boundreis: not smaller than that date 


-- CHECKING FOR INVALID DATE ORDERS 
SELECT * FROM [Bronze].[crm_sales_details]
WHERE [sls_order_dt]>[sls_ship_dt] OR [sls_ship_dt] > [sls_due_dt]

--CHECKING DATA CONSISTENCY: BETWEEN Sales , Quantity, Price 
  -- sales = quantity *  price 

SELECT 
       [sls_sales] OLDS
      ,[sls_quantity] OLDQ
      ,[sls_price] OLDP
      ,CASE WHEN [sls_sales] IS NULL OR [sls_sales] <= 0 OR [sls_sales] != [sls_quantity] * ABS([sls_price])
                THEN [sls_quantity] * ABS([sls_price])    -- returns the absolute (non-negative) value
            ELSE [sls_sales]
        END [sls_sales]
      ,CASE WHEN [sls_price] IS NULL OR [sls_price] <= 0
                THEN [sls_sales] / NULLIF([sls_quantity],0)
            ELSE [sls_price]
        END [sls_price]
FROM [Bronze].[crm_sales_details]
WHERE [sls_sales] != [sls_quantity] * [sls_price]
    OR [sls_sales] IS NULL OR [sls_quantity] IS NULL OR [sls_price] IS NULL
    OR [sls_sales] <= 0 OR [sls_quantity] <= 0 OR [sls_price] <= 0 
ORDER BY [sls_sales],[sls_quantity],[sls_price]


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

SELECT * FROM Bronze.erp_cust_az12

-- TRANSFORMING  cid
SELECT *,
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE cid
	END AS cid
FROM Bronze.erp_cust_az12
      
-- CHECKING IS THAT KEYS PRESENT IN THE OTHER TABLE 
SELECT *,
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE cid
	END AS cid
FROM Bronze.erp_cust_az12
WHERE CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM Silver.crm_cust_info)

--IDNTIFY OUT OF RANGE BDATES
SELECT DISTINCT 
bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--DATA STANDARDIZATION AND CONSISTENCY 
SELECT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'N/A'
END AS gen
FROM  Bronze.erp_cust_az12

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

SELECT TOP(5) * FROM Silver.crm_cust_info
SELECT TOP(5) * FROM Bronze.erp_loc_a101

--TRANSFORMING CID
SELECT 
REPLACE(cid, '-','') cid
FROM Bronze.erp_loc_a101

-- CHECKING FOR CID PRESENT IN CUST_INFO KEY
SELECT 
REPLACE(cid, '-','') cid
FROM Bronze.erp_loc_a101
WHERE REPLACE(cid, '-','') NOT IN (SELECT cst_key FROM Silver.crm_cust_info)

--DATA STANDERDIZATION AND CONSISTENCY
SELECT DISTINCT cntry 
FROM Bronze.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT cntry,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END
FROM Bronze.erp_loc_a101
ORDER BY cntry

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

SELECT  [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [Bronze].[erp_px_cat_g1v2]
WHERE [id] NOT IN (SELECT cat_id FROM Silver.crm_prd_info)

--CHECKING FOR UNWANTED SPECES 
SELECT * FROM [Bronze].[erp_px_cat_g1v2]
WHERE TRIM(cat) != cat OR subcat != TRIM(subcat) OR [maintenance] != TRIM([maintenance])

--DATA STANDERDIZATION AND CONSISTENCY 
SELECT DISTINCT [subcat]
FROM [Bronze].[erp_px_cat_g1v2]
