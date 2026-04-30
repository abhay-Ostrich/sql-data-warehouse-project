/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW  gold.dim_customers AS 
-- A view is a saved query that you can use like a table.
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
	CS.cst_id customer_id,
	CS.cst_key customer_number,
	CS.cst_firstname first_name,
	CS.cst_lastname last_name,
	LO.cntry country,
	CS.cst_marital_status marital_status,
	CASE WHEN CS.cst_gndr != 'N/A' THEN CS.cst_gndr --CRM IS MASTAER FOR GENDER INFO
		 ELSE COALESCE(CU.gen, 'N/A')
	END AS gender,
	CU.bdate birthdate,
	CS.cst_create_date create_date
FROM Silver.crm_cust_info CS
LEFT JOIN Silver.erp_cust_az12 CU
ON CS.cst_key = CU.cid
LEFT JOIN Silver.erp_loc_a101 LO
ON CU.cid = LO.cid
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt,prd_key) product_key,
	CP.prd_id product_id,
	CP.prd_key product_number,
	CP.prd_nm product_name,
	CP.cat_id category_id,
	EP.cat category,
	EP.subcat subcategory,
	EP.maintenance,
	CP.prd_cost cost,
	CP.prd_line product_line,
	CP.prd_start_dt start_date
FROM Silver.crm_prd_info CP
LEFT JOIN Silver.erp_px_cat_g1v2 EP
ON CP.cat_id = EP.id
WHERE prd_end_dt IS NULL  --IF END DATE IS NULL THEN IT IS CURRENT INFO OF PRODUCT 
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
        SD.[sls_ord_num] order_number,
        DP.[product_key],
        DC.customer_key
      ,SD.[sls_order_dt] order_date
      ,SD.[sls_ship_dt] shipping_date
      ,SD.[sls_due_dt] due_date
      ,SD.[sls_sales] sales_amount
      ,SD.[sls_quantity]
      ,SD.[sls_price]
  FROM [Silver].[crm_sales_details] SD
  LEFT JOIN Gold.dim_products DP
  ON SD.[sls_prd_key] = DP.[product_number]
  LEFT JOIN Gold.dim_customers DC
  ON SD.[sls_cust_id] = DC.customer_id
GO
