--checking 'gold_dim_customers'

--CHECKING ANY DUPLICATES IN PRIMARY KEY 
SELECT cst_id, COUNT(*) FROM (
SELECT 
	CS.cst_id,
	CS.cst_key,
	CS.cst_firstname,
	CS.cst_lastname,
	CS.cst_marital_status,
	CS.cst_gndr,
	CS.cst_create_date,
	CU.bdate,
	CU.gen,
	LO.cntry
FROM Silver.crm_cust_info CS
LEFT JOIN Silver.erp_cust_az12 CU
ON CS.cst_key = CU.cid
LEFT JOIN Silver.erp_loc_a101 LO
ON CU.cid = LO.cid)T
GROUP BY cst_id
HAVING COUNT(*) > 1

--DATA INTEGRATION 
SELECT DISTINCT 
	CS.cst_gndr,
	CU.gen,
	CASE WHEN CS.cst_gndr != 'N/A' THEN CS.cst_gndr --CRM IS MASTAER FOR GENDER INFO
		 ELSE COALESCE(CU.gen, 'N/A')
	END AS new_gen
FROM Silver.crm_cust_info CS
LEFT JOIN Silver.erp_cust_az12 CU
ON CS.cst_key = CU.cid
LEFT JOIN Silver.erp_loc_a101 LO
ON CU.cid = LO.cid
ORDER BY 1,2 --sorts the result first by the first selected column (CS.cst_gndr) 
                --and then by the second column (CU.gen) in ascending order


  
  --checking 'gold_dim_products'

--CHECKING DUPLICATES IN PRD KEY
SELECT prd_key, COUNT(*) FROM (
SELECT 
	CP.prd_id,
	CP.cat_id,
	CP.prd_key,
	CP.prd_nm,
	CP.prd_cost,
	CP.prd_line,
	CP.prd_start_dt,
	EP.cat,
	EP.subcat,
	EP.maintenance
FROM Silver.crm_prd_info CP
LEFT JOIN Silver.erp_px_cat_g1v2 EP
ON CP.cat_id = EP.id
WHERE prd_end_dt IS NULL)T
GROUP BY prd_key
HAVING COUNT(*) > 1

-- SORT THE COLUMNS INTO LOGICAL GROUPS TO IMPROVE READABILITY



--checking 'gold_dim_sales'
