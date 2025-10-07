USE datawarehouse;

-- silver_crm_cust_info
SELECT * FROM silver_crm_cust_info;

-- CHECKING THAT PRIMARY KEY IS HAVING NULL AND DUPLICATE VALUES.
SELECT cst_id, COUNT(cst_id)
FROM silver_crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1;

-- CHECKING cst_key IS HAVING ANY NULL, DUPLICATES AND EXTRA SPACES. 
SELECT cst_key
FROM silver_crm_cust_info
WHERE cst_key != TRIM(cst_key);


SELECT cst_key, COUNT(cst_key)
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS "flag_test"
	FROM silver_crm_cust_info
	WHERE cst_id != 0 AND cst_id IS NOT NULL
	) t
WHERE t.flag_test = 1
GROUP BY cst_key
HAVING COUNT(cst_key) > 1;

SELECT cst_key
FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS "flag_test"
	FROM silver_crm_cust_info
	WHERE cst_id != 0 AND cst_id IS NOT NULL
	) t
WHERE t.flag_test = 1 AND cst_key IS NULL;

-- CHECKING THAT cst_firstname, cst_lastname HAS EXTRA SPACES.
SELECT cst_firstname
FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver_crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- STANDARDISING AND NORMALISING cst_marital_status COLUMN.
SELECT cst_marital_status
FROM silver_crm_cust_info
WHERE cst_marital_status NOT IN ("M", "S");

SELECT DISTINCT cst_marital_status
FROM silver_crm_cust_info;

-- STANDARDISING AND NORMALISING cst_marital_status AND cst_gndr COLUMN.
SELECT DISTINCT cst_gndr
FROM silver_crm_cust_info;

-- NOW CHECKING cst_create_date FOR NULL AND BLANK VALUES OR IRREVLANT DATE
SELECT *
FROM silver_crm_cust_info
WHERE cst_create_date IS NULL;

-- silver_crm_prd_info
SELECT * 
FROM silver_crm_prd_info;

-- NOW CHECKING prd_id FOR NULL, DUPLICATE VALUES
SELECT *
FROM silver_crm_prd_info
WHERE prd_id IS NULL OR prd_id = 0;

SELECT prd_id, COUNT(prd_id)
FROM silver_crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1;

-- NOW CHECKING prd_cost HAVING NULL OR NEGATIVE VALUE.
SELECT * FROM silver_crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- CHECKING prd_line FOR DISTINCT VALUE.
SELECT prd_line
FROM silver_crm_prd_info
WHERE prd_line  = "";

-- NOW CHECKING prd_start_dt AND prd_end_dt COLUMN
SELECT * FROM silver_crm_prd_info
ORDER BY prd_key, Prd_start_dt;

-- silver_crm_sales_details
SELECT * FROM silver_crm_sales_details;

-- CHECKING SLS_ORD_NUM COLUMN WITH NULL, DUPLICATE
SELECT sls_ord_num
FROM silver_crm_sales_details
WHERE sls_ord_num IS NULL OR sls_ord_num = "";

SELECT sls_ord_num, COUNT(sls_ord_num)
FROM silver_crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(sls_ord_num) > 1;

SELECT sls_ord_num
FROM silver_crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- CHECKING sls_prd_key
SELECT sls_prd_key
FROM silver_crm_sales_details
WHERE sls_prd_key IS NULL OR sls_prd_key = "";

SELECT sls_prd_key
FROM silver_crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);

-- CHECKING SLS_ORDER_DT
SELECT sls_order_dt
FROM silver_crm_sales_details
WHERE sls_order_dt IS NULL OR sls_order_dt = "" OR sls_order_dt = 0;

-- NOW CHECKING SLS_SALES, SLS_QUANTITY, SLS_PRICE COLUMNS
SELECT *
FROM silver_crm_sales_details
WHERE sls_sales < 0 OR sls_sales IS NULL OR sls_sales = '' OR sls_sales != sls_price * sls_quantity;

-- silver_erp_cust_az12
SELECT * FROM silver_erp_cust_az12;

-- silver_erp_cust_az12
SELECT * FROM silver_erp_cust_az12;
-- CHECKING THAT CID HAS ANY NULL DUPLICATE OR EXTRA SPACE
SELECT CID 
FROM silver_erp_cust_az12
WHERE CID LIKE 'NAS%';

SELECT * 
FROM silver_erp_cust_az12
WHERE CID != TRIM(CID) OR BDATE != TRIM(BDATE) OR GEN != TRIM(GEN);

-- CHECKING DISTINCT VALUES IN GEN
SELECT DISTINCT GEN
FROM silver_erp_cust_az12;

SELECT T.CID, COUNT(T.CID)
FROM (
SELECT
	SUBSTRING(CID, 4, LENGTH(CID)) AS CID
FROM silver_erp_cust_az12
WHERE CID LIKE 'NAS%'
UNION ALL
SELECT CID
FROM silver_erp_cust_az12
WHERE CID NOT LIKE 'NAS%'
) T
GROUP BY T.CID
HAVING COUNT(T.CID) > 1;

-- silver_erp_loc_a101
-- REPLACING '-' IN CID WITH BLANK
SELECT * FROM silver_erp_loc_a101;

-- CHECKING CNTRY FOR DISTINCT VALUE
SELECT DISTINCT
  CNTRY,
CASE
	WHEN CNTRY = '' OR CNTRY IS NULL THEN 'N/A'
    ELSE CNTRY
END AS 'HAPPY'
FROM silver_erp_loc_a101;

-- silver_erp_px_cat_g1v2
SELECT * FROM silver_erp_px_cat_g1v2;

-- REPLACING '_' WITH '-' BCOZ IN PRODUCT INFORMATION TABLE WE HAVE THAT TYPE OF CAT ID
SELECT DISTINCT MAINTENANCE FROM silver_erp_px_cat_g1v2;
