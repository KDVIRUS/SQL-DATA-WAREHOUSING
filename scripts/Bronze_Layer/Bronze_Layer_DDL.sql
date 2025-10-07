-- HERE WE ARE WRITING TO CREATE BRONZE LAYER.
-- STEPS: 
-- 			1.CREATING TABLES.
--             2.ADDING DATA IN THOSE TABLE WITH THE APPROACH OF BULK INSERT.
--             3.ADDING CODE IN TRY CATCH BLOCK.
--             4.CREATING STORED PROCEDURE FOR FINAL CODE.

USE datawarehouse;

-- NOW WE ARE CHECKING THE TABLE IS ALREADY PRESENT IN DATABASE-bronze OR NOT AND IF IT IS NOT PRESENT THEN WE ARE CREATING IT.

-- 1.crm_cust_info
DROP TABLE IF EXISTS bronze_crm_cust_info;
CREATE TABLE bronze_crm_cust_info (
	cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(10),
    cst_lastname VARCHAR(20),
    cst_marital_status VARCHAR(10),
    cst_gndr VARCHAR(10),
    cst_create_date DATE
);

-- 2.crm_prd_info
DROP TABLE IF EXISTS bronze_crm_prd_info;
CREATE TABLE bronze_crm_prd_info (
	prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost FLOAT,
    prd_line VARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- 3.crm_sales_details
DROP TABLE IF EXISTS bronze_crm_sales_details;
CREATE TABLE bronze_crm_sales_details (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price FLOAT
);

-- 4.erp_cust_az12
DROP TABLE IF EXISTS bronze_erp_cust_az12;
CREATE TABLE bronze_erp_cust_az12 (	
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(10)
);

-- 5.erp_loc_a101
DROP TABLE IF EXISTS bronze_erp_loc_a101;
CREATE TABLE bronze_erp_loc_a101 (
	CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

-- 6.erp_px_cat_g1v2
DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2;
CREATE TABLE bronze_erp_px_cat_g1v2 (	
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);