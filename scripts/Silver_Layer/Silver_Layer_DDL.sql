-- HERE WE ARE WRITING SQL QUERIES TO CREATE SILVER LAYER.
-- STEPS: 
-- 			1.CREATING TABLES.
--          2.ADDING DATA IN THOSE TABLE WITH THE APPROACH OF BULK INSERT FROM silver LAYER.
--          3.CREATING STORED PROCEDURE FOR FINAL CODE.

USE datawarehouse;

-- NOW WE ARE CHECKING THE TABLE IS ALREADY PRESENT IN DATABASE-silver OR NOT AND IF IT IS NOT PRESENT THEN WE ARE CREATING IT.

-- 1.crm_cust_info
DROP TABLE IF EXISTS silver_crm_cust_info;
CREATE TABLE silver_crm_cust_info (
	cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(10),
    cst_lastname VARCHAR(20),
    cst_marital_status VARCHAR(10),
    cst_gndr VARCHAR(10),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2.crm_prd_info
DROP TABLE IF EXISTS silver_crm_prd_info;
CREATE TABLE silver_crm_prd_info (
	prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost FLOAT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
	dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 3.crm_sales_details
DROP TABLE IF EXISTS silver_crm_sales_details;
CREATE TABLE silver_crm_sales_details (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 4.erp_cust_az12
DROP TABLE IF EXISTS silver_erp_cust_az12;
CREATE TABLE silver_erp_cust_az12 (	
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(10),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 5.erp_loc_a101
DROP TABLE IF EXISTS silver_erp_loc_a101;
CREATE TABLE silver_erp_loc_a101 (
	CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 6.erp_px_cat_g1v2
DROP TABLE IF EXISTS silver_erp_px_cat_g1v2;
CREATE TABLE silver_erp_px_cat_g1v2 (	
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);