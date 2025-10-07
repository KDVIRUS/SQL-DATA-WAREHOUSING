-- THIS CODE IS TO CREATE STORED PROCEDURE THAT DO ETL OPERATION ON EVERY TABLE IN BRONZE LAYER AND LOADS DATA.
DELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN
    DECLARE start_time, end_time, batch_start_time, batch_end_time DATETIME;
    SET batch_start_time = NOW();

-- bronze_crm_cust_info
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_crm_cust_info';
    TRUNCATE TABLE silver_crm_cust_info;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) IN ('M', 'MARRIED') THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) IN ('S', 'SINGLE') THEN 'Single'
            ELSE 'N/A'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'N/A'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_test
        FROM bronze_crm_cust_info
        WHERE cst_id != 0 AND cst_id IS NOT NULL
    ) t
    WHERE t.flag_test = 1;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

-- bronze_crm_prd_info
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_crm_prd_info';
    TRUNCATE TABLE silver_crm_prd_info;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_crm_prd_info(
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        SUBSTRING(TRIM(prd_key), 1, 5),
        SUBSTRING(TRIM(prd_key), 7, CHAR_LENGTH(prd_key)),
        TRIM(prd_nm),
        prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END,
        prd_start_dt,
        DATE_SUB(
            LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        )
    FROM bronze_crm_prd_info;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

-- bronze_crm_sales_details
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_crm_sales_details';
    TRUNCATE TABLE silver_crm_sales_details;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN LENGTH(sls_order_dt) = 8 THEN STR_TO_DATE(sls_order_dt, '%Y%m%d') ELSE NULL END,
        CASE WHEN LENGTH(sls_ship_dt) = 8 THEN STR_TO_DATE(sls_ship_dt, '%Y%m%d') ELSE NULL END,
        CASE WHEN LENGTH(sls_due_dt) = 8 THEN STR_TO_DATE(sls_due_dt, '%Y%m%d') ELSE NULL END,
        sls_sales,
        sls_quantity,
        sls_price
    FROM bronze_crm_sales_details;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

-- bronze_erp_cust_az12
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_erp_cust_az12';
    TRUNCATE TABLE silver_erp_cust_az12;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_erp_cust_az12(
        CID, BDATE, GEN
    )
    SELECT 
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID)) ELSE CID END,
        BDATE,
        CASE
            WHEN UPPER(GEN) LIKE 'M%' THEN 'Male'
            WHEN UPPER(GEN) LIKE 'F%' THEN 'Female'
            ELSE 'N/A'
        END
    FROM bronze_erp_cust_az12;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

-- bronze_erp_loc_a101
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_erp_loc_a101';
    TRUNCATE TABLE silver_erp_loc_a101;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_erp_loc_a101(
        CID, CNTRY
    )
    SELECT REPLACE(TRIM(CID), '-', ''), CNTRY
    FROM bronze_erp_loc_a101;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

-- bronze_erp_px_cat_g1v2
    SET start_time = NOW();
    SELECT 'TRUNCATING TABLE: silver_erp_px_cat_g1v2';
    TRUNCATE TABLE silver_erp_px_cat_g1v2;
    SELECT 'ADDING DATA....';
    INSERT INTO silver_erp_px_cat_g1v2(
        ID, CAT, SUBCAT, MAINTENANCE
    )
    SELECT REPLACE(TRIM(ID), '_', '-'), CAT, SUBCAT, MAINTENANCE
    FROM bronze_erp_px_cat_g1v2;
    SET end_time = NOW();
    SELECT CONCAT('TASK COMPLETION TIME: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

    SET batch_end_time = NOW();
    SELECT CONCAT('TOTAL TIME TAKEN TO LOAD DATA: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds');
END$$

DELIMITER ;


CALL load_silver();