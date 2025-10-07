-- IN THIS CODE WE ARE ADDING DATA IN ALL TABLE WHICH ARE CREATED UNDER BRONZE LAYER.
-- WE ARE USING APPROCH OF BULK INSERT.
DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time, end_time, batch_start_time, batch_end_time DATETIME;
    SET batch_start_time = NOW();

    -- 1. bronze_crm_cust_info
    SET start_time = NOW();
    SELECT "TRUNCATING TABLE: CRM_CUST_INFO";
    TRUNCATE TABLE datawarehouse.bronze_crm_cust_info;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_crm/cust_info.csv'
    INTO TABLE datawarehouse.bronze_crm_cust_info
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);
	SET end_time = NOW();
    SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);

    -- 2. bronze_crm_sales_details
    SET start_time = NOW();
    SELECT "TRUNCATING TABLE: CRM_SALES_DETAILS";
    TRUNCATE TABLE datawarehouse.bronze_crm_sales_details;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_crm/sales_details.csv'
    INTO TABLE datawarehouse.bronze_crm_sales_details
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);
	SET end_time = NOW();
	SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);

    -- 3. bronze_crm_prd_info
    SET start_time = NOW();
    SELECT "TRUNCATING TABLE: CRM_PRD_INFO";
    TRUNCATE TABLE datawarehouse.bronze_crm_prd_info;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_crm/prd_info.csv'
    INTO TABLE datawarehouse.bronze_crm_prd_info
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);
	SET end_time = NOW();
	SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);

    -- 4. bronze_erp_cust_az12
    SET start_time = NOW();
    SELECT "TRUNCATING TABLE: ERP_CUST_AZ12";
    TRUNCATE TABLE datawarehouse.bronze_erp_cust_az12;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE datawarehouse.bronze_erp_cust_az12
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (CID, BDATE, GEN);
	SET end_time = NOW();
	SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);

    -- 5. bronze_erp_loc_a101
	SET start_time = NOW();
    SELECT "TRUNCATING TABLE: ERP_LOC_A101";
    TRUNCATE TABLE datawarehouse.bronze_erp_loc_a101;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_erp/LOC_A101.csv'
    INTO TABLE datawarehouse.bronze_erp_loc_a101
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (CID, CNTRY);
    SET end_time = NOW();
	SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);

    -- 6. bronze_erp_px_cat_g1v2
    SET start_time = NOW();
    SELECT "TRUNCATING TABLE: ERP_PX_CAT_G1V2";
    TRUNCATE TABLE datawarehouse.bronze_erp_px_cat_g1v2;
    SELECT "ADDING DATA....";
    LOAD DATA LOCAL INFILE 'D:/PROJECTS/SQL + DATA WAREHOUSE/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE datawarehouse.bronze_erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (ID, CAT, SUBCAT, MAINTENANCE);
    SET end_time = NOW();
	SELECT "TASK COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, start_time, end_time) AS CHAR);
    SET batch_end_time = NOW();
	SELECT "TOTAL LOAD COMPLETION TIME: " + CAST(TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time) AS CHAR);

END$$

DELIMITER ;
