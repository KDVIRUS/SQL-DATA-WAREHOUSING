-- HERE WE ARE WRITING TO CREATE FINAL LAYER OF OUR PROJECT WHICH IS NOTHING BUT GOLD LAYER.
-- WE ARE CREATING FACT AND DIMENSION TABLE AND THEN CREATING VIEW FOR THOSE TABLE.

-- CREATING CUSTOMER DIMENSION TABLE VIEW
SELECT * FROM silver_crm_cust_info;

SELECT * FROM silver_erp_cust_az12;

SELECT * FROM silver_erp_loc_a101;

-- JOINING ABOVE 3 TABLES, ADDING SURROGATE KEY AND CREATING CUSTOMER VIEW.
CREATE OR REPLACE VIEW gold_dim_customer AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cu.cst_create_date) AS Customer_Number, 
    cu.cst_id AS Customer_ID,
    cu.cst_key AS Customer_Key,
    cu.cst_firstname AS First_Name,
    cu.cst_lastname AS Last_Name, 
    cu.cst_marital_status AS Marital_Status,
    CASE
        WHEN cu.cst_gndr != 'N/A' THEN cu.cst_gndr
        WHEN cd.GEN = 'N/A' THEN 'N/A'
        ELSE cd.GEN
    END AS Gender,
    cu.cst_create_date AS Create_Date,
    cd.BDATE AS Birth_Date,
    cl.CNTRY AS Country
FROM silver_crm_cust_info cu
LEFT JOIN silver_erp_cust_az12 cd
    ON cu.cst_key = cd.CID
LEFT JOIN silver_erp_loc_a101 cl
    ON cu.cst_key = cl.CID;

SELECT * FROM gold_dim_customer;



-- CREATING PRODUCT DIMENSION TABLE VIEW.
SELECT * FROM silver_crm_prd_info;

SELECT * FROM silver_erp_px_cat_g1v2;

-- JOINING ABOVE 2 TABLES, ADDING SURROGATE KEY AND CREATING PRODUCT VIEW.
CREATE OR REPLACE VIEW gold_dim_product AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt) AS Product_Number,
	pi.prd_id AS Product_ID,
    pi.cat_id AS Category_ID,
    pi.prd_key AS Product_Key,
    pi.prd_nm AS Product_Name,
    pc.CAT AS Category,
    pc.SUBCAT AS Subcategory,
    pi.prd_line AS Product_Line,
    pi.prd_cost AS Cost,
    pc.MAINTENANCE,
    pi.prd_start_dt AS Start_Date
FROM silver_crm_prd_info pi
LEFT JOIN silver_erp_px_cat_g1v2 pc
ON pi.cat_id = pc.ID
WHERE pi.prd_end_dt IS NULL;

select * from gold_dim_product;

-- NOW CREATING FACT TABLE FOR SALES.
SELECT * FROM silver_crm_sales_details;

-- JOINING FACT TABLE WITH DIMENSION TABLES TO GET SURROGATE KEY AND CREATING VIEW.
CREATE OR REPLACE VIEW gold_fact_sales AS
SELECT 
	sls_ord_num AS Order_Number,
    prd.Product_Number,
    cu.Customer_Number,
    sls_order_dt AS Order_Date,
    sls_ship_dt AS Shipping_Date,
    sls_due_dt AS Due_Date,
    sls_sales AS Total_Sale,
    sls_quantity AS Total_Quantity,
    sls_price AS Price
FROM silver_crm_sales_details sl
LEFT JOIN gold_dim_customer cu
ON sl.sls_cust_id = cu.Customer_ID
LEFT JOIN gold_dim_product prd
ON sl.sls_prd_key = prd.Product_Key;

SELECT * FROM gold_fact_sales;