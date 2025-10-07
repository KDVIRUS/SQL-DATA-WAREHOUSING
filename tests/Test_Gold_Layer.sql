-- HERE WE TESTING QUALITY OF GOLD LAYER.

SELECT Customer_Number, COUNT(Customer_Number)
FROM ( 
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
		ON cu.cst_key = cl.CID
) t
GROUP BY Customer_Number
HAVING COUNT(Customer_Number) > 1;


SELECT Product_Number, COUNT(Product_Number)
FROM (
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
WHERE pi.prd_end_dt IS NULL
) t
GROUP BY Product_Number
HAVING COUNT(Product_Number) > 1;


SELECT Order_Number, COUNT(Order_Number)
FROM gold_fact_sales
GROUP BY Order_Number
HAVING COUNT(Order_Number) > 1;