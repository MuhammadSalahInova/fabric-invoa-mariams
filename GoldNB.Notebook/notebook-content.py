# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2d18090-7295-438d-905e-205a42447bdc",
# META       "default_lakehouse_name": "BronzeSalesLH",
# META       "default_lakehouse_workspace_id": "03a51e0a-fbda-4ed9-a1a5-f5259819c960",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2d18090-7295-438d-905e-205a42447bdc"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BronzeSalesLH.gold_fact_order_sales ")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. DimCustomer

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_customer
USING DELTA
AS

SELECT
    CAST(crc32(concat_ws('||',COALESCE(CAST(CustomerId AS STRING), ''),COALESCE(_scd_start_date, ''),COALESCE(_scd_hash, ''))) AS BIGINT) AS CustomerKey,
    CustomerId,
    CompanyName,
    ContactName,
    ContactTitle,
    City,
    Country,
    CreatedDate,
    ModifiedDate,
    CAST(_scd_start_date AS TIMESTAMP) AS ValidFrom,
    CAST(COALESCE(_scd_end_date,'9999-12-31 23:59:59') AS TIMESTAMP) AS ValidTo,
    _scd_is_current AS IsCurrent
FROM silver_customers
WHERE _scd_is_current = true;

""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_product
USING DELTA
AS

SELECT
    CAST(crc32(concat_ws('||',CAST(p.ProductId AS STRING),COALESCE(p._scd_start_date, ''),COALESCE(p._scd_hash, ''))) AS BIGINT) AS ProductKey,
    p.ProductId,
    p.ProductName,
    p.SupplierId,
    p.CategoryId,
    c.CategoryName,
    p.QuantityPerUnit,
    p.UnitPrice,
    p.UnitsInStock,
    p.UnitsOnOrder,
    p.ReorderLevel,
    p.Discontinued,
    p.CreatedDate,
    p.QuantityPerUnitCount,
    p.QuantityPerUnitUnit,
    p.ModifiedDate,
    CAST(p._scd_start_date AS TIMESTAMP) AS ValidFrom,
    COALESCE(CAST(p._scd_end_date AS TIMESTAMP),CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS ValidTo,
    p._scd_is_current AS IsCurrent

FROM silver_products p

LEFT JOIN silver_categories c
    ON p.CategoryId = c.CategoryId
    AND CAST(p._scd_start_date AS TIMESTAMP)>= CAST(c._scd_start_date AS TIMESTAMP)
    AND CAST(p._scd_start_date AS TIMESTAMP)<COALESCE(CAST(c._scd_end_date AS TIMESTAMP),CAST('9999-12-31 23:59:59' AS TIMESTAMP))

WHERE p._scd_is_current = true ;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_supplier
USING DELTA
AS
SELECT
    CAST(crc32(concat_ws('||',CAST(SupplierId AS STRING),COALESCE(_scd_start_date, ''),COALESCE(_scd_hash, ''))) AS BIGINT) AS SupplierKey,
    SupplierId,
    CompanyName,
    ContactName,
    ContactTitle,
    City,
    Country,
    CreatedDate,
    ModifiedDate,
    CAST(_scd_start_date AS TIMESTAMP) AS ValidFrom,
    COALESCE(CAST(_scd_end_date AS TIMESTAMP),CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS ValidTo,
    _scd_is_current AS IsCurrent

FROM silver_suppliers

WHERE _scd_is_current = true;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_shipper
USING DELTA
AS
SELECT
    CAST(crc32(concat_ws('||',CAST(ShipperId AS STRING),COALESCE(_scd_start_date, ''),COALESCE(_scd_hash, ''))) AS BIGINT) AS ShipperKey,
    ShipperId,
    CompanyName,
    CreatedDate,
    ModifiedDate,
    CAST(_scd_start_date AS TIMESTAMP) AS ValidFrom,
    COALESCE(CAST(_scd_end_date AS TIMESTAMP),CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS ValidTo,
    _scd_is_current AS IsCurrent

FROM silver_shippers

WHERE _scd_is_current = true;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_employee
USING DELTA
AS

SELECT
    CAST(crc32(concat_ws('||',CAST(EmployeeId AS STRING),COALESCE(_scd_start_date, ''),COALESCE(_scd_hash, ''))) AS BIGINT) AS EmployeeKey,
    EmployeeId,
    Title,
    TitleOfCourtesy,
    BirthDate,
    HireDate,
    City,
    Country,
    ReportsTo,
    CreatedDate,
    FullName,
    ModifiedDate,
    CAST(_scd_start_date AS TIMESTAMP ) AS ValidFrom,
    CAST(COALESCE(_scd_end_date,'9999-12-31 23:59:59') AS TIMESTAMP ) AS ValidTo,
    _scd_is_current AS IsCurrent
FROM silver_employees

WHERE _scd_is_current = true;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_dim_date
USING DELTA
AS

WITH date_range AS (
    SELECT
        explode(
            sequence(
                (
                    SELECT MIN(d)
                    FROM (
                        SELECT to_date(OrderDate)    AS d FROM silver_orders
                        UNION ALL SELECT to_date(RequiredDate) FROM silver_orders
                        UNION ALL SELECT to_date(ShippedDate)  FROM silver_orders
                    )
                    WHERE d IS NOT NULL
                ),
                (
                    SELECT MAX(d)
                    FROM (
                        SELECT to_date(OrderDate)    AS d FROM silver_orders
                        UNION ALL SELECT to_date(RequiredDate) FROM silver_orders
                        UNION ALL SELECT to_date(ShippedDate)  FROM silver_orders
                    )
                    WHERE d IS NOT NULL
                ),
                interval 1 day
            )
        ) AS DateValue
)

SELECT 
    CAST(date_format(DateValue, 'yyyyMMdd') AS INT) AS DateKey,
    DateValue AS FullDate,
    YEAR(DateValue) AS Year,
    QUARTER(DateValue) AS Quarter,
    MONTH(DateValue) AS Month,
    date_format(DateValue, 'MMMM') AS MonthName,
    WEEKOFYEAR(DateValue) AS WeekOfYear,
    DAY(DateValue) AS DayOfMonth,
    DAYOFWEEK(DateValue) AS DayOfWeek,
    date_format(DateValue, 'EEEE') AS DayName,
    CASE WHEN DAYOFWEEK(DateValue) IN (1,7) THEN true ELSE false END AS IsWeekend
FROM date_range


""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_fact_order_sales
USING DELTA
AS

SELECT
    CAST(crc32(concat_ws('||',CAST(od.OrderId AS STRING),CAST(od.ProductId AS STRING))) AS BIGINT) AS FactSalesKey,
    CAST(o.OrderId AS BIGINT) AS OrderKey,
    CAST(c.CustomerKey AS BIGINT) AS CustomerKey,
    CAST(e.EmployeeKey AS BIGINT) AS EmployeeKey,
    CAST(p.ProductKey AS BIGINT) AS ProductKey,
    CAST(s.SupplierKey AS BIGINT) AS SupplierKey,
    CAST(sh.ShipperKey AS BIGINT) AS ShipperKey,
    
    CAST(date_format(CAST(o.OrderDate AS TIMESTAMP), 'yyyyMMdd') AS INT)    AS OrderDateKey,
    CAST(date_format(CAST(o.RequiredDate AS TIMESTAMP), 'yyyyMMdd') AS INT) AS RequiredDateKey,
    CAST(date_format(CAST(o.ShippedDate AS TIMESTAMP), 'yyyyMMdd') AS INT)  AS ShippedDateKey,

    CAST(od.Quantity AS BIGINT) AS Quantity,
    CAST(od.UnitPrice AS DOUBLE) AS UnitPrice,
    CAST(od.Discount AS DOUBLE) AS Discount,
    CAST(o.Freight AS DOUBLE) AS Freight

FROM silver_order_details od

INNER JOIN silver_orders o ON od.OrderId = o.OrderId

LEFT JOIN gold_dim_customer c ON o.CustomerId = c.CustomerId

LEFT JOIN gold_dim_employee e ON o.EmployeeId = e.EmployeeId

LEFT JOIN gold_dim_product p ON od.ProductId = p.ProductId

LEFT JOIN gold_dim_supplier s ON p.SupplierId = s.SupplierId

LEFT JOIN gold_dim_shipper sh ON o.ShipVia = sh.ShipperId

WHERE od._scd_is_current = true and o._scd_is_current = true ;
""")
print("done")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BronzeSalesLH.gold_fact_order_sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
