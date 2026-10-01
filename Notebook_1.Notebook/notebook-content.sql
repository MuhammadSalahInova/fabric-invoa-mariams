-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "f2d18090-7295-438d-905e-205a42447bdc",
-- META       "default_lakehouse_name": "BronzeSalesLH",
-- META       "default_lakehouse_workspace_id": "03a51e0a-fbda-4ed9-a1a5-f5259819c960",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f2d18090-7295-438d-905e-205a42447bdc"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "9b69b19e-b7b1-45bf-9fdc-b1ba4fe48857",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "9b69b19e-b7b1-45bf-9fdc-b1ba4fe48857",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity * UnitPrice * (1 - Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity * UnitPrice) AS DECIMAL(18,2)) AS GrossRevenue 
FROM gold_fact_order_sales;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity * UnitPrice * Discount) AS DECIMAL(18,2)) AS DiscountAmount 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity * UnitPrice * Discount) / NULLIF(SUM(Quantity * UnitPrice), 0) AS DECIMAL(9,4)) AS DiscountPctOfRevenue 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT OrderKey) AS TotalOrders FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT SUM(Quantity) AS TotalQuantitySold 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT OrderKey), 0) AS DECIMAL(9,2)) AS AvgItemsPerOrder 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity * UnitPrice * (1 - Discount)) / NULLIF(COUNT(DISTINCT OrderKey), 0) AS DECIMAL(18,2)) AS AvgOrderValue 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT d.Year AS OrderYear, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey 
GROUP BY d.Year 
ORDER BY d.Year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT d.Year AS OrderYear, COUNT(DISTINCT f.OrderKey) AS TotalOrders 
FROM gold_fact_order_sales f 
JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey 
GROUP BY d.Year 
ORDER BY d.Year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CASE WHEN d.IsWeekend=1 THEN 'Weekend' ELSE 'Weekday' END AS DayType, COUNT(DISTINCT f.OrderKey) AS TotalOrders 
FROM gold_fact_order_sales f 
JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey 
GROUP BY CASE WHEN d.IsWeekend=1 THEN 'Weekend' ELSE 'Weekday' END 
ORDER BY TotalOrders DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT CustomerId) AS TotalCustomers 
FROM gold_dim_customer 
WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT CustomerKey) AS ActiveCustomers FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT c.CustomerId) AS CustomersWithNoOrders 
FROM gold_dim_customer c 
LEFT JOIN gold_fact_order_sales f ON c.CustomerKey=f.CustomerKey 
WHERE c.IsCurrent=1 AND f.CustomerKey IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT c.CompanyName AS CustomerName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_customer c ON f.CustomerKey=c.CustomerKey 
GROUP BY c.CompanyName 
ORDER BY NetRevenue DESC
limit 10;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT c.Country AS Country, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_customer c ON f.CustomerKey=c.CustomerKey 
GROUP BY c.Country 
ORDER BY NetRevenue DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT c.CustomerId AS CustomerCode, c.CompanyName AS CustomerName 
FROM gold_dim_customer c 
LEFT JOIN gold_fact_order_sales f ON c.CustomerKey=f.CustomerKey 
WHERE c.IsCurrent=1 AND f.CustomerKey IS NULL 
ORDER BY c.CompanyName;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT p.ProductName AS ProductName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_product p ON f.ProductKey=p.ProductKey 
GROUP BY p.ProductName 
ORDER BY NetRevenue DESC
limit 10;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT p.ProductName AS ProductName, SUM(f.Quantity) AS TotalQuantitySold 
FROM gold_fact_order_sales f 
JOIN gold_dim_product p ON f.ProductKey=p.ProductKey 
GROUP BY p.ProductName 
ORDER BY TotalQuantitySold DESC
limit 10;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT p.CategoryName AS CategoryName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_product p ON f.ProductKey=p.ProductKey 
GROUP BY p.CategoryName 
ORDER BY NetRevenue DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT p.CategoryName AS CategoryName, SUM(f.Quantity) AS TotalQuantitySold 
FROM gold_fact_order_sales f 
JOIN gold_dim_product p ON f.ProductKey=p.ProductKey 
GROUP BY p.CategoryName 
ORDER BY TotalQuantitySold DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CASE WHEN p.UnitPrice<10 THEN 'OnBudget' 
        WHEN p.UnitPrice<40 THEN 'Mid-Range' 
        WHEN p.UnitPrice<70 THEN 'Premium' 
        ELSE 'Luxury' END AS PriceTier, COUNT(DISTINCT p.ProductId) AS ProductCount 

FROM gold_dim_product p 
WHERE p.IsCurrent=1 
GROUP BY CASE WHEN p.UnitPrice<10 THEN 'OnBudget' 
            WHEN p.UnitPrice<40 THEN 'Mid-Range' 
            WHEN p.UnitPrice<70 THEN 'Premium' 
            ELSE 'Luxury' END 
            
ORDER BY ProductCount DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT SupplierId) AS TotalSuppliers 
FROM gold_dim_supplier 
WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT s.CompanyName AS SupplierName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_supplier s ON f.SupplierKey=s.SupplierKey 
GROUP BY s.CompanyName ORDER BY NetRevenue DESC
limit 10;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT s.CompanyName AS SupplierName, COUNT(DISTINCT p.ProductId) AS ProductsPerSupplier 
FROM gold_dim_supplier s 
LEFT JOIN gold_dim_product p ON s.SupplierId=p.SupplierId AND p.IsCurrent=1 
WHERE s.IsCurrent=1 
GROUP BY s.CompanyName 
ORDER BY ProductsPerSupplier DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT e.FullName AS EmployeeName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_employee e ON f.EmployeeKey=e.EmployeeKey 
GROUP BY e.FullName 
ORDER BY NetRevenue DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(Quantity*UnitPrice*(1-Discount))/NULLIF(COUNT(DISTINCT EmployeeKey),0) AS DECIMAL(18,2)) AS RevenuePerEmployee 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT e.FullName AS EmployeeName, d.Year AS OrderYear, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_employee e ON f.EmployeeKey=e.EmployeeKey 
JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey 
GROUP BY e.FullName, d.Year 
ORDER BY e.FullName, d.Year;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COALESCE(m.FullName,'No Manager') AS ManagerName, CAST(SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS DECIMAL(18,2)) AS NetRevenue 
FROM gold_fact_order_sales f 
JOIN gold_dim_employee e ON f.EmployeeKey=e.EmployeeKey 
LEFT JOIN gold_dim_employee m ON e.ReportsTo=m.EmployeeId AND m.IsCurrent=1 
GROUP BY COALESCE(m.FullName,'No Manager') 
ORDER BY NetRevenue DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH ol AS 
    (SELECT DISTINCT OrderKey, RequiredDateKey, ShippedDateKey 
    FROM gold_fact_order_sales 
    WHERE ShippedDateKey IS NOT NULL AND RequiredDateKey IS NOT NULL) 
    
SELECT CAST(SUM(CASE WHEN ShippedDateKey<=RequiredDateKey THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS DECIMAL(9,4)) AS OnTimeDeliveryPct 
FROM ol;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH ol AS 
    (SELECT DISTINCT OrderKey, ShipperKey, RequiredDateKey, ShippedDateKey 
    FROM gold_fact_order_sales 
    WHERE ShippedDateKey IS NOT NULL AND RequiredDateKey IS NOT NULL) 

SELECT sh.CompanyName AS ShipperName, CAST(SUM(CASE WHEN ol.ShippedDateKey<=ol.RequiredDateKey THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS DECIMAL(9,4)) AS OnTimeDeliveryPct 
FROM ol 
JOIN gold_dim_shipper sh ON ol.ShipperKey=sh.ShipperKey 
GROUP BY sh.CompanyName 
ORDER BY OnTimeDeliveryPct DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(COUNT(DISTINCT CASE WHEN ShippedDateKey IS NOT NULL THEN OrderKey END)*1.0/NULLIF(COUNT(DISTINCT OrderKey),0) AS DECIMAL(9,4)) AS ShipRatePct 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CASE WHEN ShippedDateKey IS NULL THEN 'Not Shipped' WHEN ShippedDateKey>RequiredDateKey THEN 'Late' ELSE 'On Time' END AS ShipmentStatus, COUNT(*) AS OrderLines 
FROM gold_fact_order_sales 
GROUP BY CASE WHEN ShippedDateKey IS NULL THEN 'Not Shipped' WHEN ShippedDateKey>RequiredDateKey THEN 'Late' ELSE 'On Time' END 
ORDER BY OrderLines DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT sh.CompanyName AS ShipperName, CAST(AVG(CAST(DATEDIFF(DAY, dreq.FullDate, dship.FullDate) AS DECIMAL(9,2))) AS DECIMAL(9,2)) AS AvgDaysLate 
FROM gold_fact_order_sales f 
JOIN gold_dim_shipper sh ON f.ShipperKey=sh.ShipperKey 
JOIN gold_dim_date dreq ON f.RequiredDateKey=dreq.DateKey 
JOIN gold_dim_date dship ON f.ShippedDateKey=dship.DateKey 
WHERE f.ShippedDateKey IS NOT NULL AND f.RequiredDateKey IS NOT NULL AND f.ShippedDateKey>f.RequiredDateKey 
GROUP BY sh.CompanyName 
ORDER BY AvgDaysLate DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH of_ AS 
    (SELECT OrderKey, MAX(OrderDateKey) AS OrderDateKey, MAX(Freight) AS Freight 
    FROM gold_fact_order_sales 
    GROUP BY OrderKey), 

fy AS 
    (SELECT d.Year AS OrderYear, SUM(of_.Freight) AS TotalFreight 
    FROM of_ 
    JOIN gold_dim_date d ON of_.OrderDateKey=d.DateKey 
    GROUP BY d.Year), 
    
ry AS 
    (SELECT d.Year AS OrderYear, SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) AS NetRevenue 
    FROM gold_fact_order_sales f 
    JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey 
    GROUP BY d.Year) 

SELECT ry.OrderYear, CAST(fy.TotalFreight/NULLIF(ry.NetRevenue,0) AS DECIMAL(9,4)) AS ShippingCostPctOfNetRevenue 
FROM ry 
LEFT JOIN fy ON ry.OrderYear=fy.OrderYear 
ORDER BY ry.OrderYear;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT c.CompanyName AS CustomerName 
FROM gold_fact_order_sales f 
JOIN gold_dim_customer c ON f.CustomerKey=c.CustomerKey 
GROUP BY c.CompanyName 
ORDER BY SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) DESC
limit 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT p.ProductName 
FROM gold_fact_order_sales f 
JOIN gold_dim_product p ON f.ProductKey=p.ProductKey 
GROUP BY p.ProductName 
ORDER BY SUM(f.Quantity*f.UnitPrice*(1-f.Discount)) DESC
limit 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT MAX(UnitsInStock) AS MaxUnitsInStock 
FROM gold_dim_product 
WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(*) AS DiscontinuedProductCount 
FROM gold_dim_product 
WHERE IsCurrent=1 AND Discontinued=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT ProductId) AS LowStockProducts 
FROM gold_dim_product 
WHERE IsCurrent=1 AND UnitsInStock<=ReorderLevel;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(UnitsInStock*UnitPrice) AS DECIMAL(18,2)) AS TotalInventoryValue 
FROM gold_dim_product 
WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH ol AS 
    (SELECT OrderKey, MAX(Freight) AS Freight 
    FROM gold_fact_order_sales 
    GROUP BY OrderKey) 
SELECT CAST(MAX(Freight) AS DECIMAL(18,2)) AS MaxShippingCost 
FROM ol;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT MIN(d.FullDate) AS EarliestOrderDate 
FROM gold_fact_order_sales f 
JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT MAX(d.FullDate) AS LatestOrderDate FROM gold_fact_order_sales f JOIN gold_dim_date d ON f.OrderDateKey=d.DateKey;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT ProductId) AS DistinctCurrentProducts 
FROM gold_dim_product WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT ShipperId) AS DistinctShippers 
FROM gold_dim_shipper WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT EmployeeKey) AS EmployeesWithSales FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(MAX(Discount) AS DECIMAL(9,4)) AS MaxDiscount 
FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT COUNT(DISTINCT OrderDateKey) AS DistinctOrderDates FROM gold_fact_order_sales;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT CAST(SUM(CASE WHEN Discontinued=1 THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS DECIMAL(9,2)) AS DiscontinuedPct 
FROM gold_dim_product WHERE IsCurrent=1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
