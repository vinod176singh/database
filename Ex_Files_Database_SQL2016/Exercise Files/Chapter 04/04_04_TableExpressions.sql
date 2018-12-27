-- create a new database for this example
CREATE DATABASE TableExpressionsDB;
GO
USE TableExpressionsDB;
GO

-- create a table and populate values
CREATE TABLE Sales (
    EmployeeName nvarchar(100) NOT NULL,
    SalesYear smallint,
    SalesAmount smallmoney
);
GO
INSERT Sales
    VALUES  ('Malcom','2012', 15648),
            ('Rory','2012', 48094),
            ('Brianne','2012', 48905),
            ('Malcom','2013', 89048),
            ('Rory','2013', 80460),
            ('Brianne','2013', 26047),
            ('Malcom','2014', 90596),
            ('Rory','2014', 65044),
            ('Brianne','2014', 90504),
            ('Malcom','2015', 60507),
            ('Rory','2015', 14425),
            ('Brianne','2015', 84287),
            ('Malcom','2016', 21461),
            ('Rory','2016', 84205),
            ('Brianne','2016', 54852)
;
GO
SELECT * FROM Sales;
GO


-- find the average sales amount for each year
SELECT SalesYear, AVG(SalesAmount) AS AverageSales
FROM Sales
GROUP BY SalesYear;

-- use those values in a common table expression
WITH TotalSales_CTE (SalesYear, AverageSales)
AS
(
    SELECT SalesYear, AVG(SalesAmount)
    FROM Sales
    GROUP BY SalesYear
)
SELECT Sales.EmployeeName, Sales.SalesYear, Sales.SalesAmount, TotalSales_CTE.AverageSales
FROM Sales
    INNER JOIN TotalSales_CTE
    ON Sales.SalesYear = TotalSales_CTE.SalesYear
ORDER BY Sales.SalesYear DESC, Sales.EmployeeName;
GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE TableExpressionsDB;
GO