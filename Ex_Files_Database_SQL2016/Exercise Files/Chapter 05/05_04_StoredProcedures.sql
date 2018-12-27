-- create a new database for this example
CREATE DATABASE StoredProcDB;
GO
USE StoredProcDB;
GO

-- create a table
CREATE TABLE dbo.Orders (
    OrderNumber int PRIMARY KEY);
GO

-- create a stored procedure to insert values
CREATE PROCEDURE dbo.InsertRecords
AS
DECLARE @i int = 100
WHILE @i > 0
    BEGIN
        INSERT dbo.Orders VALUES (@i)
        SET @i -=1
    END;
GO

EXECUTE dbo.InsertRecords;
GO

-- view the contents of the table
SELECT * FROM dbo.Orders;

-- clean up the instance
USE tempdb;
GO
DROP DATABASE StoredProcDB;
GO