-- create a new database for this example
CREATE DATABASE StoredProcParametersDB;
GO
USE StoredProcParametersDB;
GO

-- create a disk based table
CREATE TABLE dbo.Orders (
    OrderNumber int);
GO

-- create a stored procedure with an input parameter
CREATE PROCEDURE dbo.InsertVariableRecords
    @paramQuantity smallint
AS
DECLARE @i int = @paramQuantity
WHILE @i > 0
    BEGIN
        INSERT dbo.Orders VALUES (@i)
        SET @i -=1
    END;
GO

EXECUTE dbo.InsertVariableRecords 10;
GO

-- view the contents of the table
SELECT * FROM dbo.Orders
ORDER BY OrderNumber;
GO

-- create a stored procedure with input and output parameters
CREATE PROCEDURE dbo.CountRecords
    @OrderNumber int,
    @CountOfOrders int OUTPUT
AS
    SELECT @CountOfOrders = Count(OrderNumber)
	FROM dbo.Orders
	WHERE OrderNumber = @OrderNumber;
RETURN
GO

-- declare the variable to receive the output value of the procedure.
DECLARE @ViewOutput int;
EXECUTE dbo.CountRecords 8, @CountOfOrders = @ViewOutput OUTPUT;
PRINT @ViewOutput;
GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE StoredProcParametersDB;
GO