-- create a new database for this example
CREATE DATABASE FragmentedDB;
GO
USE FragmentedDB;
GO

-- create a basic table and force fragmentation
CREATE TABLE dbo.Products (
    ProductID int IDENTITY(1,1) PRIMARY KEY,
    ProductName nvarchar(100) NOT NULL
);
GO

CREATE INDEX IX_ProductName on dbo.Products(ProductName);
GO

DECLARE @i int = 100
WHILE @i > 0
BEGIN
    INSERT dbo.Products
        VALUES  ('Mixed Nuts'),
                ('Shelled Peanuts'),
                ('Roasted Almonds')
    SET @i -=1
END;
GO

DELETE FROM dbo.Products
WHERE ProductName = 'Shelled Peanuts';
GO

-- detect fragmentation on an index
SELECT * FROM sys.dm_db_index_physical_stats
    (DB_ID('FragmentedDB'), OBJECT_ID('dbo.Products'), NULL, NULL , 'DETAILED');
GO

SELECT * FROM sys.indexes
WHERE object_id = 565577053;
GO

-- for fragmentation between 5% and 30%:
-- ALTER INDEX IX_ProductName
-- ON dbo.Products
-- REORGANIZE;

-- for fragmentation above 30%
-- rebuild index to remove fragmentation
ALTER INDEX IX_ProductName
ON dbo.Products
REBUILD WITH (ONLINE = ON);

-- review fragmentation stats
SELECT * FROM sys.dm_db_index_physical_stats
    (DB_ID('FragmentedDB'), OBJECT_ID('dbo.Products'), '2', NULL , 'DETAILED');
GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE FragmentedDB;
GO