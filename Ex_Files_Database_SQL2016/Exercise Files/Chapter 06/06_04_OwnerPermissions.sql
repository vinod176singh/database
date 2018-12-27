-- create a new database for this example
CREATE DATABASE OwnershipDB;
GO
USE OwnershipDB;
GO

-- create a schema
CREATE SCHEMA Inventory AUTHORIZATION dbo;
GO

-- create a basic table
CREATE TABLE Inventory.Products (
    ProductID int IDENTITY(1,1) PRIMARY KEY,
    ProductName nvarchar(100) NOT NULL
);
GO

INSERT Inventory.Products
    VALUES  ('Mixed Nuts'),
            ('Shelled Peanuts'),
            ('Roasted Almonds');
GO

SELECT * FROM Inventory.Products;
GO

-- create user and switch security contexts
CREATE USER Charlie WITHOUT LOGIN;
GO

-- access objects
EXECUTE AS USER = 'Charlie';
GO
SELECT USER_NAME();
GO
SELECT * FROM Inventory.Products;
GO

-- return to dbo
REVERT;
GO
SELECT USER_NAME();
GO

-- transfer ownership of schema to Charlie
ALTER AUTHORIZATION ON SCHEMA::Inventory TO Charlie;
GO

-- verify ownership
SELECT schema_name, schema_owner
FROM information_schema.schemata;
GO

-- test ownership permissions
EXECUTE AS USER = 'Charlie';
GO
SELECT USER_NAME();
GO
SELECT * FROM Inventory.Products;
GO

INSERT Inventory.Products
    VALUES  ('Salted Cashews');
GO

-- return to dbo
REVERT;
GO
DENY SELECT ON Inventory.Products TO Charlie;
GO

-- clean up the instance
REVERT;
GO
USE tempdb;
GO
DROP DATABASE OwnershipDB;
GO