-- create a new database for this example
CREATE DATABASE PermissionsDB;
GO
USE PermissionsDB;
GO

-- create a basic table
CREATE TABLE Products (
    ProductID int IDENTITY(1,1) PRIMARY KEY,
    ProductName nvarchar(100) NOT NULL
);
GO

INSERT Products
    VALUES  ('Mixed Nuts'),
            ('Shelled Peanuts'),
            ('Roasted Almonds');
GO

SELECT * FROM Products;
GO

CREATE TABLE Employees (
    EmployeeID int IDENTITY(1,1) PRIMARY KEY,
    EmployeeName nvarchar(100) NOT NULL
);
GO

INSERT Employees
    VALUES  ('Charlie'),
            ('Oscar'),
            ('Mike');
GO

SELECT * FROM Employees;
GO

-- create user and switch security contexts
CREATE USER Juliett WITHOUT LOGIN;
GO
SELECT USER_NAME();
GO
EXECUTE AS USER = 'Juliett';
GO
SELECT USER_NAME();
GO

-- access objects
SELECT * FROM Products;
GO

INSERT Products
    VALUES  ('Salted Cashews');
GO

-- return to dbo
REVERT;
GO
SELECT USER_NAME();
GO

-- assign Juliett some permissions
ALTER ROLE db_datareader ADD MEMBER Juliett;
GO
GRANT INSERT ON Products TO Juliett;
GO

-- test permissions
EXECUTE AS USER = 'Juliett';
GO

SELECT * FROM Products;
GO

SELECT * FROM Employees;
GO

INSERT Products
    VALUES  ('Salted Cashews');
GO

INSERT Employees
    VALUES  ('Victor');
GO

-- return to dbo
REVERT;
GO
SELECT USER_NAME();
GO

-- revoke permission granted through group membership
REVOKE SELECT ON Products TO Juliett;
GO

EXECUTE AS USER = 'Juliett';
GO

SELECT * FROM Products;
GO

-- return to dbo
REVERT;
GO
SELECT USER_NAME();
GO

-- deny permission granted through group membership
DENY SELECT ON Products TO Juliett;
GO

EXECUTE AS USER = 'Juliett';
GO

SELECT * FROM Products;
GO

-- clean up the instance
REVERT;
GO
USE tempdb;
GO
DROP DATABASE PermissionsDB;
GO