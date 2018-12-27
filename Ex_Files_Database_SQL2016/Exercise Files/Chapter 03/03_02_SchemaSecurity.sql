-- create a new database for this example
CREATE DATABASE SchemaSecurityDB;
GO
USE SchemaSecurityDB;
GO

-- create a new schema in the database
CREATE SCHEMA HumanResources AUTHORIZATION dbo;
GO

-- insert some data
CREATE TABLE HumanResources.Employees (
    EmployeeID int IDENTITY(1000,1) PRIMARY KEY,
    EmployeeName nvarchar(100) NOT NULL,
    EmployeePhone  nvarchar(8) NULL,
    EmployeeSalary smallmoney
);
GO
INSERT HumanResources.Employees
    VALUES  ('Malcom','555-0123', 2500.00),
            ('Rory','555-1234', 3200.00),
            ('Brianne','555-7890', 1950.00);
GO
SELECT * FROM HumanResources.Employees;
GO

-- create loginless user and switch security contexts
CREATE USER Maria WITHOUT LOGIN;
GO

SELECT USER_NAME();
GO

EXECUTE AS USER = 'Maria';
GO

SELECT USER_NAME();
GO

SELECT * FROM HumanResources.Employees;
GO

-- switch security context back to original user
REVERT;
GO
SELECT USER_NAME();
GO

-- give Maria some selected permissions
GRANT SELECT ON SCHEMA :: HumanResources
TO Maria;
GO

-- verify Maria's access
EXECUTE AS USER = 'Maria';
GO
SELECT * FROM HumanResources.Employees;
GO

INSERT HumanResources.Employees
    VALUES  ('Kate','555-1235', 2500.00);
GO

-- clean up the instance
REVERT;
GO
USE tempdb;
GO
DROP DATABASE SchemaSecurityDB;
GO