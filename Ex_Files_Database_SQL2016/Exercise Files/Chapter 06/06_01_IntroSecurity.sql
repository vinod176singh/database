-- enable contained databases on the server
sp_configure 'contained database authentication', 1;
GO
RECONFIGURE;
GO

-- create a new database for this example
CREATE DATABASE ContainedUserDB;
GO
USE ContainedUserDB;
GO

-- create a basic table
CREATE TABLE Products(
	ProductID int
);
GO

-- set containment level
USE master;
GO
ALTER DATABASE ContainedUserDB SET CONTAINMENT = PARTIAL;
GO

-- create user
USE ContainedUserDB;
GO

CREATE USER Foxtrot
    WITH PASSWORD = 'abc123';
GO

-- login to the instance as the contained user

-- return as dbo
-- clean up the instance
USE tempdb;
GO
DROP DATABASE ContainedUserDB;
GO
sp_configure 'contained database authentication', 0;
GO
RECONFIGURE;
GO