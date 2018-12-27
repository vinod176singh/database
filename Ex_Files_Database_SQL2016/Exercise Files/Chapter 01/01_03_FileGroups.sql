-- create a new database for this example
CREATE DATABASE FileGroupExample;
GO
USE FileGroupExample;
GO

-- create an additional filegroup

ALTER DATABASE FileGroupExample
ADD FILEGROUP IndexFG;
GO

ALTER DATABASE FileGroupExample
ADD FILE
(
    NAME = IndexData,
    FILENAME = 'C:\TempDatabases\IndexData.ndf',
    SIZE = 10MB,
    MAXSIZE = 100MB,
    FILEGROWTH = 10MB
)
TO FILEGROUP IndexFG;
GO

-- view the current location of data and log files
SELECT name, physical_name, state_desc
FROM sys.master_files
WHERE database_id = DB_ID(N'FileGroupExample');
GO

-- add a table
CREATE TABLE dbo.Employees (
    EmployeeID int IDENTITY(1000,1) PRIMARY KEY,
    EmployeeName nvarchar(100) NOT NULL,
    EmployeePhone  nvarchar(8) NULL,
    EmployeeSalary smallmoney
);
GO

-- create index on the new filegroup
CREATE INDEX IX_EmployeeName
ON dbo.Employees (EmployeeName)
ON IndexFG;
GO

-- check the location of the database's indexes

SELECT objects.name, objects.type, indexes.name, indexes.index_id, filegroups.name
FROM sys.indexes indexes INNER JOIN sys.filegroups filegroups
	ON indexes.data_space_id = filegroups.data_space_id INNER JOIN sys.all_objects objects
	ON indexes.object_id = objects.object_id
WHERE indexes.data_space_id = filegroups.data_space_id AND objects.type = 'U';
GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE FileGroupExample;
GO