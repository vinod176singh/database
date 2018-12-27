-- create a new database for this example
CREATE DATABASE SourceDB;
GO
USE SourceDB;
GO

-- create a table and populate 100 records
CREATE TABLE People (
    PersonID int IDENTITY(1000,1) PRIMARY KEY NOT NULL,
    PersonValue int
);
GO
CREATE PROCEDURE InsertPeople
AS
DECLARE @i int = 100
WHILE @i > 0
    BEGIN
        INSERT People (PersonValue) VALUES (@i)
        Set @i -=1
    END
GO
EXECUTE InsertPeople;
GO
SELECT * FROM People;
GO

-- view the current location of data and log files
SELECT name, physical_name, state_desc
FROM sys.master_files
WHERE database_id = DB_ID(N'SourceDB');
GO

-- create a database snapshot and view the results
CREATE DATABASE SnapshotExample_Q1
ON (NAME = SourceDB, FILENAME = 'C:\TempDatabases\SnapshotExample_Q1_Data.snap')
AS SNAPSHOT OF SourceDB;
GO

-- review the data in object explorer
USE SnapshotExample_Q1;
GO
SELECT * FROM People;
GO

-- try and add data to the snapshot
INSERT People (PersonValue) VALUES (1);
GO

-- add additional records to the source database
USE SourceDB;
GO
EXECUTE InsertPeople;
GO
SELECT * FROM People;
GO

-- review the snapshot records again
USE SnapshotExample_Q1;
GO
SELECT * FROM People;
GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE SourceDB;
GO

-- must drop snapshots first!
DROP DATABASE SnapshotExample_Q1;
GO
DROP DATABASE SourceDB;
GO
