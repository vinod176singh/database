-- create a new database for this example
CREATE DATABASE TailLogDB;
GO
USE TailLogDB;
GO

-- insert some data
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

-- create a full backup
BACKUP DATABASE TailLogDB
TO DISK = 'C:\TempDatabases\TailLogDB_FULL.bak'

-- insert additional records
EXECUTE InsertPeople;
GO
SELECT * FROM People;
GO

-- take TailLogDB offline
-- delete .mdf data file from the hard drive
USE master;
GO

-- create a tail-log backup
BACKUP LOG TailLogDB
TO DISK = 'C:\TempDatabases\TailLogDB.log'
WITH CONTINUE_AFTER_ERROR;
GO

-- attempt to take TailLogDB online

-- restore the database
USE master
RESTORE DATABASE TailLogDB
FROM DISK = 'C:\TempDatabases\TailLogDB_FULL.bak'
WITH NORECOVERY;
GO

RESTORE LOG TailLogDB
FROM DISK = 'C:\TempDatabases\TailLogDB.log';
GO

-- verify the results
USE TailLogDB
SELECT * FROM People;
GO

-- clean up the instance and delete files from C:\TempDatabases
USE tempdb;
GO
DROP DATABASE TailLogDB;
GO