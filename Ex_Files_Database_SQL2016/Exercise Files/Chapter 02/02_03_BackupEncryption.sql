-- create a new database for this example
CREATE DATABASE TopSecretDB;
GO
USE master;
GO

-- create master key and certificate
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '1StrongPassword';
GO

CREATE CERTIFICATE TopSecretDBCert
    WITH SUBJECT = 'TopSecretDB Backup Certificate';
GO

-- export backup certificate to a file
BACKUP CERTIFICATE TopSecretDBCert TO FILE = 'C:\TempDatabases\TopSecretDBCert.cert'
WITH PRIVATE KEY (
FILE = 'C:\TempDatabases\TopSecretDBCert.key',
ENCRYPTION BY PASSWORD = 'abc123')

-- backup database with encryption
BACKUP DATABASE TopSecretDB
TO DISK = 'C:\TempDatabases\TopSecretDB.bak'
WITH ENCRYPTION (ALGORITHM = AES_256, SERVER CERTIFICATE = TopSecretDBCert)

-- clean up the instance
DROP DATABASE TopSecretDB;
GO
DROP CERTIFICATE TopSecretDBCert;
GO

-- restore the database from backup
RESTORE DATABASE TopSecretDB
FROM DISK = 'C:\TempDatabases\TopSecretDB.bak';
GO

-- restore the certificate
CREATE CERTIFICATE TopSecretDBCert
FROM FILE = 'C:\TempDatabases\TopSecretDBCert.cert'
WITH PRIVATE KEY (FILE = 'C:\TempDatabases\TopSecretDBCert.key',
DECRYPTION BY PASSWORD = 'abc123');
GO

-- attempt the restore again

-- clean up the instance
DROP DATABASE TopSecretDB;
GO
DROP CERTIFICATE TopSecretDBCert;
GO
DROP MASTER KEY;
GO
