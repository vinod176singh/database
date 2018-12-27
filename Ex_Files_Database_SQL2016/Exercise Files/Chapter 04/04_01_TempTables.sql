-- create a new database for this example
CREATE DATABASE TablesDB;
GO
USE TablesDB;
GO

-- create temporary table to store books
CREATE TABLE #tmpBooks (
    ISBN char(16) PRIMARY KEY,
    Title nvarchar(100) NOT NULL,
    AuthorFirstName nvarchar(50) NULL,
    AuthorLastName  nvarchar(50) NOT NULL
);
GO

-- insert values into the books temporary table
INSERT #tmpBooks
    VALUES  ('1','Anna Karenina','Leo','Tolstoy'),
            ('2','War and Peace','Leo','Tolstoy'),
            ('3','Ulysses','James','Joyce'),
            ('4','Emma','Jane','Austen'),
            ('5','Pride and Prejudice','Jane','Austen');
GO

SELECT * FROM #tmpBooks;
GO

-- disconnect, reconnect, then query the table again
USE TablesDB;
GO
SELECT * FROM #tmpBooks;
GO

-- create and use table variables in a single batch
DECLARE @tmpAuthors TABLE (
    AuthorID int IDENTITY(1000,1) PRIMARY KEY,
    AuthorFirstName nvarchar(50) NULL,
    AuthorLastName  nvarchar(50) NOT NULL
);

INSERT @tmpAuthors
    VALUES  ('Leo','Tolstoy'),
            ('James','Joyce'),
            ('Jane','Austen');

DECLARE @tmpBooks TABLE (
    ISBN char(16) PRIMARY KEY,
    Title nvarchar(100) NOT NULL,
    AuthorID int NOT NULL
);

INSERT @tmpBooks
    VALUES  ('1','Anna Karenina','1000'),
            ('2','War and Peace','1000'),
            ('3','Ulysses','1001'),
            ('4','Emma','1002'),
            ('5','Pride and Prejudice','1002');

SELECT *
    FROM @tmpBooks B
    JOIN @tmpAuthors A
    ON B.AuthorID = A.AuthorID;

GO

-- clean up the instance
USE tempdb;
GO
DROP DATABASE TablesDB;
GO