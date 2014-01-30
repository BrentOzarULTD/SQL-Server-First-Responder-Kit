--Requires databases on all SQL Versions to be named AdventureWorks
--Run this twice if SQL Server has been restarted recently

----------------------------------------
-- Create havoc!
----------------------------------------
USE AdventureWorks2012;


--Create a non-unique clustered index
IF OBJECT_ID('nonuniqueCX') IS NULL
BEGIN
	CREATE TABLE dbo.nonuniqueCX ( i INT, j INT, k INT);
	CREATE CLUSTERED INDEX cx_nonuniquecx ON dbo.nonuniqueCX(i,j);
END
GO
SELECT * FROM dbo.nonuniqueCX
GO 8

--Create two hyopthetical indexes
IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonProduct_ClassStyle_HYPOTHETICALINDEX' AND [is_hypothetical]=1) =0
BEGIN
	CREATE INDEX ixPersonProduct_ClassStyle_HYPOTHETICALINDEX ON [Production].Product ([Class],[Style]) WITH STATISTICS_ONLY;
END

IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonProduct_ListPrice_HYPOTHETICALINDEX' AND [is_hypothetical]=1) =0
BEGIN
	CREATE INDEX ixPersonProduct_ListPrice_HYPOTHETICALINDEX ON [Production].Product ([ListPrice]) WITH STATISTICS_ONLY;
END



--Create and disable two indexes
IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixProductionProduct_DISABLEDINDEX') =0
BEGIN
	CREATE INDEX ixProductionProduct_DISABLEDINDEX ON [Production].Product ([SellStartDate], [SellEndDate]);
END
IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixProductionProduct_DISABLEDINDEX' AND [is_disabled]=1) =0
BEGIN
	ALTER INDEX ixProductionProduct_DISABLEDINDEX ON [Production].Product DISABLE;
END

IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonProduct_DISABLEDINDEX') =0
BEGIN
	CREATE INDEX ixPersonProduct_DISABLEDINDEX ON [Production].Product (ProductID);
END

IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonProduct_DISABLEDINDEX' AND [is_disabled]=1) =0
BEGIN
	ALTER INDEX ixPersonProduct_DISABLEDINDEX ON [Production].Product DISABLE;
END



--Create two duplicate indexes
IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonAddress_DUPLICATEINDEX') =0
BEGIN
	CREATE INDEX ixPersonAddress_DUPLICATEINDEX ON Person.Address (AddressLine1, AddressLine2, City, StateProvinceID, PostalCode);
END

IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonAddress_DUPLICATEINDEX2') =0
BEGIN
	CREATE INDEX ixPersonAddress_DUPLICATEINDEX2 ON Person.Address (AddressLine1, AddressLine2, City, StateProvinceID, PostalCode);
END

--create two borderline duplicate indexes
IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonAddress_DUPLICATEINDEX3') =0
BEGIN
	CREATE INDEX ixPersonAddress_DUPLICATEINDEX3 ON Person.Address (AddressLine1, AddressLine2) INCLUDE (City, StateProvinceID, PostalCode);
END

IF (SELECT COUNT(*) FROM sys.indexes WHERE name='ixPersonAddress_BORDERLINEDUPLICATEINDEX') =0
BEGIN
	CREATE INDEX ixPersonAddress_BORDERLINEDUPLICATEINDEX ON Person.Address (AddressLine1) INCLUDE (AddressLine2, City, StateProvinceID, PostalCode, AddressID, ModifiedDate);
END
GO

--Compress two indexes (2008 + )
--EXEC sp_helpindex 'Sales.SalesPerson'
IF (SELECT CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256)),3) AS NUMERIC(3,1))) >= 10
BEGIN
	EXEC sp_executesql N'ALTER INDEX [AK_SalesPerson_rowguid] ON [Sales].[SalesPerson] REBUILD WITH (DATA_COMPRESSION=PAGE)'
END

IF (SELECT CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256)),3) AS NUMERIC(3,1))) >= 10
BEGIN
	EXEC sp_executesql N'ALTER INDEX [PK_SalesPerson_BusinessEntityID] ON [Sales].[SalesPerson] REBUILD WITH (DATA_COMPRESSION=ROW)'
END

--Create a columnstore index (2012 version only)
IF (SELECT CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256)),3) AS NUMERIC(3,1))) >= 11
BEGIN
	EXEC sp_executesql N'IF (SELECT COUNT(*) FROM sys.indexes WHERE name=''PurchasingVendor_COLUMNSTORE'') =0 
	CREATE COLUMNSTORE INDEX [PurchasingVendor_COLUMNSTORE] ON [Purchasing].[Vendor] (BusinessEntityID, AccountNumber, Name, CreditRating)
	'
END

--------------------------------
--Create a partitioned table with a non-aligned index
--------------------------------

--Create the partition function: dailyPF
--Start out 3 days ago

IF (SELECT COUNT(*) FROM sys.[partition_functions] WHERE name='DailyPF')=0
BEGIN

	--DROP PARTITION FUNCTION DailyPF
	DECLARE @StartDay DATETIME
	SELECT @StartDay=DATEADD(dd,-2,CAST(GETDATE() AS DATETIME));
	CREATE PARTITION FUNCTION DailyPF (DATETIME)
		AS RANGE RIGHT FOR VALUES
		(@StartDay, DATEADD(dd,1,@StartDay), DATEADD(dd,2,@StartDay) );

	--DROP PARTITION SCHEME DailyPS
	CREATE PARTITION SCHEME DailyPS 
		AS PARTITION DailyPF
		ALL TO ( [PRIMARY] );
END
GO




--Create a tally table, we'll use this to populate rows
--This method from Itzik Ben-Gan
IF OBJECT_ID('dbo.Tally') IS NULL
	CREATE TABLE dbo.Tally (N INT NOT NULL)
GO

--Delete all the records. This gives us an active heap 
DELETE FROM dbo.Tally;
GO

;WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), --2 rows
	Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),--4 rows
    Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),--16 rows
    Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),--256 rows
    Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),--65536 rows
    Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),--4,294,967,296 rows
    Tally0 AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
INSERT [Tally](N)
SELECT  N
FROM    Tally0
WHERE   N <= 100000;
GO


--DROP TABLE dbo.OrdersDaily
IF OBJECT_ID('OrdersDaily','U') is NULL
BEGIN
	CREATE TABLE dbo.OrdersDaily (
		OrderDate DATETIME NOT NULL,
		OrderId int IDENTITY NOT NULL,
		OrderName nvarchar(256) NOT NULL
	) on DailyPS(OrderDate)

	--OK-- let's insert some records.
	--Three days ago = 1000 rows
	INSERT dbo.OrdersDaily(OrderDate, OrderName) 
	SELECT DATEADD(ss, t.N, DATEADD(dd,-3,CAST(CONVERT(CHAR(10),GETDATE(),120)AS DATETIME))) AS OrderDate,
		CASE WHEN t.N % 3 = 0 THEN 'Robot' WHEN t.N % 4 = 0 THEN 'Badger'  ELSE 'Pen' END AS OrderName
	FROM dbo.Tally AS t
	WHERE N < = 1000
	
	--Two days ago = 2000 rows
	INSERT dbo.OrdersDaily(OrderDate, OrderName) 
	SELECT DATEADD(ss, t.N, DATEADD(dd,-2,CAST(CONVERT(CHAR(10),GETDATE(),120)AS DATETIME))) AS OrderDate,
		CASE WHEN t.N % 3 = 0 THEN 'Flying Monkey' WHEN t.N % 4 = 0 THEN 'Junebug'  ELSE 'Pen' END AS OrderName
	FROM dbo.Tally AS t
	WHERE N < = 2000

	--Yesterday= 3000 rows
	INSERT dbo.OrdersDaily(OrderDate, OrderName) 
	SELECT DATEADD(ss, t.N, DATEADD(dd,-1,CAST(CONVERT(CHAR(10),GETDATE(),120)AS DATETIME))) AS OrderDate,
		CASE WHEN t.N % 2 = 0 THEN 'Turtle' WHEN t.N % 5 = 0 THEN 'Eraser'  ELSE 'Pen' END AS OrderName
	FROM dbo.Tally AS t
	WHERE N < = 3000

	--Today=  4000 rows
	INSERT dbo.OrdersDaily(OrderDate, OrderName) 
	SELECT DATEADD(ss, t.N, CAST(CONVERT(CHAR(10),GETDATE(),120)AS DATETIME)) AS OrderDate,
		CASE WHEN t.N % 3 = 0 THEN 'Lasso' WHEN t.N % 2 = 0 THEN 'Cattle Prod'  ELSE 'Pen' END AS OrderName
	FROM dbo.Tally AS t
	WHERE N < = 4000

	--Add a Clustered Index-- Not a heap anymore
	--Note that this isn't unique
	ALTER TABLE dbo.OrdersDaily
	ADD CONSTRAINT PKOrdersDaily
		PRIMARY KEY CLUSTERED(OrderDate,OrderId)

	--An aligned NCI
	--We don't have to specify the partition function.
	CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily ON dbo.OrdersDaily(OrderId)


	--compress one partition if SQL 2008 or higher
	IF (SELECT CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(256)),3) AS NUMERIC(3,1))) >= 10
	BEGIN
		EXEC sp_executesql N'ALTER INDEX [NCOrderIdOrdersDaily] ON dbo.OrdersDaily REBUILD PARTITION=2 WITH (DATA_COMPRESSION=PAGE)'
	END

	--An NCI that is NOT aligned
	CREATE NONCLUSTERED INDEX NCOrderNameOrdersDailyNonAligned ON dbo.OrdersDaily(OrderName) ON [PRIMARY]
END
GO

--Create a high value missing indexes
--It's trashy, but oh well
DECLARE @count INT;
SELECT @count=COUNT(*)
FROM [Sales].[vIndividualCustomer]
WHERE [LastName] LIKE 'R%'
GO 1000

--Create identity tables near the end of ranges
IF OBJECT_ID('IdentityHigh') IS NULL
create table dbo.IdentityHigh (
	i int identity  (2141483647,10) not null,
	j char(10) default('foo') not null
);
GO
IF OBJECT_ID('IdentityNegative') IS NULL
create table dbo.IdentityNegative (
	i int identity  (-2041483647,10) not null,
	j char(10) default('foo') not null
);
GO

--Create identity tables near the end of ranges
IF OBJECT_ID('IdentityBigIntHigh') IS NULL
create table dbo.IdentityBigIntHigh (
	i bigint identity  (9223372036854775806,10) not null,
	j char(10) default('foo') not null
);
GO
IF OBJECT_ID('IdentityBigIntNegative') IS NULL
create table dbo.IdentityBigIntNegative (
	i bigint identity  (-9223372036854775806,10) not null,
	j char(10) default('foo') not null
);
GO

--create table with all but one column nullable
--Also make it all varchar/nvarchar except one column
IF OBJECT_ID('AddictedToNullsAndAllCharVarchar') IS NULL
create table dbo.AddictedToNullsAndAllCharVarchar (
	i int identity primary key,
	j varchar(512),
	k varchar(512),
	l varchar(512),
	createdate varchar(512),
	myguid nvarchar(512)
);
GO


IF OBJECT_ID('AllLob') IS NULL
create table dbo.AllLob (
	i int identity primary key,
	j xml,
	k varchar(max),
	l varchar(8000)
);
GO


--createHEAP
IF OBJECT_ID('Heapy') IS NULL
create table dbo.Heapy (
	j varchar(512) default 'foo'
);
GO

INSERT dbo.Heapy DEFAULT VALUES
GO 100

DELETE FROM Heapy;

----------------------------------------
-- TEST
----------------------------------------

EXEC dbo.sp_BlitzIndex @databasename='AdventureWorks';
exec sp_BlitzIndex @TableName='Tally'

EXEC master.dbo.sp_BlitzIndex @databasename='AdventureWorks', @filter=1;
EXEC master.dbo.sp_BlitzIndex @databasename='AdventureWorks', @filter=2;

EXEC master.dbo.sp_BlitzIndex @databasename='AdventureWorks', @mode=1;
EXEC master.dbo.sp_BlitzIndex @databasename='AdventureWorks', @mode=2;
EXEC master.dbo.sp_BlitzIndex @databasename='AdventureWorks', @mode=3;
--GO
EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks', @schemaname=	'dbo', @tablename='OrdersDaily';
EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks', @schemaname=	'Production', @tablename='Product';
--GO

----Indexed view
EXEC dbo.sp_BlitzIndex @databasename='AdventureWorks', @schemaname='Production', @tablename='vProductAndDescription';


--Duplicate indexes against
------Person.Address.ixPersonAddress_DUPLICATEINDEX (6)
------Person.Address.ixPersonAddress_DUPLICATEINDEX2 (7)
--Borderline duplicates against
--Person.Address.ixPersonAddress_BORDERLINEDUPLICATEINDEX (9)
--Person.Address.ixPersonAddress_DUPLICATEINDEX3 (8)
--16 multi-column CX
--One Non-uniqueCX (with 8 reads)
--Two hypothetical indexes
--Two disabled indexes
--Self Loathing Indexes: Heaps with forwarded records or deletes
----0 forwarded fetches, 200000 deletes against heap:dbo.Tally (0)
--Columnstore index: Purchasing.Vendor.PurchasingVendor_COLUMNSTORE (3)
--Compressed indexes:
----dbo.OrdersDaily.NCOrderIdOrdersDaily (2). COMPRESSION:  NONE, PAGE, NONE, NONE
----Sales.SalesPerson.PK_SalesPerson_BusinessEntityID (1). COMPRESSION:  ROW
----Sales.SalesPerson.AK_SalesPerson_rowguid (2). COMPRESSION:  PAGE
-- Abnormal Psychology: Non-Aligned index on a partitioned table
	


--Test a lot of partitions
--You have to connect to just a 2012 instance to do this one
--This will take around 15 seconds. That's not awesome, but it's better than 1 minute.
EXEC dbo.sp_BlitzIndex @databasename='Partition5000';
EXEC dbo.sp_BlitzIndex @databasename='Partition5000', @schemaname='dbo', @tablename='OrdersDaily';

--dbo.sp_WhoIsActive @get_outer_command=1