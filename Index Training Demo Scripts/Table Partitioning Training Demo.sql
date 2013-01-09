/*******************************
SETUP: CREATE AND USE THE DATABASE
This blows away the db if  it already exists.
*******************************/

SET NOCOUNT ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF db_id('PartitionThis') IS NOT NULL 
BEGIN
	USE master; 
	ALTER DATABASE [PartitionThis] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [PartitionThis];

END 

CREATE DATABASE [PartitionThis]
GO

ALTER DATABASE [PartitionThis]
	MODIFY FILE ( NAME = N'PartitionThis', SIZE = 256MB , MAXSIZE = 1GB , FILEGROWTH = 256MB );
ALTER DATABASE [PartitionThis]	
	MODIFY FILE ( NAME = N'PartitionThis_log', SIZE = 256MB , FILEGROWTH = 128MB );
GO

USE PartitionThis
GO









/*******************************
SETUP: CREATE HELPER OBJECTS
*******************************/

--Create a schema for "partition helper" objects
CREATE SCHEMA ph AUTHORIZATION dbo
GO



--Create a view to see partition information by filegroup
CREATE VIEW ph.FileGroupDetail
AS
    SELECT  pf.name AS pf_name ,
            ps.name AS partition_scheme_name ,
            p.partition_number ,
            ds.name AS partition_filegroup ,
            pf.type_desc AS pf_type_desc ,
            pf.fanout AS pf_fanout ,
            pf.boundary_value_on_right ,
            OBJECT_NAME(si.object_id) AS object_name ,
            rv.value AS range_value ,
            SUM(CASE WHEN si.index_id IN ( 1, 0 ) THEN p.rows
                     ELSE 0
                END) AS num_rows ,
            SUM(dbps.reserved_page_count) * 8 / 1024. AS reserved_mb_all_indexes ,
            SUM(CASE ISNULL(si.index_id,0) WHEN 0 THEN 0 ELSE 1 END) AS num_indexes
    FROM    SYS.destination_data_spaces AS dds
    JOIN    sys.data_spaces AS ds
            ON dds.data_space_id = ds.data_space_id
    JOIN    sys.partition_schemes AS ps
            ON dds.partition_scheme_id = ps.data_space_id
    JOIN    sys.partition_functions AS pf
            ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values AS rv
            ON pf.function_id = rv.function_id
               AND dds.destination_id = CASE pf.boundary_value_on_right
                                          WHEN 0 THEN rv.boundary_id
                                          ELSE rv.boundary_id + 1
                                        END
    LEFT JOIN sys.indexes AS si
            ON dds.partition_scheme_id = si.data_space_id
    LEFT JOIN sys.partitions AS p
            ON si.object_id = p.object_id
               AND si.index_id = p.index_id
               AND dds.destination_id = p.partition_number
    LEFT JOIN sys.dm_db_partition_stats AS dbps
            ON p.object_id = dbps.object_id
               AND p.partition_id = dbps.partition_id
    GROUP BY ds.name ,
            p.partition_number ,
            pf.name ,
            pf.type_desc ,
            pf.fanout ,
            pf.boundary_value_on_right ,
            ps.name ,
            si.object_id ,
            rv.VALUE
GO	








--Create a view to see partition information by object
CREATE VIEW ph.ObjectDetail
AS
    SELECT  SCHEMA_NAME(so.schema_id) AS schema_name ,
            OBJECT_NAME(p.object_id) AS object_name ,
            p.partition_number ,
            p.data_compression_desc ,
            dbps.row_count ,
            dbps.reserved_page_count * 8 / 1024. AS reserved_mb ,
            si.index_id ,
            CASE WHEN si.index_id = 0 THEN '(heap!)'
                 ELSE si.name
            END AS index_name ,
            si.is_unique ,
            si.data_space_id ,
            mappedto.name AS mapped_to_name ,
            mappedto.type_desc AS mapped_to_type_desc ,
            partitionds.name AS partition_filegroup ,
            pf.name AS pf_name ,
            pf.type_desc AS pf_type_desc ,
            pf.fanout AS pf_fanout ,
            pf.boundary_value_on_right ,
            ps.name AS partition_scheme_name ,
            rv.value AS range_value
    FROM    SYS.partitions p
    JOIN    sys.objects so
            ON p.object_id = so.object_id
               AND so.is_ms_shipped = 0
    LEFT JOIN sys.dm_db_partition_stats AS dbps
            ON p.object_id = dbps.object_id
               AND p.partition_id = dbps.partition_id
    JOIN    sys.indexes si
            ON p.object_id = si.object_id
               AND p.index_id = si.index_id
    LEFT JOIN sys.data_spaces mappedto
            ON si.data_space_id = mappedto.data_space_id
    LEFT JOIN sys.destination_data_spaces dds
            ON si.data_space_id = dds.partition_scheme_id
               AND p.partition_number = dds.destination_id
    LEFT JOIN sys.data_spaces partitionds
            ON dds.data_space_id = partitionds.data_space_id
    LEFT JOIN sys.partition_schemes AS ps
            ON dds.partition_scheme_id = ps.data_space_id
    LEFT JOIN sys.partition_functions AS pf
            ON ps.function_id = pf.function_id
    LEFT JOIN sys.partition_range_values AS rv
            ON pf.function_id = rv.function_id
               AND dds.destination_id = CASE pf.boundary_value_on_right
                                          WHEN 0 THEN rv.boundary_id
                                          ELSE rv.boundary_id + 1
                                        END

GO










--Create a tally table, we'll use this to populate rows
--This method from Itzik Ben-Gan
;WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), --2 rows
	Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),--4 rows
    Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),--16 rows
    Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),--256 rows
    Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),--65536 rows
    Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),--4,294,967,296 rows
    Tally AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
SELECT  N
INTO    ph.Tally
FROM    Tally
WHERE   N <= 100000;
    










/******************
CREATE OUR HERO, THE PARTITION FUNCTION
Daily Example: RIGHT bound partition function with three boundary points.
Cool point: It can use variables and functions
******************/

--Create the partition function: dailyPF
--Start out 3 days ago
DECLARE @StartDay DATE=DATEADD(dd,-2,CAST(SYSDATETIME() AS DATE));
CREATE PARTITION FUNCTION DailyPF (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (@StartDay, DATEADD(dd,1,@StartDay), DATEADD(dd,2,@StartDay) );
GO



--When typing dates to create a partition, use ODBC standard date format 









--Here's how we see the partition function
SELECT name,type_desc, fanout, boundary_value_on_right, create_date 
FROM sys.partition_functions











/******************
SET UP SOME FILEGROUPS FOR OUR PARTITIONS TO LIVE ON.
In production they'd be on different drives with the appropriate RAID and spindles.
******************/

--Add filegroups.
--We need FOUR filegroups for THREE boundary points
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG1
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG2
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG3
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG4
GO 





/******************
CREATE THE PARTITION SCHEME
This will map the filegroups to the partition function.
******************/

--Create the partition scheme: dailyPS 
CREATE PARTITION SCHEME DailyPS 
	AS PARTITION DailyPF
	TO (DailyFG1, DailyFG2, DailyFG3, DailyFG4);










--Look at how this is mapped out now
SELECT *
FROM ph.FileGroupDetail













/******************
CREATE OBJECTS ON THE PARTITION SCHEME
******************/

--Create a partitioned heap... yep, you can do that!
--When would a partitioned heap be useful?
--What could go wrong with a partitioned heap?
if OBJECT_ID('OrdersDaily','U') is null
CREATE TABLE OrdersDaily (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on dailyPS(OrderDate)
GO












--Where would records go for different days?
--You can use the $PARTITION function
SELECT $PARTITION.DailyPF( DATEADD(dd,-7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberSevenDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-3,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberThreeDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-2,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTwoDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTomorrow,
	$PARTITION.DailyPF( DATEADD(dd,7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberNextWeek









--Let's insert some rows for three days ago 
/*
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(mi, t.N, DATEADD(dd,-3,CAST(SYSDATETIME() AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Robot' WHEN t.N % 4 = 0 THEN 'Badger'  ELSE 'Pen' END AS OrderName
FROM ph.tally AS t
WHERE N < = 100

*/	

	






-- What went wrong???
-- We didn't add any files to our filegroups.... SAD TROMBONE.
--Figure out what path the database is on
DECLARE @path NVARCHAR(256), @i TINYINT=1, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) FROM sys.database_files WHERE name='PartitionThis'

WHILE @i <= 4 
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql
	SET @i+=1
END













--OK-- let's insert some records again.
--Three days ago = 1000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-3,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Robot' WHEN t.N % 4 = 0 THEN 'Badger'  ELSE 'Pen' END AS OrderName
FROM ph.Tally AS t
WHERE N < = 1000
	
--Two days ago = 2000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Flying Monkey' WHEN t.N % 4 = 0 THEN 'Junebug'  ELSE 'Pen' END AS OrderName
FROM ph.Tally AS t
WHERE N < = 2000

--Yesterday= 3000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 2 = 0 THEN 'Turtle' WHEN t.N % 5 = 0 THEN 'Eraser'  ELSE 'Pen' END AS OrderName
FROM ph.Tally AS t
WHERE N < = 3000

--Today=  4000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Lasso' WHEN t.N % 2 = 0 THEN 'Cattle Prod'  ELSE 'Pen' END AS OrderName
FROM ph.Tally AS t
WHERE N < = 4000








--Now look at our heap
SELECT *
FROM ph.ObjectDetail
WHERE object_name='OrdersDaily'
order by partition_number











/******************
Let's add some indexes ....
******************/

--Add a Clustered Index-- Not a heap anymore
--Note that this isn't unique
ALTER TABLE OrdersDaily
ADD CONSTRAINT PKOrdersDaily
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO









--An aligned NCI
--We don't have to specify the partition function.
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily ON OrdersDaily(OrderId)
GO










--An NCI that is NOT aligned
CREATE NONCLUSTERED INDEX NCOrderNameOrdersDailyNonAligned ON OrdersDaily(OrderName) ON [PRIMARY]
GO












--Look at the CI and NCs
SELECT partition_number, row_count, range_value, reserved_mb, index_id, index_name,mapped_to_name,mapped_to_type_desc, partition_filegroup, pf_name
FROM ph.ObjectDetail
WHERE object_name='OrdersDaily'
order by index_name, partition_number
--compare to:
EXEC sp_helpindex OrdersDaily













/*
Look at secret columns...
OrderDate is added to the aligned index NCOrderIdOrdersDaily, which is only on OrderId.
It is not part of the key, nor is it an included column.
What does this tell us about uniqueness?
*/
SELECT
	CASE WHEN ic.key_ordinal=0 THEN '***Not In Key, Look at Me!***' ELSE '' END AS Note,
	i.name as indexName,
	i.index_id as indexId,
	i.type_desc as typeDesc,
	c.name as columnName,
	ic.key_ordinal as keyOrdinal, --0 is non-key
	ic.partition_ordinal as partitionOrdinal,
	ic.is_included_column as isIncludedCol
FROM sys.indexes AS i
JOIN sys.index_columns AS ic on
	i.object_id=ic.object_id
	AND i.index_id=ic.index_id
JOIN sys.columns as c on
	ic.object_id=c.object_id
	and ic.column_id=c.column_id
where OBJECT_NAME(i.object_id)='OrdersDaily'














/******************
SWITCHING IN NEW PARTITIONS...
******************/

--I have three right partition boundaries currently
--I want to load data for tomorrow and then switch it in.
--First, add a filegroup.
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG5






--Add a file for the filegroup.
DECLARE @path NVARCHAR(256), @i TINYINT=5, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) FROM sys.database_files WHERE name='PartitionThis'
WHILE @i = 5
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql
	SET @i+=1
END









--In production, you should pre-create FGs and partitions for future days--
--Here, it's all done at once for demonstration.









--Create a staging table on our new filegroup (dailyFG5)
--Why are we seeding the identity here?
--What would happen if we didn't?
CREATE TABLE OrdersDailyLoad (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY (10001,1) NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [dailyFG5]
GO












--Insert some records into our staging table
--Tomorrow=5000 rows
INSERT OrdersDailyLoad(OrderDate, OrderName) 
SELECT DATEADD(mi, t.N, DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Bow and Arrow' WHEN t.N % 2 = 0 THEN 'First Aid Kit'  ELSE 'Pen' END AS OrderName
FROM ph.Tally AS t
WHERE N < = 5000
GO












--Create indexes on our staging table
ALTER TABLE OrdersDailyLoad
ADD CONSTRAINT PKOrdersDailyLoad
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO







--Create the aligned NC as well. It can have a different name.
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDailyLoad ON OrdersDailyLoad(OrderId)
GO









--Create a check constraint on the staging table.
--This will ensure data is consistent with the  partition it's being switched into.

DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad
CHECK (OrderDate >= ''' + convert(CHAR(10),DATEADD(dd,1,CAST(SYSDATETIME() AS DATE))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0)
--Run it
EXEC sp_executesql @tsql









--What would happen if you inserted some rows into OrdersDaily itself for the day you're loading?










--Set our new filegroup as 'Next used' in our partition scheme
--This is how you add it to the partition scheme
ALTER PARTITION SCHEME DailyPS
NEXT USED DailyFG5

--This means DailyFG5 will receive any additional partition of a partitioned table or index as a result of an ALTER PARTITION FUNCTION statement.










--Examine our partition function with assocated scheme, filegroups, and boundary points
SELECT *
FROM ph.FileGroupDetail










--Add a new boundary  point to our partition. 
ALTER PARTITION FUNCTION DailyPF() 
SPLIT RANGE (DATEADD(dd,1,CAST(SYSDATETIME() AS DATE)))
GO


--If you don't add a filegroup to the partition scheme first with NEXT USED, you'll get the error:
--Msg 7707, Level 16, State 1, Line 2
--The associated partition function 'DailyPF' generates more partitions 
--than there are file groups mentioned in the scheme 'DailyPS'.

--But note that you *CAN* use a FileGroup for  more than one partition
--To do this,  you just set an existing one with NEXT USED.








--Now check the partition function and object-- what's different?
SELECT *
FROM ph.FileGroupDetail

SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number









--Switch in!
/*
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 5
*/












--Uh oh...
--We must disable (or drop) this non-aligned index to make switching work
ALTER INDEX NCOrderNameOrdersDailyNonAligned ON OrdersDaily DISABLE
GO







--Switch in!
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 5












-- Let's look at our partitioned table and loading table now...
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number







--Let's go ahead and drop the staging table
DROP TABLE OrdersDailyLoad




/******************
SWITCHING OUT OLD DATA
******************/
--I have four right partition boundaries currently
--I want to switch out my oldest data


--Look at how this is mapped out now. 
--We want to get rid of our oldest 100 rows.
SELECT *
FROM ph.FileGroupDetail





--Create a staging table to hold switched out data 
--PUT THIS ON THE SAME FILEGROUP YOU'RE SWITCHING OUT OF
CREATE TABLE OrdersDailyOut (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [dailyFG1]
GO



--Create the primary key our switch out table
ALTER TABLE OrdersDailyOut
ADD CONSTRAINT PKOrdersDailyOut
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO



--Switch OUT!
RAISERROR ('Switching out.',0,0)
ALTER TABLE OrdersDaily
SWITCH PARTITION 1 TO OrdersDailyOUT



--Look at our switch OUT table
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyOut')
ORDER BY object_name DESC, partition_number





/*****
Note: when switching out to an empty table, we needed to add the clustered index. 
However, we did NOT need to add NCIs or a check constraint
****/




--Programmatically find the boundary point to merge
--This is done so we don't have to hard code dates in the script
DECLARE @MergeBoundaryPoint DATETIME2(0), @msg NVARCHAR(2000);
SELECT @MergeBoundaryPoint = CAST(MIN(rv.value) AS DATETIME2(0))
FROM sys.partition_functions  pf
JOIN sys.partition_range_values rv ON pf.function_id=rv.function_id
where pf.name='DailyPF'

IF (
	SELECT COUNT(*)
	FROM dbo.OrdersDaily
	WHERE orderDate < @MergeBoundaryPoint
) =0
BEGIN
	SET @msg='No records found, merging boundary point ' + CAST(@MergeBoundaryPoint AS CHAR(10)) + '.'
	RAISERROR (@msg,0,0)
	ALTER PARTITION FUNCTION DailyPF ()
		MERGE RANGE ( @MergeBoundaryPoint )
END
ELSE
BEGIN
	SET @msg='ERROR: Records exist below boundary point ' + CAST(@MergeBoundaryPoint AS CHAR(10)) + '. Not merging.'
	RAISERROR (@msg,16,1)
END





--What would happen if we didn't have a safety to make sure there were no records?





--Look at how this is mapped out after switch-out.
SELECT *
FROM ph.FileGroupDetail





--Let's go ahead and drop the switch OUT table
DROP TABLE OrdersDailyOUT





/******************
PARTITION ELIMINATION
******************/
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
--CTRL + M  for execution plans

--We do 76 logical reads to read the whole table
SELECT * 
FROM ordersDaily







--Here's basic partition elimination
--It does 20 reads to access everything in partition 3
SELECT OrderDate, OrderId, OrderName, $PARTITION.DailyPF(OrderDate) AS PartitionNumber
FROM dbo.OrdersDaily
WHERE $PARTITION.DailyPF(OrderDate)=3







--Can you see partition elimination in estimated plans?







--Why does this one do so many reads? (It should only need to do three logical reads for this partition.)
--Why does the plan look the way it does?
DECLARE @d DATETIME=dateadd(dd,-2,getdate())
SELECT OrderDate, OrderId, OrderName, $PARTITION.DailyPF(OrderDate) AS PartitionNumber
FROM dbo.OrdersDaily
WHERE OrderDate >= @d AND OrderDate < DATEADD(dd,1,@d)







--We know *this* is bad because of the function.
--Look at the reads and the comparative cost.
DECLARE @d2 DATE=dateadd(dd,-2,getdate())
SELECT OrderDate, OrderId, OrderName, $PARTITION.DailyPF(OrderDate) AS PartitionNumber
FROM dbo.OrdersDaily
WHERE cast(OrderDate AS DATE) = @d2








--This is better. 
--Why is that?
DECLARE @d3 DATETIME2(0)=dateadd(dd,-2,getdate()),
	@d4 DATETIME2(0) = dateadd(dd,-1,getdate())
SELECT OrderDate, OrderId, OrderName, $PARTITION.DailyPF(OrderDate) AS PartitionNumber
FROM dbo.OrdersDaily
WHERE OrderDate >= @d3 and OrderDate < @d4






--This looks perfect.
DECLARE @d5 DATETIME2(0)=dateadd(dd,-2,cast(getdate()as date))
declare	@d6 DATETIME2(0);
set @d6 = dateadd(ss,-1, dateadd(dd,1,@d5));
SELECT OrderDate, OrderId, OrderName, $PARTITION.DailyPF(OrderDate) AS PartitionNumber
FROM dbo.OrdersDaily
WHERE OrderDate between @d5 and @d6


select * from ordersdaily







/************************
What if this table wasn't clustered?
************************/
CREATE TABLE dbo.OrdersDaily_NP (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [PRIMARY]
GO

INSERT dbo.OrdersDaily_NP (OrderDate, OrderId, OrderName)
SELECT OrderDate, OrderId, OrderName
FROM dbo.OrdersDaily
ORDER BY OrderId

--Add Indexes
ALTER TABLE OrdersDaily_NP
ADD CONSTRAINT PKOrdersDaily_NP
	PRIMARY KEY CLUSTERED(OrderDate, OrderId)
GO
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily_NP ON OrdersDaily_NP(OrderId)
GO


--Compare the tables
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDaily_NP')

SELECT OrderDate OrderId, OrderName
FROM ordersDaily

SELECT OrderDate OrderId, OrderName
FROM ordersDaily_NP




/******************
PARTITIONING EXISTING OBJECTS	
******************/

--Say that some orders  have related notes
--When those exist, we store them in another table.
--Here's our first stab at the table. It's not partitioned.

CREATE TABLE dbo.OrdersDailyNotes_NP (
	OrderId INT NOT NULL,
	OrderNote nvarchar(256) NOT NULL
) on [PRIMARY]
GO


INSERT dbo.OrdersDailyNotes_NP (OrderId, OrderNote)
SELECT OrderId, 'Customer sure likes ' + OrderName
FROM dbo.OrdersDaily
WHERE OrderId % 2 = 0
ORDER BY OrderId


--Add Clustered Index
ALTER TABLE dbo.OrdersDailyNotes_NP
ADD CONSTRAINT PKOrdersDailyNotes_NP
	PRIMARY KEY CLUSTERED(OrderId)
GO

/*****
Now we'll create a new version of dbo.OrdersDailyNotes
We'll use our existing partition function and scheme.
Note: in production we would create a new partition scheme.
*/

CREATE TABLE dbo.OrdersDailyNotes (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId INT NOT NULL,
	OrderNote nvarchar(256) NOT NULL
) on [PRIMARY]
GO


INSERT dbo.OrdersDailyNotes (OrderDate, OrderId, OrderNote)
SELECT OrderDate, OrderId, 'Customer sure likes ' + OrderName
FROM dbo.OrdersDaily
WHERE OrderId % 2 = 0
ORDER BY OrderId


--Add Clustered Index and simultaneously move to the existing filegroups
ALTER TABLE dbo.OrdersDailyNotes
ADD CONSTRAINT PKOrdersDailyNotes
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
ON DailyPS(OrderDate)
GO

--Look at how this is now organized
SELECT *
FROM ph.FileGroupDetail


--Compare ExecutionPlans
SELECT  od.OrderId, od.OrderDate, odn.OrderNote
FROM    dbo.OrdersDaily od
JOIN    dbo.OrdersDailyNotes odn
        ON od.OrderDate = odn.OrderDate
           AND od.orderId = odn.OrderID
WHERE od.orderDate BETWEEN CAST('2011-05-09 00:02:00' AS DATETIME2(0))
	AND CAST('2011-05-09 00:03:00' AS DATETIME2(0))

SELECT  od.OrderId, od.OrderDate, odn.OrderNote
FROM    dbo.OrdersDaily od
JOIN    dbo.OrdersDailyNotes_NP odn
        ON od.orderId = odn.OrderID
WHERE od.orderDate BETWEEN CAST('2011-05-09 00:02:00' AS DATETIME2(0))
	AND CAST('2011-05-09 00:03:00' AS DATETIME2(0))


--We don't have enough rows/high enough cost to show join co-location


/******************
--Repro the top/max bug
--See http://connect.microsoft.com/SQLServer/feedback/details/240968/partition-table-using-min-max-functions-and-top-n-index-selection-and-performance
*************************/



--This does an NCI index seek and a Top N Sort
--It has a scan count of 4/ accessed four partitions, did 32 logical reads 
SELECT TOP 20 OrderID
FROM dbo.ordersdaily
WHERE orderId < 14589
ORDER BY orderId desc

--Normal/ non-partitioned
--This does an NCI index seek and a TOP
--This has a scan count of 1 and did 2 logical reads.
SELECT TOP 20 OrderID
FROM dbo.ordersdaily_NP
WHERE orderId < 14589
ORDER BY orderId desc


