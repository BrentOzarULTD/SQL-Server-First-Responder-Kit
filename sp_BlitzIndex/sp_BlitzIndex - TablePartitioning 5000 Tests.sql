--******************
--Copyright 2013, Brent Ozar PLF, LLC DBA Brent Ozar Unlimited.
--******************

--******************
--Don't just run the whole thing.
--Run this step by step to learn!
--******************
--DECLARE @msg NVARCHAR(MAX);
--SET @msg = N'Did you mean to run this whole script?' + CHAR(10)
--    + N'MAKE SURE YOU ARE RUNNING AGAINST A TEST ENVIRONMENT ONLY!'

--RAISERROR(@msg,20,1) WITH LOG;
--GO



--******************
--1. CREATE OUR DEMO DATABASE
--Blow it away if it already exists
--******************
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF db_id('Partition5000') IS NOT NULL 
BEGIN
	USE master; 
	ALTER DATABASE [Partition5000] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [Partition5000];
END 
GO

CREATE DATABASE [Partition5000]
GO

ALTER DATABASE [Partition5000]
	MODIFY FILE ( NAME = N'Partition5000', SIZE = 256MB , MAXSIZE = 10GB , FILEGROWTH = 512MB );
ALTER DATABASE [Partition5000]	
	MODIFY FILE ( NAME = N'Partition5000_log', SIZE = 128MB , FILEGROWTH = 128MB );
GO

USE Partition5000;
GO






--*******************************
--2 CREATE HELPER OBJECTS
--Why do we need these?
--Do they HAVE to be in the database with the partitioned objects?
--*******************************

--Create a schema for "partition helper" objects
CREATE SCHEMA [ph] AUTHORIZATION dbo;
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
        SUM(CASE ISNULL(si.index_id, 0)
                WHEN 0 THEN 0
                ELSE 1
            END) AS num_indexes
FROM    sys.destination_data_spaces AS dds
        JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
        JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
        JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
        LEFT JOIN sys.partition_range_values AS rv ON pf.function_id = rv.function_id
                                                        AND dds.destination_id = CASE pf.boundary_value_on_right
                                                                                    WHEN 0 THEN rv.boundary_id
                                                                                    ELSE rv.boundary_id + 1
                                                                                END
        LEFT JOIN sys.indexes AS si ON dds.partition_scheme_id = si.data_space_id
        LEFT JOIN sys.partitions AS p ON si.object_id = p.object_id
                                            AND si.index_id = p.index_id
                                            AND dds.destination_id = p.partition_number
        LEFT JOIN sys.dm_db_partition_stats AS dbps ON p.object_id = dbps.object_id
                                                        AND p.partition_id = dbps.partition_id
GROUP BY ds.name ,
        p.partition_number ,
        pf.name ,
        pf.type_desc ,
        pf.fanout ,
        pf.boundary_value_on_right ,
        ps.name ,
        si.object_id ,
        rv.value;
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
FROM    sys.partitions p
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


--Create a tally table with ~4 million rows, we'll use this to populate test tables
--This general method attributed to Itzik Ben-Gan
;WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), 
	Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),
	Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),
	Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),
	Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),
	Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),
	tally AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
SELECT  N
INTO    ph.tally
FROM    tally
WHERE   N <= 100000;
GO







--******************
--3. CREATE OUR HERO, THE PARTITION FUNCTION
--Daily Example: RIGHT bound partition function 
--Cool point: It can use variables and functions
--******************

--13 years * 365= 4745 days
DECLARE @StartDay DATETIME2(0)='1/1/2004',
	@EndDay DATETIME2(0)='1/1/2015'

--Create the partition function: dailyPF
--Just one boundary point
CREATE PARTITION FUNCTION DailyPF (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (@StartDay);
--now add the other 4744 boundary points
--This is just done this way out of laziness--
--It's quicker to code.

WHILE @StartDay <= @EndDay
BEGIN
	SET @StartDay=DATEADD(dd,1,@StartDay);

	ALTER PARTITION FUNCTION DailyPF()
		SPLIT RANGE (@StartDay)
END
GO



----Here's how we see the partition function
--SELECT name,type_desc, fanout, boundary_value_on_right, create_date 
--FROM sys.partition_functions;
--GO


--******************
--CREATE THE PARTITION SCHEME
--This maps the filegroups to the partition function.
--******************

--Create the partition scheme: dailyPS 
CREATE PARTITION SCHEME DailyPS 
	AS PARTITION DailyPF
	ALL TO ([PRIMARY]);

--Look at how this is mapped out now
--SELECT *
--FROM ph.FileGroupDetail;
--GO



--******************
--CREATE OBJECTS ON THE PARTITION SCHEME
--******************

--Create a partitioned heap... yep, you can do that!
--When would a partitioned heap be useful?
--What could go wrong with a partitioned heap?
if OBJECT_ID('OrdersDaily','U') is null
CREATE TABLE OrdersDaily (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on DailyPS(OrderDate)
GO


--******************
--LET'S ADD SOME INDEXES ....
--******************
--Add a Clustered Index-- Not a heap anymore
ALTER TABLE OrdersDaily
ADD CONSTRAINT PKOrdersDaily
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO



--******************
--INSERT ROWS
--******************
--Adds a variable number of rows based on the ordinal of the day in the month 
--63,208 rows
INSERT dbo.OrdersDaily (OrderDate,  OrderName)
SELECT DATEADD(MI,t.N,CAST(range_value AS DATETIME2(0))), 
	CASE WHEN DATEPART(dd,CAST(range_value AS DATETIME2(0))) % 3 = 0 THEN 'Robot' 
		WHEN DATEPART(dd,CAST(range_value AS DATETIME2(0))) % 5 = 0 THEN 'Badger'  
		ELSE 'Pen' END AS OrderName
FROM ph.FileGroupDetail fg
JOIN ph.tally t ON  DATEPART(dd,CAST(range_value AS DATETIME2(0))) >= t.N
WHERE pf_name='DailyPF'
AND range_value IS NOT NULL
;



--Compress one partition.
ALTER INDEX [PKOrdersDaily] ON dbo.OrdersDaily REBUILD PARTITION=2 WITH (DATA_COMPRESSION=ROW)




--An aligned NCI
--We don't have to specify the partition function.
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily 
	ON OrdersDaily(OrderId)
GO


--An NCI that is NOT aligned
CREATE NONCLUSTERED INDEX NCOrderNameOrdersDailyNonAligned 
	ON OrdersDaily(OrderName) ON [PRIMARY]
GO

--Look at the CI and NCs
SELECT partition_number, row_count, range_value, reserved_mb, 
	index_id, index_name,mapped_to_name,mapped_to_type_desc, partition_filegroup, pf_name
FROM ph.ObjectDetail
WHERE object_name='OrdersDaily'
order by index_name, partition_number
--compare to:
EXEC sp_helpindex OrdersDaily


SELECT * FROM ph.ObjectDetail	


