SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('dbo.sp_BlitzIndex') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzIndex AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_BlitzIndex
    @DatabaseName NVARCHAR(128) = NULL, /*Defaults to current DB if not specified*/
    @SchemaName NVARCHAR(128) = NULL, /*Requires table_name as well.*/
    @TableName NVARCHAR(128) = NULL,  /*Requires schema_name as well.*/
    @Mode TINYINT=0, /*0=Diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail, 4=Diagnose Details*/
        /*Note:@Mode doesn't matter if you're specifying schema_name and @TableName.*/
    @Filter TINYINT = 0, /* 0=no filter (default). 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB */
        /*Note:@Filter doesn't do anything unless @Mode=0*/
    @SkipPartitions BIT	= 0,
    @SkipStatistics BIT	= 1,
    @GetAllDatabases BIT = 0,
	@ShowColumnstoreOnly BIT = 0, /* Will show only the Row Group and Segment details for a table with a columnstore index. */
    @BringThePain BIT = 0,
    @IgnoreDatabases NVARCHAR(MAX) = NULL, /* Comma-delimited list of databases you want to skip */
    @ThresholdMB INT = 250 /* Number of megabytes that an object must be before we include it in basic results */,
	@OutputType VARCHAR(20) = 'TABLE' ,
    @OutputServerName NVARCHAR(256) = NULL ,
    @OutputDatabaseName NVARCHAR(256) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
	@IncludeInactiveIndexes BIT = 0 /* Will skip indexes with no reads or writes */,
    @ShowAllMissingIndexRequests BIT = 0 /*Will make all missing index requests show up*/,
	@ShowPartitionRanges BIT = 0 /* Will add partition range values column to columnstore visualization */,
	@SortOrder NVARCHAR(50) = NULL, /* Only affects @Mode = 2. */
	@SortDirection NVARCHAR(4) = 'DESC', /* Only affects @Mode = 2. */
    @Help TINYINT = 0,
	@Debug BIT = 0,
    @Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
WITH RECOMPILE
AS
SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT @Version = '8.10', @VersionDate = '20220718';
SET @OutputType  = UPPER(@OutputType);

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

IF @Help = 1 
BEGIN
PRINT '
/*
sp_BlitzIndex from http://FirstResponderKit.org
	
This script analyzes the design and performance of your indexes.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
 - Index create statements are just to give you a rough idea of the syntax. It includes filters and fillfactor.
 --        Example 1: index creates use ONLINE=? instead of ONLINE=ON / ONLINE=OFF. This is because it is important 
           for the user to understand if it is going to be offline and not just run a script.
 --        Example 2: they do not include all the options the index may have been created with (padding, compression
           filegroup/partition scheme etc.)
 --        (The compression and filegroup index create syntax is not trivial because it is set at the partition 
           level and is not trivial to code.)
 - Does not advise you about data modeling for clustered indexes and primary keys (primarily looks for signs of insanity.)

Unknown limitations of this version:
 - We knew them once, but we forgot.


MIT License

Copyright (c) 2021 Brent Ozar Unlimited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
';
RETURN;
END;    /* @Help = 1 */

DECLARE @ScriptVersionName NVARCHAR(50);
DECLARE @DaysUptime NUMERIC(23,2);
DECLARE @DatabaseID INT;
DECLARE @ObjectID INT;
DECLARE @dsql NVARCHAR(MAX);
DECLARE @params NVARCHAR(MAX);
DECLARE @msg NVARCHAR(4000);
DECLARE @ErrorSeverity INT;
DECLARE @ErrorState INT;
DECLARE @Rowcount BIGINT;
DECLARE @SQLServerProductVersion NVARCHAR(128);
DECLARE @SQLServerEdition INT;
DECLARE @FilterMB INT;
DECLARE @collation NVARCHAR(256);
DECLARE @NumDatabases INT;
DECLARE @LineFeed NVARCHAR(5);
DECLARE @DaysUptimeInsertValue NVARCHAR(256);
DECLARE @DatabaseToIgnore NVARCHAR(MAX);
DECLARE @ColumnList NVARCHAR(MAX);
DECLARE @ColumnListWithApostrophes NVARCHAR(MAX);
DECLARE @PartitionCount INT;
DECLARE @OptimizeForSequentialKey BIT = 0;
DECLARE @StringToExecute NVARCHAR(MAX);


/* Let's get @SortOrder set to lower case here for comparisons later */
SET @SortOrder = REPLACE(LOWER(@SortOrder), N' ', N'_');
SET @SortDirection = LOWER(@SortDirection);

SET @LineFeed = CHAR(13) + CHAR(10);
SELECT @SQLServerProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @SQLServerEdition =CAST(SERVERPROPERTY('EngineEdition') AS INT); /* We default to online index creates where EngineEdition=3*/
SET @FilterMB=250;
SELECT @ScriptVersionName = 'sp_BlitzIndex(TM) v' + @Version + ' - ' + DATENAME(MM, @VersionDate) + ' ' + RIGHT('0'+DATENAME(DD, @VersionDate),2) + ', ' + DATENAME(YY, @VersionDate);
SET @IgnoreDatabases = REPLACE(REPLACE(LTRIM(RTRIM(@IgnoreDatabases)), CHAR(10), ''), CHAR(13), '');

SELECT
    @OptimizeForSequentialKey =
	    CASE WHEN EXISTS
		          (
                      SELECT
                          1/0
                      FROM sys.all_columns AS ac
                      WHERE ac.object_id = OBJECT_ID('sys.indexes')
                      AND   ac.name = N'optimize_for_sequential_key'
				  )
			THEN 1
			ELSE 0
		END;

RAISERROR(N'Starting run. %s', 0,1, @ScriptVersionName) WITH NOWAIT;
																					
IF(@OutputType NOT IN ('TABLE','NONE'))
BEGIN
    RAISERROR('Invalid value for parameter @OutputType. Expected: (TABLE;NONE)',12,1);
    RETURN;
END;
                       
IF(@OutputType = 'NONE')
BEGIN
    IF(@OutputTableName IS NULL OR @OutputSchemaName IS NULL OR @OutputDatabaseName IS NULL)
    BEGIN
        RAISERROR('This procedure should be called with a value for @Output* parameters, as @OutputType is set to NONE',12,1);
        RETURN;
    END;
    IF(@BringThePain = 1)
    BEGIN
        RAISERROR('Incompatible Parameters: @BringThePain set to 1 and @OutputType set to NONE',12,1);
        RETURN;
    END;
	/* Eventually limit by mode																			   
    IF(@Mode not in (0,4)) 
	BEGIN
        RAISERROR('Incompatible Parameters: @Mode set to %d and @OutputType set to NONE',12,1,@Mode);
        RETURN;
	END;
	*/
END;

IF OBJECT_ID('tempdb..#IndexSanity') IS NOT NULL 
    DROP TABLE #IndexSanity;

IF OBJECT_ID('tempdb..#IndexPartitionSanity') IS NOT NULL 
    DROP TABLE #IndexPartitionSanity;

IF OBJECT_ID('tempdb..#IndexSanitySize') IS NOT NULL 
    DROP TABLE #IndexSanitySize;

IF OBJECT_ID('tempdb..#IndexColumns') IS NOT NULL 
    DROP TABLE #IndexColumns;

IF OBJECT_ID('tempdb..#MissingIndexes') IS NOT NULL 
    DROP TABLE #MissingIndexes;

IF OBJECT_ID('tempdb..#ForeignKeys') IS NOT NULL 
    DROP TABLE #ForeignKeys;

IF OBJECT_ID('tempdb..#UnindexedForeignKeys') IS NOT NULL 
    DROP TABLE #UnindexedForeignKeys;

IF OBJECT_ID('tempdb..#BlitzIndexResults') IS NOT NULL 
    DROP TABLE #BlitzIndexResults;
        
IF OBJECT_ID('tempdb..#IndexCreateTsql') IS NOT NULL    
    DROP TABLE #IndexCreateTsql;

IF OBJECT_ID('tempdb..#DatabaseList') IS NOT NULL 
    DROP TABLE #DatabaseList;

IF OBJECT_ID('tempdb..#Statistics') IS NOT NULL 
    DROP TABLE #Statistics;

IF OBJECT_ID('tempdb..#PartitionCompressionInfo') IS NOT NULL 
    DROP TABLE #PartitionCompressionInfo;

IF OBJECT_ID('tempdb..#ComputedColumns') IS NOT NULL 
    DROP TABLE #ComputedColumns;
	
IF OBJECT_ID('tempdb..#TraceStatus') IS NOT NULL
	DROP TABLE #TraceStatus;

IF OBJECT_ID('tempdb..#TemporalTables') IS NOT NULL
	DROP TABLE #TemporalTables;

IF OBJECT_ID('tempdb..#CheckConstraints') IS NOT NULL
	DROP TABLE #CheckConstraints;

IF OBJECT_ID('tempdb..#FilteredIndexes') IS NOT NULL
	DROP TABLE #FilteredIndexes;
		
IF OBJECT_ID('tempdb..#Ignore_Databases') IS NOT NULL 
    DROP TABLE #Ignore_Databases

        RAISERROR (N'Create temp tables.',0,1) WITH NOWAIT;
        CREATE TABLE #BlitzIndexResults
            (
              blitz_result_id INT IDENTITY PRIMARY KEY,
              check_id INT NOT NULL,
              index_sanity_id INT NULL,
              Priority INT NULL,
              findings_group NVARCHAR(4000) NOT NULL,
              finding NVARCHAR(200) NOT NULL,
              [database_name] NVARCHAR(128) NULL,
              URL NVARCHAR(200) NOT NULL,
              details NVARCHAR(MAX) NOT NULL,
              index_definition NVARCHAR(MAX) NOT NULL,
              secret_columns NVARCHAR(MAX) NULL,
              index_usage_summary NVARCHAR(MAX) NULL,
              index_size_summary NVARCHAR(MAX) NULL,
              create_tsql NVARCHAR(MAX) NULL,
              more_info NVARCHAR(MAX) NULL,
              sample_query_plan XML NULL
            );

        CREATE TABLE #IndexSanity
            (
              [index_sanity_id] INT IDENTITY PRIMARY KEY CLUSTERED,
              [database_id] SMALLINT NOT NULL ,
              [object_id] INT NOT NULL ,
              [index_id] INT NOT NULL ,
              [index_type] TINYINT NOT NULL,
              [database_name] NVARCHAR(128) NOT NULL ,
              [schema_name] NVARCHAR(128) NOT NULL ,
              [object_name] NVARCHAR(128) NOT NULL ,
              index_name NVARCHAR(128) NULL ,
              key_column_names NVARCHAR(MAX) NULL ,
              key_column_names_with_sort_order NVARCHAR(MAX) NULL ,
              key_column_names_with_sort_order_no_types NVARCHAR(MAX) NULL ,
              count_key_columns INT NULL ,
              include_column_names NVARCHAR(MAX) NULL ,
              include_column_names_no_types NVARCHAR(MAX) NULL ,
              count_included_columns INT NULL ,
              partition_key_column_name NVARCHAR(MAX) NULL,
              filter_definition NVARCHAR(MAX) NOT NULL ,
			  optimize_for_sequential_key BIT NULL,
			  is_indexed_view BIT NOT NULL ,
              is_unique BIT NOT NULL ,
              is_primary_key BIT NOT NULL ,
			  is_unique_constraint BIT NOT NULL ,
			  is_XML bit NOT NULL,
              is_spatial BIT NOT NULL,
              is_NC_columnstore BIT NOT NULL,
              is_CX_columnstore BIT NOT NULL,
              is_in_memory_oltp BIT NOT NULL ,
              is_disabled BIT NOT NULL ,
              is_hypothetical BIT NOT NULL ,
              is_padded BIT NOT NULL ,
              fill_factor SMALLINT NOT NULL ,
              user_seeks BIGINT NOT NULL ,
              user_scans BIGINT NOT NULL ,
              user_lookups BIGINT NOT  NULL ,
              user_updates BIGINT NULL ,
              last_user_seek DATETIME NULL ,
              last_user_scan DATETIME NULL ,
              last_user_lookup DATETIME NULL ,
              last_user_update DATETIME NULL ,
              is_referenced_by_foreign_key BIT DEFAULT(0),
              secret_columns NVARCHAR(MAX) NULL,
              count_secret_columns INT NULL,
              create_date DATETIME NOT NULL,
              modify_date DATETIME NOT NULL,
              filter_columns_not_in_index NVARCHAR(MAX),
            [db_schema_object_name] AS [schema_name] + N'.' + [object_name]  ,
            [db_schema_object_indexid] AS [schema_name] + N'.' + [object_name]
                + CASE WHEN [index_name] IS NOT NULL THEN N'.' + index_name
                ELSE N''
                END + N' (' + CAST(index_id AS NVARCHAR(20)) + N')' ,
            first_key_column_name AS CASE    WHEN count_key_columns > 1
                THEN LEFT(key_column_names, CHARINDEX(',', key_column_names, 0) - 1)
                ELSE key_column_names
                END ,
            index_definition AS 
            CASE WHEN partition_key_column_name IS NOT NULL 
                THEN N'[PARTITIONED BY:' + partition_key_column_name +  N']' 
                ELSE '' 
                END +
                CASE index_id
                    WHEN 0 THEN N'[HEAP] '
                    WHEN 1 THEN N'[CX] '
                    ELSE N'' END + CASE WHEN is_indexed_view = 1 THEN N'[VIEW] '
                    ELSE N'' END + CASE WHEN is_primary_key = 1 THEN N'[PK] '
                    ELSE N'' END + CASE WHEN is_XML = 1 THEN N'[XML] '
                    ELSE N'' END + CASE WHEN is_spatial = 1 THEN N'[SPATIAL] '
                    ELSE N'' END + CASE WHEN is_NC_columnstore = 1 THEN N'[COLUMNSTORE] '
                    ELSE N'' END + CASE WHEN is_in_memory_oltp = 1 THEN N'[IN-MEMORY] '
                    ELSE N'' END + CASE WHEN is_disabled = 1 THEN N'[DISABLED] '
                    ELSE N'' END + CASE WHEN is_hypothetical = 1 THEN N'[HYPOTHETICAL] '
                    ELSE N'' END + CASE WHEN is_unique = 1 AND is_primary_key = 0 AND is_unique_constraint = 0 THEN N'[UNIQUE] '
					ELSE N'' END + CASE WHEN is_unique_constraint = 1 AND is_primary_key = 0 THEN N'[UNIQUE CONSTRAINT] '
                    ELSE N'' END + CASE WHEN count_key_columns > 0 THEN 
                        N'[' + CAST(count_key_columns AS NVARCHAR(10)) + N' KEY' 
                            + CASE WHEN count_key_columns > 1 THEN  N'S' ELSE N'' END
                            + N'] ' + LTRIM(key_column_names_with_sort_order)
                    ELSE N'' END + CASE WHEN count_included_columns > 0 THEN 
                        N' [' + CAST(count_included_columns AS NVARCHAR(10))  + N' INCLUDE' + 
                            + CASE WHEN count_included_columns > 1 THEN  N'S' ELSE N'' END                    
                            + N'] ' + include_column_names
                    ELSE N'' END + CASE WHEN filter_definition <> N'' THEN N' [FILTER] ' + filter_definition
                    ELSE N'' END ,
            [total_reads] AS user_seeks + user_scans + user_lookups,
            [reads_per_write] AS CAST(CASE WHEN user_updates > 0
                THEN ( user_seeks + user_scans + user_lookups )  / (1.0 * user_updates)
                ELSE 0 END AS MONEY) ,
            [index_usage_summary] AS
				CASE WHEN is_spatial = 1 THEN N'Not Tracked'
				WHEN is_disabled = 1 THEN N'Disabled'
				ELSE N'Reads: ' + 
					REPLACE(CONVERT(NVARCHAR(30),CAST((user_seeks + user_scans + user_lookups) AS MONEY), 1), N'.00', N'')
					+ CASE WHEN user_seeks + user_scans + user_lookups > 0 THEN
						N' (' 
							+ RTRIM(
							CASE WHEN user_seeks > 0 THEN REPLACE(CONVERT(NVARCHAR(30),CAST((user_seeks) AS MONEY), 1), N'.00', N'') + N' seek ' ELSE N'' END
							+ CASE WHEN user_scans > 0 THEN REPLACE(CONVERT(NVARCHAR(30),CAST((user_scans) AS MONEY), 1), N'.00', N'') + N' scan '  ELSE N'' END
							+ CASE WHEN user_lookups > 0 THEN  REPLACE(CONVERT(NVARCHAR(30),CAST((user_lookups) AS MONEY), 1), N'.00', N'') + N' lookup' ELSE N'' END
							)
							+ N') '
						ELSE N' '
						END 
					+ N'Writes: ' + 
					REPLACE(CONVERT(NVARCHAR(30),CAST(user_updates AS MONEY), 1), N'.00', N'')
				END /* First "end" is about is_spatial */,
				[more_info] AS 
				CASE WHEN is_in_memory_oltp = 1 
					THEN N'EXEC dbo.sp_BlitzInMemoryOLTP @dbName=' + QUOTENAME([database_name],N'''') + 
					N', @tableName=' + QUOTENAME([object_name],N'''') + N';'
				ELSE N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name],N'''') + 
					N', @SchemaName=' + QUOTENAME([schema_name],N'''') + N', @TableName=' + QUOTENAME([object_name],N'''') + N';'
				END
		);
        RAISERROR (N'Adding UQ index on #IndexSanity (database_id, object_id, index_id)',0,1) WITH NOWAIT;
        IF NOT EXISTS(SELECT 1 FROM tempdb.sys.indexes WHERE name='uq_database_id_object_id_index_id') 
            CREATE UNIQUE INDEX uq_database_id_object_id_index_id ON #IndexSanity (database_id, object_id, index_id);


        CREATE TABLE #IndexPartitionSanity
            (
              [index_partition_sanity_id] INT IDENTITY,
              [index_sanity_id] INT NULL ,
              [database_id] INT NOT NULL ,
              [object_id] INT NOT NULL ,
			  [schema_name] NVARCHAR(128) NOT NULL,
              [index_id] INT NOT NULL ,
              [partition_number] INT NOT NULL ,
              row_count BIGINT NOT NULL ,
              reserved_MB NUMERIC(29,2) NOT NULL ,
              reserved_LOB_MB NUMERIC(29,2) NOT NULL ,
              reserved_row_overflow_MB NUMERIC(29,2) NOT NULL ,
              reserved_dictionary_MB NUMERIC(29,2) NOT NULL ,
              leaf_insert_count BIGINT NULL ,
              leaf_delete_count BIGINT NULL ,
              leaf_update_count BIGINT NULL ,
              range_scan_count BIGINT NULL ,
              singleton_lookup_count BIGINT NULL , 
              forwarded_fetch_count BIGINT NULL ,
              lob_fetch_in_pages BIGINT NULL ,
              lob_fetch_in_bytes BIGINT NULL ,
              row_overflow_fetch_in_pages BIGINT NULL ,
              row_overflow_fetch_in_bytes BIGINT NULL ,
              row_lock_count BIGINT NULL ,
              row_lock_wait_count BIGINT NULL ,
              row_lock_wait_in_ms BIGINT NULL ,
              page_lock_count BIGINT NULL ,
              page_lock_wait_count BIGINT NULL ,
              page_lock_wait_in_ms BIGINT NULL ,
              index_lock_promotion_attempt_count BIGINT NULL ,
              index_lock_promotion_count BIGINT NULL,
              data_compression_desc NVARCHAR(60) NULL,
			  page_latch_wait_count BIGINT NULL,
			  page_latch_wait_in_ms BIGINT NULL,
			  page_io_latch_wait_count BIGINT NULL,
			  page_io_latch_wait_in_ms BIGINT NULL,
              lock_escalation_desc nvarchar(60) NULL
            );

        CREATE TABLE #IndexSanitySize
            (
              [index_sanity_size_id] INT IDENTITY NOT NULL ,
              [index_sanity_id] INT NULL ,
              [database_id] INT NOT NULL,
			  [schema_name] NVARCHAR(128) NOT NULL,
              partition_count INT NOT NULL ,
              total_rows BIGINT NOT NULL ,
              total_reserved_MB NUMERIC(29,2) NOT NULL ,
              total_reserved_LOB_MB NUMERIC(29,2) NOT NULL ,
              total_reserved_row_overflow_MB NUMERIC(29,2) NOT NULL ,
              total_reserved_dictionary_MB NUMERIC(29,2) NOT NULL ,
              total_leaf_delete_count BIGINT NULL,
              total_leaf_update_count BIGINT NULL,
              total_range_scan_count BIGINT NULL,
              total_singleton_lookup_count BIGINT NULL,
              total_forwarded_fetch_count BIGINT NULL,
              total_row_lock_count BIGINT NULL ,
              total_row_lock_wait_count BIGINT NULL ,
              total_row_lock_wait_in_ms BIGINT NULL ,
              avg_row_lock_wait_in_ms BIGINT NULL ,
              total_page_lock_count BIGINT NULL ,
              total_page_lock_wait_count BIGINT NULL ,
              total_page_lock_wait_in_ms BIGINT NULL ,
              avg_page_lock_wait_in_ms BIGINT NULL ,
               total_index_lock_promotion_attempt_count BIGINT NULL ,
              total_index_lock_promotion_count BIGINT NULL ,
              data_compression_desc NVARCHAR(4000) NULL,
			  page_latch_wait_count BIGINT NULL,
			  page_latch_wait_in_ms BIGINT NULL,
			  page_io_latch_wait_count BIGINT NULL,
			  page_io_latch_wait_in_ms BIGINT NULL,
              lock_escalation_desc nvarchar(60) NULL,
              index_size_summary AS ISNULL(
                CASE WHEN partition_count > 1
                        THEN N'[' + CAST(partition_count AS NVARCHAR(10)) + N' PARTITIONS] '
                        ELSE N''
                END + REPLACE(CONVERT(NVARCHAR(30),CAST([total_rows] AS MONEY), 1), N'.00', N'') + N' rows; '
                + CASE WHEN total_reserved_MB > 1024 THEN 
                    CAST(CAST(total_reserved_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB'
                ELSE 
                    CAST(CAST(total_reserved_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB'
                END
                + CASE WHEN total_reserved_LOB_MB > 1024 THEN 
                    N'; ' + CAST(CAST(total_reserved_LOB_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB ' + CASE WHEN total_reserved_dictionary_MB = 0 THEN N'LOB' ELSE N'Columnstore' END
                WHEN total_reserved_LOB_MB > 0 THEN
                    N'; ' + CAST(CAST(total_reserved_LOB_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB ' + CASE WHEN total_reserved_dictionary_MB = 0 THEN N'LOB' ELSE N'Columnstore' END
                ELSE ''
                END
                 + CASE WHEN total_reserved_row_overflow_MB > 1024 THEN
                    N'; ' + CAST(CAST(total_reserved_row_overflow_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB Row Overflow'
                WHEN total_reserved_row_overflow_MB > 0 THEN
                    N'; ' + CAST(CAST(total_reserved_row_overflow_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB Row Overflow'
                ELSE ''
                END
                 + CASE WHEN total_reserved_dictionary_MB > 1024 THEN
                    N'; ' + CAST(CAST(total_reserved_dictionary_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB Dictionaries'
                WHEN total_reserved_dictionary_MB > 0 THEN
                    N'; ' + CAST(CAST(total_reserved_dictionary_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB Dictionaries'
                ELSE ''
                END ,
                    N'Error- NULL in computed column'),
            index_op_stats AS ISNULL(
                (
                    REPLACE(CONVERT(NVARCHAR(30),CAST(total_singleton_lookup_count AS MONEY), 1),N'.00',N'') + N' singleton lookups; '
                    + REPLACE(CONVERT(NVARCHAR(30),CAST(total_range_scan_count AS MONEY), 1),N'.00',N'') + N' scans/seeks; '
                    + REPLACE(CONVERT(NVARCHAR(30),CAST(total_leaf_delete_count AS MONEY), 1),N'.00',N'') + N' deletes; '
                    + REPLACE(CONVERT(NVARCHAR(30),CAST(total_leaf_update_count AS MONEY), 1),N'.00',N'') + N' updates; '
                    + CASE WHEN ISNULL(total_forwarded_fetch_count,0) >0 THEN
                        REPLACE(CONVERT(NVARCHAR(30),CAST(total_forwarded_fetch_count AS MONEY), 1),N'.00',N'') + N' forward records fetched; '
                    ELSE N'' END

                    /* rows will only be in this dmv when data is in memory for the table */
                ), N'Table metadata not in memory'),
            index_lock_wait_summary AS ISNULL(
                CASE WHEN total_row_lock_wait_count = 0 AND  total_page_lock_wait_count = 0 AND
                    total_index_lock_promotion_attempt_count = 0 THEN N'0 lock waits; '
                    + CASE WHEN lock_escalation_desc = N'DISABLE' THEN N'Lock escalation DISABLE.'
                      ELSE N''
                      END
                ELSE
                    CASE WHEN total_row_lock_wait_count > 0 THEN
                        N'Row lock waits: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_row_lock_wait_count AS MONEY), 1), N'.00', N'')
                        + N'; total duration: ' + 
                            CASE WHEN total_row_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
                                REPLACE(CONVERT(NVARCHAR(30),CAST((total_row_lock_wait_in_ms/60000) AS MONEY), 1), N'.00', N'') + N' minutes; '
                            ELSE                         
                                REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_row_lock_wait_in_ms/1000,0) AS MONEY), 1), N'.00', N'') + N' seconds; '
                            END
                        + N'avg duration: ' + 
                            CASE WHEN avg_row_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
                                REPLACE(CONVERT(NVARCHAR(30),CAST((avg_row_lock_wait_in_ms/60000) AS MONEY), 1), N'.00', N'') + N' minutes; '
                            ELSE                         
                                REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(avg_row_lock_wait_in_ms/1000,0) AS MONEY), 1), N'.00', N'') + N' seconds; '
                            END
                    ELSE N''
                    END +
                    CASE WHEN total_page_lock_wait_count > 0 THEN
                        N'Page lock waits: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_page_lock_wait_count AS MONEY), 1), N'.00', N'')
                        + N'; total duration: ' + 
                            CASE WHEN total_page_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
                                REPLACE(CONVERT(NVARCHAR(30),CAST((total_page_lock_wait_in_ms/60000) AS MONEY), 1), N'.00', N'') + N' minutes; '
                            ELSE                         
                                REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_page_lock_wait_in_ms/1000,0) AS MONEY), 1), N'.00', N'') + N' seconds; '
                            END
                        + N'avg duration: ' + 
                            CASE WHEN avg_page_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
                                REPLACE(CONVERT(NVARCHAR(30),CAST((avg_page_lock_wait_in_ms/60000) AS MONEY), 1), N'.00', N'') + N' minutes; '
                            ELSE                         
                                REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(avg_page_lock_wait_in_ms/1000,0) AS MONEY), 1), N'.00', N'') + N' seconds; '
                            END
                    ELSE N''
                    END +
                    CASE WHEN total_index_lock_promotion_attempt_count > 0 THEN
                        N'Lock escalation attempts: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_index_lock_promotion_attempt_count AS MONEY), 1), N'.00', N'')
                        + N'; Actual Escalations: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_index_lock_promotion_count,0) AS MONEY), 1), N'.00', N'') +N'; '
                    ELSE N''
                    END +
                    CASE WHEN lock_escalation_desc = N'DISABLE' THEN
                        N'Lock escalation is disabled.'
                    ELSE N''
                    END
                END                  
                    ,'Error- NULL in computed column')
            );

        CREATE TABLE #IndexColumns
            (
              [database_id] INT NOT NULL,
			  [schema_name] NVARCHAR(128),
              [object_id] INT NOT NULL ,
              [index_id] INT NOT NULL ,
              [key_ordinal] INT NULL ,
              is_included_column BIT NULL ,
              is_descending_key BIT NULL ,
              [partition_ordinal] INT NULL ,
              column_name NVARCHAR(256) NOT NULL ,
              system_type_name NVARCHAR(256) NOT NULL,
              max_length SMALLINT NOT NULL,
              [precision] TINYINT NOT NULL,
              [scale] TINYINT NOT NULL,
              collation_name NVARCHAR(256) NULL,
              is_nullable BIT NULL,
              is_identity BIT NULL,
              is_computed BIT NULL,
              is_replicated BIT NULL,
              is_sparse BIT NULL,
              is_filestream BIT NULL,
              seed_value DECIMAL(38,0) NULL,
              increment_value DECIMAL(38,0) NULL ,
              last_value DECIMAL(38,0) NULL,
              is_not_for_replication BIT NULL
            );
        CREATE CLUSTERED INDEX CLIX_database_id_object_id_index_id ON #IndexColumns
            (database_id, object_id, index_id);

        CREATE TABLE #MissingIndexes
            ([database_id] INT NOT NULL,
			[object_id] INT NOT NULL,
            [database_name] NVARCHAR(128) NOT NULL ,
            [schema_name] NVARCHAR(128) NOT NULL ,
            [table_name] NVARCHAR(128),
            [statement] NVARCHAR(512) NOT NULL,
            magic_benefit_number AS (( user_seeks + user_scans ) * avg_total_user_cost * avg_user_impact),
            avg_total_user_cost NUMERIC(29,4) NOT NULL,
            avg_user_impact NUMERIC(29,1) NOT NULL,
            user_seeks BIGINT NOT NULL,
            user_scans BIGINT NOT NULL,
            unique_compiles BIGINT NULL,
            equality_columns NVARCHAR(MAX),
            equality_columns_with_data_type NVARCHAR(MAX),
            inequality_columns NVARCHAR(MAX),
            inequality_columns_with_data_type NVARCHAR(MAX),
            included_columns NVARCHAR(MAX),
            included_columns_with_data_type NVARCHAR(MAX),
			is_low BIT,
                [index_estimated_impact] AS 
                    REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(
                                    (user_seeks + user_scans)
                                     AS BIGINT) AS MONEY), 1), '.00', '') + N' use' 
                        + CASE WHEN (user_seeks + user_scans) > 1 THEN N's' ELSE N'' END
                         +N'; Impact: ' + CAST(avg_user_impact AS NVARCHAR(30))
                        + N'%; Avg query cost: '
                        + CAST(avg_total_user_cost AS NVARCHAR(30)),
                [missing_index_details] AS
                    CASE WHEN COALESCE(equality_columns_with_data_type,equality_columns) IS NOT NULL
						THEN N'EQUALITY: ' + COALESCE(CAST(equality_columns_with_data_type AS NVARCHAR(MAX)), CAST(equality_columns AS NVARCHAR(MAX))) + N' '
                         ELSE N'' END +

                    CASE WHEN COALESCE(inequality_columns_with_data_type,inequality_columns) IS NOT NULL
						THEN N'INEQUALITY: ' + COALESCE(CAST(inequality_columns_with_data_type AS NVARCHAR(MAX)), CAST(inequality_columns AS NVARCHAR(MAX))) + N' '
                         ELSE N'' END +

                    CASE WHEN COALESCE(included_columns_with_data_type,included_columns) IS NOT NULL
						THEN N'INCLUDE: ' + COALESCE(CAST(included_columns_with_data_type AS NVARCHAR(MAX)), CAST(included_columns AS NVARCHAR(MAX))) + N' '
                         ELSE N'' END,
                [create_tsql] AS N'CREATE INDEX [' 
                    + LEFT(REPLACE(REPLACE(REPLACE(REPLACE(
                        ISNULL(equality_columns,N'')+ 
                        CASE WHEN equality_columns IS NOT NULL AND inequality_columns IS NOT NULL THEN N'_' ELSE N'' END
                        + ISNULL(inequality_columns,''),',','')
                        ,'[',''),']',''),' ','_') 
                    + CASE WHEN included_columns IS NOT NULL THEN N'_Includes' ELSE N'' END, 128) + N'] ON ' 
                    + [statement] + N' (' + ISNULL(equality_columns,N'')
                    + CASE WHEN equality_columns IS NOT NULL AND inequality_columns IS NOT NULL THEN N', ' ELSE N'' END
                    + CASE WHEN inequality_columns IS NOT NULL THEN inequality_columns ELSE N'' END + 
                    ') ' + CASE WHEN included_columns IS NOT NULL THEN N' INCLUDE (' + included_columns + N')' ELSE N'' END
                    + N' WITH (' 
                        + N'FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?' 
                    + N')'
                    + N';'
                    ,
                [more_info] AS N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name],'''') + 
                    N', @SchemaName=' + QUOTENAME([schema_name],'''') + N', @TableName=' + QUOTENAME([table_name],'''') + N';',
				[sample_query_plan] XML NULL
            );

        CREATE TABLE #ForeignKeys (
			[database_id] INT NOT NULL,
            [database_name] NVARCHAR(128) NOT NULL ,
			[schema_name] NVARCHAR(128) NOT NULL ,
            foreign_key_name NVARCHAR(256),
            parent_object_id INT,
            parent_object_name NVARCHAR(256),
            referenced_object_id INT,
            referenced_object_name NVARCHAR(256),
            is_disabled BIT,
            is_not_trusted BIT,
            is_not_for_replication BIT,
            parent_fk_columns NVARCHAR(MAX),
            referenced_fk_columns NVARCHAR(MAX),
            update_referential_action_desc NVARCHAR(16),
            delete_referential_action_desc NVARCHAR(60)
        );

        CREATE TABLE #UnindexedForeignKeys 
        (
        	[database_id] INT NOT NULL,
            [database_name] NVARCHAR(128) NOT NULL ,
        	[schema_name] NVARCHAR(128) NOT NULL ,
            foreign_key_name NVARCHAR(256),
            parent_object_name NVARCHAR(256),
			parent_object_id INT,
			referenced_object_name NVARCHAR(256),
			referenced_object_id INT
        );
        
        CREATE TABLE #IndexCreateTsql (
            index_sanity_id INT NOT NULL,
            create_tsql NVARCHAR(MAX) NOT NULL
        );

        CREATE TABLE #DatabaseList (
			DatabaseName NVARCHAR(256),
            secondary_role_allow_connections_desc NVARCHAR(50)

        );

		CREATE TABLE #PartitionCompressionInfo (
			[index_sanity_id] INT NULL,
			[partition_compression_detail] NVARCHAR(4000) NULL
        );

		CREATE TABLE #Statistics (
		  database_id INT NOT NULL,
		  database_name NVARCHAR(256) NOT NULL,
		  table_name NVARCHAR(128) NULL,
		  schema_name NVARCHAR(128) NULL,
		  index_name  NVARCHAR(128) NULL,
		  column_names  NVARCHAR(MAX) NULL,
		  statistics_name NVARCHAR(128) NULL,
		  last_statistics_update DATETIME NULL,
		  days_since_last_stats_update INT NULL,
		  rows BIGINT NULL,
		  rows_sampled BIGINT NULL,
		  percent_sampled DECIMAL(18, 1) NULL,
		  histogram_steps INT NULL,
		  modification_counter BIGINT NULL,
		  percent_modifications DECIMAL(18, 1) NULL,
		  modifications_before_auto_update INT NULL,
		  index_type_desc NVARCHAR(128) NULL,
		  table_create_date DATETIME NULL,
		  table_modify_date DATETIME NULL,
		  no_recompute BIT NULL,
		  has_filter BIT NULL,
		  filter_definition NVARCHAR(MAX) NULL
		); 

		CREATE TABLE #ComputedColumns
		(
		  index_sanity_id INT IDENTITY(1, 1) NOT NULL,
		  database_name NVARCHAR(128) NULL,
		  database_id INT NOT NULL,
		  table_name NVARCHAR(128) NOT NULL,
		  schema_name NVARCHAR(128) NOT NULL,
		  column_name NVARCHAR(128) NULL,
		  is_nullable BIT NULL,
		  definition NVARCHAR(MAX) NULL,
		  uses_database_collation BIT NOT NULL,
		  is_persisted BIT NOT NULL,
		  is_computed BIT NOT NULL,
		  is_function INT NOT NULL,
		  column_definition NVARCHAR(MAX) NULL
		);
		
		CREATE TABLE #TraceStatus
		(
		 TraceFlag NVARCHAR(10) ,
		 status BIT ,
		 Global BIT ,
		 Session BIT
		);

        CREATE TABLE #TemporalTables
        (
            index_sanity_id INT IDENTITY(1, 1) NOT NULL,
            database_name NVARCHAR(128) NOT NULL,
            database_id INT NOT NULL,
            schema_name NVARCHAR(128) NOT NULL,
            table_name NVARCHAR(128) NOT NULL,
            history_table_name NVARCHAR(128) NOT NULL,
            history_schema_name NVARCHAR(128) NOT NULL,
            start_column_name NVARCHAR(128) NOT NULL,
            end_column_name NVARCHAR(128) NOT NULL,
            period_name NVARCHAR(128) NOT NULL
        );

		CREATE TABLE #CheckConstraints
		(
		  index_sanity_id INT IDENTITY(1, 1) NOT NULL,
		  database_name NVARCHAR(128) NULL,
		  database_id INT NOT NULL,
		  table_name NVARCHAR(128) NOT NULL,
		  schema_name NVARCHAR(128) NOT NULL,
		  constraint_name NVARCHAR(128) NULL,
		  is_disabled BIT NULL,
		  definition NVARCHAR(MAX) NULL,
		  uses_database_collation BIT NOT NULL,
		  is_not_trusted BIT NOT NULL,
		  is_function INT NOT NULL,
		  column_definition NVARCHAR(MAX) NULL
		);

		CREATE TABLE #FilteredIndexes
		(
		  index_sanity_id INT IDENTITY(1, 1) NOT NULL,
		  database_name NVARCHAR(128) NULL,
		  database_id INT NOT NULL,
		  schema_name NVARCHAR(128) NOT NULL,
		  table_name NVARCHAR(128) NOT NULL,
		  index_name NVARCHAR(128) NULL,
		  column_name NVARCHAR(128) NULL
		);

        CREATE TABLE #Ignore_Databases 
        (
          DatabaseName NVARCHAR(128), 
          Reason NVARCHAR(100)
        );

/* Sanitize our inputs */
SELECT
	@OutputServerName = QUOTENAME(@OutputServerName),
	@OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
	@OutputSchemaName = QUOTENAME(@OutputSchemaName),
	@OutputTableName = QUOTENAME(@OutputTableName);
					
					
IF @GetAllDatabases = 1
    BEGIN
        INSERT INTO #DatabaseList (DatabaseName)
        SELECT  DB_NAME(database_id)
        FROM    sys.databases
        WHERE user_access_desc = 'MULTI_USER'
        AND state_desc = 'ONLINE'
        AND database_id > 4
        AND DB_NAME(database_id) NOT LIKE 'ReportServer%'
        AND DB_NAME(database_id) NOT LIKE 'rdsadmin%'
		AND LOWER(name) NOT IN('dbatools', 'dbadmin', 'dbmaintenance')
        AND is_distributor = 0
		OPTION    ( RECOMPILE );

        /* Skip non-readable databases in an AG - see Github issue #1160 */
        IF EXISTS (SELECT * FROM sys.all_objects o INNER JOIN sys.all_columns c ON o.object_id = c.object_id AND o.name = 'dm_hadr_availability_replica_states' AND c.name = 'role_desc')
            BEGIN
            SET @dsql = N'UPDATE #DatabaseList SET secondary_role_allow_connections_desc = ''NO'' WHERE DatabaseName IN (
                        SELECT d.name 
                        FROM sys.dm_hadr_availability_replica_states rs
                        INNER JOIN sys.databases d ON rs.replica_id = d.replica_id
                        INNER JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
                        WHERE rs.role_desc = ''SECONDARY''
                        AND r.secondary_role_allow_connections_desc = ''NO'')
						OPTION    ( RECOMPILE );';
            EXEC sp_executesql @dsql;

            IF EXISTS (SELECT * FROM #DatabaseList WHERE secondary_role_allow_connections_desc = 'NO')
                BEGIN
                INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, database_name, URL, details, index_definition,
                                                index_usage_summary, index_size_summary )
                VALUES  ( 1, 
				          0, 
		                  N'Skipped non-readable AG secondary databases.',
                          N'You are running this on an AG secondary, and some of your databases are configured as non-readable when this is a secondary node.',
				          N'To analyze those databases, run sp_BlitzIndex on the primary, or on a readable secondary.',
                          'http://FirstResponderKit.org', '', '', '', ''
                        );        
                END;
            END;

        IF @IgnoreDatabases IS NOT NULL
            AND LEN(@IgnoreDatabases) > 0
            BEGIN
                RAISERROR(N'Setting up filter to ignore databases', 0, 1) WITH NOWAIT;
                SET @DatabaseToIgnore = '';

                WHILE LEN(@IgnoreDatabases) > 0
                BEGIN
                    IF PATINDEX('%,%', @IgnoreDatabases) > 0
                    BEGIN  
                        SET @DatabaseToIgnore = SUBSTRING(@IgnoreDatabases, 0, PATINDEX('%,%',@IgnoreDatabases)) ;
                        
                        INSERT INTO #Ignore_Databases (DatabaseName, Reason)
                        SELECT LTRIM(RTRIM(@DatabaseToIgnore)), 'Specified in the @IgnoreDatabases parameter'
                        OPTION (RECOMPILE) ;
                        
                        SET @IgnoreDatabases = SUBSTRING(@IgnoreDatabases, LEN(@DatabaseToIgnore + ',') + 1, LEN(@IgnoreDatabases)) ;
                    END;
                    ELSE
                    BEGIN
                        SET @DatabaseToIgnore = @IgnoreDatabases ;
                        SET @IgnoreDatabases = NULL ;

                        INSERT INTO #Ignore_Databases (DatabaseName, Reason)
                        SELECT LTRIM(RTRIM(@DatabaseToIgnore)), 'Specified in the @IgnoreDatabases parameter'
                        OPTION (RECOMPILE) ;
                    END;
            END;
                
        END

    END;
ELSE
    BEGIN
        INSERT INTO #DatabaseList
                ( DatabaseName )
        SELECT CASE 
		            WHEN @DatabaseName IS NULL OR @DatabaseName = N'' 
		            THEN DB_NAME()
                    ELSE @DatabaseName END;
               END;

SET @NumDatabases = (SELECT COUNT(*) FROM #DatabaseList AS D LEFT OUTER JOIN #Ignore_Databases AS I ON D.DatabaseName = I.DatabaseName WHERE I.DatabaseName IS NULL);
SET @msg = N'Number of databases to examine: ' + CAST(@NumDatabases AS NVARCHAR(50));
RAISERROR (@msg,0,1) WITH NOWAIT;



/* Running on 50+ databases can take a reaaallly long time, so we want explicit permission to do so (and only after warning about it) */


BEGIN TRY
        IF @NumDatabases >= 50 AND @BringThePain != 1 AND @TableName IS NULL
        BEGIN

            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( -1, 
			          0 , 
		              @ScriptVersionName,
                      CASE WHEN @GetAllDatabases = 1 THEN N'All Databases' ELSE N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + CONVERT(NVARCHAR(16), GETDATE(), 121) END, 
                      N'From Your Community Volunteers',   
					  N'http://FirstResponderKit.org',
                      N'',
                      N'',
					  N''
                    );
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, database_name, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( 1, 
			          0, 
		              N'You''re trying to run sp_BlitzIndex on a server with ' + CAST(@NumDatabases AS NVARCHAR(8)) + N' databases. ',
                      N'Running sp_BlitzIndex on a server with 50+ databases may cause temporary insanity for the server and/or user.',
				      N'If you''re sure you want to do this, run again with the parameter @BringThePain = 1.',
                      'http://FirstResponderKit.org', 
					  '', 
					  '', 
					  '', 
					  ''
                    );        
            
			if(@OutputType <> 'NONE')
			BEGIN
				SELECT bir.blitz_result_id,
					   bir.check_id,
					   bir.index_sanity_id,
					   bir.Priority,
					   bir.findings_group,
					   bir.finding,
					   bir.database_name,
					   bir.URL,
					   bir.details,
					   bir.index_definition,
					   bir.secret_columns,
					   bir.index_usage_summary,
					   bir.index_size_summary,
					   bir.create_tsql,
					   bir.more_info 
					   FROM #BlitzIndexResults AS bir;
				RAISERROR('Running sp_BlitzIndex on a server with 50+ databases may cause temporary insanity for the server', 12, 1);
			END;

		RETURN;

		END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure to execute due to number of databases.', 0,1) WITH NOWAIT;

        SELECT  @msg = ERROR_MESSAGE(), 
		          @ErrorSeverity = ERROR_SEVERITY(), 
				  @ErrorState = ERROR_STATE();

        RAISERROR (@msg, @ErrorSeverity, @ErrorState);
        
        WHILE @@trancount > 0 
            ROLLBACK;

        RETURN;
    END CATCH;


RAISERROR (N'Checking partition counts to exclude databases with over 100 partitions',0,1) WITH NOWAIT;
IF @BringThePain = 0 AND @SkipPartitions = 0 AND @TableName IS NULL
    BEGIN   
        DECLARE partition_cursor CURSOR FOR
        SELECT dl.DatabaseName
        FROM #DatabaseList dl
        LEFT OUTER JOIN #Ignore_Databases i ON dl.DatabaseName = i.DatabaseName
        WHERE COALESCE(dl.secondary_role_allow_connections_desc, 'OK') <> 'NO' 
        AND i.DatabaseName IS NULL

        OPEN partition_cursor
        FETCH NEXT FROM partition_cursor INTO @DatabaseName
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            /* Count the total number of partitions */
            SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    SELECT @RowcountOUT = SUM(1) FROM ' + QUOTENAME(@DatabaseName) + '.sys.partitions WHERE partition_number > 1 OPTION    ( RECOMPILE );';
            EXEC sp_executesql @dsql, N'@RowcountOUT BIGINT OUTPUT', @RowcountOUT = @Rowcount OUTPUT;
            IF @Rowcount > 100
                BEGIN
                   RAISERROR (N'Skipping database %s because > 100 partitions were found. To check this database, you must set @BringThePain = 1.',0,1,@DatabaseName) WITH NOWAIT;
				INSERT INTO #Ignore_Databases (DatabaseName, Reason)
				SELECT @DatabaseName, 'Over 100 partitions found - use @BringThePain = 1 to analyze'
                END;
            FETCH NEXT FROM partition_cursor INTO @DatabaseName
        END;
        CLOSE partition_cursor
        DEALLOCATE partition_cursor

    END;					

INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                index_usage_summary, index_size_summary )
SELECT  1, 0 , 
        'Database Skipped',
        i.DatabaseName,
        'http://FirstResponderKit.org',
        i.Reason, '', '', ''
FROM #Ignore_Databases i;


/* Last startup */
IF COLUMNPROPERTY(OBJECT_ID('sys.dm_os_sys_info'),'sqlserver_start_time','ColumnID') IS NOT NULL
BEGIN
	SELECT @DaysUptime = CAST(DATEDIFF(HOUR, sqlserver_start_time, GETDATE()) / 24. AS NUMERIC (23,2))
	FROM sys.dm_os_sys_info;
END
ELSE
BEGIN
	SELECT  @DaysUptime = CAST(DATEDIFF(HOUR, create_date, GETDATE()) / 24. AS NUMERIC (23,2))
	FROM    sys.databases
	WHERE   database_id = 2;
END

IF @DaysUptime = 0 OR @DaysUptime IS NULL 
  SET @DaysUptime = .01;

SELECT @DaysUptimeInsertValue = 'Server: ' + (CONVERT(VARCHAR(256), (SERVERPROPERTY('ServerName')))) + ' Days Uptime: ' + RTRIM(@DaysUptime);


/* Permission granted or unnecessary? Ok, let's go! */

RAISERROR (N'Starting loop through databases',0,1) WITH NOWAIT;
DECLARE c1 CURSOR 
LOCAL FAST_FORWARD 
FOR 
SELECT dl.DatabaseName 
FROM #DatabaseList dl
LEFT OUTER JOIN #Ignore_Databases i ON dl.DatabaseName = i.DatabaseName
WHERE COALESCE(dl.secondary_role_allow_connections_desc, 'OK') <> 'NO' 
  AND i.DatabaseName IS NULL
ORDER BY dl.DatabaseName;

OPEN c1;
FETCH NEXT FROM c1 INTO @DatabaseName;
     WHILE @@FETCH_STATUS = 0

BEGIN
    
    RAISERROR (@LineFeed, 0, 1) WITH NOWAIT;
    RAISERROR (@LineFeed, 0, 1) WITH NOWAIT;
    RAISERROR (@DatabaseName, 0, 1) WITH NOWAIT;

SELECT   @DatabaseID = [database_id]
FROM     sys.databases
         WHERE [name] = @DatabaseName
         AND user_access_desc='MULTI_USER'
         AND state_desc = 'ONLINE';

----------------------------------------
--STEP 1: OBSERVE THE PATIENT
--This step puts index information into temp tables.
----------------------------------------
BEGIN TRY
    BEGIN

        --Validate SQL Server Version

        IF (SELECT LEFT(@SQLServerProductVersion,
              CHARINDEX('.',@SQLServerProductVersion,0)-1
              )) <= 9
        BEGIN
            SET @msg=N'sp_BlitzIndex is only supported on SQL Server 2008 and higher. The version of this instance is: ' + @SQLServerProductVersion;
            RAISERROR(@msg,16,1);
        END;

        --Short circuit here if database name does not exist.
        IF @DatabaseName IS NULL OR @DatabaseID IS NULL
        BEGIN
            SET @msg='Database does not exist or is not online/multi-user: cannot proceed.';
            RAISERROR(@msg,16,1);
        END;    

        --Validate parameters.
        IF (@Mode NOT IN (0,1,2,3,4))
        BEGIN
            SET @msg=N'Invalid @Mode parameter. 0=diagnose, 1=summarize, 2=index detail, 3=missing index detail, 4=diagnose detail';
            RAISERROR(@msg,16,1);
        END;

        IF (@Mode <> 0 AND @TableName IS NOT NULL)
        BEGIN
            SET @msg=N'Setting the @Mode doesn''t change behavior if you supply @TableName. Use default @Mode=0 to see table detail.';
            RAISERROR(@msg,16,1);
        END;

        IF ((@Mode <> 0 OR @TableName IS NOT NULL) AND @Filter <> 0)
        BEGIN
            SET @msg=N'@Filter only applies when @Mode=0 and @TableName is not specified. Please try again.';
            RAISERROR(@msg,16,1);
        END;

        IF (@SchemaName IS NOT NULL AND @TableName IS NULL) 
        BEGIN
            SET @msg='We can''t run against a whole schema! Specify a @TableName, or leave both NULL for diagnosis.';
            RAISERROR(@msg,16,1);
        END;


        IF  (@TableName IS NOT NULL AND @SchemaName IS NULL)
        BEGIN
            SET @SchemaName=N'dbo';
            SET @msg='@SchemaName wasn''t specified-- assuming schema=dbo.';
            RAISERROR(@msg,1,1) WITH NOWAIT;
        END;

        --If a table is specified, grab the object id.
        --Short circuit if it doesn't exist.
        IF @TableName IS NOT NULL
        BEGIN
            SET @dsql = N'
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    SELECT  @ObjectID= OBJECT_ID
                    FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so
                    JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS sc on 
                        so.schema_id=sc.schema_id
                    where so.type in (''U'', ''V'')
                    and so.name=' + QUOTENAME(@TableName,'''')+ N'
                    and sc.name=' + QUOTENAME(@SchemaName,'''')+ N'
                    /*Has a row in sys.indexes. This lets us get indexed views.*/
                    and exists (
                        SELECT si.name
                        FROM ' + QUOTENAME(@DatabaseName) + '.sys.indexes AS si 
                        WHERE so.object_id=si.object_id)
                    OPTION (RECOMPILE);';

            SET @params='@ObjectID INT OUTPUT';                

            IF @dsql IS NULL 
                RAISERROR('@dsql is null',16,1);

            EXEC sp_executesql @dsql, @params, @ObjectID=@ObjectID OUTPUT;
            
            IF @ObjectID IS NULL
                    BEGIN
                        SET @msg=N'Oh, this is awkward. I can''t find the table or indexed view you''re looking for in that database.' + CHAR(10) +
                            N'Please check your parameters.';
                        RAISERROR(@msg,1,1);
                        RETURN;
                    END;
        END;

        --set @collation
        SELECT @collation=collation_name
        FROM sys.databases
        WHERE database_id=@DatabaseID;

        --insert columns for clustered indexes and heaps
        --collect info on identity columns for this one
        SET @dsql = N'/* sp_BlitzIndex */
				SET LOCK_TIMEOUT 1000; /* To fix locking bug in sys.identity_columns. See Github issue #2176. */
				SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT ' + CAST(@DatabaseID AS NVARCHAR(16)) + ',
					s.name,    
                    si.object_id, 
                    si.index_id, 
                    sc.key_ordinal, 
                    sc.is_included_column, 
                    sc.is_descending_key,
                    sc.partition_ordinal,
                    c.name as column_name, 
                    st.name as system_type_name,
                    c.max_length,
                    c.[precision],
                    c.[scale],
                    c.collation_name,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.is_replicated,
                    ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'c.is_sparse' ELSE N'NULL as is_sparse' END + N',
                    ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'c.is_filestream' ELSE N'NULL as is_filestream' END + N',
                    CAST(ic.seed_value AS DECIMAL(38,0)),
                    CAST(ic.increment_value AS DECIMAL(38,0)),
                    CAST(ic.last_value AS DECIMAL(38,0)),
                    ic.is_not_for_replication
                FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.indexes si
                JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.columns c ON
                    si.object_id=c.object_id
                LEFT JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns sc ON 
                    sc.object_id = si.object_id
                    and sc.index_id=si.index_id
                    AND sc.column_id=c.column_id
                LEFT JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.identity_columns ic ON
                    c.object_id=ic.object_id and
                    c.column_id=ic.column_id
                JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.types st ON 
                    c.system_type_id=st.system_type_id
                    AND c.user_type_id=st.user_type_id
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so  ON si.object_id = so.object_id
																		  AND so.is_ms_shipped = 0
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                WHERE si.index_id in (0,1) ' 
                    + CASE WHEN @ObjectID IS NOT NULL 
                        THEN N' AND si.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
                    ELSE N'' END 
                + N'OPTION (RECOMPILE);';

        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #IndexColumns for clustered indexes and heaps',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
		BEGIN TRY
			INSERT    #IndexColumns ( database_id, [schema_name], [object_id], index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
				column_name, system_type_name, max_length, precision, scale, collation_name, is_nullable, is_identity, is_computed,
				is_replicated, is_sparse, is_filestream, seed_value, increment_value, last_value, is_not_for_replication )
					EXEC sp_executesql @dsql;
		END TRY
		BEGIN CATCH
			RAISERROR (N'Failure inserting data into #IndexColumns for clustered indexes and heaps.', 0,1) WITH NOWAIT;

			IF @dsql IS NOT NULL
			BEGIN
				SET @msg= 'Last @dsql: ' + @dsql;
				RAISERROR(@msg, 0, 1) WITH NOWAIT;
			END;

			SELECT  @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(),
				@ErrorSeverity = 0, @ErrorState = ERROR_STATE();
			RAISERROR (@msg,@ErrorSeverity, @ErrorState )WITH NOWAIT;

			WHILE @@trancount > 0 
				ROLLBACK;

			RETURN;
		END CATCH;


        --insert columns for nonclustered indexes
        --this uses a full join to sys.index_columns
        --We don't collect info on identity columns here. They may be in NC indexes, but we just analyze identities in the base table.
        SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT ' + CAST(@DatabaseID AS NVARCHAR(16)) + ', 
					s.name,    
                    si.object_id, 
                    si.index_id, 
                    sc.key_ordinal, 
                    sc.is_included_column, 
                    sc.is_descending_key,
                    sc.partition_ordinal,
                    c.name as column_name, 
                    st.name as system_type_name,
                    c.max_length,
                    c.[precision],
                    c.[scale],
                    c.collation_name,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.is_replicated,
                    ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'c.is_sparse' ELSE N'NULL AS is_sparse' END + N',
                    ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'c.is_filestream' ELSE N'NULL AS is_filestream' END + N'                
                FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS si
                JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c ON
                    si.object_id=c.object_id
                JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns AS sc ON 
                    sc.object_id = si.object_id
                    and sc.index_id=si.index_id
                    AND sc.column_id=c.column_id
                JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.types AS st ON 
                    c.system_type_id=st.system_type_id
                    AND c.user_type_id=st.user_type_id
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so  ON si.object_id = so.object_id
																		  AND so.is_ms_shipped = 0
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                WHERE si.index_id not in (0,1) ' 
                    + CASE WHEN @ObjectID IS NOT NULL 
                        THEN N' AND si.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
                    ELSE N'' END 
                + N'OPTION (RECOMPILE);';

        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #IndexColumns for nonclustered indexes',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
        INSERT    #IndexColumns ( database_id, [schema_name], [object_id], index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
            column_name, system_type_name, max_length, precision, scale, collation_name, is_nullable, is_identity, is_computed,
            is_replicated, is_sparse, is_filestream )
                EXEC sp_executesql @dsql;
           
        SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT  ' + CAST(@DatabaseID AS NVARCHAR(10)) + N' AS database_id, 
                        so.object_id, 
                        si.index_id, 
                        si.type,
                        @i_DatabaseName AS database_name, 
                        COALESCE(sc.NAME, ''Unknown'') AS [schema_name],
                        COALESCE(so.name, ''Unknown'') AS [object_name], 
                        COALESCE(si.name, ''Unknown'') AS [index_name],
                        CASE    WHEN so.[type] = CAST(''V'' AS CHAR(2)) THEN 1 ELSE 0 END, 
                        si.is_unique, 
                        si.is_primary_key, 
						si.is_unique_constraint,
						CASE when si.type = 3 THEN 1 ELSE 0 END AS is_XML,
                        CASE when si.type = 4 THEN 1 ELSE 0 END AS is_spatial,
                        CASE when si.type = 6 THEN 1 ELSE 0 END AS is_NC_columnstore,
                        CASE when si.type = 5 then 1 else 0 end as is_CX_columnstore,
                        CASE when si.data_space_id = 0 then 1 else 0 end as is_in_memory_oltp,
                        si.is_disabled,
                        si.is_hypothetical, 
                        si.is_padded, 
                        si.fill_factor,'
                        + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'
                        CASE WHEN si.filter_definition IS NOT NULL THEN si.filter_definition
                             ELSE N''''
                        END AS filter_definition' ELSE N''''' AS filter_definition' END 
						+ CASE
						      WHEN @OptimizeForSequentialKey = 1
						      THEN N', si.optimize_for_sequential_key'
							  ELSE N', CONVERT(BIT, NULL) AS optimize_for_sequential_key'
						  END
						+ N',
						ISNULL(us.user_seeks, 0),
                        ISNULL(us.user_scans, 0),
                        ISNULL(us.user_lookups, 0),
                        ISNULL(us.user_updates, 0),
                        us.last_user_seek,
                        us.last_user_scan,
                        us.last_user_lookup,
                        us.last_user_update,
                        so.create_date,
                        so.modify_date
                FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS si WITH (NOLOCK)
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so WITH (NOLOCK) ON si.object_id = so.object_id
                                               AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                                               AND so.type <> ''TF'' /*Exclude table valued functions*/
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas sc ON so.schema_id = sc.schema_id
                        LEFT JOIN sys.dm_db_index_usage_stats AS us WITH (NOLOCK) ON si.[object_id] = us.[object_id]
                                                                       AND si.index_id = us.index_id
                                                                       AND us.database_id = ' + CAST(@DatabaseID AS NVARCHAR(10)) + N'
                WHERE    si.[type] IN ( 0, 1, 2, 3, 4, 5, 6 ) 
                /* Heaps, clustered, nonclustered, XML, spatial, Cluster Columnstore, NC Columnstore */ ' +
                CASE WHEN @TableName IS NOT NULL THEN N' and so.name=' + QUOTENAME(@TableName,N'''') + N' ' ELSE N'' END +
                CASE WHEN ( @IncludeInactiveIndexes = 0
                            AND @Mode IN (0, 4)
                            AND @TableName IS NULL )
                     THEN N'AND ( us.user_seeks + us.user_scans + us.user_lookups + us.user_updates ) > 0'
                     ELSE N''
                END
        + N'OPTION    ( RECOMPILE );
        ';
        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #IndexSanity',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
        INSERT    #IndexSanity ( [database_id], [object_id], [index_id], [index_type], [database_name], [schema_name], [object_name],
                                index_name, is_indexed_view, is_unique, is_primary_key, is_unique_constraint, is_XML, is_spatial, is_NC_columnstore, is_CX_columnstore, is_in_memory_oltp,
                                is_disabled, is_hypothetical, is_padded, fill_factor, filter_definition,  [optimize_for_sequential_key], user_seeks, user_scans, 
                                user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update,
                                create_date, modify_date )
                EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;


        RAISERROR (N'Checking partition count',0,1) WITH NOWAIT;
        IF @BringThePain = 0 AND @SkipPartitions = 0 AND @TableName IS NULL
            BEGIN
                /* Count the total number of partitions */
                SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT @RowcountOUT = SUM(1) FROM ' + QUOTENAME(@DatabaseName) + '.sys.partitions WHERE partition_number > 1 OPTION    ( RECOMPILE );';
                EXEC sp_executesql @dsql, N'@RowcountOUT BIGINT OUTPUT', @RowcountOUT = @Rowcount OUTPUT;
                IF @Rowcount > 100
                    BEGIN
                        RAISERROR (N'Setting @SkipPartitions = 1 because > 100 partitions were found. To check them, you must set @BringThePain = 1.',0,1) WITH NOWAIT;
                        SET @SkipPartitions = 1;
                        INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                                        index_usage_summary, index_size_summary )
                        VALUES  ( 1, 0 , 
		                       'Some Checks Were Skipped',
                               '@SkipPartitions Forced to 1',
                               'http://FirstResponderKit.org', CAST(@Rowcount AS NVARCHAR(50)) + ' partitions found. To analyze them, use @BringThePain = 1.', 'We try to keep things quick - and warning, running @BringThePain = 1 can take tens of minutes.', '', ''
                                );
                    END;
            END;



		 IF (@SkipPartitions = 0)
			BEGIN			
			IF (SELECT LEFT(@SQLServerProductVersion,
			      CHARINDEX('.',@SQLServerProductVersion,0)-1 )) <= 2147483647 --Make change here 			
			BEGIN
            
			RAISERROR (N'Preferring non-2012 syntax with LEFT JOIN to sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;

            --NOTE: If you want to use the newer syntax for 2012+, you'll have to change 2147483647 to 11 on line ~819
			--This change was made because on a table with lots of paritions, the OUTER APPLY was crazy slow.
            SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT  ' + CAST(@DatabaseID AS NVARCHAR(10)) + N' AS database_id,
                                ps.object_id, 
								s.name,
                                ps.index_id, 
                                ps.partition_number, 
                                ps.row_count,
                                ps.reserved_page_count * 8. / 1024. AS reserved_MB,
                                ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
                                ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
								le.lock_escalation_desc,
                            ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'par.data_compression_desc ' ELSE N'null as data_compression_desc ' END + N',
                                SUM(os.leaf_insert_count), 
                                SUM(os.leaf_delete_count), 
                                SUM(os.leaf_update_count), 
                                SUM(os.range_scan_count), 
                                SUM(os.singleton_lookup_count),  
                                SUM(os.forwarded_fetch_count),
                                SUM(os.lob_fetch_in_pages), 
                                SUM(os.lob_fetch_in_bytes), 
                                SUM(os.row_overflow_fetch_in_pages),
                                SUM(os.row_overflow_fetch_in_bytes), 
                                SUM(os.row_lock_count), 
                                SUM(os.row_lock_wait_count),
                                SUM(os.row_lock_wait_in_ms), 
                                SUM(os.page_lock_count), 
                                SUM(os.page_lock_wait_count), 
                                SUM(os.page_lock_wait_in_ms),
                                SUM(os.index_lock_promotion_attempt_count), 
                                SUM(os.index_lock_promotion_count), 
								SUM(os.page_latch_wait_count),
								SUM(os.page_latch_wait_in_ms),
								SUM(os.page_io_latch_wait_count),								
								SUM(os.page_io_latch_wait_in_ms), ';

		    /* Get columnstore dictionary size - more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2585 */
			IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'column_store_dictionaries')
				SET @dsql = @dsql + N' COALESCE((SELECT SUM (on_disk_size / 1024.0 / 1024) FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_dictionaries dict WHERE dict.partition_id = ps.partition_id),0) AS reserved_dictionary_MB ';
			ELSE
				SET @dsql = @dsql + N' 0 AS reserved_dictionary_MB ';


            SET @dsql = @dsql + N'
			FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_partition_stats AS ps  
                    JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partitions AS par on ps.partition_id=par.partition_id
                    JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so ON ps.object_id = so.object_id
                               AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                               AND so.type <> ''TF'' /*Exclude table valued functions*/
					JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                    LEFT JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_index_operational_stats('
                + CAST(@DatabaseID AS NVARCHAR(10)) + N', NULL, NULL,NULL) AS os ON
                    ps.object_id=os.object_id and ps.index_id=os.index_id and ps.partition_number=os.partition_number 
			            OUTER APPLY (SELECT st.lock_escalation_desc
			                         FROM ' + QUOTENAME(@DatabaseName) + N'.sys.tables st
			                         WHERE st.object_id = ps.object_id
			                             AND ps.index_id < 2 ) le
                    WHERE 1=1 
                    ' + CASE WHEN @ObjectID IS NOT NULL THEN N'AND so.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' ELSE N' ' END + N'
                    ' + CASE WHEN @Filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@FilterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + N'
            GROUP BY ps.object_id, 
								s.name,
                                ps.index_id, 
                                ps.partition_number, 
								ps.partition_id,
                                ps.row_count,
                                ps.reserved_page_count,
                                ps.lob_reserved_page_count,
                                ps.row_overflow_reserved_page_count,
								le.lock_escalation_desc,
                            ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'par.data_compression_desc ' ELSE N'null as data_compression_desc ' END + N'
			ORDER BY ps.object_id,  ps.index_id, ps.partition_number
            OPTION    ( RECOMPILE );
            ';
        END;
        ELSE
        BEGIN
        RAISERROR (N'Using 2012 syntax to query sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;
		--This is the syntax that will be used if you change 2147483647 to 11 on line ~819.
		--If you have a lot of paritions and this suddenly starts running for a long time, change it back.
         SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT  ' + CAST(@DatabaseID AS NVARCHAR(10)) + N' AS database_id,
                                ps.object_id, 
								s.name,
                                ps.index_id, 
                                ps.partition_number, 
                                ps.row_count,
                                ps.reserved_page_count * 8. / 1024. AS reserved_MB,
                                ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
                                ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
								le.lock_escalation_desc,
                                ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'par.data_compression_desc ' ELSE N'null as data_compression_desc' END + N',
                                SUM(os.leaf_insert_count), 
                                SUM(os.leaf_delete_count), 
                                SUM(os.leaf_update_count), 
                                SUM(os.range_scan_count), 
                                SUM(os.singleton_lookup_count),  
                                SUM(os.forwarded_fetch_count),
                                SUM(os.lob_fetch_in_pages), 
                                SUM(os.lob_fetch_in_bytes), 
                                SUM(os.row_overflow_fetch_in_pages),
                                SUM(os.row_overflow_fetch_in_bytes), 
                                SUM(os.row_lock_count), 
                                SUM(os.row_lock_wait_count),
                                SUM(os.row_lock_wait_in_ms), 
                                SUM(os.page_lock_count), 
                                SUM(os.page_lock_wait_count), 
                                SUM(os.page_lock_wait_in_ms),
                                SUM(os.index_lock_promotion_attempt_count), 
                                SUM(os.index_lock_promotion_count),
								SUM(os.page_latch_wait_count),
								SUM(os.page_latch_wait_in_ms),
								SUM(os.page_io_latch_wait_count),								
								SUM(os.page_io_latch_wait_in_ms)';

		    /* Get columnstore dictionary size - more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2585 */
			IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'column_store_dictionaries')
				SET @dsql = @dsql + N' COALESCE((SELECT SUM (on_disk_size / 1024.0 / 1024) FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_dictionaries dict WHERE dict.partition_id = ps.partition_id),0) AS reserved_dictionary_MB ';
			ELSE
				SET @dsql = @dsql + N' 0 AS reserved_dictionary_MB ';


            SET @dsql = @dsql + N'
                        FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_partition_stats AS ps  
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partitions AS par on ps.partition_id=par.partition_id
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so ON ps.object_id = so.object_id
                                   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                                   AND so.type <> ''TF'' /*Exclude table valued functions*/
						JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                        OUTER APPLY ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_index_operational_stats('
                    + CAST(@DatabaseID AS NVARCHAR(10)) + N', ps.object_id, ps.index_id,ps.partition_number) AS os
			            OUTER APPLY (SELECT st.lock_escalation_desc
			                         FROM ' + QUOTENAME(@DatabaseName) + N'.sys.tables st
			                         WHERE st.object_id = ps.object_id
			                             AND ps.index_id < 2 ) le
                        WHERE 1=1 
                        ' + CASE WHEN @ObjectID IS NOT NULL THEN N'AND so.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' ELSE N' ' END + N'
                        ' + CASE WHEN @Filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@FilterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + '
	            GROUP BY ps.object_id, 
								s.name,
                                ps.index_id, 
                                ps.partition_number,
								ps.partition_id,
                                ps.row_count,
                                ps.reserved_page_count,
                                ps.lob_reserved_page_count,
                                ps.row_overflow_reserved_page_count,
								le.lock_escalation_desc,
                            ' + CASE WHEN @SQLServerProductVersion NOT LIKE '9%' THEN N'par.data_compression_desc ' ELSE N'null as data_compression_desc ' END + N'
				ORDER BY ps.object_id,  ps.index_id, ps.partition_number
                OPTION    ( RECOMPILE );
                ';
        END;       

        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #IndexPartitionSanity',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
        INSERT    #IndexPartitionSanity ( [database_id],
                                          [object_id], 
										  [schema_name],
                                          index_id, 
                                          partition_number, 
                                          row_count, 
                                          reserved_MB,
                                          reserved_LOB_MB, 
                                          reserved_row_overflow_MB,
										  lock_escalation_desc,										   
                                          data_compression_desc, 
                                          leaf_insert_count,
                                          leaf_delete_count, 
                                          leaf_update_count, 
                                          range_scan_count,
                                          singleton_lookup_count,
                                          forwarded_fetch_count, 
                                          lob_fetch_in_pages, 
                                          lob_fetch_in_bytes, 
                                          row_overflow_fetch_in_pages,
                                          row_overflow_fetch_in_bytes, 
                                          row_lock_count, 
                                          row_lock_wait_count,
                                          row_lock_wait_in_ms, 
                                          page_lock_count, 
                                          page_lock_wait_count,
                                          page_lock_wait_in_ms, 
                                          index_lock_promotion_attempt_count,
                                          index_lock_promotion_count,
								          page_latch_wait_count,
								          page_latch_wait_in_ms,
								          page_io_latch_wait_count,								
								          page_io_latch_wait_in_ms,
										  reserved_dictionary_MB)
                EXEC sp_executesql @dsql;
        
		END; --End Check For @SkipPartitions = 0



        RAISERROR (N'Inserting data into #MissingIndexes',0,1) WITH NOWAIT;
        SET @dsql=N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'


		SET @dsql = @dsql + 'WITH ColumnNamesWithDataTypes AS(SELECT id.index_handle,id.object_id,cn.IndexColumnType,STUFF((SELECT '', '' + cn_inner.ColumnName + '' '' +
			N'' {'' + CASE	 WHEN ty.name IN ( ''varchar'', ''char'' ) THEN ty.name + ''('' + CASE WHEN co.max_length = -1 THEN ''max'' ELSE CAST(co.max_length AS VARCHAR(25)) END + '')''
								WHEN ty.name IN ( ''nvarchar'', ''nchar'' ) THEN ty.name + ''('' + CASE WHEN co.max_length = -1 THEN ''max'' ELSE CAST(co.max_length / 2 AS VARCHAR(25)) END + '')''
								WHEN ty.name IN ( ''decimal'', ''numeric'' ) THEN ty.name + ''('' + CAST(co.precision AS VARCHAR(25)) + '', '' + CAST(co.scale AS VARCHAR(25)) + '')''
								WHEN ty.name IN ( ''datetime2'' ) THEN ty.name + ''('' + CAST(co.scale AS VARCHAR(25)) + '')''
								ELSE ty.name END + ''}''
				FROM	' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_details AS id_inner
				CROSS APPLY(
					SELECT	LTRIM(RTRIM(v.value(''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Equality'' AS IndexColumnType
					FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id_inner.equality_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
					CROSS APPLY n.nodes(''x'') node(v)
				UNION ALL
					SELECT	LTRIM(RTRIM(v.value(N''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Inequality'' AS IndexColumnType
					FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id_inner.inequality_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
					CROSS APPLY n.nodes(''x'') node(v)
				UNION ALL
					SELECT	LTRIM(RTRIM(v.value(''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Included'' AS IndexColumnType
					FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id_inner.included_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
					CROSS APPLY n.nodes(''x'') node(v)
			)AS cn_inner'
		+ /*split the string otherwise dsql cuts some of it out*/
		'		JOIN	' + QUOTENAME(@DatabaseName) + N'.sys.columns AS co ON co.object_id = id_inner.object_id AND ''['' + co.name + '']'' = cn_inner.ColumnName
				JOIN	' + QUOTENAME(@DatabaseName) + N'.sys.types AS ty ON ty.user_type_id = co.user_type_id 
                WHERE id_inner.index_handle = id.index_handle
				AND	id_inner.object_id = id.object_id
				AND cn_inner.IndexColumnType = cn.IndexColumnType
				FOR XML PATH('''')
			 ),1,1,'''') AS ReplaceColumnNames
            FROM ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_details AS id
           CROSS APPLY(
						SELECT	LTRIM(RTRIM(v.value(''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Equality'' AS IndexColumnType
						FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id.equality_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
						CROSS APPLY n.nodes(''x'') node(v)
				    UNION ALL
						SELECT	LTRIM(RTRIM(v.value(''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Inequality'' AS IndexColumnType
						FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id.inequality_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
						CROSS APPLY n.nodes(''x'') node(v)
				    UNION ALL
						SELECT	LTRIM(RTRIM(v.value(''(./text())[1]'', ''varchar(max)''))) AS ColumnName, ''Included'' AS IndexColumnType
						FROM	(VALUES (CONVERT(XML, N''<x>'' + REPLACE((SELECT CAST(id.included_columns AS nvarchar(max)) FOR XML PATH('''')), N'','', N''</x><x>'') + N''</x>''))) x(n)
						CROSS APPLY n.nodes(''x'') node(v)
					)AS cn
				GROUP BY	id.index_handle,id.object_id,cn.IndexColumnType
				)
                SELECT  id.database_id, id.object_id, @i_DatabaseName, sc.[name], so.[name], id.statement , gs.avg_total_user_cost, 
                        gs.avg_user_impact, gs.user_seeks, gs.user_scans, gs.unique_compiles, id.equality_columns, id.inequality_columns, id.included_columns,
				(
                    SELECT ColumnNamesWithDataTypes.ReplaceColumnNames 
                    FROM ColumnNamesWithDataTypes WHERE ColumnNamesWithDataTypes.index_handle = id.index_handle
                    AND ColumnNamesWithDataTypes.object_id = id.object_id
                    AND ColumnNamesWithDataTypes.IndexColumnType = ''Equality''
                ) AS equality_columns_with_data_type
                ,(
                    SELECT ColumnNamesWithDataTypes.ReplaceColumnNames 
                    FROM ColumnNamesWithDataTypes WHERE ColumnNamesWithDataTypes.index_handle = id.index_handle
                    AND ColumnNamesWithDataTypes.object_id = id.object_id
                    AND ColumnNamesWithDataTypes.IndexColumnType = ''Inequality''
                ) AS inequality_columns_with_data_type
                ,(
                    SELECT ColumnNamesWithDataTypes.ReplaceColumnNames 
                    FROM ColumnNamesWithDataTypes WHERE ColumnNamesWithDataTypes.index_handle = id.index_handle
                    AND ColumnNamesWithDataTypes.object_id = id.object_id
                    AND ColumnNamesWithDataTypes.IndexColumnType = ''Included''
                ) AS included_columns_with_data_type '

		/* Get the sample query plan if it's available, and if there are less than 1,000 rows in the DMV: */
        IF NOT EXISTS
		(
		    SELECT
			    1/0
			FROM sys.all_objects AS o
			WHERE o.name = 'dm_db_missing_index_group_stats_query'
	    )
            SELECT
                @dsql += N' , NULL AS sample_query_plan '
        ELSE
		BEGIN
            /* The DMV is only supposed to have 600 rows in it. If it's got more,
            they could see performance slowdowns - see Github #3085. */
            DECLARE @MissingIndexPlans BIGINT;
            SET @StringToExecute = N'SELECT @MissingIndexPlans = COUNT(*) FROM ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_group_stats_query;'
            EXEC sp_executesql @StringToExecute, N'@MissingIndexPlans BIGINT OUT', @MissingIndexPlans OUT;

            IF @MissingIndexPlans > 1000
                BEGIN
                SELECT @dsql += N' , NULL AS sample_query_plan /* Over 1000 plans found, skipping */ ';
                RAISERROR (N'Over 1000 plans found in sys.dm_db_missing_index_group_stats_query - your SQL Server is hitting a bug: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/3085',0,1) WITH NOWAIT;
                END
            ELSE
                SELECT
                    @dsql += N'
                , sample_query_plan =
                (
                    SELECT TOP (1)
                        p.query_plan
                    FROM sys.dm_db_missing_index_group_stats gs 
                    CROSS APPLY
                    (
                        SELECT TOP (1)
                            s.plan_handle
                        FROM ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_group_stats_query q 
                        INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.dm_exec_query_stats s
                            ON q.query_plan_hash = s.query_plan_hash
                        WHERE gs.group_handle = q.group_handle 
                        ORDER BY (q.user_seeks + q.user_scans) DESC, s.total_logical_reads DESC
                    ) q2
                    CROSS APPLY sys.dm_exec_query_plan(q2.plan_handle) p
                    WHERE ig.index_group_handle = gs.group_handle
                ) '
		END
        
        

		SET @dsql = @dsql + N'FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_groups ig
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_details id ON ig.index_handle = id.index_handle
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_missing_index_group_stats gs ON ig.index_group_handle = gs.group_handle
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects so on 
                            id.object_id=so.object_id
                        JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas sc on 
                            so.schema_id=sc.schema_id
                WHERE    id.database_id = ' + CAST(@DatabaseID AS NVARCHAR(30)) + '
                ' + CASE WHEN @ObjectID IS NULL THEN N'' 
                    ELSE N'and id.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
                END +
        N'OPTION (RECOMPILE);';

        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
        INSERT    #MissingIndexes ( [database_id], [object_id], [database_name], [schema_name], [table_name], [statement], avg_total_user_cost, 
                                    avg_user_impact, user_seeks, user_scans, unique_compiles, equality_columns, 
                                    inequality_columns, included_columns, equality_columns_with_data_type, inequality_columns_with_data_type, 
                                    included_columns_with_data_type, sample_query_plan)
        EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;

        SET @dsql = N'
            SELECT DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N') AS [database_id], 
			    @i_DatabaseName AS database_name,
				s.name,
                fk_object.name AS foreign_key_name,
                parent_object.[object_id] AS parent_object_id,
                parent_object.name AS parent_object_name,
                referenced_object.[object_id] AS referenced_object_id,
                referenced_object.name AS referenced_object_name,
                fk.is_disabled,
                fk.is_not_trusted,
                fk.is_not_for_replication,
                parent.fk_columns,
                referenced.fk_columns,
                [update_referential_action_desc],
                [delete_referential_action_desc]
            FROM ' + QUOTENAME(@DatabaseName) + N'.sys.foreign_keys fk
            JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects fk_object ON fk.object_id=fk_object.object_id
            JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects parent_object ON fk.parent_object_id=parent_object.object_id
            JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects referenced_object ON fk.referenced_object_id=referenced_object.object_id
			JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s ON fk.schema_id=s.schema_id
            CROSS APPLY ( SELECT  STUFF( (SELECT  N'', '' + c_parent.name AS fk_columns
                                            FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.foreign_key_columns fkc 
                                            JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c_parent ON fkc.parent_object_id=c_parent.[object_id]
                                                AND fkc.parent_column_id=c_parent.column_id
                                            WHERE    fk.parent_object_id=fkc.parent_object_id
                                                AND fk.[object_id]=fkc.constraint_object_id
                                            ORDER BY fkc.constraint_column_id 
                                    FOR      XML PATH('''') ,
                                              TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''')/*This is how we remove the first comma*/ ) parent ( fk_columns )
            CROSS APPLY ( SELECT  STUFF( (SELECT  N'', '' + c_referenced.name AS fk_columns
                                            FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.    foreign_key_columns fkc 
                                            JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c_referenced ON fkc.referenced_object_id=c_referenced.[object_id]
                                                AND fkc.referenced_column_id=c_referenced.column_id
                                            WHERE    fk.referenced_object_id=fkc.referenced_object_id
                                                and fk.[object_id]=fkc.constraint_object_id
                                            ORDER BY fkc.constraint_column_id  /*order by col name, we don''t have anything better*/
                                    FOR      XML PATH('''') ,
                                              TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''') ) referenced ( fk_columns )
            ' + CASE WHEN @ObjectID IS NOT NULL THEN 
                    'WHERE fk.parent_object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' OR fk.referenced_object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' 
                    ELSE N' ' END + '
            ORDER BY parent_object_name, foreign_key_name
			OPTION (RECOMPILE);';
        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #ForeignKeys',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;
        INSERT  #ForeignKeys ( [database_id], [database_name], [schema_name], foreign_key_name, parent_object_id,parent_object_name, referenced_object_id, referenced_object_name,
                                is_disabled, is_not_trusted, is_not_for_replication, parent_fk_columns, referenced_fk_columns,
                                [update_referential_action_desc], [delete_referential_action_desc] )
                EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;


        SET @dsql = N'
                SELECT 
                    DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N') AS [database_id], 
			        @i_DatabaseName AS database_name,
                    foreign_key_schema = 
                        s.name,
                    foreign_key_name = 
                        fk.name,
                    foreign_key_table = 
                        OBJECT_NAME(fk.parent_object_id, DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N')),
					fk.parent_object_id,
					foreign_key_referenced_table = 
                        OBJECT_NAME(fk.referenced_object_id, DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N')),
					fk.referenced_object_id
                FROM ' + QUOTENAME(@DatabaseName) + N'.sys.foreign_keys fk
                JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s
                    ON s.schema_id = fk.schema_id
                WHERE fk.is_disabled = 0
                AND   EXISTS
                      (
                          SELECT  
                              1/0
                          FROM ' + QUOTENAME(@DatabaseName) + N'.sys.foreign_key_columns fkc
                          WHERE fkc.constraint_object_id = fk.object_id
                          AND NOT EXISTS
                              (
                                  SELECT  
                                      1/0
                                  FROM  ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns ic
                                  WHERE ic.object_id = fkc.parent_object_id
                                  AND   ic.column_id = fkc.parent_column_id
                                  AND   ic.index_column_id = fkc.constraint_column_id
                              )
                      )
				OPTION (RECOMPILE);'
        IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #ForeignKeys',0,1) WITH NOWAIT;
        IF @Debug = 1
            BEGIN
                PRINT SUBSTRING(@dsql, 0, 4000);
                PRINT SUBSTRING(@dsql, 4000, 8000);
                PRINT SUBSTRING(@dsql, 8000, 12000);
                PRINT SUBSTRING(@dsql, 12000, 16000);
                PRINT SUBSTRING(@dsql, 16000, 20000);
                PRINT SUBSTRING(@dsql, 20000, 24000);
                PRINT SUBSTRING(@dsql, 24000, 28000);
                PRINT SUBSTRING(@dsql, 28000, 32000);
                PRINT SUBSTRING(@dsql, 32000, 36000);
                PRINT SUBSTRING(@dsql, 36000, 40000);
            END;

        INSERT
		    #UnindexedForeignKeys
        (
            database_id,
            database_name,
            schema_name,
            foreign_key_name,
            parent_object_name,
			parent_object_id,
			referenced_object_name,
			referenced_object_id
        )
        EXEC sys.sp_executesql
            @dsql,
            N'@i_DatabaseName sysname',
            @DatabaseName;


		IF @SkipStatistics = 0 /* AND DB_NAME() = @DatabaseName /* Can only get stats in the current database - see https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1947 */ */
			BEGIN
		IF  ((PARSENAME(@SQLServerProductVersion, 4) >= 12)
		OR   (PARSENAME(@SQLServerProductVersion, 4) = 11 AND PARSENAME(@SQLServerProductVersion, 2) >= 3000)
		OR   (PARSENAME(@SQLServerProductVersion, 4) = 10 AND PARSENAME(@SQLServerProductVersion, 3) = 50 AND PARSENAME(@SQLServerProductVersion, 2) >= 2500))
		BEGIN
		RAISERROR (N'Gathering Statistics Info With Newer Syntax.',0,1) WITH NOWAIT;
		SET @dsql=N'USE ' + @DatabaseName + N'; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
			INSERT #Statistics ( database_id, database_name, table_name, schema_name, index_name, column_names, statistics_name, last_statistics_update, 
								days_since_last_stats_update, rows, rows_sampled, percent_sampled, histogram_steps, modification_counter, 
								percent_modifications, modifications_before_auto_update, index_type_desc, table_create_date, table_modify_date,
								no_recompute, has_filter, filter_definition)
				SELECT DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N') AS [database_id], 
				    @i_DatabaseName AS database_name,
					obj.name AS table_name,
					sch.name AS schema_name,
			        ISNULL(i.name, ''System Or User Statistic'') AS index_name,
			        ca.column_names AS column_names,
			        s.name AS statistics_name,
			        CONVERT(DATETIME, ddsp.last_updated) AS last_statistics_update,
			        DATEDIFF(DAY, ddsp.last_updated, GETDATE()) AS days_since_last_stats_update,
			        ddsp.rows,
			        ddsp.rows_sampled,
			        CAST(ddsp.rows_sampled / ( 1. * NULLIF(ddsp.rows, 0) ) * 100 AS DECIMAL(18, 1)) AS percent_sampled,
			        ddsp.steps AS histogram_steps,
			        ddsp.modification_counter,
			        CASE WHEN ddsp.modification_counter > 0
			             THEN CAST(ddsp.modification_counter / ( 1. * NULLIF(ddsp.rows, 0) ) * 100 AS DECIMAL(18, 1))
			             ELSE ddsp.modification_counter
			        END AS percent_modifications,
			        CASE WHEN ddsp.rows < 500 THEN 500
			             ELSE CAST(( ddsp.rows * .20 ) + 500 AS INT)
			        END AS modifications_before_auto_update,
			        ISNULL(i.type_desc, ''System Or User Statistic - N/A'') AS index_type_desc,
			        CONVERT(DATETIME, obj.create_date) AS table_create_date,
			        CONVERT(DATETIME, obj.modify_date) AS table_modify_date,
					s.no_recompute,
					s.has_filter,
					s.filter_definition
			FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.stats AS s
			JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.objects obj
			ON      s.object_id = obj.object_id
			JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas sch
			ON		sch.schema_id = obj.schema_id
			LEFT JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS i
			ON      i.object_id = s.object_id
			        AND i.index_id = s.stats_id
			OUTER APPLY ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_stats_properties(s.object_id, s.stats_id) AS ddsp
			CROSS APPLY ( SELECT  STUFF((SELECT   '', '' + c.name
						  FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.stats_columns AS sc
						  JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c
						  ON       sc.column_id = c.column_id AND sc.object_id = c.object_id
						  WHERE    sc.stats_id = s.stats_id AND sc.object_id = s.object_id
						  ORDER BY sc.stats_column_id
						  FOR   XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 2, '''') 
						) ca (column_names)
			WHERE obj.is_ms_shipped = 0
			OPTION (RECOMPILE);';
			
			IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

			RAISERROR (N'Inserting data into #Statistics',0,1) WITH NOWAIT;
            IF @Debug = 1
                BEGIN
                    PRINT SUBSTRING(@dsql, 0, 4000);
                    PRINT SUBSTRING(@dsql, 4000, 8000);
                    PRINT SUBSTRING(@dsql, 8000, 12000);
                    PRINT SUBSTRING(@dsql, 12000, 16000);
                    PRINT SUBSTRING(@dsql, 16000, 20000);
                    PRINT SUBSTRING(@dsql, 20000, 24000);
                    PRINT SUBSTRING(@dsql, 24000, 28000);
                    PRINT SUBSTRING(@dsql, 28000, 32000);
                    PRINT SUBSTRING(@dsql, 32000, 36000);
                    PRINT SUBSTRING(@dsql, 36000, 40000);
                END;
			
			EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;
			END;
			ELSE 
			BEGIN
			RAISERROR (N'Gathering Statistics Info With Older Syntax.',0,1) WITH NOWAIT;
			SET @dsql=N'USE ' + @DatabaseName + N'; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
			INSERT #Statistics(database_id, database_name, table_name, schema_name, index_name, column_names, statistics_name, 
								last_statistics_update, days_since_last_stats_update, rows, modification_counter, 
								percent_modifications, modifications_before_auto_update, index_type_desc, table_create_date, table_modify_date,
								no_recompute, has_filter, filter_definition)
							SELECT DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N') AS [database_id], 
							    @i_DatabaseName AS database_name,
								obj.name AS table_name,
								sch.name AS schema_name,
						        ISNULL(i.name, ''System Or User Statistic'') AS index_name,
						        ca.column_names  AS column_names,
						        s.name AS statistics_name,
						        CONVERT(DATETIME, STATS_DATE(s.object_id, s.stats_id)) AS last_statistics_update,
						        DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) AS days_since_last_stats_update,
						        si.rowcnt,
						        si.rowmodctr,
						        CASE WHEN si.rowmodctr > 0 THEN CAST(si.rowmodctr / ( 1. * NULLIF(si.rowcnt, 0) ) * 100 AS DECIMAL(18, 1))
						             ELSE si.rowmodctr
						        END AS percent_modifications,
						        CASE WHEN si.rowcnt < 500 THEN 500
						             ELSE CAST(( si.rowcnt * .20 ) + 500 AS INT)
						        END AS modifications_before_auto_update,
						        ISNULL(i.type_desc, ''System Or User Statistic - N/A'') AS index_type_desc,
						        CONVERT(DATETIME, obj.create_date) AS table_create_date,
						        CONVERT(DATETIME, obj.modify_date) AS table_modify_date,
								s.no_recompute,
								'
								+ CASE WHEN @SQLServerProductVersion NOT LIKE '9%' 
								THEN N's.has_filter,
									   s.filter_definition' 
								ELSE N'NULL AS has_filter,
								       NULL AS filter_definition' END 
						+ N'								
						FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.stats AS s
						INNER HASH JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.sysindexes si
						ON      si.name = s.name AND s.object_id = si.id
						INNER HASH JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.objects obj
						ON      s.object_id = obj.object_id
						INNER HASH JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas sch
						ON		sch.schema_id = obj.schema_id
						LEFT HASH JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS i
						ON      i.object_id = s.object_id
						        AND i.index_id = s.stats_id
						CROSS APPLY ( SELECT  STUFF((SELECT   '', '' + c.name
									  FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.stats_columns AS sc
									  JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c
									  ON       sc.column_id = c.column_id AND sc.object_id = c.object_id
									  WHERE    sc.stats_id = s.stats_id AND sc.object_id = s.object_id
									  ORDER BY sc.stats_column_id
									  FOR   XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 2, '''') 
									) ca (column_names)
						WHERE obj.is_ms_shipped = 0
						AND si.rowcnt > 0
						OPTION (RECOMPILE);';

			IF @dsql IS NULL 
            RAISERROR('@dsql is null',16,1);

			RAISERROR (N'Inserting data into #Statistics',0,1) WITH NOWAIT;
            IF @Debug = 1
                BEGIN
                    PRINT SUBSTRING(@dsql, 0, 4000);
                    PRINT SUBSTRING(@dsql, 4000, 8000);
                    PRINT SUBSTRING(@dsql, 8000, 12000);
                    PRINT SUBSTRING(@dsql, 12000, 16000);
                    PRINT SUBSTRING(@dsql, 16000, 20000);
                    PRINT SUBSTRING(@dsql, 20000, 24000);
                    PRINT SUBSTRING(@dsql, 24000, 28000);
                    PRINT SUBSTRING(@dsql, 28000, 32000);
                    PRINT SUBSTRING(@dsql, 32000, 36000);
                    PRINT SUBSTRING(@dsql, 36000, 40000);
                END;
			
			EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;
			END;

			END;

			IF  (PARSENAME(@SQLServerProductVersion, 4) >= 10)
			BEGIN
			RAISERROR (N'Gathering Computed Column Info.',0,1) WITH NOWAIT;
			SET @dsql=N'SELECT DB_ID(@i_DatabaseName) AS [database_id], 
							   @i_DatabaseName AS database_name,
   					   		   t.name AS table_name,
   					           s.name AS schema_name,
   					           c.name AS column_name,
   					           cc.is_nullable,
   					           cc.definition,
   					           cc.uses_database_collation,
   					           cc.is_persisted,
   					           cc.is_computed,
   					   		   CASE WHEN cc.definition LIKE ''%|].|[%'' ESCAPE ''|'' THEN 1 ELSE 0 END AS is_function,
   					   		   ''ALTER TABLE '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name) + 
   					   		   '' ADD '' + QUOTENAME(c.name) + '' AS '' + cc.definition  + 
							   CASE WHEN is_persisted = 1 THEN '' PERSISTED'' ELSE '''' END + '';'' COLLATE DATABASE_DEFAULT AS [column_definition]
   					   FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.computed_columns AS cc
   					   JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c
   					   ON      cc.object_id = c.object_id
   					   		   AND cc.column_id = c.column_id
   					   JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.tables AS t
   					   ON      t.object_id = cc.object_id
   					   JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s
   					   ON      s.schema_id = t.schema_id
					   OPTION (RECOMPILE);';

			IF @dsql IS NULL RAISERROR('@dsql is null',16,1);

			INSERT #ComputedColumns
			        ( database_id, [database_name], table_name, schema_name, column_name, is_nullable, definition, 
					  uses_database_collation, is_persisted, is_computed, is_function, column_definition )			
			EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;

			END; 
			
			RAISERROR (N'Gathering Trace Flag Information',0,1) WITH NOWAIT;
			INSERT #TraceStatus
			EXEC ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');			

			IF  (PARSENAME(@SQLServerProductVersion, 4) >= 13)
			BEGIN
			RAISERROR (N'Gathering Temporal Table Info',0,1) WITH NOWAIT;
			SET @dsql=N'SELECT ' + QUOTENAME(@DatabaseName,'''') + N' AS database_name,
								   DB_ID(N' + QUOTENAME(@DatabaseName,'''') + N') AS [database_id], 
								   s.name AS schema_name,
								   t.name AS table_name, 
								   oa.hsn as history_schema_name,
								   oa.htn AS history_table_name, 
								   c1.name AS start_column_name,
								   c2.name AS end_column_name,
								   p.name AS period_name
							FROM ' + QUOTENAME(@DatabaseName) + N'.sys.periods AS p
							INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.tables AS t
							ON  p.object_id = t.object_id
							INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c1
							ON  t.object_id = c1.object_id
							    AND p.start_column_id = c1.column_id
							INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c2
							ON  t.object_id = c2.object_id
							    AND p.end_column_id = c2.column_id
							INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s
							ON t.schema_id = s.schema_id
							CROSS APPLY ( SELECT s2.name as hsn, t2.name htn
							              FROM ' + QUOTENAME(@DatabaseName) + N'.sys.tables AS t2
										  INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s2
										  ON t2.schema_id = s2.schema_id
							              WHERE t2.object_id = t.history_table_id
							              AND t2.temporal_type = 1 /*History table*/ ) AS oa
							WHERE t.temporal_type IN ( 2, 4 ) /*BOL currently points to these types, but has no definition for 4*/
							OPTION (RECOMPILE);
							';
			
			IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);
			
			INSERT #TemporalTables ( database_name, database_id, schema_name, table_name, history_schema_name, 
									 history_table_name, start_column_name, end_column_name, period_name )
					
			EXEC sp_executesql @dsql;

             SET @dsql=N'SELECT DB_ID(@i_DatabaseName) AS [database_id], 
             				   @i_DatabaseName AS database_name,
             		   		   t.name AS table_name,
             		           s.name AS schema_name,
             		           cc.name AS constraint_name,
             		           cc.is_disabled,
             		           cc.definition,
             		           cc.uses_database_collation,
             		           cc.is_not_trusted,
             		   		   CASE WHEN cc.definition LIKE ''%|].|[%'' ESCAPE ''|'' THEN 1 ELSE 0 END AS is_function,
             		   		   ''ALTER TABLE '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name) + 
             		   		   '' ADD CONSTRAINT '' + QUOTENAME(cc.name) + '' CHECK '' + cc.definition  + '';'' COLLATE DATABASE_DEFAULT AS [column_definition]
             		   FROM    ' + QUOTENAME(@DatabaseName) + N'.sys.check_constraints AS cc
             		   JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.tables AS t
             		   ON      t.object_id = cc.parent_object_id
             		   JOIN    ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s
             		   ON      s.schema_id = t.schema_id
             		   OPTION (RECOMPILE);';
             
             INSERT #CheckConstraints
                     ( database_id, [database_name], table_name, schema_name, constraint_name, is_disabled, definition, 
             		  uses_database_collation, is_not_trusted, is_function, column_definition )		
             EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;


            SET @dsql=N'SELECT DB_ID(@i_DatabaseName) AS [database_id], 
             				   @i_DatabaseName AS database_name,
                               s.name AS missing_schema_name,
                               t.name AS missing_table_name,
                               i.name AS missing_index_name,
                               c.name AS missing_column_name
                        FROM   ' + QUOTENAME(@DatabaseName) + N'.sys.sql_expression_dependencies AS sed
                        JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.tables AS t
                            ON t.object_id = sed.referenced_id
                        JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS s
                            ON t.schema_id = s.schema_id
                        JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS i
                            ON i.object_id = sed.referenced_id
                            AND i.index_id = sed.referencing_minor_id
                        JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c
                            ON c.object_id = sed.referenced_id
                            AND c.column_id = sed.referenced_minor_id
                        WHERE  sed.referencing_class = 7
                        AND    sed.referenced_class = 1
                        AND    i.has_filter = 1
                        AND    NOT EXISTS (   SELECT 1/0
                                              FROM   ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns AS ic
                                              WHERE  ic.index_id = sed.referencing_minor_id
                                              AND    ic.column_id = sed.referenced_minor_id
                                              AND    ic.object_id = sed.referenced_id )
                        OPTION(RECOMPILE);'

                INSERT #FilteredIndexes ( database_id, database_name, schema_name, table_name, index_name, column_name )
                EXEC sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)', @i_DatabaseName = @DatabaseName;


    END;
			
END;                    
END TRY
BEGIN CATCH
        RAISERROR (N'Failure populating temp tables.', 0,1) WITH NOWAIT;

        IF @dsql IS NOT NULL
        BEGIN
            SET @msg= 'Last @dsql: ' + @dsql;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT  @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
        RAISERROR (@msg,@ErrorSeverity, @ErrorState )WITH NOWAIT;
        
        
        WHILE @@trancount > 0 
            ROLLBACK;

        RETURN;
END CATCH;
 FETCH NEXT FROM c1 INTO @DatabaseName;
END;
DEALLOCATE c1;






----------------------------------------
--STEP 2: PREP THE TEMP TABLES
--EVERY QUERY AFTER THIS GOES AGAINST TEMP TABLES ONLY.
----------------------------------------

RAISERROR (N'Updating #IndexSanity.key_column_names',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        key_column_names = D1.key_column_names
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + c.column_name 
                            + N' {' + system_type_name +
							CASE max_length WHEN -1 THEN N' (max)' ELSE
								CASE  
									WHEN system_type_name IN (N'char',N'varchar',N'binary',N'varbinary') THEN N' (' + CAST(max_length AS NVARCHAR(20)) + N')' 
									WHEN system_type_name IN (N'nchar',N'nvarchar') THEN N' (' + CAST(max_length/2 AS NVARCHAR(20)) + N')' 
									ELSE N' ' + CAST(max_length AS NVARCHAR(50))
								END
							END
							+ N'}'
                                AS col_definition
                            FROM    #IndexColumns c
                            WHERE    c.database_id= si.database_id
									AND c.schema_name = si.schema_name
                                    AND c.object_id = si.object_id
                                    AND c.index_id = si.index_id
                                    AND c.is_included_column = 0 /*Just Keys*/
                                    AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                            ORDER BY c.object_id, c.index_id, c.key_ordinal    
                    FOR      XML PATH('') ,TYPE).value('.', 'nvarchar(max)'), 1, 1, ''))
                                ) D1 ( key_column_names );

RAISERROR (N'Updating #IndexSanity.partition_key_column_name',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        partition_key_column_name = D1.partition_key_column_name
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + c.column_name AS col_definition
                            FROM    #IndexColumns c
                            WHERE    c.database_id= si.database_id
									AND c.schema_name = si.schema_name
                                    AND c.object_id = si.object_id
                                    AND c.index_id = si.index_id
                                    AND c.partition_ordinal <> 0 /*Just Partitioned Keys*/
                            ORDER BY c.object_id, c.index_id, c.key_ordinal    
                    FOR      XML PATH('') , TYPE).value('.', 'nvarchar(max)'), 1, 1,''))) D1 
                                ( partition_key_column_name );

RAISERROR (N'Updating #IndexSanity.key_column_names_with_sort_order',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        key_column_names_with_sort_order = D2.key_column_names_with_sort_order
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + c.column_name + CASE c.is_descending_key
                            WHEN 1 THEN N' DESC'
                            ELSE N''
							END
                            + N' {' + system_type_name +
							CASE max_length WHEN -1 THEN N' (max)' ELSE
								CASE  
									WHEN system_type_name IN (N'char',N'varchar',N'binary',N'varbinary') THEN N' (' + CAST(max_length AS NVARCHAR(20)) + N')' 
									WHEN system_type_name IN (N'nchar',N'nvarchar') THEN N' (' + CAST(max_length/2 AS NVARCHAR(20)) + N')' 
									ELSE N' ' + CAST(max_length AS NVARCHAR(50))
								END
							END
							+ N'}'
                                AS col_definition
                    FROM    #IndexColumns c
                    WHERE    c.database_id= si.database_id
							AND c.schema_name = si.schema_name
                            AND c.object_id = si.object_id
                            AND c.index_id = si.index_id
                            AND c.is_included_column = 0 /*Just Keys*/
                            AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                    ORDER BY c.object_id, c.index_id, c.key_ordinal    
            FOR      XML PATH('') , TYPE).value('.', 'nvarchar(max)'), 1, 1, ''))
            ) D2 ( key_column_names_with_sort_order );

RAISERROR (N'Updating #IndexSanity.key_column_names_with_sort_order_no_types (for create tsql)',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        key_column_names_with_sort_order_no_types = D2.key_column_names_with_sort_order_no_types
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + QUOTENAME(c.column_name) + CASE c.is_descending_key
                            WHEN 1 THEN N' DESC'
                            ELSE N''
                        END AS col_definition
                    FROM    #IndexColumns c
                    WHERE    c.database_id= si.database_id
							AND c.schema_name = si.schema_name
                            AND c.object_id = si.object_id
                            AND c.index_id = si.index_id
                            AND c.is_included_column = 0 /*Just Keys*/
                            AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                    ORDER BY c.object_id, c.index_id, c.key_ordinal    
            FOR      XML PATH('') , TYPE).value('.', 'nvarchar(max)'), 1, 1, ''))
            ) D2 ( key_column_names_with_sort_order_no_types );

RAISERROR (N'Updating #IndexSanity.include_column_names',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        include_column_names = D3.include_column_names
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + c.column_name
								+ N' {' + system_type_name +
								CASE max_length WHEN -1 THEN N' (max)' ELSE
									CASE
										WHEN system_type_name IN (N'char',N'varchar',N'binary',N'varbinary') THEN N' (' + CAST(max_length AS NVARCHAR(20)) + N')'
										WHEN system_type_name IN (N'nchar',N'nvarchar') THEN N' (' + CAST(max_length/2 AS NVARCHAR(20)) + N')'
										ELSE N' ' + CAST(max_length AS NVARCHAR(50))
									END
								END
								+ N'}'
                        FROM    #IndexColumns c
                        WHERE    c.database_id= si.database_id
								AND c.schema_name = si.schema_name
                                AND c.object_id = si.object_id
                                AND c.index_id = si.index_id
                                AND c.is_included_column = 1 /*Just includes*/
                        ORDER BY c.column_name /*Order doesn't matter in includes, 
                                this is here to make rows easy to compare.*/ 
                FOR      XML PATH('') ,  TYPE).value('.', 'nvarchar(max)'), 1, 1, ''))
                ) D3 ( include_column_names );

RAISERROR (N'Updating #IndexSanity.include_column_names_no_types (for create tsql)',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        include_column_names_no_types = D3.include_column_names_no_types
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + QUOTENAME(c.column_name)
                        FROM    #IndexColumns c
                                WHERE    c.database_id= si.database_id
								AND c.schema_name = si.schema_name
                                AND c.object_id = si.object_id
                                AND c.index_id = si.index_id
                                AND c.is_included_column = 1 /*Just includes*/
                        ORDER BY c.column_name /*Order doesn't matter in includes, 
                                this is here to make rows easy to compare.*/ 
                FOR      XML PATH('') ,  TYPE).value('.', 'nvarchar(max)'), 1, 1, ''))
                ) D3 ( include_column_names_no_types );

RAISERROR (N'Updating #IndexSanity.count_key_columns and count_include_columns',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        count_included_columns = D4.count_included_columns,
        count_key_columns = D4.count_key_columns
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  SUM(CASE WHEN is_included_column = 'true' THEN 1
                                            ELSE 0
                                    END) AS count_included_columns,
                                SUM(CASE WHEN is_included_column = 'false' AND c.key_ordinal > 0 THEN 1
                                            ELSE 0
                                    END) AS count_key_columns
                        FROM        #IndexColumns c
                            WHERE    c.database_id= si.database_id
									AND c.schema_name = si.schema_name
                                    AND c.object_id = si.object_id
                                AND c.index_id = si.index_id 
                                ) AS D4 ( count_included_columns, count_key_columns );

RAISERROR (N'Updating index_sanity_id on #IndexPartitionSanity',0,1) WITH NOWAIT;
UPDATE    #IndexPartitionSanity
SET        index_sanity_id = i.index_sanity_id
FROM #IndexPartitionSanity ps
        JOIN #IndexSanity i ON ps.[object_id] = i.[object_id]
                                AND ps.index_id = i.index_id
                                AND i.database_id = ps.database_id
								AND i.schema_name = ps.schema_name;


RAISERROR (N'Inserting data into #IndexSanitySize',0,1) WITH NOWAIT;
INSERT    #IndexSanitySize ( [index_sanity_id], [database_id], [schema_name], [lock_escalation_desc], partition_count, total_rows, total_reserved_MB,
                                total_reserved_LOB_MB, total_reserved_row_overflow_MB, total_reserved_dictionary_MB, total_range_scan_count,
                                total_singleton_lookup_count, total_leaf_delete_count, total_leaf_update_count, 
                                total_forwarded_fetch_count,total_row_lock_count,
                                total_row_lock_wait_count, total_row_lock_wait_in_ms, avg_row_lock_wait_in_ms,
                                total_page_lock_count, total_page_lock_wait_count, total_page_lock_wait_in_ms,
                                avg_page_lock_wait_in_ms, total_index_lock_promotion_attempt_count, 
                                total_index_lock_promotion_count, data_compression_desc, 
								page_latch_wait_count, page_latch_wait_in_ms, page_io_latch_wait_count, page_io_latch_wait_in_ms)
        SELECT  index_sanity_id, ipp.database_id, ipp.schema_name, ipp.lock_escalation_desc,						
				COUNT(*), SUM(row_count), SUM(reserved_MB),
				SUM(reserved_LOB_MB) - SUM(reserved_dictionary_MB), /* Subtract columnstore dictionaries from LOB data */
                SUM(reserved_row_overflow_MB), 
                SUM(reserved_dictionary_MB), 
                SUM(range_scan_count),
                SUM(singleton_lookup_count),
                SUM(leaf_delete_count), 
                SUM(leaf_update_count),
                SUM(forwarded_fetch_count),
                SUM(row_lock_count), 
                SUM(row_lock_wait_count),
                SUM(row_lock_wait_in_ms), 
                CASE WHEN SUM(row_lock_wait_in_ms) > 0 THEN
                    SUM(row_lock_wait_in_ms)/(1.*SUM(row_lock_wait_count))
                ELSE 0 END AS avg_row_lock_wait_in_ms,           
                SUM(page_lock_count), 
                SUM(page_lock_wait_count),
                SUM(page_lock_wait_in_ms), 
                CASE WHEN SUM(page_lock_wait_in_ms) > 0 THEN
                    SUM(page_lock_wait_in_ms)/(1.*SUM(page_lock_wait_count))
                ELSE 0 END AS avg_page_lock_wait_in_ms,           
                SUM(index_lock_promotion_attempt_count),
                SUM(index_lock_promotion_count),
                LEFT(MAX(data_compression_info.data_compression_rollup),4000),
				SUM(page_latch_wait_count), 
				SUM(page_latch_wait_in_ms), 
				SUM(page_io_latch_wait_count), 
				SUM(page_io_latch_wait_in_ms)
        FROM #IndexPartitionSanity ipp
        /* individual partitions can have distinct compression settings, just roll them into a list here*/
        OUTER APPLY (SELECT STUFF((
            SELECT  N', ' + data_compression_desc
            FROM #IndexPartitionSanity ipp2
            WHERE ipp.[object_id]=ipp2.[object_id]
                AND ipp.[index_id]=ipp2.[index_id]
                AND ipp.database_id = ipp2.database_id
				AND ipp.schema_name = ipp2.schema_name
            ORDER BY ipp2.partition_number
            FOR      XML PATH(''),TYPE).value('.', 'nvarchar(max)'), 1, 1, '')) 
                data_compression_info(data_compression_rollup)
        GROUP BY index_sanity_id, ipp.database_id, ipp.schema_name, ipp.lock_escalation_desc
        ORDER BY index_sanity_id 
OPTION    ( RECOMPILE );

RAISERROR (N'Determining index usefulness',0,1) WITH NOWAIT;
UPDATE #MissingIndexes 
SET is_low = CASE WHEN (user_seeks + user_scans) < 5000 
					    OR unique_compiles = 1
				  THEN 1
				  ELSE 0 
			  END;

RAISERROR (N'Updating #IndexSanity.referenced_by_foreign_key',0,1) WITH NOWAIT;
UPDATE #IndexSanity
    SET is_referenced_by_foreign_key=1
FROM #IndexSanity s
JOIN #ForeignKeys fk ON 
    s.object_id=fk.referenced_object_id
    AND s.database_id=fk.database_id
    AND LEFT(s.key_column_names,LEN(fk.referenced_fk_columns)) = fk.referenced_fk_columns;

RAISERROR (N'Update index_secret on #IndexSanity for NC indexes.',0,1) WITH NOWAIT;
UPDATE nc 
SET secret_columns=
    N'[' + 
    CASE tb.count_key_columns WHEN 0 THEN '1' ELSE CAST(tb.count_key_columns AS NVARCHAR(10)) END +
    CASE nc.is_unique WHEN 1 THEN N' INCLUDE' ELSE N' KEY' END +
    CASE WHEN tb.count_key_columns > 1 THEN  N'S] ' ELSE N'] ' END +
    CASE tb.index_id WHEN 0 THEN '[RID]' ELSE LTRIM(tb.key_column_names) +
        /* Uniquifiers only needed on non-unique clustereds-- not heaps */
        CASE tb.is_unique WHEN 0 THEN ' [UNIQUIFIER]' ELSE N'' END
    END
    , count_secret_columns=
    CASE tb.index_id WHEN 0 THEN 1 ELSE 
        tb.count_key_columns +
            CASE tb.is_unique WHEN 0 THEN 1 ELSE 0 END
    END
FROM #IndexSanity AS nc
JOIN #IndexSanity AS tb ON nc.object_id=tb.object_id
	AND nc.database_id = tb.database_id
	AND nc.schema_name = tb.schema_name
    AND tb.index_id IN (0,1) 
WHERE nc.index_id > 1;

RAISERROR (N'Update index_secret on #IndexSanity for heaps and non-unique clustered.',0,1) WITH NOWAIT;
UPDATE tb
SET secret_columns=    CASE tb.index_id WHEN 0 THEN '[RID]' ELSE '[UNIQUIFIER]' END
    , count_secret_columns = 1
FROM #IndexSanity AS tb
WHERE tb.index_id = 0 /*Heaps-- these have the RID */
    OR (tb.index_id=1 AND tb.is_unique=0); /* Non-unique CX: has uniquifer (when needed) */


RAISERROR (N'Populate #IndexCreateTsql.',0,1) WITH NOWAIT;
INSERT #IndexCreateTsql (index_sanity_id, create_tsql)
SELECT
    index_sanity_id,
    ISNULL (
    CASE index_id WHEN 0 THEN N'ALTER TABLE ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name])  + ' REBUILD;'
    ELSE 
        CASE WHEN is_XML = 1 OR is_spatial = 1 OR is_in_memory_oltp = 1 THEN N'' /* Not even trying for these just yet...*/
        ELSE 
            CASE WHEN is_primary_key=1 THEN
                N'ALTER TABLE ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) +
                    N'.' + QUOTENAME([object_name]) + 
                    N' ADD CONSTRAINT [' +
                    index_name + 
                    N'] PRIMARY KEY ' + 
                    CASE WHEN index_id=1 THEN N'CLUSTERED (' ELSE N'(' END +
                    key_column_names_with_sort_order_no_types + N' )' 
				WHEN is_unique_constraint = 1 AND is_primary_key = 0
				THEN
			   N'ALTER TABLE ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) +
                    N'.' + QUOTENAME([object_name]) + 
                    N' ADD CONSTRAINT [' +
                    index_name + 
                    N'] UNIQUE ' + 
                    CASE WHEN index_id=1 THEN N'CLUSTERED (' ELSE N'(' END +
                    key_column_names_with_sort_order_no_types + N' )' 
				WHEN is_CX_columnstore= 1 THEN
                        N'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(index_name) + N' on ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name])
            ELSE /*Else not a PK or cx columnstore */ 
                N'CREATE ' + 
                CASE WHEN is_unique=1 THEN N'UNIQUE ' ELSE N'' END +
                CASE WHEN index_id=1 THEN N'CLUSTERED ' ELSE N'' END +
                CASE WHEN is_NC_columnstore=1 THEN N'NONCLUSTERED COLUMNSTORE ' 
                ELSE N'' END +
                N'INDEX ['
                        + index_name + N'] ON ' + 
                    QUOTENAME([database_name]) + N'.' + 
                    QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name]) + 
                        CASE WHEN is_NC_columnstore=1 THEN 
                            N' (' + ISNULL(include_column_names_no_types,'') +  N' )' 
                        ELSE /*Else not columnstore */ 
                            N' (' + ISNULL(key_column_names_with_sort_order_no_types,'') +  N' )' 
                            + CASE WHEN include_column_names_no_types IS NOT NULL THEN 
                                N' INCLUDE (' + include_column_names_no_types + N')' 
                                ELSE N'' 
                            END
                        END /*End non-columnstore case */ 
                    + CASE WHEN filter_definition <> N'' THEN N' WHERE ' + filter_definition ELSE N'' END
                END /*End Non-PK index CASE */ 
            + CASE WHEN is_NC_columnstore=0 AND is_CX_columnstore=0 THEN
                N' WITH (' 
                    + N'FILLFACTOR=' + CASE fill_factor WHEN 0 THEN N'100' ELSE CAST(fill_factor AS NVARCHAR(5)) END + ', '
                    + N'ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?'
                + N')'
            ELSE N'' END
            + N';'
            END /*End non-spatial and non-xml CASE */ 
    END, '[Unknown Error]')
        AS create_tsql
FROM #IndexSanity;
	  
RAISERROR (N'Populate #PartitionCompressionInfo.',0,1) WITH NOWAIT;
WITH maps
    AS
     (
         SELECT ips.index_sanity_id,
                ips.partition_number,
                ips.data_compression_desc,
                ips.partition_number - ROW_NUMBER() OVER ( PARTITION BY ips.index_sanity_id, ips.data_compression_desc
                                                           ORDER BY ips.partition_number ) AS rn
         FROM   #IndexPartitionSanity AS ips
     )
SELECT *
INTO   #maps
FROM   maps;

WITH grps
    AS
     (
         SELECT   MIN(maps.partition_number) AS MinKey,
                  MAX(maps.partition_number) AS MaxKey,
                  maps.index_sanity_id,
                  maps.data_compression_desc
         FROM     #maps AS maps
         GROUP BY maps.rn, maps.index_sanity_id, maps.data_compression_desc
     )
SELECT *
INTO   #grps
FROM   grps;

INSERT #PartitionCompressionInfo ( index_sanity_id, partition_compression_detail )
SELECT DISTINCT
       grps.index_sanity_id,
       SUBSTRING(
           ( STUFF(
                 (   SELECT   N', ' + N' Partition'
                              + CASE
                                     WHEN grps2.MinKey < grps2.MaxKey
                                     THEN
                                     + N's ' + CAST(grps2.MinKey AS NVARCHAR(10)) + N' - '
                                     + CAST(grps2.MaxKey AS NVARCHAR(10)) + N' use ' + grps2.data_compression_desc
                                     ELSE
                                     N' ' + CAST(grps2.MinKey AS NVARCHAR(10)) + N' uses ' + grps2.data_compression_desc
                                END AS Partitions
                     FROM     #grps AS grps2
                     WHERE    grps2.index_sanity_id = grps.index_sanity_id
                     ORDER BY grps2.MinKey, grps2.MaxKey
                     FOR XML PATH(''), TYPE ).value('.', 'NVARCHAR(MAX)'), 1, 1, '')), 0, 8000) AS partition_compression_detail
FROM   #grps AS grps;
		
RAISERROR (N'Update #PartitionCompressionInfo.',0,1) WITH NOWAIT;
UPDATE sz
SET sz.data_compression_desc = pci.partition_compression_detail
FROM #IndexSanitySize sz
JOIN #PartitionCompressionInfo AS pci
ON pci.index_sanity_id = sz.index_sanity_id;

RAISERROR (N'Update #IndexSanity for filtered indexes with columns not in the index definition.',0,1) WITH NOWAIT;
UPDATE    #IndexSanity
SET        filter_columns_not_in_index = D1.filter_columns_not_in_index
FROM    #IndexSanity si
        CROSS APPLY ( SELECT  RTRIM(STUFF( (SELECT  N', ' + c.column_name AS col_definition
                            FROM    #FilteredIndexes AS c
                            WHERE    c.database_id= si.database_id
									AND c.schema_name = si.schema_name
                                    AND c.table_name = si.object_name
                                    AND c.index_name = si.index_name   
                                    ORDER BY c.index_sanity_id
                    FOR      XML PATH('') , TYPE).value('.', 'nvarchar(max)'), 1, 1,''))) D1 
                                ( filter_columns_not_in_index );


IF @Debug = 1
BEGIN
    SELECT '#BlitzIndexResults' AS table_name, * FROM  #BlitzIndexResults AS bir;
    SELECT '#IndexSanity' AS table_name, * FROM  #IndexSanity;
    SELECT '#IndexPartitionSanity' AS table_name, * FROM  #IndexPartitionSanity;
    SELECT '#IndexSanitySize' AS table_name, * FROM  #IndexSanitySize;
    SELECT '#IndexColumns' AS table_name, * FROM  #IndexColumns;
    SELECT '#MissingIndexes' AS table_name, * FROM  #MissingIndexes;
    SELECT '#ForeignKeys' AS table_name, * FROM  #ForeignKeys;
	SELECT '#UnindexedForeignKeys' AS table_name, * FROM  #UnindexedForeignKeys;
    SELECT '#BlitzIndexResults' AS table_name, * FROM  #BlitzIndexResults;
    SELECT '#IndexCreateTsql' AS table_name, * FROM  #IndexCreateTsql;
    SELECT '#DatabaseList' AS table_name, * FROM  #DatabaseList;
    SELECT '#Statistics' AS table_name, * FROM  #Statistics;
    SELECT '#PartitionCompressionInfo' AS table_name, * FROM  #PartitionCompressionInfo;
    SELECT '#ComputedColumns' AS table_name, * FROM  #ComputedColumns;
    SELECT '#TraceStatus' AS table_name, * FROM  #TraceStatus;   
    SELECT '#CheckConstraints' AS table_name, * FROM  #CheckConstraints;   
    SELECT '#FilteredIndexes' AS table_name, * FROM  #FilteredIndexes;                   
END


----------------------------------------
--STEP 3: DIAGNOSE THE PATIENT
----------------------------------------


BEGIN TRY
----------------------------------------
--If @TableName is specified, just return information for that table.
--The @Mode parameter doesn't matter if you're looking at a specific table.
----------------------------------------
IF @TableName IS NOT NULL
BEGIN
    RAISERROR(N'@TableName specified, giving detail only on that table.', 0,1) WITH NOWAIT;

    --We do a left join here in case this is a disabled NC.
    --In that case, it won't have any size info/pages allocated.
 
   	IF (@ShowColumnstoreOnly = 0)
	BEGIN
	   WITH table_mode_cte AS (
        SELECT 
            s.db_schema_object_indexid, 
            s.key_column_names,
            s.index_definition, 
            ISNULL(s.secret_columns,N'') AS secret_columns,
            s.fill_factor,
            s.index_usage_summary, 
            sz.index_op_stats,
            ISNULL(sz.index_size_summary,'') /*disabled NCs will be null*/ AS index_size_summary,
			partition_compression_detail ,
            ISNULL(sz.index_lock_wait_summary,'') AS index_lock_wait_summary,
            s.is_referenced_by_foreign_key,
            (SELECT COUNT(*)
                FROM #ForeignKeys fk WHERE fk.parent_object_id=s.object_id
                AND PATINDEX (fk.parent_fk_columns, s.key_column_names)=1) AS FKs_covered_by_index,
            s.last_user_seek,
            s.last_user_scan,
            s.last_user_lookup,
            s.last_user_update,
            s.create_date,
            s.modify_date,
			sz.page_latch_wait_count,
			CONVERT(VARCHAR(10), (sz.page_latch_wait_in_ms / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (sz.page_latch_wait_in_ms / 1000), 0), 108) AS page_latch_wait_time,
			sz.page_io_latch_wait_count,
			CONVERT(VARCHAR(10), (sz.page_io_latch_wait_in_ms / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (sz.page_io_latch_wait_in_ms / 1000), 0), 108) AS page_io_latch_wait_time,
            ct.create_tsql,
            CASE 
                WHEN s.is_primary_key = 1 AND s.index_definition <> '[HEAP]'
                THEN N'--ALTER TABLE ' + QUOTENAME(s.[database_name]) + N'.' + QUOTENAME(s.[schema_name]) + N'.' + QUOTENAME(s.[object_name])
                        + N' DROP CONSTRAINT ' + QUOTENAME(s.index_name) + N';'
                WHEN s.is_primary_key = 0 AND is_unique_constraint = 1 AND s.index_definition <> '[HEAP]'
                THEN N'--ALTER TABLE ' + QUOTENAME(s.[database_name]) + N'.' + QUOTENAME(s.[schema_name]) + N'.' + QUOTENAME(s.[object_name])
                        + N' DROP CONSTRAINT ' + QUOTENAME(s.index_name) + N';'
                WHEN s.is_primary_key = 0 AND s.index_definition <> '[HEAP]'
                    THEN N'--DROP INDEX '+ QUOTENAME(s.index_name) + N' ON ' + QUOTENAME(s.[database_name]) + N'.' + 
                        QUOTENAME(s.[schema_name]) + N'.' + QUOTENAME(s.[object_name]) + N';'
                ELSE N''
            END AS drop_tsql,
            1 AS display_order
        FROM #IndexSanity s
        LEFT JOIN #IndexSanitySize sz ON 
            s.index_sanity_id=sz.index_sanity_id
        LEFT JOIN #IndexCreateTsql ct ON 
            s.index_sanity_id=ct.index_sanity_id
		LEFT JOIN #PartitionCompressionInfo pci ON 
			pci.index_sanity_id = s.index_sanity_id
        WHERE s.[object_id]=@ObjectID
        UNION ALL
        SELECT  N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + CONVERT(NVARCHAR(16),GETDATE(),121) +             
                N' (' + @ScriptVersionName + ')' ,   
                N'SQL Server First Responder Kit' ,   
                N'http://FirstResponderKit.org' ,
                N'From Your Community Volunteers',
                NULL,@DaysUptimeInsertValue,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                0 AS display_order
    )
    SELECT 
            db_schema_object_indexid AS [Details: db_schema.table.index(indexid)], 
            index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
            secret_columns AS [Secret Columns],
            fill_factor AS [Fillfactor],
            index_usage_summary AS [Usage Stats], 
            index_op_stats AS [Op Stats],
            index_size_summary AS [Size],
			partition_compression_detail AS [Compression Type],
            index_lock_wait_summary AS [Lock Waits],
            is_referenced_by_foreign_key AS [Referenced by FK?],
            FKs_covered_by_index AS [FK Covered by Index?],
            last_user_seek AS [Last User Seek],
            last_user_scan AS [Last User Scan],
            last_user_lookup AS [Last User Lookup],
            last_user_update AS [Last User Write],
            create_date AS [Created],
            modify_date AS [Last Modified],
			page_latch_wait_count AS [Page Latch Wait Count],
			page_latch_wait_time as [Page Latch Wait Time (D:H:M:S)],
			page_io_latch_wait_count AS [Page IO Latch Wait Count],								
			page_io_latch_wait_time as [Page IO Latch Wait Time (D:H:M:S)],
            create_tsql AS [Create TSQL],
            drop_tsql AS [Drop TSQL]
    FROM table_mode_cte
    ORDER BY display_order ASC, key_column_names ASC
    OPTION    ( RECOMPILE );                        

    IF (SELECT TOP 1 [object_id] FROM    #MissingIndexes mi) IS NOT NULL
    BEGIN;

	WITH create_date AS (
						SELECT i.database_id,
							   i.schema_name,
							   i.[object_id], 
							   ISNULL(NULLIF(MAX(DATEDIFF(DAY, i.create_date, SYSDATETIME())), 0), 1) AS create_days
						FROM #IndexSanity AS i
						GROUP BY i.database_id, i.schema_name, i.object_id
						)
        SELECT  N'Missing index.' AS Finding ,
                N'https://www.brentozar.com/go/Indexaphobia' AS URL ,
                mi.[statement] + 
                ' Est. Benefit: '
                    + CASE WHEN magic_benefit_number >= 922337203685477 THEN '>= 922,337,203,685,477'
                    ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(
                                        (magic_benefit_number / CASE WHEN cd.create_days < @DaysUptime THEN cd.create_days ELSE @DaysUptime END)
                                        AS BIGINT) AS MONEY), 1), '.00', '')
                    END AS [Estimated Benefit],
                missing_index_details AS [Missing Index Request] ,
                index_estimated_impact AS [Estimated Impact],
                create_tsql AS [Create TSQL],
				sample_query_plan AS [Sample Query Plan]
        FROM    #MissingIndexes mi
		LEFT JOIN create_date AS cd
		ON mi.[object_id] =  cd.object_id 
		AND mi.database_id = cd.database_id
		AND mi.schema_name = cd.schema_name
        WHERE   mi.[object_id] = @ObjectID
        AND (@ShowAllMissingIndexRequests=1
                /* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
            OR (magic_benefit_number / CASE WHEN cd.create_days < @DaysUptime THEN cd.create_days ELSE @DaysUptime END) >= 100000)
        ORDER BY magic_benefit_number DESC
        OPTION    ( RECOMPILE );
    END;       
    ELSE     
    SELECT 'No missing indexes.' AS finding;

    SELECT   
        column_name AS [Column Name],
        (SELECT COUNT(*)  
            FROM #IndexColumns c2 
            WHERE c2.column_name=c.column_name
            AND c2.key_ordinal IS NOT NULL)
        + CASE WHEN c.index_id = 1 AND c.key_ordinal IS NOT NULL THEN
            -1+ (SELECT COUNT(DISTINCT index_id)
            FROM #IndexColumns c3
            WHERE c3.index_id NOT IN (0,1))
            ELSE 0 END
                AS [Found In],
        system_type_name + 
            CASE max_length WHEN -1 THEN N' (max)' ELSE
                CASE  
                    WHEN system_type_name IN (N'char',N'varchar',N'binary',N'varbinary') THEN N' (' + CAST(max_length AS NVARCHAR(20)) + N')' 
                    WHEN system_type_name IN (N'nchar',N'nvarchar') THEN N' (' + CAST(max_length/2 AS NVARCHAR(20)) + N')' 
                    ELSE '' 
                END
            END
            AS [Type],
        CASE is_computed WHEN 1 THEN 'yes' ELSE '' END AS [Computed?],
        max_length AS [Length (max bytes)],
        [precision] AS [Prec],
        [scale] AS [Scale],
        CASE is_nullable WHEN 1 THEN 'yes' ELSE '' END AS [Nullable?],
        CASE is_identity WHEN 1 THEN 'yes' ELSE '' END AS [Identity?],
        CASE is_replicated WHEN 1 THEN 'yes' ELSE '' END AS [Replicated?],
        CASE is_sparse WHEN 1 THEN 'yes' ELSE '' END AS [Sparse?],
        CASE is_filestream WHEN 1 THEN 'yes' ELSE '' END AS [Filestream?],
        collation_name AS [Collation]
    FROM #IndexColumns AS c
    WHERE index_id IN (0,1);

    IF (SELECT TOP 1 parent_object_id FROM #ForeignKeys) IS NOT NULL
    BEGIN
        SELECT [database_name] + N':' + parent_object_name + N': ' + foreign_key_name AS [Foreign Key],
            parent_fk_columns AS [Foreign Key Columns],
            referenced_object_name AS [Referenced Table],
            referenced_fk_columns AS [Referenced Table Columns],
            is_disabled AS [Is Disabled?],
            is_not_trusted AS [Not Trusted?],
            is_not_for_replication [Not for Replication?],
            [update_referential_action_desc] AS [Cascading Updates?],
            [delete_referential_action_desc] AS [Cascading Deletes?]
        FROM #ForeignKeys
        ORDER BY [Foreign Key]
        OPTION    ( RECOMPILE );
    END;
    ELSE
    SELECT 'No foreign keys.' AS finding;

    /* Show histograms for all stats on this table. More info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1900 */
    IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_db_stats_histogram')
    BEGIN
        SET @dsql = N'USE ' + QUOTENAME(@DatabaseName) + N';
					SELECT s.name AS [Stat Name], c.name AS [Leading Column Name], hist.step_number AS [Step Number], 
                        hist.range_high_key AS [Range High Key], hist.range_rows AS [Range Rows], 
                        hist.equal_rows AS [Equal Rows], hist.distinct_range_rows AS [Distinct Range Rows], hist.average_range_rows AS [Average Range Rows],
                        s.auto_created AS [Auto-Created], s.user_created AS [User-Created],
                        props.last_updated AS [Last Updated], props.modification_counter AS [Modification Counter], props.rows AS [Table Rows],
						props.rows_sampled AS [Rows Sampled], s.stats_id AS [StatsID]
                    FROM sys.stats AS s
                    INNER JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id AND sc.stats_column_id = 1
                    INNER JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
                    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS props  
                    CROSS APPLY sys.dm_db_stats_histogram(s.[object_id], s.stats_id) AS hist
                    WHERE s.object_id = @ObjectID
                    ORDER BY s.auto_created, s.user_created, s.name, hist.step_number;';
        EXEC sp_executesql @dsql, N'@ObjectID INT', @ObjectID;
     END
	END

    /* Visualize columnstore index contents. More info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2584 */
    IF 2 = (SELECT SUM(1) FROM sys.all_objects WHERE name IN ('column_store_row_groups','column_store_segments'))
    BEGIN
        RAISERROR(N'Visualizing columnstore index contents.', 0,1) WITH NOWAIT;

		SET @dsql = N'USE ' + QUOTENAME(@DatabaseName) + N'; 
			IF EXISTS(SELECT * FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_row_groups WHERE object_id = @ObjectID)
				BEGIN
				SELECT @ColumnList = N'''', @ColumnListWithApostrophes = N'''';
				WITH DistinctColumns AS (
				SELECT DISTINCT QUOTENAME(c.name) AS column_name, QUOTENAME(c.name,'''''''') AS ColumnNameWithApostrophes, c.column_id
					FROM ' + QUOTENAME(@DatabaseName) + N'.sys.partitions p
					INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c ON p.object_id = c.object_id
                    INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns ic on ic.column_id = c.column_id and ic.object_id = c.object_id AND ic.index_id = p.index_id
					INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.types t ON c.system_type_id = t.system_type_id AND c.user_type_id = t.user_type_id
					WHERE p.object_id = @ObjectID
					AND EXISTS (SELECT * FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_segments seg WHERE p.partition_id = seg.partition_id AND seg.column_id = ic.index_column_id)
					AND p.data_compression IN (3,4)
				)
				SELECT @ColumnList = @ColumnList + column_name + N'', '',
					@ColumnListWithApostrophes = @ColumnListWithApostrophes + ColumnNameWithApostrophes + N'', ''
				FROM DistinctColumns
				ORDER BY column_id;
				SELECT @PartitionCount = COUNT(1) FROM ' + QUOTENAME(@DatabaseName) + N'.sys.partitions WHERE object_id = @ObjectID AND data_compression IN (3,4);
				END';

		IF @Debug = 1
			BEGIN
				PRINT SUBSTRING(@dsql, 0, 4000);
				PRINT SUBSTRING(@dsql, 4000, 8000);
				PRINT SUBSTRING(@dsql, 8000, 12000);
				PRINT SUBSTRING(@dsql, 12000, 16000);
				PRINT SUBSTRING(@dsql, 16000, 20000);
				PRINT SUBSTRING(@dsql, 20000, 24000);
				PRINT SUBSTRING(@dsql, 24000, 28000);
				PRINT SUBSTRING(@dsql, 28000, 32000);
				PRINT SUBSTRING(@dsql, 32000, 36000);
				PRINT SUBSTRING(@dsql, 36000, 40000);
			END;

        EXEC sp_executesql @dsql, N'@ObjectID INT, @ColumnList NVARCHAR(MAX) OUTPUT, @ColumnListWithApostrophes NVARCHAR(MAX) OUTPUT, @PartitionCount INT OUTPUT', @ObjectID, @ColumnList OUTPUT, @ColumnListWithApostrophes OUTPUT, @PartitionCount OUTPUT;

		IF @PartitionCount < 2
			SET @ShowPartitionRanges = 0;

		IF @Debug = 1
			SELECT @ColumnList AS ColumnstoreColumnList, @ColumnListWithApostrophes AS ColumnstoreColumnListWithApostrophes, @PartitionCount AS PartitionCount, @ShowPartitionRanges AS ShowPartitionRanges;

		IF @ColumnList <> ''
		BEGIN
			/* Remove the trailing comma */
			SET @ColumnList = LEFT(@ColumnList, LEN(@ColumnList) - 1);
			SET @ColumnListWithApostrophes = LEFT(@ColumnListWithApostrophes, LEN(@ColumnListWithApostrophes) - 1);

			SET @dsql = N'USE ' + QUOTENAME(@DatabaseName) + N'; 
				SELECT partition_number, '
				+ CASE WHEN @ShowPartitionRanges = 1 THEN N' COALESCE(range_start_op + '' '' + range_start + '' '', '''') + COALESCE(range_end_op + '' '' + range_end, '''') AS partition_range, ' ELSE N' ' END
				+ N' row_group_id, total_rows, deleted_rows, ' 
				+ @ColumnList
				+ CASE WHEN @ShowPartitionRanges = 1 THEN N'
				FROM (
					SELECT column_name, partition_number, row_group_id, total_rows, deleted_rows, details,
						range_start_op,
						CASE
							WHEN format_type IS NULL THEN CAST(range_start_value AS NVARCHAR(4000))
							ELSE CONVERT(NVARCHAR(4000), range_start_value, format_type) END range_start,
						range_end_op,
						CASE
							WHEN format_type IS NULL THEN CAST(range_end_value AS NVARCHAR(4000))
							ELSE CONVERT(NVARCHAR(4000), range_end_value, format_type) END range_end' ELSE N' ' END + N'
					FROM (
						SELECT c.name AS column_name, p.partition_number, rg.row_group_id, rg.total_rows, rg.deleted_rows,
							details = CAST(seg.min_data_id AS VARCHAR(20)) + '' to '' + CAST(seg.max_data_id AS VARCHAR(20)) + '', '' + CAST(CAST((seg.on_disk_size / 1024.0 / 1024) AS DECIMAL(18,0)) AS VARCHAR(20)) + '' MB''' 
							+ CASE WHEN @ShowPartitionRanges = 1 THEN N',
							CASE
								WHEN pp.system_type_id IN (40, 41, 42, 43, 58, 61) THEN 126
								WHEN pp.system_type_id IN (59, 62) THEN 3
								WHEN pp.system_type_id IN (60, 122) THEN 2
								ELSE NULL END format_type,
							CASE WHEN pf.boundary_value_on_right = 0 THEN ''>'' ELSE ''>='' END range_start_op,
							prvs.value range_start_value,
							CASE WHEN pf.boundary_value_on_right = 0 THEN ''<='' ELSE ''<'' END range_end_op,
							prve.value range_end_value ' ELSE N' ' END + N'
						FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_row_groups rg 
						INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c ON rg.object_id = c.object_id
						INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partitions p ON rg.object_id = p.object_id AND rg.partition_number = p.partition_number
						INNER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns ic on ic.column_id = c.column_id AND ic.object_id = c.object_id AND ic.index_id = p.index_id ' + CASE WHEN @ShowPartitionRanges = 1 THEN N' 
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.indexes i ON i.object_id = rg.object_id AND i.index_id = rg.index_id
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partition_functions pf ON pf.function_id = ps.function_id
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partition_parameters pp ON pp.function_id = pf.function_id
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partition_range_values prvs ON prvs.function_id = pf.function_id AND prvs.boundary_id = p.partition_number - 1
						LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partition_range_values prve ON prve.function_id = pf.function_id AND prve.boundary_id = p.partition_number ' ELSE N' ' END 
						+ N' LEFT OUTER JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_segments seg ON p.partition_id = seg.partition_id AND ic.index_column_id = seg.column_id AND rg.row_group_id = seg.segment_id
						WHERE rg.object_id = @ObjectID
						AND c.name IN ( ' + @ColumnListWithApostrophes + N')' 
						+ CASE WHEN @ShowPartitionRanges = 1 THEN N'
					) AS y ' ELSE N' ' END + N'
				) AS x
				PIVOT (MAX(details) FOR column_name IN ( ' + @ColumnList + N')) AS pivot1
				ORDER BY partition_number, row_group_id;';
 
			IF @Debug = 1
				BEGIN
					PRINT SUBSTRING(@dsql, 0, 4000);
					PRINT SUBSTRING(@dsql, 4000, 8000);
					PRINT SUBSTRING(@dsql, 8000, 12000);
					PRINT SUBSTRING(@dsql, 12000, 16000);
					PRINT SUBSTRING(@dsql, 16000, 20000);
					PRINT SUBSTRING(@dsql, 20000, 24000);
					PRINT SUBSTRING(@dsql, 24000, 28000);
					PRINT SUBSTRING(@dsql, 28000, 32000);
					PRINT SUBSTRING(@dsql, 32000, 36000);
					PRINT SUBSTRING(@dsql, 36000, 40000);
				END;

			IF @dsql IS NULL 
				RAISERROR('@dsql is null',16,1);
			ELSE
				EXEC sp_executesql @dsql, N'@ObjectID INT', @ObjectID;
		END
		ELSE /* No columns were found for this object */
		BEGIN
			SELECT N'No compressed columnstore rowgroups were found for this object.' AS Columnstore_Visualization
			UNION ALL
			SELECT N'SELECT * FROM ' + QUOTENAME(@DatabaseName) + N'.sys.column_store_row_groups WHERE object_id = ' + CAST(@ObjectID AS NVARCHAR(100));
		END
        RAISERROR(N'Done visualizing columnstore index contents.', 0,1) WITH NOWAIT;
    END

END; /* IF @TableName IS NOT NULL */
































ELSE IF @Mode IN (0, 4) /* DIAGNOSE*//* IF @TableName IS NOT NULL, so  @TableName must not be null */
BEGIN;
	IF @Mode IN (0, 4) /* DIAGNOSE priorities 1-100 */
	BEGIN;
        RAISERROR(N'@Mode=0 or 4, running rules for priorities 1-100.', 0,1) WITH NOWAIT;

        ----------------------------------------
        --Multiple Index Personalities: Check_id 0-10
        ----------------------------------------
        RAISERROR('check_id 1: Duplicate keys', 0,1) WITH NOWAIT;
            WITH    duplicate_indexes
                      AS ( SELECT  [object_id], key_column_names, database_id, [schema_name]
                           FROM        #IndexSanity AS ip
                           WHERE  index_type IN (1,2) /* Clustered, NC only*/
                                AND is_hypothetical = 0
                                AND is_disabled = 0
								AND is_primary_key = 0
								AND EXISTS (
											SELECT 1/0
											FROM #IndexSanitySize ips 
											WHERE ip.index_sanity_id = ips.index_sanity_id 
								            AND ip.database_id = ips.database_id
											AND ip.schema_name = ips.schema_name
								            AND ips.total_reserved_MB >= CASE 
											                             WHEN (@GetAllDatabases = 1 OR @Mode = 0) 
																		 THEN @ThresholdMB 
																		 ELSE ips.total_reserved_MB 
																		 END
								            )
                           GROUP BY    [object_id], key_column_names, database_id, [schema_name]
                           HAVING    COUNT(*) > 1)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  1 AS check_id, 
                                ip.index_sanity_id,
                                20 AS Priority,
                                'Multiple Index Personalities' AS findings_group,
                                'Duplicate keys' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/duplicateindex' AS URL,
                                N'Index Name: ' + ip.index_name + N' Table Name: ' + ip.db_schema_object_name AS details,
                                ip.index_definition, 
                                ip.secret_columns, 
                                ip.index_usage_summary,
                                ips.index_size_summary
                        FROM    duplicate_indexes di
                                JOIN #IndexSanity ip ON di.[object_id] = ip.[object_id]
                                                         AND ip.database_id = di.database_id
														 AND ip.[schema_name] = di.[schema_name]
                                                         AND di.key_column_names = ip.key_column_names
                                JOIN #IndexSanitySize ips ON ip.index_sanity_id = ips.index_sanity_id 
								                          AND ip.database_id = ips.database_id
														  AND ip.schema_name = ips.schema_name
                        /* WHERE clause limits to only @ThresholdMB or larger duplicate indexes when getting all databases or using PainRelief mode */
                        WHERE ips.total_reserved_MB >= CASE WHEN (@GetAllDatabases = 1 OR @Mode = 0) THEN @ThresholdMB ELSE ips.total_reserved_MB END
						AND ip.is_primary_key = 0
                        ORDER BY ips.total_rows DESC, ip.[schema_name], ip.[object_name], ip.key_column_names_with_sort_order    
                OPTION    ( RECOMPILE );

        RAISERROR('check_id 2: Keys w/ identical leading columns.', 0,1) WITH NOWAIT;
            WITH    borderline_duplicate_indexes
                      AS ( SELECT DISTINCT database_id, [object_id], first_key_column_name, key_column_names,
                                    COUNT([object_id]) OVER ( PARTITION BY database_id, [object_id], first_key_column_name ) AS number_dupes
                           FROM        #IndexSanity
                           WHERE index_type IN (1,2) /* Clustered, NC only*/
                            AND is_hypothetical=0
                            AND is_disabled=0
							AND is_primary_key = 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  2 AS check_id, 
                                ip.index_sanity_id,
                                30 AS Priority,
                                'Multiple Index Personalities' AS findings_group,
                                'Borderline duplicate keys' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/duplicateindex' AS URL,
                                ip.db_schema_object_indexid AS details, 
                                ip.index_definition, 
                                ip.secret_columns,
                                ip.index_usage_summary,
                                ips.index_size_summary
                        FROM    #IndexSanity AS ip 
                        JOIN #IndexSanitySize ips ON ip.index_sanity_id = ips.index_sanity_id
                        WHERE EXISTS (
                            SELECT di.[object_id]
                            FROM borderline_duplicate_indexes AS di
                            WHERE di.[object_id] = ip.[object_id] AND
                                di.database_id = ip.database_id AND
                                di.first_key_column_name = ip.first_key_column_name AND
                                di.key_column_names <> ip.key_column_names AND
                                di.number_dupes > 1    
                        )
						AND ip.is_primary_key = 0                                          
                        ORDER BY ips.total_rows DESC, ip.[schema_name], ip.[object_name], ip.key_column_names, ip.include_column_names
            OPTION    ( RECOMPILE );

        ----------------------------------------
        --Aggressive Indexes: Check_id 10-19
        ----------------------------------------

        RAISERROR(N'check_id 11: Total lock wait time > 5 minutes (row + page)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                SELECT  11 AS check_id, 
                        i.index_sanity_id,
                        70 AS Priority,
                        N'Aggressive ' 
                            + CASE COALESCE((SELECT SUM(1) 
							                 FROM #IndexSanity iMe 
											 INNER JOIN #IndexSanity iOthers 
												ON iMe.database_id = iOthers.database_id 
												AND iMe.object_id = iOthers.object_id 
												AND iOthers.index_id > 1 
											 WHERE i.index_sanity_id = iMe.index_sanity_id
												AND iOthers.is_hypothetical = 0
												AND iOthers.is_disabled = 0
											), 0)
                                WHEN 0 THEN N'Under-Indexing'
                                WHEN 1 THEN N'Under-Indexing'
                                WHEN 2 THEN N'Under-Indexing'
                                WHEN 3 THEN N'Under-Indexing'
                                WHEN 4 THEN N'Indexes'
                                WHEN 5 THEN N'Indexes'
                                WHEN 6 THEN N'Indexes'
                                WHEN 7 THEN N'Indexes'
                                WHEN 8 THEN N'Indexes'
                                WHEN 9 THEN N'Indexes'
                                ELSE N'Over-Indexing'
                                END AS findings_group,
                        N'Total lock wait time > 5 minutes (row + page)' AS finding, 
                        [database_name] AS [Database Name],
                        N'https://www.brentozar.com/go/AggressiveIndexes' AS URL,
                        (i.db_schema_object_indexid + N': ' +
                            sz.index_lock_wait_summary + N' NC indexes on table: ') COLLATE DATABASE_DEFAULT +
							 CAST(COALESCE((SELECT SUM(1) 
							                FROM #IndexSanity iMe 
											INNER JOIN #IndexSanity iOthers 
												ON iMe.database_id = iOthers.database_id 
												AND iMe.object_id = iOthers.object_id 
												AND iOthers.index_id > 1 
											WHERE i.index_sanity_id = iMe.index_sanity_id
											AND iOthers.is_hypothetical = 0
											AND iOthers.is_disabled = 0
										   ), 0)
                                         AS NVARCHAR(30))	 AS details, 
                        i.index_definition,
                        i.secret_columns,
                        i.index_usage_summary,
                        sz.index_size_summary
                FROM    #IndexSanity AS i
                JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                WHERE    (total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) > 300000
				GROUP BY i.index_sanity_id, [database_name], i.db_schema_object_indexid, sz.index_lock_wait_summary, i.index_definition, i.secret_columns, i.index_usage_summary, sz.index_size_summary, sz.index_sanity_id
                ORDER BY SUM(total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) DESC, 4, [database_name], 8
                OPTION    ( RECOMPILE );



        ---------------------------------------- 
        --Index Hoarder: Check_id 20-29
        ----------------------------------------
            RAISERROR(N'check_id 20: >= 10 NC indexes on any given table. Yes, 10 is an arbitrary number.', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  20 AS check_id, 
                                MAX(i.index_sanity_id) AS index_sanity_id, 
                                10 AS Priority,
                                'Index Hoarder' AS findings_group,
                                'Many NC Indexes on a Single Table' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                CAST (COUNT(*) AS NVARCHAR(30)) + ' NC indexes on ' + i.db_schema_object_name AS details,
                                i.db_schema_object_name + ' (' + CAST (COUNT(*) AS NVARCHAR(30)) + ' indexes)' AS index_definition,
                                '' AS secret_columns,
                                REPLACE(CONVERT(NVARCHAR(30),CAST(SUM(total_reads) AS MONEY), 1), N'.00', N'') + N' reads (ALL); '
                                    + REPLACE(CONVERT(NVARCHAR(30),CAST(SUM(user_updates) AS MONEY), 1), N'.00', N'') + N' writes (ALL); ',
                                REPLACE(CONVERT(NVARCHAR(30),CAST(MAX(total_rows) AS MONEY), 1), N'.00', N'') + N' rows (MAX)'
                                    + CASE WHEN SUM(total_reserved_MB) > 1024 THEN 
                                        N'; ' + CAST(CAST(SUM(total_reserved_MB)/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'GB (ALL)'
                                    WHEN SUM(total_reserved_MB) > 0 THEN
                                        N'; ' + CAST(CAST(SUM(total_reserved_MB) AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'MB (ALL)'
                                    ELSE ''
                                    END AS index_size_summary
                        FROM    #IndexSanity i
                        JOIN #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        WHERE    index_id NOT IN ( 0, 1 )
                        GROUP BY db_schema_object_name, [i].[database_name]
                        HAVING    COUNT(*) >= 10
                        ORDER BY i.db_schema_object_name DESC  
						OPTION    ( RECOMPILE );

                RAISERROR(N'check_id 22: NC indexes with 0 reads. (Borderline) and >= 10,000 writes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  22 AS check_id, 
                                i.index_sanity_id,
                                10 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Unused NC Index with High Writes' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                N'Reads: 0,'
								+ N' Writes: ' 
								+ REPLACE(CONVERT(NVARCHAR(30), CAST((i.user_updates) AS MONEY), 1), N'.00', N'')
								+ N' on: '
								+ i.db_schema_object_indexid
								AS details, 
                                i.index_definition, 
                                i.secret_columns, 
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity AS i
                        JOIN    #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.total_reads=0
						    AND i.user_updates >= 10000
                                AND i.index_id NOT IN (0,1) /*NCs only*/
                                AND i.is_unique = 0
                                AND sz.total_reserved_MB >= CASE WHEN (@GetAllDatabases = 1 OR @Mode = 0) THEN @ThresholdMB ELSE sz.total_reserved_MB END
								AND @Filter <> 1 /* 1 = "ignore unused */
                        ORDER BY i.db_schema_object_indexid
                        OPTION    ( RECOMPILE );


		RAISERROR(N'check_id 34: Filtered index definition columns not in index definition', 0,1) WITH NOWAIT;
                 
                 INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  34 AS check_id, 
                                i.index_sanity_id,
                                80 AS Priority,
                                N'Abnormal Psychology' AS findings_group,
                                N'Filter Columns Not In Index Definition' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexFeatures' AS URL,
                                N'The index '
                                + QUOTENAME(i.index_name)
                                + N' on ['
                                + i.db_schema_object_name
                                + N'] has a filter on ['
                                + i.filter_definition
                                + N'] but is missing ['
                                + LTRIM(i.filter_columns_not_in_index)
                                + N'] from the index definition.'
                                AS details, 
                                i.index_definition, 
                                i.secret_columns, 
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE   i.filter_columns_not_in_index IS NOT NULL
                        ORDER BY i.db_schema_object_indexid
                        OPTION    ( RECOMPILE );
                                
         ----------------------------------------
        --Self Loathing Indexes : Check_id 40-49
        ----------------------------------------
        
            RAISERROR(N'check_id 40: Fillfactor in nonclustered 80 percent or less', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  40 AS check_id, 
                            i.index_sanity_id,
                            100 AS Priority,
                            N'Self Loathing Indexes' AS findings_group,
                            N'Low Fill Factor on Nonclustered Index' AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                            CAST(fill_factor AS NVARCHAR(10)) + N'% fill factor on ' + db_schema_object_indexid + N'. '+
                                CASE WHEN (last_user_update IS NULL OR user_updates < 1)
                                THEN N'No writes have been made.'
                                ELSE
                                    N'Last write was ' +  CONVERT(NVARCHAR(16),last_user_update,121) + N' and ' + 
                                    CAST(user_updates AS NVARCHAR(25)) + N' updates have been made.'
                                END
                                AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            sz.index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN    #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE    index_id > 1
                    AND    fill_factor BETWEEN 1 AND 80 OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 40: Fillfactor in clustered 80 percent or less', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  40 AS check_id, 
                            i.index_sanity_id,
                            100 AS Priority,
                            N'Self Loathing Indexes' AS findings_group,
                            N'Low Fill Factor on Clustered Index' AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                            N'Fill factor on ' + db_schema_object_indexid + N' is ' + CAST(fill_factor AS NVARCHAR(10)) + N'%. '+
                                CASE WHEN (last_user_update IS NULL OR user_updates < 1)
                                THEN N'No writes have been made.'
                                ELSE
                                    N'Last write was ' +  CONVERT(NVARCHAR(16),last_user_update,121) + N' and ' + 
                                    CAST(user_updates AS NVARCHAR(25)) + N' updates have been made.'
                                END
                                AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            sz.index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE    index_id = 1
                    AND fill_factor BETWEEN 1 AND 80 OPTION    ( RECOMPILE );


            RAISERROR(N'check_id 43: Heaps with forwarded records', 0,1) WITH NOWAIT;
            WITH    heaps_cte
                      AS ( SELECT   [object_id],
								    [database_id],
								    [schema_name],
                                    SUM(forwarded_fetch_count) AS forwarded_fetch_count,
                                    SUM(leaf_delete_count) AS leaf_delete_count
                           FROM        #IndexPartitionSanity
                           GROUP BY    [object_id],
								       [database_id],
								       [schema_name]
                           HAVING    SUM(forwarded_fetch_count) > 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  43 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Heaps with Forwarded Fetches' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                                CASE WHEN h.forwarded_fetch_count >= 922337203685477 THEN '>= 922,337,203,685,477'
                                    WHEN @DaysUptime < 1 THEN CAST(h.forwarded_fetch_count AS NVARCHAR(256)) + N' forwarded fetches against heap: ' + db_schema_object_indexid
                                    ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(
                                    (h.forwarded_fetch_count /*/@DaysUptime */)
                                     AS BIGINT) AS MONEY), 1), '.00', '') 
                                    END + N' forwarded fetches per day against heap: '
                                + db_schema_object_indexid AS details, 
                                i.index_definition, 
                                i.secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        JOIN heaps_cte h ON i.[object_id] = h.[object_id] 
							 AND i.[database_id] = h.[database_id]
							 AND i.[schema_name] = h.[schema_name]
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_id = 0 
                        AND h.forwarded_fetch_count / @DaysUptime > 1000
                        AND sz.total_reserved_MB >= CASE WHEN NOT (@GetAllDatabases = 1 OR @Mode = 4) THEN @ThresholdMB ELSE sz.total_reserved_MB END
                OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 44: Large Heaps with reads or writes.', 0,1) WITH NOWAIT;
            WITH    heaps_cte
                      AS ( SELECT   [object_id],
								    [database_id],
								    [schema_name], 
									SUM(forwarded_fetch_count) AS forwarded_fetch_count,
                                    SUM(leaf_delete_count) AS leaf_delete_count
                           FROM        #IndexPartitionSanity
                           GROUP BY  [object_id],
								     [database_id],
								     [schema_name]
                           HAVING    SUM(forwarded_fetch_count) > 0
                                    OR SUM(leaf_delete_count) > 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  44 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Large Active Heap' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                                N'Should this table be a heap? ' + db_schema_object_indexid AS details, 
                                i.index_definition, 
                                'N/A' AS secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        LEFT JOIN heaps_cte h ON i.[object_id] = h.[object_id] 
								AND i.[database_id] = h.[database_id]
								AND i.[schema_name] = h.[schema_name]
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_id = 0 
                                AND (i.total_reads > 0 OR i.user_updates > 0)
								AND sz.total_rows >= 100000
                                AND h.[object_id] IS NULL /*don't duplicate the prior check.*/
                OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 45: Medium Heaps with reads or writes.', 0,1) WITH NOWAIT;
            WITH    heaps_cte
                      AS ( SELECT   [object_id],
								    [database_id],
								    [schema_name], 
									SUM(forwarded_fetch_count) AS forwarded_fetch_count,
                                    SUM(leaf_delete_count) AS leaf_delete_count
                           FROM        #IndexPartitionSanity
                           GROUP BY  [object_id],
								     [database_id],
								     [schema_name]
                           HAVING    SUM(forwarded_fetch_count) > 0
                                    OR SUM(leaf_delete_count) > 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  45 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Medium Active heap' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                                N'Should this table be a heap? ' + db_schema_object_indexid AS details, 
                                i.index_definition, 
                                'N/A' AS secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        LEFT JOIN heaps_cte h ON i.[object_id] = h.[object_id] 
								AND i.[database_id] = h.[database_id]
								AND i.[schema_name] = h.[schema_name]
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_id = 0 
                                AND 
                                    (i.total_reads > 0 OR i.user_updates > 0)
								AND sz.total_rows >= 10000 AND sz.total_rows < 100000
                                AND h.[object_id] IS NULL /*don't duplicate the prior check.*/
                OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 46: Small Heaps with reads or writes.', 0,1) WITH NOWAIT;
            WITH    heaps_cte
                      AS ( SELECT   [object_id],
								    [database_id],
								    [schema_name], 
									SUM(forwarded_fetch_count) AS forwarded_fetch_count,
                                    SUM(leaf_delete_count) AS leaf_delete_count
                           FROM        #IndexPartitionSanity
                           GROUP BY  [object_id],
								     [database_id],
								     [schema_name]
                           HAVING    SUM(forwarded_fetch_count) > 0
                                    OR SUM(leaf_delete_count) > 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  46 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Small Active heap' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                                N'Should this table be a heap? ' + db_schema_object_indexid AS details, 
                                i.index_definition, 
                                'N/A' AS secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        LEFT JOIN heaps_cte h ON i.[object_id] = h.[object_id] 
								AND i.[database_id] = h.[database_id]
								AND i.[schema_name] = h.[schema_name]
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_id = 0 
                                AND 
                                    (i.total_reads > 0 OR i.user_updates > 0)
								AND sz.total_rows < 10000
                                AND h.[object_id] IS NULL /*don't duplicate the prior check.*/
						OPTION    ( RECOMPILE );

				            RAISERROR(N'check_id 47: Heap with a Nonclustered Primary Key', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  47 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Heap with a Nonclustered Primary Key' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
								db_schema_object_indexid + N' is a HEAP with a Nonclustered Primary Key' AS details, 
                                i.index_definition, 
                                i.secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_type = 2 AND i.is_primary_key = 1
                        AND EXISTS 
                            (
                              SELECT 1/0 
                              FROM #IndexSanity AS isa
                              WHERE i.database_id = isa.database_id
                              AND   i.object_id = isa.object_id
                              AND   isa.index_id = 0
                            )
						OPTION    ( RECOMPILE );

	            RAISERROR(N'check_id 48: Nonclustered indexes with a bad read to write ratio', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  48 AS check_id, 
                                i.index_sanity_id,
                                100 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'NC index with High Writes:Reads' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                N'Reads: '
								+ REPLACE(CONVERT(NVARCHAR(30), CAST((i.total_reads) AS MONEY), 1), N'.00', N'') 
								+ N' Writes: ' 
								+ REPLACE(CONVERT(NVARCHAR(30), CAST((i.user_updates) AS MONEY), 1), N'.00', N'')
								+ N' on: '
								+ i.db_schema_object_indexid AS details, 
                                i.index_definition, 
                                i.secret_columns, 
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.total_reads > 0 /*Not totally unused*/
								AND i.user_updates >= 10000 /*Decent write activity*/
								AND i.total_reads < 10000
								AND ((i.total_reads * 10) < i.user_updates) /*10x more writes than reads*/
                                AND i.index_id NOT IN (0,1) /*NCs only*/
                                AND i.is_unique = 0 
                                AND sz.total_reserved_MB >= CASE WHEN (@GetAllDatabases = 1 OR @Mode = 0) THEN @ThresholdMB ELSE sz.total_reserved_MB END
                        ORDER BY i.db_schema_object_indexid
                        OPTION    ( RECOMPILE );

        ----------------------------------------
        --Indexaphobia
        --Missing indexes with value >= 5 million: : Check_id 50-59
        ----------------------------------------
            RAISERROR(N'check_id 50: Indexaphobia.', 0,1) WITH NOWAIT;
            WITH    index_size_cte
                      AS ( SELECT   i.database_id,
									i.schema_name,
									i.[object_id], 
                                    MAX(i.index_sanity_id) AS index_sanity_id,
									ISNULL(NULLIF(MAX(DATEDIFF(DAY, i.create_date, SYSDATETIME())), 0), 1) AS create_days,
                                ISNULL (
                                    CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN 1 ELSE 0 END)
                                         AS NVARCHAR(30))+ N' NC indexes exist (' + 
                                    CASE WHEN SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) > 1024
                                        THEN CAST(CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END )/1024. 

                                            AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB); ' 
                                        ELSE CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) 
                                            AS NVARCHAR(30)) + N'MB); '
                                    END + 
                                        CASE WHEN MAX(sz.[total_rows]) >= 922337203685477 THEN '>= 922,337,203,685,477'
                                        ELSE REPLACE(CONVERT(NVARCHAR(30),CAST(MAX(sz.[total_rows]) AS MONEY), 1), '.00', '') 
                                        END +
                                    + N' Estimated Rows;' 
                                ,N'') AS index_size_summary
                            FROM    #IndexSanity AS i
                            LEFT    JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id  AND i.database_id = sz.database_id
							WHERE i.is_hypothetical = 0
                                  AND i.is_disabled = 0
                           GROUP BY    i.database_id, i.schema_name, i.[object_id])
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               index_usage_summary, index_size_summary, create_tsql, more_info, sample_query_plan )
                        
                        SELECT check_id, t.index_sanity_id, t.check_id, t.findings_group, t.finding, t.[Database Name], t.URL, t.details, t.[definition],
                                index_estimated_impact, t.index_size_summary, create_tsql, more_info, sample_query_plan
                        FROM
                        (
                            SELECT  ROW_NUMBER() OVER (ORDER BY magic_benefit_number DESC) AS rownum,
                                50 AS check_id, 
                                sz.index_sanity_id,
                                40 AS Priority,
                                N'Indexaphobia' AS findings_group,
                                N'High Value Missing Index' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/Indexaphobia' AS URL,
                                mi.[statement] + 
                                N' Est. benefit per day: ' + 
                                    CASE WHEN magic_benefit_number >= 922337203685477 THEN '>= 922,337,203,685,477'
                                    ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(
                                    (magic_benefit_number/@DaysUptime)
                                     AS BIGINT) AS MONEY), 1), '.00', '') 
                                    END AS details,
                                missing_index_details AS [definition],
                                index_estimated_impact,
                                sz.index_size_summary,
                                mi.create_tsql,
                                mi.more_info,
                                magic_benefit_number,
								mi.is_low,
                                mi.sample_query_plan
                        FROM    #MissingIndexes mi
                                LEFT JOIN index_size_cte sz ON mi.[object_id] = sz.object_id 
										  AND mi.database_id = sz.database_id
										  AND mi.schema_name = sz.schema_name
                                        /* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
                        WHERE @ShowAllMissingIndexRequests=1
                        OR ( @Mode = 4 AND (magic_benefit_number / CASE WHEN sz.create_days < @DaysUptime THEN sz.create_days ELSE @DaysUptime END) >= 100000 ) 
						OR (magic_benefit_number / CASE WHEN sz.create_days < @DaysUptime THEN sz.create_days ELSE @DaysUptime END) >= 100000
                        ) AS t
                        WHERE t.rownum <= CASE WHEN (@Mode <> 4) THEN 20 ELSE t.rownum END
                        ORDER BY magic_benefit_number DESC
						OPTION    ( RECOMPILE );



            RAISERROR(N'check_id 68: Identity columns within 30 percent of the end of range', 0,1) WITH NOWAIT;
            -- Allowed Ranges: 
                --int -2,147,483,648 to 2,147,483,647
                --smallint -32,768 to 32,768
                --tinyint 0 to 255

                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  68 AS check_id, 
                                i.index_sanity_id, 
                                80 AS Priority,
                                N'Abnormal Psychology' AS findings_group,
                                N'Identity Column Within ' +                                     
                                    CAST (calc1.percent_remaining AS NVARCHAR(256))
                                    + N' Percent End of Range' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                                i.db_schema_object_name + N'.' +  QUOTENAME(ic.column_name)
                                    + N' is an identity with type ' + ic.system_type_name 
                                    + N', last value of ' 
                                        + ISNULL((CONVERT(NVARCHAR(256),CAST(ic.last_value AS DECIMAL(38,0)), 1)),N'NULL')
                                    + N', seed of '
                                        + ISNULL((CONVERT(NVARCHAR(256),CAST(ic.seed_value AS DECIMAL(38,0)), 1)),N'NULL')
                                    + N', increment of ' + CAST(ic.increment_value AS NVARCHAR(256)) 
                                    + N', and range of ' +
                                        CASE ic.system_type_name WHEN 'int' THEN N'+/- 2,147,483,647'
                                            WHEN 'smallint' THEN N'+/- 32,768'
                                            WHEN 'tinyint' THEN N'0 to 255'
                                            ELSE 'unknown'
                                        END
                                        AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexColumns ic ON
                            i.object_id=ic.object_id
							AND i.database_id = ic.database_id
							AND i.schema_name = ic.schema_name
                            AND i.index_id IN (0,1) /* heaps and cx only */
                            AND ic.is_identity=1
                            AND ic.system_type_name IN ('tinyint', 'smallint', 'int')
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        CROSS APPLY (
                            SELECT CAST(CASE WHEN ic.increment_value >= 0
                                    THEN
                                        CASE ic.system_type_name 
                                            WHEN 'int' THEN (2147483647 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 2147483647.*100
                                            WHEN 'smallint' THEN (32768 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 32768.*100
                                            WHEN 'tinyint' THEN ( 255 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 255.*100
                                            ELSE 999
                                        END
                                ELSE --ic.increment_value is negative
                                        CASE ic.system_type_name 
                                            WHEN 'int' THEN ABS(-2147483647 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 2147483647.*100
                                            WHEN 'smallint' THEN ABS(-32768 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 32768.*100
                                            WHEN 'tinyint' THEN ABS( 0 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 255.*100
                                            ELSE -1
                                        END 
                                END AS NUMERIC(5,1)) AS percent_remaining
                                ) AS calc1
                        WHERE    i.index_id IN (1,0)
                            AND calc1.percent_remaining <= 30
                        OPTION (RECOMPILE);


		RAISERROR(N'check_id 72: Columnstore indexes with Trace Flag 834', 0,1) WITH NOWAIT;
            IF EXISTS (SELECT * FROM #IndexSanity WHERE index_type IN (5,6))
			AND EXISTS (SELECT * FROM #TraceStatus WHERE TraceFlag = 834 AND status = 1)
			BEGIN
			INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                            secret_columns, index_usage_summary, index_size_summary )
                SELECT  72 AS check_id, 
                        i.index_sanity_id,
                        80 AS Priority,
                        N'Abnormal Psychology' AS findings_group,
                        'Columnstore Indexes with Trace Flag 834' AS finding, 
                        [database_name] AS [Database Name],
                        N'https://support.microsoft.com/en-us/kb/3210239' AS URL,
                        i.db_schema_object_indexid AS details, 
                        i.index_definition,
                        i.secret_columns,
                        i.index_usage_summary,
                        ISNULL(sz.index_size_summary,'') AS index_size_summary
                FROM    #IndexSanity AS i
                JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                WHERE i.index_type IN (5,6)
                OPTION    ( RECOMPILE );
			END;

        ----------------------------------------
        --Statistics Info: Check_id 90-99
        ----------------------------------------

        RAISERROR(N'check_id 90: Outdated statistics', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  90 AS check_id, 
				90 AS Priority,
				'Functioning Statistaholics' AS findings_group,
				'Statistics Not Updated Recently',
				s.database_name,
				'https://www.brentozar.com/go/stats' AS URL,
				'Statistics on this table were last updated ' + 
					CASE WHEN s.last_statistics_update IS NULL THEN N' NEVER '
					ELSE CONVERT(NVARCHAR(20), s.last_statistics_update) + 
						' have had ' + CONVERT(NVARCHAR(100), s.modification_counter) +
						' modifications in that time, which is ' +
						CONVERT(NVARCHAR(100), s.percent_modifications) + 
						'% of the table.'
					END AS details,
				QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' + QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' + QUOTENAME(s.statistics_name) + '.' + QUOTENAME(s.column_names) AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #Statistics AS s
		WHERE s.last_statistics_update <= CONVERT(DATETIME, GETDATE() - 30) 
		AND s.percent_modifications >= 10. 
		AND s.rows >= 10000
		OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 91: Statistics with a low sample rate', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  91 AS check_id, 
				90 AS Priority,
				'Functioning Statistaholics' AS findings_group,
				'Low Sampling Rates',
				s.database_name,
				'https://www.brentozar.com/go/stats' AS URL,
				'Only ' + CONVERT(NVARCHAR(100), s.percent_sampled) + '% of the rows were sampled during the last statistics update. This may lead to poor cardinality estimates.' AS details,
				QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' + QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' + QUOTENAME(s.statistics_name) + '.' + QUOTENAME(s.column_names) AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #Statistics AS s
		WHERE (s.rows BETWEEN 10000 AND 1000000 AND s.percent_sampled < 10)
		  OR (s.rows > 1000000 AND s.percent_sampled < 1)
		OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 92: Statistics with NO RECOMPUTE', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  92 AS check_id, 
				90 AS Priority,
				'Functioning Statistaholics' AS findings_group,
				'Statistics With NO RECOMPUTE',
				s.database_name,
				'https://www.brentozar.com/go/stats' AS URL,
				'The statistic ' + QUOTENAME(s.statistics_name) +  ' is set to not recompute. This can be helpful if data is really skewed, but harmful if you expect automatic statistics updates.' AS details,
				QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' + QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' + QUOTENAME(s.statistics_name) + '.' + QUOTENAME(s.column_names) AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #Statistics AS s
		WHERE s.no_recompute = 1
		OPTION    ( RECOMPILE );


	     RAISERROR(N'check_id 94: Check Constraints That Reference Functions', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  94 AS check_id, 
				100 AS Priority,
				'Serial Forcer' AS findings_group,
				'Check Constraint with Scalar UDF' AS finding,
				cc.database_name,
				'https://www.brentozar.com/go/computedscalar' AS URL,
				'The check constraint ' + QUOTENAME(cc.constraint_name) + ' on ' + QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' is based on ' + cc.definition 
				+ '. That indicates it may reference a scalar function, or a CLR function with data access, which can cause all queries and maintenance to run serially.' AS details,
				cc.column_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #CheckConstraints AS cc
		WHERE cc.is_function = 1
		OPTION    ( RECOMPILE );

		RAISERROR(N'check_id 99: Computed Columns That Reference Functions', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  99 AS check_id, 
				100 AS Priority,
				'Serial Forcer' AS findings_group,
				'Computed Column with Scalar UDF' AS finding,
				cc.database_name,
				'https://www.brentozar.com/go/serialudf' AS URL,
				'The computed column ' + QUOTENAME(cc.column_name) + ' on ' + QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' is based on ' + cc.definition 
				+ '. That indicates it may reference a scalar function, or a CLR function with data access, which can cause all queries and maintenance to run serially.' AS details,
				cc.column_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #ComputedColumns AS cc
		WHERE cc.is_function = 1
		OPTION    ( RECOMPILE );



	END /* IF @Mode IN (0, 4) DIAGNOSE priorities 1-100 */

























    IF @Mode = 4 /* DIAGNOSE*/
    BEGIN;
        RAISERROR(N'@Mode=4, running rules for priorities 101+.', 0,1) WITH NOWAIT;

            RAISERROR(N'check_id 21: More Than 5 Percent NC Indexes Are Unused', 0,1) WITH NOWAIT;
            DECLARE @percent_NC_indexes_unused NUMERIC(29,1);
            DECLARE @NC_indexes_unused_reserved_MB NUMERIC(29,1);

            SELECT  @percent_NC_indexes_unused = ( 100.00 * SUM(CASE 
					                                                WHEN total_reads = 0 
																	THEN 1
                                                                    ELSE 0
                                                                    END) ) / COUNT(*),
                    @NC_indexes_unused_reserved_MB = SUM(CASE 
							                                    WHEN total_reads = 0 
																THEN sz.total_reserved_MB
                                                                ELSE 0
                                                            END) 
            FROM    #IndexSanity i
            JOIN    #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
            WHERE    index_id NOT IN ( 0, 1 ) 
                    AND i.is_unique = 0
					/*Skipping tables created in the last week, or modified in past 2 days*/
					AND	i.create_date >= DATEADD(dd,-7,GETDATE()) 
					AND i.modify_date > DATEADD(dd,-2,GETDATE()) 
            OPTION    ( RECOMPILE );
            IF @percent_NC_indexes_unused >= 5 
            INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                            secret_columns, index_usage_summary, index_size_summary )
                        SELECT  21 AS check_id, 
                                MAX(i.index_sanity_id) AS index_sanity_id, 
                                150 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'More Than 5 Percent NC Indexes Are Unused' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                CAST (@percent_NC_indexes_unused AS NVARCHAR(30)) + N' percent NC indexes (' + CAST(COUNT(*) AS NVARCHAR(10)) + N') unused. ' +
                                N'These take up ' + CAST (@NC_indexes_unused_reserved_MB AS NVARCHAR(30)) + N'MB of space.' AS details,
                                i.database_name + ' (' + CAST (COUNT(*) AS NVARCHAR(30)) + N' indexes)' AS index_definition,
                                '' AS secret_columns, 
                                CAST(SUM(total_reads) AS NVARCHAR(256)) + N' reads (ALL); '
                                    + CAST(SUM([user_updates]) AS NVARCHAR(256)) + N' writes (ALL)' AS index_usage_summary,
                                
                                REPLACE(CONVERT(NVARCHAR(30),CAST(MAX([total_rows]) AS MONEY), 1), '.00', '') + N' rows (MAX)'
                                    + CASE WHEN SUM(total_reserved_MB) > 1024 THEN 
                                        N'; ' + CAST(CAST(SUM(total_reserved_MB)/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'GB (ALL)'
                                    WHEN SUM(total_reserved_MB) > 0 THEN
                                        N'; ' + CAST(CAST(SUM(total_reserved_MB) AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'MB (ALL)'
                                    ELSE ''
                                    END AS index_size_summary
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    index_id NOT IN ( 0, 1 )
                                AND i.is_unique = 0
                                AND total_reads = 0
								/*Skipping tables created in the last week, or modified in past 2 days*/
								AND	i.create_date >= DATEADD(dd,-7,GETDATE()) 
								AND i.modify_date > DATEADD(dd,-2,GETDATE())
                        GROUP BY i.database_name 
                OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 23: Indexes with 7 or more columns. (Borderline)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  23 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority, 
                            N'Index Hoarder' AS findings_group,
                            N'Borderline: Wide Indexes (7 or More Columns)' AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                            CAST(count_key_columns + count_included_columns AS NVARCHAR(10)) + ' columns on '
                            + i.db_schema_object_indexid AS details, i.index_definition, 
                            i.secret_columns, 
                            i.index_usage_summary,
                            sz.index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN    #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE    ( count_key_columns + count_included_columns ) >= 7
                    OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 24: Wide clustered indexes (> 3 columns or > 16 bytes).', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT database_id, [object_id],
                                SUM(CASE max_length WHEN -1 THEN 0 ELSE max_length END) AS sum_max_length
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            AND key_ordinal > 0
                            GROUP BY database_id, object_id
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  24 AS check_id, 
                                i.index_sanity_id, 
                                150 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Wide Clustered Index (> 3 columns OR > 16 bytes)' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                CAST (i.count_key_columns AS NVARCHAR(10)) + N' columns with potential size of '
                                    + CAST(cc.sum_max_length AS NVARCHAR(10))
                                    + N' bytes in clustered index:' + i.db_schema_object_name 
                                    + N'. ' + 
                                        (SELECT CAST(COUNT(*) AS NVARCHAR(23)) 
										 FROM #IndexSanity i2 
                                         WHERE i2.[object_id]=i.[object_id] 
										 AND i2.database_id = i.database_id 
										 AND i2.index_id <> 1
                                         AND i2.is_disabled=0 
										 AND i2.is_hypothetical=0)
                                        + N' NC indexes on the table.'
                                    AS details,
                                i.index_definition,
                                secret_columns, 
                                i.index_usage_summary,
                                ip.index_size_summary
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
                                                   AND i.database_id = cc.database_id
                        WHERE    index_id = 1 /* clustered only */
                                AND 
                                    (count_key_columns > 3 /*More than three key columns.*/
                                    OR cc.sum_max_length > 16 /*More than 16 bytes in key */)
									AND i.is_CX_columnstore = 0
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 25: Addicted to nullable columns.', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT [object_id],
								   [database_id],
								   [schema_name],
                                SUM(CASE is_nullable WHEN 1 THEN 0 ELSE 1 END) AS non_nullable_columns,
                                COUNT(*) AS total_columns
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            GROUP BY [object_id],
								     [database_id],
								     [schema_name]
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  25 AS check_id, 
                                i.index_sanity_id, 
                                200 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Addicted to Nulls' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                i.db_schema_object_name 
                                    + N' allows null in ' + CAST((total_columns-non_nullable_columns) AS NVARCHAR(10))
                                    + N' of ' + CAST(total_columns AS NVARCHAR(10))
                                    + N' columns.' AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
								AND cc.database_id = ip.database_id
								AND cc.[schema_name] = ip.[schema_name]
                        WHERE    i.index_id IN (1,0)
                            AND cc.non_nullable_columns < 2
                            AND cc.total_columns > 3
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 26: Wide tables (35+ cols or > 2000 non-LOB bytes).', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT [object_id],
								   [database_id],
								   [schema_name],
                                SUM(CASE max_length WHEN -1 THEN 1 ELSE 0 END) AS count_lob_columns,
                                SUM(CASE max_length WHEN -1 THEN 0 ELSE max_length END) AS sum_max_length,
                                COUNT(*) AS total_columns
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            GROUP BY [object_id],
								     [database_id],
								     [schema_name]
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  26 AS check_id, 
                                i.index_sanity_id, 
                                150 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Wide Tables: 35+ cols or > 2000 non-LOB bytes' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                i.db_schema_object_name 
                                    + N' has ' + CAST((total_columns) AS NVARCHAR(10))
                                    + N' total columns with a max possible width of ' + CAST(sum_max_length AS NVARCHAR(10))
                                    + N' bytes.' +
                                    CASE WHEN count_lob_columns > 0 THEN CAST((count_lob_columns) AS NVARCHAR(10))
                                        + ' columns are LOB types.' ELSE ''
                                    END
                                        AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
								AND cc.database_id = i.database_id
								AND cc.[schema_name] = i.[schema_name]
                        WHERE    i.index_id IN (1,0)
                            AND 
                            (cc.total_columns >= 35 OR
                            cc.sum_max_length >= 2000)
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );
                    
            RAISERROR(N'check_id 27: Addicted to strings.', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT [object_id],
								   [database_id],
								   [schema_name],
                                SUM(CASE WHEN system_type_name IN ('varchar','nvarchar','char') OR max_length=-1 THEN 1 ELSE 0 END) AS string_or_LOB_columns,
                                COUNT(*) AS total_columns
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            GROUP BY [object_id],
								     [database_id],
								     [schema_name]
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  27 AS check_id, 
                                i.index_sanity_id, 
                                200 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Addicted to strings' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                i.db_schema_object_name 
                                    + N' uses string or LOB types for ' + CAST((string_or_LOB_columns) AS NVARCHAR(10))
                                    + N' of ' + CAST(total_columns AS NVARCHAR(10))
                                    + N' columns. Check if data types are valid.' AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
								AND cc.database_id = i.database_id
								AND cc.[schema_name] = i.[schema_name]
                        CROSS APPLY (SELECT cc.total_columns - string_or_LOB_columns AS non_string_or_lob_columns) AS calc1
                        WHERE    i.index_id IN (1,0)
                            AND calc1.non_string_or_lob_columns <= 1
                            AND cc.total_columns > 3
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 28: Non-unique clustered index.', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  28 AS check_id, 
                                i.index_sanity_id, 
                                150 AS Priority,
                                N'Index Hoarder' AS findings_group,
                                N'Non-Unique Clustered Index' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                                N'Uniquifiers will be required! Clustered index: ' + i.db_schema_object_name 
                                    + N' and all NC indexes. ' + 
                                        (SELECT CAST(COUNT(*) AS NVARCHAR(23)) 
										 FROM #IndexSanity i2 
                                         WHERE i2.[object_id]=i.[object_id] 
										 AND i2.database_id = i.database_id 
										 AND i2.index_id <> 1
                                         AND i2.is_disabled=0 
										 AND i2.is_hypothetical=0)
                                        + N' NC indexes on the table.'
                                    AS details,
                                i.index_definition,
                                secret_columns, 
                                i.index_usage_summary,
                                ip.index_size_summary
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        WHERE    index_id = 1 /* clustered only */
                                AND is_unique=0 /* not unique */
                                AND is_CX_columnstore=0 /* not a clustered columnstore-- no unique option on those */
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 29: NC indexes with 0 reads. (Borderline) and < 10,000 writes', 0,1) WITH NOWAIT;
        INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                        secret_columns, index_usage_summary, index_size_summary )
                SELECT  29 AS check_id, 
                        i.index_sanity_id,
                        150 AS Priority,
                        N'Index Hoarder' AS findings_group,
                        N'Unused NC index with Low Writes' AS finding, 
                        [database_name] AS [Database Name],
                        N'https://www.brentozar.com/go/IndexHoarder' AS URL,
                        N'0 reads: ' + i.db_schema_object_indexid AS details, 
                        i.index_definition, 
                        i.secret_columns, 
                        i.index_usage_summary,
                        sz.index_size_summary
                FROM    #IndexSanity AS i
                JOIN    #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                WHERE    i.total_reads=0
						AND i.user_updates < 10000
                        AND i.index_id NOT IN (0,1) /*NCs only*/
                        AND i.is_unique = 0
                        AND sz.total_reserved_MB >= CASE WHEN (@GetAllDatabases = 1 OR @Mode = 0) THEN @ThresholdMB ELSE sz.total_reserved_MB END
						/*Skipping tables created in the last week, or modified in past 2 days*/
						AND	i.create_date >= DATEADD(dd,-7,GETDATE()) 
						AND i.modify_date > DATEADD(dd,-2,GETDATE())
                ORDER BY i.db_schema_object_indexid
                OPTION    ( RECOMPILE );


        ----------------------------------------
        --Feature-Phobic Indexes: Check_id 30-39
        ---------------------------------------- 
            RAISERROR(N'check_id 30: No indexes with includes', 0,1) WITH NOWAIT;
            /* This does not work the way you'd expect with @GetAllDatabases = 1. For details:
               https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/825
            */

			SELECT  database_name,
					SUM(CASE WHEN count_included_columns > 0 THEN 1 ELSE 0 END) AS number_indexes_with_includes,
					100.* SUM(CASE WHEN count_included_columns > 0 THEN 1 ELSE 0 END) / ( 1.0 * COUNT(*) ) AS percent_indexes_with_includes
			INTO #index_includes
            FROM    #IndexSanity
			WHERE is_hypothetical = 0
			AND is_disabled = 0
			AND NOT (@GetAllDatabases = 1 OR @Mode = 0)
			GROUP BY database_name;

                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  30 AS check_id, 
                                NULL AS index_sanity_id, 
                                250 AS Priority,
                                N'Feature-Phobic Indexes' AS findings_group,
								database_name AS [Database Name],
                                N'No Indexes Use Includes' AS finding, 'https://www.brentozar.com/go/IndexFeatures' AS URL,
                                N'No Indexes Use Includes' AS details,
                                database_name + N' (Entire database)' AS index_definition, 
                                N'' AS secret_columns, 
                                N'N/A' AS index_usage_summary, 
                                N'N/A' AS index_size_summary 
						FROM #index_includes
						WHERE number_indexes_with_includes = 0
						OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 31: < 3 percent of indexes have includes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
					SELECT  31 AS check_id,
					        NULL AS index_sanity_id, 
					        250 AS Priority,
					        N'Feature-Phobic Indexes' AS findings_group,
					        N'Few Indexes Use Includes' AS findings,
					        database_name AS [Database Name],
					        N'https://www.brentozar.com/go/IndexFeatures' AS URL,
					        N'Only ' + CAST(percent_indexes_with_includes AS NVARCHAR(20)) + '% of indexes have includes' AS details, 
					        N'Entire database' AS index_definition, 
					        N'' AS secret_columns,
					        N'N/A' AS index_usage_summary, 
					        N'N/A' AS index_size_summary
					FROM #index_includes
					WHERE number_indexes_with_includes > 0 AND percent_indexes_with_includes <= 3
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 32: filtered indexes and indexed views', 0,1) WITH NOWAIT;

                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
					SELECT  DISTINCT
							32 AS check_id, 
					        NULL AS index_sanity_id,
					        250 AS Priority,
					        N'Feature-Phobic Indexes' AS findings_group,
					        N'No Filtered Indexes or Indexed Views' AS finding, 
					        i.database_name AS [Database Name],
					        N'https://www.brentozar.com/go/IndexFeatures' AS URL,
					        N'These are NOT always needed-- but do you know when you would use them?' AS details,
					        i.database_name + N' (Entire database)' AS index_definition, 
					        N'' AS secret_columns,
					        N'N/A' AS index_usage_summary, 
					        N'N/A' AS index_size_summary 
					FROM #IndexSanity i
					WHERE i.database_name NOT IN (                
							SELECT   database_name
							FROM     #IndexSanity
							WHERE    filter_definition <> '' )
					AND i.database_name NOT IN (
					       SELECT  database_name
						   FROM    #IndexSanity
						   WHERE   is_indexed_view = 1 )
					OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 33: Potential filtered indexes based on column names.', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
					SELECT  33 AS check_id, 
					        i.index_sanity_id AS index_sanity_id,
					        250 AS Priority,
					        N'Feature-Phobic Indexes' AS findings_group,
					        N'Potential Filtered Index (Based on Column Name)' AS finding, 
					        [database_name] AS [Database Name],
					        N'https://www.brentozar.com/go/IndexFeatures' AS URL,
					        N'A column name in this index suggests it might be a candidate for filtering (is%, %archive%, %active%, %flag%)' AS details,
					        i.index_definition, 
					        i.secret_columns,
					        i.index_usage_summary, 
					        sz.index_size_summary
					FROM #IndexColumns ic 
					JOIN #IndexSanity i ON ic.[object_id]=i.[object_id] 
						AND ic.database_id =i.database_id
						AND ic.schema_name = i.schema_name
						AND ic.[index_id]=i.[index_id] 
						AND i.[index_id] > 1 /* non-clustered index */
					JOIN    #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE (column_name LIKE 'is%'
					    OR column_name LIKE '%archive%'
					    OR column_name LIKE '%active%'
					    OR column_name LIKE '%flag%')
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 41: Hypothetical indexes ', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  41 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Self Loathing Indexes' AS findings_group,
                            N'Hypothetical Index' AS finding,
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                            N'Hypothetical Index: ' + db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            N'' AS index_usage_summary, 
                            N'' AS index_size_summary
                    FROM    #IndexSanity AS i
                    WHERE    is_hypothetical = 1 
                    OPTION    ( RECOMPILE );


            RAISERROR(N'check_id 42: Disabled indexes', 0,1) WITH NOWAIT;
            --Note: disabled NC indexes will have O rows in #IndexSanitySize!
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  42 AS check_id, 
                            index_sanity_id,
                            150 AS Priority,
                            N'Self Loathing Indexes' AS findings_group,
                            N'Disabled Index' AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                            N'Disabled Index:' + db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            'DISABLED' AS index_size_summary
                    FROM    #IndexSanity AS i
                    WHERE    is_disabled = 1
                    OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 49: Heaps with deletes', 0,1) WITH NOWAIT;
            WITH    heaps_cte
                      AS ( SELECT   [object_id],
								    [database_id],
								    [schema_name],
                                    SUM(leaf_delete_count) AS leaf_delete_count
                           FROM        #IndexPartitionSanity
                           GROUP BY    [object_id],
								       [database_id],
								       [schema_name]
                           HAVING    SUM(forwarded_fetch_count) < 1000 * @DaysUptime /* Only alert about indexes with no forwarded fetches - we already alerted about those in check_id 43 */
                                    AND SUM(leaf_delete_count) > 0)
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  49 AS check_id, 
                                i.index_sanity_id,
                                200 AS Priority,
                                N'Self Loathing Indexes' AS findings_group,
                                N'Heaps with Deletes' AS finding, 
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/SelfLoathing' AS URL,
                                CAST(h.leaf_delete_count AS NVARCHAR(256)) + N' deletes against heap:'
                                + db_schema_object_indexid AS details, 
                                i.index_definition, 
                                i.secret_columns,
                                i.index_usage_summary,
                                sz.index_size_summary
                        FROM    #IndexSanity i
                        JOIN heaps_cte h ON i.[object_id] = h.[object_id] 
							 AND i.[database_id] = h.[database_id]
							 AND i.[schema_name] = h.[schema_name]
                        JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                        WHERE    i.index_id = 0 
                        AND sz.total_reserved_MB >= CASE WHEN NOT (@GetAllDatabases = 1 OR @Mode = 4) THEN @ThresholdMB ELSE sz.total_reserved_MB END
                OPTION    ( RECOMPILE );

         ----------------------------------------
        --Abnormal Psychology : Check_id 60-79
        ----------------------------------------
            RAISERROR(N'check_id 60: XML indexes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  60 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'XML Index' AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            N'' AS index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.is_XML = 1 
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 61: Columnstore indexes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  61 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            CASE WHEN i.is_NC_columnstore=1
                                THEN N'NC Columnstore Index' 
                                ELSE N'Clustered Columnstore Index' 
                                END AS finding, 
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.is_NC_columnstore = 1 OR i.is_CX_columnstore=1
                    OPTION    ( RECOMPILE );


            RAISERROR(N'check_id 62: Spatial indexes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  62 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Spatial Index' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.is_spatial = 1 
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 63: Compressed indexes', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  63 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Compressed Index' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid  + N'. COMPRESSION: ' + sz.data_compression_desc AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE sz.data_compression_desc LIKE '%PAGE%' OR sz.data_compression_desc LIKE '%ROW%' 
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 64: Partitioned', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  64 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Partitioned Index' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.partition_key_column_name IS NOT NULL 
					OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 65: Non-Aligned Partitioned', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  65 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Non-Aligned Index on a Partitioned Table' AS finding,
                            i.[database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanity AS iParent ON
                        i.[object_id]=iParent.[object_id]
						AND i.database_id = iParent.database_id
						AND i.schema_name = iParent.schema_name
                        AND iParent.index_id IN (0,1) /* could be a partitioned heap or clustered table */
                        AND iParent.partition_key_column_name IS NOT NULL /* parent is partitioned*/         
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.partition_key_column_name IS NULL 
                    OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 66: Recently created tables/indexes (1 week)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  66 AS check_id, 
                            i.index_sanity_id,
                            200 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Recently Created Tables/Indexes (1 week)' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid + N' was created on ' + 
                                CONVERT(NVARCHAR(16),i.create_date,121) + 
                                N'. Tables/indexes which are dropped/created regularly require special methods for index tuning.'
                                     AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.create_date >= DATEADD(dd,-7,GETDATE()) 
                    OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 67: Recently modified tables/indexes (2 days)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  67 AS check_id, 
                            i.index_sanity_id,
                            200 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Recently Modified Tables/Indexes (2 days)' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid + N' was modified on ' + 
                                CONVERT(NVARCHAR(16),i.modify_date,121) + 
                                N'. A large amount of recently modified indexes may mean a lot of rebuilds are occurring each night.'
                                     AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.modify_date > DATEADD(dd,-2,GETDATE()) 
                    AND /*Exclude recently created tables.*/
                    i.create_date < DATEADD(dd,-7,GETDATE()) 
                    OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 69: Column collation does not match database collation', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT [object_id],
								   database_id,
								   schema_name,
                                   COUNT(*) AS column_count
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            AND collation_name <> @collation
                            GROUP BY [object_id],
								     database_id,
								     schema_name
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  69 AS check_id, 
                                i.index_sanity_id, 
                                150 AS Priority,
                                N'Abnormal Psychology' AS findings_group,
                                N'Column Collation Does Not Match Database Collation' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                                i.db_schema_object_name 
                                    + N' has ' + CAST(column_count AS NVARCHAR(20))
                                    + N' column' + CASE WHEN column_count > 1 THEN 's' ELSE '' END
                                    + N' with a different collation than the db collation of '
                                    + @collation    AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
								AND cc.database_id = i.database_id
								AND cc.schema_name = i.schema_name
                        WHERE    i.index_id IN (1,0)
                        ORDER BY i.db_schema_object_name DESC OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 70: Replicated columns', 0,1) WITH NOWAIT;
                WITH count_columns AS (
                            SELECT [object_id],
								   database_id,
								   schema_name,
                                   COUNT(*) AS column_count,
                                   SUM(CASE is_replicated WHEN 1 THEN 1 ELSE 0 END) AS replicated_column_count
                            FROM #IndexColumns ic
                            WHERE index_id IN (1,0) /*Heap or clustered only*/
                            GROUP BY object_id,
								     database_id,
								     schema_name
                            )
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                        SELECT  70 AS check_id, 
                                i.index_sanity_id,
                                200 AS Priority, 
                                N'Abnormal Psychology' AS findings_group,
                                N'Replicated Columns' AS finding,
                                [database_name] AS [Database Name],
                                N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                                i.db_schema_object_name 
                                    + N' has ' + CAST(replicated_column_count AS NVARCHAR(20))
                                    + N' out of ' + CAST(column_count AS NVARCHAR(20))
                                    + N' column' + CASE WHEN column_count > 1 THEN 's' ELSE '' END
                                    + N' in one or more publications.'
                                        AS details,
                                i.index_definition,
                                secret_columns, 
                                ISNULL(i.index_usage_summary,''),
                                ISNULL(ip.index_size_summary,'')
                        FROM    #IndexSanity i
                        JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                        JOIN    count_columns AS cc ON i.[object_id]=cc.[object_id]
								AND i.database_id = cc.database_id
								AND i.schema_name = cc.schema_name
                        WHERE    i.index_id IN (1,0)
                            AND replicated_column_count > 0
                        ORDER BY i.db_schema_object_name DESC 
						OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 71: Cascading updates or cascading deletes.', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary, more_info )
            SELECT  71 AS check_id, 
                    NULL AS index_sanity_id,
                    150 AS Priority,
                    N'Abnormal Psychology' AS findings_group,
                    N'Cascading Updates or Deletes' AS finding, 
                    [database_name] AS [Database Name],
                    N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                    N'Foreign Key ' + QUOTENAME(foreign_key_name) +
                    N' on ' + QUOTENAME(parent_object_name)  + N'(' + LTRIM(parent_fk_columns) + N')'
                        + N' referencing ' + QUOTENAME(referenced_object_name) + N'(' + LTRIM(referenced_fk_columns) + N')'
                        + N' has settings:'
                        + CASE [delete_referential_action_desc] WHEN N'NO_ACTION' THEN N'' ELSE N' ON DELETE ' +[delete_referential_action_desc] END
                        + CASE [update_referential_action_desc] WHEN N'NO_ACTION' THEN N'' ELSE N' ON UPDATE ' + [update_referential_action_desc] END
                            AS details, 
                    [fk].[database_name] AS index_definition, 
                    N'N/A' AS secret_columns,
                    N'N/A' AS index_usage_summary,
                    N'N/A' AS index_size_summary,
                    (SELECT TOP 1 more_info FROM #IndexSanity i WHERE i.object_id=fk.parent_object_id AND i.database_id = fk.database_id AND i.schema_name = fk.schema_name)
                        AS more_info
            FROM #ForeignKeys fk
            WHERE ([delete_referential_action_desc] <> N'NO_ACTION'
            OR [update_referential_action_desc] <> N'NO_ACTION')
			OPTION    ( RECOMPILE );

            RAISERROR(N'check_id 72: Unindexed foreign keys.', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary, more_info )
            SELECT  72 AS check_id, 
                    NULL AS index_sanity_id,
                    150 AS Priority,
                    N'Abnormal Psychology' AS findings_group,
                    N'Unindexed Foreign Keys' AS finding, 
                    [database_name] AS [Database Name],
                    N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                    N'Foreign Key ' + QUOTENAME(foreign_key_name) +
                    N' on ' + QUOTENAME(parent_object_name)  + N''
                        + N' referencing ' + QUOTENAME(referenced_object_name) + N''
                        + N' does not appear to have a supporting index.' AS details, 
                    N'N/A' AS index_definition, 
                    N'N/A' AS secret_columns,
                    N'N/A' AS index_usage_summary,
                    N'N/A' AS index_size_summary,
                    (SELECT TOP 1 more_info FROM #IndexSanity i WHERE i.object_id=fk.parent_object_id AND i.database_id = fk.database_id AND i.schema_name = fk.schema_name)
                        AS more_info
            FROM #UnindexedForeignKeys AS fk
			OPTION    ( RECOMPILE );


            RAISERROR(N'check_id 73: In-Memory OLTP', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
                    SELECT  73 AS check_id, 
                            i.index_sanity_id,
                            150 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'In-Memory OLTP' AS finding,
                            [database_name] AS [Database Name], 
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_indexid AS details, 
                            i.index_definition,
                            i.secret_columns,
                            i.index_usage_summary,
                            ISNULL(sz.index_size_summary,'') AS index_size_summary
                    FROM    #IndexSanity AS i
                    JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
                    WHERE i.is_in_memory_oltp = 1
					OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 74: Identity column with unusual seed', 0,1) WITH NOWAIT;
            INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                            secret_columns, index_usage_summary, index_size_summary )
                    SELECT  74 AS check_id, 
                            i.index_sanity_id, 
                            200 AS Priority,
                            N'Abnormal Psychology' AS findings_group,
                            N'Identity Column Using a Negative Seed or Increment Other Than 1' AS finding,
                            [database_name] AS [Database Name],
                            N'https://www.brentozar.com/go/AbnormalPsychology' AS URL,
                            i.db_schema_object_name + N'.' +  QUOTENAME(ic.column_name)
                                + N' is an identity with type ' + ic.system_type_name 
                                + N', last value of ' 
                                    + ISNULL((CONVERT(NVARCHAR(256),CAST(ic.last_value AS DECIMAL(38,0)), 1)),N'NULL')
                                + N', seed of '
                                    + ISNULL((CONVERT(NVARCHAR(256),CAST(ic.seed_value AS DECIMAL(38,0)), 1)),N'NULL')
                                + N', increment of ' + CAST(ic.increment_value AS NVARCHAR(256)) 
                                + N', and range of ' +
                                    CASE ic.system_type_name WHEN 'int' THEN N'+/- 2,147,483,647'
                                        WHEN 'smallint' THEN N'+/- 32,768'
                                        WHEN 'tinyint' THEN N'0 to 255'
                                        ELSE 'unknown'
                                    END
                                    AS details,
                            i.index_definition,
                            secret_columns, 
                            ISNULL(i.index_usage_summary,''),
                            ISNULL(ip.index_size_summary,'')
                    FROM    #IndexSanity i
                    JOIN    #IndexColumns ic ON
                        i.object_id=ic.object_id
						AND i.database_id = ic.database_id
						AND i.schema_name = ic.schema_name
                        AND i.index_id IN (0,1) /* heaps and cx only */
                        AND ic.is_identity=1
                        AND ic.system_type_name IN ('tinyint', 'smallint', 'int')
                    JOIN    #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
                    WHERE    i.index_id IN (1,0)
                        AND (ic.seed_value < 0 OR ic.increment_value <> 1)
                    ORDER BY finding, details DESC 
					OPTION    ( RECOMPILE );

        ----------------------------------------
        --Workaholics: Check_id 80-89
        ----------------------------------------

        RAISERROR(N'check_id 80: Most scanned indexes (index_usage_stats)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )

        --Workaholics according to index_usage_stats
        --This isn't perfect: it mentions the number of scans present in a plan
        --A "scan" isn't necessarily a full scan, but hey, we gotta do the best with what we've got.
        --in the case of things like indexed views, the operator might be in the plan but never executed
        SELECT TOP 5 
            80 AS check_id,
            i.index_sanity_id AS index_sanity_id,
            200 AS Priority,
            N'Workaholics' AS findings_group,
            N'Scan-a-lots (index-usage-stats)' AS finding,
            [database_name] AS [Database Name],
            N'https://www.brentozar.com/go/Workaholics' AS URL,
            REPLACE(CONVERT( NVARCHAR(50),CAST(i.user_scans AS MONEY),1),'.00','')
                + N' scans against ' + i.db_schema_object_indexid
                + N'. Latest scan: ' + ISNULL(CAST(i.last_user_scan AS NVARCHAR(128)),'?') + N'. ' 
                + N'ScanFactor=' + CAST(((i.user_scans * iss.total_reserved_MB)/1000000.) AS NVARCHAR(256)) AS details,
            ISNULL(i.key_column_names_with_sort_order,'N/A') AS index_definition,
            ISNULL(i.secret_columns,'') AS secret_columns,
            i.index_usage_summary AS index_usage_summary,
            iss.index_size_summary AS index_size_summary
        FROM #IndexSanity i
        JOIN #IndexSanitySize iss ON i.index_sanity_id=iss.index_sanity_id
        WHERE ISNULL(i.user_scans,0) > 0
        ORDER BY  i.user_scans * iss.total_reserved_MB DESC
		OPTION    ( RECOMPILE );

        RAISERROR(N'check_id 81: Top recent accesses (op stats)', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, index_sanity_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
        --Workaholics according to index_operational_stats
        --This isn't perfect either: range_scan_count contains full scans, partial scans, even seeks in nested loop ops
        --But this can help bubble up some most-accessed tables 
        SELECT TOP 5 
            81 AS check_id,
            i.index_sanity_id AS index_sanity_id,
            200 AS Priority,
            N'Workaholics' AS findings_group,
            N'Top Recent Accesses (index-op-stats)' AS finding,
            [database_name] AS [Database Name],
            N'https://www.brentozar.com/go/Workaholics' AS URL,
            ISNULL(REPLACE(
                    CONVERT(NVARCHAR(50),CAST((iss.total_range_scan_count + iss.total_singleton_lookup_count) AS MONEY),1),
                    N'.00',N'') 
                + N' uses of ' + i.db_schema_object_indexid + N'. '
                + REPLACE(CONVERT(NVARCHAR(50), CAST(iss.total_range_scan_count AS MONEY),1),N'.00',N'') + N' scans or seeks. '
                + REPLACE(CONVERT(NVARCHAR(50), CAST(iss.total_singleton_lookup_count AS MONEY), 1),N'.00',N'') + N' singleton lookups. '
                + N'OpStatsFactor=' + CAST(((((iss.total_range_scan_count + iss.total_singleton_lookup_count) * iss.total_reserved_MB))/1000000.) AS VARCHAR(256)),'') AS details,
            ISNULL(i.key_column_names_with_sort_order,'N/A') AS index_definition,
            ISNULL(i.secret_columns,'') AS secret_columns,
            i.index_usage_summary AS index_usage_summary,
            iss.index_size_summary AS index_size_summary
        FROM #IndexSanity i
        JOIN #IndexSanitySize iss ON i.index_sanity_id=iss.index_sanity_id
        WHERE (ISNULL(iss.total_range_scan_count,0)  > 0 OR ISNULL(iss.total_singleton_lookup_count,0) > 0)
        ORDER BY ((iss.total_range_scan_count + iss.total_singleton_lookup_count) * iss.total_reserved_MB) DESC
		OPTION    ( RECOMPILE );



        RAISERROR(N'check_id 93: Statistics with filters', 0,1) WITH NOWAIT;
                INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  93 AS check_id, 
				200 AS Priority,
				'Functioning Statistaholics' AS findings_group,
				'Filter Fixation',
				s.database_name,
				'https://www.brentozar.com/go/stats' AS URL,
				'The statistic ' + QUOTENAME(s.statistics_name) +  ' is filtered on [' + s.filter_definition + ']. It could be part of a filtered index, or just a filtered statistic. This is purely informational.' AS details,
				 QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' + QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' + QUOTENAME(s.statistics_name) + '.' + QUOTENAME(s.column_names) AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #Statistics AS s
		WHERE s.has_filter = 1
		OPTION    ( RECOMPILE );


		RAISERROR(N'check_id 100: Computed Columns that are not Persisted.', 0,1) WITH NOWAIT;
        INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )
		SELECT  100 AS check_id, 
				200 AS Priority,
				'Cold Calculators' AS findings_group,
				'Definition Defeatists' AS finding,
				cc.database_name,
				'' AS URL,
				'The computed column ' + QUOTENAME(cc.column_name) + ' on ' + QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' is not persisted, which means it will be calculated when a query runs.' + 
				'You can change this with the following command, if the definition is deterministic: ALTER TABLE ' + QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' ALTER COLUMN ' + cc.column_name +
				' ADD PERSISTED'  AS details,
				cc.column_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #ComputedColumns AS cc
		WHERE cc.is_persisted = 0
		OPTION    ( RECOMPILE );

		RAISERROR(N'check_id 110: Temporal Tables.', 0,1) WITH NOWAIT;
        INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )

				SELECT  110 AS check_id, 
				200 AS Priority,
				'Abnormal Psychology' AS findings_group,
				'Temporal Tables',
				t.database_name,
				'' AS URL,
				'The table ' + QUOTENAME(t.schema_name) + '.' + QUOTENAME(t.table_name) + ' is a temporal table, with rows versioned in ' 
					+ QUOTENAME(t.history_schema_name) + '.' + QUOTENAME(t.history_table_name) + ' on History columns ' + QUOTENAME(t.start_column_name) + ' and ' + QUOTENAME(t.end_column_name) + '.'
				 AS details,
				'' AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #TemporalTables AS t
		OPTION    ( RECOMPILE );

		RAISERROR(N'check_id 121: Optimized For Sequental Keys.', 0,1) WITH NOWAIT;
        INSERT    #BlitzIndexResults ( check_id, Priority, findings_group, finding, [database_name], URL, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary )

				SELECT  121 AS check_id, 
				200 AS Priority,
				'Medicated Indexes' AS findings_group,
				'Optimized For Sequential Keys',
				i.database_name,
				'' AS URL,
				'The table ' + QUOTENAME(i.schema_name) + '.' + QUOTENAME(i.object_name) + ' is optimized for sequential keys.' AS details,
				'' AS index_definition,
				'N/A' AS secret_columns,
				'N/A' AS index_usage_summary,
				'N/A' AS index_size_summary
		FROM #IndexSanity AS i
		WHERE i.optimize_for_sequential_key = 1
		OPTION    ( RECOMPILE );



	END /* IF @Mode = 4 */



























 
        RAISERROR(N'Insert a row to help people find help', 0,1) WITH NOWAIT;
        IF DATEDIFF(MM, @VersionDate, GETDATE()) > 6
		BEGIN
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( -1, 0 , 
		           'Outdated sp_BlitzIndex', 'sp_BlitzIndex is Over 6 Months Old', 'http://FirstResponderKit.org/', 
                   'Fine wine gets better with age, but this ' + @ScriptVersionName + ' is more like bad cheese. Time to get a new one.',
                    @DaysUptimeInsertValue,N'',N''
                    );
        END;

        IF EXISTS(SELECT * FROM #BlitzIndexResults)
		BEGIN
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( -1, 0 , 
		            @ScriptVersionName,
                    CASE WHEN @GetAllDatabases = 1 THEN N'All Databases' ELSE N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + CONVERT(NVARCHAR(16),GETDATE(),121) END, 
                    N'From Your Community Volunteers' ,   N'http://FirstResponderKit.org' ,
                    @DaysUptimeInsertValue,N'',N''
                    );
        END;
        ELSE IF @Mode = 0 OR (@GetAllDatabases = 1 AND @Mode <> 4)
        BEGIN
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( -1, 0 , 
		            @ScriptVersionName,
                    CASE WHEN @GetAllDatabases = 1 THEN N'All Databases' ELSE N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + CONVERT(NVARCHAR(16),GETDATE(),121) END, 
                    N'From Your Community Volunteers' ,   N'http://FirstResponderKit.org' ,
                    @DaysUptimeInsertValue, N'',N''
                    );
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( 1, 0 , 
		           N'No Major Problems Found',
                   N'Nice Work!',
                   N'http://FirstResponderKit.org', 
                   N'Consider running with @Mode = 4 in individual databases (not all) for more detailed diagnostics.', 
                   N'The default Mode 0 only looks for very serious index issues.', 
                   @DaysUptimeInsertValue, N''
                    );

        END;
        ELSE
        BEGIN
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( -1, 0 , 
		            @ScriptVersionName,
                    CASE WHEN @GetAllDatabases = 1 THEN N'All Databases' ELSE N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + CONVERT(NVARCHAR(16),GETDATE(),121) END, 
                    N'From Your Community Volunteers' ,   N'http://FirstResponderKit.org' ,
                    @DaysUptimeInsertValue, N'',N''
                    );
            INSERT    #BlitzIndexResults ( Priority, check_id, findings_group, finding, URL, details, index_definition,
                                            index_usage_summary, index_size_summary )
            VALUES  ( 1, 0 , 
		           N'No Problems Found',
                   N'Nice job! Or more likely, you have a nearly empty database.',
                   N'http://FirstResponderKit.org', 'Time to go read some blog posts.', 
                   @DaysUptimeInsertValue, N'', N''
                    );

        END;

        RAISERROR(N'Returning results.', 0,1) WITH NOWAIT;
            
        /*Return results.*/
        IF (@Mode = 0)
        BEGIN
			IF(@OutputType <> 'NONE')
			BEGIN
				SELECT Priority, ISNULL(br.findings_group,N'') + 
						CASE WHEN ISNULL(br.finding,N'') <> N'' THEN N': ' ELSE N'' END
						+ br.finding AS [Finding], 
					br.[database_name] AS [Database Name],
					br.details AS [Details: schema.table.index(indexid)], 
					br.index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
					ISNULL(br.secret_columns,'') AS [Secret Columns],          
					br.index_usage_summary AS [Usage], 
					br.index_size_summary AS [Size],
					COALESCE(br.more_info,sn.more_info,'') AS [More Info],
					br.URL, 
					COALESCE(br.create_tsql,ts.create_tsql,'') AS [Create TSQL],
                    br.sample_query_plan AS [Sample Query Plan]
				FROM #BlitzIndexResults br
				LEFT JOIN #IndexSanity sn ON 
					br.index_sanity_id=sn.index_sanity_id
				LEFT JOIN #IndexCreateTsql ts ON 
					br.index_sanity_id=ts.index_sanity_id
				ORDER BY br.Priority ASC, br.check_id ASC, br.blitz_result_id ASC, br.findings_group ASC
				OPTION (RECOMPILE);
			 END;

        END;
        ELSE IF (@Mode = 4)
			IF(@OutputType <> 'NONE')
		 	BEGIN	
				SELECT Priority, ISNULL(br.findings_group,N'') + 
						CASE WHEN ISNULL(br.finding,N'') <> N'' THEN N': ' ELSE N'' END
						+ br.finding AS [Finding], 
					br.[database_name] AS [Database Name],
					br.details AS [Details: schema.table.index(indexid)], 
					br.index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
					ISNULL(br.secret_columns,'') AS [Secret Columns],          
					br.index_usage_summary AS [Usage], 
					br.index_size_summary AS [Size],
					COALESCE(br.more_info,sn.more_info,'') AS [More Info],
					br.URL, 
					COALESCE(br.create_tsql,ts.create_tsql,'') AS [Create TSQL],
                    br.sample_query_plan AS [Sample Query Plan]
				FROM #BlitzIndexResults br
				LEFT JOIN #IndexSanity sn ON 
					br.index_sanity_id=sn.index_sanity_id
				LEFT JOIN #IndexCreateTsql ts ON 
					br.index_sanity_id=ts.index_sanity_id
				ORDER BY br.Priority ASC, br.check_id ASC, br.blitz_result_id ASC, br.findings_group ASC
				OPTION (RECOMPILE);
			 END;

END /* End @Mode=0 or 4 (diagnose)*/

ELSE IF (@Mode=1) /*Summarize*/
    BEGIN
    --This mode is to give some overall stats on the database.
	 	IF(@OutputType <> 'NONE')
	 	BEGIN
			RAISERROR(N'@Mode=1, we are summarizing.', 0,1) WITH NOWAIT;

			SELECT DB_NAME(i.database_id) AS [Database Name],
				CAST((COUNT(*)) AS NVARCHAR(256)) AS [Number Objects],
				CAST(CAST(SUM(sz.total_reserved_MB)/
					1024. AS NUMERIC(29,1)) AS NVARCHAR(500)) AS [All GB],
				CAST(CAST(SUM(sz.total_reserved_LOB_MB)/
					1024. AS NUMERIC(29,1)) AS NVARCHAR(500)) AS [LOB GB],
				CAST(CAST(SUM(sz.total_reserved_row_overflow_MB)/
					1024. AS NUMERIC(29,1)) AS NVARCHAR(500)) AS [Row Overflow GB],
				CAST(SUM(CASE WHEN index_id=1 THEN 1 ELSE 0 END)AS NVARCHAR(50)) AS [Clustered Tables],
				CAST(SUM(CASE WHEN index_id=1 THEN sz.total_reserved_MB ELSE 0 END)
					/1024. AS NUMERIC(29,1)) AS [Clustered Tables GB],
				SUM(CASE WHEN index_id NOT IN (0,1) THEN 1 ELSE 0 END) AS [NC Indexes],
				CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
					/1024. AS NUMERIC(29,1)) AS [NC Indexes GB],
				CASE WHEN SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)  > 0 THEN
					CAST(SUM(CASE WHEN index_id IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
						/ SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) AS NUMERIC(29,1)) 
					ELSE 0 END AS [ratio table: NC Indexes],
				SUM(CASE WHEN index_id=0 THEN 1 ELSE 0 END) AS [Heaps],
				CAST(SUM(CASE WHEN index_id=0 THEN sz.total_reserved_MB ELSE 0 END)
					/1024. AS NUMERIC(29,1)) AS [Heaps GB],
				SUM(CASE WHEN index_id IN (0,1) AND partition_key_column_name IS NOT NULL THEN 1 ELSE 0 END) AS [Partitioned Tables],
				SUM(CASE WHEN index_id NOT IN (0,1) AND  partition_key_column_name IS NOT NULL THEN 1 ELSE 0 END) AS [Partitioned NCs],
				CAST(SUM(CASE WHEN partition_key_column_name IS NOT NULL THEN sz.total_reserved_MB ELSE 0 END)/1024. AS NUMERIC(29,1)) AS [Partitioned GB],
				SUM(CASE WHEN filter_definition <> '' THEN 1 ELSE 0 END) AS [Filtered Indexes],
				SUM(CASE WHEN is_indexed_view=1 THEN 1 ELSE 0 END) AS [Indexed Views],
				MAX(total_rows) AS [Max Row Count],
				CAST(MAX(CASE WHEN index_id IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
					/1024. AS NUMERIC(29,1)) AS [Max Table GB],
				CAST(MAX(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
					/1024. AS NUMERIC(29,1)) AS [Max NC Index GB],
				SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 1024 THEN 1 ELSE 0 END) AS [Count Tables > 1GB],
				SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 10240 THEN 1 ELSE 0 END) AS [Count Tables > 10GB],
				SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 102400 THEN 1 ELSE 0 END) AS [Count Tables > 100GB],    
				SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 1024 THEN 1 ELSE 0 END) AS [Count NCs > 1GB],
				SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 10240 THEN 1 ELSE 0 END) AS [Count NCs > 10GB],
				SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 102400 THEN 1 ELSE 0 END) AS [Count NCs > 100GB],
				MIN(create_date) AS [Oldest Create Date],
				MAX(create_date) AS [Most Recent Create Date],
				MAX(modify_date) AS [Most Recent Modify Date],
				1 AS [Display Order]
			FROM #IndexSanity AS i
			--left join here so we don't lose disabled nc indexes
			LEFT JOIN #IndexSanitySize AS sz 
				ON i.index_sanity_id=sz.index_sanity_id
			GROUP BY DB_NAME(i.database_id)	 
			UNION ALL
			SELECT  CASE WHEN @GetAllDatabases = 1 THEN N'All Databases' ELSE N'Database ' + N' as of ' + CONVERT(NVARCHAR(16),GETDATE(),121) END,        
					@ScriptVersionName,   
					N'From Your Community Volunteers' ,   
					N'http://FirstResponderKit.org' ,
					@DaysUptimeInsertValue,
					NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
					NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
					NULL,NULL,0 AS display_order
			ORDER BY [Display Order] ASC
			OPTION (RECOMPILE);
	  	END;
           
    END; /* End @Mode=1 (summarize)*/
    ELSE IF (@Mode=2) /*Index Detail*/
    BEGIN
        --This mode just spits out all the detail without filters.
        --This supports slicing AND dicing in Excel
        RAISERROR(N'@Mode=2, here''s the details on existing indexes.', 0,1) WITH NOWAIT;

		
		/* Checks if @OutputServerName is populated with a valid linked server, and that the database name specified is valid */
		DECLARE @ValidOutputServer BIT;
		DECLARE @ValidOutputLocation BIT;
		DECLARE @LinkedServerDBCheck NVARCHAR(2000);
		DECLARE @ValidLinkedServerDB INT;
		DECLARE @tmpdbchk TABLE (cnt INT);
		
		IF @OutputServerName IS NOT NULL
			BEGIN
				IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
					BEGIN
						RAISERROR('Due to the nature of temporary tables, outputting to a linked server requires a permanent table.', 16, 0);
					END;
				ELSE IF EXISTS (SELECT server_id FROM sys.servers WHERE QUOTENAME([name]) = @OutputServerName)
					BEGIN
						SET @LinkedServerDBCheck = 'SELECT 1 WHERE EXISTS (SELECT * FROM '+@OutputServerName+'.master.sys.databases WHERE QUOTENAME([name]) = '''+@OutputDatabaseName+''')';
						INSERT INTO @tmpdbchk EXEC sys.sp_executesql @LinkedServerDBCheck;
						SET @ValidLinkedServerDB = (SELECT COUNT(*) FROM @tmpdbchk);
						IF (@ValidLinkedServerDB > 0)
							BEGIN
								SET @ValidOutputServer = 1;
								SET @ValidOutputLocation = 1;
							END;
						ELSE
							RAISERROR('The specified database was not found on the output server', 16, 0);
					END;
				ELSE
					BEGIN
						RAISERROR('The specified output server was not found', 16, 0);
					END;
			END;
		ELSE
			BEGIN
				IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
					BEGIN
						SET @StringToExecute = N' IF (OBJECT_ID(''[tempdb].[dbo].@@@OutputTableName@@@'') IS NOT NULL) DROP TABLE @@@OutputTableName@@@';
						SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputTableName@@@', @OutputTableName); 
						EXEC(@StringToExecute);
						
						SET @OutputServerName = QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)));
						SET @OutputDatabaseName = '[tempdb]';
						SET @OutputSchemaName = '[dbo]';
						SET @ValidOutputLocation = 1;
					END;
				ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
					BEGIN
						RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
					END;
				ELSE IF @OutputDatabaseName IS NOT NULL
					AND @OutputSchemaName IS NOT NULL
					AND @OutputTableName IS NOT NULL
					AND EXISTS ( SELECT *
						 FROM   sys.databases
						 WHERE  QUOTENAME([name]) = @OutputDatabaseName)
					BEGIN
						SET @ValidOutputLocation = 1;
						SET @OutputServerName = QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)));
					END;
				ELSE IF @OutputDatabaseName IS NOT NULL
					AND @OutputSchemaName IS NOT NULL
					AND @OutputTableName IS NOT NULL
					AND NOT EXISTS ( SELECT *
						 FROM   sys.databases
						 WHERE  QUOTENAME([name]) = @OutputDatabaseName)
					BEGIN
						RAISERROR('The specified output database was not found on this server', 16, 0);
					END;
				ELSE
					BEGIN
						SET @ValidOutputLocation = 0; 
					END;
			END;
																										
        IF (@ValidOutputLocation = 0 AND @OutputType = 'NONE')
        BEGIN
            RAISERROR('Invalid output location and no output asked',12,1);
            RETURN;
        END;
																										
		/* @OutputTableName lets us export the results to a permanent table */
		DECLARE @RunID UNIQUEIDENTIFIER;
		SET @RunID = NEWID();
		
		IF (@ValidOutputLocation = 1 AND COALESCE(@OutputServerName, @OutputDatabaseName, @OutputSchemaName, @OutputTableName) IS NOT NULL)
			BEGIN
				DECLARE @TableExists BIT;
				DECLARE @SchemaExists BIT;
				SET @StringToExecute = 
					N'SET @SchemaExists = 0;
					SET @TableExists = 0;
					IF EXISTS(SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''@@@OutputSchemaName@@@'') 
						SET @SchemaExists = 1
					IF EXISTS (SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''@@@OutputSchemaName@@@'' AND QUOTENAME(TABLE_NAME) = ''@@@OutputTableName@@@'')
					BEGIN
						SET @TableExists = 1
						IF NOT EXISTS(SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.COLUMNS WHERE QUOTENAME(TABLE_SCHEMA) = ''@@@OutputSchemaName@@@''
										AND QUOTENAME(TABLE_NAME) = ''@@@OutputTableName@@@'' AND QUOTENAME(COLUMN_NAME) = ''[total_forwarded_fetch_count]'')
							EXEC @@@OutputServerName@@@.@@@OutputDatabaseName@@@.dbo.sp_executesql N''ALTER TABLE @@@OutputSchemaName@@@.@@@OutputTableName@@@ ADD [total_forwarded_fetch_count] BIGINT''
					END';
	
				SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputServerName@@@', @OutputServerName);
				SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputDatabaseName@@@', @OutputDatabaseName);
				SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputSchemaName@@@', @OutputSchemaName); 
				SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputTableName@@@', @OutputTableName);
	
				EXEC sp_executesql @StringToExecute, N'@TableExists BIT OUTPUT, @SchemaExists BIT OUTPUT', @TableExists OUTPUT, @SchemaExists OUTPUT;
				
				IF @SchemaExists = 1
					BEGIN
						IF @TableExists = 0
							BEGIN
								SET @StringToExecute = 
									N'CREATE TABLE @@@OutputDatabaseName@@@.@@@OutputSchemaName@@@.@@@OutputTableName@@@ 
										(
											[id] INT IDENTITY(1,1) NOT NULL, 
											[run_id] UNIQUEIDENTIFIER,
											[run_datetime] DATETIME, 
											[server_name] NVARCHAR(128), 
											[database_name] NVARCHAR(128), 
											[schema_name] NVARCHAR(128), 
											[table_name] NVARCHAR(128), 
											[index_name] NVARCHAR(128),
                                            [Drop_Tsql] NVARCHAR(MAX),
                                            [Create_Tsql] NVARCHAR(MAX), 
											[index_id] INT, 
											[db_schema_object_indexid] NVARCHAR(500), 
											[object_type] NVARCHAR(15), 
											[index_definition] NVARCHAR(MAX), 
											[key_column_names_with_sort_order] NVARCHAR(MAX), 
											[count_key_columns] INT, 
											[include_column_names] NVARCHAR(MAX), 
											[count_included_columns] INT, 
											[secret_columns] NVARCHAR(MAX), 
											[count_secret_columns] INT, 
											[partition_key_column_name] NVARCHAR(MAX), 
											[filter_definition] NVARCHAR(MAX), 
											[is_indexed_view] BIT, 
											[is_primary_key] BIT,
											[is_unique_constraint] BIT,
											[is_XML] BIT, 
											[is_spatial] BIT, 
											[is_NC_columnstore] BIT, 
											[is_CX_columnstore] BIT, 
											[is_in_memory_oltp] BIT, 
											[is_disabled] BIT, 
											[is_hypothetical] BIT, 
											[is_padded] BIT, 
											[fill_factor] INT, 
											[is_referenced_by_foreign_key] BIT,
											[last_user_seek] DATETIME, 
											[last_user_scan] DATETIME, 
											[last_user_lookup] DATETIME, 
											[last_user_update] DATETIME, 
											[total_reads] BIGINT, 
											[user_updates] BIGINT, 
											[reads_per_write] MONEY, 
											[index_usage_summary] NVARCHAR(200), 
											[total_singleton_lookup_count] BIGINT, 
											[total_range_scan_count] BIGINT, 
											[total_leaf_delete_count] BIGINT, 
											[total_leaf_update_count] BIGINT, 
											[index_op_stats] NVARCHAR(200), 
											[partition_count] INT, 
											[total_rows] BIGINT, 
											[total_reserved_MB] NUMERIC(29,2), 
											[total_reserved_LOB_MB] NUMERIC(29,2), 
											[total_reserved_row_overflow_MB] NUMERIC(29,2), 
											[index_size_summary] NVARCHAR(300), 
											[total_row_lock_count] BIGINT, 
											[total_row_lock_wait_count] BIGINT, 
											[total_row_lock_wait_in_ms] BIGINT, 
											[avg_row_lock_wait_in_ms] BIGINT, 
											[total_page_lock_count] BIGINT, 
											[total_page_lock_wait_count] BIGINT, 
											[total_page_lock_wait_in_ms] BIGINT, 
											[avg_page_lock_wait_in_ms] BIGINT, 
											[total_index_lock_promotion_attempt_count] BIGINT, 
											[total_index_lock_promotion_count] BIGINT, 
											[total_forwarded_fetch_count] BIGINT,
											[data_compression_desc] NVARCHAR(4000), 
						                    [page_latch_wait_count] BIGINT,
								            [page_latch_wait_in_ms] BIGINT,
								            [page_io_latch_wait_count] BIGINT,								
								            [page_io_latch_wait_in_ms] BIGINT,
											[create_date] DATETIME, 
											[modify_date] DATETIME, 
											[more_info] NVARCHAR(500),
											[display_order] INT,
											CONSTRAINT [PK_ID_@@@RunID@@@] PRIMARY KEY CLUSTERED ([id] ASC)
										);';
		
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputDatabaseName@@@', @OutputDatabaseName);
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputSchemaName@@@', @OutputSchemaName); 
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputTableName@@@', @OutputTableName); 
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@RunID@@@', @RunID); 
								
								IF @ValidOutputServer = 1
									BEGIN
										SET @StringToExecute = REPLACE(@StringToExecute,'''','''''');
										EXEC('EXEC('''+@StringToExecute+''') AT ' + @OutputServerName);
									END;   
								ELSE
									BEGIN
										EXEC(@StringToExecute);
									END;
							END; /* @TableExists = 0 */
					
						SET @StringToExecute = 
							N'IF EXISTS(SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''@@@OutputSchemaName@@@'') 
								AND NOT EXISTS (SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''@@@OutputSchemaName@@@'' AND QUOTENAME(TABLE_NAME) = ''@@@OutputTableName@@@'')
								SET @TableExists = 0
							ELSE
								SET @TableExists = 1';
				
						SET @TableExists = NULL;
						SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputServerName@@@', @OutputServerName);
						SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputDatabaseName@@@', @OutputDatabaseName);
						SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputSchemaName@@@', @OutputSchemaName); 
						SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputTableName@@@', @OutputTableName); 
			
						EXEC sp_executesql @StringToExecute, N'@TableExists BIT OUTPUT', @TableExists OUTPUT;
						
						IF @TableExists = 1
							BEGIN
								SET @StringToExecute = 
									N'INSERT @@@OutputServerName@@@.@@@OutputDatabaseName@@@.@@@OutputSchemaName@@@.@@@OutputTableName@@@
										(
											[run_id], 
											[run_datetime], 
											[server_name], 
											[database_name], 
											[schema_name], 
											[table_name], 
											[index_name],
                                            [Drop_Tsql],
                                            [Create_Tsql], 
											[index_id], 
											[db_schema_object_indexid], 
											[object_type], 
											[index_definition], 
											[key_column_names_with_sort_order], 
											[count_key_columns], 
											[include_column_names], 
											[count_included_columns], 
											[secret_columns], 
											[count_secret_columns], 
											[partition_key_column_name], 
											[filter_definition], 
											[is_indexed_view], 
											[is_primary_key],
											[is_unique_constraint],
											[is_XML], 
											[is_spatial], 
											[is_NC_columnstore], 
											[is_CX_columnstore], 
                                            [is_in_memory_oltp],
											[is_disabled], 
											[is_hypothetical], 
											[is_padded], 
											[fill_factor], 
											[is_referenced_by_foreign_key], 
											[last_user_seek], 
											[last_user_scan], 
											[last_user_lookup], 
											[last_user_update], 
											[total_reads], 
											[user_updates], 
											[reads_per_write], 
											[index_usage_summary], 
											[total_singleton_lookup_count],
											[total_range_scan_count],
											[total_leaf_delete_count],
											[total_leaf_update_count],
											[index_op_stats],
											[partition_count], 
											[total_rows], 
											[total_reserved_MB], 
											[total_reserved_LOB_MB], 
											[total_reserved_row_overflow_MB], 
											[index_size_summary], 
											[total_row_lock_count], 
											[total_row_lock_wait_count], 
											[total_row_lock_wait_in_ms], 
											[avg_row_lock_wait_in_ms], 
											[total_page_lock_count], 
											[total_page_lock_wait_count], 
											[total_page_lock_wait_in_ms], 
											[avg_page_lock_wait_in_ms], 
											[total_index_lock_promotion_attempt_count], 
											[total_index_lock_promotion_count], 
											[total_forwarded_fetch_count],
											[data_compression_desc], 
						                    [page_latch_wait_count],
								            [page_latch_wait_in_ms],
								            [page_io_latch_wait_count],								
								            [page_io_latch_wait_in_ms],
											[create_date], 
											[modify_date], 
											[more_info],
											[display_order]
										)
									SELECT ''@@@RunID@@@'',
										''@@@GETDATE@@@'',
										''@@@LocalServerName@@@'',
										-- Below should be a copy/paste of the real query
										-- Make sure all quotes are escaped
										i.[database_name] AS [Database Name], 
										i.[schema_name] AS [Schema Name], 
										i.[object_name] AS [Object Name], 
										ISNULL(i.index_name, '''') AS [Index Name],
                                        CASE 
						                    WHEN i.is_primary_key = 1 AND i.index_definition <> ''[HEAP]''
							                    THEN N''--ALTER TABLE '' + QUOTENAME(i.[database_name]) + N''.'' + QUOTENAME(i.[schema_name]) + N''.'' + QUOTENAME(i.[object_name]) +
							                         N'' DROP CONSTRAINT '' + QUOTENAME(i.index_name) + N'';''
						                    WHEN i.is_primary_key = 0 AND i.is_unique_constraint = 1 AND i.index_definition <> ''[HEAP]''
							                    THEN N''--ALTER TABLE '' + QUOTENAME(i.[database_name]) + N''.'' + QUOTENAME(i.[schema_name]) + N''.'' + QUOTENAME(i.[object_name]) +
							                         N'' DROP CONSTRAINT '' + QUOTENAME(i.index_name) + N'';''
						                    WHEN i.is_primary_key = 0 AND i.index_definition <> ''[HEAP]''
						                        THEN N''--DROP INDEX ''+ QUOTENAME(i.index_name) + N'' ON '' + QUOTENAME(i.[database_name]) + N''.'' +
							                         QUOTENAME(i.[schema_name]) + N''.'' + QUOTENAME(i.[object_name]) + N'';''
						                ELSE N''''
						                END AS [Drop TSQL],
					                    CASE 
						                    WHEN i.index_definition = ''[HEAP]'' THEN N''''
					                            ELSE N''--'' + ict.create_tsql END AS [Create TSQL],
										CAST(i.index_id AS NVARCHAR(10))AS [Index ID],
										db_schema_object_indexid AS [Details: schema.table.index(indexid)], 
										CASE    WHEN index_id IN ( 1, 0 ) THEN ''TABLE''
											ELSE ''NonClustered''
											END AS [Object Type], 
										LEFT(index_definition,4000) AS [Definition: [Property]] ColumnName {datatype maxbytes}],
										ISNULL(LTRIM(key_column_names_with_sort_order), '''') AS [Key Column Names With Sort],
										ISNULL(count_key_columns, 0) AS [Count Key Columns],
										ISNULL(include_column_names, '''') AS [Include Column Names], 
										ISNULL(count_included_columns,0) AS [Count Included Columns],
										ISNULL(secret_columns,'''') AS [Secret Column Names], 
										ISNULL(count_secret_columns,0) AS [Count Secret Columns],
										ISNULL(partition_key_column_name, '''') AS [Partition Key Column Name],
										ISNULL(filter_definition, '''') AS [Filter Definition], 
										is_indexed_view AS [Is Indexed View], 
										is_primary_key AS [Is Primary Key],
										is_unique_constraint AS [Is Unique Constraint],
										is_XML AS [Is XML],
										is_spatial AS [Is Spatial],
										is_NC_columnstore AS [Is NC Columnstore],
										is_CX_columnstore AS [Is CX Columnstore],
										is_in_memory_oltp AS [Is In-Memory OLTP],
										is_disabled AS [Is Disabled], 
										is_hypothetical AS [Is Hypothetical],
										is_padded AS [Is Padded], 
										fill_factor AS [Fill Factor], 
										is_referenced_by_foreign_key AS [Is Reference by Foreign Key], 
										last_user_seek AS [Last User Seek], 
										last_user_scan AS [Last User Scan], 
										last_user_lookup AS [Last User Lookup],
										last_user_update AS [Last User Update], 
										total_reads AS [Total Reads], 
										user_updates AS [User Updates], 
										reads_per_write AS [Reads Per Write], 
										index_usage_summary AS [Index Usage], 
										sz.total_singleton_lookup_count AS [Singleton Lookups],
										sz.total_range_scan_count AS [Range Scans],
										sz.total_leaf_delete_count AS [Leaf Deletes],
										sz.total_leaf_update_count AS [Leaf Updates],
										sz.index_op_stats AS [Index Op Stats],
										sz.partition_count AS [Partition Count],
										sz.total_rows AS [Rows], 
										sz.total_reserved_MB AS [Reserved MB], 
										sz.total_reserved_LOB_MB AS [Reserved LOB MB], 
										sz.total_reserved_row_overflow_MB AS [Reserved Row Overflow MB],
										sz.index_size_summary AS [Index Size], 
										sz.total_row_lock_count AS [Row Lock Count],
										sz.total_row_lock_wait_count AS [Row Lock Wait Count],
										sz.total_row_lock_wait_in_ms AS [Row Lock Wait ms],
										sz.avg_row_lock_wait_in_ms AS [Avg Row Lock Wait ms],
										sz.total_page_lock_count AS [Page Lock Count],
										sz.total_page_lock_wait_count AS [Page Lock Wait Count],
										sz.total_page_lock_wait_in_ms AS [Page Lock Wait ms],
										sz.avg_page_lock_wait_in_ms AS [Avg Page Lock Wait ms],
										sz.total_index_lock_promotion_attempt_count AS [Lock Escalation Attempts],
										sz.total_index_lock_promotion_count AS [Lock Escalations],
										sz.total_forwarded_fetch_count AS [Forwarded Fetches],
										sz.data_compression_desc AS [Data Compression],
						                sz.page_latch_wait_count,
								        sz.page_latch_wait_in_ms,
								        sz.page_io_latch_wait_count,								
								        sz.page_io_latch_wait_in_ms,
										i.create_date AS [Create Date],
										i.modify_date AS [Modify Date],
										more_info AS [More Info],
										1 AS [Display Order]
									FROM #IndexSanity AS i
									LEFT JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                                    LEFT JOIN #IndexCreateTsql AS ict  ON i.index_sanity_id = ict.index_sanity_id
									ORDER BY [Database Name], [Schema Name], [Object Name], [Index ID]
									OPTION (RECOMPILE);';
	
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputServerName@@@', @OutputServerName);
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputDatabaseName@@@', @OutputDatabaseName);
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputSchemaName@@@', @OutputSchemaName); 
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@OutputTableName@@@', @OutputTableName); 
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@RunID@@@', @RunID);
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@GETDATE@@@', GETDATE());
								SET @StringToExecute = REPLACE(@StringToExecute, '@@@LocalServerName@@@', CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)));
								EXEC(@StringToExecute);
							END; /* @TableExists = 1 */
						ELSE
							RAISERROR('Creation of the output table failed.', 16, 0);
					END; /* @TableExists = 0 */
				ELSE
					RAISERROR (N'Invalid schema name, data could not be saved.', 16, 0);
			END; /* @ValidOutputLocation = 1 */
		ELSE
	
		IF(@OutputType <> 'NONE')
		BEGIN
			SELECT  i.[database_name] AS [Database Name], 
					i.[schema_name] AS [Schema Name], 
					i.[object_name] AS [Object Name], 
					ISNULL(i.index_name, '') AS [Index Name],
					CAST(i.index_id AS NVARCHAR(10))AS [Index ID],
					db_schema_object_indexid AS [Details: schema.table.index(indexid)], 
					CASE    WHEN index_id IN ( 1, 0 ) THEN 'TABLE'
						ELSE 'NonClustered'
						END AS [Object Type], 
					index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}],
					ISNULL(LTRIM(key_column_names_with_sort_order), '') AS [Key Column Names With Sort],
					ISNULL(count_key_columns, 0) AS [Count Key Columns],
					ISNULL(include_column_names, '') AS [Include Column Names], 
					ISNULL(count_included_columns,0) AS [Count Included Columns],
					ISNULL(secret_columns,'') AS [Secret Column Names], 
					ISNULL(count_secret_columns,0) AS [Count Secret Columns],
					ISNULL(partition_key_column_name, '') AS [Partition Key Column Name],
					ISNULL(filter_definition, '') AS [Filter Definition], 
					is_indexed_view AS [Is Indexed View], 
					is_primary_key AS [Is Primary Key],
					is_unique_constraint AS [Is Unique Constraint] ,
					is_XML AS [Is XML],
					is_spatial AS [Is Spatial],
					is_NC_columnstore AS [Is NC Columnstore],
					is_CX_columnstore AS [Is CX Columnstore],
					is_in_memory_oltp AS [Is In-Memory OLTP],
					is_disabled AS [Is Disabled], 
					is_hypothetical AS [Is Hypothetical],
					is_padded AS [Is Padded], 
					fill_factor AS [Fill Factor], 
					is_referenced_by_foreign_key AS [Is Reference by Foreign Key], 
					last_user_seek AS [Last User Seek], 
					last_user_scan AS [Last User Scan], 
					last_user_lookup AS [Last User Lookup],
					last_user_update AS [Last User Update], 
					total_reads AS [Total Reads], 
					user_updates AS [User Updates], 
					reads_per_write AS [Reads Per Write], 
					index_usage_summary AS [Index Usage], 
					sz.total_singleton_lookup_count AS [Singleton Lookups],
					sz.total_range_scan_count AS [Range Scans],
					sz.total_leaf_delete_count AS [Leaf Deletes],
					sz.total_leaf_update_count AS [Leaf Updates],
					sz.index_op_stats AS [Index Op Stats],
					sz.partition_count AS [Partition Count],
					sz.total_rows AS [Rows], 
					sz.total_reserved_MB AS [Reserved MB], 
					sz.total_reserved_LOB_MB AS [Reserved LOB MB], 
					sz.total_reserved_row_overflow_MB AS [Reserved Row Overflow MB],
					sz.index_size_summary AS [Index Size], 
					sz.total_row_lock_count AS [Row Lock Count],
					sz.total_row_lock_wait_count AS [Row Lock Wait Count],
					sz.total_row_lock_wait_in_ms AS [Row Lock Wait ms],
					sz.avg_row_lock_wait_in_ms AS [Avg Row Lock Wait ms],
					sz.total_page_lock_count AS [Page Lock Count],
					sz.total_page_lock_wait_count AS [Page Lock Wait Count],
					sz.total_page_lock_wait_in_ms AS [Page Lock Wait ms],
					sz.avg_page_lock_wait_in_ms AS [Avg Page Lock Wait ms],
					sz.total_index_lock_promotion_attempt_count AS [Lock Escalation Attempts],
					sz.total_index_lock_promotion_count AS [Lock Escalations],
					sz.page_latch_wait_count AS [Page Latch Wait Count],
					sz.page_latch_wait_in_ms AS [Page Latch Wait ms],
					sz.page_io_latch_wait_count AS [Page IO Latch Wait Count],								
					sz.page_io_latch_wait_in_ms as [Page IO Latch Wait ms],
                    sz.total_forwarded_fetch_count AS [Forwarded Fetches],
					sz.data_compression_desc AS [Data Compression],
					i.create_date AS [Create Date],
					i.modify_date AS [Modify Date],
					more_info AS [More Info],
                    CASE 
						 WHEN i.is_primary_key = 1 AND i.index_definition <> '[HEAP]'
							THEN N'--ALTER TABLE ' + QUOTENAME(i.[database_name]) + N'.' + QUOTENAME(i.[schema_name]) + N'.' + QUOTENAME(i.[object_name])
							     + N' DROP CONSTRAINT ' + QUOTENAME(i.index_name) + N';'
						 WHEN i.is_primary_key = 0 AND i.is_unique_constraint = 1 AND i.index_definition <> '[HEAP]'
							THEN N'--ALTER TABLE ' + QUOTENAME(i.[database_name]) + N'.' + QUOTENAME(i.[schema_name]) + N'.' + QUOTENAME(i.[object_name])
							     + N' DROP CONSTRAINT ' + QUOTENAME(i.index_name) + N';'
						 WHEN i.is_primary_key = 0 AND i.index_definition <> '[HEAP]'
						     THEN N'--DROP INDEX '+ QUOTENAME(i.index_name) + N' ON ' + QUOTENAME(i.[database_name]) + N'.' + 
							     QUOTENAME(i.[schema_name]) + N'.' + QUOTENAME(i.[object_name]) + N';'
						 ELSE N''
						 END AS [Drop TSQL],
					CASE 
						WHEN i.index_definition = '[HEAP]' THEN N''
					    ELSE N'--' + ict.create_tsql END AS [Create TSQL], 
					1 AS [Display Order]
			FROM    #IndexSanity AS i --left join here so we don't lose disabled nc indexes
					LEFT JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                    LEFT JOIN #IndexCreateTsql AS ict ON i.index_sanity_id = ict.index_sanity_id
			ORDER BY    /* Shout out to DHutmacher */
						/*DESC*/
						CASE WHEN @SortOrder = N'rows' AND @SortDirection = N'desc' THEN sz.total_rows ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'reserved_mb' AND @SortDirection = N'desc' THEN sz.total_reserved_MB ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'size' AND @SortDirection = N'desc' THEN sz.total_reserved_MB ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'reserved_lob_mb' AND @SortDirection = N'desc' THEN sz.total_reserved_LOB_MB ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'lob' AND @SortDirection = N'desc' THEN sz.total_reserved_LOB_MB ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'total_row_lock_wait_in_ms' AND @SortDirection = N'desc' THEN COALESCE(sz.total_row_lock_wait_in_ms,0) ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'total_page_lock_wait_in_ms' AND @SortDirection = N'desc' THEN COALESCE(sz.total_page_lock_wait_in_ms,0) ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'lock_time' AND @SortDirection = N'desc' THEN (COALESCE(sz.total_row_lock_wait_in_ms,0) + COALESCE(sz.total_page_lock_wait_in_ms,0)) ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'total_reads' AND @SortDirection = N'desc' THEN total_reads ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'reads' AND @SortDirection = N'desc' THEN total_reads ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'user_updates' AND @SortDirection = N'desc' THEN user_updates ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'writes' AND @SortDirection = N'desc' THEN user_updates ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'reads_per_write' AND @SortDirection = N'desc' THEN reads_per_write ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'ratio' AND @SortDirection = N'desc' THEN reads_per_write ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'forward_fetches' AND @SortDirection = N'desc' THEN sz.total_forwarded_fetch_count ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'fetches' AND @SortDirection = N'desc' THEN sz.total_forwarded_fetch_count ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'create_date' AND @SortDirection = N'desc' THEN CONVERT(DATETIME, i.create_date) ELSE NULL END DESC,
						CASE WHEN @SortOrder = N'modify_date' AND @SortDirection = N'desc' THEN CONVERT(DATETIME, i.modify_date) ELSE NULL END DESC,
						/*ASC*/
						CASE WHEN @SortOrder = N'rows' AND @SortDirection = N'asc' THEN sz.total_rows ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'reserved_mb' AND @SortDirection = N'asc' THEN sz.total_reserved_MB ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'size' AND @SortDirection = N'asc' THEN sz.total_reserved_MB ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'reserved_lob_mb' AND @SortDirection = N'asc' THEN sz.total_reserved_LOB_MB ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'lob' AND @SortDirection = N'asc' THEN sz.total_reserved_LOB_MB ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'total_row_lock_wait_in_ms' AND @SortDirection = N'asc' THEN COALESCE(sz.total_row_lock_wait_in_ms,0) ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'total_page_lock_wait_in_ms' AND @SortDirection = N'asc' THEN COALESCE(sz.total_page_lock_wait_in_ms,0) ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'lock_time' AND @SortDirection = N'asc' THEN (COALESCE(sz.total_row_lock_wait_in_ms,0) + COALESCE(sz.total_page_lock_wait_in_ms,0)) ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'total_reads' AND @SortDirection = N'asc' THEN total_reads ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'reads' AND @SortDirection = N'asc' THEN total_reads ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'user_updates' AND @SortDirection = N'asc' THEN user_updates ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'writes' AND @SortDirection = N'asc' THEN user_updates ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'reads_per_write' AND @SortDirection = N'asc' THEN reads_per_write ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'ratio' AND @SortDirection = N'asc' THEN reads_per_write ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'forward_fetches' AND @SortDirection = N'asc' THEN sz.total_forwarded_fetch_count ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'fetches' AND @SortDirection = N'asc' THEN sz.total_forwarded_fetch_count ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'create_date' AND @SortDirection = N'asc' THEN CONVERT(DATETIME, i.create_date) ELSE NULL END ASC,
						CASE WHEN @SortOrder = N'modify_date' AND @SortDirection = N'asc' THEN CONVERT(DATETIME, i.modify_date) ELSE NULL END ASC,
				i.[database_name], [Schema Name], [Object Name], [Index ID]
			OPTION (RECOMPILE);
  		END;

    END; /* End @Mode=2 (index detail)*/
    ELSE IF (@Mode=3) /*Missing index Detail*/
    BEGIN
		IF(@OutputType <> 'NONE')
		BEGIN;
			WITH create_date AS (
						SELECT i.database_id,
							   i.schema_name,
							   i.[object_id], 
							   ISNULL(NULLIF(MAX(DATEDIFF(DAY, i.create_date, SYSDATETIME())), 0), 1) AS create_days
						FROM #IndexSanity AS i
						GROUP BY i.database_id, i.schema_name, i.object_id
						)
			SELECT 
				mi.database_name AS [Database Name], 
				mi.[schema_name] AS [Schema], 
				mi.table_name AS [Table], 
				CAST((mi.magic_benefit_number / CASE WHEN cd.create_days < @DaysUptime THEN cd.create_days ELSE @DaysUptime END) AS BIGINT)
					AS [Magic Benefit Number], 
				mi.missing_index_details AS [Missing Index Details], 
				mi.avg_total_user_cost AS [Avg Query Cost], 
				mi.avg_user_impact AS [Est Index Improvement], 
				mi.user_seeks AS [Seeks], 
				mi.user_scans AS [Scans],
				mi.unique_compiles AS [Compiles],
				mi.equality_columns_with_data_type AS [Equality Columns],
				mi.inequality_columns_with_data_type AS [Inequality Columns],
				mi.included_columns_with_data_type AS [Included Columns], 
				mi.index_estimated_impact AS [Estimated Impact], 
				mi.create_tsql AS [Create TSQL], 
				mi.more_info AS [More Info],
				1 AS [Display Order],
				mi.is_low,
				mi.sample_query_plan AS [Sample Query Plan]
			FROM #MissingIndexes AS mi
			LEFT JOIN create_date AS cd
			ON mi.[object_id] =  cd.object_id 
			AND mi.database_id = cd.database_id
			AND mi.schema_name = cd.schema_name
			/* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
			WHERE @ShowAllMissingIndexRequests=1 
            OR (mi.magic_benefit_number / CASE WHEN cd.create_days < @DaysUptime THEN cd.create_days ELSE @DaysUptime END) >= 100000
			UNION ALL
			SELECT               
				@ScriptVersionName,   
				N'From Your Community Volunteers' ,   
				N'http://FirstResponderKit.org' ,
				100000000000,
				@DaysUptimeInsertValue,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL, NULL, NULL, NULL, 0 AS [Display Order], NULL AS is_low, NULL
			ORDER BY [Display Order] ASC, [Magic Benefit Number] DESC
			OPTION (RECOMPILE);
	  	END;

	IF  (@BringThePain = 1
	AND @DatabaseName IS NOT NULL
	AND @GetAllDatabases = 0)

	BEGIN

		EXEC sp_BlitzCache @SortOrder = 'sp_BlitzIndex', @DatabaseName = @DatabaseName, @BringThePain = 1, @QueryFilter = 'statement', @HideSummary = 1;
	                              
	END;


    END; /* End @Mode=3 (index detail)*/

END TRY

BEGIN CATCH
        RAISERROR (N'Failure analyzing temp tables.', 0,1) WITH NOWAIT;

        SELECT  @msg = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();

        RAISERROR (@msg, 
               @ErrorSeverity, 
               @ErrorState 
               );
        
        WHILE @@trancount > 0 
            ROLLBACK;

        RETURN;
    END CATCH;
GO
