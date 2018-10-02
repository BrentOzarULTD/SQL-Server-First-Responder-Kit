SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF (
SELECT
  CASE 
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '8%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '9%' THEN 0
	 ELSE 1
  END 
) = 0
BEGIN
	DECLARE @msg VARCHAR(8000); 
	SELECT @msg = 'Sorry, sp_BlitzCache doesn''t work on versions of SQL prior to 2008.' + REPLICATE(CHAR(13), 7933);
	PRINT @msg;
	RETURN;
END;

IF OBJECT_ID('dbo.sp_BlitzCache') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzCache AS RETURN 0;');
GO

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##bou_BlitzCacheProcs', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##bou_BlitzCacheProcs;');
GO

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##bou_BlitzCacheResults', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##bou_BlitzCacheResults;');
GO

CREATE TABLE ##bou_BlitzCacheResults (
    SPID INT,
    ID INT IDENTITY(1,1),
    CheckID INT,
    Priority TINYINT,
    FindingsGroup VARCHAR(50),
    Finding VARCHAR(200),
    URL VARCHAR(200),
    Details VARCHAR(4000) 
);

CREATE TABLE ##bou_BlitzCacheProcs (
        SPID INT ,
        QueryType NVARCHAR(258),
        DatabaseName sysname,
        AverageCPU DECIMAL(38,4),
        AverageCPUPerMinute DECIMAL(38,4),
        TotalCPU DECIMAL(38,4),
        PercentCPUByType MONEY,
        PercentCPU MONEY,
        AverageDuration DECIMAL(38,4),
        TotalDuration DECIMAL(38,4),
        PercentDuration MONEY,
        PercentDurationByType MONEY,
        AverageReads BIGINT,
        TotalReads BIGINT,
        PercentReads MONEY,
        PercentReadsByType MONEY,
        ExecutionCount BIGINT,
        PercentExecutions MONEY,
        PercentExecutionsByType MONEY,
        ExecutionsPerMinute MONEY,
        TotalWrites BIGINT,
        AverageWrites MONEY,
        PercentWrites MONEY,
        PercentWritesByType MONEY,
        WritesPerMinute MONEY,
        PlanCreationTime DATETIME,
		PlanCreationTimeHours AS DATEDIFF(HOUR, PlanCreationTime, SYSDATETIME()),
        LastExecutionTime DATETIME,
        PlanHandle VARBINARY(64),
		[Remove Plan Handle From Cache] AS 
			CASE WHEN [PlanHandle] IS NOT NULL 
			THEN 'DBCC FREEPROCCACHE (' + CONVERT(VARCHAR(128), [PlanHandle], 1) + ');'
			ELSE 'N/A' END,
		SqlHandle VARBINARY(64),
			[Remove SQL Handle From Cache] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN 'DBCC FREEPROCCACHE (' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ');'
			ELSE 'N/A' END,
		[SQL Handle More Info] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN 'EXEC sp_BlitzCache @OnlySqlHandles = ''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '''; '
			ELSE 'N/A' END,
		QueryHash BINARY(8),
		[Query Hash More Info] AS 
			CASE WHEN [QueryHash] IS NOT NULL 
			THEN 'EXEC sp_BlitzCache @OnlyQueryHashes = ''' + CONVERT(VARCHAR(32), [QueryHash], 1) + '''; '
			ELSE 'N/A' END,
        QueryPlanHash BINARY(8),
        StatementStartOffset INT,
        StatementEndOffset INT,
        MinReturnedRows BIGINT,
        MaxReturnedRows BIGINT,
        AverageReturnedRows MONEY,
        TotalReturnedRows BIGINT,
        LastReturnedRows BIGINT,
		/*The Memory Grant columns are only supported 
		  in certain versions, giggle giggle.
		*/
		MinGrantKB BIGINT,
		MaxGrantKB BIGINT,
		MinUsedGrantKB BIGINT, 
		MaxUsedGrantKB BIGINT,
		PercentMemoryGrantUsed MONEY,
		AvgMaxMemoryGrant MONEY,
		MinSpills BIGINT,
		MaxSpills BIGINT,
		TotalSpills BIGINT,
		AvgSpills MONEY,
        QueryText NVARCHAR(MAX),
        QueryPlan XML,
        /* these next four columns are the total for the type of query.
            don't actually use them for anything apart from math by type.
            */
        TotalWorkerTimeForType BIGINT,
        TotalElapsedTimeForType BIGINT,
        TotalReadsForType BIGINT,
        TotalExecutionCountForType BIGINT,
        TotalWritesForType BIGINT,
        NumberOfPlans INT,
        NumberOfDistinctPlans INT,
        SerialDesiredMemory FLOAT,
        SerialRequiredMemory FLOAT,
        CachedPlanSize FLOAT,
        CompileTime FLOAT,
        CompileCPU FLOAT ,
        CompileMemory FLOAT ,
		MaxCompileMemory FLOAT ,
        min_worker_time BIGINT,
        max_worker_time BIGINT,
        is_forced_plan BIT,
        is_forced_parameterized BIT,
        is_cursor BIT,
		is_optimistic_cursor BIT,
		is_forward_only_cursor BIT,
		is_fast_forward_cursor BIT,
		is_cursor_dynamic BIT,
        is_parallel BIT,
		is_forced_serial BIT,
		is_key_lookup_expensive BIT,
		key_lookup_cost FLOAT,
		is_remote_query_expensive BIT,
		remote_query_cost FLOAT,
        frequent_execution BIT,
        parameter_sniffing BIT,
        unparameterized_query BIT,
        near_parallel BIT,
        plan_warnings BIT,
        plan_multiple_plans BIT,
        long_running BIT,
        downlevel_estimator BIT,
        implicit_conversions BIT,
        busy_loops BIT,
        tvf_join BIT,
        tvf_estimate BIT,
        compile_timeout BIT,
        compile_memory_limit_exceeded BIT,
        warning_no_join_predicate BIT,
        QueryPlanCost FLOAT,
        missing_index_count INT,
        unmatched_index_count INT,
        min_elapsed_time BIGINT,
        max_elapsed_time BIGINT,
        age_minutes MONEY,
        age_minutes_lifetime MONEY,
        is_trivial BIT,
		trace_flags_session VARCHAR(1000),
		is_unused_grant BIT,
		function_count INT,
		clr_function_count INT,
		is_table_variable BIT,
		no_stats_warning BIT,
		relop_warnings BIT,
		is_table_scan BIT,
	    backwards_scan BIT,
	    forced_index BIT,
	    forced_seek BIT,
	    forced_scan BIT,
		columnstore_row_mode BIT,
		is_computed_scalar BIT ,
		is_sort_expensive BIT,
		sort_cost FLOAT,
		is_computed_filter BIT,
		op_name VARCHAR(100) NULL,
		index_insert_count INT NULL,
		index_update_count INT NULL,
		index_delete_count INT NULL,
		cx_insert_count INT NULL,
		cx_update_count INT NULL,
		cx_delete_count INT NULL,
		table_insert_count INT NULL,
		table_update_count INT NULL,
		table_delete_count INT NULL,
		index_ops AS (index_insert_count + index_update_count + index_delete_count + 
					  cx_insert_count + cx_update_count + cx_delete_count +
					  table_insert_count + table_update_count + table_delete_count),
		is_row_level BIT,
		is_spatial BIT,
		index_dml BIT,
		table_dml BIT,
		long_running_low_cpu BIT,
		low_cost_high_cpu BIT,
		stale_stats BIT, 
		is_adaptive BIT,
		index_spool_cost FLOAT,
		index_spool_rows FLOAT,
		is_spool_expensive BIT,
		is_spool_more_rows BIT,
		estimated_rows FLOAT,
		is_bad_estimate BIT, 
		is_paul_white_electric BIT,
		is_row_goal BIT,
		is_big_spills BIT,
		is_mstvf BIT,
		is_mm_join BIT,
        is_nonsargable BIT,
		implicit_conversion_info XML,
		cached_execution_parameters XML,
		missing_indexes XML,
        SetOptions VARCHAR(MAX),
        Warnings VARCHAR(MAX)
    );
GO 

ALTER PROCEDURE dbo.sp_BlitzCache
    @Help BIT = 0,
    @Top INT = NULL,
    @SortOrder VARCHAR(50) = 'CPU',
    @UseTriggersAnyway BIT = NULL,
    @ExportToExcel BIT = 0,
    @ExpertMode TINYINT = 0,
    @OutputServerName NVARCHAR(258) = NULL ,
    @OutputDatabaseName NVARCHAR(258) = NULL ,
    @OutputSchemaName NVARCHAR(258) = NULL ,
    @OutputTableName NVARCHAR(258) = NULL ,
    @ConfigurationDatabaseName NVARCHAR(128) = NULL ,
    @ConfigurationSchemaName NVARCHAR(258) = NULL ,
    @ConfigurationTableName NVARCHAR(258) = NULL ,
    @DurationFilter DECIMAL(38,4) = NULL ,
    @HideSummary BIT = 0 ,
    @IgnoreSystemDBs BIT = 1 ,
    @OnlyQueryHashes VARCHAR(MAX) = NULL ,
    @IgnoreQueryHashes VARCHAR(MAX) = NULL ,
    @OnlySqlHandles VARCHAR(MAX) = NULL ,
	@IgnoreSqlHandles VARCHAR(MAX) = NULL ,
    @QueryFilter VARCHAR(10) = 'ALL' ,
    @DatabaseName NVARCHAR(128) = NULL ,
    @StoredProcName NVARCHAR(128) = NULL,
    @Reanalyze BIT = 0 ,
    @SkipAnalysis BIT = 0 ,
    @BringThePain BIT = 0, /* This will forcibly set @Top to 2,147,483,647 */
    @MinimumExecutionCount INT = 0,
	@Debug BIT = 0,
	@CheckDateOverride DATETIMEOFFSET = NULL,
	@MinutesBack INT = NULL,
	@VersionDate DATETIME = NULL OUTPUT
WITH RECOMPILE
AS
BEGIN
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Version VARCHAR(30);
SET @Version = '6.10';
SET @VersionDate = '20181001';

IF @Help = 1 PRINT '
sp_BlitzCache from http://FirstResponderKit.org
	
This script displays your most resource-intensive queries from the plan cache,
and points to ways you can tune these queries to make them faster.


To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - This query will not run on SQL Server 2005.
 - SQL Server 2008 and 2008R2 have a bug in trigger stats, so that output is
   excluded by default.
 - @IgnoreQueryHashes and @OnlyQueryHashes require a CSV list of hashes
   with no spaces between the hash values.
 - @OutputServerName is not functional yet.

Unknown limitations of this version:
 - May or may not be vulnerable to the wick effect.

Changes - for the full list of improvements and fixes in this version, see:
https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/



MIT License

Copyright (c) 2018 Brent Ozar Unlimited

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

DECLARE @nl NVARCHAR(2) = NCHAR(13) + NCHAR(10) ;

IF @Help = 1
BEGIN
    SELECT N'@Help' AS [Parameter Name] ,
           N'BIT' AS [Data Type] ,
           N'Displays this help message.' AS [Parameter Description]

    UNION ALL
    SELECT N'@Top',
           N'INT',
           N'The number of records to retrieve and analyze from the plan cache. The following DMVs are used as the plan cache: dm_exec_query_stats, dm_exec_procedure_stats, dm_exec_trigger_stats.'

    UNION ALL
    SELECT N'@SortOrder',
           N'VARCHAR(10)',
           N'Data processing and display order. @SortOrder will still be used, even when preparing output for a table or for excel. Possible values are: "CPU", "Reads", "Writes", "Duration", "Executions", "Recent Compilations", "Memory Grant", "Spills". Additionally, the word "Average" or "Avg" can be used to sort on averages rather than total. "Executions per minute" and "Executions / minute" can be used to sort by execution per minute. For the truly lazy, "xpm" can also be used. Note that when you use all or all avg, the only parameters you can use are @Top and @DatabaseName. All others will be ignored.'

    UNION ALL
    SELECT N'@UseTriggersAnyway',
           N'BIT',
           N'On SQL Server 2008R2 and earlier, trigger execution count is incorrect - trigger execution count is incremented once per execution of a SQL agent job. If you still want to see relative execution count of triggers, then you can force sp_BlitzCache to include this information.'

    UNION ALL
    SELECT N'@ExportToExcel',
           N'BIT',
           N'Prepare output for exporting to Excel. Newlines and additional whitespace are removed from query text and the execution plan is not displayed.'

    UNION ALL
    SELECT N'@ExpertMode',
           N'TINYINT',
           N'Default 0. When set to 1, results include more columns. When 2, mode is optimized for Opserver, the open source dashboard.'

    UNION ALL
    SELECT N'@OutputDatabaseName',
           N'NVARCHAR(128)',
           N'The output database. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@OutputSchemaName',
           N'NVARCHAR(258)',
           N'The output schema. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@OutputTableName',
           N'NVARCHAR(258)',
           N'The output table. If this does not exist, it will be created for you.'

    UNION ALL
    SELECT N'@DurationFilter',
           N'DECIMAL(38,4)',
           N'Excludes queries with an average duration (in seconds) less than @DurationFilter.'

    UNION ALL
    SELECT N'@HideSummary',
           N'BIT',
           N'Hides the findings summary result set.'

    UNION ALL
    SELECT N'@IgnoreSystemDBs',
           N'BIT',
           N'Ignores plans found in the system databases (master, model, msdb, tempdb, and resourcedb)'

    UNION ALL
    SELECT N'@OnlyQueryHashes',
           N'VARCHAR(MAX)',
           N'A list of query hashes to query. All other query hashes will be ignored. Stored procedures and triggers will be ignored.'

    UNION ALL
    SELECT N'@IgnoreQueryHashes',
           N'VARCHAR(MAX)',
           N'A list of query hashes to ignore.'
    
    UNION ALL
    SELECT N'@OnlySqlHandles',
           N'VARCHAR(MAX)',
           N'One or more sql_handles to use for filtering results.'
    
	UNION ALL
    SELECT N'@IgnoreSqlHandles',
           N'VARCHAR(MAX)',
           N'One or more sql_handles to ignore.'

    UNION ALL
    SELECT N'@DatabaseName',
           N'NVARCHAR(128)',
           N'A database name which is used for filtering results.'

    UNION ALL
    SELECT N'@StoredProcName',
           N'NVARCHAR(128)',
           N'Name of stored procedure you want to find plans for.'

    UNION ALL
    SELECT N'@BringThePain',
           N'BIT',
           N'This forces sp_BlitzCache to examine the entire plan cache. Be careful running this on servers with a lot of memory or a large execution plan cache.'

    UNION ALL
    SELECT N'@QueryFilter',
           N'VARCHAR(10)',
           N'Filter out stored procedures or statements. The default value is ''ALL''. Allowed values are ''procedures'', ''statements'', ''functions'', or ''all'' (any variation in capitalization is acceptable).'

    UNION ALL
    SELECT N'@Reanalyze',
           N'BIT',
           N'The default is 0. When set to 0, sp_BlitzCache will re-evalute the plan cache. Set this to 1 to reanalyze existing results'
           
    UNION ALL
    SELECT N'@MinimumExecutionCount',
           N'INT',
           N'Queries with fewer than this number of executions will be omitted from results.'
    
	UNION ALL
    SELECT N'@Debug',
           N'BIT',
           N'Setting this to 1 will print dynamic SQL and select data from all tables used.'

    UNION ALL
    SELECT N'@MinutesBack',
           N'INT',
           N'How many minutes back to begin plan cache analysis. If you put in a positive number, we''ll flip it to negtive.';


    /* Column definitions */
    SELECT N'# Executions' AS [Column Name],
           N'BIGINT' AS [Data Type],
           N'The number of executions of this particular query. This is computed across statements, procedures, and triggers and aggregated by the SQL handle.' AS [Column Description]

    UNION ALL
    SELECT N'Executions / Minute',
           N'MONEY',
           N'Number of executions per minute - calculated for the life of the current plan. Plan life is the last execution time minus the plan creation time.'

    UNION ALL
    SELECT N'Execution Weight',
           N'MONEY',
           N'An arbitrary metric of total "execution-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Database',
           N'sysname',
           N'The name of the database where the plan was encountered. If the database name cannot be determined for some reason, a value of NA will be substituted. A value of 32767 indicates the plan comes from ResourceDB.'

    UNION ALL
    SELECT N'Total CPU',
           N'BIGINT',
           N'Total CPU time, reported in milliseconds, that was consumed by all executions of this query since the last compilation.'

    UNION ALL
    SELECT N'Avg CPU',
           N'BIGINT',
           N'Average CPU time, reported in milliseconds, consumed by each execution of this query since the last compilation.'

    UNION ALL
    SELECT N'CPU Weight',
           N'MONEY',
           N'An arbitrary metric of total "CPU-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Total Duration',
           N'BIGINT',
           N'Total elapsed time, reported in milliseconds, consumed by all executions of this query since last compilation.'

    UNION ALL
    SELECT N'Avg Duration',
           N'BIGINT',
           N'Average elapsed time, reported in milliseconds, consumed by each execution of this query since the last compilation.'

    UNION ALL
    SELECT N'Duration Weight',
           N'MONEY',
           N'An arbitrary metric of total "Duration-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Total Reads',
           N'BIGINT',
           N'Total logical reads performed by this query since last compilation.'

    UNION ALL
    SELECT N'Average Reads',
           N'BIGINT',
           N'Average logical reads performed by each execution of this query since the last compilation.'

    UNION ALL
    SELECT N'Read Weight',
           N'MONEY',
           N'An arbitrary metric of "Read-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Total Writes',
           N'BIGINT',
           N'Total logical writes performed by this query since last compilation.'

    UNION ALL
    SELECT N'Average Writes',
           N'BIGINT',
           N'Average logical writes performed by each execution this query since last compilation.'

    UNION ALL
    SELECT N'Write Weight',
           N'MONEY',
           N'An arbitrary metric of "Write-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Query Type',
           N'NVARCHAR(258)',
           N'The type of query being examined. This can be "Procedure", "Statement", or "Trigger".'

    UNION ALL
    SELECT N'Query Text',
           N'NVARCHAR(4000)',
           N'The text of the query. This may be truncated by either SQL Server or by sp_BlitzCache(tm) for display purposes.'

    UNION ALL
    SELECT N'% Executions (Type)',
           N'MONEY',
           N'Percent of executions relative to the type of query - e.g. 17.2% of all stored procedure executions.'

    UNION ALL
    SELECT N'% CPU (Type)',
           N'MONEY',
           N'Percent of CPU time consumed by this query for a given type of query - e.g. 22% of CPU of all stored procedures executed.'

    UNION ALL
    SELECT N'% Duration (Type)',
           N'MONEY',
           N'Percent of elapsed time consumed by this query for a given type of query - e.g. 12% of all statements executed.'

    UNION ALL
    SELECT N'% Reads (Type)',
           N'MONEY',
           N'Percent of reads consumed by this query for a given type of query - e.g. 34.2% of all stored procedures executed.'

    UNION ALL
    SELECT N'% Writes (Type)',
           N'MONEY',
           N'Percent of writes performed by this query for a given type of query - e.g. 43.2% of all statements executed.'

    UNION ALL
    SELECT N'Total Rows',
           N'BIGINT',
           N'Total number of rows returned for all executions of this query. This only applies to query level stats, not stored procedures or triggers.'

    UNION ALL
    SELECT N'Average Rows',
           N'MONEY',
           N'Average number of rows returned by each execution of the query.'

    UNION ALL
    SELECT N'Min Rows',
           N'BIGINT',
           N'The minimum number of rows returned by any execution of this query.'

    UNION ALL
    SELECT N'Max Rows',
           N'BIGINT',
           N'The maximum number of rows returned by any execution of this query.'

    UNION ALL
    SELECT N'MinGrantKB',
           N'BIGINT',
           N'The minimum memory grant the query received in kb.'

    UNION ALL
    SELECT N'MaxGrantKB',
           N'BIGINT',
           N'The maximum memory grant the query received in kb.'

    UNION ALL
    SELECT N'MinUsedGrantKB',
           N'BIGINT',
           N'The minimum used memory grant the query received in kb.'

    UNION ALL
    SELECT N'MaxUsedGrantKB',
           N'BIGINT',
           N'The maximum used memory grant the query received in kb.';

    SELECT N'MinSpills',
           N'BIGINT',
           N'The minimum amount this query has spilled to tempdb in 8k pages.'

    UNION ALL
    SELECT N'MaxSpills',
           N'BIGINT',
           N'The maximum amount this query has spilled to tempdb in 8k pages.'

    UNION ALL
    SELECT N'TotalSpills',
           N'BIGINT',
           N'The total amount this query has spilled to tempdb in 8k pages.'

    UNION ALL
    SELECT N'AvgSpills',
           N'BIGINT',
           N'The average amount this query has spilled to tempdb in 8k pages.'

    UNION ALL
    SELECT N'PercentMemoryGrantUsed',
           N'MONEY',
           N'Result of dividing the maximum grant used by the minimum granted.'

    UNION ALL
    SELECT N'AvgMaxMemoryGrant',
           N'MONEY',
           N'The average maximum memory grant for a query.'

    UNION ALL
    SELECT N'# Plans',
           N'INT',
           N'The total number of execution plans found that match a given query.'

    UNION ALL
    SELECT N'# Distinct Plans',
           N'INT',
           N'The number of distinct execution plans that match a given query. '
            + NCHAR(13) + NCHAR(10)
            + N'This may be caused by running the same query across multiple databases or because of a lack of proper parameterization in the database.'

    UNION ALL
    SELECT N'Created At',
           N'DATETIME',
           N'Time that the execution plan was last compiled.'

    UNION ALL
    SELECT N'Last Execution',
           N'DATETIME',
           N'The last time that this query was executed.'

    UNION ALL
    SELECT N'Query Plan',
           N'XML',
           N'The query plan. Click to display a graphical plan or, if you need to patch SSMS, a pile of XML.'

    UNION ALL
    SELECT N'Plan Handle',
           N'VARBINARY(64)',
           N'An arbitrary identifier referring to the compiled plan this query is a part of.'

    UNION ALL
    SELECT N'SQL Handle',
           N'VARBINARY(64)',
           N'An arbitrary identifier referring to a batch or stored procedure that this query is a part of.'

    UNION ALL
    SELECT N'Query Hash',
           N'BINARY(8)',
           N'A hash of the query. Queries with the same query hash have similar logic but only differ by literal values or database.'

    UNION ALL
    SELECT N'Warnings',
           N'VARCHAR(MAX)',
           N'A list of individual warnings generated by this query.' ;


           
    /* Configuration table description */
    SELECT N'Frequent Execution Threshold' AS [Configuration Parameter] ,
           N'100' AS [Default Value] ,
           N'Executions / Minute' AS [Unit of Measure] ,
           N'Executions / Minute before a "Frequent Execution Threshold" warning is triggered.' AS [Description]

    UNION ALL
    SELECT N'Parameter Sniffing Variance Percent' ,
           N'30' ,
           N'Percent' ,
           N'Variance required between min/max values and average values before a "Parameter Sniffing" warning is triggered. Applies to worker time and returned rows.'

    UNION ALL
    SELECT N'Parameter Sniffing IO Threshold' ,
           N'100,000' ,
           N'Logical reads' ,
           N'Minimum number of average logical reads before parameter sniffing checks are evaluated.'

    UNION ALL
    SELECT N'Cost Threshold for Parallelism Warning' AS [Configuration Parameter] ,
           N'10' ,
           N'Percent' ,
           N'Trigger a "Nearly Parallel" warning when a query''s cost is within X percent of the cost threshold for parallelism.'

    UNION ALL
    SELECT N'Long Running Query Warning' AS [Configuration Parameter] ,
           N'300' ,
           N'Seconds' ,
           N'Triggers a "Long Running Query Warning" when average duration, max CPU time, or max clock time is higher than this number.'

    UNION ALL
    SELECT N'Unused Memory Grant Warning' AS [Configuration Parameter] ,
           N'10' ,
           N'Percent' ,
           N'Triggers an "Unused Memory Grant Warning" when a query uses >= X percent of its memory grant.';
    RETURN;
END;

/*Validate version*/
IF (
SELECT
  CASE 
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '8%' THEN 0
     WHEN CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')) LIKE '9%' THEN 0
	 ELSE 1
  END 
) = 0
BEGIN
	DECLARE @version_msg VARCHAR(8000); 
	SELECT @version_msg = 'Sorry, sp_BlitzCache doesn''t work on versions of SQL prior to 2008.' + REPLICATE(CHAR(13), 7933);
	PRINT @version_msg;
	RETURN;
END;

/* Set @Top based on sort */
IF (
     @Top IS NULL
     AND LOWER(@SortOrder) IN ( 'all', 'all sort' )
   )
   BEGIN
         SET @Top = 5;
   END;

IF (
     @Top IS NULL
     AND LOWER(@SortOrder) NOT IN ( 'all', 'all sort' )
   )
   BEGIN
         SET @Top = 10;
   END;

/* validate user inputs */
IF @Top IS NULL 
    OR @SortOrder IS NULL 
    OR @QueryFilter IS NULL 
    OR @Reanalyze IS NULL
BEGIN
    RAISERROR(N'Several parameters (@Top, @SortOrder, @QueryFilter, @renalyze) are required. Do not set them to NULL. Please try again.', 16, 1) WITH NOWAIT;
    RETURN;
END;

RAISERROR(N'Checking @MinutesBack validity.', 0, 1) WITH NOWAIT;
IF @MinutesBack IS NOT NULL
    BEGIN
        IF @MinutesBack > 0
            BEGIN
                RAISERROR(N'Setting @MinutesBack to a negative number', 0, 1) WITH NOWAIT;
				SET @MinutesBack *=-1;
            END;
        IF @MinutesBack = 0
            BEGIN
                RAISERROR(N'@MinutesBack can''t be 0, setting to -1', 0, 1) WITH NOWAIT;
				SET @MinutesBack = -1;
            END;
    END;


RAISERROR(N'Creating temp tables for results and warnings.', 0, 1) WITH NOWAIT;

IF OBJECT_ID('tempdb.dbo.##bou_BlitzCacheResults') IS NULL
BEGIN
    CREATE TABLE ##bou_BlitzCacheResults (
        SPID INT,
        ID INT IDENTITY(1,1),
        CheckID INT,
        Priority TINYINT,
        FindingsGroup VARCHAR(50),
        Finding VARCHAR(200),
        URL VARCHAR(200),
        Details VARCHAR(4000)
    );
END;

IF OBJECT_ID('tempdb.dbo.##bou_BlitzCacheProcs') IS NULL
BEGIN
    CREATE TABLE ##bou_BlitzCacheProcs (
        SPID INT ,
        QueryType NVARCHAR(258),
        DatabaseName sysname,
        AverageCPU DECIMAL(38,4),
        AverageCPUPerMinute DECIMAL(38,4),
        TotalCPU DECIMAL(38,4),
        PercentCPUByType MONEY,
        PercentCPU MONEY,
        AverageDuration DECIMAL(38,4),
        TotalDuration DECIMAL(38,4),
        PercentDuration MONEY,
        PercentDurationByType MONEY,
        AverageReads BIGINT,
        TotalReads BIGINT,
        PercentReads MONEY,
        PercentReadsByType MONEY,
        ExecutionCount BIGINT,
        PercentExecutions MONEY,
        PercentExecutionsByType MONEY,
        ExecutionsPerMinute MONEY,
        TotalWrites BIGINT,
        AverageWrites MONEY,
        PercentWrites MONEY,
        PercentWritesByType MONEY,
        WritesPerMinute MONEY,
        PlanCreationTime DATETIME,
		PlanCreationTimeHours AS DATEDIFF(HOUR, PlanCreationTime, SYSDATETIME()),
        LastExecutionTime DATETIME,
        PlanHandle VARBINARY(64),
		[Remove Plan Handle From Cache] AS 
			CASE WHEN [PlanHandle] IS NOT NULL 
			THEN 'DBCC FREEPROCCACHE (' + CONVERT(VARCHAR(128), [PlanHandle], 1) + ');'
			ELSE 'N/A' END,
		SqlHandle VARBINARY(64),
			[Remove SQL Handle From Cache] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN 'DBCC FREEPROCCACHE (' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ');'
			ELSE 'N/A' END,
		[SQL Handle More Info] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN 'EXEC sp_BlitzCache @OnlySqlHandles = ''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '''; '
			ELSE 'N/A' END,
		QueryHash BINARY(8),
		[Query Hash More Info] AS 
			CASE WHEN [QueryHash] IS NOT NULL 
			THEN 'EXEC sp_BlitzCache @OnlyQueryHashes = ''' + CONVERT(VARCHAR(32), [QueryHash], 1) + '''; '
			ELSE 'N/A' END,
        QueryPlanHash BINARY(8),
        StatementStartOffset INT,
        StatementEndOffset INT,
        MinReturnedRows BIGINT,
        MaxReturnedRows BIGINT,
        AverageReturnedRows MONEY,
        TotalReturnedRows BIGINT,
        LastReturnedRows BIGINT,
		MinGrantKB BIGINT,
		MaxGrantKB BIGINT,
		MinUsedGrantKB BIGINT, 
		MaxUsedGrantKB BIGINT,
		PercentMemoryGrantUsed MONEY,
		AvgMaxMemoryGrant MONEY,
		MinSpills BIGINT,
		MaxSpills BIGINT,
		TotalSpills BIGINT,
		AvgSpills MONEY,
        QueryText NVARCHAR(MAX),
        QueryPlan XML,
        /* these next four columns are the total for the type of query.
            don't actually use them for anything apart from math by type.
            */
        TotalWorkerTimeForType BIGINT,
        TotalElapsedTimeForType BIGINT,
        TotalReadsForType BIGINT,
        TotalExecutionCountForType BIGINT,
        TotalWritesForType BIGINT,
        NumberOfPlans INT,
        NumberOfDistinctPlans INT,
        SerialDesiredMemory FLOAT,
        SerialRequiredMemory FLOAT,
        CachedPlanSize FLOAT,
        CompileTime FLOAT,
        CompileCPU FLOAT ,
        CompileMemory FLOAT ,
		MaxCompileMemory FLOAT ,
        min_worker_time BIGINT,
        max_worker_time BIGINT,
        is_forced_plan BIT,
        is_forced_parameterized BIT,
        is_cursor BIT,
		is_optimistic_cursor BIT,
		is_forward_only_cursor BIT,
        is_fast_forward_cursor BIT,
		is_cursor_dynamic BIT,
        is_parallel BIT,
		is_forced_serial BIT,
		is_key_lookup_expensive BIT,
		key_lookup_cost FLOAT,
		is_remote_query_expensive BIT,
		remote_query_cost FLOAT,
        frequent_execution BIT,
        parameter_sniffing BIT,
        unparameterized_query BIT,
        near_parallel BIT,
        plan_warnings BIT,
        plan_multiple_plans BIT,
        long_running BIT,
        downlevel_estimator BIT,
        implicit_conversions BIT,
        busy_loops BIT,
        tvf_join BIT,
        tvf_estimate BIT,
        compile_timeout BIT,
        compile_memory_limit_exceeded BIT,
        warning_no_join_predicate BIT,
        QueryPlanCost FLOAT,
        missing_index_count INT,
        unmatched_index_count INT,
        min_elapsed_time BIGINT,
        max_elapsed_time BIGINT,
        age_minutes MONEY,
        age_minutes_lifetime MONEY,
        is_trivial BIT,
		trace_flags_session VARCHAR(1000),
		is_unused_grant BIT,
		function_count INT,
		clr_function_count INT,
		is_table_variable BIT,
		no_stats_warning BIT,
		relop_warnings BIT,
		is_table_scan BIT,
	    backwards_scan BIT,
	    forced_index BIT,
	    forced_seek BIT,
	    forced_scan BIT,
		columnstore_row_mode BIT,
		is_computed_scalar BIT ,
		is_sort_expensive BIT,
		sort_cost FLOAT,
		is_computed_filter BIT,
		op_name VARCHAR(100) NULL,
		index_insert_count INT NULL,
		index_update_count INT NULL,
		index_delete_count INT NULL,
		cx_insert_count INT NULL,
		cx_update_count INT NULL,
		cx_delete_count INT NULL,
		table_insert_count INT NULL,
		table_update_count INT NULL,
		table_delete_count INT NULL,
		index_ops AS (index_insert_count + index_update_count + index_delete_count + 
			  cx_insert_count + cx_update_count + cx_delete_count +
			  table_insert_count + table_update_count + table_delete_count),
		is_row_level BIT,
		is_spatial BIT,
		index_dml BIT,
		table_dml BIT,
		long_running_low_cpu BIT,
		low_cost_high_cpu BIT,
		stale_stats BIT, 
		is_adaptive BIT,
		index_spool_cost FLOAT,
		index_spool_rows FLOAT,
		is_spool_expensive BIT,
		is_spool_more_rows BIT,
		estimated_rows FLOAT,
		is_bad_estimate BIT, 
		is_paul_white_electric BIT,
		is_row_goal BIT,
		is_big_spills BIT,
		is_mstvf BIT,
		is_mm_join BIT,
        is_nonsargable BIT,
		implicit_conversion_info XML,
		cached_execution_parameters XML,
		missing_indexes XML,
        SetOptions VARCHAR(MAX),
        Warnings VARCHAR(MAX)
    );
END;

DECLARE @DurationFilter_i INT,
		@MinMemoryPerQuery INT,
        @msg NVARCHAR(4000),
		@NoobSaibot BIT = 0;

IF @SortOrder = 'sp_BlitzIndex'
BEGIN
	RAISERROR(N'OUTSTANDING!', 0, 1) WITH NOWAIT;
	SET @SortOrder = 'reads';
	SET @NoobSaibot = 1;

END


IF @BringThePain = 1
   BEGIN
   RAISERROR(N'You have chosen to bring the pain. Setting top to 2147483647.', 0, 1) WITH NOWAIT;
   SET @Top = 2147483647;
   END; 

/* Change duration from seconds to milliseconds */
IF @DurationFilter IS NOT NULL
  BEGIN
  RAISERROR(N'Converting Duration Filter to milliseconds', 0, 1) WITH NOWAIT;
  SET @DurationFilter_i = CAST((@DurationFilter * 1000.0) AS INT);
  END; 

RAISERROR(N'Checking database validity', 0, 1) WITH NOWAIT;
SET @DatabaseName = LTRIM(RTRIM(@DatabaseName)) ;
IF (DB_ID(@DatabaseName)) IS NULL AND @DatabaseName <> N''
BEGIN
   RAISERROR('The database you specified does not exist. Please check the name and try again.', 16, 1);
   RETURN;
END;
IF (SELECT DATABASEPROPERTYEX(@DatabaseName, 'Status')) <> 'ONLINE'
BEGIN
   RAISERROR('The database you specified is not readable. Please check the name and try again. Better yet, check your server.', 16, 1);
   RETURN;
END;

SELECT @MinMemoryPerQuery = CONVERT(INT, c.value) FROM sys.configurations AS c WHERE c.name = 'min memory per query (KB)';

SET @SortOrder = LOWER(@SortOrder);
SET @SortOrder = REPLACE(REPLACE(@SortOrder, 'average', 'avg'), '.', '');
SET @SortOrder = REPLACE(@SortOrder, 'executions per minute', 'avg executions');
SET @SortOrder = REPLACE(@SortOrder, 'executions / minute', 'avg executions');
SET @SortOrder = REPLACE(@SortOrder, 'xpm', 'avg executions');
SET @SortOrder = REPLACE(@SortOrder, 'recent compilations', 'compiles');

RAISERROR(N'Checking sort order', 0, 1) WITH NOWAIT;
IF @SortOrder NOT IN ('cpu', 'avg cpu', 'reads', 'avg reads', 'writes', 'avg writes',
                       'duration', 'avg duration', 'executions', 'avg executions',
                       'compiles', 'memory grant', 'avg memory grant',
					   'spills', 'avg spills', 'all', 'all avg', 'sp_BlitzIndex')
  BEGIN
  RAISERROR(N'Invalid sort order chosen, reverting to cpu', 0, 1) WITH NOWAIT;
  SET @SortOrder = 'cpu';
  END; 

SELECT @OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
       @OutputSchemaName   = QUOTENAME(@OutputSchemaName),
       @OutputTableName    = QUOTENAME(@OutputTableName);

SET @QueryFilter = LOWER(@QueryFilter);

IF LEFT(@QueryFilter, 3) NOT IN ('all', 'sta', 'pro', 'fun')
  BEGIN
  RAISERROR(N'Invalid query filter chosen. Reverting to all.', 0, 1) WITH NOWAIT;
  SET @QueryFilter = 'all';
  END;

IF @SkipAnalysis = 1
  BEGIN
  RAISERROR(N'Skip Analysis set to 1, hiding Summary', 0, 1) WITH NOWAIT;
  SET @HideSummary = 1;
  END; 

IF @Reanalyze = 1 AND OBJECT_ID('tempdb..##bou_BlitzCacheResults') IS NULL
  BEGIN
  RAISERROR(N'##bou_BlitzCacheResults does not exist, can''t reanalyze', 0, 1) WITH NOWAIT;
  SET @Reanalyze = 0;
  END;

IF @Reanalyze = 0
  BEGIN
  RAISERROR(N'Cleaning up old warnings for your SPID', 0, 1) WITH NOWAIT;
  DELETE ##bou_BlitzCacheResults
    WHERE SPID = @@SPID
	OPTION (RECOMPILE) ;
  RAISERROR(N'Cleaning up old plans for your SPID', 0, 1) WITH NOWAIT;
  DELETE ##bou_BlitzCacheProcs
    WHERE SPID = @@SPID
	OPTION (RECOMPILE) ;
  END;  

IF @Reanalyze = 1 
	BEGIN
	RAISERROR(N'Reanalyzing current data, skipping to results', 0, 1) WITH NOWAIT;
    GOTO Results;
	END;

IF @SortOrder IN ('all', 'all avg')
	BEGIN
	RAISERROR(N'Checking all sort orders, please be patient', 0, 1) WITH NOWAIT;
    GOTO AllSorts;
	END;


RAISERROR(N'Creating temp tables for internal processing', 0, 1) WITH NOWAIT;
IF OBJECT_ID('tempdb..#only_query_hashes') IS NOT NULL
    DROP TABLE #only_query_hashes ;

IF OBJECT_ID('tempdb..#ignore_query_hashes') IS NOT NULL
    DROP TABLE #ignore_query_hashes ;

IF OBJECT_ID('tempdb..#only_sql_handles') IS NOT NULL
    DROP TABLE #only_sql_handles ;

IF OBJECT_ID('tempdb..#ignore_sql_handles') IS NOT NULL
    DROP TABLE #ignore_sql_handles ;
   
IF OBJECT_ID('tempdb..#p') IS NOT NULL
    DROP TABLE #p;

IF OBJECT_ID ('tempdb..#checkversion') IS NOT NULL
    DROP TABLE #checkversion;

IF OBJECT_ID ('tempdb..#configuration') IS NOT NULL
    DROP TABLE #configuration;

IF OBJECT_ID ('tempdb..#stored_proc_info') IS NOT NULL
    DROP TABLE #stored_proc_info;

IF OBJECT_ID ('tempdb..#plan_creation') IS NOT NULL
    DROP TABLE #plan_creation;

IF OBJECT_ID ('tempdb..#est_rows') IS NOT NULL
    DROP TABLE #est_rows;

IF OBJECT_ID ('tempdb..#plan_cost') IS NOT NULL
    DROP TABLE #plan_cost;

IF OBJECT_ID ('tempdb..#proc_costs') IS NOT NULL
    DROP TABLE #proc_costs;

IF OBJECT_ID ('tempdb..#stats_agg') IS NOT NULL
    DROP TABLE #stats_agg;

IF OBJECT_ID ('tempdb..#trace_flags') IS NOT NULL
    DROP TABLE #trace_flags;

IF OBJECT_ID('tempdb..#variable_info') IS NOT NULL
    DROP TABLE #variable_info;

IF OBJECT_ID('tempdb..#conversion_info') IS NOT NULL
    DROP TABLE #conversion_info;

IF OBJECT_ID('tempdb..#missing_index_xml') IS NOT NULL
    DROP TABLE #missing_index_xml;

IF OBJECT_ID('tempdb..#missing_index_schema') IS NOT NULL
    DROP TABLE #missing_index_schema;

IF OBJECT_ID('tempdb..#missing_index_usage') IS NOT NULL
    DROP TABLE #missing_index_usage;

IF OBJECT_ID('tempdb..#missing_index_detail') IS NOT NULL
    DROP TABLE #missing_index_detail;

IF OBJECT_ID('tempdb..#missing_index_pretty') IS NOT NULL
    DROP TABLE #missing_index_pretty;

IF OBJECT_ID('tempdb..#index_spool_ugly') IS NOT NULL
    DROP TABLE #index_spool_ugly;

CREATE TABLE #only_query_hashes (
    query_hash BINARY(8)
);

CREATE TABLE #ignore_query_hashes (
    query_hash BINARY(8)
);

CREATE TABLE #only_sql_handles (
    sql_handle VARBINARY(64)
);

CREATE TABLE #ignore_sql_handles (
    sql_handle VARBINARY(64)
);

CREATE TABLE #p (
    SqlHandle VARBINARY(64),
    TotalCPU BIGINT,
    TotalDuration BIGINT,
    TotalReads BIGINT,
    TotalWrites BIGINT,
    ExecutionCount BIGINT
);

CREATE TABLE #checkversion (
    version NVARCHAR(128),
    common_version AS SUBSTRING(version, 1, CHARINDEX('.', version) + 1 ),
    major AS PARSENAME(CONVERT(VARCHAR(32), version), 4),
    minor AS PARSENAME(CONVERT(VARCHAR(32), version), 3),
    build AS PARSENAME(CONVERT(VARCHAR(32), version), 2),
    revision AS PARSENAME(CONVERT(VARCHAR(32), version), 1)
);

CREATE TABLE #configuration (
    parameter_name VARCHAR(100),
    value DECIMAL(38,0)
);

CREATE TABLE #plan_creation
(
    percent_24 DECIMAL(5, 2),
    percent_4 DECIMAL(5, 2),
    percent_1 DECIMAL(5, 2),
	total_plans INT,
    SPID INT
);

CREATE TABLE #est_rows
(
    QueryHash BINARY(8),
    estimated_rows FLOAT
);

CREATE TABLE #plan_cost
(
    QueryPlanCost FLOAT,
    SqlHandle VARBINARY(64),
	PlanHandle VARBINARY(64),
    QueryHash BINARY(8),
    QueryPlanHash BINARY(8)
);

CREATE TABLE #proc_costs
(
    PlanTotalQuery FLOAT,
    PlanHandle VARBINARY(64),
    SqlHandle VARBINARY(64)
);

CREATE TABLE #stats_agg
(
    SqlHandle VARBINARY(64),
	LastUpdate DATETIME2(7),
    ModificationCount INT,
    SamplingPercent FLOAT,
    [Statistics] NVARCHAR(258),
    [Table] NVARCHAR(258),
    [Schema] NVARCHAR(258),
    [Database] NVARCHAR(258),
);

CREATE TABLE #trace_flags
(
    SqlHandle VARBINARY(64),
    QueryHash BINARY(8),
    global_trace_flags VARCHAR(1000),
    session_trace_flags VARCHAR(1000)
);

CREATE TABLE #stored_proc_info
(
    SPID INT,
	SqlHandle VARBINARY(64),
    QueryHash BINARY(8),
    variable_name NVARCHAR(258),
    variable_datatype NVARCHAR(258),
	converted_column_name NVARCHAR(258),
    compile_time_value NVARCHAR(258),
    proc_name NVARCHAR(1000),
    column_name NVARCHAR(4000),
    converted_to NVARCHAR(258),
	set_options NVARCHAR(1000)
);

CREATE TABLE #variable_info
(
    SPID INT,
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    proc_name NVARCHAR(1000),
    variable_name NVARCHAR(258),
    variable_datatype NVARCHAR(258),
    compile_time_value NVARCHAR(258)
);

CREATE TABLE #conversion_info
(
    SPID INT,
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    proc_name NVARCHAR(258),
    expression NVARCHAR(4000),
    at_charindex AS CHARINDEX('@', expression),
    bracket_charindex AS CHARINDEX(']', expression, CHARINDEX('@', expression)) - CHARINDEX('@', expression),
    comma_charindex AS CHARINDEX(',', expression) + 1,
    second_comma_charindex AS
        CHARINDEX(',', expression, CHARINDEX(',', expression) + 1) - CHARINDEX(',', expression) - 1,
    equal_charindex AS CHARINDEX('=', expression) + 1,
    paren_charindex AS CHARINDEX('(', expression) + 1,
    comma_paren_charindex AS
        CHARINDEX(',', expression, CHARINDEX('(', expression) + 1) - CHARINDEX('(', expression) - 1,
    convert_implicit_charindex AS CHARINDEX('=CONVERT_IMPLICIT', expression)
);


CREATE TABLE #missing_index_xml
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    index_xml XML
);


CREATE TABLE #missing_index_schema
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
    index_xml XML
);


CREATE TABLE #missing_index_usage
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	usage NVARCHAR(128),
    index_xml XML
);


CREATE TABLE #missing_index_detail
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
    usage NVARCHAR(128),
    column_name NVARCHAR(128)
);


CREATE TABLE #missing_index_pretty
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	equality NVARCHAR(MAX),
	inequality NVARCHAR(MAX),
	[include] NVARCHAR(MAX),
	executions NVARCHAR(128),
	query_cost NVARCHAR(128),
	creation_hours NVARCHAR(128),
	is_spool BIT,
	details AS N'/* '
	           + CHAR(10) 
			   + CASE is_spool 
			          WHEN 0 
					  THEN N'The Query Processor estimates that implementing the '
					  ELSE N'We estimate that implementing the '
				 END 
			   + N'following index could improve query cost (' + query_cost + N')'
			   + CHAR(10) 
			   + N'by '
			   + CONVERT(NVARCHAR(30), impact)
			   + N'% for ' + executions + N' executions of the query'
			   + N' over the last ' + 
					CASE WHEN creation_hours < 24
					     THEN creation_hours + N' hours.'
						 WHEN creation_hours = 24
						 THEN ' 1 day.'
						 WHEN creation_hours > 24
						 THEN (CONVERT(NVARCHAR(128), creation_hours / 24)) + N' days.'
					     ELSE N''
					END
			   + CHAR(10)
			   + N'*/'
			   + CHAR(10) + CHAR(13) 
			   + N'/* '
			   + CHAR(10)
			   + N'USE '
			   + database_name
			   + CHAR(10)
			   + N'GO'
			   + CHAR(10) + CHAR(13)
			   + N'CREATE NONCLUSTERED INDEX ix_'
			   + ISNULL(REPLACE(REPLACE(REPLACE(equality,'[', ''), ']', ''),   ', ', '_'), '')
			   + ISNULL(REPLACE(REPLACE(REPLACE(inequality,'[', ''), ']', ''), ', ', '_'), '')
			   + CASE WHEN [include] IS NOT NULL THEN + N'Includes' ELSE N'' END
			   + CHAR(10)
			   + N' ON '
			   + schema_name
			   + N'.'
			   + table_name
			   + N' (' + 
			   + CASE WHEN equality IS NOT NULL 
					  THEN equality
						+ CASE WHEN inequality IS NOT NULL
							   THEN N', ' + inequality
							   ELSE N''
						  END
					 ELSE inequality
				 END			   
			   + N')' 
			   + CHAR(10)
			   + CASE WHEN include IS NOT NULL
					  THEN N'INCLUDE (' + include + N') WITH (FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?);'
					  ELSE N' WITH (FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?);'
				 END
			   + CHAR(10)
			   + N'GO'
			   + CHAR(10)
			   + N'*/'
);


CREATE TABLE #index_spool_ugly
(
    QueryHash BINARY(8),
    SqlHandle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	equality NVARCHAR(MAX),
	inequality NVARCHAR(MAX),
	[include] NVARCHAR(MAX),
	executions NVARCHAR(128),
	query_cost NVARCHAR(128),
	creation_hours NVARCHAR(128)
);


RAISERROR(N'Checking plan cache age', 0, 1) WITH NOWAIT;
WITH x AS (
SELECT SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 24 THEN 1 ELSE 0 END) AS [plans_24],
	   SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 4 THEN 1 ELSE 0 END) AS [plans_4],
	   SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 1 THEN 1 ELSE 0 END) AS [plans_1],
	   COUNT(deqs.creation_time) AS [total_plans]
FROM sys.dm_exec_query_stats AS deqs
)
INSERT INTO #plan_creation ( percent_24, percent_4, percent_1, total_plans, SPID )
SELECT CONVERT(DECIMAL(3,2), NULLIF(x.plans_24, 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_24],
	   CONVERT(DECIMAL(3,2), NULLIF(x.plans_4 , 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_4],
	   CONVERT(DECIMAL(3,2), NULLIF(x.plans_1 , 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_1],
	   x.total_plans,
	   @@SPID AS SPID
FROM x
OPTION (RECOMPILE) ;


SET @OnlySqlHandles = LTRIM(RTRIM(@OnlySqlHandles)) ;
SET @OnlyQueryHashes = LTRIM(RTRIM(@OnlyQueryHashes)) ;
SET @IgnoreQueryHashes = LTRIM(RTRIM(@IgnoreQueryHashes)) ;

DECLARE @individual VARCHAR(100) ;

IF (@OnlySqlHandles IS NOT NULL AND @IgnoreSqlHandles IS NOT NULL)
BEGIN
RAISERROR('You shouldn''t need to ignore and filter on SqlHandle at the same time.', 0, 1) WITH NOWAIT;
RETURN;
END;

IF (@StoredProcName IS NOT NULL AND (@OnlySqlHandles IS NOT NULL OR @IgnoreSqlHandles IS NOT NULL))
BEGIN
RAISERROR('You can''t filter on stored procedure name and SQL Handle.', 0, 1) WITH NOWAIT;
RETURN;
END;

IF @OnlySqlHandles IS NOT NULL
    AND LEN(@OnlySqlHandles) > 0
BEGIN
    RAISERROR(N'Processing SQL Handles', 0, 1) WITH NOWAIT;
	SET @individual = '';

    WHILE LEN(@OnlySqlHandles) > 0
    BEGIN
        IF PATINDEX('%,%', @OnlySqlHandles) > 0
        BEGIN  
               SET @individual = SUBSTRING(@OnlySqlHandles, 0, PATINDEX('%,%',@OnlySqlHandles)) ;
               
               INSERT INTO #only_sql_handles
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;
               
               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

               SET @OnlySqlHandles = SUBSTRING(@OnlySqlHandles, LEN(@individual + ',') + 1, LEN(@OnlySqlHandles)) ;
        END;
        ELSE
        BEGIN
               SET @individual = @OnlySqlHandles;
               SET @OnlySqlHandles = NULL;

               INSERT INTO #only_sql_handles
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;

               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
        END;
    END;
END;    

IF @IgnoreSqlHandles IS NOT NULL
    AND LEN(@IgnoreSqlHandles) > 0
BEGIN
    RAISERROR(N'Processing SQL Handles To Ignore', 0, 1) WITH NOWAIT;
	SET @individual = '';

    WHILE LEN(@IgnoreSqlHandles) > 0
    BEGIN
        IF PATINDEX('%,%', @IgnoreSqlHandles) > 0
        BEGIN  
               SET @individual = SUBSTRING(@IgnoreSqlHandles, 0, PATINDEX('%,%',@IgnoreSqlHandles)) ;
               
               INSERT INTO #ignore_sql_handles
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;
               
               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

               SET @IgnoreSqlHandles = SUBSTRING(@IgnoreSqlHandles, LEN(@individual + ',') + 1, LEN(@IgnoreSqlHandles)) ;
        END;
        ELSE
        BEGIN
               SET @individual = @IgnoreSqlHandles;
               SET @IgnoreSqlHandles = NULL;

               INSERT INTO #ignore_sql_handles
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;

               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
        END;
    END;
END;  

IF @StoredProcName IS NOT NULL AND @StoredProcName <> N''

BEGIN
	RAISERROR(N'Setting up filter for stored procedure name', 0, 1) WITH NOWAIT;
	INSERT #only_sql_handles
	        ( sql_handle )
	SELECT  ISNULL(deps.sql_handle, CONVERT(VARBINARY(64),'0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'))
	FROM sys.dm_exec_procedure_stats AS deps
	WHERE OBJECT_NAME(deps.object_id, deps.database_id) = @StoredProcName
	OPTION (RECOMPILE) ;

		IF (SELECT COUNT(*) FROM #only_sql_handles) = 0
			BEGIN
			RAISERROR(N'No information for that stored procedure was found.', 0, 1) WITH NOWAIT;
			RETURN;
			END;

END;



IF ((@OnlyQueryHashes IS NOT NULL AND LEN(@OnlyQueryHashes) > 0)
    OR (@IgnoreQueryHashes IS NOT NULL AND LEN(@IgnoreQueryHashes) > 0))
   AND LEFT(@QueryFilter, 3) IN ('pro', 'fun')
BEGIN
   RAISERROR('You cannot limit by query hash and filter by stored procedure', 16, 1);
   RETURN;
END;

/* If the user is attempting to limit by query hash, set up the
   #only_query_hashes temp table. This will be used to narrow down
   results.

   Just a reminder: Using @OnlyQueryHashes will ignore stored
   procedures and triggers.
 */
IF @OnlyQueryHashes IS NOT NULL
   AND LEN(@OnlyQueryHashes) > 0
BEGIN
	RAISERROR(N'Setting up filter for Query Hashes', 0, 1) WITH NOWAIT;
    SET @individual = '';

   WHILE LEN(@OnlyQueryHashes) > 0
   BEGIN
        IF PATINDEX('%,%', @OnlyQueryHashes) > 0
        BEGIN  
               SET @individual = SUBSTRING(@OnlyQueryHashes, 0, PATINDEX('%,%',@OnlyQueryHashes)) ;
               
               INSERT INTO #only_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;
               
               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

               SET @OnlyQueryHashes = SUBSTRING(@OnlyQueryHashes, LEN(@individual + ',') + 1, LEN(@OnlyQueryHashes)) ;
        END;
        ELSE
        BEGIN
               SET @individual = @OnlyQueryHashes;
               SET @OnlyQueryHashes = NULL;

               INSERT INTO #only_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos)
			   OPTION (RECOMPILE) ;

               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
        END;
   END;
END;

/* If the user is setting up a list of query hashes to ignore, those
   values will be inserted into #ignore_query_hashes. This is used to
   exclude values from query results.

   Just a reminder: Using @IgnoreQueryHashes will ignore stored
   procedures and triggers.
 */
IF @IgnoreQueryHashes IS NOT NULL
   AND LEN(@IgnoreQueryHashes) > 0
BEGIN
	RAISERROR(N'Setting up filter to ignore query hashes', 0, 1) WITH NOWAIT;
   SET @individual = '' ;

   WHILE LEN(@IgnoreQueryHashes) > 0
   BEGIN
        IF PATINDEX('%,%', @IgnoreQueryHashes) > 0
        BEGIN  
               SET @individual = SUBSTRING(@IgnoreQueryHashes, 0, PATINDEX('%,%',@IgnoreQueryHashes)) ;
               
               INSERT INTO #ignore_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos) 
			   OPTION (RECOMPILE) ;
               
               SET @IgnoreQueryHashes = SUBSTRING(@IgnoreQueryHashes, LEN(@individual + ',') + 1, LEN(@IgnoreQueryHashes)) ;
        END;
        ELSE
        BEGIN
               SET @individual = @IgnoreQueryHashes ;
               SET @IgnoreQueryHashes = NULL ;

               INSERT INTO #ignore_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos) 
			   OPTION (RECOMPILE) ;
        END;
   END;
END;

IF @ConfigurationDatabaseName IS NOT NULL
BEGIN
   RAISERROR(N'Reading values from Configuration Database', 0, 1) WITH NOWAIT;
   DECLARE @config_sql NVARCHAR(MAX) = N'INSERT INTO #configuration SELECT parameter_name, value FROM '
        + QUOTENAME(@ConfigurationDatabaseName)
        + '.' + QUOTENAME(@ConfigurationSchemaName)
        + '.' + QUOTENAME(@ConfigurationTableName)
        + ' ; ' ;
   EXEC(@config_sql);
END;

RAISERROR(N'Setting up variables', 0, 1) WITH NOWAIT;
DECLARE @sql NVARCHAR(MAX) = N'',
        @insert_list NVARCHAR(MAX) = N'',
        @plans_triggers_select_list NVARCHAR(MAX) = N'',
        @body NVARCHAR(MAX) = N'',
        @body_where NVARCHAR(MAX) = N'WHERE 1 = 1 ' + @nl,
        @body_order NVARCHAR(MAX) = N'ORDER BY #sortable# DESC OPTION (RECOMPILE) ',
        
        @q NVARCHAR(1) = N'''',
        @pv VARCHAR(20),
        @pos TINYINT,
        @v DECIMAL(6,2),
        @build INT;


RAISERROR (N'Determining SQL Server version.',0,1) WITH NOWAIT;

INSERT INTO #checkversion (version)
SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))
OPTION (RECOMPILE);


SELECT @v = common_version ,
       @build = build
FROM   #checkversion
OPTION (RECOMPILE);

IF (@SortOrder IN ('memory grant', 'avg memory grant')) 
AND ((@v < 11)
OR (@v = 11 AND @build < 6020) 
OR (@v = 12 AND @build < 5000) 
OR (@v = 13 AND @build < 1601))
BEGIN
   RAISERROR('Your version of SQL does not support sorting by memory grant or average memory grant. Please use another sort order.', 16, 1);
   RETURN;
END;

IF (@SortOrder IN ('spills', 'avg spills')) 
AND (@v < 13 
	OR @v = 13 AND @build < 5026
	OR @v = 14 AND @build < 3015)
BEGIN
   RAISERROR('Your version of SQL does not support sorting by spills or average spills. Please use another sort order.', 16, 1);
   RETURN;
END;

IF ((LEFT(@QueryFilter, 3) = 'fun') AND (@v < 13))
BEGIN
   RAISERROR('Your version of SQL does not support filtering by functions. Please use another filter.', 16, 1);
   RETURN;
END;

RAISERROR (N'Creating dynamic SQL based on SQL Server version.',0,1) WITH NOWAIT;

SET @insert_list += N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT INTO ##bou_BlitzCacheProcs (SPID, QueryType, DatabaseName, AverageCPU, TotalCPU, AverageCPUPerMinute, PercentCPUByType, PercentDurationByType,
                    PercentReadsByType, PercentExecutionsByType, AverageDuration, TotalDuration, AverageReads, TotalReads, ExecutionCount,
                    ExecutionsPerMinute, TotalWrites, AverageWrites, PercentWritesByType, WritesPerMinute, PlanCreationTime,
                    LastExecutionTime, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows,
                    LastReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, 
					QueryText, QueryPlan, TotalWorkerTimeForType, TotalElapsedTimeForType, TotalReadsForType,
                    TotalExecutionCountForType, TotalWritesForType, SqlHandle, PlanHandle, QueryHash, QueryPlanHash,
                    min_worker_time, max_worker_time, is_parallel, min_elapsed_time, max_elapsed_time, age_minutes, age_minutes_lifetime) ' ;

SET @body += N'
FROM   (SELECT TOP (@Top) x.*, xpa.*,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE()) 
                          ELSE NULL END) as MONEY) as age_minutes,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time) 
                          ELSE Null END) as MONEY) as age_minutes_lifetime
        FROM   sys.#view# x
               CROSS APPLY (SELECT * FROM sys.dm_exec_plan_attributes(x.plan_handle) AS ixpa 
                            WHERE ixpa.attribute = ''dbid'') AS xpa ' + @nl ;

SET @body += N'        WHERE  1 = 1 ' +  @nl ;


IF @IgnoreSystemDBs = 1
    BEGIN
	RAISERROR(N'Ignoring system databases by default', 0, 1) WITH NOWAIT;
	SET @body += N'               AND COALESCE(DB_NAME(CAST(xpa.value AS INT)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(CAST(xpa.value AS INT)), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;
	END; 

IF @DatabaseName IS NOT NULL OR @DatabaseName <> N''
	BEGIN 
    RAISERROR(N'Filtering database name chosen', 0, 1) WITH NOWAIT;
	SET @body += N'               AND CAST(xpa.value AS BIGINT) = DB_ID(N'
                 + QUOTENAME(@DatabaseName, N'''')
                 + N') ' + @nl;
	END; 

IF (SELECT COUNT(*) FROM #only_sql_handles) > 0
BEGIN
    RAISERROR(N'Including only chosen SQL Handles', 0, 1) WITH NOWAIT;
	SET @body += N'               AND EXISTS(SELECT 1/0 FROM #only_sql_handles q WHERE q.sql_handle = x.sql_handle) ' + @nl ;
END;      

IF (SELECT COUNT(*) FROM #ignore_sql_handles) > 0
BEGIN
    RAISERROR(N'Including only chosen SQL Handles', 0, 1) WITH NOWAIT;
	SET @body += N'               AND NOT EXISTS(SELECT 1/0 FROM #ignore_sql_handles q WHERE q.sql_handle = x.sql_handle) ' + @nl ;
END;    

IF (SELECT COUNT(*) FROM #only_query_hashes) > 0
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0
   AND (SELECT COUNT(*) FROM #only_sql_handles) = 0
   AND (SELECT COUNT(*) FROM #ignore_sql_handles) = 0
BEGIN
    RAISERROR(N'Including only chosen Query Hashes', 0, 1) WITH NOWAIT;
	SET @body += N'               AND EXISTS(SELECT 1/0 FROM #only_query_hashes q WHERE q.query_hash = x.query_hash) ' + @nl ;
END;

/* filtering for query hashes */
IF (SELECT COUNT(*) FROM #ignore_query_hashes) > 0
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0
BEGIN
    RAISERROR(N'Excluding chosen Query Hashes', 0, 1) WITH NOWAIT;
	SET @body += N'               AND NOT EXISTS(SELECT 1/0 FROM #ignore_query_hashes iq WHERE iq.query_hash = x.query_hash) ' + @nl ;
END;
/* end filtering for query hashes */


IF @DurationFilter IS NOT NULL
    BEGIN 
	RAISERROR(N'Setting duration filter', 0, 1) WITH NOWAIT;
	SET @body += N'       AND (total_elapsed_time / 1000.0) / execution_count > @min_duration ' + @nl ;
	END; 

IF @MinutesBack IS NOT NULL
	BEGIN
	RAISERROR(N'Setting minutes back filter', 0, 1) WITH NOWAIT;
	SET @body += N'       AND x.last_execution_time >= DATEADD(MINUTE, @min_back, GETDATE()) ' + @nl ;
	END;


/* Apply the sort order here to only grab relevant plans.
   This should make it faster to process since we'll be pulling back fewer
   plans for processing.
 */
RAISERROR(N'Applying chosen sort order', 0, 1) WITH NOWAIT;
SELECT @body += N'        ORDER BY ' +
                CASE @SortOrder  WHEN N'cpu' THEN N'total_worker_time'
                                 WHEN N'reads' THEN N'total_logical_reads'
                                 WHEN N'writes' THEN N'total_logical_writes'
                                 WHEN N'duration' THEN N'total_elapsed_time'
                                 WHEN N'executions' THEN N'execution_count'
                                 WHEN N'compiles' THEN N'cached_time'
								 WHEN N'memory grant' THEN N'max_grant_kb'
								 WHEN N'spills' THEN N'max_spills'
                                 /* And now the averages */
                                 WHEN N'avg cpu' THEN N'total_worker_time / execution_count'
                                 WHEN N'avg reads' THEN N'total_logical_reads / execution_count'
                                 WHEN N'avg writes' THEN N'total_logical_writes / execution_count'
                                 WHEN N'avg duration' THEN N'total_elapsed_time / execution_count'
								 WHEN N'avg memory grant' THEN N'CASE WHEN max_grant_kb = 0 THEN 0 ELSE max_grant_kb / execution_count END'
                                 WHEN N'avg spills' THEN N'CASE WHEN total_spills = 0 THEN 0 ELSE total_spills / execution_count END'
								 WHEN N'avg executions' THEN 'CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE())
                          ELSE NULL END) as MONEY), CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time)
                          ELSE Null END) as MONEY), 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE())
                          ELSE NULL END) as MONEY), CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time)
                          ELSE Null END) as MONEY))) AS money)
            END '
                END + N' DESC ' + @nl ;


                          
SET @body += N') AS qs 
	   CROSS JOIN(SELECT SUM(execution_count) AS t_TotalExecs,
                         SUM(CAST(total_elapsed_time AS BIGINT) / 1000.0) AS t_TotalElapsed,
                         SUM(CAST(total_worker_time AS BIGINT) / 1000.0) AS t_TotalWorker,
                         SUM(CAST(total_logical_reads AS BIGINT)) AS t_TotalReads,
                         SUM(CAST(total_logical_writes AS BIGINT)) AS t_TotalWrites
                  FROM   sys.#view#) AS t
       CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
       CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
       CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp ' + @nl ;

SET @body_where += N'       AND pa.attribute = ' + QUOTENAME('dbid', @q ) + @nl ;

IF @NoobSaibot = 1
BEGIN
	SET @body_where += N'       AND qp.query_plan.exist(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";//p:StmtSimple//p:MissingIndex'') = 1' + @nl ;
END

SET @plans_triggers_select_list += N'
SELECT TOP (@Top)
       @@SPID ,
       ''Procedure or Function: '' 
	   + QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(qs.object_id, qs.database_id),''''))
	   + ''.''
	   + QUOTENAME(COALESCE(OBJECT_NAME(qs.object_id, qs.database_id),'''')) AS QueryType,
       COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), N''-- N/A --'') AS DatabaseName,
       (total_worker_time / 1000.0) / execution_count AS AvgCPU ,
       (total_worker_time / 1000.0) AS TotalCPU ,
       CASE WHEN total_worker_time = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((total_worker_time / 1000.0) / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time)) AS MONEY)
            END AS AverageCPUPerMinute ,
       CASE WHEN t.t_TotalWorker = 0 THEN 0
            ELSE CAST(ROUND(100.00 * (total_worker_time / 1000.0) / t.t_TotalWorker, 2) AS MONEY)
            END AS PercentCPUByType,
       CASE WHEN t.t_TotalElapsed = 0 THEN 0
            ELSE CAST(ROUND(100.00 * (total_elapsed_time / 1000.0) / t.t_TotalElapsed, 2) AS MONEY)
            END AS PercentDurationByType,
       CASE WHEN t.t_TotalReads = 0 THEN 0
            ELSE CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY)
            END AS PercentReadsByType,
       CASE WHEN t.t_TotalExecs = 0 THEN 0
            ELSE CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY)
            END AS PercentExecutionsByType,
       (total_elapsed_time / 1000.0) / execution_count AS AvgDuration ,
       (total_elapsed_time / 1000.0) AS TotalDuration ,
       total_logical_reads / execution_count AS AvgReads ,
       total_logical_reads AS TotalReads ,
       execution_count AS ExecutionCount ,
       CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time))) AS money)
            END AS ExecutionsPerMinute ,
       total_logical_writes AS TotalWrites ,
       total_logical_writes / execution_count AS AverageWrites ,
       CASE WHEN t.t_TotalWrites = 0 THEN 0
            ELSE CAST(ROUND(100.00 * total_logical_writes / t.t_TotalWrites, 2) AS MONEY)
            END AS PercentWritesByType,
       CASE WHEN total_logical_writes = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((1.00 * total_logical_writes / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0)) AS money)
            END AS WritesPerMinute,
       qs.cached_time AS PlanCreationTime,
       qs.last_execution_time AS LastExecutionTime,
       NULL AS StatementStartOffset,
       NULL AS StatementEndOffset,
       NULL AS MinReturnedRows,
       NULL AS MaxReturnedRows,
       NULL AS AvgReturnedRows,
       NULL AS TotalReturnedRows,
       NULL AS LastReturnedRows,
       NULL AS MinGrantKB,
       NULL AS MaxGrantKB,
       NULL AS MinUsedGrantKB, 
	   NULL AS MaxUsedGrantKB,
	   NULL AS PercentMemoryGrantUsed, 
	   NULL AS AvgMaxMemoryGrant,';

		IF @v >=15 OR (@v = 14 AND @build >= 3015) OR (@v = 13 AND @build >= 5026)
    BEGIN
        RAISERROR(N'Getting spill information for newer versions of SQL', 0, 1) WITH NOWAIT;
		SET @plans_triggers_select_list += N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills, ';
    END;
    ELSE
    BEGIN
        RAISERROR(N'Substituting NULLs for spill columns in older versions of SQL', 0, 1) WITH NOWAIT;
		SET @plans_triggers_select_list += N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills, 
		   NULL AS AvgSpills, ' ;
    END;		       
	     
		SET @plans_triggers_select_list +=  
	 N'st.text AS QueryText ,
       query_plan AS QueryPlan,
       t.t_TotalWorker,
       t.t_TotalElapsed,
       t.t_TotalReads,
       t.t_TotalExecs,
       t.t_TotalWrites,
       qs.sql_handle AS SqlHandle,
       qs.plan_handle AS PlanHandle,
       NULL AS QueryHash,
       NULL AS QueryPlanHash,
       qs.min_worker_time / 1000.0,
       qs.max_worker_time / 1000.0,
       CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN 1 ELSE 0 END,
       qs.min_elapsed_time / 1000.0,
       qs.max_elapsed_time / 1000.0,
       age_minutes, 
       age_minutes_lifetime ';


IF LEFT(@QueryFilter, 3) IN ('all', 'sta')
BEGIN
    SET @sql += @insert_list;
    
    SET @sql += N'
    SELECT TOP (@Top)
           @@SPID ,
           ''Statement'' AS QueryType,
           COALESCE(DB_NAME(CAST(pa.value AS INT)), N''-- N/A --'') AS DatabaseName,
           (total_worker_time / 1000.0) / execution_count AS AvgCPU ,
           (total_worker_time / 1000.0) AS TotalCPU ,
           CASE WHEN total_worker_time = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((total_worker_time / 1000.0) / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time)) AS MONEY)
                END AS AverageCPUPerMinute ,
           CASE WHEN t.t_TotalWorker = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_worker_time / t.t_TotalWorker, 2) AS MONEY)
                END AS PercentCPUByType,
           CASE WHEN t.t_TotalElapsed = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_elapsed_time / t.t_TotalElapsed, 2) AS MONEY)
                END AS PercentDurationByType,
           CASE WHEN t.t_TotalReads = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY)
                END AS PercentReadsByType,
           CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY) AS PercentExecutionsByType,
           (total_elapsed_time / 1000.0) / execution_count AS AvgDuration ,
           (total_elapsed_time / 1000.0) AS TotalDuration ,
           total_logical_reads / execution_count AS AvgReads ,
           total_logical_reads AS TotalReads ,
           execution_count AS ExecutionCount ,
           CASE WHEN execution_count = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time))) AS money)
                END AS ExecutionsPerMinute ,
           total_logical_writes AS TotalWrites ,
           total_logical_writes / execution_count AS AverageWrites ,
           CASE WHEN t.t_TotalWrites = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_logical_writes / t.t_TotalWrites, 2) AS MONEY)
                END AS PercentWritesByType,
           CASE WHEN total_logical_writes = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((1.00 * total_logical_writes / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0)) AS money)
                END AS WritesPerMinute,
           qs.creation_time AS PlanCreationTime,
           qs.last_execution_time AS LastExecutionTime,
           qs.statement_start_offset AS StatementStartOffset,
           qs.statement_end_offset AS StatementEndOffset, ';
    
    IF (@v >= 11) OR (@v >= 10.5 AND @build >= 2500)
    BEGIN
        RAISERROR(N'Adding additional info columns for newer versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           qs.min_rows AS MinReturnedRows,
           qs.max_rows AS MaxReturnedRows,
           CAST(qs.total_rows as MONEY) / execution_count AS AvgReturnedRows,
           qs.total_rows AS TotalReturnedRows,
           qs.last_rows AS LastReturnedRows, ' ;
    END;
    ELSE
    BEGIN
		RAISERROR(N'Substituting NULLs for more info columns in older versions of SQL', 0, 1) WITH NOWAIT;
        SET @sql += N'
           NULL AS MinReturnedRows,
           NULL AS MaxReturnedRows,
           NULL AS AvgReturnedRows,
           NULL AS TotalReturnedRows,
           NULL AS LastReturnedRows, ' ;
    END;

    IF (@v = 11 AND @build >= 6020) OR (@v = 12 AND @build >= 5000) OR (@v = 13 AND @build >= 1601) OR (@v >= 14)

    BEGIN
        RAISERROR(N'Getting memory grant information for newer versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           min_grant_kb AS MinGrantKB,
           max_grant_kb AS MaxGrantKB,
           min_used_grant_kb AS MinUsedGrantKB,
           max_used_grant_kb AS MaxUsedGrantKB,
           CAST(ISNULL(NULLIF(( max_used_grant_kb * 1.00 ), 0) / NULLIF(min_grant_kb, 0), 0) * 100. AS MONEY) AS PercentMemoryGrantUsed,
		   CAST(ISNULL(NULLIF(( max_grant_kb * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgMaxMemoryGrant, ';
    END;
    ELSE
    BEGIN
        RAISERROR(N'Substituting NULLs for memory grant columns in older versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           NULL AS MinGrantKB,
           NULL AS MaxGrantKB,
           NULL AS MinUsedGrantKB, 
		   NULL AS MaxUsedGrantKB,
		   NULL AS PercentMemoryGrantUsed, 
		   NULL AS AvgMaxMemoryGrant, ' ;
    END;

		IF @v >=15 OR (@v = 14 AND @build >= 3015) OR (@v = 13 AND @build >= 5026)
    BEGIN
        RAISERROR(N'Getting spill information for newer versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills,';
    END;
    ELSE
    BEGIN
        RAISERROR(N'Substituting NULLs for spill columns in older versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills, 
		   NULL AS AvgSpills, ' ;
    END;		       
    
    SET @sql += N'
           SUBSTRING(st.text, ( qs.statement_start_offset / 2 ) + 1, ( ( CASE qs.statement_end_offset
                                                                            WHEN -1 THEN DATALENGTH(st.text)
                                                                            ELSE qs.statement_end_offset
                                                                          END - qs.statement_start_offset ) / 2 ) + 1) AS QueryText ,
           query_plan AS QueryPlan,
           t.t_TotalWorker,
           t.t_TotalElapsed,
           t.t_TotalReads,
           t.t_TotalExecs,
           t.t_TotalWrites,
           qs.sql_handle AS SqlHandle,
           qs.plan_handle AS PlanHandle,
           qs.query_hash AS QueryHash,
           qs.query_plan_hash AS QueryPlanHash,
           qs.min_worker_time / 1000.0,
           qs.max_worker_time / 1000.0,
           CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN 1 ELSE 0 END,
           qs.min_elapsed_time / 1000.0,
           qs.max_worker_time  / 1000.0,
           age_minutes,
           age_minutes_lifetime ';
    
    SET @sql += REPLACE(REPLACE(@body, '#view#', 'dm_exec_query_stats'), 'cached_time', 'creation_time') ;
    
    SET @sql += REPLACE(@body_where, 'cached_time', 'creation_time') ;
    
    SET @sql += @body_order + @nl + @nl + @nl;

    IF @SortOrder = 'compiles'
    BEGIN
        RAISERROR(N'Sorting by compiles', 0, 1) WITH NOWAIT;
		SET @sql = REPLACE(@sql, '#sortable#', 'creation_time');
    END;
END;


IF (@QueryFilter = 'all' 
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0 
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0) 
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant'))
   OR (LEFT(@QueryFilter, 3) = 'pro')
BEGIN
    SET @sql += @insert_list;
    SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Stored Procedure') ;

    SET @sql += REPLACE(@body, '#view#', 'dm_exec_procedure_stats') ; 
    SET @sql += @body_where ;

    IF @IgnoreSystemDBs = 1
       SET @sql += N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;

    SET @sql += @body_order + @nl + @nl + @nl ;
END;

IF (@v >= 13
   AND @QueryFilter = 'all'
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0 
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0) 
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant'))
   AND (@SortOrder NOT IN ('spills', 'avg spills'))
   OR (LEFT(@QueryFilter, 3) = 'fun')
BEGIN
    SET @sql += @insert_list;
    SET @sql += REPLACE(REPLACE(@plans_triggers_select_list, '#query_type#', 'Function')
			, N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills, ', 
		   N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills, 
		   NULL AS AvgSpills, ') ;

    SET @sql += REPLACE(@body, '#view#', 'dm_exec_function_stats') ; 
    SET @sql += @body_where ;

    IF @IgnoreSystemDBs = 1
       SET @sql += N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;

    SET @sql += @body_order + @nl + @nl + @nl ;
END;


/*******************************************************************************
 *
 * Because the trigger execution count in SQL Server 2008R2 and earlier is not
 * correct, we ignore triggers for these versions of SQL Server. If you'd like
 * to include trigger numbers, just know that the ExecutionCount,
 * PercentExecutions, and ExecutionsPerMinute are wildly inaccurate for
 * triggers on these versions of SQL Server.
 *
 * This is why we can't have nice things.
 *
 ******************************************************************************/
IF (@UseTriggersAnyway = 1 OR @v >= 11)
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0
   AND (@QueryFilter = 'all')
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant'))
BEGIN
   RAISERROR (N'Adding SQL to collect trigger stats.',0,1) WITH NOWAIT;

   /* Trigger level information from the plan cache */
   SET @sql += @insert_list ;

   SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Trigger') ;

   SET @sql += REPLACE(@body, '#view#', 'dm_exec_trigger_stats') ;

   SET @sql += @body_where ;

   IF @IgnoreSystemDBs = 1
      SET @sql += N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;
   
   SET @sql += @body_order + @nl + @nl + @nl ;
END;

DECLARE @sort NVARCHAR(MAX);

SELECT @sort = CASE @SortOrder  WHEN N'cpu' THEN N'total_worker_time'
                                WHEN N'reads' THEN N'total_logical_reads'
                                WHEN N'writes' THEN N'total_logical_writes'
                                WHEN N'duration' THEN N'total_elapsed_time'
                                WHEN N'executions' THEN N'execution_count'
                                WHEN N'compiles' THEN N'cached_time'
								WHEN N'memory grant' THEN N'max_grant_kb'
								WHEN N'spills' THEN N'max_spills'
                                /* And now the averages */
                                WHEN N'avg cpu' THEN N'total_worker_time / execution_count'
                                WHEN N'avg reads' THEN N'total_logical_reads / execution_count'
                                WHEN N'avg writes' THEN N'total_logical_writes / execution_count'
                                WHEN N'avg duration' THEN N'total_elapsed_time / execution_count'
								WHEN N'avg memory grant' THEN N'CASE WHEN max_grant_kb = 0 THEN 0 ELSE max_grant_kb / execution_count END'
                                WHEN N'avg spills' THEN N'CASE WHEN total_spills = 0 THEN 0 ELSE total_spills / execution_count END'
                                WHEN N'avg executions' THEN N'CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
               END ;

SELECT @sql = REPLACE(@sql, '#sortable#', @sort);

SET @sql += N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT INTO #p (SqlHandle, TotalCPU, TotalReads, TotalDuration, TotalWrites, ExecutionCount)
SELECT  SqlHandle,
        TotalCPU,
        TotalReads,
        TotalDuration,
        TotalWrites,
        ExecutionCount
FROM    (SELECT  SqlHandle,
                 TotalCPU,
                 TotalReads,
                 TotalDuration,
                 TotalWrites,
                 ExecutionCount,
                 ROW_NUMBER() OVER (PARTITION BY SqlHandle ORDER BY #sortable# DESC) AS rn
         FROM    ##bou_BlitzCacheProcs
		 WHERE SPID = @@SPID) AS x
WHERE x.rn = 1
OPTION (RECOMPILE);

WITH d AS (
SELECT  SPID,
        ROW_NUMBER() OVER (PARTITION BY SqlHandle, QueryHash ORDER BY #sortable# DESC) AS rn
FROM    ##bou_BlitzCacheProcs
WHERE SPID = @@SPID
)
DELETE d
WHERE d.rn > 1
AND SPID = @@SPID
OPTION (RECOMPILE);
';

SELECT @sort = CASE @SortOrder  WHEN N'cpu' THEN N'TotalCPU'
                                WHEN N'reads' THEN N'TotalReads'
                                WHEN N'writes' THEN N'TotalWrites'
                                WHEN N'duration' THEN N'TotalDuration'
                                WHEN N'executions' THEN N'ExecutionCount'
                                WHEN N'compiles' THEN N'PlanCreationTime'
								WHEN N'memory grant' THEN N'MaxGrantKB'
								WHEN N'spills' THEN N'MaxSpills'
                                /* And now the averages */
                                WHEN N'avg cpu' THEN N'TotalCPU / ExecutionCount'
                                WHEN N'avg reads' THEN N'TotalReads / ExecutionCount'
                                WHEN N'avg writes' THEN N'TotalWrites / ExecutionCount'
                                WHEN N'avg duration' THEN N'TotalDuration / ExecutionCount'
								WHEN N'avg memory grant' THEN N'AvgMaxMemoryGrant'
                                WHEN N'avg spills' THEN N'AvgSpills'
                                WHEN N'avg executions' THEN N'CASE WHEN ExecutionCount = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * ExecutionCount / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
               END ;

SELECT @sql = REPLACE(@sql, '#sortable#', @sort);


IF @Debug = 1
    BEGIN
        PRINT SUBSTRING(@sql, 0, 4000);
        PRINT SUBSTRING(@sql, 4000, 8000);
        PRINT SUBSTRING(@sql, 8000, 12000);
        PRINT SUBSTRING(@sql, 12000, 16000);
        PRINT SUBSTRING(@sql, 16000, 20000);
        PRINT SUBSTRING(@sql, 20000, 24000);
        PRINT SUBSTRING(@sql, 24000, 28000);
        PRINT SUBSTRING(@sql, 28000, 32000);
        PRINT SUBSTRING(@sql, 32000, 36000);
        PRINT SUBSTRING(@sql, 36000, 40000);
    END;

IF @Reanalyze = 0
BEGIN
    RAISERROR('Collecting execution plan information.', 0, 1) WITH NOWAIT;

    EXEC sp_executesql @sql, N'@Top INT, @min_duration INT, @min_back INT', @Top, @DurationFilter_i, @MinutesBack;
END;

IF @SkipAnalysis = 1
    BEGIN
	RAISERROR(N'Skipping analysis, going to results', 0, 1) WITH NOWAIT; 
	GOTO Results ;
	END; 


/* Update ##bou_BlitzCacheProcs to get Stored Proc info 
 * This should get totals for all statements in a Stored Proc
 */
RAISERROR(N'Attempting to aggregate stored proc info from separate statements', 0, 1) WITH NOWAIT;
;WITH agg AS (
    SELECT 
        b.SqlHandle,
        SUM(b.MinReturnedRows) AS MinReturnedRows,
        SUM(b.MaxReturnedRows) AS MaxReturnedRows,
        SUM(b.AverageReturnedRows) AS AverageReturnedRows,
        SUM(b.TotalReturnedRows) AS TotalReturnedRows,
        SUM(b.LastReturnedRows) AS LastReturnedRows,
		SUM(b.MinGrantKB) AS MinGrantKB,
		SUM(b.MaxGrantKB) AS MaxGrantKB,
		SUM(b.MinUsedGrantKB) AS MinUsedGrantKB,
		SUM(b.MaxUsedGrantKB) AS MaxUsedGrantKB,
        SUM(b.MinSpills) AS MinSpills,
        SUM(b.MaxSpills) AS MaxSpills,
        SUM(b.TotalSpills) AS TotalSpills
    FROM ##bou_BlitzCacheProcs b
    WHERE b.SPID = @@SPID
	AND b.QueryHash IS NOT NULL
    GROUP BY b.SqlHandle
)
UPDATE b
    SET 
        b.MinReturnedRows     = b2.MinReturnedRows,
        b.MaxReturnedRows     = b2.MaxReturnedRows,
        b.AverageReturnedRows = b2.AverageReturnedRows,
        b.TotalReturnedRows   = b2.TotalReturnedRows,
        b.LastReturnedRows    = b2.LastReturnedRows,
		b.MinGrantKB		  = b2.MinGrantKB,
		b.MaxGrantKB		  = b2.MaxGrantKB,
		b.MinUsedGrantKB	  = b2.MinUsedGrantKB,
		b.MaxUsedGrantKB      = b2.MaxUsedGrantKB,
        b.MinSpills           = b2.MinSpills,
        b.MaxSpills           = b2.MaxSpills,
        b.TotalSpills         = b2.TotalSpills
FROM ##bou_BlitzCacheProcs b
JOIN agg b2
ON b2.SqlHandle = b.SqlHandle
WHERE b.QueryHash IS NULL
AND b.SPID = @@SPID
OPTION (RECOMPILE) ;

/* Compute the total CPU, etc across our active set of the plan cache.
 * Yes, there's a flaw - this doesn't include anything outside of our @Top
 * metric.
 */
RAISERROR('Computing CPU, duration, read, and write metrics', 0, 1) WITH NOWAIT;
DECLARE @total_duration MONEY,
        @total_cpu MONEY,
        @total_reads MONEY,
        @total_writes MONEY,
        @total_execution_count MONEY;

SELECT  @total_cpu = SUM(TotalCPU),
        @total_duration = SUM(TotalDuration),
        @total_reads = SUM(TotalReads),
        @total_writes = SUM(TotalWrites),
        @total_execution_count = SUM(ExecutionCount)
FROM    #p 
OPTION (RECOMPILE) ;

DECLARE @cr NVARCHAR(1) = NCHAR(13);
DECLARE @lf NVARCHAR(1) = NCHAR(10);
DECLARE @tab NVARCHAR(1) = NCHAR(9);

/* Update CPU percentage for stored procedures */
RAISERROR(N'Update CPU percentage for stored procedures', 0, 1) WITH NOWAIT;
UPDATE ##bou_BlitzCacheProcs
SET     PercentCPU = y.PercentCPU,
        PercentDuration = y.PercentDuration,
        PercentReads = y.PercentReads,
        PercentWrites = y.PercentWrites,
        PercentExecutions = y.PercentExecutions,
        ExecutionsPerMinute = y.ExecutionsPerMinute,
        /* Strip newlines and tabs. Tabs are replaced with multiple spaces
           so that the later whitespace trim will completely eliminate them
         */
        QueryText = REPLACE(REPLACE(REPLACE(QueryText, @cr, ' '), @lf, ' '), @tab, '  ')
FROM (
    SELECT  PlanHandle,
            CASE @total_cpu WHEN 0 THEN 0
                 ELSE CAST((100. * TotalCPU) / @total_cpu AS MONEY) END AS PercentCPU,
            CASE @total_duration WHEN 0 THEN 0
                 ELSE CAST((100. * TotalDuration) / @total_duration AS MONEY) END AS PercentDuration,
            CASE @total_reads WHEN 0 THEN 0
                 ELSE CAST((100. * TotalReads) / @total_reads AS MONEY) END AS PercentReads,
            CASE @total_writes WHEN 0 THEN 0
                 ELSE CAST((100. * TotalWrites) / @total_writes AS MONEY) END AS PercentWrites,
            CASE @total_execution_count WHEN 0 THEN 0
                 ELSE CAST((100. * ExecutionCount) / @total_execution_count AS MONEY) END AS PercentExecutions,
            CASE DATEDIFF(mi, PlanCreationTime, LastExecutionTime)
                WHEN 0 THEN 0
                ELSE CAST((1.00 * ExecutionCount / DATEDIFF(mi, PlanCreationTime, LastExecutionTime)) AS MONEY)
            END AS ExecutionsPerMinute
    FROM (
        SELECT  PlanHandle,
                TotalCPU,
                TotalDuration,
                TotalReads,
                TotalWrites,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
        FROM    ##bou_BlitzCacheProcs
        WHERE   PlanHandle IS NOT NULL
		AND SPID = @@SPID
        GROUP BY PlanHandle,
                TotalCPU,
                TotalDuration,
                TotalReads,
                TotalWrites,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
    ) AS x
) AS y
WHERE ##bou_BlitzCacheProcs.PlanHandle = y.PlanHandle
      AND ##bou_BlitzCacheProcs.PlanHandle IS NOT NULL
	  AND ##bou_BlitzCacheProcs.SPID = @@SPID
OPTION (RECOMPILE) ;


RAISERROR(N'Gather percentage information from grouped results', 0, 1) WITH NOWAIT;
UPDATE ##bou_BlitzCacheProcs
SET     PercentCPU = y.PercentCPU,
        PercentDuration = y.PercentDuration,
        PercentReads = y.PercentReads,
        PercentWrites = y.PercentWrites,
        PercentExecutions = y.PercentExecutions,
        ExecutionsPerMinute = y.ExecutionsPerMinute,
        /* Strip newlines and tabs. Tabs are replaced with multiple spaces
           so that the later whitespace trim will completely eliminate them
         */
        QueryText = REPLACE(REPLACE(REPLACE(QueryText, @cr, ' '), @lf, ' '), @tab, '  ')
FROM (
    SELECT  DatabaseName,
            SqlHandle,
            QueryHash,
            CASE @total_cpu WHEN 0 THEN 0
                 ELSE CAST((100. * TotalCPU) / @total_cpu AS MONEY) END AS PercentCPU,
            CASE @total_duration WHEN 0 THEN 0
                 ELSE CAST((100. * TotalDuration) / @total_duration AS MONEY) END AS PercentDuration,
            CASE @total_reads WHEN 0 THEN 0
                 ELSE CAST((100. * TotalReads) / @total_reads AS MONEY) END AS PercentReads,
            CASE @total_writes WHEN 0 THEN 0
                 ELSE CAST((100. * TotalWrites) / @total_writes AS MONEY) END AS PercentWrites,
            CASE @total_execution_count WHEN 0 THEN 0
                 ELSE CAST((100. * ExecutionCount) / @total_execution_count AS MONEY) END AS PercentExecutions,
            CASE  DATEDIFF(mi, PlanCreationTime, LastExecutionTime)
                WHEN 0 THEN 0
                ELSE CAST((1.00 * ExecutionCount / DATEDIFF(mi, PlanCreationTime, LastExecutionTime)) AS MONEY)
            END AS ExecutionsPerMinute
    FROM (
        SELECT  DatabaseName,
                SqlHandle,
                QueryHash,
                TotalCPU,
                TotalDuration,
                TotalReads,
                TotalWrites,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
        FROM    ##bou_BlitzCacheProcs
		WHERE SPID = @@SPID
        GROUP BY DatabaseName,
                SqlHandle,
                QueryHash,
                TotalCPU,
                TotalDuration,
                TotalReads,
                TotalWrites,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
    ) AS x
) AS y
WHERE   ##bou_BlitzCacheProcs.SqlHandle = y.SqlHandle
        AND ##bou_BlitzCacheProcs.QueryHash = y.QueryHash
        AND ##bou_BlitzCacheProcs.DatabaseName = y.DatabaseName
        AND ##bou_BlitzCacheProcs.PlanHandle IS NULL
OPTION (RECOMPILE) ;



/* Testing using XML nodes to speed up processing */
RAISERROR(N'Begin XML nodes processing', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
		PlanHandle,
        q.n.query('.') AS statement
INTO    #statements
FROM    ##bou_BlitzCacheProcs p
        CROSS APPLY p.QueryPlan.nodes('//p:StmtSimple') AS q(n) 
WHERE p.SPID = @@SPID
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements
SELECT  QueryHash ,
        SqlHandle ,
		PlanHandle,
        q.n.query('.') AS statement
FROM    ##bou_BlitzCacheProcs p
        CROSS APPLY p.QueryPlan.nodes('//p:StmtCursor') AS q(n) 
WHERE p.SPID = @@SPID
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
        q.n.query('.') AS query_plan
INTO    #query_plan
FROM    #statements p
        CROSS APPLY p.statement.nodes('//p:QueryPlan') AS q(n) 
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
        q.n.query('.') AS relop
INTO    #relop
FROM    #query_plan p
        CROSS APPLY p.query_plan.nodes('//p:RelOp') AS q(n) 
OPTION (RECOMPILE) ;

-- high level plan stuff
RAISERROR(N'Gathering high level plan information', 0, 1) WITH NOWAIT;
UPDATE  ##bou_BlitzCacheProcs
SET     NumberOfDistinctPlans = distinct_plan_count,
        NumberOfPlans = number_of_plans ,
        plan_multiple_plans = CASE WHEN distinct_plan_count < number_of_plans THEN 1 END
FROM (
        SELECT  COUNT(DISTINCT QueryHash) AS distinct_plan_count,
                COUNT(QueryHash) AS number_of_plans,
                QueryHash
        FROM    ##bou_BlitzCacheProcs
		WHERE SPID = @@SPID
        GROUP BY QueryHash
) AS x
WHERE ##bou_BlitzCacheProcs.QueryHash = x.QueryHash
OPTION (RECOMPILE) ;

-- query level checks
RAISERROR(N'Performing query level checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##bou_BlitzCacheProcs
SET     missing_index_count = query_plan.value('count(//p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int') ,
		unmatched_index_count = CASE WHEN is_trivial <> 1 THEN query_plan.value('count(//p:QueryPlan/p:UnmatchedIndexes/p:Parameterization/p:Object)', 'int') END ,
        SerialDesiredMemory = query_plan.value('sum(//p:QueryPlan/p:MemoryGrantInfo/@SerialDesiredMemory)', 'float') ,
        SerialRequiredMemory = query_plan.value('sum(//p:QueryPlan/p:MemoryGrantInfo/@SerialRequiredMemory)', 'float'),
        CachedPlanSize = query_plan.value('sum(//p:QueryPlan/@CachedPlanSize)', 'float') ,
        CompileTime = query_plan.value('sum(//p:QueryPlan/@CompileTime)', 'float') ,
        CompileCPU = query_plan.value('sum(//p:QueryPlan/@CompileCPU)', 'float') ,
        CompileMemory = query_plan.value('sum(//p:QueryPlan/@CompileMemory)', 'float'),
		MaxCompileMemory = query_plan.value('sum(//p:QueryPlan/p:OptimizerHardwareDependentProperties/@MaxCompileMemory)', 'float')
FROM    #query_plan qp
WHERE   qp.QueryHash = ##bou_BlitzCacheProcs.QueryHash
AND     qp.SqlHandle = ##bou_BlitzCacheProcs.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

-- statement level checks
RAISERROR(N'Performing compile timeout checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_timeout = 1 
FROM    #statements s
JOIN ##bou_BlitzCacheProcs b
ON  s.QueryHash = b.QueryHash
AND SPID = @@SPID
WHERE statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
OPTION (RECOMPILE);

RAISERROR(N'Performing compile memory limit exceeded checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_memory_limit_exceeded = 1 
FROM    #statements s
JOIN ##bou_BlitzCacheProcs b
ON  s.QueryHash = b.QueryHash
AND SPID = @@SPID
WHERE statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing unparameterized query checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
unparameterized_query AS (
	SELECT s.QueryHash,
		   unparameterized_query = CASE WHEN statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 1 AND
                                             statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList/p:ColumnReference') = 0 THEN 1
                                        WHEN statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 0 AND
                                             statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/*/p:RelOp/descendant::p:ScalarOperator/p:Identifier/p:ColumnReference[contains(@Column, "@")]') = 1 THEN 1
                                   END
	FROM #statements AS s
			)
UPDATE b
SET b.unparameterized_query = u.unparameterized_query
FROM ##bou_BlitzCacheProcs b
JOIN unparameterized_query u
ON  u.QueryHash = b.QueryHash
AND SPID = @@SPID
WHERE u.unparameterized_query = 1
OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing index DML checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
index_dml AS (
	SELECT	s.QueryHash,	
			index_dml = CASE WHEN statement.exist('//p:StmtSimple/@StatementType[.="CREATE INDEX"]') = 1 THEN 1
							 WHEN statement.exist('//p:StmtSimple/@StatementType[.="DROP INDEX"]') = 1 THEN 1
						END
	FROM    #statements s
			)
	UPDATE b
		SET b.index_dml = i.index_dml
	FROM ##bou_BlitzCacheProcs AS b
	JOIN index_dml i
	ON i.QueryHash = b.QueryHash
	WHERE i.index_dml = 1
	AND b.SPID = @@SPID
	OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing table DML checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
table_dml AS (
	SELECT s.QueryHash,			
		   table_dml = CASE WHEN statement.exist('//p:StmtSimple/@StatementType[.="CREATE TABLE"]') = 1 THEN 1
							WHEN statement.exist('//p:StmtSimple/@StatementType[.="DROP OBJECT"]') = 1 THEN 1
							END
		 FROM #statements AS s
		 )
	UPDATE b
		SET b.table_dml = t.table_dml
	FROM ##bou_BlitzCacheProcs AS b
	JOIN table_dml t
	ON t.QueryHash = b.QueryHash
	WHERE t.table_dml = 1
	AND b.SPID = @@SPID
	OPTION (RECOMPILE);
END; 


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Gathering row estimates', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
INSERT INTO #est_rows
SELECT DISTINCT 
		CONVERT(BINARY(8), RIGHT('0000000000000000' + SUBSTRING(c.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18), 16), 2) AS QueryHash,
		c.n.value('(/p:StmtSimple/@StatementEstRows)[1]', 'FLOAT') AS estimated_rows
FROM   #statements AS s
CROSS APPLY s.statement.nodes('/p:StmtSimple') AS c(n)
WHERE  c.n.exist('/p:StmtSimple[@StatementEstRows > 0]') = 1;

	UPDATE b
		SET b.estimated_rows = er.estimated_rows
	FROM ##bou_BlitzCacheProcs AS b
	JOIN #est_rows er
	ON er.QueryHash = b.QueryHash
	WHERE b.SPID = @@SPID
	AND b.QueryType = 'Statement'
	OPTION (RECOMPILE);
END;

RAISERROR(N'Gathering trivial plans', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
UPDATE b
SET b.is_trivial = 1
FROM ##bou_BlitzCacheProcs AS b
JOIN (
SELECT  s.SqlHandle
FROM    #statements AS s
JOIN    (   SELECT  r.SqlHandle
            FROM    #relop AS r
            WHERE   r.relop.exist('//p:RelOp[contains(@LogicalOp, "Scan")]') = 1 ) AS r
    ON r.SqlHandle = s.SqlHandle
WHERE   s.statement.exist('//p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1
) AS s
ON b.SqlHandle = s.SqlHandle
OPTION (RECOMPILE);


--Gather costs
RAISERROR(N'Gathering statement costs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #plan_cost ( QueryPlanCost, SqlHandle, PlanHandle, QueryHash, QueryPlanHash )
SELECT  DISTINCT
		statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') QueryPlanCost,
		s.SqlHandle,
		s.PlanHandle,
		CONVERT(BINARY(8), RIGHT('0000000000000000' + SUBSTRING(q.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18), 16), 2) AS QueryHash,
		CONVERT(BINARY(8), RIGHT('0000000000000000' + SUBSTRING(q.n.value('@QueryPlanHash', 'VARCHAR(18)'), 3, 18), 16), 2) AS QueryPlanHash
FROM #statements s
CROSS APPLY s.statement.nodes('/p:StmtSimple') AS q(n)
WHERE statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') > 0
OPTION (RECOMPILE);

RAISERROR(N'Updating statement costs', 0, 1) WITH NOWAIT;
WITH pc AS (
	SELECT SUM(DISTINCT pc.QueryPlanCost) AS QueryPlanCostSum, pc.QueryHash, pc.QueryPlanHash, pc.SqlHandle, pc.PlanHandle
	FROM #plan_cost AS pc
	GROUP BY pc.QueryHash, pc.QueryPlanHash, pc.SqlHandle, pc.PlanHandle
)
	UPDATE b
		SET b.QueryPlanCost = ISNULL(pc.QueryPlanCostSum, 0)
		FROM pc
		JOIN ##bou_BlitzCacheProcs b
		ON b.SqlHandle = pc.SqlHandle
		AND b.QueryHash = pc.QueryHash
		WHERE b.QueryType NOT LIKE '%Procedure%'
	OPTION (RECOMPILE);

IF EXISTS (
SELECT 1
FROM ##bou_BlitzCacheProcs AS b
WHERE b.QueryType LIKE 'Procedure%'
)

BEGIN

RAISERROR(N'Gathering stored procedure costs', 0, 1) WITH NOWAIT;
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, QueryCost AS (
  SELECT
	DISTINCT
    statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') AS SubTreeCost,
    s.PlanHandle,
	s.SqlHandle
  FROM #statements AS s
  WHERE PlanHandle IS NOT NULL
)
, QueryCostUpdate AS (
  SELECT
	SUM(qc.SubTreeCost) OVER (PARTITION BY SqlHandle, PlanHandle) PlanTotalQuery,
    qc.PlanHandle,
    qc.SqlHandle
  FROM QueryCost qc
)
INSERT INTO #proc_costs
SELECT qcu.PlanTotalQuery, PlanHandle, SqlHandle
FROM QueryCostUpdate AS qcu
OPTION (RECOMPILE);


UPDATE b
    SET b.QueryPlanCost = ca.PlanTotalQuery
FROM ##bou_BlitzCacheProcs AS b
CROSS APPLY (
		SELECT TOP 1 PlanTotalQuery 
		FROM #proc_costs qcu 
		WHERE qcu.PlanHandle = b.PlanHandle 
		ORDER BY PlanTotalQuery DESC
) ca
WHERE b.QueryType LIKE 'Procedure%'
AND b.SPID = @@SPID
OPTION (RECOMPILE);

END;

UPDATE b
SET b.QueryPlanCost = 0.0
FROM ##bou_BlitzCacheProcs b
WHERE b.QueryPlanCost IS NULL
AND b.SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Checking for plan warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##bou_BlitzCacheProcs
SET plan_warnings = 1
FROM    #query_plan qp
WHERE   qp.SqlHandle = ##bou_BlitzCacheProcs.SqlHandle
AND SPID = @@SPID
AND query_plan.exist('/p:QueryPlan/p:Warnings') = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking for implicit conversion', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##bou_BlitzCacheProcs
SET implicit_conversions = 1
FROM    #query_plan qp
WHERE   qp.SqlHandle = ##bou_BlitzCacheProcs.SqlHandle
AND SPID = @@SPID
AND query_plan.exist('/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') = 1
OPTION (RECOMPILE);

-- operator level checks
IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing busy loops checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE p
SET    busy_loops = CASE WHEN (x.estimated_executions / 100.0) > x.estimated_rows THEN 1 END 
FROM   ##bou_BlitzCacheProcs p
       JOIN (
            SELECT qs.SqlHandle,
                   relop.value('sum(/p:RelOp/@EstimateRows)', 'float') AS estimated_rows ,
                   relop.value('sum(/p:RelOp/@EstimateRewinds)', 'float') + relop.value('sum(/p:RelOp/@EstimateRebinds)', 'float') + 1.0 AS estimated_executions 
            FROM   #relop qs
       ) AS x ON p.SqlHandle = x.SqlHandle
WHERE SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Performing TVF join check', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE p
SET    p.tvf_join = CASE WHEN x.tvf_join = 1 THEN 1 END
FROM   ##bou_BlitzCacheProcs p
       JOIN (
			SELECT r.SqlHandle,
				   1 AS tvf_join
			FROM #relop AS r
			WHERE r.relop.exist('//p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
			AND   r.relop.exist('//p:RelOp[contains(@LogicalOp, "Join")]') = 1
       ) AS x ON p.SqlHandle = x.SqlHandle
WHERE SPID = @@SPID
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for operator warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.SqlHandle,
	   c.n.exist('//p:Warnings[(@NoJoinPredicate[.="1"])]') AS warning_no_join_predicate,
	   c.n.exist('//p:ColumnsWithNoStatistics') AS no_stats_warning ,
	   c.n.exist('//p:Warnings') AS relop_warnings
FROM #relop AS r
CROSS APPLY r.relop.nodes('/p:RelOp/p:Warnings') AS c(n)
)
UPDATE p
SET	   p.warning_no_join_predicate = x.warning_no_join_predicate,
	   p.no_stats_warning = x.no_stats_warning,
	   p.relop_warnings = x.relop_warnings
FROM ##bou_BlitzCacheProcs AS p
JOIN x ON x.SqlHandle = p.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Checking for table variables', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.SqlHandle,
	   c.n.value('substring(@Table, 2, 1)','VARCHAR(100)') AS first_char
FROM   #relop r
CROSS APPLY r.relop.nodes('//p:Object') AS c(n)
)
UPDATE p
SET	   is_table_variable = 1
FROM ##bou_BlitzCacheProcs AS p
JOIN x ON x.SqlHandle = p.SqlHandle
AND SPID = @@SPID
WHERE x.first_char = '@'
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for functions', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT qs.SqlHandle,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS function_count,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[@IsClrFunction = "1"]))', 'INT') AS clr_function_count
FROM   #relop qs
CROSS APPLY relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
)
UPDATE p
SET	   p.function_count = x.function_count,
	   p.clr_function_count = x.clr_function_count
FROM ##bou_BlitzCacheProcs AS p
JOIN x ON x.SqlHandle = p.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Checking for expensive key lookups', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET key_lookup_cost = x.key_lookup_cost
FROM (
SELECT 
       qs.SqlHandle,
	   MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS key_lookup_cost
FROM   #relop qs
WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
GROUP BY qs.SqlHandle
) AS x
WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);


RAISERROR(N'Checking for expensive remote queries', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET remote_query_cost = x.remote_query_cost
FROM (
SELECT 
       qs.SqlHandle,
	   MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS remote_query_cost
FROM   #relop qs
WHERE [relop].exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
GROUP BY qs.SqlHandle
) AS x
WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Checking for expensive sorts', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET sort_cost = y.max_sort_cost 
FROM (
	SELECT x.SqlHandle, MAX((x.sort_io + x.sort_cpu)) AS max_sort_cost
	FROM (
		SELECT 
		       qs.SqlHandle,
			   relop.value('sum(/p:RelOp/@EstimateIO)', 'float') AS sort_io,
			   relop.value('sum(/p:RelOp/@EstimateCPU)', 'float') AS sort_cpu
		FROM   #relop qs
		WHERE [relop].exist('/p:RelOp[(@PhysicalOp[.="Sort"])]') = 1
		) AS x
	GROUP BY x.SqlHandle
	) AS y
WHERE ##bou_BlitzCacheProcs.SqlHandle = y.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Checking for Optimistic cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_optimistic_cursor =  1
FROM ##bou_BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking if cursor is Forward Only', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_forward_only_cursor = 1
FROM ##bou_BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking if cursor is Fast Forward', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_fast_forward_cursor = 1
FROM ##bou_BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorActualType[.="FastForward"]') = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking for Dynamic cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_cursor_dynamic =  1
FROM ##bou_BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorActualType[.="Dynamic"]') = 1
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for bad scans and plan forcing', 0, 1) WITH NOWAIT;
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET 
b.is_table_scan = x.is_table_scan,
b.backwards_scan = x.backwards_scan,
b.forced_index = x.forced_index,
b.forced_seek = x.forced_seek,
b.forced_scan = x.forced_scan
FROM ##bou_BlitzCacheProcs b
JOIN (
SELECT 
       qs.SqlHandle,
	   0 AS is_table_scan,
	   q.n.exist('@ScanDirection[.="BACKWARD"]') AS backwards_scan,
	   q.n.value('@ForcedIndex', 'bit') AS forced_index,
	   q.n.value('@ForceSeek', 'bit') AS forced_seek,
	   q.n.value('@ForceScan', 'bit') AS forced_scan
FROM   #relop qs
CROSS APPLY qs.relop.nodes('//p:IndexScan') AS q(n)
UNION ALL
SELECT 
       qs.SqlHandle,
	   1 AS is_table_scan,
	   q.n.exist('@ScanDirection[.="BACKWARD"]') AS backwards_scan,
	   q.n.value('@ForcedIndex', 'bit') AS forced_index,
	   q.n.value('@ForceSeek', 'bit') AS forced_seek,
	   q.n.value('@ForceScan', 'bit') AS forced_scan
FROM   #relop qs
CROSS APPLY qs.relop.nodes('//p:TableScan') AS q(n)
) AS x ON b.SqlHandle = x.SqlHandle
WHERE SPID = @@SPID
OPTION (RECOMPILE);
END; 


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for computed columns that reference scalar UDFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET is_computed_scalar = x.computed_column_function
FROM (
SELECT qs.SqlHandle,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS computed_column_function
FROM   #relop qs
CROSS APPLY relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
WHERE n.fn.exist('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ColumnReference[(@ComputedColumn[.="1"])]') = 1
) AS x
WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Checking for filters that reference scalar UDFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET is_computed_filter = x.filter_function
FROM (
SELECT 
r.SqlHandle, 
c.n.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS filter_function
FROM #relop AS r
CROSS APPLY r.relop.nodes('/p:RelOp/p:Filter/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator/p:UserDefinedFunction') c(n) 
) x
WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking modification queries that hit lots of indexes', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),	
IndexOps AS 
(
	SELECT 
	r.QueryHash,
	c.n.value('@PhysicalOp', 'VARCHAR(100)') AS op_name,
	c.n.exist('@PhysicalOp[.="Index Insert"]') AS ii,
	c.n.exist('@PhysicalOp[.="Index Update"]') AS iu,
	c.n.exist('@PhysicalOp[.="Index Delete"]') AS id,
	c.n.exist('@PhysicalOp[.="Clustered Index Insert"]') AS cii,
	c.n.exist('@PhysicalOp[.="Clustered Index Update"]') AS ciu,
	c.n.exist('@PhysicalOp[.="Clustered Index Delete"]') AS cid,
	c.n.exist('@PhysicalOp[.="Table Insert"]') AS ti,
	c.n.exist('@PhysicalOp[.="Table Update"]') AS tu,
	c.n.exist('@PhysicalOp[.="Table Delete"]') AS td
	FROM #relop AS r
	CROSS APPLY r.relop.nodes('/p:RelOp') c(n)
	OUTER APPLY r.relop.nodes('/p:RelOp/p:ScalarInsert/p:Object') q(n)
	OUTER APPLY r.relop.nodes('/p:RelOp/p:Update/p:Object') o2(n)
	OUTER APPLY r.relop.nodes('/p:RelOp/p:SimpleUpdate/p:Object') o3(n)
), iops AS 
(
		SELECT	ios.QueryHash,
		SUM(CONVERT(TINYINT, ios.ii)) AS index_insert_count,
		SUM(CONVERT(TINYINT, ios.iu)) AS index_update_count,
		SUM(CONVERT(TINYINT, ios.id)) AS index_delete_count,
		SUM(CONVERT(TINYINT, ios.cii)) AS cx_insert_count,
		SUM(CONVERT(TINYINT, ios.ciu)) AS cx_update_count,
		SUM(CONVERT(TINYINT, ios.cid)) AS cx_delete_count,
		SUM(CONVERT(TINYINT, ios.ti)) AS table_insert_count,
		SUM(CONVERT(TINYINT, ios.tu)) AS table_update_count,
		SUM(CONVERT(TINYINT, ios.td)) AS table_delete_count
		FROM IndexOps AS ios
		WHERE ios.op_name IN ('Index Insert', 'Index Delete', 'Index Update', 
							  'Clustered Index Insert', 'Clustered Index Delete', 'Clustered Index Update', 
							  'Table Insert', 'Table Delete', 'Table Update')
		GROUP BY ios.QueryHash) 
UPDATE b
SET b.index_insert_count = iops.index_insert_count,
	b.index_update_count = iops.index_update_count,
	b.index_delete_count = iops.index_delete_count,
	b.cx_insert_count = iops.cx_insert_count,
	b.cx_update_count = iops.cx_update_count,
	b.cx_delete_count = iops.cx_delete_count,
	b.table_insert_count = iops.table_insert_count,
	b.table_update_count = iops.table_update_count,
	b.table_delete_count = iops.table_delete_count
FROM ##bou_BlitzCacheProcs AS b
JOIN iops ON  iops.QueryHash = b.QueryHash
WHERE SPID = @@SPID
OPTION (RECOMPILE);
END; 


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for Spatial index use', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET is_spatial = x.is_spatial
FROM (
SELECT qs.SqlHandle,
	   1 AS is_spatial
FROM   #relop qs
CROSS APPLY relop.nodes('/p:RelOp//p:Object') n(fn)
WHERE n.fn.exist('(@IndexKind[.="Spatial"])') = 1
) AS x
WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR('Checking for wonky Index Spools', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES (
    'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
, selects
AS ( SELECT s.QueryHash
     FROM   #statements AS s
     WHERE  s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 )
, spools
AS ( SELECT DISTINCT r.QueryHash,
	   c.n.value('@EstimateRows', 'FLOAT') AS estimated_rows,
       c.n.value('@EstimateIO', 'FLOAT') AS estimated_io,
       c.n.value('@EstimateCPU', 'FLOAT') AS estimated_cpu,
       c.n.value('@EstimateRebinds', 'FLOAT') AS estimated_rebinds
FROM   #relop AS r
JOIN   selects AS s
ON s.QueryHash = r.QueryHash
CROSS APPLY r.relop.nodes('/p:RelOp') AS c(n)
WHERE  r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1
)
UPDATE b
		SET b.index_spool_rows = sp.estimated_rows,
			b.index_spool_cost = ((sp.estimated_io * sp.estimated_cpu) * CASE WHEN sp.estimated_rebinds < 1 THEN 1 ELSE sp.estimated_rebinds END)
FROM ##bou_BlitzCacheProcs b
JOIN spools sp
ON sp.QueryHash = b.QueryHash
OPTION (RECOMPILE);


/* 2012+ only */
IF @v >= 11
BEGIN

	RAISERROR(N'Checking for forced serialization', 0, 1) WITH NOWAIT;
	WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
	UPDATE  ##bou_BlitzCacheProcs
	SET is_forced_serial = 1
	FROM    #query_plan qp
	WHERE   qp.SqlHandle = ##bou_BlitzCacheProcs.SqlHandle
	AND SPID = @@SPID
	AND query_plan.exist('/p:QueryPlan/@NonParallelPlanReason') = 1
	AND (##bou_BlitzCacheProcs.is_parallel = 0 OR ##bou_BlitzCacheProcs.is_parallel IS NULL)
	OPTION (RECOMPILE);
	
	IF @ExpertMode > 0
	BEGIN
	RAISERROR(N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) WITH NOWAIT;
	WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
	UPDATE ##bou_BlitzCacheProcs
	SET columnstore_row_mode = x.is_row_mode
	FROM (
	SELECT 
	       qs.SqlHandle,
		   relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') AS is_row_mode
	FROM   #relop qs
	WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
	) AS x
	WHERE ##bou_BlitzCacheProcs.SqlHandle = x.SqlHandle
	AND SPID = @@SPID
	OPTION (RECOMPILE);
	END; 

END;

/* 2014+ only */
IF @v >= 12
BEGIN
    RAISERROR('Checking for downlevel cardinality estimators being used on SQL Server 2014.', 0, 1) WITH NOWAIT;

    WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
    UPDATE  p
    SET     downlevel_estimator = CASE WHEN statement.value('min(//p:StmtSimple/@CardinalityEstimationModelVersion)', 'int') < (@v * 10) THEN 1 END
    FROM    ##bou_BlitzCacheProcs p
            JOIN #statements s ON p.QueryHash = s.QueryHash 
	WHERE SPID = @@SPID
	OPTION (RECOMPILE);
END ;

/* 2016+ only */
IF @v >= 13 AND @ExpertMode > 0
BEGIN
    RAISERROR('Checking for row level security in 2016 only', 0, 1) WITH NOWAIT;

    WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
    UPDATE  p
    SET     p.is_row_level = 1
    FROM    ##bou_BlitzCacheProcs p
            JOIN #statements s ON p.QueryHash = s.QueryHash 
	WHERE SPID = @@SPID
	AND statement.exist('/p:StmtSimple/@SecurityPolicyApplied[.="true"]') = 1
	OPTION (RECOMPILE);
END ;

/* 2017+ only */
IF @v >= 14 OR	(@v = 13 AND @build >= 5026)
BEGIN

IF @ExpertMode > 0
BEGIN
RAISERROR('Gathering stats information', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT INTO #stats_agg
SELECT qp.SqlHandle,
	   x.c.value('@LastUpdate', 'DATETIME2(7)') AS LastUpdate,
	   x.c.value('@ModificationCount', 'INT') AS ModificationCount,
	   x.c.value('@SamplingPercent', 'FLOAT') AS SamplingPercent,
	   x.c.value('@Statistics', 'NVARCHAR(258)') AS [Statistics], 
	   x.c.value('@Table', 'NVARCHAR(258)') AS [Table], 
	   x.c.value('@Schema', 'NVARCHAR(258)') AS [Schema], 
	   x.c.value('@Database', 'NVARCHAR(258)') AS [Database]
FROM #query_plan AS qp
CROSS APPLY qp.query_plan.nodes('//p:OptimizerStatsUsage/p:StatisticsInfo') x (c)
OPTION (RECOMPILE);


RAISERROR('Checking for stale stats', 0, 1) WITH NOWAIT;
WITH  stale_stats AS (
	SELECT sa.SqlHandle
	FROM #stats_agg AS sa
	GROUP BY sa.SqlHandle
	HAVING MAX(sa.LastUpdate) <= DATEADD(DAY, -7, SYSDATETIME())
	AND AVG(sa.ModificationCount) >= 100000
)
UPDATE b
SET stale_stats = 1
FROM ##bou_BlitzCacheProcs b
JOIN stale_stats os
ON b.SqlHandle = os.SqlHandle
AND b.SPID = @@SPID
OPTION (RECOMPILE);
END;

IF @v >= 14 AND @ExpertMode > 0
BEGIN
RAISERROR('Checking for adaptive joins', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
aj AS (
	SELECT 
			SqlHandle
	FROM #relop AS r
	CROSS APPLY r.relop.nodes('//p:RelOp') x(c)
	WHERE x.c.exist('@IsAdaptive[.=1]') = 1
)
UPDATE b
SET b.is_adaptive = 1
FROM ##bou_BlitzCacheProcs b
JOIN aj
ON b.SqlHandle = aj.SqlHandle
AND b.SPID = @@SPID
OPTION (RECOMPILE);
END; 

IF @ExpertMode > 0
BEGIN;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
row_goals AS(
SELECT qs.QueryHash
FROM   #relop qs
WHERE relop.value('sum(/p:RelOp/@EstimateRowsWithoutRowGoal)', 'float') > 0
)
UPDATE b
SET b.is_row_goal = 1
FROM ##bou_BlitzCacheProcs b
JOIN row_goals
ON b.QueryHash = row_goals.QueryHash
AND b.SPID = @@SPID
OPTION (RECOMPILE);
END ;

END;


/* END Testing using XML nodes to speed up processing */
RAISERROR(N'Gathering additional plan level information', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET NumberOfDistinctPlans = distinct_plan_count,
    NumberOfPlans = number_of_plans,
    plan_multiple_plans = CASE WHEN distinct_plan_count < number_of_plans THEN 1 END 
FROM (
SELECT COUNT(DISTINCT QueryHash) AS distinct_plan_count,
       COUNT(QueryHash) AS number_of_plans,
       QueryHash
FROM   ##bou_BlitzCacheProcs
WHERE SPID = @@SPID
GROUP BY QueryHash
) AS x
WHERE ##bou_BlitzCacheProcs.QueryHash = x.QueryHash 
OPTION (RECOMPILE);

/* Update to grab stored procedure name for individual statements */
RAISERROR(N'Attempting to get stored procedure name for individual statements', 0, 1) WITH NOWAIT;
UPDATE  p
SET     QueryType = QueryType + ' (parent ' +
                    + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
                    + '.'
                    + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + ')'
FROM    ##bou_BlitzCacheProcs p
        JOIN sys.dm_exec_procedure_stats s ON p.SqlHandle = s.sql_handle
WHERE   QueryType = 'Statement'
AND SPID = @@SPID
OPTION (RECOMPILE);

/* Trace Flag Checks 2012 SP3, 2014 SP2 and 2016 SP1 only)*/
IF @v >= 11
BEGIN

RAISERROR(N'Trace flag checks', 0, 1) WITH NOWAIT;
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, tf_pretty AS (
SELECT  qp.QueryHash,
		qp.SqlHandle,
		q.n.value('@Value', 'INT') AS trace_flag,
		q.n.value('@Scope', 'VARCHAR(10)') AS scope
FROM    #query_plan qp
CROSS APPLY qp.query_plan.nodes('/p:QueryPlan/p:TraceFlags/p:TraceFlag') AS q(n)
)
INSERT INTO #trace_flags
SELECT DISTINCT tf1.SqlHandle , tf1.QueryHash,
    STUFF((
          SELECT DISTINCT ', ' + CONVERT(VARCHAR(5), tf2.trace_flag)
          FROM  tf_pretty AS tf2 
          WHERE tf1.SqlHandle = tf2.SqlHandle 
		  AND tf1.QueryHash = tf2.QueryHash
		  AND tf2.scope = 'Global'
        FOR XML PATH(N'')), 1, 2, N''
      ) AS global_trace_flags,
    STUFF((
          SELECT DISTINCT ', ' + CONVERT(VARCHAR(5), tf2.trace_flag)
          FROM  tf_pretty AS tf2 
          WHERE tf1.SqlHandle = tf2.SqlHandle 
		  AND tf1.QueryHash = tf2.QueryHash
		  AND tf2.scope = 'Session'
        FOR XML PATH(N'')), 1, 2, N''
      ) AS session_trace_flags
FROM tf_pretty AS tf1
OPTION (RECOMPILE);

UPDATE p
SET    p.trace_flags_session = tf.session_trace_flags
FROM   ##bou_BlitzCacheProcs p
JOIN #trace_flags tf ON tf.QueryHash = p.QueryHash 
WHERE SPID = @@SPID
OPTION (RECOMPILE);

END;


RAISERROR(N'Checking for MSTVFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_mstvf = 1
FROM #relop AS r
JOIN ##bou_BlitzCacheProcs AS b
ON b.SqlHandle = r.SqlHandle
WHERE  r.relop.exist('/p:RelOp[(@EstimateRows="100" or @EstimateRows="1") and @LogicalOp="Table-valued function"]') = 1
OPTION (RECOMPILE);


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for many to many merge joins', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_mm_join = 1
FROM #relop AS r
JOIN ##bou_BlitzCacheProcs AS b
ON b.SqlHandle = r.SqlHandle
WHERE  r.relop.exist('/p:RelOp/p:Merge/@ManyToMany[.="1"]') = 1
OPTION (RECOMPILE);
END ;


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Is Paul White Electric?', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
is_paul_white_electric AS (
SELECT 1 AS [is_paul_white_electric], 
r.SqlHandle
FROM #relop AS r
CROSS APPLY r.relop.nodes('//p:RelOp') c(n)
WHERE c.n.exist('@PhysicalOp[.="Switch"]') = 1
)
UPDATE b
SET    b.is_paul_white_electric = ipwe.is_paul_white_electric
FROM   ##bou_BlitzCacheProcs AS b
JOIN is_paul_white_electric ipwe 
ON ipwe.SqlHandle = b.SqlHandle 
WHERE b.SPID = @@SPID
OPTION (RECOMPILE);
END ;


RAISERROR(N'Checking for non-sargable predicates', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
, nsarg
    AS (   SELECT       r.QueryHash, 1 AS fn, 0 AS jo, 0 AS lk
           FROM         #relop AS r
           CROSS APPLY  r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator') AS ca(x)
           WHERE        (   ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName') = 1
                            OR     ca.x.exist('//p:ScalarOperator/p:IF') = 1 )
           UNION ALL
           SELECT       r.QueryHash, 0 AS fn, 1 AS jo, 0 AS lk
           FROM         #relop AS r
           CROSS APPLY  r.relop.nodes('/p:RelOp//p:ScalarOperator') AS ca(x)
           WHERE        r.relop.exist('/p:RelOp[contains(@LogicalOp, "Join")]') = 1
                        AND ca.x.exist('//p:ScalarOperator[contains(@ScalarString, "Expr")]') = 1
           UNION ALL
           SELECT       r.QueryHash, 0 AS fn, 0 AS jo, 1 AS lk
           FROM         #relop AS r
           CROSS APPLY  r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator') AS ca(x)
           CROSS APPLY  ca.x.nodes('//p:Const') AS co(x)
           WHERE        ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName[.="like"]') = 1
                        AND (   (   co.x.value('substring(@ConstValue, 1, 1)', 'VARCHAR(100)') <> 'N'
                                    AND co.x.value('substring(@ConstValue, 2, 1)', 'VARCHAR(100)') = '%' )
                                OR (   co.x.value('substring(@ConstValue, 1, 1)', 'VARCHAR(100)') = 'N'
                                       AND co.x.value('substring(@ConstValue, 3, 1)', 'VARCHAR(100)') = '%' ))),
  d_nsarg
    AS (   SELECT   DISTINCT
                    nsarg.QueryHash
           FROM     nsarg
           WHERE    nsarg.fn = 1
                    OR nsarg.jo = 1
                    OR nsarg.lk = 1 )
UPDATE  b
SET     b.is_nonsargable = 1
FROM    d_nsarg AS d
JOIN    ##bou_BlitzCacheProcs AS b
    ON b.QueryHash = d.QueryHash
WHERE   b.SPID = @@SPID
OPTION ( RECOMPILE );

/*Begin implicit conversion and parameter info */

RAISERROR(N'Getting information about implicit conversions and stored proc parameters', 0, 1) WITH NOWAIT;

RAISERROR(N'Getting variable info', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
INSERT #variable_info ( SPID, QueryHash, SqlHandle, proc_name, variable_name, variable_datatype, compile_time_value )
SELECT      DISTINCT @@SPID,
            qp.QueryHash,
            qp.SqlHandle,
            b.QueryType AS proc_name,
            q.n.value('@Column', 'NVARCHAR(258)') AS variable_name,
            q.n.value('@ParameterDataType', 'NVARCHAR(258)') AS variable_datatype,
            q.n.value('@ParameterCompiledValue', 'NVARCHAR(258)') AS compile_time_value
FROM        #query_plan AS qp
JOIN        ##bou_BlitzCacheProcs AS b
ON (b.QueryType = 'adhoc' AND b.QueryHash = qp.QueryHash)
OR 	(b.QueryType <> 'adhoc' AND b.SqlHandle = qp.SqlHandle)
CROSS APPLY qp.query_plan.nodes('//p:QueryPlan/p:ParameterList/p:ColumnReference') AS q(n)
WHERE  b.SPID = @@SPID
OPTION (RECOMPILE);


RAISERROR(N'Getting conversion info', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
INSERT #conversion_info ( SPID, QueryHash, SqlHandle, proc_name, expression )
SELECT      DISTINCT @@SPID,
            qp.QueryHash,
            qp.SqlHandle,
            b.QueryType AS proc_name,
            qq.c.value('@Expression', 'NVARCHAR(4000)') AS expression
FROM        #query_plan AS qp
JOIN        ##bou_BlitzCacheProcs AS b
ON (b.QueryType = 'adhoc' AND b.QueryHash = qp.QueryHash)
OR 	(b.QueryType <> 'adhoc' AND b.SqlHandle = qp.SqlHandle)
CROSS APPLY qp.query_plan.nodes('//p:QueryPlan/p:Warnings/p:PlanAffectingConvert') AS qq(c)
WHERE       qq.c.exist('@ConvertIssue[.="Seek Plan"]') = 1
            AND qp.QueryHash IS NOT NULL
            AND b.implicit_conversions = 1
AND b.SPID = @@SPID
OPTION (RECOMPILE);


RAISERROR(N'Parsing conversion info', 0, 1) WITH NOWAIT;
INSERT #stored_proc_info ( SPID, SqlHandle, QueryHash, proc_name, variable_name, variable_datatype, converted_column_name, column_name, converted_to, compile_time_value )
SELECT @@SPID AS SPID,
       ci.SqlHandle,
       ci.QueryHash,
       REPLACE(REPLACE(REPLACE(ci.proc_name, ')', ''), 'Statement (parent ', ''), 'Procedure or Function: ', '') AS proc_name,
       CASE WHEN ci.at_charindex > 0
                 AND ci.bracket_charindex > 0 
			THEN SUBSTRING(ci.expression, ci.at_charindex, ci.bracket_charindex)
            ELSE N'**no_variable**'
       END AS variable_name,
	   N'**no_variable**' AS variable_datatype,
       CASE WHEN ci.at_charindex = 0
                 AND ci.comma_charindex > 0
                 AND ci.second_comma_charindex > 0 
			THEN SUBSTRING(ci.expression, ci.comma_charindex, ci.second_comma_charindex)
            ELSE N'**no_column**'
       END AS converted_column_name,
       CASE WHEN ci.at_charindex = 0
                 AND ci.equal_charindex > 0 
				 AND ci.convert_implicit_charindex = 0
			THEN SUBSTRING(ci.expression, ci.equal_charindex, 4000)
			WHEN ci.at_charindex = 0
                 AND ci.equal_charindex > 0 
				 AND ci.convert_implicit_charindex > 0
			THEN SUBSTRING(ci.expression, 0, ci.equal_charindex -1)
            WHEN ci.at_charindex > 0
                 AND ci.comma_charindex > 0
                 AND ci.second_comma_charindex > 0 
			THEN SUBSTRING(ci.expression, ci.comma_charindex, ci.second_comma_charindex)
            ELSE N'**no_column **'
       END AS column_name,
       CASE WHEN ci.paren_charindex > 0
                 AND ci.comma_paren_charindex > 0 
			THEN SUBSTRING(ci.expression, ci.paren_charindex, ci.comma_paren_charindex)
       END AS converted_to,
       CASE WHEN ci.at_charindex = 0
                 AND ci.convert_implicit_charindex = 0
                 AND ci.proc_name = 'Statement' 
			THEN SUBSTRING(ci.expression, ci.equal_charindex, 4000)
            ELSE '**idk_man**'
       END AS compile_time_value
FROM   #conversion_info AS ci
OPTION (RECOMPILE);


RAISERROR(N'Updating variables for inserted procs', 0, 1) WITH NOWAIT;
UPDATE sp
SET sp.variable_datatype = vi.variable_datatype,
	sp.compile_time_value = vi.compile_time_value
FROM   #stored_proc_info AS sp
JOIN #variable_info AS vi
ON (sp.proc_name = 'adhoc' AND sp.QueryHash = vi.QueryHash)
OR 	(sp.proc_name <> 'adhoc' AND sp.SqlHandle = vi.SqlHandle)
AND sp.variable_name = vi.variable_name
OPTION (RECOMPILE);


RAISERROR(N'Inserting variables for other procs', 0, 1) WITH NOWAIT;
INSERT #stored_proc_info 
		( SPID, SqlHandle, QueryHash, variable_name, variable_datatype, compile_time_value, proc_name )
SELECT vi.SPID, vi.SqlHandle, vi.QueryHash, vi.variable_name, vi.variable_datatype, vi.compile_time_value, REPLACE(REPLACE(REPLACE(vi.proc_name, ')', ''), 'Statement (parent ', ''), 'Procedure or Function: ', '') AS proc_name
FROM #variable_info AS vi
WHERE NOT EXISTS
(
	SELECT * 
	FROM   #stored_proc_info AS sp
	WHERE (sp.proc_name = 'adhoc' AND sp.QueryHash = vi.QueryHash)
	OR 	(sp.proc_name <> 'adhoc' AND sp.SqlHandle = vi.SqlHandle)
)
OPTION (RECOMPILE);


RAISERROR(N'Updating procs', 0, 1) WITH NOWAIT;
UPDATE s
SET    s.variable_datatype = CASE WHEN s.variable_datatype LIKE '%(%)%' THEN
                                      LEFT(s.variable_datatype, CHARINDEX('(', s.variable_datatype) - 1)
								  ELSE s.variable_datatype
                             END,
       s.converted_to = CASE WHEN s.converted_to LIKE '%(%)%' THEN
                                 LEFT(s.converted_to, CHARINDEX('(', s.converted_to) - 1)
                             ELSE s.converted_to
                        END,
	   s.compile_time_value = CASE WHEN s.compile_time_value LIKE '%(%)%' THEN
										SUBSTRING(s.compile_time_value, 
													CHARINDEX('(', s.compile_time_value) + 1,
													CHARINDEX(')', s.compile_time_value) - 1
													- CHARINDEX('(', s.compile_time_value)
													)
									WHEN variable_datatype NOT IN ('bit', 'tinyint', 'smallint', 'int', 'bigint') 
										AND s.variable_datatype NOT LIKE '%binary%' 
										AND s.compile_time_value NOT LIKE 'N''%'''
										AND s.compile_time_value NOT LIKE '''%''' THEN
										QUOTENAME(compile_time_value, '''')
									ELSE s.compile_time_value 
							  END
FROM   #stored_proc_info AS s
OPTION (RECOMPILE);


RAISERROR(N'Updating SET options', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE s
SET set_options = set_options.ansi_set_options
FROM #stored_proc_info AS s
JOIN (
		SELECT  x.SqlHandle,
				N'SET ANSI_NULLS ' + CASE WHEN [ANSI_NULLS] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
				N'SET ANSI_PADDING ' + CASE WHEN [ANSI_PADDING] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
				N'SET ANSI_WARNINGS ' + CASE WHEN [ANSI_WARNINGS] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
				N'SET ARITHABORT ' + CASE WHEN [ARITHABORT] = 'true' THEN N'ON ' ELSE N' OFF ' END + NCHAR(10) +
				N'SET CONCAT_NULL_YIELDS_NULL ' + CASE WHEN [CONCAT_NULL_YIELDS_NULL] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
				N'SET NUMERIC_ROUNDABORT ' + CASE WHEN [NUMERIC_ROUNDABORT] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
				N'SET QUOTED_IDENTIFIER ' + CASE WHEN [QUOTED_IDENTIFIER] = 'true' THEN N'ON ' ELSE N'OFF ' + NCHAR(10) END AS [ansi_set_options]
		FROM (
			SELECT
				s.SqlHandle,
				so.o.value('@ANSI_NULLS', 'NVARCHAR(20)') AS [ANSI_NULLS],
				so.o.value('@ANSI_PADDING', 'NVARCHAR(20)') AS [ANSI_PADDING],
				so.o.value('@ANSI_WARNINGS', 'NVARCHAR(20)') AS [ANSI_WARNINGS],
				so.o.value('@ARITHABORT', 'NVARCHAR(20)') AS [ARITHABORT],
				so.o.value('@CONCAT_NULL_YIELDS_NULL', 'NVARCHAR(20)') AS [CONCAT_NULL_YIELDS_NULL],
				so.o.value('@NUMERIC_ROUNDABORT', 'NVARCHAR(20)') AS [NUMERIC_ROUNDABORT],
				so.o.value('@QUOTED_IDENTIFIER', 'NVARCHAR(20)') AS [QUOTED_IDENTIFIER]
			FROM #statements AS s
			CROSS APPLY s.statement.nodes('//p:StatementSetOptions') AS so(o)
		   ) AS x
) AS set_options ON set_options.SqlHandle = s.SqlHandle
OPTION(RECOMPILE);


RAISERROR(N'Updating conversion XML', 0, 1) WITH NOWAIT;
WITH precheck AS (
SELECT spi.SPID,
	   spi.SqlHandle,
	   spi.proc_name,
			(SELECT  
			  CASE WHEN spi.proc_name <> 'Statement' 
				   THEN N'The stored procedure ' + spi.proc_name 
				   ELSE N'This ad hoc statement' 
			  END
			+ N' had the following implicit conversions: '
			+ CHAR(10)
			+ STUFF((
				SELECT DISTINCT 
						@nl
						+ CASE WHEN spi2.variable_name <> N'**no_variable**'
							   THEN N'The variable '
							   WHEN spi2.variable_name = N'**no_variable**' AND (spi2.column_name = spi2.converted_column_name OR spi2.column_name LIKE '%CONVERT_IMPLICIT%')
							   THEN N'The compiled value '
							   WHEN spi2.column_name LIKE '%Expr%'
							   THEN 'The expression '
							   ELSE N'The column '
						  END 
						+ CASE WHEN spi2.variable_name <> N'**no_variable**'
							   THEN spi2.variable_name
							   WHEN spi2.variable_name = N'**no_variable**' AND (spi2.column_name = spi2.converted_column_name OR spi2.column_name LIKE '%CONVERT_IMPLICIT%')
							   THEN spi2.compile_time_value

							   ELSE spi2.column_name
						  END 
						+ N' has a data type of '
						+ CASE WHEN spi2.variable_datatype = N'**no_variable**' THEN spi2.converted_to
							   ELSE spi2.variable_datatype 
						  END
						+ N' which caused implicit conversion on the column '
						+ CASE WHEN spi2.column_name LIKE N'%CONVERT_IMPLICIT%'
							   THEN spi2.converted_column_name
							   WHEN spi2.column_name = N'**no_column**'
							   THEN spi2.converted_column_name
							   WHEN spi2.converted_column_name = N'**no_column**'
							   THEN spi2.column_name
							   WHEN spi2.column_name <> spi2.converted_column_name
							   THEN spi2.converted_column_name
							   ELSE spi2.column_name
						  END
						+ CASE WHEN spi2.variable_name = N'**no_variable**' AND (spi2.column_name = spi2.converted_column_name OR spi2.column_name LIKE '%CONVERT_IMPLICIT%')
							   THEN N''
							   WHEN spi2.column_name LIKE '%Expr%'
							   THEN N''
							   WHEN spi2.compile_time_value NOT IN ('**declared in proc**', '**idk_man**')
							   AND spi2.compile_time_value <> spi2.column_name
							   THEN ' with the value ' + RTRIM(spi2.compile_time_value)
							ELSE N''
						 END 
						+ '.'
				FROM #stored_proc_info AS spi2
				WHERE spi.SqlHandle = spi2.SqlHandle
				FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
			  AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
			AS implicit_conversion_info
FROM #stored_proc_info AS spi
GROUP BY spi.SPID, spi.SqlHandle, spi.proc_name
)
UPDATE b
SET b.implicit_conversion_info = pk.implicit_conversion_info
FROM ##bou_BlitzCacheProcs AS b
JOIN precheck pk
ON pk.SqlHandle = b.SqlHandle
AND pk.SPID = b.SPID
OPTION (RECOMPILE);


RAISERROR(N'Updating cached parameter XML for stored procs', 0, 1) WITH NOWAIT;
WITH precheck AS (
SELECT spi.SPID,
	   spi.SqlHandle,
	   spi.proc_name,
	   (SELECT 
			set_options
			+ @nl
			+ @nl
			+ N'EXEC ' 
			+ spi.proc_name 
			+ N' '
			+ STUFF((
				SELECT DISTINCT N', ' 
						+ CASE WHEN spi2.variable_name <> N'**no_variable**' AND spi2.compile_time_value <> N'**idk_man**'
								THEN spi2.variable_name + N' = '
								ELSE @nl + N' We could not find any cached parameter values for this stored proc. ' 
						  END
						+ CASE WHEN spi2.variable_name = N'**no_variable**' OR spi2.compile_time_value = N'**idk_man**'
							   THEN @nl + N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
							   WHEN spi2.compile_time_value = N'NULL' 
							   THEN spi2.compile_time_value 
							   ELSE RTRIM(spi2.compile_time_value)
						  END
				FROM #stored_proc_info AS spi2
				WHERE spi.SqlHandle = spi2.SqlHandle
				AND spi2.proc_name <> N'Statement'
				FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
			AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
			AS cached_execution_parameters
FROM #stored_proc_info AS spi
GROUP BY spi.SPID, spi.SqlHandle, spi.proc_name, spi.set_options
) 
UPDATE b
SET b.cached_execution_parameters = pk.cached_execution_parameters
FROM ##bou_BlitzCacheProcs AS b
JOIN precheck pk
ON pk.SqlHandle = b.SqlHandle
AND pk.SPID = b.SPID
WHERE b.QueryType <> N'Statement'
OPTION (RECOMPILE);


RAISERROR(N'Updating cached parameter XML for statements', 0, 1) WITH NOWAIT;
WITH precheck AS (
SELECT spi.SPID,
	   spi.SqlHandle,
	   spi.proc_name,
	   (SELECT 
			set_options
			+ @nl
			+ @nl
			+ N' See QueryText column for full query text'
			+ @nl
			+ @nl
			+ STUFF((
				SELECT DISTINCT N', ' 
						+ CASE WHEN spi2.variable_name <> N'**no_variable**' AND spi2.compile_time_value <> N'**idk_man**'
								THEN spi2.variable_name + N' = '
								ELSE @nl + N' We could not find any cached parameter values for this stored proc. ' 
						  END
						+ CASE WHEN spi2.variable_name = N'**no_variable**' OR spi2.compile_time_value = N'**idk_man**'
							   THEN @nl + N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
							   WHEN spi2.compile_time_value = N'NULL' 
							   THEN spi2.compile_time_value 
							   ELSE RTRIM(spi2.compile_time_value)
						  END
				FROM #stored_proc_info AS spi2
				WHERE spi.SqlHandle = spi2.SqlHandle
				AND spi2.proc_name = N'Statement'
				AND spi2.variable_name NOT LIKE N'%msparam%'
				FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
			AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
			AS cached_execution_parameters
FROM #stored_proc_info AS spi
GROUP BY spi.SPID, spi.SqlHandle, spi.proc_name, spi.set_options
) 
UPDATE b
SET b.cached_execution_parameters = pk.cached_execution_parameters
FROM ##bou_BlitzCacheProcs AS b
JOIN precheck pk
ON pk.SqlHandle = b.SqlHandle
AND pk.SPID = b.SPID
WHERE b.QueryType = N'Statement'
OPTION (RECOMPILE);

RAISERROR(N'Filling in implicit conversion and cached plan parameter info', 0, 1) WITH NOWAIT;
UPDATE b
SET b.implicit_conversion_info = CASE WHEN b.implicit_conversion_info IS NULL 
									  OR CONVERT(NVARCHAR(MAX), b.implicit_conversion_info) = N''
									  THEN '<?NoNeedToClickMe -- N/A --?>' 
							     ELSE b.implicit_conversion_info END,
	b.cached_execution_parameters = CASE WHEN b.cached_execution_parameters IS NULL 
										 OR CONVERT(NVARCHAR(MAX), b.cached_execution_parameters) = N''
										 THEN '<?NoNeedToClickMe -- N/A --?>' 
									ELSE b.cached_execution_parameters END
FROM ##bou_BlitzCacheProcs AS b
WHERE b.SPID = @@SPID
OPTION (RECOMPILE);

/*End implicit conversion and parameter info*/

/*Begin Missing Index*/
IF EXISTS ( SELECT 1/0 
            FROM ##bou_BlitzCacheProcs AS bbcp 
            WHERE bbcp.missing_index_count > 0
		    OR bbcp.index_spool_cost > 0
		    OR bbcp.index_spool_rows > 0
		    AND bbcp.SPID = @@SPID )
		   
		BEGIN		
		RAISERROR(N'Inserting to #missing_index_xml', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT 	#missing_index_xml
		SELECT qp.QueryHash,
		       qp.SqlHandle,
		       c.mg.value('@Impact', 'FLOAT') AS Impact,
			   c.mg.query('.') AS cmg
		FROM   #query_plan AS qp
		CROSS APPLY qp.query_plan.nodes('//p:MissingIndexes/p:MissingIndexGroup') AS c(mg)
		WHERE  qp.QueryHash IS NOT NULL
		OPTION(RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_schema', 0, 1) WITH NOWAIT;	
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_schema
		SELECT mix.QueryHash, mix.SqlHandle, mix.impact,
		       c.mi.value('@Database', 'NVARCHAR(128)'),
		       c.mi.value('@Schema', 'NVARCHAR(128)'),
		       c.mi.value('@Table', 'NVARCHAR(128)'),
			   c.mi.query('.')
		FROM #missing_index_xml AS mix
		CROSS APPLY mix.index_xml.nodes('//p:MissingIndex') AS c(mi)
		OPTION(RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_usage', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_usage
		SELECT ms.QueryHash, ms.SqlHandle, ms.impact, ms.database_name, ms.schema_name, ms.table_name,
				c.cg.value('@Usage', 'NVARCHAR(128)'),
				c.cg.query('.')
		FROM #missing_index_schema ms
		CROSS APPLY ms.index_xml.nodes('//p:ColumnGroup') AS c(cg)
		OPTION(RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_detail', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_detail
		SELECT miu.QueryHash,
		       miu.SqlHandle,
		       miu.impact,
		       miu.database_name,
		       miu.schema_name,
		       miu.table_name,
		       miu.usage,
		       c.c.value('@Name', 'NVARCHAR(128)')
		FROM #missing_index_usage AS miu
		CROSS APPLY miu.index_xml.nodes('//p:Column') AS c(c)
		OPTION (RECOMPILE);
		
		RAISERROR(N'Inserting to missing indexes to #missing_index_pretty', 0, 1) WITH NOWAIT;
		INSERT #missing_index_pretty 
		       ( QueryHash,   SqlHandle,   impact,   database_name,   schema_name,   table_name, equality, inequality, include, executions, query_cost, creation_hours, is_spool )
		SELECT DISTINCT m.QueryHash, m.SqlHandle, m.impact, m.database_name, m.schema_name, m.table_name
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'EQUALITY'
						 AND m.QueryHash = m2.QueryHash
						 AND m.SqlHandle = m2.SqlHandle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS equality
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'INEQUALITY'
						 AND m.QueryHash = m2.QueryHash
						 AND m.SqlHandle = m2.SqlHandle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS inequality
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'INCLUDE'
						 AND m.QueryHash = m2.QueryHash
						 AND m.SqlHandle = m2.SqlHandle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS [include],
		bbcp.ExecutionCount,
		bbcp.QueryPlanCost,
		bbcp.PlanCreationTimeHours,
		0 as is_spool
		FROM #missing_index_detail AS m
		JOIN ##bou_BlitzCacheProcs AS bbcp
		ON m.SqlHandle = bbcp.SqlHandle
		OPTION (RECOMPILE);
		
		RAISERROR(N'Inserting to #index_spool_ugly', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
		INSERT #index_spool_ugly 
		        (QueryHash, SqlHandle, impact, database_name, schema_name, table_name, equality, inequality, include, executions, query_cost, creation_hours)
		SELECT p.QueryHash, 
		       p.SqlHandle,                                                                               
		       (c.n.value('@EstimateIO', 'FLOAT') + (c.n.value('@EstimateCPU', 'FLOAT'))) 
					/ ( 1 * NULLIF(p.QueryPlanCost, 0)) * 100 AS impact, 
			   o.n.value('@Database', 'NVARCHAR(128)') AS output_database, 
			   o.n.value('@Schema', 'NVARCHAR(128)') AS output_schema, 
			   o.n.value('@Table', 'NVARCHAR(128)') AS output_table, 
		       k.n.value('@Column', 'NVARCHAR(128)') AS range_column,
			   e.n.value('@Column', 'NVARCHAR(128)') AS expression_column,
			   o.n.value('@Column', 'NVARCHAR(128)') AS output_column, 
			   p.ExecutionCount, 
			   p.QueryPlanCost, 
			   p.PlanCreationTimeHours
		FROM #relop AS r
		JOIN ##bou_BlitzCacheProcs p
		ON p.QueryHash = r.QueryHash
		CROSS APPLY r.relop.nodes('/p:RelOp') AS c(n)
		CROSS APPLY r.relop.nodes('/p:RelOp/p:OutputList/p:ColumnReference') AS o(n)
		OUTER APPLY r.relop.nodes('/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeColumns/p:ColumnReference') AS k(n)
		OUTER APPLY r.relop.nodes('/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeExpressions/p:ColumnReference') AS e(n)
		WHERE  r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1

		RAISERROR(N'Inserting to spools to #missing_index_pretty', 0, 1) WITH NOWAIT;
		INSERT #missing_index_pretty
			(QueryHash, SqlHandle, impact, database_name, schema_name, table_name, equality, inequality, include, executions, query_cost, creation_hours, is_spool)
		SELECT DISTINCT 
		       isu.QueryHash,
		       isu.SqlHandle,
		       isu.impact,
		       isu.database_name,
		       isu.schema_name,
		       isu.table_name
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.equality, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.equality IS NOT NULL
			   						 AND isu.QueryHash = isu2.QueryHash
			   						 AND isu.SqlHandle = isu2.SqlHandle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS equality
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.inequality, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.inequality IS NOT NULL
			   						 AND isu.QueryHash = isu2.QueryHash
			   						 AND isu.SqlHandle = isu2.SqlHandle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS inequality
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.include, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.include IS NOT NULL
			   						 AND isu.QueryHash = isu2.QueryHash
			   						 AND isu.SqlHandle = isu2.SqlHandle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS include,
		       isu.executions,
		       isu.query_cost,
		       isu.creation_hours,
			   1 AS is_spool
		FROM #index_spool_ugly AS isu


		RAISERROR(N'Updating missing index information', 0, 1) WITH NOWAIT;
		WITH missing AS (
		SELECT DISTINCT
		       mip.QueryHash,
		       mip.SqlHandle, 
			   mip.executions,
			   N'<MissingIndexes><![CDATA['
			   + CHAR(10) + CHAR(13)
			   + STUFF((   SELECT CHAR(10) + CHAR(13) + ISNULL(mip2.details, '') AS details
		                   FROM   #missing_index_pretty AS mip2
						   WHERE mip.QueryHash = mip2.QueryHash
						   AND mip.SqlHandle = mip2.SqlHandle
						   AND mip.executions = mip2.executions
						   GROUP BY mip2.details
		                   ORDER BY MAX(mip2.impact) DESC
						   FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') 
			   + CHAR(10) + CHAR(13)
			   + N']]></MissingIndexes>' 
			   AS full_details
		FROM #missing_index_pretty AS mip
						)
		UPDATE bbcp
			SET bbcp.missing_indexes = m.full_details
		FROM ##bou_BlitzCacheProcs AS bbcp
		JOIN missing AS m
		ON m.SqlHandle = bbcp.SqlHandle
		AND m.QueryHash = bbcp.QueryHash
		AND m.executions = bbcp.ExecutionCount
		AND SPID = @@SPID
		OPTION (RECOMPILE);

	END;

	RAISERROR(N'Filling in missing index blanks', 0, 1) WITH NOWAIT;
	UPDATE b
	SET b.missing_indexes = 
		CASE WHEN b.missing_indexes IS NULL 
			 THEN '<?NoNeedToClickMe -- N/A --?>' 
			 ELSE b.missing_indexes 
		END
	FROM ##bou_BlitzCacheProcs AS b
	WHERE b.SPID = @@SPID
	OPTION (RECOMPILE);

/*End Missing Index*/


/* Set configuration values */
RAISERROR(N'Setting configuration values', 0, 1) WITH NOWAIT;
DECLARE @execution_threshold INT = 1000 ,
        @parameter_sniffing_warning_pct TINYINT = 30,
        /* This is in average reads */
        @parameter_sniffing_io_threshold BIGINT = 100000 ,
        @ctp_threshold_pct TINYINT = 10,
        @long_running_query_warning_seconds BIGINT = 300 * 1000 ,
		@memory_grant_warning_percent INT = 10;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'frequent execution threshold' = LOWER(parameter_name))
BEGIN
    SELECT @execution_threshold = CAST(value AS INT)
    FROM   #configuration
    WHERE  'frequent execution threshold' = LOWER(parameter_name) ;

    SET @msg = ' Setting "frequent execution threshold" to ' + CAST(@execution_threshold AS VARCHAR(10)) ;

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'parameter sniffing variance percent' = LOWER(parameter_name))
BEGIN
    SELECT @parameter_sniffing_warning_pct = CAST(value AS TINYINT)
    FROM   #configuration
    WHERE  'parameter sniffing variance percent' = LOWER(parameter_name) ;

    SET @msg = ' Setting "parameter sniffing variance percent" to ' + CAST(@parameter_sniffing_warning_pct AS VARCHAR(3)) ;

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'parameter sniffing io threshold' = LOWER(parameter_name))
BEGIN
    SELECT @parameter_sniffing_io_threshold = CAST(value AS BIGINT)
    FROM   #configuration
    WHERE 'parameter sniffing io threshold' = LOWER(parameter_name) ;

    SET @msg = ' Setting "parameter sniffing io threshold" to ' + CAST(@parameter_sniffing_io_threshold AS VARCHAR(10));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'cost threshold for parallelism warning' = LOWER(parameter_name))
BEGIN
    SELECT @ctp_threshold_pct = CAST(value AS TINYINT)
    FROM   #configuration
    WHERE 'cost threshold for parallelism warning' = LOWER(parameter_name) ;

    SET @msg = ' Setting "cost threshold for parallelism warning" to ' + CAST(@ctp_threshold_pct AS VARCHAR(3));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'long running query warning (seconds)' = LOWER(parameter_name))
BEGIN
    SELECT @long_running_query_warning_seconds = CAST(value * 1000 AS BIGINT)
    FROM   #configuration
    WHERE 'long running query warning (seconds)' = LOWER(parameter_name) ;

    SET @msg = ' Setting "long running query warning (seconds)" to ' + CAST(@long_running_query_warning_seconds AS VARCHAR(10));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'unused memory grant' = LOWER(parameter_name))
BEGIN
    SELECT @memory_grant_warning_percent = CAST(value AS INT)
    FROM   #configuration
    WHERE 'unused memory grant' = LOWER(parameter_name) ;

    SET @msg = ' Setting "unused memory grant" to ' + CAST(@memory_grant_warning_percent AS VARCHAR(10));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END;

DECLARE @ctp INT ;

SELECT  @ctp = NULLIF(CAST(value AS INT), 0)
FROM    sys.configurations
WHERE   name = 'cost threshold for parallelism'
OPTION (RECOMPILE);


/* Update to populate checks columns */
RAISERROR('Checking for query level SQL Server issues.', 0, 1) WITH NOWAIT;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET    frequent_execution = CASE WHEN ExecutionsPerMinute > @execution_threshold THEN 1 END ,
       parameter_sniffing = CASE WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND min_worker_time < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND max_worker_time > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND MinReturnedRows < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND MaxReturnedRows > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1 END ,
       near_parallel = CASE WHEN QueryPlanCost BETWEEN @ctp * (1 - (@ctp_threshold_pct / 100.0)) AND @ctp THEN 1 END,
       long_running = CASE WHEN AverageDuration > @long_running_query_warning_seconds THEN 1
                           WHEN max_worker_time > @long_running_query_warning_seconds THEN 1
                           WHEN max_elapsed_time > @long_running_query_warning_seconds THEN 1 END,
	   is_key_lookup_expensive = CASE WHEN QueryPlanCost >= (@ctp / 2) AND key_lookup_cost >= QueryPlanCost * .5 THEN 1 END,
	   is_sort_expensive = CASE WHEN QueryPlanCost >= (@ctp / 2) AND sort_cost >= QueryPlanCost * .5 THEN 1 END,
	   is_remote_query_expensive = CASE WHEN remote_query_cost >= QueryPlanCost * .05 THEN 1 END,
	   is_forced_serial = CASE WHEN is_forced_serial = 1 THEN 1 END,
	   is_unused_grant = CASE WHEN PercentMemoryGrantUsed <= @memory_grant_warning_percent AND MinGrantKB > @MinMemoryPerQuery THEN 1 END,
	   long_running_low_cpu = CASE WHEN AverageDuration > AverageCPU * 4 AND AverageCPU < 500. THEN 1 END,
	   low_cost_high_cpu = CASE WHEN QueryPlanCost <= 10 AND AverageCPU > 5000. THEN 1 END,
	   is_spool_expensive = CASE WHEN QueryPlanCost > (@ctp / 5) AND index_spool_cost >= QueryPlanCost * .1 THEN 1 END,
	   is_spool_more_rows = CASE WHEN index_spool_rows >= (AverageReturnedRows / ISNULL(NULLIF(ExecutionCount, 0), 1)) THEN 1 END,
	   is_bad_estimate = CASE WHEN AverageReturnedRows > 0 AND (estimated_rows * 1000 < AverageReturnedRows OR estimated_rows > AverageReturnedRows * 1000) THEN 1 END,
	   is_big_spills = CASE WHEN (AvgSpills / 128.) > 499. THEN 1 END
WHERE SPID = @@SPID
OPTION (RECOMPILE);



RAISERROR('Checking for forced parameterization and cursors.', 0, 1) WITH NOWAIT;

/* Set options checks */
UPDATE p
       SET is_forced_parameterized = CASE WHEN (CAST(pa.value AS INT) & 131072 = 131072) THEN 1 END ,
       is_forced_plan = CASE WHEN (CAST(pa.value AS INT) & 4 = 4) THEN 1 END ,
       SetOptions = SUBSTRING(
                    CASE WHEN (CAST(pa.value AS INT) & 1 = 1) THEN ', ANSI_PADDING' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 8 = 8) THEN ', CONCAT_NULL_YIELDS_NULL' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 16 = 16) THEN ', ANSI_WARNINGS' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 32 = 32) THEN ', ANSI_NULLS' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 64 = 64) THEN ', QUOTED_IDENTIFIER' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 4096 = 4096) THEN ', ARITH_ABORT' ELSE '' END +
                    CASE WHEN (CAST(pa.value AS INT) & 8192 = 8191) THEN ', NUMERIC_ROUNDABORT' ELSE '' END 
                    , 2, 200000)
FROM   ##bou_BlitzCacheProcs p
       CROSS APPLY sys.dm_exec_plan_attributes(p.PlanHandle) pa
WHERE  pa.attribute = 'set_options' 
AND SPID = @@SPID
OPTION (RECOMPILE);


/* Cursor checks */
UPDATE p
SET    is_cursor = CASE WHEN CAST(pa.value AS INT) <> 0 THEN 1 END
FROM   ##bou_BlitzCacheProcs p
       CROSS APPLY sys.dm_exec_plan_attributes(p.PlanHandle) pa
WHERE  pa.attribute LIKE '%cursor%' 
AND SPID = @@SPID
OPTION (RECOMPILE);

UPDATE p
SET    is_cursor = 1
FROM   ##bou_BlitzCacheProcs p
WHERE QueryHash = 0x0000000000000000
OR QueryPlanHash = 0x0000000000000000
AND SPID = @@SPID
OPTION (RECOMPILE);



RAISERROR('Populating Warnings column', 0, 1) WITH NOWAIT;
/* Populate warnings */
UPDATE ##bou_BlitzCacheProcs
SET    Warnings = SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN ', No Join Predicate' ELSE '' END +
                  CASE WHEN compile_timeout = 1 THEN ', Compilation Timeout' ELSE '' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN ', Compile Memory Limit Exceeded' ELSE '' END +
                  CASE WHEN busy_loops = 1 THEN ', Busy Loops' ELSE '' END +
                  CASE WHEN is_forced_plan = 1 THEN ', Forced Plan' ELSE '' END +
                  CASE WHEN is_forced_parameterized = 1 THEN ', Forced Parameterization' ELSE '' END +
                  CASE WHEN unparameterized_query = 1 THEN ', Unparameterized Query' ELSE '' END +
                  CASE WHEN missing_index_count > 0 THEN ', Missing Indexes (' + CAST(missing_index_count AS VARCHAR(3)) + ')' ELSE '' END +
                  CASE WHEN unmatched_index_count > 0 THEN ', Unmatched Indexes (' + CAST(unmatched_index_count AS VARCHAR(3)) + ')' ELSE '' END +                  
                  CASE WHEN is_cursor = 1 THEN ', Cursor' 
							+ CASE WHEN is_optimistic_cursor = 1 THEN '; optimistic' ELSE '' END
							+ CASE WHEN is_forward_only_cursor = 0 THEN '; not forward only' ELSE '' END
							+ CASE WHEN is_cursor_dynamic = 1 THEN '; dynamic' ELSE '' END
                            + CASE WHEN is_fast_forward_cursor = 1 THEN '; fast forward' ELSE '' END		
				  ELSE '' END +
                  CASE WHEN is_parallel = 1 THEN ', Parallel' ELSE '' END +
                  CASE WHEN near_parallel = 1 THEN ', Nearly Parallel' ELSE '' END +
                  CASE WHEN frequent_execution = 1 THEN ', Frequent Execution' ELSE '' END +
                  CASE WHEN plan_warnings = 1 THEN ', Plan Warnings' ELSE '' END +
                  CASE WHEN parameter_sniffing = 1 THEN ', Parameter Sniffing' ELSE '' END +
                  CASE WHEN long_running = 1 THEN ', Long Running Query' ELSE '' END +
                  CASE WHEN downlevel_estimator = 1 THEN ', Downlevel CE' ELSE '' END +
                  CASE WHEN implicit_conversions = 1 THEN ', Implicit Conversions' ELSE '' END +
                  CASE WHEN tvf_join = 1 THEN ', Function Join' ELSE '' END +
                  CASE WHEN plan_multiple_plans = 1 THEN ', Multiple Plans' ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session ELSE '' END +
				  CASE WHEN is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), function_count) + ' function(s)' ELSE '' END + 
				  CASE WHEN clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), clr_function_count) + ' CLR function(s)' ELSE '' END + 
				  CASE WHEN PlanCreationTimeHours <= 4 THEN ', Plan created last 4hrs' ELSE '' END +
				  CASE WHEN is_table_variable = 1 THEN ', Table Variables' ELSE '' END +
				  CASE WHEN no_stats_warning = 1 THEN ', Columns With No Statistics' ELSE '' END +
				  CASE WHEN relop_warnings = 1 THEN ', Operator Warnings' ELSE '' END  + 
				  CASE WHEN is_table_scan = 1 THEN ', Table Scans' ELSE '' END  + 
				  CASE WHEN backwards_scan = 1 THEN ', Backwards Scans' ELSE '' END  + 
				  CASE WHEN forced_index = 1 THEN ', Forced Indexes' ELSE '' END  + 
				  CASE WHEN forced_seek = 1 THEN ', Forced Seeks' ELSE '' END  + 
				  CASE WHEN forced_scan = 1 THEN ', Forced Scans' ELSE '' END  +
				  CASE WHEN columnstore_row_mode = 1 THEN ', ColumnStore Row Mode ' ELSE '' END +
				  CASE WHEN is_computed_scalar = 1 THEN ', Computed Column UDF ' ELSE '' END  +
				  CASE WHEN is_sort_expensive = 1 THEN ', Expensive Sort' ELSE '' END +
				  CASE WHEN is_computed_filter = 1 THEN ', Filter UDF' ELSE '' END +
				  CASE WHEN index_ops >= 5 THEN ', >= 5 Indexes Modified' ELSE '' END +
				  CASE WHEN is_row_level = 1 THEN ', Row Level Security' ELSE '' END + 
				  CASE WHEN is_spatial = 1 THEN ', Spatial Index' ELSE '' END + 
				  CASE WHEN index_dml = 1 THEN ', Index DML' ELSE '' END +
				  CASE WHEN table_dml = 1 THEN ', Table DML' ELSE '' END +
				  CASE WHEN low_cost_high_cpu = 1 THEN ', Low Cost High CPU' ELSE '' END + 
				  CASE WHEN long_running_low_cpu = 1 THEN + ', Long Running With Low CPU' ELSE '' END +
				  CASE WHEN stale_stats = 1 THEN + ', Statistics used have > 100k modifications in the last 7 days' ELSE '' END +
				  CASE WHEN is_adaptive = 1 THEN + ', Adaptive Joins' ELSE '' END +
				  CASE WHEN is_spool_expensive = 1 THEN + ', Expensive Index Spool' ELSE '' END +
				  CASE WHEN is_spool_more_rows = 1 THEN + ', Large Index Row Spool' ELSE '' END +
				  CASE WHEN is_bad_estimate = 1 THEN + ', Row estimate mismatch' ELSE '' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN ', SWITCH!' ELSE '' END + 
				  CASE WHEN is_row_goal = 1 THEN ', Row Goals' ELSE '' END + 
                  CASE WHEN is_big_spills = 1 THEN ', >500mb spills' ELSE '' END + 
				  CASE WHEN is_mstvf = 1 THEN ', MSTVFs' ELSE '' END + 
				  CASE WHEN is_mm_join = 1 THEN ', Many to Many Merge' ELSE '' END + 
                  CASE WHEN is_nonsargable = 1 THEN ', non-SARGables' ELSE '' END + 
				  CASE WHEN CompileTime > 5000 THEN ', Long Compile Time' ELSE '' END +
				  CASE WHEN CompileCPU > 5000 THEN ', High Compile CPU' ELSE '' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN ', High Compile Memory' ELSE '' END
				  , 3, 200000) 
WHERE SPID = @@SPID
OPTION (RECOMPILE);


RAISERROR('Populating Warnings column for stored procedures', 0, 1) WITH NOWAIT;
WITH statement_warnings AS 
	(
SELECT  DISTINCT
		SqlHandle,
		Warnings = SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN ', No Join Predicate' ELSE '' END +
                  CASE WHEN compile_timeout = 1 THEN ', Compilation Timeout' ELSE '' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN ', Compile Memory Limit Exceeded' ELSE '' END +
                  CASE WHEN busy_loops = 1 THEN ', Busy Loops' ELSE '' END +
                  CASE WHEN is_forced_plan = 1 THEN ', Forced Plan' ELSE '' END +
                  CASE WHEN is_forced_parameterized = 1 THEN ', Forced Parameterization' ELSE '' END +
                  --CASE WHEN unparameterized_query = 1 THEN ', Unparameterized Query' ELSE '' END +
                  CASE WHEN missing_index_count > 0 THEN ', Missing Indexes (' + CONVERT(VARCHAR(10), (SELECT SUM(b2.missing_index_count) FROM ##bou_BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL) ) + ')' ELSE '' END +
                  CASE WHEN unmatched_index_count > 0 THEN ', Unmatched Indexes (' + CONVERT(VARCHAR(10), (SELECT SUM(b2.unmatched_index_count) FROM ##bou_BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL) ) + ')' ELSE '' END +                  
                  CASE WHEN is_cursor = 1 THEN ', Cursor' 
							+ CASE WHEN is_optimistic_cursor = 1 THEN '; optimistic' ELSE '' END
							+ CASE WHEN is_forward_only_cursor = 0 THEN '; not forward only' ELSE '' END
							+ CASE WHEN is_cursor_dynamic = 1 THEN '; dynamic' ELSE '' END
                            + CASE WHEN is_fast_forward_cursor = 1 THEN '; fast forward' ELSE '' END								
				  ELSE '' END +
                  CASE WHEN is_parallel = 1 THEN ', Parallel' ELSE '' END +
                  CASE WHEN near_parallel = 1 THEN ', Nearly Parallel' ELSE '' END +
                  CASE WHEN frequent_execution = 1 THEN ', Frequent Execution' ELSE '' END +
                  CASE WHEN plan_warnings = 1 THEN ', Plan Warnings' ELSE '' END +
                  CASE WHEN parameter_sniffing = 1 THEN ', Parameter Sniffing' ELSE '' END +
                  CASE WHEN long_running = 1 THEN ', Long Running Query' ELSE '' END +
                  CASE WHEN downlevel_estimator = 1 THEN ', Downlevel CE' ELSE '' END +
                  CASE WHEN implicit_conversions = 1 THEN ', Implicit Conversions' ELSE '' END +
                  CASE WHEN tvf_join = 1 THEN ', Function Join' ELSE '' END +
                  CASE WHEN plan_multiple_plans = 1 THEN ', Multiple Plans' ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session ELSE '' END +
				  CASE WHEN is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), (SELECT SUM(b2.function_count) FROM ##bou_BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL) ) + ' function(s)' ELSE '' END + 
				  CASE WHEN clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), (SELECT SUM(b2.clr_function_count) FROM ##bou_BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL) ) + ' CLR function(s)' ELSE '' END + 
				  CASE WHEN PlanCreationTimeHours <= 4 THEN ', Plan created last 4hrs' ELSE '' END +
				  CASE WHEN is_table_variable = 1 THEN ', Table Variables' ELSE '' END +
				  CASE WHEN no_stats_warning = 1 THEN ', Columns With No Statistics' ELSE '' END +
				  CASE WHEN relop_warnings = 1 THEN ', Operator Warnings' ELSE '' END  + 
				  CASE WHEN is_table_scan = 1 THEN ', Table Scans' ELSE '' END  + 
				  CASE WHEN backwards_scan = 1 THEN ', Backwards Scans' ELSE '' END  + 
				  CASE WHEN forced_index = 1 THEN ', Forced Indexes' ELSE '' END  + 
				  CASE WHEN forced_seek = 1 THEN ', Forced Seeks' ELSE '' END  + 
				  CASE WHEN forced_scan = 1 THEN ', Forced Scans' ELSE '' END  +
				  CASE WHEN columnstore_row_mode = 1 THEN ', ColumnStore Row Mode ' ELSE '' END +
				  CASE WHEN is_computed_scalar = 1 THEN ', Computed Column UDF ' ELSE '' END  +
				  CASE WHEN is_sort_expensive = 1 THEN ', Expensive Sort' ELSE '' END +
				  CASE WHEN is_computed_filter = 1 THEN ', Filter UDF' ELSE '' END +
				  CASE WHEN index_ops >= 5 THEN ', >= 5 Indexes Modified' ELSE '' END +
				  CASE WHEN is_row_level = 1 THEN ', Row Level Security' ELSE '' END + 
				  CASE WHEN is_spatial = 1 THEN ', Spatial Index' ELSE '' END +
				  CASE WHEN index_dml = 1 THEN ', Index DML' ELSE '' END +
				  CASE WHEN table_dml = 1 THEN ', Table DML' ELSE '' END + 
				  CASE WHEN low_cost_high_cpu = 1 THEN ', Low Cost High CPU' ELSE '' END + 
				  CASE WHEN long_running_low_cpu = 1 THEN + ', Long Running With Low CPU' ELSE '' END + 
				  CASE WHEN stale_stats = 1 THEN + ', Statistics used have > 100k modifications in the last 7 days' ELSE '' END +
				  CASE WHEN is_adaptive = 1 THEN + ', Adaptive Joins' ELSE '' END +
				  CASE WHEN is_spool_expensive = 1 THEN + ', Expensive Index Spool' ELSE '' END +
				  CASE WHEN is_spool_more_rows = 1 THEN + ', Large Index Row Spool' ELSE '' END +
				  CASE WHEN is_bad_estimate = 1 THEN + ', Row estimate mismatch' ELSE '' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN ', SWITCH!' ELSE '' END + 
				  CASE WHEN is_row_goal = 1 THEN ', Row Goals' ELSE '' END + 
                  CASE WHEN is_big_spills = 1 THEN ', >500mb spills' ELSE '' END + 
				  CASE WHEN is_mstvf = 1 THEN ', MSTVFs' ELSE '' END + 
				  CASE WHEN is_mm_join = 1 THEN ', Many to Many Merge' ELSE '' END + 
                  CASE WHEN is_nonsargable = 1 THEN ', non-SARGables' ELSE '' END +
				  CASE WHEN CompileTime > 5000 THEN ', Long Compile Time' ELSE '' END +
				  CASE WHEN CompileCPU > 5000 THEN ', High Compile CPU' ELSE '' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN ', High Compile Memory' ELSE '' END
                  , 3, 200000) 
FROM ##bou_BlitzCacheProcs b
WHERE SPID = @@SPID
AND QueryType LIKE 'Statement (parent%'
	)
UPDATE b
SET b.Warnings = s.Warnings
FROM ##bou_BlitzCacheProcs AS b
JOIN statement_warnings s
ON b.SqlHandle = s.SqlHandle
WHERE QueryType LIKE 'Procedure or Function%'
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR('Checking for plans with >128 levels of nesting', 0, 1) WITH NOWAIT;	
WITH plan_handle AS (
SELECT b.PlanHandle
FROM ##bou_BlitzCacheProcs b
   CROSS APPLY sys.dm_exec_text_query_plan(b.PlanHandle, 0, -1) tqp
   CROSS APPLY sys.dm_exec_query_plan(b.PlanHandle) qp
   WHERE tqp.encrypted = 0
   AND b.SPID = @@SPID
   AND (qp.query_plan IS NULL
			AND tqp.query_plan IS NOT NULL)
)
UPDATE b
SET Warnings = ISNULL('Your query plan is >128 levels of nested nodes, and can''t be converted to XML. Use SELECT * FROM sys.dm_exec_text_query_plan('+ CONVERT(VARCHAR(128), ph.PlanHandle, 1) + ', 0, -1) to get more information' 
                        , 'We couldn''t find a plan for this query. Possible reasons for this include dynamic SQL, RECOMPILE hints, and encrypted code.')
FROM ##bou_BlitzCacheProcs b
LEFT JOIN plan_handle ph ON
b.PlanHandle = ph.PlanHandle
WHERE b.QueryPlan IS NULL
AND b.SPID = @@SPID
OPTION (RECOMPILE);			  

RAISERROR('Checking for plans with no warnings', 0, 1) WITH NOWAIT;	
UPDATE ##bou_BlitzCacheProcs
SET Warnings = 'No warnings detected. ' + CASE @ExpertMode 
											WHEN 0 
											THEN ' Try running sp_BlitzCache with @ExpertMode = 1 to find more advanced problems.' 
											ELSE '' 
										  END
WHERE Warnings = '' OR	Warnings IS NULL
AND SPID = @@SPID
OPTION (RECOMPILE);


Results:
IF @OutputDatabaseName IS NOT NULL
   AND @OutputSchemaName IS NOT NULL
   AND @OutputTableName IS NOT NULL
BEGIN
    RAISERROR('Writing results to table.', 0, 1) WITH NOWAIT;

    /* send results to a table */
    DECLARE @insert_sql NVARCHAR(MAX) = N'' ;

    SET @insert_sql = 'USE '
        + @OutputDatabaseName
        + '; IF EXISTS(SELECT * FROM '
        + @OutputDatabaseName
        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
        + @OutputSchemaName
        + ''') AND NOT EXISTS (SELECT * FROM '
        + @OutputDatabaseName
        + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
        + @OutputSchemaName + ''' AND QUOTENAME(TABLE_NAME) = '''
        + @OutputTableName + ''') CREATE TABLE '
        + @OutputSchemaName + '.'
        + @OutputTableName
        + N'(ID bigint NOT NULL IDENTITY(1,1),
          ServerName NVARCHAR(258),
		  CheckDate DATETIMEOFFSET,
          Version NVARCHAR(258),
          QueryType NVARCHAR(258),
          Warnings varchar(max),
          DatabaseName sysname,
          SerialDesiredMemory float,
          SerialRequiredMemory float,
          AverageCPU bigint,
          TotalCPU bigint,
          PercentCPUByType money,
          CPUWeight money,
          AverageDuration bigint,
          TotalDuration bigint,
          DurationWeight money,
          PercentDurationByType money,
          AverageReads bigint,
          TotalReads bigint,
          ReadWeight money,
          PercentReadsByType money,
          AverageWrites bigint,
          TotalWrites bigint,
          WriteWeight money,
          PercentWritesByType money,
          ExecutionCount bigint,
          ExecutionWeight money,
          PercentExecutionsByType money,' + N'
          ExecutionsPerMinute money,
          PlanCreationTime datetime,
		  PlanCreationTimeHours AS DATEDIFF(HOUR, PlanCreationTime, SYSDATETIME()),
          LastExecutionTime datetime,
		  PlanHandle varbinary(64),
		  [Remove Plan Handle From Cache] AS 
			CASE WHEN [PlanHandle] IS NOT NULL 
			THEN ''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [PlanHandle], 1) + '');''
			ELSE ''N/A'' END,
		  SqlHandle varbinary(64),
			[Remove SQL Handle From Cache] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN ''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '');''
			ELSE ''N/A'' END,
		  [SQL Handle More Info] AS 
			CASE WHEN [SqlHandle] IS NOT NULL 
			THEN ''EXEC sp_BlitzCache @OnlySqlHandles = '''''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ''''''; ''
			ELSE ''N/A'' END,
		  QueryHash binary(8),
		  [Query Hash More Info] AS 
			CASE WHEN [QueryHash] IS NOT NULL 
			THEN ''EXEC sp_BlitzCache @OnlyQueryHashes = '''''' + CONVERT(VARCHAR(32), [QueryHash], 1) + ''''''; ''
			ELSE ''N/A'' END,
          QueryPlanHash binary(8),
          StatementStartOffset int,
          StatementEndOffset int,
          MinReturnedRows bigint,
          MaxReturnedRows bigint,
          AverageReturnedRows money,
          TotalReturnedRows bigint,
          QueryText nvarchar(max),
          QueryPlan xml,
          NumberOfPlans int,
          NumberOfDistinctPlans int,
		  MinGrantKB BIGINT,
		  MaxGrantKB BIGINT,
		  MinUsedGrantKB BIGINT, 
		  MaxUsedGrantKB BIGINT,
		  PercentMemoryGrantUsed MONEY,
		  AvgMaxMemoryGrant MONEY,
		  MinSpills BIGINT,
		  MaxSpills BIGINT,
		  TotalSpills BIGINT,
		  AvgSpills MONEY,
		  QueryPlanCost FLOAT,
          CONSTRAINT [PK_' +CAST(NEWID() AS NCHAR(36)) + '] PRIMARY KEY CLUSTERED(ID))';

    		IF @Debug = 1
			BEGIN
			    PRINT SUBSTRING(@insert_sql, 0, 4000);
			    PRINT SUBSTRING(@insert_sql, 4000, 8000);
			    PRINT SUBSTRING(@insert_sql, 8000, 12000);
			    PRINT SUBSTRING(@insert_sql, 12000, 16000);
			    PRINT SUBSTRING(@insert_sql, 16000, 20000);
			    PRINT SUBSTRING(@insert_sql, 20000, 24000);
			    PRINT SUBSTRING(@insert_sql, 24000, 28000);
			    PRINT SUBSTRING(@insert_sql, 28000, 32000);
			    PRINT SUBSTRING(@insert_sql, 32000, 36000);
			    PRINT SUBSTRING(@insert_sql, 36000, 40000);
			END;

	EXEC sp_executesql @insert_sql ;

    IF @CheckDateOverride IS NULL
        BEGIN
        SET @CheckDateOverride = SYSDATETIMEOFFSET();
        END;


    SET @insert_sql = N' IF EXISTS(SELECT * FROM '
          + @OutputDatabaseName
          + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
          + @OutputSchemaName + N''') '
		  + N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
          + 'INSERT '
          + @OutputDatabaseName + '.'
          + @OutputSchemaName + '.'
          + @OutputTableName
          + N' (ServerName, CheckDate, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
          + N' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
          + N' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost ) '
          + N'SELECT TOP (@Top) '
          + QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), N'''') + N', @CheckDateOverride, '
          + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), N'''') + ', '
          + N' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
          + N' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
          + N' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost '
          + N' FROM ##bou_BlitzCacheProcs '
		  + N' WHERE 1=1 ';
   
   IF @MinimumExecutionCount IS NOT NULL
      BEGIN
		SET @insert_sql += N' AND ExecutionCount >= @MinimumExecutionCount ';
	  END;

   IF @MinutesBack IS NOT NULL
      BEGIN
		SET @insert_sql += N' AND LastExecutionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
	  END;

	SET @insert_sql += N' AND SPID = @@SPID ';
          
    SELECT @insert_sql += N' ORDER BY ' + CASE @SortOrder WHEN 'cpu' THEN N' TotalCPU '
                                                    WHEN N'reads' THEN N' TotalReads '
                                                    WHEN N'writes' THEN N' TotalWrites '
                                                    WHEN N'duration' THEN N' TotalDuration '
                                                    WHEN N'executions' THEN N' ExecutionCount '
                                                    WHEN N'compiles' THEN N' PlanCreationTime '
													WHEN N'memory grant' THEN N' MaxGrantKB'
													WHEN N'spills' THEN N' MaxSpills'
                                                    WHEN N'avg cpu' THEN N' AverageCPU'
                                                    WHEN N'avg reads' THEN N' AverageReads'
                                                    WHEN N'avg writes' THEN N' AverageWrites'
                                                    WHEN N'avg duration' THEN N' AverageDuration'
                                                    WHEN N'avg executions' THEN N' ExecutionsPerMinute'
													WHEN N'avg memory grant' THEN N' AvgMaxMemoryGrant'
													WHEN 'avg spills' THEN N' AvgSpills'
                                                    END + N' DESC ';

    SET @insert_sql += N' OPTION (RECOMPILE) ; ';    
    
    	IF @Debug = 1
		BEGIN
		    PRINT SUBSTRING(@insert_sql, 0, 4000);
		    PRINT SUBSTRING(@insert_sql, 4000, 8000);
		    PRINT SUBSTRING(@insert_sql, 8000, 12000);
		    PRINT SUBSTRING(@insert_sql, 12000, 16000);
		    PRINT SUBSTRING(@insert_sql, 16000, 20000);
		    PRINT SUBSTRING(@insert_sql, 20000, 24000);
		    PRINT SUBSTRING(@insert_sql, 24000, 28000);
		    PRINT SUBSTRING(@insert_sql, 28000, 32000);
		    PRINT SUBSTRING(@insert_sql, 32000, 36000);
		    PRINT SUBSTRING(@insert_sql, 36000, 40000);
		END;

    EXEC sp_executesql @insert_sql, N'@Top INT, @min_duration INT, @min_back INT, @CheckDateOverride DATETIMEOFFSET, @MinimumExecutionCount INT', @Top, @DurationFilter_i, @MinutesBack, @CheckDateOverride, @MinimumExecutionCount;

    RETURN;
END;
ELSE IF @ExportToExcel = 1
BEGIN
    RAISERROR('Displaying results with Excel formatting (no plans).', 0, 1) WITH NOWAIT;

    /* excel output */
    UPDATE ##bou_BlitzCacheProcs
    SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),' ','<>'),'><',''),'<>',' '), 1, 32000)
	OPTION(RECOMPILE);

    SET @sql = N'
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT  TOP (@Top)
            DatabaseName AS [Database Name],
            QueryPlanCost AS [Cost],
            QueryText,
            QueryType AS [Query Type],
            Warnings,
            ExecutionCount,
            ExecutionsPerMinute AS [Executions / Minute],
            PercentExecutions AS [Execution Weight],
            PercentExecutionsByType AS [% Executions (Type)],
            SerialDesiredMemory AS [Serial Desired Memory],
            SerialRequiredMemory AS [Serial Required Memory],
            TotalCPU AS [Total CPU (ms)],
            AverageCPU AS [Avg CPU (ms)],
            PercentCPU AS [CPU Weight],
            PercentCPUByType AS [% CPU (Type)],
            TotalDuration AS [Total Duration (ms)],
            AverageDuration AS [Avg Duration (ms)],
            PercentDuration AS [Duration Weight],
            PercentDurationByType AS [% Duration (Type)],
            TotalReads AS [Total Reads],
            AverageReads AS [Average Reads],
            PercentReads AS [Read Weight],
            PercentReadsByType AS [% Reads (Type)],
            TotalWrites AS [Total Writes],
            AverageWrites AS [Average Writes],
            PercentWrites AS [Write Weight],
            PercentWritesByType AS [% Writes (Type)],
            TotalReturnedRows,
            AverageReturnedRows,
            MinReturnedRows,
            MaxReturnedRows,
		    MinGrantKB,
		    MaxGrantKB,
		    MinUsedGrantKB, 
		    MaxUsedGrantKB,
		    PercentMemoryGrantUsed,
			AvgMaxMemoryGrant,
			MinSpills, 
			MaxSpills, 
			TotalSpills, 
			AvgSpills,
            NumberOfPlans,
            NumberOfDistinctPlans,
            PlanCreationTime AS [Created At],
            LastExecutionTime AS [Last Execution],
            StatementStartOffset,
            StatementEndOffset,
			PlanHandle AS [Plan Handle],  
			SqlHandle AS [SQL Handle],  
            QueryHash,
            QueryPlanHash,
            COALESCE(SetOptions, '''') AS [SET Options]
    FROM    ##bou_BlitzCacheProcs
    WHERE   1 = 1 
	AND SPID = @@SPID ' + @nl;

    IF @MinimumExecutionCount IS NOT NULL
      BEGIN
		SET @sql += N' AND ExecutionCount >= @minimumExecutionCount ';
	  END;
	
   IF @MinutesBack IS NOT NULL
      BEGIN
		SET @sql += N' AND LastExecutionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
	  END;

	SELECT @sql += N' ORDER BY ' + CASE @SortOrder WHEN N'cpu' THEN N' TotalCPU '
                              WHEN N'reads' THEN N' TotalReads '
                              WHEN N'writes' THEN N' TotalWrites '
                              WHEN N'duration' THEN N' TotalDuration '
                              WHEN N'executions' THEN N' ExecutionCount '
                              WHEN N'compiles' THEN N' PlanCreationTime '
							  WHEN N'memory grant' THEN N' MaxGrantKB'
							  WHEN N'spills' THEN N' MaxSpills'
                              WHEN N'avg cpu' THEN N' AverageCPU'
                              WHEN N'avg reads' THEN N' AverageReads'
                              WHEN N'avg writes' THEN N' AverageWrites'
                              WHEN N'avg duration' THEN N' AverageDuration'
                              WHEN N'avg executions' THEN N' ExecutionsPerMinute'
							  WHEN N'avg memory grant' THEN N' AvgMaxMemoryGrant'
							  WHEN N'avg spills' THEN N' AvgSpills'
                              END + N' DESC ';

    SET @sql += N' OPTION (RECOMPILE) ; ';

	IF @Debug = 1
	BEGIN
	    PRINT SUBSTRING(@sql, 0, 4000);
	    PRINT SUBSTRING(@sql, 4000, 8000);
	    PRINT SUBSTRING(@sql, 8000, 12000);
	    PRINT SUBSTRING(@sql, 12000, 16000);
	    PRINT SUBSTRING(@sql, 16000, 20000);
	    PRINT SUBSTRING(@sql, 20000, 24000);
	    PRINT SUBSTRING(@sql, 24000, 28000);
	    PRINT SUBSTRING(@sql, 28000, 32000);
	    PRINT SUBSTRING(@sql, 32000, 36000);
	    PRINT SUBSTRING(@sql, 36000, 40000);
	END;

    EXEC sp_executesql @sql, N'@Top INT, @min_duration INT, @min_back INT, @minimumExecutionCount INT', @Top, @DurationFilter_i, @MinutesBack, @MinimumExecutionCount;
END;


RAISERROR('Displaying analysis of plan cache.', 0, 1) WITH NOWAIT;

DECLARE @columns NVARCHAR(MAX) = N'' ;

IF @ExpertMode = 0
BEGIN
    RAISERROR(N'Returning ExpertMode = 0', 0, 1) WITH NOWAIT;
	SET @columns = N' DatabaseName AS [Database],
    QueryPlanCost AS [Cost],
    QueryText AS [Query Text],
    QueryType AS [Query Type],
    Warnings AS [Warnings],
	QueryPlan AS [Query Plan],
	missing_indexes AS [Missing Indexes],
	implicit_conversion_info AS [Implicit Conversion Info],
	cached_execution_parameters AS [Cached Execution Parameters],
    ExecutionCount AS [# Executions],
    ExecutionsPerMinute AS [Executions / Minute],
    PercentExecutions AS [Execution Weight],
    TotalCPU AS [Total CPU (ms)],
    AverageCPU AS [Avg CPU (ms)],
    PercentCPU AS [CPU Weight],
    TotalDuration AS [Total Duration (ms)],
    AverageDuration AS [Avg Duration (ms)],
    PercentDuration AS [Duration Weight],
    TotalReads AS [Total Reads],
    AverageReads AS [Avg Reads],
    PercentReads AS [Read Weight],
    TotalWrites AS [Total Writes],
    AverageWrites AS [Avg Writes],
    PercentWrites AS [Write Weight],
    AverageReturnedRows AS [Average Rows],
	MinGrantKB AS [Minimum Memory Grant KB],
	MaxGrantKB AS [Maximum Memory Grant KB],
	MinUsedGrantKB AS [Minimum Used Grant KB], 
	MaxUsedGrantKB AS [Maximum Used Grant KB],
	AvgMaxMemoryGrant AS [Average Max Memory Grant],
	MinSpills AS [Min Spills],
	MaxSpills AS [Max Spills],
	TotalSpills AS [Total Spills],
	AvgSpills AS [Avg Spills],
    PlanCreationTime AS [Created At],
    LastExecutionTime AS [Last Execution],
	PlanHandle AS [Plan Handle], 
	SqlHandle AS [SQL Handle], 
    COALESCE(SetOptions, '''') AS [SET Options] ';
END;
ELSE
BEGIN
    SET @columns = N' DatabaseName AS [Database],
		QueryPlanCost AS [Cost],
        QueryText AS [Query Text],
        QueryType AS [Query Type],
        Warnings AS [Warnings], 
		QueryPlan AS [Query Plan], 
		missing_indexes AS [Missing Indexes],
		implicit_conversion_info AS [Implicit Conversion Info],
		cached_execution_parameters AS [Cached Execution Parameters], ' + @nl;

    IF @ExpertMode = 2 /* Opserver */
    BEGIN
        RAISERROR(N'Returning Expert Mode = 2', 0, 1) WITH NOWAIT;
		SET @columns += N'        
				  SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN '', 20'' ELSE '''' END +
                  CASE WHEN compile_timeout = 1 THEN '', 18'' ELSE '''' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN '', 19'' ELSE '''' END +
                  CASE WHEN busy_loops = 1 THEN '', 16'' ELSE '''' END +
                  CASE WHEN is_forced_plan = 1 THEN '', 3'' ELSE '''' END +
                  CASE WHEN is_forced_parameterized > 0 THEN '', 5'' ELSE '''' END +
                  CASE WHEN unparameterized_query = 1 THEN '', 23'' ELSE '''' END +
                  CASE WHEN missing_index_count > 0 THEN '', 10'' ELSE '''' END +
                  CASE WHEN unmatched_index_count > 0 THEN '', 22'' ELSE '''' END +                  
                  CASE WHEN is_cursor = 1 THEN '', 4'' ELSE '''' END +
                  CASE WHEN is_parallel = 1 THEN '', 6'' ELSE '''' END +
                  CASE WHEN near_parallel = 1 THEN '', 7'' ELSE '''' END +
                  CASE WHEN frequent_execution = 1 THEN '', 1'' ELSE '''' END +
                  CASE WHEN plan_warnings = 1 THEN '', 8'' ELSE '''' END +
                  CASE WHEN parameter_sniffing = 1 THEN '', 2'' ELSE '''' END +
                  CASE WHEN long_running = 1 THEN '', 9'' ELSE '''' END +
                  CASE WHEN downlevel_estimator = 1 THEN '', 13'' ELSE '''' END +
                  CASE WHEN implicit_conversions = 1 THEN '', 14'' ELSE '''' END +
                  CASE WHEN tvf_join = 1 THEN '', 17'' ELSE '''' END +
                  CASE WHEN plan_multiple_plans = 1 THEN '', 21'' ELSE '''' END +
                  CASE WHEN unmatched_index_count > 0 THEN '', 22'' ELSE '''' END + 
                  CASE WHEN is_trivial = 1 THEN '', 24'' ELSE '''' END + 
				  CASE WHEN is_forced_serial = 1 THEN '', 25'' ELSE '''' END +
                  CASE WHEN is_key_lookup_expensive = 1 THEN '', 26'' ELSE '''' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN '', 28'' ELSE '''' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN '', 29'' ELSE '''' END + 
				  CASE WHEN is_unused_grant = 1 THEN '', 30'' ELSE '''' END +
				  CASE WHEN function_count > 0 THEN '', 31'' ELSE '''' END +
				  CASE WHEN clr_function_count > 0 THEN '', 32'' ELSE '''' END +
				  CASE WHEN PlanCreationTimeHours <= 4 THEN '', 33'' ELSE '''' END +
				  CASE WHEN is_table_variable = 1 THEN '', 34'' ELSE '''' END  + 
				  CASE WHEN no_stats_warning = 1 THEN '', 35'' ELSE '''' END  +
				  CASE WHEN relop_warnings = 1 THEN '', 36'' ELSE '''' END +
				  CASE WHEN is_table_scan = 1 THEN '', 37'' ELSE '''' END +
				  CASE WHEN backwards_scan = 1 THEN '', 38'' ELSE '''' END + 
				  CASE WHEN forced_index = 1 THEN '', 39'' ELSE '''' END +
				  CASE WHEN forced_seek = 1 OR forced_scan = 1 THEN '', 40'' ELSE '''' END +
				  CASE WHEN columnstore_row_mode = 1 THEN '', 41'' ELSE '''' END + 
				  CASE WHEN is_computed_scalar = 1 THEN '', 42'' ELSE '''' END +
				  CASE WHEN is_sort_expensive = 1 THEN '', 43'' ELSE '''' END +
				  CASE WHEN is_computed_filter = 1 THEN '', 44'' ELSE '''' END + 
				  CASE WHEN index_ops >= 5 THEN  '', 45'' ELSE '''' END +
				  CASE WHEN is_row_level = 1 THEN  '', 46'' ELSE '''' END +
				  CASE WHEN is_spatial = 1 THEN '', 47'' ELSE '''' END +
				  CASE WHEN index_dml = 1 THEN '', 48'' ELSE '''' END +
				  CASE WHEN table_dml = 1 THEN '', 49'' ELSE '''' END + 
				  CASE WHEN long_running_low_cpu = 1 THEN '', 50'' ELSE '''' END +
				  CASE WHEN low_cost_high_cpu = 1 THEN '', 51'' ELSE '''' END + 
				  CASE WHEN stale_stats = 1 THEN '', 52'' ELSE '''' END +
				  CASE WHEN is_adaptive = 1 THEN '', 53'' ELSE '''' END	+
				  CASE WHEN is_spool_expensive = 1 THEN + '', 54'' ELSE '''' END +
				  CASE WHEN is_spool_more_rows = 1 THEN + '', 55'' ELSE '''' END  +
				  CASE WHEN is_bad_estimate = 1 THEN + '', 56'' ELSE '''' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN '', 57'' ELSE '''' END + 
				  CASE WHEN is_row_goal = 1 THEN '', 58'' ELSE '''' END + 
                  CASE WHEN is_big_spills = 1 THEN '', 59'' ELSE '''' END +
				  CASE WHEN is_mstvf = 1 THEN '', 60'' ELSE '''' END + 
				  CASE WHEN is_mm_join = 1 THEN '', 61'' ELSE '''' END  + 
                  CASE WHEN is_nonsargable = 1 THEN '', 62'' ELSE '''' END + 
				  CASE WHEN CompileTime > 5000 THEN '', 63 '' ELSE '''' END +
				  CASE WHEN CompileCPU > 5000 THEN '', 64 '' ELSE '''' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN '', 65 '' ELSE '''' END
				  , 3, 200000) AS opserver_warning , ' + @nl ;
    END;
    
    SET @columns += N'        ExecutionCount AS [# Executions],
        ExecutionsPerMinute AS [Executions / Minute],
        PercentExecutions AS [Execution Weight],
        SerialDesiredMemory AS [Serial Desired Memory],
        SerialRequiredMemory AS [Serial Required Memory],
        TotalCPU AS [Total CPU (ms)],
        AverageCPU AS [Avg CPU (ms)],
        PercentCPU AS [CPU Weight],
        TotalDuration AS [Total Duration (ms)],
        AverageDuration AS [Avg Duration (ms)],
        PercentDuration AS [Duration Weight],
        TotalReads AS [Total Reads],
        AverageReads AS [Average Reads],
        PercentReads AS [Read Weight],
        TotalWrites AS [Total Writes],
        AverageWrites AS [Average Writes],
        PercentWrites AS [Write Weight],
        PercentExecutionsByType AS [% Executions (Type)],
        PercentCPUByType AS [% CPU (Type)],
        PercentDurationByType AS [% Duration (Type)],
        PercentReadsByType AS [% Reads (Type)],
        PercentWritesByType AS [% Writes (Type)],
        TotalReturnedRows AS [Total Rows],
        AverageReturnedRows AS [Avg Rows],
        MinReturnedRows AS [Min Rows],
        MaxReturnedRows AS [Max Rows],
		MinGrantKB AS [Minimum Memory Grant KB],
		MaxGrantKB AS [Maximum Memory Grant KB],
		MinUsedGrantKB AS [Minimum Used Grant KB], 
		MaxUsedGrantKB AS [Maximum Used Grant KB],
		AvgMaxMemoryGrant AS [Average Max Memory Grant],
		MinSpills AS [Min Spills],
		MaxSpills AS [Max Spills],
		TotalSpills AS [Total Spills],
		AvgSpills AS [Avg Spills],
        NumberOfPlans AS [# Plans],
        NumberOfDistinctPlans AS [# Distinct Plans],
        PlanCreationTime AS [Created At],
        LastExecutionTime AS [Last Execution],
        CachedPlanSize AS [Cached Plan Size (KB)],
        CompileTime AS [Compile Time (ms)],
        CompileCPU AS [Compile CPU (ms)],
        CompileMemory AS [Compile memory (KB)],
        COALESCE(SetOptions, '''') AS [SET Options],
		PlanHandle AS [Plan Handle], 
		SqlHandle AS [SQL Handle], 
		[SQL Handle More Info],
        QueryHash AS [Query Hash],
		[Query Hash More Info],
        QueryPlanHash AS [Query Plan Hash],
        StatementStartOffset,
        StatementEndOffset,
		[Remove Plan Handle From Cache],
		[Remove SQL Handle From Cache]';
END;



SET @sql = N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT  TOP (@Top) ' + @columns + @nl + N'
FROM    ##bou_BlitzCacheProcs
WHERE   SPID = @spid ' + @nl;

IF @MinimumExecutionCount IS NOT NULL
	BEGIN
		SET @sql += N' AND ExecutionCount >= @minimumExecutionCount ' + @nl;
	END;

IF @MinutesBack IS NOT NULL
    BEGIN
		SET @sql += N' AND LastExecutionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ' + @nl;
    END;

SELECT @sql += N' ORDER BY ' + CASE @SortOrder WHEN  N'cpu' THEN N' TotalCPU '
                                                WHEN N'reads' THEN N' TotalReads '
                                                WHEN N'writes' THEN N' TotalWrites '
                                                WHEN N'duration' THEN N' TotalDuration '
                                                WHEN N'executions' THEN N' ExecutionCount '
                                                WHEN N'compiles' THEN N' PlanCreationTime '
												WHEN N'memory grant' THEN N' MaxGrantKB'
												WHEN N'spills' THEN N' MaxSpills'
                                                WHEN N'avg cpu' THEN N' AverageCPU'
                                                WHEN N'avg reads' THEN N' AverageReads'
                                                WHEN N'avg writes' THEN N' AverageWrites'
                                                WHEN N'avg duration' THEN N' AverageDuration'
                                                WHEN N'avg executions' THEN N' ExecutionsPerMinute'
												WHEN N'avg memory grant' THEN N' AvgMaxMemoryGrant'
												WHEN N'avg spills' THEN N' AvgSpills'
                               END + N' DESC ';
SET @sql += N' OPTION (RECOMPILE) ; ';

IF @Debug = 1
    BEGIN
        PRINT SUBSTRING(@sql, 0, 4000);
        PRINT SUBSTRING(@sql, 4000, 8000);
        PRINT SUBSTRING(@sql, 8000, 12000);
        PRINT SUBSTRING(@sql, 12000, 16000);
        PRINT SUBSTRING(@sql, 16000, 20000);
        PRINT SUBSTRING(@sql, 20000, 24000);
        PRINT SUBSTRING(@sql, 24000, 28000);
        PRINT SUBSTRING(@sql, 28000, 32000);
        PRINT SUBSTRING(@sql, 32000, 36000);
        PRINT SUBSTRING(@sql, 36000, 40000);
    END;

EXEC sp_executesql @sql, N'@Top INT, @spid INT, @minimumExecutionCount INT, @min_back INT', @Top, @@SPID, @MinimumExecutionCount, @MinutesBack;

IF @HideSummary = 0 AND @ExportToExcel = 0
BEGIN
    IF @Reanalyze = 0
    BEGIN
        RAISERROR('Building query plan summary data.', 0, 1) WITH NOWAIT;

        /* Build summary data */
        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE frequent_execution = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    1,
                    100,
                    'Execution Pattern',
                    'Frequently Executed Queries',
                    'http://brentozar.com/blitzcache/frequently-executed-queries/',
                    'Queries are being executed more than '
                    + CAST (@execution_threshold AS VARCHAR(5))
                    + ' times per minute. This can put additional load on the server, even when queries are lightweight.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  parameter_sniffing = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    2,
                    50,
                    'Parameterization',
                    'Parameter Sniffing',
                    'http://brentozar.com/blitzcache/parameter-sniffing/',
                    'There are signs of parameter sniffing (wide variance in rows return or time to execute). Investigate query patterns and tune code appropriately.') ;

        /* Forced execution plans */
        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_forced_plan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    3,
                    50,
                    'Parameterization',
                    'Forced Plans',
                    'http://brentozar.com/blitzcache/forced-plans/',
                    'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are cursors in the plan cache. This is neither good nor bad, but it is a thing. Cursors are weird in SQL Server.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_optimistic_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Optimistic Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are optimistic cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_forward_only_cursor = 0
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Non-forward Only Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are non-forward only cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_cursor_dynamic = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Dynamic Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Dynamic Cursors inhibit parallelism!.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_fast_forward_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Fast Forward Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Fast forward cursors inhibit parallelism!.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_forced_parameterized = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    5,
                    50,
                    'Parameterization',
                    'Forced Parameterization',
                    'http://brentozar.com/blitzcache/forced-parameterization/',
                    'Execution plans have been compiled with forced parameterization.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_parallel = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    6,
                    200,
                    'Execution Plans',
                    'Parallelism',
                    'http://brentozar.com/blitzcache/parallel-plans-detected/',
                    'Parallel plans detected. These warrant investigation, but are neither good nor bad.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  near_parallel = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    7,
                    200,
                    'Execution Plans',
                    'Nearly Parallel',
                    'http://brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                    'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  plan_warnings = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    8,
                    50,
                    'Execution Plans',
                    'Query Plan Warnings',
                    'http://brentozar.com/blitzcache/query-plan-warnings/',
                    'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  long_running = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    9,
                    50,
                    'Performance',
                    'Long Running Queries',
                    'http://brentozar.com/blitzcache/long-running-queries/',
                    'Long running queries have been found. These are queries with an average duration longer than '
                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 AS VARCHAR(5))
                    + ' second(s). These queries should be investigated for additional tuning options.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.missing_index_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    10,
                    50,
                    'Performance',
                    'Missing Index Request',
                    'http://brentozar.com/blitzcache/missing-index-request/',
                    'Queries found with missing indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.downlevel_estimator = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    13,
                    200,
                    'Cardinality',
                    'Legacy Cardinality Estimator in Use',
                    'http://brentozar.com/blitzcache/legacy-cardinality-estimator/',
                    'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

        IF EXISTS (SELECT 1/0
                   FROM ##bou_BlitzCacheProcs p
                   WHERE implicit_conversions = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    14,
                    50,
                    'Performance',
                    'Implicit Conversions',
                    'http://brentozar.com/go/implicit',
                    'One or more queries are comparing two fields that are not of the same data type.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  busy_loops = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                16,
                100,
                'Performance',
                'Busy Loops',
                'http://brentozar.com/blitzcache/busy-loops/',
                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  tvf_join = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                17,
                50,
                'Performance',
                'Joining to table valued functions',
                'http://brentozar.com/blitzcache/tvf-join/',
                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  compile_timeout = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                18,
                50,
                'Execution Plans',
                'Compilation timeout',
                'http://brentozar.com/blitzcache/compilation-timeout/',
                'Query compilation timed out for one or more queries. SQL Server did not find a plan that meets acceptable performance criteria in the time allotted so the best guess was returned. There is a very good chance that this plan isn''t even below average - it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  compile_memory_limit_exceeded = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                19,
                50,
                'Execution Plans',
                'Compilation memory limit exceeded',
                'http://brentozar.com/blitzcache/compile-memory-limit-exceeded/',
                'The optimizer has a limited amount of memory available. One or more queries are complex enough that SQL Server was unable to allocate enough memory to fully optimize the query. A best fit plan was found, and it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  warning_no_join_predicate = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                20,
                50,
                'Execution Plans',
                'No join predicate',
                'http://brentozar.com/blitzcache/no-join-predicate/',
                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  plan_multiple_plans = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                21,
                200,
                'Execution Plans',
                'Multiple execution plans',
                'http://brentozar.com/blitzcache/multiple-plans/',
                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  unmatched_index_count > 0
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                22,
                100,
                'Performance',
                'Unmatched indexes',
                'http://brentozar.com/blitzcache/unmatched-indexes',
                'An index could have been used, but SQL Server chose not to use it - likely due to parameterization and filtered indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  unparameterized_query = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                23,
                100,
                'Parameterization',
                'Unparameterized queries',
                'http://brentozar.com/blitzcache/unparameterized-queries',
                'Unparameterized queries found. These could be ad hoc queries, data exploration, or queries using "OPTIMIZE FOR UNKNOWN".');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_trivial = 1
				   AND SPID = @@SPID)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                24,
                100,
                'Execution Plans',
                'Trivial Plans',
                'http://brentozar.com/blitzcache/trivial-plans',
                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');
    
        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_forced_serial= 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    25,
                    10,
                    'Execution Plans',
                    'Forced Serialization',
                    'http://www.brentozar.com/blitzcache/forced-serialization/',
                    'Something in your plan is forcing a serial query. Further investigation is needed if this is not by design.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_key_lookup_expensive= 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    26,
                    100,
                    'Execution Plans',
                    'Expensive Key Lookups',
                    'http://www.brentozar.com/blitzcache/expensive-key-lookups/',
                    'There''s a key lookup in your plan that costs >=50% of the total plan cost.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_remote_query_expensive= 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    28,
                    100,
                    'Execution Plans',
                    'Expensive Remote Query',
                    'http://www.brentozar.com/blitzcache/expensive-remote-query/',
                    'There''s a remote query in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.trace_flags_session IS NOT NULL
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    29,
                    200,
                    'Trace Flags',
                    'Session Level Trace Flags Enabled',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'Someone is enabling session level Trace Flags in a query.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_unused_grant IS NOT NULL
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    30,
                    100,
                    'Unused memory grants',
                    'Queries are asking for more memory than they''re using',
                    'https://www.brentozar.com/blitzcache/unused-memory-grants/',
                    'Queries have large unused memory grants. This can cause concurrency issues, if queries are waiting a long time to get memory to run.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.function_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    31,
                    100,
                    'Compute Scalar That References A Function',
                    'This could be trouble if you''re using Scalar Functions or MSTVFs',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'Both of these will force queries to run serially, run at least once per row, and may result in poor cardinality estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.clr_function_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    32,
                    100,
                    'Compute Scalar That References A CLR Function',
                    'This could be trouble if your CLR functions perform data access',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'May force queries to run serially, run at least once per row, and may result in poor cardinality estimates.') ;


        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_table_variable = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    33,
                    100,
                    'Table Variables detected',
                    'Beware nasty side effects',
                    'https://www.brentozar.com/blitzcache/table-variables/',
                    'All modifications are single threaded, and selects have really low row estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.no_stats_warning = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    35,
                    100,
                    'Columns with no statistics',
                    'Poor cardinality estimates may ensue',
                    'https://www.brentozar.com/blitzcache/columns-no-statistics/',
                    'Sometimes this happens with indexed views, other times because auto create stats is turned off.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.relop_warnings = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    36,
                    100,
                    'Operator Warnings',
                    'SQL is throwing operator level plan warnings',
                    'http://brentozar.com/blitzcache/query-plan-warnings/',
                    'Check the plan for more details.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_table_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    37,
                    100,
                    'Table Scans',
                    'Your database has HEAPs',
                    'https://www.brentozar.com/archive/2012/05/video-heaps/',
                    'This may not be a problem. Run sp_BlitzIndex for more information.') ;
        
		IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.backwards_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    38,
                    200,
                    'Backwards Scans',
                    'Indexes are being read backwards',
                    'https://www.brentozar.com/blitzcache/backwards-scans/',
                    'This isn''t always a problem. They can cause serial zones in plans, and may need an index to match sort order.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.forced_index = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    39,
                    100,
                    'Index forcing',
                    'Someone is using hints to force index usage',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans, and will prevent missing index requests.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.forced_seek = 1
				   OR p.forced_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    40,
                    100,
                    'Seek/Scan forcing',
                    'Someone is using hints to force index seeks/scans',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.columnstore_row_mode = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    41,
                    100,
                    'ColumnStore indexes operating in Row Mode',
                    'Batch Mode is optimal for ColumnStore indexes',
                    'https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/',
                    'ColumnStore indexes operating in Row Mode indicate really poor query choices.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.is_computed_scalar = 1
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    42,
                    50,
                    'Computed Columns Referencing Scalar UDFs',
                    'This makes a whole lot of stuff run serially',
                    'https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/',
                    'This can cause a whole mess of bad serializartion problems.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_sort_expensive = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     43,
                     100,
                     'Execution Plans',
                     'Expensive Sort',
                     'http://www.brentozar.com/blitzcache/expensive-sorts/',
                     'There''s a sort in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_computed_filter = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     44,
                     50,
                     'Filters Referencing Scalar UDFs',
                     'This forces serialization',
                     'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                     'Someone put a Scalar UDF in the WHERE clause!') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.index_ops >= 5
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     45,
                     100,
                     'Many Indexes Modified',
                     'Write Queries Are Hitting >= 5 Indexes',
                     'https://www.brentozar.com/blitzcache/many-indexes-modified/',
                     'This can cause lots of hidden I/O -- Run sp_BlitzIndex for more information.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_row_level = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     46,
                     200,
                     'Plan Confusion',
                     'Row Level Security is in use',
                     'https://www.brentozar.com/blitzcache/row-level-security/',
                     'You may see a lot of confusing junk in your query plan.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_spatial = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     47,
                     200,
                     'Spatial Abuse',
                     'You hit a Spatial Index',
                     'https://www.brentozar.com/blitzcache/spatial-indexes/',
                     'Purely informational.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.index_dml = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     48,
                     150,
                     'Index DML',
                     'Indexes were created or dropped',
                     'https://www.brentozar.com/blitzcache/index-dml/',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.table_dml = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     49,
                     150,
                     'Table DML',
                     'Tables were created or dropped',
                     'https://www.brentozar.com/blitzcache/table-dml/',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.long_running_low_cpu = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     50,
                     150,
                     'Long Running Low CPU',
                     'You have a query that runs for much longer than it uses CPU',
                     'https://www.brentozar.com/blitzcache/long-running-low-cpu/',
                     'This can be a sign of blocking, linked servers, or poor client application code (ASYNC_NETWORK_IO).') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.low_cost_high_cpu = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     51,
                     150,
                     'Low Cost Query With High CPU',
                     'You have a low cost query that uses a lot of CPU',
                     'https://www.brentozar.com/blitzcache/low-cost-high-cpu/',
                     'This can be a sign of functions or Dynamic SQL that calls black-box code.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.stale_stats = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     52,
                     150,
                     'Biblical Statistics',
                     'Statistics used in queries are >7 days old with >100k modifications',
                     'https://www.brentozar.com/blitzcache/stale-statistics/',
                     'Ever heard of updating statistics?') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_adaptive = 1
  					AND SPID = @@SPID)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     53,
                     200,
                     'Adaptive joins',
                     'This is pretty cool -- you''re living in the future.',
                     'https://www.brentozar.com/blitzcache/adaptive-joins/',
                     'Joe Sack rules.') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_spool_expensive = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     54,
                     150,
                     'Expensive Index Spool',
                     'You have an index spool, this is usually a sign that there''s an index missing somewhere.',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_spool_more_rows = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     55,
                     150,
                     'Index Spools Many Rows',
                     'You have an index spool that spools more rows than the query returns',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;
					 
        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_bad_estimate = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     56,
                     100,
                     'Potentially bad cardinality estimates',
                     'Estimated rows are different from average rows by a factor of 10000',
                     'https://www.brentozar.com/blitzcache/bad-estimates/',
                     'This may indicate a performance problem if mismatches occur regularly') ;	
					 					 						 				 
        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_paul_white_electric = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     57,
                     200,
                     'Is Paul White Electric?',
                     'This query has a Switch operator in it!',
                     'http://sqlblog.com/blogs/paul_white/archive/2013/06/11/hello-operator-my-switch-is-bored.aspx',
                     'You should email this query plan to Paul: SQLkiwi at gmail dot com') ;	

		IF @v >= 14 OR (@v = 13 AND @build >= 5026)
			BEGIN	

				INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT 
				@@SPID,
				999,
				200,
				'Database Level Statistics',
				'The database ' + sa.[Database] + ' last had a stats update on '  + CONVERT(NVARCHAR(10), CONVERT(DATE, MAX(sa.LastUpdate))) + ' and has ' + CONVERT(NVARCHAR(10), AVG(sa.ModificationCount)) + ' modifications on average.' AS [Finding],
				'https://www.brentozar.com/blitzcache/stale-statistics/' AS URL,
				'Consider updating statistics more frequently,' AS [Details]
				FROM #stats_agg AS sa
				GROUP BY sa.[Database]
				HAVING MAX(sa.LastUpdate) <= DATEADD(DAY, -7, SYSDATETIME())
				AND AVG(sa.ModificationCount) >= 100000;

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_row_goal = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     58,
                     200,
                     'Row Goals',
                     'This query had row goals introduced',
                     'https://www.brentozar.com/go/rowgoals/',
                     'This can be good or bad, and should be investigated for high read queries') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_big_spills = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     59,
                     100,
                     'tempdb Spills',
                     'This query spills >500mb to tempdb on average',
                     'https://www.brentozar.com/blitzcache/tempdb-spills/',
                     'One way or another, this query didn''t get enough memory') ;	


			END; 

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_mstvf = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     60,
                     100,
                     'MSTVFs',
                     'These have many of the same problems scalar UDFs have',
                     'http://brentozar.com/blitzcache/tvf-join/',
					 'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_mm_join = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     61,
                     100,
                     'Many to Many Merge',
                     'These use secret worktables that could be doing lots of reads',
                     'https://www.brentozar.com/archive/2018/04/many-mysteries-merge-joins/',
					 'Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  p.is_nonsargable = 1
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     62,
                     50,
                     'Non-SARGable queries',
                     'Queries may have non-SARGable predicates',
                     'http://brentozar.com/go/sargable',
					 'Looks for intrinsic functions and expressions as predicates, and leading wildcard LIKE searches.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  CompileTime > 5000
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     63,
                     100,
                     'High Compile Time',
                     'Queries taking >5 seconds to compile',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'This can be normal for large plans, but be careful if they compile frequently');	

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  CompileCPU > 5000
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     64,
                     50,
                     'High Compile CPU',
                     'Queries taking >5 seconds of CPU to compile',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'If CPU is high and plans like this compile frequently, they may be related');

        IF EXISTS (SELECT 1/0
                    FROM   ##bou_BlitzCacheProcs p
                    WHERE  CompileMemory > 1024 
					AND    ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10.
  					)
             INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     65,
                     50,
                     'High Compile Memory',
                     'Queries taking 10% of Max Compile Memory',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'If you see high RESOURCE_SEMAPHORE_QUERY_COMPILE waits, these may be related');

        IF EXISTS (SELECT 1/0
                   FROM   #plan_creation p
                   WHERE (p.percent_24 > 0)
				   AND SPID = @@SPID)
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT SPID,
                    999,
                    254,
                    'Plan Cache Information',
                    'You have ' + CONVERT(NVARCHAR(10), ISNULL(p.total_plans, 0)) 
								+ ' total plans in your cache, with ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_24, 0)) 
								+ '% plans created in the past 24 hours, ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_4, 0)) 
								+ '% created in the past 4 hours, and ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_1, 0)) 
								+ '% created in the past 1 hour.',
                    '',
                    'If these percentages are high, it may be a sign of memory pressure or plan cache instability.'
			FROM   #plan_creation p	;
		
		IF @v >= 11
		BEGIN	
        IF EXISTS (SELECT 1/0
                   FROM   #trace_flags AS tf 
                   WHERE  tf.global_trace_flags IS NOT NULL
				   )
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    1000,
                    255,
                    'Global Trace Flags Enabled',
                    'You have Global Trace Flags enabled on your server',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'You have the following Global Trace Flags enabled: ' + (SELECT TOP 1 tf.global_trace_flags FROM #trace_flags AS tf WHERE tf.global_trace_flags IS NOT NULL)) ;
		END; 

        IF NOT EXISTS (SELECT 1/0
					   FROM   ##bou_BlitzCacheResults AS bcr
                       WHERE  bcr.Priority = 2147483646
				      )
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    2147483646,
                    255,
                    'Need more help?' ,
                    'Paste your plan on the internet!',
                    'http://pastetheplan.com',
                    'This makes it easy to share plans and post them to Q&A sites like https://dba.stackexchange.com/!') ;



        IF NOT EXISTS (SELECT 1/0
					   FROM   ##bou_BlitzCacheResults AS bcr
                       WHERE  bcr.Priority = 2147483647
				      )
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    2147483647,
                    255,
                    'Thanks for using sp_BlitzCache!' ,
                    'From Your Community Volunteers',
                    'http://FirstResponderKit.org',
                    'We hope you found this tool useful. Current version: ' + @Version + ' released on ' + CONVERT(NVARCHAR(30), @VersionDate) + '.') ;
	
		END;            
    
	
    SELECT  Priority,
            FindingsGroup,
            Finding,
            URL,
            Details,
            CheckID
    FROM    ##bou_BlitzCacheResults
    WHERE   SPID = @@SPID
    GROUP BY Priority,
            FindingsGroup,
            Finding,
            URL,
            Details,
            CheckID
    ORDER BY Priority ASC, FindingsGroup, Finding, CheckID ASC
    OPTION (RECOMPILE);
END;

IF @Debug = 1
    BEGIN
		
		SELECT '##bou_BlitzCacheResults' AS table_name, *
		FROM   ##bou_BlitzCacheResults
		OPTION ( RECOMPILE );
		
		SELECT '##bou_BlitzCacheProcs' AS table_name, *
		FROM   ##bou_BlitzCacheProcs
		OPTION ( RECOMPILE );
		
		SELECT '#statements' AS table_name,  *
		FROM #statements AS s
		OPTION (RECOMPILE);

		SELECT '#query_plan' AS table_name,  *
		FROM #query_plan AS qp
		OPTION (RECOMPILE);
		
		SELECT '#relop' AS table_name,  *
		FROM #relop AS r
		OPTION (RECOMPILE);

		SELECT '#only_query_hashes' AS table_name, *
		FROM   #only_query_hashes
		OPTION ( RECOMPILE );
		
		SELECT '#ignore_query_hashes' AS table_name, *
		FROM   #ignore_query_hashes
		OPTION ( RECOMPILE );
		
		SELECT '#only_sql_handles' AS table_name, *
		FROM   #only_sql_handles
		OPTION ( RECOMPILE );
		
		SELECT '#ignore_sql_handles' AS table_name, *
		FROM   #ignore_sql_handles
		OPTION ( RECOMPILE );
		
		SELECT '#p' AS table_name, *
		FROM   #p
		OPTION ( RECOMPILE );
		
		SELECT '#checkversion' AS table_name, *
		FROM   #checkversion
		OPTION ( RECOMPILE );
		
		SELECT '#configuration' AS table_name, *
		FROM   #configuration
		OPTION ( RECOMPILE );
		
		SELECT '#stored_proc_info' AS table_name, *
		FROM   #stored_proc_info
		OPTION ( RECOMPILE );

		SELECT '#conversion_info' AS table_name, *
		FROM #conversion_info AS ci
		OPTION ( RECOMPILE );
		
		SELECT '#variable_info' AS table_name, *
		FROM #variable_info AS vi
		OPTION ( RECOMPILE );

		SELECT '#missing_index_xml' AS table_name, *
		FROM #missing_index_xml AS mix
		OPTION ( RECOMPILE );

		SELECT '#missing_index_schema' AS table_name, *
		FROM #missing_index_schema AS mis
		OPTION ( RECOMPILE );

		SELECT '#missing_index_usage' AS table_name, *
		FROM #missing_index_usage AS miu
		OPTION ( RECOMPILE );

		SELECT '#missing_index_detail' AS table_name, *
		FROM #missing_index_detail AS mid
		OPTION ( RECOMPILE );

		SELECT '#missing_index_pretty' AS table_name, *
		FROM #missing_index_pretty AS mip
		OPTION ( RECOMPILE );

		SELECT '#plan_creation' AS table_name, *
		FROM   #plan_creation
		OPTION ( RECOMPILE );
		
		SELECT '#plan_cost' AS table_name, *
		FROM   #plan_cost
		OPTION ( RECOMPILE );
		
		SELECT '#proc_costs' AS table_name, *
		FROM   #proc_costs
		OPTION ( RECOMPILE );
		
		SELECT '#stats_agg' AS table_name, *
		FROM   #stats_agg
		OPTION ( RECOMPILE );
		
		SELECT '#trace_flags' AS table_name, *
		FROM   #trace_flags
		OPTION ( RECOMPILE );

    END;


RETURN; --Avoid going into the AllSort GOTO

/*Begin code to sort by all*/
AllSorts:
RAISERROR('Beginning all sort loop', 0, 1) WITH NOWAIT;


IF (
     @Top > 10
     AND @BringThePain = 0
   )
   BEGIN
         RAISERROR(
				  '		  
		  You''ve chosen a value greater than 10 to sort the whole plan cache by. 
		  That can take a long time and harm performance. 
		  Please choose a number <= 10, or set @BringThePain = 1 to signify you understand this might be a bad idea.
		          ', 0, 1) WITH NOWAIT;
         RETURN;
   END;


IF OBJECT_ID('tempdb..#checkversion_allsort') IS NULL
   BEGIN
         CREATE TABLE #checkversion_allsort
         (
           version NVARCHAR(128),
           common_version AS SUBSTRING(version, 1, CHARINDEX('.', version) + 1),
           major AS PARSENAME(CONVERT(VARCHAR(32), version), 4),
           minor AS PARSENAME(CONVERT(VARCHAR(32), version), 3),
           build AS PARSENAME(CONVERT(VARCHAR(32), version), 2),
           revision AS PARSENAME(CONVERT(VARCHAR(32), version), 1)
         );

         INSERT INTO #checkversion_allsort
                (version)
         SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))
         OPTION ( RECOMPILE );
   END;


SELECT  @v = common_version,
        @build = build
FROM    #checkversion_allsort
OPTION  ( RECOMPILE );

IF OBJECT_ID('tempdb.. #bou_allsort') IS NULL
   BEGIN
         CREATE TABLE #bou_allsort
         (
           Id INT IDENTITY(1, 1),
           DatabaseName NVARCHAR(128),
           Cost FLOAT,
           QueryText NVARCHAR(MAX),
           QueryType NVARCHAR(258),
           Warnings VARCHAR(MAX),
		   QueryPlan XML,
		   missing_indexes XML,
		   implicit_conversion_info XML,
		   cached_execution_parameters XML,
           ExecutionCount BIGINT,
           ExecutionsPerMinute MONEY,
           ExecutionWeight MONEY,
           TotalCPU BIGINT,
           AverageCPU BIGINT,
           CPUWeight MONEY,
           TotalDuration BIGINT,
           AverageDuration BIGINT,
           DurationWeight MONEY,
           TotalReads BIGINT,
           AverageReads BIGINT,
           ReadWeight MONEY,
           TotalWrites BIGINT,
           AverageWrites BIGINT,
           WriteWeight MONEY,
           AverageReturnedRows MONEY,
           MinGrantKB BIGINT,
           MaxGrantKB BIGINT,
           MinUsedGrantKB BIGINT,
           MaxUsedGrantKB BIGINT,
           AvgMaxMemoryGrant MONEY,
		   MinSpills BIGINT,
		   MaxSpills BIGINT,
		   TotalSpills BIGINT,
		   AvgSpills MONEY,
           PlanCreationTime DATETIME,
           LastExecutionTime DATETIME,
           PlanHandle VARBINARY(64),
           SqlHandle VARBINARY(64),
           SetOptions VARCHAR(MAX),
           Pattern NVARCHAR(20)
         );
   END;

DECLARE @AllSortSql NVARCHAR(MAX) = N'';
DECLARE @MemGrant BIT;
SELECT  @MemGrant = CASE WHEN (
                                ( @v < 11 )
                                OR (
                                     @v = 11
                                     AND @build < 6020
                                   )
                                OR (
                                     @v = 12
                                     AND @build < 5000
                                   )
                                OR (
                                     @v = 13
                                     AND @build < 1601
                                   )
                              ) THEN 0
                         ELSE 1
                    END;

DECLARE @Spills BIT;
SELECT @Spills = CASE WHEN (@v >= 15 OR (@v = 14 AND @build >= 3015) OR (@v = 13 AND @build >= 5026)) THEN 1 ELSE 0 END;
		 

IF LOWER(@SortOrder) = 'all'
BEGIN
RAISERROR('Beginning for ALL', 0, 1) WITH NOWAIT;
SET @AllSortSql += N'
					DECLARE @ISH NVARCHAR(MAX) = N''''

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions ) 					 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''cpu'', @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )					 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''reads'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )		 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''writes'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )				 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''duration'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )				 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''executions'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);
					 
					 '; 
					
					IF @MemGrant = 0
					BEGIN
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

					END; 
					
					IF @MemGrant = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''memory grant'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 					  
										  UPDATE #bou_allsort SET Pattern = ''memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF @Spills = 0
					BEGIN
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

					END; 
					
					IF @Spills = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''spills'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 					  
										  UPDATE #bou_allsort SET Pattern = ''memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;
					SET @AllSortSql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
										  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
										  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
										  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions, Pattern 
										  FROM #bou_allsort 
										  ORDER BY Id 
										  OPTION(RECOMPILE);  ';

				
END; 			


IF LOWER(@SortOrder) = 'all avg'
BEGIN 
RAISERROR('Beginning for ALL AVG', 0, 1) WITH NOWAIT;
SET @AllSortSql += N' 
					DECLARE @ISH NVARCHAR(MAX) = N'''' 
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
					
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg cpu'', @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )		 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg reads'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )	 
					
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg writes'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg duration'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg executions'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);
					 
					 ';
					 
					IF @MemGrant = 0
					BEGIN
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END;  

					END; 
					
					IF @MemGrant = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg memory grant'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 					  
										  UPDATE #bou_allsort SET Pattern = ''avg memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF @Spills = 0
					BEGIN
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END;  

					END; 
					
					IF @Spills = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions )			 
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg spills'', @IgnoreSqlHandles = @ISH, @DatabaseName = @i_DatabaseName WITH RECOMPILE;
					 					  
										  UPDATE #bou_allsort SET Pattern = ''avg memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##bou_BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					SET @AllSortSql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
										  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
										  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
										  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, SetOptions, Pattern
										  FROM #bou_allsort 
										  ORDER BY Id 
										  OPTION(RECOMPILE);  ';
END;

					IF @Debug = 1
						BEGIN
						    PRINT SUBSTRING(@AllSortSql, 0, 4000);
						    PRINT SUBSTRING(@AllSortSql, 4000, 8000);
						    PRINT SUBSTRING(@AllSortSql, 8000, 12000);
						    PRINT SUBSTRING(@AllSortSql, 12000, 16000);
						    PRINT SUBSTRING(@AllSortSql, 16000, 20000);
						    PRINT SUBSTRING(@AllSortSql, 20000, 24000);
						    PRINT SUBSTRING(@AllSortSql, 24000, 28000);
						    PRINT SUBSTRING(@AllSortSql, 28000, 32000);
						    PRINT SUBSTRING(@AllSortSql, 32000, 36000);
						    PRINT SUBSTRING(@AllSortSql, 36000, 40000);
						END;

					EXEC sys.sp_executesql @stmt = @AllSortSql, @params = N'@i_DatabaseName NVARCHAR(128), @i_Top INT', @i_DatabaseName = @DatabaseName, @i_Top = @Top;


/*End of AllSort section*/

END; /*Final End*/

GO
