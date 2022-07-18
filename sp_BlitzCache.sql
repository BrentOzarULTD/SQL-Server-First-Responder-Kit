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

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##BlitzCacheProcs', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##BlitzCacheProcs;');
GO

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##BlitzCacheResults', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##BlitzCacheResults;');
GO

CREATE TABLE ##BlitzCacheResults (
    SPID INT,
    ID INT IDENTITY(1,1),
    CheckID INT,
    Priority TINYINT,
    FindingsGroup VARCHAR(50),
    Finding VARCHAR(500),
    URL VARCHAR(200),
    Details VARCHAR(4000) 
);

CREATE TABLE ##BlitzCacheProcs (
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
		LastCompletionTime DATETIME,
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
		PlanGenerationNum BIGINT,
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
        TotalReadsForType DECIMAL(30),
        TotalExecutionCountForType BIGINT,
        TotalWritesForType DECIMAL(30),
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
        plan_multiple_plans INT,
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
		table_spool_cost FLOAT,
		table_spool_rows FLOAT,
		is_spool_expensive BIT,
		is_spool_more_rows BIT,
		is_table_spool_expensive BIT,
		is_table_spool_more_rows BIT,
		estimated_rows FLOAT,
		is_bad_estimate BIT, 
		is_paul_white_electric BIT,
		is_row_goal BIT,
		is_big_spills BIT,
		is_mstvf BIT,
		is_mm_join BIT,
        is_nonsargable BIT,
		select_with_writes BIT,
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
	@OutputType VARCHAR(20) = 'TABLE' ,
    @OutputServerName NVARCHAR(258) = NULL ,
    @OutputDatabaseName NVARCHAR(258) = NULL ,
    @OutputSchemaName NVARCHAR(258) = NULL ,
    @OutputTableName NVARCHAR(258) = NULL , -- do NOT use ##BlitzCacheResults or ##BlitzCacheProcs as they are used as work tables in this procedure
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
	@SlowlySearchPlansFor NVARCHAR(4000) = NULL,
    @Reanalyze BIT = 0 ,
    @SkipAnalysis BIT = 0 ,
    @BringThePain BIT = 0 ,
    @MinimumExecutionCount INT = 0,
	@Debug BIT = 0,
	@CheckDateOverride DATETIMEOFFSET = NULL,
	@MinutesBack INT = NULL,
	@Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
	@VersionCheckMode BIT = 0
WITH RECOMPILE
AS
BEGIN
SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT @Version = '8.10', @VersionDate = '20220718';
SET @OutputType = UPPER(@OutputType);

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

DECLARE @nl NVARCHAR(2) = NCHAR(13) + NCHAR(10) ;
	
IF @Help = 1 
	BEGIN
	PRINT '
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

	Unknown limitations of this version:
	 - May or may not be vulnerable to the wick effect.

	Changes - for the full list of improvements and fixes in this version, see:
	https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/



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
			N'Data processing and display order. @SortOrder will still be used, even when preparing output for a table or for excel. Possible values are: "CPU", "Reads", "Writes", "Duration", "Executions", "Recent Compilations", "Memory Grant", "Unused Grant", "Spills", "Query Hash". Additionally, the word "Average" or "Avg" can be used to sort on averages rather than total. "Executions per minute" and "Executions / minute" can be used to sort by execution per minute. For the truly lazy, "xpm" can also be used. Note that when you use all or all avg, the only parameters you can use are @Top and @DatabaseName. All others will be ignored.'

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
	SELECT N'@OutputType',
			N'NVARCHAR(258)',
			N'If set to "NONE", this will tell this procedure not to run any query leading to a results set thrown to caller.'
			
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
			N'Ignores plans found in the system databases (master, model, msdb, tempdb, dbmaintenance, dbadmin, dbatools and resourcedb)'

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
	SELECT N'@SlowlySearchPlansFor',
			N'NVARCHAR(4000)',
			N'String to search for in plan text. % wildcards allowed.'

	UNION ALL
	SELECT N'@BringThePain',
			N'BIT',
			N'When using @SortOrder = ''all'' and @Top > 10, we require you to set @BringThePain = 1 so you understand that sp_BlitzCache will take a while to run.'

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
			N'The maximum used memory grant the query received in kb.'

	UNION ALL
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
	END; /* IF @Help = 1  */



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

IF(@OutputType = 'NONE' AND (@OutputTableName IS NULL OR @OutputSchemaName IS NULL OR @OutputDatabaseName IS NULL))
BEGIN
    RAISERROR('This procedure should be called with a value for all @Output* parameters, as @OutputType is set to NONE',12,1);
    RETURN;
END;

IF(@OutputType = 'NONE') 
BEGIN
    SET @HideSummary = 1;
END;


/* Lets get @SortOrder set to lower case here for comparisons later */
SET @SortOrder = LOWER(@SortOrder);

/* If they want to sort by query hash, populate the @OnlyQueryHashes list for them */
IF @SortOrder LIKE 'query hash%'
	BEGIN
	RAISERROR('Beginning query hash sort', 0, 1) WITH NOWAIT;

    SELECT qs.query_hash, 
           MAX(qs.max_worker_time) AS max_worker_time,
           COUNT_BIG(*) AS records
    INTO #query_hash_grouped
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY (   SELECT pa.value
                    FROM   sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
                    WHERE  pa.attribute = 'dbid' ) AS ca
    GROUP BY qs.query_hash, ca.value
    HAVING COUNT_BIG(*) > 1
    ORDER BY max_worker_time DESC,
             records DESC;
    
    SELECT TOP (1)
	         @OnlyQueryHashes = STUFF((SELECT DISTINCT N',' + CONVERT(NVARCHAR(MAX), qhg.query_hash, 1) 
    FROM #query_hash_grouped AS qhg 
    WHERE qhg.query_hash <> 0x00
    FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
	OPTION(RECOMPILE);

	/* When they ran it, @SortOrder probably looked like 'query hash, cpu', so strip the first sort order out: */
    SELECT @SortOrder = LTRIM(REPLACE(REPLACE(@SortOrder,'query hash', ''), ',', ''));
	
	/* If they just called it with @SortOrder = 'query hash', set it to 'cpu' for backwards compatibility: */
	IF @SortOrder = '' SET @SortOrder = 'cpu';

	END


/* Set @Top based on sort */
IF (
     @Top IS NULL
     AND @SortOrder IN ( 'all', 'all sort' )
   )
   BEGIN
         SET @Top = 5;
   END;

IF (
     @Top IS NULL
     AND @SortOrder NOT IN ( 'all', 'all sort' )
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


IF OBJECT_ID('tempdb.dbo.##BlitzCacheResults') IS NULL
BEGIN
    CREATE TABLE ##BlitzCacheResults (
        SPID INT,
        ID INT IDENTITY(1,1),
        CheckID INT,
        Priority TINYINT,
        FindingsGroup VARCHAR(50),
        Finding VARCHAR(500),
        URL VARCHAR(200),
        Details VARCHAR(4000)
    );
END;

IF OBJECT_ID('tempdb.dbo.##BlitzCacheProcs') IS NULL
BEGIN
    CREATE TABLE ##BlitzCacheProcs (
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
		LastCompletionTime DATETIME,
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
		PlanGenerationNum BIGINT,
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
        plan_multiple_plans INT,
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
		table_spool_cost FLOAT,
		table_spool_rows FLOAT,
		is_spool_expensive BIT,
		is_spool_more_rows BIT,
		is_table_spool_expensive BIT,
		is_table_spool_more_rows BIT,
		estimated_rows FLOAT,
		is_bad_estimate BIT, 
		is_paul_white_electric BIT,
		is_row_goal BIT,
		is_big_spills BIT,
		is_mstvf BIT,
		is_mm_join BIT,
        is_nonsargable BIT,
		select_with_writes BIT,
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
		@NoobSaibot BIT = 0,
		@VersionShowsAirQuoteActualPlans BIT,
        @ObjectFullName NVARCHAR(2000),
        @user_perm_sql NVARCHAR(MAX) = N'',
        @user_perm_gb_out DECIMAL(10,2),
        @common_version DECIMAL(10,2),
        @buffer_pool_memory_gb DECIMAL(10,2),
        @user_perm_percent DECIMAL(10,2),
        @is_tokenstore_big BIT = 0,
        @sort NVARCHAR(MAX) = N'',
		@sort_filter NVARCHAR(MAX) = N'';


IF @SortOrder = 'sp_BlitzIndex'
BEGIN
	RAISERROR(N'OUTSTANDING!', 0, 1) WITH NOWAIT;
	SET @SortOrder = 'reads';
	SET @NoobSaibot = 1;

END


/* Change duration from seconds to milliseconds */
IF @DurationFilter IS NOT NULL
  BEGIN
  RAISERROR(N'Converting Duration Filter to milliseconds', 0, 1) WITH NOWAIT;
  SET @DurationFilter_i = CAST((@DurationFilter * 1000.0) AS INT);
  END; 

RAISERROR(N'Checking database validity', 0, 1) WITH NOWAIT;
SET @DatabaseName = LTRIM(RTRIM(@DatabaseName)) ;

IF SERVERPROPERTY('EngineEdition') IN (5, 6) AND DB_NAME() <> @DatabaseName
BEGIN
   RAISERROR('You specified a database name other than the current database, but Azure SQL DB does not allow you to change databases. Execute sp_BlitzCache from the database you want to analyze.', 16, 1);
   RETURN;
END;
IF (DB_ID(@DatabaseName)) IS NULL AND @DatabaseName <> N''
BEGIN
   RAISERROR('The database you specified does not exist. Please check the name and try again.', 16, 1);
   RETURN;
END;
IF (SELECT DATABASEPROPERTYEX(ISNULL(@DatabaseName, 'master'), 'Collation')) IS NULL AND SERVERPROPERTY('EngineEdition') NOT IN (5, 6, 8)
BEGIN
   RAISERROR('The database you specified is not readable. Please check the name and try again. Better yet, check your server.', 16, 1);
   RETURN;
END;

SELECT @MinMemoryPerQuery = CONVERT(INT, c.value) FROM sys.configurations AS c WHERE c.name = 'min memory per query (KB)';

SET @SortOrder = REPLACE(REPLACE(@SortOrder, 'average', 'avg'), '.', '');

SET @SortOrder = CASE 
                     WHEN @SortOrder IN ('executions per minute','execution per minute','executions / minute','execution / minute','xpm') THEN 'avg executions'
                     WHEN @SortOrder IN ('recent compilations','recent compilation','compile') THEN 'compiles'
                     WHEN @SortOrder IN ('read') THEN 'reads'
                     WHEN @SortOrder IN ('avg read') THEN 'avg reads'
                     WHEN @SortOrder IN ('write') THEN 'writes'
                     WHEN @SortOrder IN ('avg write') THEN 'avg writes'
                     WHEN @SortOrder IN ('memory grants') THEN 'memory grant'
                     WHEN @SortOrder IN ('avg memory grants') THEN 'avg memory grant'
                     WHEN @SortOrder IN ('unused grants','unused memory', 'unused memory grant', 'unused memory grants') THEN 'unused grant'
                     WHEN @SortOrder IN ('spill') THEN 'spills'
                     WHEN @SortOrder IN ('avg spill') THEN 'avg spills'
                     WHEN @SortOrder IN ('execution') THEN 'executions'
                 ELSE @SortOrder END							  
							  
RAISERROR(N'Checking sort order', 0, 1) WITH NOWAIT;
IF @SortOrder NOT IN ('cpu', 'avg cpu', 'reads', 'avg reads', 'writes', 'avg writes',
                       'duration', 'avg duration', 'executions', 'avg executions',
                       'compiles', 'memory grant', 'avg memory grant', 'unused grant',
					   'spills', 'avg spills', 'all', 'all avg', 'sp_BlitzIndex',
					   'query hash')
  BEGIN
  RAISERROR(N'Invalid sort order chosen, reverting to cpu', 16, 1) WITH NOWAIT;
  SET @SortOrder = 'cpu';
  END; 

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

DECLARE @AllSortSql NVARCHAR(MAX) = N'';
DECLARE @VersionShowsMemoryGrants BIT;
IF EXISTS(SELECT * FROM sys.all_columns WHERE OBJECT_ID = OBJECT_ID('sys.dm_exec_query_stats') AND name = 'max_grant_kb')
    SET @VersionShowsMemoryGrants = 1;
ELSE
    SET @VersionShowsMemoryGrants = 0;

DECLARE @VersionShowsSpills BIT;
IF EXISTS(SELECT * FROM sys.all_columns WHERE OBJECT_ID = OBJECT_ID('sys.dm_exec_query_stats') AND name = 'max_spills')
    SET @VersionShowsSpills = 1;
ELSE
    SET @VersionShowsSpills = 0;

IF EXISTS(SELECT * FROM sys.all_columns WHERE OBJECT_ID = OBJECT_ID('sys.dm_exec_query_plan_stats') AND name = 'query_plan')
    SET @VersionShowsAirQuoteActualPlans = 1;
ELSE
    SET @VersionShowsAirQuoteActualPlans = 0;

IF @Reanalyze = 1 AND OBJECT_ID('tempdb..##BlitzCacheResults') IS NULL
  BEGIN
  RAISERROR(N'##BlitzCacheResults does not exist, can''t reanalyze', 0, 1) WITH NOWAIT;
  SET @Reanalyze = 0;
  END;

IF @Reanalyze = 0
  BEGIN
  RAISERROR(N'Cleaning up old warnings for your SPID', 0, 1) WITH NOWAIT;
  DELETE ##BlitzCacheResults
    WHERE SPID = @@SPID
	OPTION (RECOMPILE) ;
  RAISERROR(N'Cleaning up old plans for your SPID', 0, 1) WITH NOWAIT;
  DELETE ##BlitzCacheProcs
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
	
IF OBJECT_ID('tempdb..#ReadableDBs') IS NOT NULL 
	DROP TABLE #ReadableDBs;	

IF OBJECT_ID('tempdb..#plan_usage') IS NOT NULL 
	DROP TABLE #plan_usage;	

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
    ModificationCount BIGINT,
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
			   + CASE WHEN [include] IS NOT NULL THEN + N'_Includes' ELSE N'' END 
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


CREATE TABLE #ReadableDBs 
(
database_id INT
);


CREATE TABLE #plan_usage
(
    duplicate_plan_hashes BIGINT NULL,
    percent_duplicate DECIMAL(9, 2) NULL,
    single_use_plan_count BIGINT NULL,
    percent_single DECIMAL(9, 2) NULL,
    total_plans BIGINT NULL,
	spid INT
);


IF EXISTS (SELECT * FROM sys.all_objects o WHERE o.name = 'dm_hadr_database_replica_states')
BEGIN
	RAISERROR('Checking for Read intent databases to exclude',0,0) WITH NOWAIT;

    EXEC('INSERT INTO #ReadableDBs (database_id) SELECT DBs.database_id FROM sys.databases DBs INNER JOIN sys.availability_replicas Replicas ON DBs.replica_id = Replicas.replica_id WHERE replica_server_name NOT IN (SELECT DISTINCT primary_replica FROM sys.dm_hadr_availability_group_states States) AND Replicas.secondary_role_allow_connections_desc = ''READ_ONLY'' AND replica_server_name = @@SERVERNAME OPTION (RECOMPILE);');
END

RAISERROR(N'Checking plan cache age', 0, 1) WITH NOWAIT;
WITH x AS (
SELECT SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 24 THEN 1 ELSE 0 END) AS [plans_24],
	   SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 4 THEN 1 ELSE 0 END) AS [plans_4],
	   SUM(CASE WHEN DATEDIFF(HOUR, deqs.creation_time, SYSDATETIME()) <= 1 THEN 1 ELSE 0 END) AS [plans_1],
	   COUNT(deqs.creation_time) AS [total_plans]
FROM sys.dm_exec_query_stats AS deqs
)
INSERT INTO #plan_creation ( percent_24, percent_4, percent_1, total_plans, SPID )
SELECT CONVERT(DECIMAL(5,2), NULLIF(x.plans_24, 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_24],
	   CONVERT(DECIMAL(5,2), NULLIF(x.plans_4 , 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_4],
	   CONVERT(DECIMAL(5,2), NULLIF(x.plans_1 , 0) / (1. * NULLIF(x.total_plans, 0))) * 100 AS [percent_1],
	   x.total_plans,
	   @@SPID AS SPID
FROM x
OPTION (RECOMPILE);


RAISERROR(N'Checking for single use plans and plans with many queries', 0, 1) WITH NOWAIT;
WITH total_plans AS 
(
    SELECT
	    COUNT_BIG(deqs.query_plan_hash) AS total_plans
    FROM sys.dm_exec_query_stats AS deqs
),
     many_plans AS 
(
    SELECT
	    SUM(x.duplicate_plan_hashes) AS duplicate_plan_hashes
    FROM
	(
        SELECT
		    COUNT_BIG(qs.query_plan_hash) AS duplicate_plan_hashes
        FROM sys.dm_exec_query_stats qs
        LEFT JOIN sys.dm_exec_procedure_stats ps 
            ON qs.sql_handle = ps.sql_handle
        CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
        WHERE pa.attribute = N'dbid'
        AND   qs.query_plan_hash <> 0x0000000000000000
        GROUP BY
		    /* qs.query_plan_hash,  BGO 20210524 commenting this out to fix #2909 */
            qs.query_hash,
			ps.object_id,
            pa.value
        HAVING COUNT_BIG(qs.query_plan_hash) > 5
    ) AS x
),
     single_use_plans AS 
(
    SELECT
	    COUNT_BIG(*) AS single_use_plan_count
    FROM sys.dm_exec_cached_plans AS cp
    WHERE cp.usecounts = 1
    AND   cp.objtype = N'Adhoc'
    AND   EXISTS
	      (
		      SELECT
			      1/0
              FROM sys.configurations AS c
              WHERE c.name = N'optimize for ad hoc workloads'
              AND   c.value_in_use = 0
		  )
    HAVING COUNT_BIG(*) > 1
)
INSERT
    #plan_usage
(
    duplicate_plan_hashes,
	percent_duplicate,
	single_use_plan_count,
	percent_single,
	total_plans,
	spid
)
SELECT
    m.duplicate_plan_hashes, 
    CONVERT
	(
	    decimal(5,2),
		m.duplicate_plan_hashes
		    / (1. * NULLIF(t.total_plans, 0))
    ) * 100. AS percent_duplicate,
    s.single_use_plan_count, 
    CONVERT
	(
	    decimal(5,2),
		s.single_use_plan_count
		    / (1. * NULLIF(t.total_plans, 0))
	) * 100. AS percent_single,
    t.total_plans,
	@@SPID
FROM many_plans AS m
CROSS JOIN single_use_plans AS s 
CROSS JOIN total_plans AS t;


/*
Erik Darling:
   Quoting this out to see if the above query fixes the issue
   2021-05-17, Issue #2909

UPDATE #plan_usage
	SET percent_duplicate = CASE WHEN percent_duplicate > 100 THEN 100 ELSE percent_duplicate END,
	percent_single = CASE WHEN percent_duplicate > 100 THEN 100 ELSE percent_duplicate END;
*/

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
	
    DECLARE @function_search_sql NVARCHAR(MAX) = N''
    
    INSERT #only_sql_handles
	        ( sql_handle )
	SELECT  ISNULL(deps.sql_handle, CONVERT(VARBINARY(64),'0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'))
	FROM sys.dm_exec_procedure_stats AS deps
	WHERE OBJECT_NAME(deps.object_id, deps.database_id) = @StoredProcName

    UNION ALL
    
    SELECT  ISNULL(dets.sql_handle, CONVERT(VARBINARY(64),'0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'))
	FROM sys.dm_exec_trigger_stats AS dets
	WHERE OBJECT_NAME(dets.object_id, dets.database_id) = @StoredProcName
	OPTION (RECOMPILE);

    IF EXISTS (SELECT 1/0 FROM sys.all_objects AS o WHERE o.name = 'dm_exec_function_stats')
        BEGIN
         SET @function_search_sql = @function_search_sql + N'
         SELECT  ISNULL(defs.sql_handle, CONVERT(VARBINARY(64),''0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000''))
	     FROM sys.dm_exec_function_stats AS defs
	     WHERE OBJECT_NAME(defs.object_id, defs.database_id) = @i_StoredProcName
         OPTION (RECOMPILE);
         '
        INSERT #only_sql_handles ( sql_handle )
        EXEC sys.sp_executesql @function_search_sql, N'@i_StoredProcName NVARCHAR(128)', @StoredProcName
       END
		
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

IF (@SortOrder IN ('memory grant', 'avg memory grant')) AND @VersionShowsMemoryGrants = 0
BEGIN
   RAISERROR('Your version of SQL does not support sorting by memory grant or average memory grant. Please use another sort order.', 16, 1);
   RETURN;
END;

IF (@SortOrder IN ('spills', 'avg spills') AND @VersionShowsSpills = 0)
BEGIN
   RAISERROR('Your version of SQL does not support sorting by spills. Please use another sort order.', 16, 1);
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
INSERT INTO ##BlitzCacheProcs (SPID, QueryType, DatabaseName, AverageCPU, TotalCPU, AverageCPUPerMinute, PercentCPUByType, PercentDurationByType,
                    PercentReadsByType, PercentExecutionsByType, AverageDuration, TotalDuration, AverageReads, TotalReads, ExecutionCount,
                    ExecutionsPerMinute, TotalWrites, AverageWrites, PercentWritesByType, WritesPerMinute, PlanCreationTime,
                    LastExecutionTime, LastCompletionTime, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows,
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


IF @VersionShowsAirQuoteActualPlans = 1
    BEGIN
    SET @body += N'     CROSS APPLY sys.dm_exec_query_plan_stats(x.plan_handle) AS deqps ' + @nl ;
    END

SET @body += N'        WHERE  1 = 1 ' +  @nl ;

	IF EXISTS (SELECT * FROM sys.all_objects o WHERE o.name = 'dm_hadr_database_replica_states')
    BEGIN
    RAISERROR(N'Ignoring readable secondaries databases by default', 0, 1) WITH NOWAIT;
    SET @body += N'               AND CAST(xpa.value AS INT) NOT IN (SELECT database_id FROM #ReadableDBs)' + @nl ;
    END

IF @IgnoreSystemDBs = 1
    BEGIN
	RAISERROR(N'Ignoring system databases by default', 0, 1) WITH NOWAIT;
	SET @body += N'               AND COALESCE(LOWER(DB_NAME(CAST(xpa.value AS INT))), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'', ''dbmaintenance'', ''dbadmin'', ''dbatools'') AND COALESCE(DB_NAME(CAST(xpa.value AS INT)), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;
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
	SET @body += N'       AND DATEADD(SECOND, (x.last_elapsed_time / 1000000.), x.last_execution_time) >= DATEADD(MINUTE, @min_back, GETDATE()) ' + @nl ;
	END;

IF @SlowlySearchPlansFor IS NOT NULL
    BEGIN
    RAISERROR(N'Setting string search for @SlowlySearchPlansFor, so remember, this is gonna be slow', 0, 1) WITH NOWAIT;
    SET @SlowlySearchPlansFor = REPLACE((REPLACE((REPLACE((REPLACE(@SlowlySearchPlansFor, N'[', N'_')), N']', N'_')), N'^', N'_')), N'''', N'''''');
    SET @body_where += N'       AND CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N''%' + @SlowlySearchPlansFor + N'%'' ' + @nl;
    END


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
								 WHEN N'unused grant' THEN N'max_grant_kb - max_used_grant_kb'
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
                         SUM(CAST(total_logical_reads AS DECIMAL(30))) AS t_TotalReads,
                         SUM(CAST(total_logical_writes AS DECIMAL(30))) AS t_TotalWrites
                  FROM   sys.#view#) AS t
       CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
       CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
       CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp ' + @nl ;

IF @VersionShowsAirQuoteActualPlans = 1
    BEGIN
    SET @body += N'     CROSS APPLY sys.dm_exec_query_plan_stats(qs.plan_handle) AS deqps ' + @nl ;
    END

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
	   DATEADD(SECOND, (qs.last_elapsed_time / 1000000.), qs.last_execution_time) AS LastCompletionTime,
       NULL AS StatementStartOffset,
       NULL AS StatementEndOffset,
	   NULL AS PlanGenerationNum, 
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

    IF @VersionShowsSpills = 1
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
	 N'st.text AS QueryText ,';

    IF @VersionShowsAirQuoteActualPlans = 1
        BEGIN
        SET @plans_triggers_select_list += N' CASE WHEN DATALENGTH(COALESCE(deqps.query_plan,'''')) > DATALENGTH(COALESCE(qp.query_plan,'''')) THEN deqps.query_plan ELSE qp.query_plan END AS QueryPlan, ' + @nl ;
        END;
    ELSE   
        BEGIN
        SET @plans_triggers_select_list += N' qp.query_plan AS QueryPlan, ' + @nl ;
        END;

	SET @plans_triggers_select_list +=  
    N't.t_TotalWorker,
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
		   DATEADD(SECOND, (qs.last_elapsed_time / 1000000.), qs.last_execution_time) AS LastCompletionTime,
           qs.statement_start_offset AS StatementStartOffset,
           qs.statement_end_offset AS StatementEndOffset,
		   qs.plan_generation_num AS PlanGenerationNum, ';
    
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

    IF @VersionShowsMemoryGrants = 1
    BEGIN
        RAISERROR(N'Getting memory grant information for newer versions of SQL', 0, 1) WITH NOWAIT;
		SET @sql += N'
           min_grant_kb AS MinGrantKB,
           max_grant_kb AS MaxGrantKB,
           min_used_grant_kb AS MinUsedGrantKB,
           max_used_grant_kb AS MaxUsedGrantKB,
           CAST(ISNULL(NULLIF(( max_used_grant_kb * 1.00 ), 0) / NULLIF(min_grant_kb, 0), 0) * 100. AS MONEY) AS PercentMemoryGrantUsed,
		   CAST(ISNULL(NULLIF(( total_grant_kb * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgMaxMemoryGrant, ';
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

	IF @VersionShowsSpills = 1
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
                                                                          END - qs.statement_start_offset ) / 2 ) + 1) AS QueryText , ' + @nl ;


    IF @VersionShowsAirQuoteActualPlans = 1
        BEGIN
        SET @sql += N'           CASE WHEN DATALENGTH(COALESCE(deqps.query_plan,'''')) > DATALENGTH(COALESCE(qp.query_plan,'''')) THEN deqps.query_plan ELSE qp.query_plan END AS QueryPlan, ' + @nl ;
        END
    ELSE
        BEGIN
        SET @sql += N'           query_plan AS QueryPlan, ' + @nl ;
        END

    SET @sql += N'
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

	SET @sort_filter += CASE @SortOrder  WHEN N'cpu' THEN N'AND total_worker_time > 0'
                                WHEN N'reads' THEN N'AND total_logical_reads > 0'
                                WHEN N'writes' THEN N'AND total_logical_writes > 0'
                                WHEN N'duration' THEN N'AND total_elapsed_time > 0'
                                WHEN N'executions' THEN N'AND execution_count > 0'
                                /* WHEN N'compiles' THEN N'AND (age_minutes + age_minutes_lifetime) > 0'  BGO 2021-01-24 commenting out for https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2772 */
								WHEN N'memory grant' THEN N'AND max_grant_kb > 0'
								WHEN N'unused grant' THEN N'AND max_grant_kb > 0'
								WHEN N'spills' THEN N'AND max_spills > 0'
                                /* And now the averages */
                                WHEN N'avg cpu' THEN N'AND (total_worker_time / execution_count) > 0'
                                WHEN N'avg reads' THEN N'AND (total_logical_reads / execution_count) > 0'
                                WHEN N'avg writes' THEN N'AND (total_logical_writes / execution_count) > 0'
                                WHEN N'avg duration' THEN N'AND (total_elapsed_time / execution_count) > 0'
								WHEN N'avg memory grant' THEN N'AND CASE WHEN max_grant_kb = 0 THEN 0 ELSE (max_grant_kb / execution_count) END > 0'
                                WHEN N'avg spills' THEN N'AND CASE WHEN total_spills = 0 THEN 0 ELSE (total_spills / execution_count) END > 0'
                                WHEN N'avg executions' THEN N'AND CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END > 0'
            ELSE N' /* No minimum threshold set */ '
               END;

    SET @sql += REPLACE(@body_where, 'cached_time', 'creation_time') ;

	SET @sql += @sort_filter + @nl;
    
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
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant', 'unused grant'))
   OR (LEFT(@QueryFilter, 3) = 'pro')
BEGIN
    SET @sql += @insert_list;
    SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Stored Procedure') ;

    SET @sql += REPLACE(@body, '#view#', 'dm_exec_procedure_stats') ; 
    SET @sql += @body_where ;

    IF @IgnoreSystemDBs = 1
       SET @sql += N' AND COALESCE(LOWER(DB_NAME(database_id)), LOWER(CAST(pa.value AS sysname)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'', ''dbmaintenance'', ''dbadmin'', ''dbatools'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;

	SET @sql += @sort_filter + @nl;

	SET @sql += @body_order + @nl + @nl + @nl ;
END;

IF (@v >= 13
   AND @QueryFilter = 'all'
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0 
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0) 
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant', 'unused grant'))
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
       SET @sql += N' AND COALESCE(LOWER(DB_NAME(database_id)), LOWER(CAST(pa.value AS sysname)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'', ''dbmaintenance'', ''dbadmin'', ''dbatools'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;

	SET @sql += @sort_filter + @nl;

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
   AND (@SortOrder NOT IN ('memory grant', 'avg memory grant', 'unused grant'))
BEGIN
   RAISERROR (N'Adding SQL to collect trigger stats.',0,1) WITH NOWAIT;

   /* Trigger level information from the plan cache */
   SET @sql += @insert_list ;

   SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Trigger') ;

   SET @sql += REPLACE(@body, '#view#', 'dm_exec_trigger_stats') ;

   SET @sql += @body_where ;

   IF @IgnoreSystemDBs = 1
      SET @sql += N' AND COALESCE(LOWER(DB_NAME(database_id)), LOWER(CAST(pa.value AS sysname)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'', ''dbmaintenance'', ''dbadmin'', ''dbatools'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' + @nl ;

   SET @sql += @sort_filter + @nl;   

   SET @sql += @body_order + @nl + @nl + @nl ;
END;



SELECT @sort = CASE @SortOrder  WHEN N'cpu' THEN N'total_worker_time'
                                WHEN N'reads' THEN N'total_logical_reads'
                                WHEN N'writes' THEN N'total_logical_writes'
                                WHEN N'duration' THEN N'total_elapsed_time'
                                WHEN N'executions' THEN N'execution_count'
                                WHEN N'compiles' THEN N'cached_time'
								WHEN N'memory grant' THEN N'max_grant_kb'
								WHEN N'unused grant' THEN N'max_grant_kb - max_used_grant_kb'
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
         FROM    ##BlitzCacheProcs
		 WHERE SPID = @@SPID) AS x
WHERE x.rn = 1
OPTION (RECOMPILE);

/* 
    This block was used to delete duplicate queries, but has been removed.
    For more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2026
WITH d AS (
SELECT  SPID,
        ROW_NUMBER() OVER (PARTITION BY SqlHandle, QueryHash ORDER BY #sortable# DESC) AS rn
FROM    ##BlitzCacheProcs
WHERE SPID = @@SPID
)
DELETE d
WHERE d.rn > 1
AND SPID = @@SPID
OPTION (RECOMPILE); 
*/
';

SELECT @sort = CASE @SortOrder  WHEN N'cpu' THEN N'TotalCPU'
                                WHEN N'reads' THEN N'TotalReads'
                                WHEN N'writes' THEN N'TotalWrites'
                                WHEN N'duration' THEN N'TotalDuration'
                                WHEN N'executions' THEN N'ExecutionCount'
                                WHEN N'compiles' THEN N'PlanCreationTime'
								WHEN N'memory grant' THEN N'MaxGrantKB'
								WHEN N'unused grant' THEN N'MaxGrantKB - MaxUsedGrantKB'
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


/* Update ##BlitzCacheProcs to get Stored Proc info 
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
    FROM ##BlitzCacheProcs b
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
FROM ##BlitzCacheProcs b
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
DECLARE @total_duration BIGINT,
        @total_cpu BIGINT,
        @total_reads BIGINT,
        @total_writes BIGINT,
        @total_execution_count BIGINT;

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
UPDATE ##BlitzCacheProcs
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
        FROM    ##BlitzCacheProcs
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
WHERE ##BlitzCacheProcs.PlanHandle = y.PlanHandle
      AND ##BlitzCacheProcs.PlanHandle IS NOT NULL
	  AND ##BlitzCacheProcs.SPID = @@SPID
OPTION (RECOMPILE) ;


RAISERROR(N'Gather percentage information from grouped results', 0, 1) WITH NOWAIT;
UPDATE ##BlitzCacheProcs
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
        FROM    ##BlitzCacheProcs
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
WHERE   ##BlitzCacheProcs.SqlHandle = y.SqlHandle
        AND ##BlitzCacheProcs.QueryHash = y.QueryHash
        AND ##BlitzCacheProcs.DatabaseName = y.DatabaseName
        AND ##BlitzCacheProcs.PlanHandle IS NULL
OPTION (RECOMPILE) ;



/* Testing using XML nodes to speed up processing */
RAISERROR(N'Begin XML nodes processing', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
		PlanHandle,
        q.n.query('.') AS statement,
        0 AS is_cursor
INTO    #statements
FROM    ##BlitzCacheProcs p
        CROSS APPLY p.QueryPlan.nodes('//p:StmtSimple') AS q(n) 
WHERE p.SPID = @@SPID
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements
SELECT  QueryHash ,
        SqlHandle ,
		PlanHandle,
        q.n.query('.') AS statement,
        1 AS is_cursor
FROM    ##BlitzCacheProcs p
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
UPDATE  ##BlitzCacheProcs
SET     NumberOfDistinctPlans = distinct_plan_count,
        NumberOfPlans = number_of_plans ,
        plan_multiple_plans = CASE WHEN distinct_plan_count < number_of_plans THEN number_of_plans END
FROM
    (
    SELECT    
        DatabaseName = 
            DB_NAME(CONVERT(int, pa.value)),
        QueryHash = 
            qs.query_hash,
        number_of_plans =
           COUNT_BIG(qs.query_plan_hash),
        distinct_plan_count = 
            COUNT_BIG(DISTINCT qs.query_plan_hash)
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
    WHERE pa.attribute = 'dbid'
    GROUP BY 
        DB_NAME(CONVERT(int, pa.value)), 
        qs.query_hash
) AS x
WHERE ##BlitzCacheProcs.QueryHash = x.QueryHash
AND   ##BlitzCacheProcs.DatabaseName = x.DatabaseName
OPTION (RECOMPILE) ;

-- query level checks
RAISERROR(N'Performing query level checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##BlitzCacheProcs
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
WHERE   qp.QueryHash = ##BlitzCacheProcs.QueryHash
AND     qp.SqlHandle = ##BlitzCacheProcs.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

-- statement level checks
RAISERROR(N'Performing compile timeout checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_timeout = 1 
FROM    #statements s
JOIN ##BlitzCacheProcs b
ON  s.QueryHash = b.QueryHash
AND SPID = @@SPID
WHERE statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
OPTION (RECOMPILE);

RAISERROR(N'Performing compile memory limit exceeded checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_memory_limit_exceeded = 1 
FROM    #statements s
JOIN ##BlitzCacheProcs b
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
FROM ##BlitzCacheProcs b
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
	FROM ##BlitzCacheProcs AS b
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
	FROM ##BlitzCacheProcs AS b
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
	FROM ##BlitzCacheProcs AS b
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
FROM ##BlitzCacheProcs AS b
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
		JOIN ##BlitzCacheProcs b
		ON b.SqlHandle = pc.SqlHandle
		AND b.QueryHash = pc.QueryHash
		WHERE b.QueryType NOT LIKE '%Procedure%'
	OPTION (RECOMPILE);

IF EXISTS (
SELECT 1
FROM ##BlitzCacheProcs AS b
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
FROM ##BlitzCacheProcs AS b
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
FROM ##BlitzCacheProcs b
WHERE b.QueryPlanCost IS NULL
AND b.SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Checking for plan warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##BlitzCacheProcs
SET plan_warnings = 1
FROM    #query_plan qp
WHERE   qp.SqlHandle = ##BlitzCacheProcs.SqlHandle
AND SPID = @@SPID
AND query_plan.exist('/p:QueryPlan/p:Warnings') = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking for implicit conversion', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##BlitzCacheProcs
SET implicit_conversions = 1
FROM    #query_plan qp
WHERE   qp.SqlHandle = ##BlitzCacheProcs.SqlHandle
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
FROM   ##BlitzCacheProcs p
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
FROM   ##BlitzCacheProcs p
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
FROM ##BlitzCacheProcs AS p
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
FROM ##BlitzCacheProcs AS p
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
FROM ##BlitzCacheProcs AS p
JOIN x ON x.SqlHandle = p.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Checking for expensive key lookups', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##BlitzCacheProcs
SET key_lookup_cost = x.key_lookup_cost
FROM (
SELECT 
       qs.SqlHandle,
	   MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS key_lookup_cost
FROM   #relop qs
WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
GROUP BY qs.SqlHandle
) AS x
WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);


RAISERROR(N'Checking for expensive remote queries', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##BlitzCacheProcs
SET remote_query_cost = x.remote_query_cost
FROM (
SELECT 
       qs.SqlHandle,
	   MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS remote_query_cost
FROM   #relop qs
WHERE [relop].exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
GROUP BY qs.SqlHandle
) AS x
WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Checking for expensive sorts', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##BlitzCacheProcs
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
WHERE ##BlitzCacheProcs.SqlHandle = y.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);

IF NOT EXISTS(SELECT 1/0 FROM #statements AS s WHERE s.is_cursor = 1)
BEGIN

RAISERROR(N'No cursor plans found, skipping', 0, 1) WITH NOWAIT;

END

IF EXISTS(SELECT 1/0 FROM #statements AS s WHERE s.is_cursor = 1)
BEGIN

RAISERROR(N'Cursor plans found, investigating', 0, 1) WITH NOWAIT;

RAISERROR(N'Checking for Optimistic cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_optimistic_cursor =  1
FROM ##BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1
AND qs.is_cursor = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking if cursor is Forward Only', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_forward_only_cursor = 1
FROM ##BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1
AND qs.is_cursor = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking if cursor is Fast Forward', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_fast_forward_cursor = 1
FROM ##BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorActualType[.="FastForward"]') = 1
AND qs.is_cursor = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking for Dynamic cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_cursor_dynamic =  1
FROM ##BlitzCacheProcs b
JOIN #statements AS qs
ON b.SqlHandle = qs.SqlHandle
CROSS APPLY qs.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE SPID = @@SPID
AND n1.fn.exist('//p:CursorPlan/@CursorActualType[.="Dynamic"]') = 1
AND qs.is_cursor = 1
OPTION (RECOMPILE);

END

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
FROM ##BlitzCacheProcs b
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
UPDATE ##BlitzCacheProcs
SET is_computed_scalar = x.computed_column_function
FROM (
SELECT qs.SqlHandle,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS computed_column_function
FROM   #relop qs
CROSS APPLY relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
WHERE n.fn.exist('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ColumnReference[(@ComputedColumn[.="1"])]') = 1
) AS x
WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
AND SPID = @@SPID
OPTION (RECOMPILE);
END; 


RAISERROR(N'Checking for filters that reference scalar UDFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##BlitzCacheProcs
SET is_computed_filter = x.filter_function
FROM (
SELECT 
r.SqlHandle, 
c.n.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS filter_function
FROM #relop AS r
CROSS APPLY r.relop.nodes('/p:RelOp/p:Filter/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator/p:UserDefinedFunction') c(n) 
) x
WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
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
FROM ##BlitzCacheProcs AS b
JOIN iops ON  iops.QueryHash = b.QueryHash
WHERE SPID = @@SPID
OPTION (RECOMPILE);
END; 


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for Spatial index use', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##BlitzCacheProcs
SET is_spatial = x.is_spatial
FROM (
SELECT qs.SqlHandle,
	   1 AS is_spatial
FROM   #relop qs
CROSS APPLY relop.nodes('/p:RelOp//p:Object') n(fn)
WHERE n.fn.exist('(@IndexKind[.="Spatial"])') = 1
) AS x
WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
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
FROM ##BlitzCacheProcs b
JOIN spools sp
ON sp.QueryHash = b.QueryHash
OPTION (RECOMPILE);

RAISERROR('Checking for wonky Table Spools', 0, 1) WITH NOWAIT;
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
WHERE  r.relop.exist('/p:RelOp[@PhysicalOp="Table Spool" and @LogicalOp="Lazy Spool"]') = 1
)
UPDATE b
		SET b.table_spool_rows = (sp.estimated_rows * sp.estimated_rebinds),
			b.table_spool_cost = ((sp.estimated_io * sp.estimated_cpu * sp.estimated_rows) * CASE WHEN sp.estimated_rebinds < 1 THEN 1 ELSE sp.estimated_rebinds END)
FROM ##BlitzCacheProcs b
JOIN spools sp
ON sp.QueryHash = b.QueryHash
OPTION (RECOMPILE);


RAISERROR('Checking for selects that cause non-spill and index spool writes', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES (
    'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
, selects
AS ( SELECT CONVERT(BINARY(8), 
                RIGHT('0000000000000000' 
				    + SUBSTRING(s.statement.value('(/p:StmtSimple/@QueryHash)[1]', 'VARCHAR(18)'), 
					    3, 18), 16), 2) AS QueryHash
     FROM   #statements AS s
	 JOIN ##BlitzCacheProcs b
	 ON s.QueryHash = b.QueryHash
	 WHERE b.index_spool_rows IS NULL
	 AND   b.index_spool_cost IS NULL
	 AND   b.table_spool_cost IS NULL
	 AND   b.table_spool_rows IS NULL
	 AND   b.is_big_spills IS NULL
	 AND   b.AverageWrites > 1024.
     AND  s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 
)
UPDATE b
   SET b.select_with_writes = 1
FROM ##BlitzCacheProcs b
JOIN selects AS s
ON s.QueryHash = b.QueryHash
AND b.AverageWrites > 1024.;

/* 2012+ only */
IF @v >= 11
BEGIN

	RAISERROR(N'Checking for forced serialization', 0, 1) WITH NOWAIT;
	WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
	UPDATE  ##BlitzCacheProcs
	SET is_forced_serial = 1
	FROM    #query_plan qp
	WHERE   qp.SqlHandle = ##BlitzCacheProcs.SqlHandle
	AND SPID = @@SPID
	AND query_plan.exist('/p:QueryPlan/@NonParallelPlanReason') = 1
	AND (##BlitzCacheProcs.is_parallel = 0 OR ##BlitzCacheProcs.is_parallel IS NULL)
	OPTION (RECOMPILE);
	
	IF @ExpertMode > 0
	BEGIN
	RAISERROR(N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) WITH NOWAIT;
	WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
	UPDATE ##BlitzCacheProcs
	SET columnstore_row_mode = x.is_row_mode
	FROM (
	SELECT 
	       qs.SqlHandle,
		   relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') AS is_row_mode
	FROM   #relop qs
	WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
	) AS x
	WHERE ##BlitzCacheProcs.SqlHandle = x.SqlHandle
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
    FROM    ##BlitzCacheProcs p
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
    FROM    ##BlitzCacheProcs p
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
	   x.c.value('@ModificationCount', 'BIGINT') AS ModificationCount,
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
FROM ##BlitzCacheProcs b
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
FROM ##BlitzCacheProcs b
JOIN aj
ON b.SqlHandle = aj.SqlHandle
AND b.SPID = @@SPID
OPTION (RECOMPILE);
END; 

IF ((@v >= 14 
       OR (@v = 13 AND @build >= 5026) 
       OR (@v = 12 AND @build >= 6024))
   AND @ExpertMode > 0)

BEGIN;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
row_goals AS(
SELECT qs.QueryHash
FROM   #relop qs
WHERE relop.value('sum(/p:RelOp/@EstimateRowsWithoutRowGoal)', 'float') > 0
)
UPDATE b
SET b.is_row_goal = 1
FROM ##BlitzCacheProcs b
JOIN row_goals
ON b.QueryHash = row_goals.QueryHash
AND b.SPID = @@SPID
OPTION (RECOMPILE);
END ;

END;


/* END Testing using XML nodes to speed up processing */


/* Update to grab stored procedure name for individual statements */
RAISERROR(N'Attempting to get stored procedure name for individual statements', 0, 1) WITH NOWAIT;
UPDATE  p
SET     QueryType = QueryType + ' (parent ' +
                    + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
                    + '.'
                    + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + ')'
FROM    ##BlitzCacheProcs p
        JOIN sys.dm_exec_procedure_stats s ON p.SqlHandle = s.sql_handle
WHERE   QueryType = 'Statement'
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR(N'Attempting to get function name for individual statements', 0, 1) WITH NOWAIT;
DECLARE @function_update_sql NVARCHAR(MAX) = N''
IF EXISTS (SELECT 1/0 FROM sys.all_objects AS o WHERE o.name = 'dm_exec_function_stats')
    BEGIN
     SET @function_update_sql = @function_update_sql + N'
     UPDATE  p
     SET     QueryType = QueryType + '' (parent '' +
                         + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
                         + ''.''
                         + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + '')''
     FROM    ##BlitzCacheProcs p
             JOIN sys.dm_exec_function_stats s ON p.SqlHandle = s.sql_handle
     WHERE   QueryType = ''Statement''
     AND SPID = @@SPID
     OPTION (RECOMPILE);
     '
    EXEC sys.sp_executesql @function_update_sql
   END


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
FROM   ##BlitzCacheProcs p
JOIN #trace_flags tf ON tf.QueryHash = p.QueryHash 
WHERE SPID = @@SPID
OPTION (RECOMPILE);

END;


RAISERROR(N'Checking for MSTVFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_mstvf = 1
FROM #relop AS r
JOIN ##BlitzCacheProcs AS b
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
JOIN ##BlitzCacheProcs AS b
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
FROM   ##BlitzCacheProcs AS b
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
JOIN    ##BlitzCacheProcs AS b
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
JOIN        ##BlitzCacheProcs AS b
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
JOIN        ##BlitzCacheProcs AS b
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
                 AND (ci.equal_charindex -1) > 0 
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
SET    s.variable_datatype = CASE WHEN s.variable_datatype LIKE '%(%)%' 
                                  THEN LEFT(s.variable_datatype, CHARINDEX('(', s.variable_datatype) - 1)
								  ELSE s.variable_datatype
                             END,
       s.converted_to = CASE WHEN s.converted_to LIKE '%(%)%' 
                             THEN LEFT(s.converted_to, CHARINDEX('(', s.converted_to) - 1)
                             ELSE s.converted_to
                        END,
	   s.compile_time_value = CASE WHEN s.compile_time_value LIKE '%(%)%' 
                                   THEN SUBSTRING(s.compile_time_value, 
												  CHARINDEX('(', s.compile_time_value) + 1,
												  CHARINDEX(')', s.compile_time_value, CHARINDEX('(', s.compile_time_value) + 1) - 1 - CHARINDEX('(', s.compile_time_value)
												  )
									WHEN variable_datatype NOT IN ('bit', 'tinyint', 'smallint', 'int', 'bigint') 
									AND s.variable_datatype NOT LIKE '%binary%' 
									AND s.compile_time_value NOT LIKE 'N''%'''
									AND s.compile_time_value NOT LIKE '''%''' 
									AND s.compile_time_value <> s.column_name
                                    AND s.compile_time_value <> '**idk_man**'
                                    THEN QUOTENAME(compile_time_value, '''')
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
FROM ##BlitzCacheProcs AS b
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
							   THEN @nl + N'More info on possible reasons: https://www.brentozar.com/go/noplans '
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
FROM ##BlitzCacheProcs AS b
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
							   THEN @nl + N' More info on possible reasons: https://www.brentozar.com/go/noplans '
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
FROM ##BlitzCacheProcs AS b
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
FROM ##BlitzCacheProcs AS b
WHERE b.SPID = @@SPID
OPTION (RECOMPILE);

/*End implicit conversion and parameter info*/

/*Begin Missing Index*/
IF EXISTS ( SELECT 1/0 
            FROM ##BlitzCacheProcs AS bbcp 
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
		JOIN ##BlitzCacheProcs AS bbcp
		ON m.SqlHandle = bbcp.SqlHandle
		AND m.QueryHash = bbcp.QueryHash
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
		JOIN ##BlitzCacheProcs p
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
		FROM ##BlitzCacheProcs AS bbcp
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
	FROM ##BlitzCacheProcs AS b
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
UPDATE ##BlitzCacheProcs
SET    frequent_execution = CASE WHEN ExecutionsPerMinute > @execution_threshold THEN 1 END ,
       parameter_sniffing = CASE WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND min_worker_time < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND max_worker_time > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND MinReturnedRows < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1
                                 WHEN ExecutionCount > 3 AND AverageReads > @parameter_sniffing_io_threshold
                                      AND MaxReturnedRows > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1 END ,
       near_parallel = CASE WHEN is_parallel <> 1 AND QueryPlanCost BETWEEN @ctp * (1 - (@ctp_threshold_pct / 100.0)) AND @ctp THEN 1 END,
       long_running = CASE WHEN AverageDuration > @long_running_query_warning_seconds THEN 1
                           WHEN max_worker_time > @long_running_query_warning_seconds THEN 1
                           WHEN max_elapsed_time > @long_running_query_warning_seconds THEN 1 END,
	   is_key_lookup_expensive = CASE WHEN QueryPlanCost >= (@ctp / 2) AND key_lookup_cost >= QueryPlanCost * .5 THEN 1 END,
	   is_sort_expensive = CASE WHEN QueryPlanCost >= (@ctp / 2) AND sort_cost >= QueryPlanCost * .5 THEN 1 END,
	   is_remote_query_expensive = CASE WHEN remote_query_cost >= QueryPlanCost * .05 THEN 1 END,
	   is_unused_grant = CASE WHEN PercentMemoryGrantUsed <= @memory_grant_warning_percent AND MinGrantKB > @MinMemoryPerQuery THEN 1 END,
	   long_running_low_cpu = CASE WHEN AverageDuration > AverageCPU * 4 AND AverageCPU < 500. THEN 1 END,
	   low_cost_high_cpu = CASE WHEN QueryPlanCost <= 10 AND AverageCPU > 5000. THEN 1 END,
	   is_spool_expensive = CASE WHEN QueryPlanCost > (@ctp / 5) AND index_spool_cost >= QueryPlanCost * .1 THEN 1 END,
	   is_spool_more_rows = CASE WHEN index_spool_rows >= (AverageReturnedRows / ISNULL(NULLIF(ExecutionCount, 0), 1)) THEN 1 END,
	   is_table_spool_expensive = CASE WHEN QueryPlanCost > (@ctp / 5) AND table_spool_cost >= QueryPlanCost / 4 THEN 1 END,
	   is_table_spool_more_rows = CASE WHEN table_spool_rows >= (AverageReturnedRows / ISNULL(NULLIF(ExecutionCount, 0), 1)) THEN 1 END,
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
FROM   ##BlitzCacheProcs p
       CROSS APPLY sys.dm_exec_plan_attributes(p.PlanHandle) pa
WHERE  pa.attribute = 'set_options' 
AND SPID = @@SPID
OPTION (RECOMPILE);


/* Cursor checks */
UPDATE p
SET    is_cursor = CASE WHEN CAST(pa.value AS INT) <> 0 THEN 1 END
FROM   ##BlitzCacheProcs p
       CROSS APPLY sys.dm_exec_plan_attributes(p.PlanHandle) pa
WHERE  pa.attribute LIKE '%cursor%' 
AND SPID = @@SPID
OPTION (RECOMPILE);

UPDATE p
SET    is_cursor = 1
FROM   ##BlitzCacheProcs p
WHERE QueryHash = 0x0000000000000000
OR QueryPlanHash = 0x0000000000000000
AND SPID = @@SPID
OPTION (RECOMPILE);



RAISERROR('Populating Warnings column', 0, 1) WITH NOWAIT;
/* Populate warnings */
UPDATE ##BlitzCacheProcs
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
                  CASE WHEN plan_multiple_plans > 0 THEN ', Multiple Plans' + COALESCE(' (' + CAST(plan_multiple_plans AS VARCHAR(10)) + ')', '') ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session ELSE '' END +
				  CASE WHEN is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), function_count) + ' Function(s)' ELSE '' END + 
				  CASE WHEN clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), clr_function_count) + ' CLR Function(s)' ELSE '' END + 
				  CASE WHEN PlanCreationTimeHours <= 4 THEN ', Plan created last 4hrs' ELSE '' END +
				  CASE WHEN is_table_variable = 1 THEN ', Table Variables' ELSE '' END +
				  CASE WHEN no_stats_warning = 1 THEN ', Columns With No Statistics' ELSE '' END +
				  CASE WHEN relop_warnings = 1 THEN ', Operator Warnings' ELSE '' END  + 
				  CASE WHEN is_table_scan = 1 THEN ', Table Scans (Heaps)' ELSE '' END  + 
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
				  CASE WHEN is_table_spool_expensive = 1 THEN + ', Expensive Table Spool' ELSE '' END +
				  CASE WHEN is_table_spool_more_rows = 1 THEN + ', Many Rows Table Spool' ELSE '' END +
				  CASE WHEN is_bad_estimate = 1 THEN + ', Row Estimate Mismatch' ELSE '' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN ', SWITCH!' ELSE '' END + 
				  CASE WHEN is_row_goal = 1 THEN ', Row Goals' ELSE '' END + 
                  CASE WHEN is_big_spills = 1 THEN ', >500mb Spills' ELSE '' END + 
				  CASE WHEN is_mstvf = 1 THEN ', MSTVFs' ELSE '' END + 
				  CASE WHEN is_mm_join = 1 THEN ', Many to Many Merge' ELSE '' END + 
                  CASE WHEN is_nonsargable = 1 THEN ', non-SARGables' ELSE '' END + 
				  CASE WHEN CompileTime > 5000 THEN ', Long Compile Time' ELSE '' END +
				  CASE WHEN CompileCPU > 5000 THEN ', High Compile CPU' ELSE '' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN ', High Compile Memory' ELSE '' END +
				  CASE WHEN select_with_writes > 0 THEN ', Select w/ Writes' ELSE '' END
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
                  CASE WHEN missing_index_count > 0 THEN ', Missing Indexes (' + CONVERT(VARCHAR(10), (SELECT SUM(b2.missing_index_count) FROM ##BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL AND SPID = @@SPID) ) + ')' ELSE '' END +
                  CASE WHEN unmatched_index_count > 0 THEN ', Unmatched Indexes (' + CONVERT(VARCHAR(10), (SELECT SUM(b2.unmatched_index_count) FROM ##BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL AND SPID = @@SPID) ) + ')' ELSE '' END +                  
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
                  CASE WHEN plan_multiple_plans > 0 THEN ', Multiple Plans' + COALESCE(' (' + CAST(plan_multiple_plans AS VARCHAR(10)) + ')', '') ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session ELSE '' END +
				  CASE WHEN is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), (SELECT SUM(b2.function_count) FROM ##BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL AND SPID = @@SPID) ) + ' Function(s)' ELSE '' END + 
				  CASE WHEN clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), (SELECT SUM(b2.clr_function_count) FROM ##BlitzCacheProcs AS b2 WHERE b2.SqlHandle = b.SqlHandle AND b2.QueryHash IS NOT NULL AND SPID = @@SPID) ) + ' CLR Function(s)' ELSE '' END + 
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
				  CASE WHEN is_table_spool_expensive = 1 THEN + ', Expensive Table Spool' ELSE '' END +
				  CASE WHEN is_table_spool_more_rows = 1 THEN + ', Many Rows Table Spool' ELSE '' END +
				  CASE WHEN is_bad_estimate = 1 THEN + ', Row Estimate Mismatch' ELSE '' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN ', SWITCH!' ELSE '' END + 
				  CASE WHEN is_row_goal = 1 THEN ', Row Goals' ELSE '' END + 
                  CASE WHEN is_big_spills = 1 THEN ', >500mb spills' ELSE '' END + 
				  CASE WHEN is_mstvf = 1 THEN ', MSTVFs' ELSE '' END + 
				  CASE WHEN is_mm_join = 1 THEN ', Many to Many Merge' ELSE '' END + 
                  CASE WHEN is_nonsargable = 1 THEN ', non-SARGables' ELSE '' END +
				  CASE WHEN CompileTime > 5000 THEN ', Long Compile Time' ELSE '' END +
				  CASE WHEN CompileCPU > 5000 THEN ', High Compile CPU' ELSE '' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN ', High Compile Memory' ELSE '' END +
				  CASE WHEN select_with_writes > 0 THEN ', Select w/ Writes' ELSE '' END
                  , 3, 200000) 
FROM ##BlitzCacheProcs b
WHERE SPID = @@SPID
AND QueryType LIKE 'Statement (parent%'
	)
UPDATE b
SET b.Warnings = s.Warnings
FROM ##BlitzCacheProcs AS b
JOIN statement_warnings s
ON b.SqlHandle = s.SqlHandle
WHERE QueryType LIKE 'Procedure or Function%'
AND SPID = @@SPID
OPTION (RECOMPILE);

RAISERROR('Checking for plans with >128 levels of nesting', 0, 1) WITH NOWAIT;	
WITH plan_handle AS (
SELECT b.PlanHandle
FROM ##BlitzCacheProcs b
   CROSS APPLY sys.dm_exec_text_query_plan(b.PlanHandle, 0, -1) tqp
   CROSS APPLY sys.dm_exec_query_plan(b.PlanHandle) qp
   WHERE tqp.encrypted = 0
   AND b.SPID = @@SPID
   AND (qp.query_plan IS NULL
			AND tqp.query_plan IS NOT NULL)
)
UPDATE b
SET Warnings = ISNULL('Your query plan is >128 levels of nested nodes, and can''t be converted to XML. Use SELECT * FROM sys.dm_exec_text_query_plan('+ CONVERT(VARCHAR(128), ph.PlanHandle, 1) + ', 0, -1) to get more information' 
                        , 'We couldn''t find a plan for this query. More info on possible reasons: https://www.brentozar.com/go/noplans')
FROM ##BlitzCacheProcs b
LEFT JOIN plan_handle ph ON
b.PlanHandle = ph.PlanHandle
WHERE b.QueryPlan IS NULL
AND b.SPID = @@SPID
OPTION (RECOMPILE);			  

RAISERROR('Checking for plans with no warnings', 0, 1) WITH NOWAIT;	
UPDATE ##BlitzCacheProcs
SET Warnings = 'No warnings detected. ' + CASE @ExpertMode 
											WHEN 0 
											THEN ' Try running sp_BlitzCache with @ExpertMode = 1 to find more advanced problems.' 
											ELSE '' 
										  END
WHERE Warnings = '' OR	Warnings IS NULL
AND SPID = @@SPID
OPTION (RECOMPILE);


Results:
IF @ExportToExcel = 1
BEGIN
    RAISERROR('Displaying results with Excel formatting (no plans).', 0, 1) WITH NOWAIT;

    /* excel output */
    UPDATE ##BlitzCacheProcs
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
			PlanGenerationNum,
			PlanHandle AS [Plan Handle],  
			SqlHandle AS [SQL Handle],  
            QueryHash,
            QueryPlanHash,
            COALESCE(SetOptions, '''') AS [SET Options]
    FROM    ##BlitzCacheProcs
    WHERE   1 = 1 
	AND SPID = @@SPID ' + @nl;

    IF @MinimumExecutionCount IS NOT NULL
      BEGIN
		SET @sql += N' AND ExecutionCount >= @minimumExecutionCount ';
	  END;
	
   IF @MinutesBack IS NOT NULL
      BEGIN
		SET @sql += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
	  END;

	SELECT @sql += N' ORDER BY ' + CASE @SortOrder WHEN N'cpu' THEN N' TotalCPU '
                              WHEN N'reads' THEN N' TotalReads '
                              WHEN N'writes' THEN N' TotalWrites '
                              WHEN N'duration' THEN N' TotalDuration '
                              WHEN N'executions' THEN N' ExecutionCount '
                              WHEN N'compiles' THEN N' PlanCreationTime '
							  WHEN N'memory grant' THEN N' MaxGrantKB'
							  WHEN N'unused grant' THEN N' MaxGrantKB - MaxUsedGrantKB'
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
    CONVERT(NVARCHAR(30), CAST((ExecutionCount) AS BIGINT), 1) AS [# Executions],
    CONVERT(NVARCHAR(30), CAST((ExecutionsPerMinute) AS BIGINT), 1) AS [Executions / Minute],
    CONVERT(NVARCHAR(30), CAST((PercentExecutions) AS BIGINT), 1) AS [Execution Weight],
    CONVERT(NVARCHAR(30), CAST((TotalCPU) AS BIGINT), 1) AS [Total CPU (ms)],
    CONVERT(NVARCHAR(30), CAST((AverageCPU) AS BIGINT), 1) AS [Avg CPU (ms)],
    CONVERT(NVARCHAR(30), CAST((PercentCPU) AS BIGINT), 1) AS [CPU Weight],
    CONVERT(NVARCHAR(30), CAST((TotalDuration) AS BIGINT), 1) AS [Total Duration (ms)],
    CONVERT(NVARCHAR(30), CAST((AverageDuration) AS BIGINT), 1) AS [Avg Duration (ms)],
    CONVERT(NVARCHAR(30), CAST((PercentDuration) AS BIGINT), 1) AS [Duration Weight],
    CONVERT(NVARCHAR(30), CAST((TotalReads) AS BIGINT), 1) AS [Total Reads],
    CONVERT(NVARCHAR(30), CAST((AverageReads) AS BIGINT), 1) AS [Avg Reads],
    CONVERT(NVARCHAR(30), CAST((PercentReads) AS BIGINT), 1) AS [Read Weight],
    CONVERT(NVARCHAR(30), CAST((TotalWrites) AS BIGINT), 1) AS [Total Writes],
    CONVERT(NVARCHAR(30), CAST((AverageWrites) AS BIGINT), 1) AS [Avg Writes],
    CONVERT(NVARCHAR(30), CAST((PercentWrites) AS BIGINT), 1) AS [Write Weight],
    CONVERT(NVARCHAR(30), CAST((AverageReturnedRows) AS BIGINT), 1) AS [Average Rows],
	CONVERT(NVARCHAR(30), CAST((MinGrantKB) AS BIGINT), 1) AS [Minimum Memory Grant KB],
	CONVERT(NVARCHAR(30), CAST((MaxGrantKB) AS BIGINT), 1) AS [Maximum Memory Grant KB],
	CONVERT(NVARCHAR(30), CAST((MinUsedGrantKB) AS BIGINT), 1) AS [Minimum Used Grant KB], 
	CONVERT(NVARCHAR(30), CAST((MaxUsedGrantKB) AS BIGINT), 1) AS [Maximum Used Grant KB],
	CONVERT(NVARCHAR(30), CAST((AvgMaxMemoryGrant) AS BIGINT), 1) AS [Average Max Memory Grant],
	CONVERT(NVARCHAR(30), CAST((MinSpills) AS BIGINT), 1) AS [Min Spills],
	CONVERT(NVARCHAR(30), CAST((MaxSpills) AS BIGINT), 1) AS [Max Spills],
	CONVERT(NVARCHAR(30), CAST((TotalSpills) AS BIGINT), 1) AS [Total Spills],
	CONVERT(NVARCHAR(30), CAST((AvgSpills) AS MONEY), 1) AS [Avg Spills],
    PlanCreationTime AS [Created At],
    LastExecutionTime AS [Last Execution],
	LastCompletionTime AS [Last Completion],
	PlanHandle AS [Plan Handle], 
	SqlHandle AS [SQL Handle], 
    COALESCE(SetOptions, '''') AS [SET Options],
	QueryHash AS [Query Hash],
	PlanGenerationNum,
	[Remove Plan Handle From Cache]';
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
                  CASE WHEN plan_multiple_plans > 0 THEN '', 21'' ELSE '''' END +
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
				  CASE WHEN is_table_spool_expensive = 1 THEN + '', 67'' ELSE '''' END +
				  CASE WHEN is_table_spool_more_rows = 1 THEN + '', 68'' ELSE '''' END  +
				  CASE WHEN is_bad_estimate = 1 THEN + '', 56'' ELSE '''' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN '', 57'' ELSE '''' END + 
				  CASE WHEN is_row_goal = 1 THEN '', 58'' ELSE '''' END + 
                  CASE WHEN is_big_spills = 1 THEN '', 59'' ELSE '''' END +
				  CASE WHEN is_mstvf = 1 THEN '', 60'' ELSE '''' END + 
				  CASE WHEN is_mm_join = 1 THEN '', 61'' ELSE '''' END  + 
                  CASE WHEN is_nonsargable = 1 THEN '', 62'' ELSE '''' END + 
				  CASE WHEN CompileTime > 5000 THEN '', 63 '' ELSE '''' END +
				  CASE WHEN CompileCPU > 5000 THEN '', 64 '' ELSE '''' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN '', 65 '' ELSE '''' END +
				  CASE WHEN select_with_writes > 0 THEN '', 66'' ELSE '''' END
				  , 3, 200000) AS opserver_warning , ' + @nl ;
    END;
    
    SET @columns += N'        
        CONVERT(NVARCHAR(30), CAST((ExecutionCount) AS BIGINT), 1) AS [# Executions],
        CONVERT(NVARCHAR(30), CAST((ExecutionsPerMinute) AS BIGINT), 1) AS [Executions / Minute],
        CONVERT(NVARCHAR(30), CAST((PercentExecutions) AS BIGINT), 1) AS [Execution Weight],
        CONVERT(NVARCHAR(30), CAST((SerialDesiredMemory) AS BIGINT), 1) AS [Serial Desired Memory],
        CONVERT(NVARCHAR(30), CAST((SerialRequiredMemory) AS BIGINT), 1) AS [Serial Required Memory],
        CONVERT(NVARCHAR(30), CAST((TotalCPU) AS BIGINT), 1) AS [Total CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((AverageCPU) AS BIGINT), 1) AS [Avg CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((PercentCPU) AS BIGINT), 1) AS [CPU Weight],
        CONVERT(NVARCHAR(30), CAST((TotalDuration) AS BIGINT), 1) AS [Total Duration (ms)],
        CONVERT(NVARCHAR(30), CAST((AverageDuration) AS BIGINT), 1) AS [Avg Duration (ms)],
        CONVERT(NVARCHAR(30), CAST((PercentDuration) AS BIGINT), 1) AS [Duration Weight],
        CONVERT(NVARCHAR(30), CAST((TotalReads) AS BIGINT), 1) AS [Total Reads],
        CONVERT(NVARCHAR(30), CAST((AverageReads) AS BIGINT), 1) AS [Average Reads],
        CONVERT(NVARCHAR(30), CAST((PercentReads) AS BIGINT), 1) AS [Read Weight],
        CONVERT(NVARCHAR(30), CAST((TotalWrites) AS BIGINT), 1) AS [Total Writes],
        CONVERT(NVARCHAR(30), CAST((AverageWrites) AS BIGINT), 1) AS [Average Writes],
        CONVERT(NVARCHAR(30), CAST((PercentWrites) AS BIGINT), 1) AS [Write Weight],
        CONVERT(NVARCHAR(30), CAST((PercentExecutionsByType) AS BIGINT), 1) AS [% Executions (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentCPUByType) AS BIGINT), 1) AS [% CPU (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentDurationByType) AS BIGINT), 1) AS [% Duration (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentReadsByType) AS BIGINT), 1) AS [% Reads (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentWritesByType) AS BIGINT), 1) AS [% Writes (Type)],
        CONVERT(NVARCHAR(30), CAST((TotalReturnedRows) AS BIGINT), 1) AS [Total Rows],
        CONVERT(NVARCHAR(30), CAST((AverageReturnedRows) AS BIGINT), 1) AS [Avg Rows],
        CONVERT(NVARCHAR(30), CAST((MinReturnedRows) AS BIGINT), 1) AS [Min Rows],
        CONVERT(NVARCHAR(30), CAST((MaxReturnedRows) AS BIGINT), 1) AS [Max Rows],
		CONVERT(NVARCHAR(30), CAST((MinGrantKB) AS BIGINT), 1) AS [Minimum Memory Grant KB],
		CONVERT(NVARCHAR(30), CAST((MaxGrantKB) AS BIGINT), 1) AS [Maximum Memory Grant KB],
		CONVERT(NVARCHAR(30), CAST((MinUsedGrantKB) AS BIGINT), 1) AS [Minimum Used Grant KB], 
		CONVERT(NVARCHAR(30), CAST((MaxUsedGrantKB) AS BIGINT), 1) AS [Maximum Used Grant KB],
		CONVERT(NVARCHAR(30), CAST((AvgMaxMemoryGrant) AS BIGINT), 1) AS [Average Max Memory Grant],
		CONVERT(NVARCHAR(30), CAST((MinSpills) AS BIGINT), 1) AS [Min Spills],
		CONVERT(NVARCHAR(30), CAST((MaxSpills) AS BIGINT), 1) AS [Max Spills],
		CONVERT(NVARCHAR(30), CAST((TotalSpills) AS BIGINT), 1) AS [Total Spills],
		CONVERT(NVARCHAR(30), CAST((AvgSpills) AS MONEY), 1) AS [Avg Spills],
        CONVERT(NVARCHAR(30), CAST((NumberOfPlans) AS BIGINT), 1) AS [# Plans],
        CONVERT(NVARCHAR(30), CAST((NumberOfDistinctPlans) AS BIGINT), 1) AS [# Distinct Plans],
        PlanCreationTime AS [Created At],
        LastExecutionTime AS [Last Execution],
		LastCompletionTime AS [Last Completion],
        CONVERT(NVARCHAR(30), CAST((CachedPlanSize) AS BIGINT), 1) AS [Cached Plan Size (KB)],
        CONVERT(NVARCHAR(30), CAST((CompileTime) AS BIGINT), 1) AS [Compile Time (ms)],
        CONVERT(NVARCHAR(30), CAST((CompileCPU) AS BIGINT), 1) AS [Compile CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((CompileMemory) AS BIGINT), 1) AS [Compile memory (KB)],
        COALESCE(SetOptions, '''') AS [SET Options],
		PlanHandle AS [Plan Handle], 
		SqlHandle AS [SQL Handle], 
		[SQL Handle More Info],
        QueryHash AS [Query Hash],
		[Query Hash More Info],
        QueryPlanHash AS [Query Plan Hash],
        StatementStartOffset,
        StatementEndOffset,
		PlanGenerationNum,
		[Remove Plan Handle From Cache],
		[Remove SQL Handle From Cache]';
END;



SET @sql = N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT  TOP (@Top) ' + @columns + @nl + N'
FROM    ##BlitzCacheProcs
WHERE   SPID = @spid ' + @nl;

IF @MinimumExecutionCount IS NOT NULL
	BEGIN
		SET @sql += N' AND ExecutionCount >= @minimumExecutionCount ' + @nl;
	END;

IF @MinutesBack IS NOT NULL
    BEGIN
		SET @sql += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ' + @nl;
    END;

SELECT @sql += N' ORDER BY ' + CASE @SortOrder WHEN  N'cpu' THEN N' TotalCPU '
                                                WHEN N'reads' THEN N' TotalReads '
                                                WHEN N'writes' THEN N' TotalWrites '
                                                WHEN N'duration' THEN N' TotalDuration '
                                                WHEN N'executions' THEN N' ExecutionCount '
                                                WHEN N'compiles' THEN N' PlanCreationTime '
												WHEN N'memory grant' THEN N' MaxGrantKB'
												WHEN N'unused grant' THEN N' MaxGrantKB - MaxUsedGrantKB '
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
IF(@OutputType <> 'NONE')
BEGIN 
    EXEC  sp_executesql @sql, N'@Top INT, @spid INT, @minimumExecutionCount INT, @min_back INT', @Top, @@SPID, @MinimumExecutionCount, @MinutesBack;
END;

/*

This section will check if:
 * >= 30% of plans were created in the last hour
 * Check on the memory_clerks DMV for space used by TokenAndPermUserStore
 * Compare that to the size of the buffer pool
 * If it's >10%, 
*/
IF EXISTS
(
    SELECT 1/0
    FROM #plan_creation AS pc
    WHERE pc.percent_1 >= 30
)
BEGIN

SELECT @common_version =
           CONVERT(DECIMAL(10,2), c.common_version)
FROM #checkversion AS c;

IF @common_version >= 11
	SET @user_perm_sql = N'
	SET @buffer_pool_memory_gb = 0;
	SELECT @buffer_pool_memory_gb = SUM(pages_kb)/ 1024. / 1024.
	FROM sys.dm_os_memory_clerks
	WHERE type = ''MEMORYCLERK_SQLBUFFERPOOL'';'
ELSE
	SET @user_perm_sql = N'
	SET @buffer_pool_memory_gb = 0;
	SELECT @buffer_pool_memory_gb = SUM(single_pages_kb + multi_pages_kb)/ 1024. / 1024.
	FROM sys.dm_os_memory_clerks
	WHERE type = ''MEMORYCLERK_SQLBUFFERPOOL'';'

EXEC sys.sp_executesql @user_perm_sql,
	N'@buffer_pool_memory_gb DECIMAL(10,2) OUTPUT',
	@buffer_pool_memory_gb = @buffer_pool_memory_gb OUTPUT;

IF @common_version >= 11
BEGIN
    SET @user_perm_sql = N'
    	SELECT @user_perm_gb = CASE WHEN (pages_kb / 1024.0 / 1024.) >= 2.
    			                    THEN CONVERT(DECIMAL(38, 2), (pages_kb / 1024.0 / 1024.))
    			                    ELSE 0 
    		                   END
    	FROM sys.dm_os_memory_clerks
    	WHERE type = ''USERSTORE_TOKENPERM''
    	AND   name = ''TokenAndPermUserStore'';';
END;

IF @common_version < 11
BEGIN
    SET @user_perm_sql = N'
    	SELECT @user_perm_gb = CASE WHEN ((single_pages_kb + multi_pages_kb) / 1024.0 / 1024.) >= 2.
    			                    THEN CONVERT(DECIMAL(38, 2), ((single_pages_kb + multi_pages_kb)  / 1024.0 / 1024.))
    			                    ELSE 0 
    		                   END
    	FROM sys.dm_os_memory_clerks
    	WHERE type = ''USERSTORE_TOKENPERM''
    	AND   name = ''TokenAndPermUserStore'';';
END;

EXEC sys.sp_executesql @user_perm_sql, 
                       N'@user_perm_gb DECIMAL(10,2) OUTPUT', 
					   @user_perm_gb = @user_perm_gb_out OUTPUT;

IF @buffer_pool_memory_gb > 0
	BEGIN
	IF (@user_perm_gb_out / (1. * @buffer_pool_memory_gb)) * 100. >= 10
		BEGIN
			SET @is_tokenstore_big = 1;
			SET @user_perm_percent = (@user_perm_gb_out / (1. * @buffer_pool_memory_gb)) * 100.;
		END
	END

END



IF @HideSummary = 0 AND @ExportToExcel = 0
BEGIN
    IF @Reanalyze = 0
    BEGIN
        RAISERROR('Building query plan summary data.', 0, 1) WITH NOWAIT;

        /* Build summary data */
        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE frequent_execution = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    1,
                    100,
                    'Execution Pattern',
                    'Frequent Execution',
                    'https://www.brentozar.com/blitzcache/frequently-executed-queries/',
                    'Queries are being executed more than '
                    + CAST (@execution_threshold AS VARCHAR(5))
                    + ' times per minute. This can put additional load on the server, even when queries are lightweight.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  parameter_sniffing = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    2,
                    50,
                    'Parameterization',
                    'Parameter Sniffing',
                    'https://www.brentozar.com/blitzcache/parameter-sniffing/',
                    'There are signs of parameter sniffing (wide variance in rows return or time to execute). Investigate query patterns and tune code appropriately.') ;

        /* Forced execution plans */
        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_forced_plan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    3,
                    50,
                    'Parameterization',
                    'Forced Plan',
                    'https://www.brentozar.com/blitzcache/forced-plans/',
                    'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Cursor',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are cursors in the plan cache. This is neither good nor bad, but it is a thing. Cursors are weird in SQL Server.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_optimistic_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Optimistic Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are optimistic cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_forward_only_cursor = 0
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Non-forward Only Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are non-forward only cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_cursor_dynamic = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Dynamic Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Dynamic Cursors inhibit parallelism!.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_cursor = 1
				   AND is_fast_forward_cursor = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    4,
                    200,
                    'Cursors',
                    'Fast Forward Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Fast forward cursors inhibit parallelism!.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_forced_parameterized = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    5,
                    50,
                    'Parameterization',
                    'Forced Parameterization',
                    'https://www.brentozar.com/blitzcache/forced-parameterization/',
                    'Execution plans have been compiled with forced parameterization.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_parallel = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    6,
                    200,
                    'Execution Plans',
                    'Parallel',
                    'https://www.brentozar.com/blitzcache/parallel-plans-detected/',
                    'Parallel plans detected. These warrant investigation, but are neither good nor bad.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  near_parallel = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    7,
                    200,
                    'Execution Plans',
                    'Nearly Parallel',
                    'https://www.brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                    'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  plan_warnings = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    8,
                    50,
                    'Execution Plans',
                    'Plan Warnings',
                    'https://www.brentozar.com/blitzcache/query-plan-warnings/',
                    'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  long_running = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    9,
                    50,
                    'Performance',
                    'Long Running Query',
                    'https://www.brentozar.com/blitzcache/long-running-queries/',
                    'Long running queries have been found. These are queries with an average duration longer than '
                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 AS VARCHAR(5))
                    + ' second(s). These queries should be investigated for additional tuning options.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.missing_index_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    10,
                    50,
                    'Performance',
                    'Missing Indexes',
                    'https://www.brentozar.com/blitzcache/missing-index-request/',
                    'Queries found with missing indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.downlevel_estimator = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    13,
                    200,
                    'Cardinality',
                    'Downlevel CE',
                    'https://www.brentozar.com/blitzcache/legacy-cardinality-estimator/',
                    'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

        IF EXISTS (SELECT 1/0
                   FROM ##BlitzCacheProcs p
                   WHERE implicit_conversions = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    14,
                    50,
                    'Performance',
                    'Implicit Conversions',
                    'https://www.brentozar.com/go/implicit',
                    'One or more queries are comparing two fields that are not of the same data type.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  busy_loops = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                16,
                100,
                'Performance',
                'Busy Loops',
                'https://www.brentozar.com/blitzcache/busy-loops/',
                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  tvf_join = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                17,
                50,
                'Performance',
                'Function Join',
                'https://www.brentozar.com/blitzcache/tvf-join/',
                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  compile_timeout = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                18,
                50,
                'Execution Plans',
                'Compilation Timeout',
                'https://www.brentozar.com/blitzcache/compilation-timeout/',
                'Query compilation timed out for one or more queries. SQL Server did not find a plan that meets acceptable performance criteria in the time allotted so the best guess was returned. There is a very good chance that this plan isn''t even below average - it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  compile_memory_limit_exceeded = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                19,
                50,
                'Execution Plans',
                'Compile Memory Limit Exceeded',
                'https://www.brentozar.com/blitzcache/compile-memory-limit-exceeded/',
                'The optimizer has a limited amount of memory available. One or more queries are complex enough that SQL Server was unable to allocate enough memory to fully optimize the query. A best fit plan was found, and it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  warning_no_join_predicate = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                20,
                50,
                'Execution Plans',
                'No Join Predicate',
                'https://www.brentozar.com/blitzcache/no-join-predicate/',
                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  plan_multiple_plans > 0
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                21,
                200,
                'Execution Plans',
                'Multiple Plans',
                'https://www.brentozar.com/blitzcache/multiple-plans/',
                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  unmatched_index_count > 0
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                22,
                100,
                'Performance',
                'Unmatched Indexes',
                'https://www.brentozar.com/blitzcache/unmatched-indexes',
                'An index could have been used, but SQL Server chose not to use it - likely due to parameterization and filtered indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  unparameterized_query = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                23,
                100,
                'Parameterization',
                'Unparameterized Query',
                'https://www.brentozar.com/blitzcache/unparameterized-queries',
                'Unparameterized queries found. These could be ad hoc queries, data exploration, or queries using "OPTIMIZE FOR UNKNOWN".');

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs
                   WHERE  is_trivial = 1
				   AND SPID = @@SPID)
        INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                24,
                100,
                'Execution Plans',
                'Trivial Plans',
                'https://www.brentozar.com/blitzcache/trivial-plans',
                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');
    
        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_forced_serial= 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    25,
                    10,
                    'Execution Plans',
                    'Forced Serialization',
                    'https://www.brentozar.com/blitzcache/forced-serialization/',
                    'Something in your plan is forcing a serial query. Further investigation is needed if this is not by design.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_key_lookup_expensive= 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    26,
                    100,
                    'Execution Plans',
                    'Expensive Key Lookup',
                    'https://www.brentozar.com/blitzcache/expensive-key-lookups/',
                    'There''s a key lookup in your plan that costs >=50% of the total plan cost.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_remote_query_expensive= 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    28,
                    100,
                    'Execution Plans',
                    'Expensive Remote Query',
                    'https://www.brentozar.com/blitzcache/expensive-remote-query/',
                    'There''s a remote query in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.trace_flags_session IS NOT NULL
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    29,
                    200,
                    'Trace Flags',
                    'Session Level Trace Flags Enabled',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'Someone is enabling session level Trace Flags in a query.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_unused_grant IS NOT NULL
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    30,
                    100,
                    'Memory Grant',
                    'Unused Memory Grant',
                    'https://www.brentozar.com/blitzcache/unused-memory-grants/',
                    'Queries have large unused memory grants. This can cause concurrency issues, if queries are waiting a long time to get memory to run.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.function_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    31,
                    100,
                    'Compute Scalar That References A Function',
                    'Calls Functions',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'Both of these will force queries to run serially, run at least once per row, and may result in poor cardinality estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.clr_function_count > 0
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    32,
                    100,
                    'Compute Scalar That References A CLR Function',
                    'Calls CLR Functions',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'May force queries to run serially, run at least once per row, and may result in poor cardinality estimates.') ;


        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_table_variable = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    33,
                    100,
                    'Table Variables detected',
                    'Table Variables',
                    'https://www.brentozar.com/blitzcache/table-variables/',
                    'All modifications are single threaded, and selects have really low row estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.no_stats_warning = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    35,
                    100,
                    'Statistics',
                    'Columns With No Statistics',
                    'https://www.brentozar.com/blitzcache/columns-no-statistics/',
                    'Sometimes this happens with indexed views, other times because auto create stats is turned off.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.relop_warnings = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    36,
                    100,
                    'Warnings',
					'Operator Warnings',
                    'https://www.brentozar.com/blitzcache/query-plan-warnings/',
                    'Check the plan for more details.') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_table_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    37,
                    100,
                    'Indexes',
                    'Table Scans (Heaps)',
                    'https://www.brentozar.com/archive/2012/05/video-heaps/',
                    'This may not be a problem. Run sp_BlitzIndex for more information.') ;
        
		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.backwards_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    38,
                    200,
                    'Indexes',
                    'Backwards Scans',
                    'https://www.brentozar.com/blitzcache/backwards-scans/',
                    'This isn''t always a problem. They can cause serial zones in plans, and may need an index to match sort order.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.forced_index = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    39,
                    100,
                    'Indexes',
                    'Forced Indexes',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans, and will prevent missing index requests.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.forced_seek = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    40,
                    100,
                    'Indexes',
					'Forced Seeks',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.forced_scan = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    40,
                    100,
                    'Indexes',
                    'Forced Scans',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.columnstore_row_mode = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    41,
                    100,
                    'Indexes',
                    'ColumnStore Row Mode',
                    'https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/',
                    'ColumnStore indexes operating in Row Mode indicate really poor query choices.') ;

		IF EXISTS (SELECT 1/0
                   FROM   ##BlitzCacheProcs p
                   WHERE  p.is_computed_scalar = 1
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    42,
                    50,
                    'Functions',
                    'Computed Column UDF',
                    'https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/',
                    'This can cause a whole mess of bad serializartion problems.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_sort_expensive = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     43,
                     100,
                     'Execution Plans',
                     'Expensive Sort',
                     'https://www.brentozar.com/blitzcache/expensive-sorts/',
                     'There''s a sort in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_computed_filter = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     44,
                     50,
                     'Functions',
                     'Filter UDF',
                     'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                     'Someone put a Scalar UDF in the WHERE clause!') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.index_ops >= 5
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     45,
                     100,
                     'Indexes',
                     '>= 5 Indexes Modified',
                     'https://www.brentozar.com/blitzcache/many-indexes-modified/',
                     'This can cause lots of hidden I/O -- Run sp_BlitzIndex for more information.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_row_level = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     46,
                     200,
                     'Complexity',
                     'Row Level Security',
                     'https://www.brentozar.com/blitzcache/row-level-security/',
                     'You may see a lot of confusing junk in your query plan.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_spatial = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     47,
                     200,
                     'Complexity',
                     'Spatial Index',
                     'https://www.brentozar.com/blitzcache/spatial-indexes/',
                     'Purely informational.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.index_dml = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     48,
                     150,
                     'Complexity',
                     'Index DML',
                     'https://www.brentozar.com/blitzcache/index-dml/',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.table_dml = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     49,
                     150,
                     'Complexity',
					 'Table DML',
                     'https://www.brentozar.com/blitzcache/table-dml/',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.long_running_low_cpu = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     50,
                     150,
                     'Blocking',
                     'Long Running Low CPU',
                     'https://www.brentozar.com/blitzcache/long-running-low-cpu/',
                     'This can be a sign of blocking, linked servers, or poor client application code (ASYNC_NETWORK_IO).') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.low_cost_high_cpu = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     51,
                     150,
                     'Complexity',
                     'Low Cost Query With High CPU',
                     'https://www.brentozar.com/blitzcache/low-cost-high-cpu/',
                     'This can be a sign of functions or Dynamic SQL that calls black-box code.') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.stale_stats = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     52,
                     150,
                     'Statistics',
                     'Statistics used have > 100k modifications in the last 7 days',
                     'https://www.brentozar.com/blitzcache/stale-statistics/',
                     'Ever heard of updating statistics?') ;

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_adaptive = 1
  					AND SPID = @@SPID)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     53,
                     200,
                     'Complexity',
					 'Adaptive joins',
                     'https://www.brentozar.com/blitzcache/adaptive-joins/',
                     'This join will sometimes do seeks, and sometimes do scans.') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_spool_expensive = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     54,
                     150,
                     'Indexes',
                     'Expensive Index Spool',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_spool_more_rows = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     55,
                     150,
                     'Indexes',
					 'Large Index Row Spool',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;
					 
        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_bad_estimate = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     56,
                     100,
                     'Complexity',
                     'Row Estimate Mismatch',
                     'https://www.brentozar.com/blitzcache/bad-estimates/',
                     'Estimated rows are different from average rows by a factor of 10000. This may indicate a performance problem if mismatches occur regularly') ;	
					 					 						 				 
        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_paul_white_electric = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     57,
                     200,
                     'Is Paul White Electric?',
                     'This query has a Switch operator in it!',
                     'https://www.sql.kiwi/2013/06/hello-operator-my-switch-is-bored.html',
                     'You should email this query plan to Paul: SQLkiwi at gmail dot com') ;	

		IF @v >= 14 OR (@v = 13 AND @build >= 5026)
			BEGIN	

				INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT 
				@@SPID,
				997,
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
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_row_goal = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     58,
                     200,
                     'Complexity',
					 'Row Goals',
                     'https://www.brentozar.com/go/rowgoals/',
                     'This query had row goals introduced, which can be good or bad, and should be investigated for high read queries.') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_big_spills = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     59,
                     100,
                     'TempDB',
					 '>500mb Spills',
                     'https://www.brentozar.com/blitzcache/tempdb-spills/',
                     'This query spills >500mb to tempdb on average. One way or another, this query didn''t get enough memory') ;	


			END; 

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_mstvf = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     60,
                     100,
                     'Functions',
					 'MSTVFs',
                     'https://www.brentozar.com/blitzcache/tvf-join/',
					 'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_mm_join = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     61,
                     100,
                     'Complexity',
					 'Many to Many Merge',
                     'https://www.brentozar.com/archive/2018/04/many-mysteries-merge-joins/',
					 'These use secret worktables that could be doing lots of reads. Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_nonsargable = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     62,
                     50,
                     'Non-SARGable queries',
                     'non-SARGables',
                     'https://www.brentozar.com/blitzcache/non-sargable-predicates/',
					 'Looks for intrinsic functions and expressions as predicates, and leading wildcard LIKE searches.');	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  CompileTime > 5000
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     63,
                     100,
                     'Complexity',
					 'Long Compile Time',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'Queries are taking >5 seconds to compile. This can be normal for large plans, but be careful if they compile frequently');	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  CompileCPU > 5000
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     64,
                     50,
                     'Complexity',
                     'High Compile CPU',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'Queries taking >5 seconds of CPU to compile. If CPU is high and plans like this compile frequently, they may be related');

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  CompileMemory > 1024 
					AND    ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10.
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     65,
                     50,
                     'Complexity',
                     'High Compile Memory',
                     'https://www.brentozar.com/blitzcache/high-compilers/',
					 'Queries taking 10% of Max Compile Memory. If you see high RESOURCE_SEMAPHORE_QUERY_COMPILE waits, these may be related');

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.select_with_writes = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     66,
                     50,
					 'Complexity',
                     'Selects w/ Writes',
                     'https://dba.stackexchange.com/questions/191825/',
					 'This is thrown when reads cause writes that are not already flagged as big spills (2016+) or index spools.');

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_table_spool_expensive = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     67,
                     150,
                     'Expensive Table Spool',
                     'You have a table spool, this is usually a sign that queries are doing unnecessary work',
                     'https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools',
                     'Check for non-SARGable predicates, or a lot of work being done inside a nested loops join') ;	

        IF EXISTS (SELECT 1/0
                    FROM   ##BlitzCacheProcs p
                    WHERE  p.is_table_spool_more_rows = 1
  					)
             INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (@@SPID,
                     68,
                     150,
                     'Table Spools Many Rows',
                     'You have a table spool that spools more rows than the query returns',
                     'https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools',
                     'Check for non-SARGable predicates, or a lot of work being done inside a nested loops join');

        IF EXISTS (SELECT 1/0
                   FROM   #plan_creation p
                   WHERE (p.percent_24 > 0)
				   AND SPID = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT SPID,
                    999,
                    CASE WHEN ISNULL(p.percent_24, 0) > 75 THEN 1 ELSE 254 END AS Priority,
                    'Plan Cache Information',
                    CASE WHEN ISNULL(p.percent_24, 0) > 75 THEN 'Plan Cache Instability' ELSE 'Plan Cache Stability' END AS Finding,
                    'https://www.brentozar.com/archive/2018/07/tsql2sday-how-much-plan-cache-history-do-you-have/',
                    'You have ' + CONVERT(NVARCHAR(10), ISNULL(p.total_plans, 0)) 
								+ ' total plans in your cache, with ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_24, 0)) 
								+ '% plans created in the past 24 hours, ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_4, 0)) 
								+ '% created in the past 4 hours, and ' 
								+ CONVERT(NVARCHAR(10), ISNULL(p.percent_1, 0)) 
								+ '% created in the past 1 hour. '
								+ 'When these percentages are high, it may be a sign of memory pressure or plan cache instability.'
			FROM   #plan_creation p	;

        IF EXISTS (SELECT 1/0
                   FROM   #plan_usage p
                   WHERE  p.percent_duplicate > 5
				   AND spid = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT spid,
                    999,
                    CASE WHEN ISNULL(p.percent_duplicate, 0) > 75 THEN 1 ELSE 254 END AS Priority,
                    'Plan Cache Information',
                    CASE WHEN ISNULL(p.percent_duplicate, 0) > 75 THEN 'Many Duplicate Plans' ELSE 'Duplicate Plans' END AS Finding,
					'https://www.brentozar.com/archive/2018/03/why-multiple-plans-for-one-query-are-bad/',
					'You have ' + CONVERT(NVARCHAR(10), p.total_plans)
					            + ' plans in your cache, and '
								+ CONVERT(NVARCHAR(10), p.percent_duplicate)
								+ '% are duplicates with more than 5 entries'
								+ ', meaning similar queries are generating the same plan repeatedly.'
								+ ' Forced Parameterization may fix the issue. To find troublemakers, use: EXEC sp_BlitzCache @SortOrder = ''query hash''; '
			FROM #plan_usage AS p ;

        IF EXISTS (SELECT 1/0
                   FROM   #plan_usage p
                   WHERE  p.percent_single > 5
				   AND spid = @@SPID)
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT spid,
                    999,
                    CASE WHEN ISNULL(p.percent_single, 0) > 75 THEN 1 ELSE 254 END AS Priority,
					'Plan Cache Information',
                    CASE WHEN ISNULL(p.percent_single, 0) > 75 THEN 'Many Single-Use Plans' ELSE 'Single-Use Plans' END AS Finding,
					'https://www.brentozar.com/blitz/single-use-plans-procedure-cache/',
					'You have ' + CONVERT(NVARCHAR(10), p.total_plans)
					            + ' plans in your cache, and '
								+ CONVERT(NVARCHAR(10), p.percent_single)
								+ '% are single use plans'
								+ ', meaning SQL Server thinks it''s seeing a lot of "new" queries and creating plans for them.'
								+ ' Forced Parameterization and/or Optimize For Ad Hoc Workloads may fix the issue.'
					            + 'To find troublemakers, use: EXEC sp_BlitzCache @SortOrder = ''query hash''; '
			FROM #plan_usage AS p ;

        IF @is_tokenstore_big = 1
		INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
		SELECT @@SPID,
		       69,
			   10,
			   N'Large USERSTORE_TOKENPERM cache: ' + CONVERT(NVARCHAR(11), @user_perm_gb_out) + N'GB',
			   N'The USERSTORE_TOKENPERM is taking up ' + CONVERT(NVARCHAR(11), @user_perm_percent)
			                                            + N'% of the buffer pool, and your plan cache seems to be unstable',
			   N'https://www.brentozar.com/go/userstore',
			   N'A growing USERSTORE_TOKENPERM cache can cause the plan cache to clear out'

		IF @v >= 11
		BEGIN	
        IF EXISTS (SELECT 1/0
                   FROM   #trace_flags AS tf 
                   WHERE  tf.global_trace_flags IS NOT NULL
				   )
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    1000,
                    255,
                    'Global Trace Flags Enabled',
                    'You have Global Trace Flags enabled on your server',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'You have the following Global Trace Flags enabled: ' + (SELECT TOP 1 tf.global_trace_flags FROM #trace_flags AS tf WHERE tf.global_trace_flags IS NOT NULL)) ;
		END; 

        IF NOT EXISTS (SELECT 1/0
					   FROM   ##BlitzCacheResults AS bcr
                       WHERE  bcr.Priority = 2147483646
				      )
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    2147483646,
                    255,
                    'Need more help?' ,
                    'Paste your plan on the internet!',
                    'http://pastetheplan.com',
                    'This makes it easy to share plans and post them to Q&A sites like https://dba.stackexchange.com/!') ;



        IF NOT EXISTS (SELECT 1/0
					   FROM   ##BlitzCacheResults AS bcr
                       WHERE  bcr.Priority = 2147483647
				      )
            INSERT INTO ##BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
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
    FROM    ##BlitzCacheResults
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
		
		SELECT '##BlitzCacheResults' AS table_name, *
		FROM   ##BlitzCacheResults
		OPTION ( RECOMPILE );
		
		SELECT '##BlitzCacheProcs' AS table_name, *
		FROM   ##BlitzCacheProcs
		OPTION ( RECOMPILE );
		
		SELECT '#statements' AS table_name, *
		FROM #statements AS s
		OPTION (RECOMPILE);

		SELECT '#query_plan' AS table_name, *
		FROM #query_plan AS qp
		OPTION (RECOMPILE);
		
		SELECT '#relop' AS table_name, *
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

		SELECT '#plan_usage' AS table_name, *
		FROM   #plan_usage
		OPTION ( RECOMPILE );

    END;

    IF @OutputTableName IS NOT NULL
	    --Allow for output to ##DB so don't check for DB or schema name here
	   GOTO OutputResultsToTable;
RETURN; --Avoid going into the AllSort GOTO

/*Begin code to sort by all*/
AllSorts:
RAISERROR('Beginning all sort loop', 0, 1) WITH NOWAIT;


IF (
     @Top > 10
	 AND @SkipAnalysis = 0
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
           ExecutionCount NVARCHAR(30),
           ExecutionsPerMinute MONEY,
           ExecutionWeight MONEY,
           TotalCPU NVARCHAR(30),
           AverageCPU NVARCHAR(30),
           CPUWeight MONEY,
           TotalDuration NVARCHAR(30),
           AverageDuration NVARCHAR(30),
           DurationWeight MONEY,
           TotalReads NVARCHAR(30),
           AverageReads NVARCHAR(30),
           ReadWeight MONEY,
           TotalWrites NVARCHAR(30),
           AverageWrites NVARCHAR(30),
           WriteWeight MONEY,
           AverageReturnedRows MONEY,
           MinGrantKB NVARCHAR(30),
           MaxGrantKB NVARCHAR(30),
           MinUsedGrantKB NVARCHAR(30),
           MaxUsedGrantKB NVARCHAR(30),
           AvgMaxMemoryGrant MONEY,
		   MinSpills NVARCHAR(30),
		   MaxSpills NVARCHAR(30),
		   TotalSpills NVARCHAR(30),
		   AvgSpills MONEY,
           PlanCreationTime DATETIME,
           LastExecutionTime DATETIME,
		   LastCompletionTime DATETIME,
           PlanHandle VARBINARY(64),
           SqlHandle VARBINARY(64),
           SetOptions VARCHAR(MAX),
		   QueryHash BINARY(8),
		   PlanGenerationNum NVARCHAR(30),
		   RemovePlanHandleFromCache NVARCHAR(200),
           Pattern NVARCHAR(20)
         );
   END;


IF @SortOrder = 'all'
BEGIN
RAISERROR('Beginning for ALL', 0, 1) WITH NOWAIT;
SET @AllSortSql += N'
					DECLARE @ISH NVARCHAR(MAX) = N''''

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''cpu'', 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''reads'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''writes'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''duration'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''executions'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);
					 
					 '; 
					
					IF @VersionShowsMemoryGrants = 0
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

					END; 
					
					IF @VersionShowsMemoryGrants = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''memory grant'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 					  
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF @VersionShowsSpills = 0
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

					END; 
					
					IF @VersionShowsSpills = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache ) 					 
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''spills'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 					  
										  UPDATE #bou_allsort SET Pattern = ''spills'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
						IF @ExportToExcel = 1
						BEGIN
							SET @AllSortSql += N'  UPDATE #bou_allsort 
												   SET 
													QueryPlan = NULL,
													implicit_conversion_info = NULL, 
													cached_execution_parameters = NULL, 
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF(@OutputType <> 'NONE')
					BEGIN
						SET @AllSortSql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache, Pattern 
											  FROM #bou_allsort 
											  ORDER BY Id 
											  OPTION(RECOMPILE);  ';
					END;
END; 			


IF @SortOrder = 'all avg'
BEGIN 
RAISERROR('Beginning for ALL AVG', 0, 1) WITH NOWAIT;
SET @AllSortSql += N' 
					DECLARE @ISH NVARCHAR(MAX) = N'''' 
					
					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg cpu'', 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg reads'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg writes'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg duration'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
					 
					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg executions'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 
					 UPDATE #bou_allsort SET Pattern = ''avg executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);
					 
					 ';
					 
					IF @VersionShowsMemoryGrants = 0
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END;  

					END; 
					
					IF @VersionShowsMemoryGrants = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg memory grant'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 					  
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF @VersionShowsSpills = 0
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END;  

					END; 
					
					IF @VersionShowsSpills = 1
					BEGIN 
					SET @AllSortSql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);
					
						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache )
										  
										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg spills'', @IgnoreSqlHandles = @ISH, 
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;
					 					  
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

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
						END; 

				    END;

					IF(@OutputType <> 'NONE')
					BEGIN
						SET @AllSortSql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight, 
											  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads, 
											  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, 
											  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, QueryHash, PlanGenerationNum, RemovePlanHandleFromCache, Pattern
											  FROM #bou_allsort 
											  ORDER BY Id 
											  OPTION(RECOMPILE);  ';
					END;
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

					EXEC sys.sp_executesql @stmt = @AllSortSql, @params = N'@i_DatabaseName NVARCHAR(128), @i_Top INT, @i_SkipAnalysis BIT, @i_OutputDatabaseName NVARCHAR(258), @i_OutputSchemaName NVARCHAR(258), @i_OutputTableName NVARCHAR(258), @i_CheckDateOverride DATETIMEOFFSET, @i_MinutesBack INT ', 
                        @i_DatabaseName = @DatabaseName, @i_Top = @Top, @i_SkipAnalysis = @SkipAnalysis, @i_OutputDatabaseName = @OutputDatabaseName, @i_OutputSchemaName = @OutputSchemaName, @i_OutputTableName = @OutputTableName, @i_CheckDateOverride = @CheckDateOverride, @i_MinutesBack = @MinutesBack;

/*End of AllSort section*/


/*Begin code to write results to table */
OutputResultsToTable:
    
RAISERROR('Writing results to table.', 0, 1) WITH NOWAIT;

SELECT @OutputServerName   = QUOTENAME(@OutputServerName),
       @OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
       @OutputSchemaName   = QUOTENAME(@OutputSchemaName),
       @OutputTableName    = QUOTENAME(@OutputTableName);

/* Checks if @OutputServerName is populated with a valid linked server, and that the database name specified is valid */
DECLARE @ValidOutputServer BIT;
DECLARE @ValidOutputLocation BIT;
DECLARE @LinkedServerDBCheck NVARCHAR(2000);
DECLARE @ValidLinkedServerDB INT;
DECLARE @tmpdbchk table (cnt int);
IF @OutputServerName IS NOT NULL
	BEGIN
					
		IF @Debug IN (1, 2) RAISERROR('Outputting to a remote server.', 0, 1) WITH NOWAIT;
					
		IF EXISTS (SELECT server_id FROM sys.servers WHERE QUOTENAME([name]) = @OutputServerName)
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
		IF @OutputDatabaseName IS NOT NULL
			AND @OutputSchemaName IS NOT NULL
			AND @OutputTableName IS NOT NULL
			AND EXISTS ( SELECT *
				FROM   sys.databases
				WHERE  QUOTENAME([name]) = @OutputDatabaseName)
			BEGIN
			SET @ValidOutputLocation = 1;
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

    /* @OutputTableName lets us export the results to a permanent table */
    DECLARE @StringToExecute NVARCHAR(MAX) = N'' ;

    IF @ValidOutputLocation = 1
		BEGIN
			SET @StringToExecute = N'USE '
				+ @OutputDatabaseName
				+ N'; IF EXISTS(SELECT * FROM '
				+ @OutputDatabaseName
				+ N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
				+ @OutputSchemaName
				+ N''') AND NOT EXISTS (SELECT * FROM '
				+ @OutputDatabaseName
				+ N'.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
				+ @OutputSchemaName + N''' AND QUOTENAME(TABLE_NAME) = '''
				+ @OutputTableName + N''') CREATE TABLE '
				+ @OutputSchemaName + N'.'
				+ @OutputTableName
				+ CONVERT
                  (
                  nvarchar(MAX),
				 N'(ID bigint NOT NULL IDENTITY(1,1),
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
					PercentExecutionsByType money,
					ExecutionsPerMinute money,
					PlanCreationTime datetime,' + N'
					PlanCreationTimeHours AS DATEDIFF(HOUR,CONVERT(DATETIMEOFFSET(7),[PlanCreationTime]),[CheckDate]),
					LastExecutionTime datetime,
					LastCompletionTime datetime, 
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
					PlanGenerationNum bigint,
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
					JoinKey AS ServerName + Cast(CheckDate AS NVARCHAR(50)),
					CONSTRAINT [PK_' + REPLACE(REPLACE(@OutputTableName,N'[',N''),N']',N'') + N'] PRIMARY KEY CLUSTERED(ID ASC));'
				  );

			SET @StringToExecute += N'IF EXISTS(SELECT * FROM '
					+@OutputDatabaseName
					+N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
					+@OutputSchemaName
					+N''') AND EXISTS (SELECT * FROM '
					+@OutputDatabaseName+
					N'.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
					+@OutputSchemaName
					+N''' AND QUOTENAME(TABLE_NAME) = '''
					+@OutputTableName
					+N''') AND EXISTS (SELECT * FROM '
					+@OutputDatabaseName+
					N'.sys.computed_columns WHERE [name] = N''PlanCreationTimeHours'' AND QUOTENAME(OBJECT_NAME(object_id)) = N'''
					+@OutputTableName
					+N''' AND [definition] = N''(datediff(hour,[PlanCreationTime],sysdatetime()))'')
BEGIN 
	RAISERROR(''We noticed that you are running an old computed column definition for PlanCreationTimeHours, fixing that now'',0,0) WITH NOWAIT;
	ALTER TABLE '+@OutputDatabaseName+N'.'+@OutputSchemaName+N'.'+@OutputTableName+N' DROP COLUMN [PlanCreationTimeHours];
	ALTER TABLE '+@OutputDatabaseName+N'.'+@OutputSchemaName+N'.'+@OutputTableName+N' ADD [PlanCreationTimeHours] AS DATEDIFF(HOUR,CONVERT(DATETIMEOFFSET(7),[PlanCreationTime]),[CheckDate]);
END ';

            IF @ValidOutputServer = 1
				BEGIN
					SET @StringToExecute = REPLACE(@StringToExecute,''''+@OutputSchemaName+'''',''''''+@OutputSchemaName+'''''');
					SET @StringToExecute = REPLACE(@StringToExecute,''''+@OutputTableName+'''',''''''+@OutputTableName+'''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'xml','nvarchar(max)');
					SET @StringToExecute = REPLACE(@StringToExecute,'''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [PlanHandle], 1) + '');''','''''DBCC FREEPROCCACHE ('''' + CONVERT(VARCHAR(128), [PlanHandle], 1) + '''');''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '');''','''''DBCC FREEPROCCACHE ('''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '''');''''');
                    SET @StringToExecute = REPLACE(@StringToExecute,'''EXEC sp_BlitzCache @OnlySqlHandles = '''''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ''''''; ''','''''EXEC sp_BlitzCache @OnlySqlHandles = '''''''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ''''''''; ''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'''EXEC sp_BlitzCache @OnlyQueryHashes = '''''' + CONVERT(VARCHAR(32), [QueryHash], 1) + ''''''; ''','''''EXEC sp_BlitzCache @OnlyQueryHashes = '''''''' + CONVERT(VARCHAR(32), [QueryHash], 1) + ''''''''; ''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'''N/A''','''''N/A''''');
					
                    IF @Debug = 1
                    BEGIN
                        PRINT SUBSTRING(@StringToExecute, 0, 4000);
                        PRINT SUBSTRING(@StringToExecute, 4000, 8000);
                        PRINT SUBSTRING(@StringToExecute, 8000, 12000);
                        PRINT SUBSTRING(@StringToExecute, 12000, 16000);
                        PRINT SUBSTRING(@StringToExecute, 16000, 20000);
                        PRINT SUBSTRING(@StringToExecute, 20000, 24000);
                        PRINT SUBSTRING(@StringToExecute, 24000, 28000);
                        PRINT SUBSTRING(@StringToExecute, 28000, 32000);
                        PRINT SUBSTRING(@StringToExecute, 32000, 36000);
                        PRINT SUBSTRING(@StringToExecute, 36000, 40000);
                    END;
                    EXEC('EXEC('''+@StringToExecute+''') AT ' + @OutputServerName);
                END;
            ELSE
                BEGIN
                    IF @Debug = 1
                    BEGIN
                        PRINT SUBSTRING(@StringToExecute, 0, 4000);
                        PRINT SUBSTRING(@StringToExecute, 4000, 8000);
                        PRINT SUBSTRING(@StringToExecute, 8000, 12000);
                        PRINT SUBSTRING(@StringToExecute, 12000, 16000);
                        PRINT SUBSTRING(@StringToExecute, 16000, 20000);
                        PRINT SUBSTRING(@StringToExecute, 20000, 24000);
                        PRINT SUBSTRING(@StringToExecute, 24000, 28000);
                        PRINT SUBSTRING(@StringToExecute, 28000, 32000);
                        PRINT SUBSTRING(@StringToExecute, 32000, 36000);
                        PRINT SUBSTRING(@StringToExecute, 36000, 40000);
                    END;
					EXEC(@StringToExecute);
                END;
            
            /* If the table doesn't have the new LastCompletionTime column, add it. See Github #2377. */
            SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
            SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + ''')) AND name = ''LastCompletionTime'')
                ALTER TABLE ' + @ObjectFullName + N' ADD LastCompletionTime DATETIME NULL;';
            IF @ValidOutputServer = 1
				BEGIN
					SET @StringToExecute = REPLACE(@StringToExecute,'''LastCompletionTime''','''''LastCompletionTime''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'''' + @ObjectFullName + '''','''''' + @ObjectFullName + '''''');
					EXEC('EXEC('''+@StringToExecute+''') AT ' + @OutputServerName);
                END;
            ELSE
                BEGIN
                    EXEC(@StringToExecute);
                END;

            /* If the table doesn't have the new PlanGenerationNum column, add it. See Github #2514. */
            SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
            SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''PlanGenerationNum'')
                ALTER TABLE ' + @ObjectFullName + N' ADD PlanGenerationNum BIGINT NULL;';
			IF @ValidOutputServer = 1
				BEGIN
					SET @StringToExecute = REPLACE(@StringToExecute,'''PlanGenerationNum''','''''PlanGenerationNum''''');
					SET @StringToExecute = REPLACE(@StringToExecute,'''' + @ObjectFullName + '''','''''' + @ObjectFullName + '''''');
                    EXEC('EXEC('''+@StringToExecute+''') AT ' + @OutputServerName);
                END;
            ELSE
                BEGIN
                    EXEC(@StringToExecute);
                END;
            
            IF @CheckDateOverride IS NULL
                BEGIN
                    SET @CheckDateOverride = SYSDATETIMEOFFSET();
                END;
            
            IF @ValidOutputServer = 1
				BEGIN
					SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
					+ @OutputServerName + '.'
					+ @OutputDatabaseName
					+ '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
					+ @OutputSchemaName + ''') INSERT '
					+ @OutputServerName + '.'
					+ @OutputDatabaseName + '.'
					+ @OutputSchemaName + '.'
					+ @OutputTableName
					+ ' (ServerName, CheckDate, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
                    + ' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
                    + ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                    + ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost ) '
                    + 'SELECT TOP (@Top) '
                    + QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), '''') + ', @CheckDateOverride, '
                    + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), '''') + ', '
                    + ' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
                    + ' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
                    + ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, CAST(QueryPlan AS NVARCHAR(MAX)), NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                    + ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost '
                    + ' FROM ##BlitzCacheProcs '
                    + ' WHERE 1=1 ';

                IF @MinimumExecutionCount IS NOT NULL
                    BEGIN
                        SET @StringToExecute += N' AND ExecutionCount >= @MinimumExecutionCount ';
                    END;
                IF @MinutesBack IS NOT NULL
                    BEGIN
                        SET @StringToExecute += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
                    END;
                    SET @StringToExecute += N' AND SPID = @@SPID ';
                    SELECT @StringToExecute += N' ORDER BY ' + CASE @SortOrder WHEN 'cpu' THEN N' TotalCPU '
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
                    SET @StringToExecute += N' OPTION (RECOMPILE) ; ';    
                    
                    IF @Debug = 1
                    BEGIN
                        PRINT SUBSTRING(@StringToExecute, 0, 4000);
                        PRINT SUBSTRING(@StringToExecute, 4000, 8000);
                        PRINT SUBSTRING(@StringToExecute, 8000, 12000);
                        PRINT SUBSTRING(@StringToExecute, 12000, 16000);
                        PRINT SUBSTRING(@StringToExecute, 16000, 20000);
                        PRINT SUBSTRING(@StringToExecute, 20000, 24000);
                        PRINT SUBSTRING(@StringToExecute, 24000, 28000);
                        PRINT SUBSTRING(@StringToExecute, 28000, 32000);
                        PRINT SUBSTRING(@StringToExecute, 32000, 36000);
                        PRINT SUBSTRING(@StringToExecute, 36000, 40000);
                    END;

                    EXEC sp_executesql @StringToExecute, N'@Top INT, @min_duration INT, @min_back INT, @CheckDateOverride DATETIMEOFFSET, @MinimumExecutionCount INT', @Top, @DurationFilter_i, @MinutesBack, @CheckDateOverride, @MinimumExecutionCount;
				END;
			ELSE
				BEGIN
					SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
					+ @OutputDatabaseName
					+ '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
					+ @OutputSchemaName + ''') INSERT '
					+ @OutputDatabaseName + '.'
					+ @OutputSchemaName + '.'
					+ @OutputTableName
					+ ' (ServerName, CheckDate, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
                    + ' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
                    + ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                    + ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost ) '
                    + 'SELECT TOP (@Top) '
                    + QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), '''') + ', @CheckDateOverride, '
                    + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), '''') + ', '
                    + ' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
                    + ' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
                    + ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                    + ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost '
                    + ' FROM ##BlitzCacheProcs '
                    + ' WHERE 1=1 ';

                IF @MinimumExecutionCount IS NOT NULL
                    BEGIN
                        SET @StringToExecute += N' AND ExecutionCount >= @MinimumExecutionCount ';
                    END;
                IF @MinutesBack IS NOT NULL
                    BEGIN
                        SET @StringToExecute += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
                    END;
                    SET @StringToExecute += N' AND SPID = @@SPID ';
                    SELECT @StringToExecute += N' ORDER BY ' + CASE @SortOrder WHEN 'cpu' THEN N' TotalCPU '
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
                    SET @StringToExecute += N' OPTION (RECOMPILE) ; ';    
                    
                    IF @Debug = 1
                    BEGIN
                        PRINT SUBSTRING(@StringToExecute, 0, 4000);
                        PRINT SUBSTRING(@StringToExecute, 4000, 8000);
                        PRINT SUBSTRING(@StringToExecute, 8000, 12000);
                        PRINT SUBSTRING(@StringToExecute, 12000, 16000);
                        PRINT SUBSTRING(@StringToExecute, 16000, 20000);
                        PRINT SUBSTRING(@StringToExecute, 20000, 24000);
                        PRINT SUBSTRING(@StringToExecute, 24000, 28000);
                        PRINT SUBSTRING(@StringToExecute, 28000, 32000);
                        PRINT SUBSTRING(@StringToExecute, 32000, 36000);
                        PRINT SUBSTRING(@StringToExecute, 36000, 40000);
                    END;

                    EXEC sp_executesql @StringToExecute, N'@Top INT, @min_duration INT, @min_back INT, @CheckDateOverride DATETIMEOFFSET, @MinimumExecutionCount INT', @Top, @DurationFilter_i, @MinutesBack, @CheckDateOverride, @MinimumExecutionCount;
				END;
		END;
	ELSE IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
		BEGIN
			IF @ValidOutputServer = 1
				BEGIN
					RAISERROR('Due to the nature of temporary tables, outputting to a linked server requires a permanent table.', 16, 0);
				END;
			ELSE IF @OutputTableName IN ('##BlitzCacheProcs','##BlitzCacheResults')
				BEGIN
					RAISERROR('OutputTableName is a reserved name for this procedure. We only use ##BlitzCacheProcs and ##BlitzCacheResults, please choose another table name.', 16, 0);
				END;
			ELSE
				BEGIN				
					SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
						+ @OutputTableName
						+ ''') IS NOT NULL) DROP TABLE ' + @OutputTableName + ';'
						+ 'CREATE TABLE '
						+ @OutputTableName
						+ ' (ID bigint NOT NULL IDENTITY(1,1),
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
						PercentExecutionsByType money,
						ExecutionsPerMinute money,
						PlanCreationTime datetime,' + N'
						PlanCreationTimeHours AS DATEDIFF(HOUR, PlanCreationTime, SYSDATETIME()),
						LastExecutionTime datetime,
						LastCompletionTime datetime, 
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
						PlanGenerationNum bigint,
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
						JoinKey AS ServerName + Cast(CheckDate AS NVARCHAR(50)),
						CONSTRAINT [PK_' + REPLACE(REPLACE(@OutputTableName,'[',''),']','') + '] PRIMARY KEY CLUSTERED(ID ASC));';
					SET @StringToExecute +=	N' INSERT '
						+ @OutputTableName
						+ ' (ServerName, CheckDate, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
						+ ' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
						+ ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
						+ ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost ) '
						+ 'SELECT TOP (@Top) '
						+ QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), '''') + ', @CheckDateOverride, '
						+ QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)), '''') + ', '
						+ ' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
						+ ' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
						+ ' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, PlanGenerationNum, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
						+ ' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost '
						+ ' FROM ##BlitzCacheProcs '
						+ ' WHERE 1=1 ';
                        
                    IF @MinimumExecutionCount IS NOT NULL
                        BEGIN
                            SET @StringToExecute += N' AND ExecutionCount >= @MinimumExecutionCount ';
                        END;

                    IF @MinutesBack IS NOT NULL
                        BEGIN
                            SET @StringToExecute += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
                        END;

                        SET @StringToExecute += N' AND SPID = @@SPID ';
                        
                        SELECT @StringToExecute += N' ORDER BY ' + CASE @SortOrder WHEN 'cpu' THEN N' TotalCPU '
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

                        SET @StringToExecute += N' OPTION (RECOMPILE) ; ';    
                        
                        IF @Debug = 1
                        BEGIN
                            PRINT SUBSTRING(@StringToExecute, 0, 4000);
                            PRINT SUBSTRING(@StringToExecute, 4000, 8000);
                            PRINT SUBSTRING(@StringToExecute, 8000, 12000);
                            PRINT SUBSTRING(@StringToExecute, 12000, 16000);
                            PRINT SUBSTRING(@StringToExecute, 16000, 20000);
                            PRINT SUBSTRING(@StringToExecute, 20000, 24000);
                            PRINT SUBSTRING(@StringToExecute, 24000, 28000);
                            PRINT SUBSTRING(@StringToExecute, 28000, 32000);
                            PRINT SUBSTRING(@StringToExecute, 32000, 36000);
                            PRINT SUBSTRING(@StringToExecute, 36000, 40000);
                            PRINT SUBSTRING(@StringToExecute, 34000, 40000);
                        END;
                    EXEC sp_executesql @StringToExecute, N'@Top INT, @min_duration INT, @min_back INT, @CheckDateOverride DATETIMEOFFSET, @MinimumExecutionCount INT', @Top, @DurationFilter_i, @MinutesBack, @CheckDateOverride, @MinimumExecutionCount;
				END;
		END;
	ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
		BEGIN
			RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
END; /* End of writing results to table */

END; /*Final End*/

GO
