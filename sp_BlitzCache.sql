IF OBJECT_ID('dbo.sp_BlitzCache') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzCache AS RETURN 0;')
GO

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##bou_BlitzCacheProcs', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##bou_BlitzCacheProcs;')
GO

IF OBJECT_ID('dbo.sp_BlitzCache') IS NOT NULL AND OBJECT_ID('tempdb.dbo.##bou_BlitzCacheResults', 'U') IS NOT NULL
    EXEC ('DROP TABLE ##bou_BlitzCacheResults;')
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
    QueryType nvarchar(256),
    DatabaseName sysname,
    AverageCPU decimal(38,4),
    AverageCPUPerMinute decimal(38,4),
    TotalCPU decimal(38,4),
    PercentCPUByType money,
    PercentCPU money,
    AverageDuration decimal(38,4),
    TotalDuration decimal(38,4),
    PercentDuration money,
    PercentDurationByType money,
    AverageReads bigint,
    TotalReads bigint,
    PercentReads money,
    PercentReadsByType money,
    ExecutionCount bigint,
    PercentExecutions money,
    PercentExecutionsByType money,
    ExecutionsPerMinute money,
    TotalWrites bigint,
    AverageWrites money,
    PercentWrites money,
    PercentWritesByType money,
    WritesPerMinute money,
    PlanCreationTime datetime,
    LastExecutionTime datetime,
    PlanHandle varbinary(64),
    SqlHandle varbinary(64),
    QueryHash binary(8),
    QueryPlanHash binary(8),
    StatementStartOffset int,
    StatementEndOffset int,
    MinReturnedRows bigint,
    MaxReturnedRows bigint,
    AverageReturnedRows money,
    TotalReturnedRows bigint,
    LastReturnedRows bigint,
    QueryText nvarchar(max),
    QueryPlan xml,
    /* these next four columns are the total for the type of query.
        don't actually use them for anything apart from math by type.
        */
    TotalWorkerTimeForType bigint,
    TotalElapsedTimeForType bigint,
    TotalReadsForType bigint,
    TotalExecutionCountForType bigint,
    TotalWritesForType bigint,
    NumberOfPlans int,
    NumberOfDistinctPlans int,
    SerialDesiredMemory float,
    SerialRequiredMemory float,
    CachedPlanSize float,
    CompileTime float,
    CompileCPU float ,
    CompileMemory float ,
    min_worker_time bigint,
    max_worker_time bigint,
    is_forced_plan bit,
    is_forced_parameterized bit,
    is_cursor bit,
    is_parallel bit,
    frequent_execution bit,
    parameter_sniffing bit,
    unparameterized_query bit,
    near_parallel bit,
    plan_warnings bit,
    plan_multiple_plans bit,
    long_running bit,
    downlevel_estimator bit,
    implicit_conversions bit,
    tempdb_spill bit,
    busy_loops bit,
    tvf_join bit,
    tvf_estimate bit,
    compile_timeout bit,
    compile_memory_limit_exceeded bit,
    warning_no_join_predicate bit,
    QueryPlanCost float,
    missing_index_count int,
    unmatched_index_count int,
    min_elapsed_time bigint,
    max_elapsed_time bigint,
    age_minutes money,
    age_minutes_lifetime money,
    is_trivial bit,
    SetOptions VARCHAR(MAX),
    Warnings VARCHAR(MAX)
);
GO

ALTER PROCEDURE dbo.sp_BlitzCache
    @get_help BIT = 0,
    @top INT = 10,
    @sort_order VARCHAR(50) = 'CPU',
    @use_triggers_anyway BIT = NULL,
    @export_to_excel BIT = 0,
    @results VARCHAR(10) = 'simple',
    @output_database_name NVARCHAR(128) = NULL ,
    @output_schema_name NVARCHAR(256) = NULL ,
    @output_table_name NVARCHAR(256) = NULL ,
    @configuration_database_name NVARCHAR(128) = NULL ,
    @configuration_schema_name NVARCHAR(256) = NULL ,
    @configuration_table_name NVARCHAR(256) = NULL ,
    @duration_filter DECIMAL(38,4) = NULL ,
    @hide_summary BIT = 0 ,
    @ignore_system_db BIT = 1 ,
    @only_query_hashes VARCHAR(MAX) = NULL ,
    @ignore_query_hashes VARCHAR(MAX) = NULL ,
    @sql_handle VARCHAR(MAX) = NULL ,
    @query_filter VARCHAR(10) = 'ALL' ,
    @database_name NVARCHAR(128) = NULL ,
    @reanalyze BIT = 0 ,
    @skip_analysis BIT = 0 ,
    @whole_cache BIT = 0 /* This will forcibly set @top to 2,147,483,647 */
WITH RECOMPILE
/******************************************
sp_BlitzCache (TM) 2016, Brent Ozar Unlimited.
(C) 2016, Brent Ozar Unlimited.
See http://BrentOzar.com/go/eula for the End User Licensing Agreement.



Description: Displays a server level view of the SQL Server plan cache.

Output: One result set is presented that contains data from the statement,
procedure, and trigger stats DMVs.

To learn more, visit http://brentozar.com/blitzcache/
where you can download new versions for free, watch training videos on
how it works, get more info on the findings, and more. To contribute
code and see your name in the change log, submit your improvements &
ideas to https://support.brentozar.com



KNOWN ISSUES:
- This query will not run on SQL Server 2005.
- SQL Server 2008 and 2008R2 have a bug in trigger stats (see below).
- @ignore_query_hashes and @only_query_hashes require a CSV list of hashes
  with no spaces between the hash values.

v2.5.1 - 2016-03-15
 - Nick Molyneux fixed an overflow error, and did an amazing job of it.

v2.5.0 - 2015-10-23
 - Now with errors when required values are set to NULL. Thanks to Raul
   Gonzalez for pointing this out.
 - changing default @top to 10
 - Added a @skip_analysis to avoid the XML processing overhead
 - Added QueryHash and QueryPlanHash to @export_to_excel and expert mode
 - Adding sort order for recent compiles.
 - Fixing potential INT overflow in totals temp table
 - Fixing slow sort performance on xpm and friends
 - Added compilation info (memory, CPU, time) and plan size to output
 - Re-structured XML processing for more better performance

v2.4.6 - 2015-06-18
 - temporary object cleanup will actually occur - thanks to Bob Klimes for
   spotting this
 - adding memory grants to expert mode and export to excel mode
 - parent object name is now displayed next to statements that come from a
   stored procedure
 - run clean up in ##bou_BlitzCacheProcs before executing - this should 
   prevent duplicate records from building up over multiple executions on 
   the same SPID.
 - added a @sql_handle parameter to filter on queries from a specific 
   sql_handle or sql_handles
 - added support for filtering on database name

v2.4.5 - 2015-04-27
 - sp_BlitzCache will no longer fail if @reanalyze = 1 AND sp_BlitzCache has
   never been run.
 - sp_BlitzCache can be run from multiple SPIDs.
 - Triggers will no longer cause sp_BlitzCache to notice itself.
 - Fixed an int overflow when determining execution time. Now using DATEDIFF
   on minutes of execution time instead of seconds. Queries that have used 
   more than 28,000 days of CPU are safe!

v2.4.4 - 2015-01-09
 - Fixed output to table. Sort order wasn't being obeyed and users limting
   results weren't seeing the same results between displaying to screen and
   saving results to a table.
   Thanks to Gail Jurey for spotting this!
 - Fixed an error where running with reanalyze after export_to_excel would
   prevent a summary from being generated.
 - Added query plan cost to export_to_excel output.
 - Cleaned up export_to_excel output to match column order to screen display.

v2.4.3 - 2014-11-11
 - Fix to remove confusing implicit conversion checks. Warnings will only be
   generated when a plan affecting convert is in place.

v2.4.2 - 2014-11-04
 - Hotfix - Randall Petty found a stray comma in the export_to_excel output.

v2.4.1 - 2014-10-31
 - Hotfix - Denis Gobo pointed out that the global temp table names could
   conflict with everyone else's global temp table rates.

v2.4 - 2014-10-31
 - Fixed a logical error in output table detection - thanks to Michael
   Bluett for pointing that out.
 - Fixed a bug where sorting on average executions broke the query.
   Thanks to Andrew Notarian and Calvin Jones for submitting this.
 - Added @query_filter to allow output restrictions to only procedures
   or individual statements
 - Adds a check for trivial execution plans.
 - Adds @reanalyze. When set to 1, this re-scans existing results rather
   than running all of the logic again. Bonus: contains GOTO.
 - Now displaying set options!

v2.3 - 2014-06-07
 - Added opserver specific output
 - Adding a `@only_query_hashes` parameter to limit results to a select set of
   query hashes.
 - Adding a `@ignore_query_hashes` parameter to exclude specific queries from
   analysis.

v2.2 - 2014-05-20
 - Added sorting on averages
 - Added configuration table parameters. Includes help messages for the
   allowed parameters and default values.
 - Missing index warning now displays the number of missing indexes.
 - Changing display to milliseconds instead of microseconds.
 - Adding a flag to ignore system databases. This is on by default.
 - Correcting a typo found by Michael Zilberstein. Thanks!
 - Fixing an XML bug for implicit conversion detection - contributed by Michael Zilberstein.
 - Added a check for unparameterized queries.

v2.1 - 2014-04-30
 - Added @duration_filter. Queries are now filtered during collection based on duration.
 - Added results summary table and hide_summary parameter.
 - Added check for > 1000 executions per minute.
 - Added check for queries with missing indexes.
 - Added check for queries with warnings in the execution plan.
 - Added check for queries using cursors.
 - Query cost will be displayed next to the execution plan for a query.
 - Added a check for plan guides and forced plans.
 - An asterisk will be displayed next to the name of queries that have gone parallel.
 - Added a check for parallel plans.
 - Added @results parameter - options are 'narrow', 'simple', and 'expert'
 - Added a check for plans using a downlevel cardinality estimator
 - Added checks for plans with implicit conversions or plan affecting convert warnings
 - Added check for queries with spill warnings
 - Consolidated warning detection into a smaller number of T-SQL statements
 - Added a Warnings column
 - Added "busy loops" check
 - Fixed bug where long-running query threshold was 300 microseconds, not seconds

v2.0 - 2014-03-23
 - Created a stored procedure
 - Added write information
 - Added option to export to a single table
 - Corrected accidental exclusion of trigger information

v1.4 - 2014-02-17
 - MOAR BUG FIXES
 - Corrected multiple sorting bugs that cause confusing displays of query
   results that weren't necessarily the top anything.
 - Updated all modification timestamps to use ISO 8601 formatting because it's
   correct, sorry Britain.
 - Added a check for SQL Server 2008R2 build greater than SP1.
   Thanks to Kevan Riley for spotting this.
 - Added the stored procedure or trigger name to the Query Type column.
   Initial suggestion from Kevan Riley.
 - Corrected erronous math that could allow for % CPU/Duration/Executions/Reads
   being higher than 100% for batches/procedures with multiple poorly
   performing statements in them.

v1.3 - 2014-02-06
 - As they say on the app store, "Bug fixes"
 - Reorganized this to put the standard, gotta-run stuff at the top.
 - Switched to YYYY/MM/DD because Brits.

v1.2 - 2014-02-04
- Removed debug code
- Fixed output where SQL Server 2008 and early don't support min_rows,
  max_rows, and total_rows.
  SQL Server 2008 and earlier will now return NULL for those columns.

v1.1 - 2014-02-02
- Incorporated sys.dm_exec_plan_attributes as recommended by Andrey
  and Michael J. Swart.
- Added additional detail columns for plan cache analysis including
  min/max rows, total rows.
- Streamlined collection of data.



*******************************************/
AS
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
/* VERSION! */
RAISERROR (N'Executing sp_BlitzCache v2.5.1', 0, 1) WITH NOWAIT ;

DECLARE @nl nvarchar(2) = NCHAR(13) + NCHAR(10) ;

IF @get_help = 1
BEGIN
    SELECT N'@get_help' AS [Parameter Name] ,
           N'BIT' AS [Data Type] ,
           N'Displays this help message.' AS [Parameter Description]

    UNION ALL
    SELECT N'@top',
           N'INT',
           N'The number of records to retrieve and analyze from the plan cache. The following DMVs are used as the plan cache: dm_exec_query_stats, dm_exec_procedure_stats, dm_exec_trigger_stats.'

    UNION ALL
    SELECT N'@sort_order',
           N'VARCHAR(10)',
           N'Data processing and display order. @sort_order will still be used, even when preparing output for a table or for excel. Possible values are: "CPU", "Reads", "Writes", "Duration", "Executions", "Recent Compilations". Additionally, the word "Average" or "Avg" can be used to sort on averages rather than total. "Executions per minute" and "Executions / minute" can be used to sort by execution per minute. For the truly lazy, "xpm" can also be used.'

    UNION ALL
    SELECT N'@use_triggers_anyway',
           N'BIT',
           N'On SQL Server 2008R2 and earlier, trigger execution count is incorrect - trigger execution count is incremented once per execution of a SQL agent job. If you still want to see relative execution count of triggers, then you can force sp_BlitzCache to include this information.'

    UNION ALL
    SELECT N'@export_to_excel',
           N'BIT',
           N'Prepare output for exporting to Excel. Newlines and additional whitespace are removed from query text and the execution plan is not displayed.'

    UNION ALL
    SELECT N'@results',
           N'VARCHAR(10)',
           N'Results mode. Options are "Narrow", "Simple", or "Expert". This determines which columns will be displayed in the analysis of the plan cache.'

    UNION ALL
    SELECT N'@output_database_name',
           N'NVARCHAR(128)',
           N'The output database. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@output_schema_name',
           N'NVARCHAR(256)',
           N'The output schema. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@output_table_name',
           N'NVARCHAR(256)',
           N'The output table. If this does not exist, it will be created for you.'

    UNION ALL
    SELECT N'@duration_filter',
           N'DECIMAL(38,4)',
           N'Excludes queries with an average duration (in seconds) less than @duration_filter.'

    UNION ALL
    SELECT N'@hide_summary',
           N'BIT',
           N'Hides the findings summary result set.'

    UNION ALL
    SELECT N'@ignore_system_db',
           N'BIT',
           N'Ignores plans found in the system databases (master, model, msdb, tempdb, and resourcedb)'

    UNION ALL
    SELECT N'@only_query_hashes',
           N'VARCHAR(MAX)',
           N'A list of query hashes to query. All other query hashes will be ignored. Stored procedures and triggers will be ignored.'

    UNION ALL
    SELECT N'@ignore_query_hashes',
           N'VARCHAR(MAX)',
           N'A list of query hashes to ignore.'
    
    UNION ALL
    SELECT N'@sql_handle',
           N'VARCHAR(MAX)',
           N'One or more sql_handles to use for filtering results.'

    UNION ALL
    SELECT N'@database_name',
           N'NVARCHAR(128)',
           N'A database name which is used for filtering results.'

    UNION ALL
    SELECT N'@whole_cache',
           N'BIT',
           N'This forces sp_BlitzCache to examine the entire plan cache. Be careful running this on servers with a lot of memory or a large execution plan cache.'

    UNION ALL
    SELECT N'@query_filter',
           N'VARCHAR(10)',
           N'Filter out stored procedures or statements. The default value is ''ALL''. Allowed values are ''procedures'', ''statements'', or ''all'' (any variation in capitalization is acceptable).'

    UNION ALL
    SELECT N'@reanalyze',
           N'BIT',
           N'The default is 0. When set to 0, sp_BlitzCache will re-evalute the plan cache. Set this to 1 to reanalyze existing results';
           


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
           N'NVARCHAR(256)',
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

    RETURN
END

/* validate user inputs */
IF @top IS NULL 
    OR @sort_order IS NULL 
    OR @query_filter IS NULL 
    OR @reanalyze IS NULL
BEGIN
    RAISERROR(N'Several parameters (@top, @sort_order, @query_filter, @renalyze) are required. Do not set them to NULL. Please try again.', 16, 1) WITH NOWAIT;
    RETURN;
END

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
END

IF OBJECT_ID('tempdb.dbo.##bou_BlitzCacheProcs') IS NULL
BEGIN
    CREATE TABLE ##bou_BlitzCacheProcs (
        SPID INT ,
        QueryType nvarchar(256),
        DatabaseName sysname,
        AverageCPU decimal(38,4),
        AverageCPUPerMinute decimal(38,4),
        TotalCPU decimal(38,4),
        PercentCPUByType money,
        PercentCPU money,
        AverageDuration decimal(38,4),
        TotalDuration decimal(38,4),
        PercentDuration money,
        PercentDurationByType money,
        AverageReads bigint,
        TotalReads bigint,
        PercentReads money,
        PercentReadsByType money,
        ExecutionCount bigint,
        PercentExecutions money,
        PercentExecutionsByType money,
        ExecutionsPerMinute money,
        TotalWrites bigint,
        AverageWrites money,
        PercentWrites money,
        PercentWritesByType money,
        WritesPerMinute money,
        PlanCreationTime datetime,
        LastExecutionTime datetime,
        PlanHandle varbinary(64),
        SqlHandle varbinary(64),
        QueryHash binary(8),
        QueryPlanHash binary(8),
        StatementStartOffset int,
        StatementEndOffset int,
        MinReturnedRows bigint,
        MaxReturnedRows bigint,
        AverageReturnedRows money,
        TotalReturnedRows bigint,
        LastReturnedRows bigint,
        QueryText nvarchar(max),
        QueryPlan xml,
        /* these next four columns are the total for the type of query.
            don't actually use them for anything apart from math by type.
            */
        TotalWorkerTimeForType bigint,
        TotalElapsedTimeForType bigint,
        TotalReadsForType bigint,
        TotalExecutionCountForType bigint,
        TotalWritesForType bigint,
        NumberOfPlans int,
        NumberOfDistinctPlans int,
        SerialDesiredMemory float,
        SerialRequiredMemory float,
        CachedPlanSize float,
        CompileTime float,
        CompileCPU float ,
        CompileMemory float ,
        min_worker_time bigint,
        max_worker_time bigint,
        is_forced_plan bit,
        is_forced_parameterized bit,
        is_cursor bit,
        is_parallel bit,
        frequent_execution bit,
        parameter_sniffing bit,
        unparameterized_query bit,
        near_parallel bit,
        plan_warnings bit,
        plan_multiple_plans bit,
        long_running bit,
        downlevel_estimator bit,
        implicit_conversions bit,
        tempdb_spill bit,
        busy_loops bit,
        tvf_join bit,
        tvf_estimate bit,
        compile_timeout bit,
        compile_memory_limit_exceeded bit,
        warning_no_join_predicate bit,
        QueryPlanCost float,
        missing_index_count int,
        unmatched_index_count int,
        min_elapsed_time bigint,
        max_elapsed_time bigint,
        age_minutes money,
        age_minutes_lifetime money,
        is_trivial bit,
        SetOptions VARCHAR(MAX),
        Warnings VARCHAR(MAX)
    );
END

DECLARE @duration_filter_i INT,
        @msg NVARCHAR(4000) ;

RAISERROR (N'Setting up temporary tables for sp_BlitzCache',0,1) WITH NOWAIT;

/* Change duration from seconds to milliseconds */
IF @duration_filter IS NOT NULL
  SET @duration_filter_i = CAST((@duration_filter * 1000.0) AS INT)

SET @database_name = LTRIM(RTRIM(@database_name)) ;

SET @sort_order = LOWER(@sort_order);
SET @sort_order = REPLACE(REPLACE(@sort_order, 'average', 'avg'), '.', '');
SET @sort_order = REPLACE(@sort_order, 'executions per minute', 'avg executions');
SET @sort_order = REPLACE(@sort_order, 'executions / minute', 'avg executions');
SET @sort_order = REPLACE(@sort_order, 'xpm', 'avg executions');
SET @sort_order = REPLACE(@sort_order, 'recent compilations', 'compiles');



IF @sort_order NOT IN ('cpu', 'avg cpu', 'reads', 'avg reads', 'writes', 'avg writes',
                       'duration', 'avg duration', 'executions', 'avg executions',
                       'compiles')
  SET @sort_order = 'cpu';

SELECT @output_database_name = QUOTENAME(@output_database_name),
       @output_schema_name   = QUOTENAME(@output_schema_name),
       @output_table_name    = QUOTENAME(@output_table_name);

SET @query_filter = LOWER(@query_filter);

IF LEFT(@query_filter, 3) NOT IN ('all', 'sta', 'pro')
  SET @query_filter = 'all';

IF @reanalyze = 1 AND OBJECT_ID('tempdb..##bou_BlitzCacheResults') IS NULL
  SET @reanalyze = 0;

if @skip_analysis = 1
    SET @hide_summary = 1;

IF @reanalyze = 1 
    GOTO Results

DELETE FROM ##bou_BlitzCacheProcs
WHERE  SPID = @@SPID ;

IF OBJECT_ID('tempdb..#only_query_hashes') IS NOT NULL
    DROP TABLE #only_query_hashes ;

IF OBJECT_ID('tempdb..#ignore_query_hashes') IS NOT NULL
    DROP TABLE #ignore_query_hashes ;

IF OBJECT_ID('tempdb..#only_sql_handles') IS NOT NULL
    DROP TABLE #only_sql_handles ;
   
IF OBJECT_ID('tempdb..#p') IS NOT NULL
    DROP TABLE #p;

IF OBJECT_ID ('tempdb..#checkversion') IS NOT NULL
    DROP TABLE #checkversion;

IF OBJECT_ID ('tempdb..#configuration') IS NOT NULL
    DROP TABLE #configuration;

CREATE TABLE #only_query_hashes (
    query_hash BINARY(8)
);

CREATE TABLE #ignore_query_hashes (
    query_hash BINARY(8)
);

CREATE TABLE #only_sql_handles (
    sql_handle VARBINARY(64)
);

CREATE TABLE #p (
    SqlHandle varbinary(64),
    TotalCPU bigint,
    TotalDuration bigint,
    TotalReads bigint,
    TotalWrites bigint,
    ExecutionCount bigint
);

CREATE TABLE #checkversion (
    version nvarchar(128),
    common_version AS SUBSTRING(version, 1, CHARINDEX('.', version) + 1 ),
    major AS PARSENAME(CONVERT(VARCHAR(32), version), 4),
    minor AS PARSENAME(CONVERT(VARCHAR(32), version), 3),
    build AS PARSENAME(CONVERT(varchar(32), version), 2),
    revision AS PARSENAME(CONVERT(VARCHAR(32), version), 1)
);

CREATE TABLE #configuration (
    parameter_name VARCHAR(100),
    value DECIMAL(38,0)
);



SET @sql_handle = LTRIM(RTRIM(@sql_handle)) ;
SET @only_query_hashes = LTRIM(RTRIM(@only_query_hashes)) ;
SET @ignore_query_hashes = LTRIM(RTRIM(@ignore_query_hashes)) ;

DECLARE @individual VARCHAR(100) ;

IF @sql_handle IS NOT NULL
    AND LEN(@sql_handle) > 0
BEGIN
    SET @individual = '';

    WHILE LEN(@sql_handle) > 0
    BEGIN
        IF PATINDEX('%,%', @sql_handle) > 0
        BEGIN  
               SET @individual = SUBSTRING(@sql_handle, 0, PATINDEX('%,%',@sql_handle)) ;
               
               INSERT INTO #only_sql_handles
               select cast('' as xml).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               from (select case substring(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
               
               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

               SET @sql_handle = SUBSTRING(@sql_handle, LEN(@individual + ',') + 1, LEN(@sql_handle)) ;
        END
        ELSE
        BEGIN
               SET @individual = @sql_handle
               SET @sql_handle = NULL

               INSERT INTO #only_sql_handles
               select cast('' as xml).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               from (select case substring(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)

               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
        END
    END
END    

IF ((@only_query_hashes IS NOT NULL AND LEN(@only_query_hashes) > 0)
    OR (@ignore_query_hashes IS NOT NULL AND LEN(@ignore_query_hashes) > 0))
   AND LEFT(@query_filter, 3) = 'pro'
BEGIN
   RAISERROR('You cannot limit by query hash and filter by stored procedure', 16, 1);
   RETURN;
END

/* If the user is attempting to limit by query hash, set up the
   #only_query_hashes temp table. This will be used to narrow down
   results.

   Just a reminder: Using @only_query_hashes will ignore stored
   procedures and triggers.
 */
IF @only_query_hashes IS NOT NULL
   AND LEN(@only_query_hashes) > 0
BEGIN
    SET @individual = '';

   WHILE LEN(@only_query_hashes) > 0
   BEGIN
        IF PATINDEX('%,%', @only_query_hashes) > 0
        BEGIN  
               SET @individual = SUBSTRING(@only_query_hashes, 0, PATINDEX('%,%',@only_query_hashes)) ;
               
               INSERT INTO #only_query_hashes
               select cast('' as xml).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               from (select case substring(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
               
               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

               SET @only_query_hashes = SUBSTRING(@only_query_hashes, LEN(@individual + ',') + 1, LEN(@only_query_hashes)) ;
        END
        ELSE
        BEGIN
               SET @individual = @only_query_hashes
               SET @only_query_hashes = NULL

               INSERT INTO #only_query_hashes
               select cast('' as xml).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               from (select case substring(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)

               --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
        END
   END
END

/* If the user is setting up a list of query hashes to ignore, those
   values will be inserted into #ignore_query_hashes. This is used to
   exclude values from query results.

   Stored procedures and triggers will still be queried.
 */
IF @ignore_query_hashes IS NOT NULL
   AND LEN(@ignore_query_hashes) > 0
BEGIN
   SET @individual = '' ;

   WHILE LEN(@ignore_query_hashes) > 0
   BEGIN
        IF PATINDEX('%,%', @ignore_query_hashes) > 0
        BEGIN  
               SET @individual = SUBSTRING(@ignore_query_hashes, 0, PATINDEX('%,%',@ignore_query_hashes)) ;
               
               INSERT INTO #ignore_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos) ;
               
               SET @ignore_query_hashes = SUBSTRING(@ignore_query_hashes, LEN(@individual + ',') + 1, LEN(@ignore_query_hashes)) ;
        END
        ELSE
        BEGIN
               SET @individual = @ignore_query_hashes ;
               SET @ignore_query_hashes = NULL ;

               INSERT INTO #ignore_query_hashes
               SELECT CAST('' AS XML).value('xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )', 'varbinary(max)')
               FROM (SELECT CASE SUBSTRING(@individual, 1, 2) WHEN '0x' THEN 3 ELSE 0 END) AS t(pos) ;
        END
   END
END

IF @configuration_database_name IS NOT NULL
BEGIN
   DECLARE @config_sql NVARCHAR(MAX) = N'INSERT INTO #configuration SELECT parameter_name, value FROM '
        + QUOTENAME(@configuration_database_name)
        + '.' + QUOTENAME(@configuration_schema_name)
        + '.' + QUOTENAME(@configuration_table_name)
        + ' ; ' ;
   EXEC(@config_sql);
END

DECLARE @sql nvarchar(MAX) = N'',
        @insert_list nvarchar(MAX) = N'',
        @plans_triggers_select_list nvarchar(MAX) = N'',
        @body nvarchar(MAX) = N'',
        @body_where nvarchar(MAX) = N'WHERE 1 = 1 ' + @nl,
        @body_order nvarchar(MAX) = N'ORDER BY #sortable# DESC OPTION (RECOMPILE) ',
        
        @q nvarchar(1) = N'''',
        @pv varchar(20),
        @pos tinyint,
        @v decimal(6,2),
        @build int;


RAISERROR (N'Determining SQL Server version.',0,1) WITH NOWAIT;

INSERT INTO #checkversion (version)
SELECT CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))
OPTION (RECOMPILE);


SELECT @v = common_version ,
       @build = build
FROM   #checkversion
OPTION (RECOMPILE);

RAISERROR (N'Creating dynamic SQL based on SQL Server version.',0,1) WITH NOWAIT;

SET @insert_list += N'
INSERT INTO ##bou_BlitzCacheProcs (SPID, QueryType, DatabaseName, AverageCPU, TotalCPU, AverageCPUPerMinute, PercentCPUByType, PercentDurationByType,
                    PercentReadsByType, PercentExecutionsByType, AverageDuration, TotalDuration, AverageReads, TotalReads, ExecutionCount,
                    ExecutionsPerMinute, TotalWrites, AverageWrites, PercentWritesByType, WritesPerMinute, PlanCreationTime,
                    LastExecutionTime, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows,
                    LastReturnedRows, QueryText, QueryPlan, TotalWorkerTimeForType, TotalElapsedTimeForType, TotalReadsForType,
                    TotalExecutionCountForType, TotalWritesForType, SqlHandle, PlanHandle, QueryHash, QueryPlanHash,
                    min_worker_time, max_worker_time, is_parallel, min_elapsed_time, max_elapsed_time, age_minutes, age_minutes_lifetime) ' ;

SET @body += N'
FROM   (SELECT TOP (@top) *,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE()) 
                          ELSE NULL END) as MONEY) as age_minutes,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time) 
                          ELSE Null END) as MONEY) as age_minutes_lifetime
        FROM   sys.#view# x
               CROSS APPLY (SELECT * FROM sys.dm_exec_plan_attributes(x.plan_handle) AS ixpa 
                            WHERE ixpa.attribute = ''dbid'') AS xpa ' + @nl ;

/* filtering for query hashes */
IF (SELECT COUNT(*) FROM #ignore_query_hashes) > 0
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0
BEGIN
    SET @body += N'               LEFT JOIN #ignore_query_hashes iqh ON iqh.query_hash = qs.query_hash ' + @nl ;
END

IF (SELECT COUNT(*) FROM #ignore_query_hashes) > 0
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0
BEGIN
    SET @body += N'                         AND iqh.query_hash IS NULL ' + @nl ;
END
/* end filtering for query hashes */



SET @body += N'        WHERE  1 = 1 ' +  @nl ;

IF @ignore_system_db = 1
    SET @body += N'               AND COALESCE(DB_NAME(CAST(xpa.value AS BIGINT)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') ' + @nl ;

IF @database_name IS NOT NULL OR @database_name <> ''
    SET @body += N'               AND CAST(xpa.value AS BIGINT) = DB_ID('
                 + QUOTENAME(@database_name, N'''')
                 + N') ' + @nl;

IF (SELECT COUNT(*) FROM #only_sql_handles) > 0
BEGIN
    SET @body += N'               AND EXISTS(SELECT 1/0 FROM #only_sql_handles q WHERE q.sql_handle = x.sql_handle) ' + @nl ;
END      

IF (SELECT COUNT(*) FROM #only_query_hashes) > 0
   AND (SELECT COUNT(*) FROM #ignore_query_hashes) = 0
   AND (SELECT COUNT(*) FROM #only_sql_handles) = 0
BEGIN
    SET @body += N'               AND EXISTS(SELECT 1/0 FROM #only_query_hashes q WHERE q.query_hash = x.query_hash) ' + @nl ;
END

IF @duration_filter IS NOT NULL
    SET @body += N'       AND (total_elapsed_time / 1000.0) / execution_count > @min_duration ' + @nl ;



/* Apply the sort order here to only grab relevant plans.
   This should make it faster to process since we'll be pulling back fewer
   plans for processing.
 */
SELECT @body += '        ORDER BY ' +
                CASE @sort_order WHEN 'cpu' THEN 'total_worker_time'
                                 WHEN 'reads' THEN 'total_logical_reads'
                                 WHEN 'writes' THEN 'total_logical_writes'
                                 WHEN 'duration' THEN 'total_elapsed_time'
                                 WHEN 'executions' THEN 'execution_count'
                                 WHEN 'compiles' THEN 'cached_time'
                                 /* And now the averages */
                                 WHEN 'avg cpu' THEN 'total_worker_time / execution_count'
                                 WHEN 'avg reads' THEN 'total_logical_reads / execution_count'
                                 WHEN 'avg writes' THEN 'total_logical_writes / execution_count'
                                 WHEN 'avg duration' THEN 'total_elapsed_time / execution_count'
                                 WHEN 'avg executions' THEN 'CASE WHEN execution_count = 0 THEN 0
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
                END + ' DESC ' + @nl ;


                          
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

SET @body_where += N'       AND pa.attribute = ' + QUOTENAME('dbid', @q) + @nl ;



SET @plans_triggers_select_list += N'
SELECT TOP (@top)
       @@SPID ,
       ''Procedure: '' + COALESCE(OBJECT_NAME(qs.object_id, qs.database_id),'''') AS QueryType,
       COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), ''-- N/A --'') AS DatabaseName,
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
       st.text AS QueryText ,
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
       age_minutes_lifetime '


IF LEFT(@query_filter, 3) IN ('all', 'sta')
BEGIN
    SET @sql += @insert_list;
    
    SET @sql += N'
    SELECT TOP (@top)
           @@SPID ,
           ''Statement'' AS QueryType,
           COALESCE(DB_NAME(CAST(pa.value AS INT)), ''-- N/A --'') AS DatabaseName,
           (total_worker_time / 1000.0) / execution_count AS AvgCPU ,
           (total_worker_time / 1000.0) AS TotalCPU ,
           CASE WHEN total_worker_time = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((total_worker_time / 1000.0) / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time)) AS MONEY)
                END AS AverageCPUPerMinute ,
           CAST(ROUND(100.00 * (total_worker_time / 1000.0) / t.t_TotalWorker, 2) AS MONEY) AS PercentCPUByType,
           CAST(ROUND(100.00 * (total_elapsed_time / 1000.0) / t.t_TotalElapsed, 2) AS MONEY) AS PercentDurationByType,
           CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY) AS PercentReadsByType,
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
           qs.statement_end_offset AS StatementEndOffset, '
    
    IF (@v >= 11) OR (@v >= 10.5 AND @build >= 2500)
    BEGIN
        SET @sql += N'
           qs.min_rows AS MinReturnedRows,
           qs.max_rows AS MaxReturnedRows,
           CAST(qs.total_rows as MONEY) / execution_count AS AvgReturnedRows,
           qs.total_rows AS TotalReturnedRows,
           qs.last_rows AS LastReturnedRows, ' ;
    END
    ELSE
    BEGIN
        SET @sql += N'
           NULL AS MinReturnedRows,
           NULL AS MaxReturnedRows,
           NULL AS AvgReturnedRows,
           NULL AS TotalReturnedRows,
           NULL AS LastReturnedRows, ' ;
    END
    
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
           NULL AS PlanHandle,
           qs.query_hash AS QueryHash,
           qs.query_plan_hash AS QueryPlanHash,
           qs.min_worker_time / 1000.0,
           qs.max_worker_time / 1000.0,
           CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN 1 ELSE 0 END,
           qs.min_elapsed_time / 1000.0,
           qs.max_worker_time  / 1000.0,
           age_minutes,
           age_minutes_lifetime '
    
    SET @sql += REPLACE(REPLACE(@body, '#view#', 'dm_exec_query_stats'), 'cached_time', 'creation_time') ;
    
    SET @sql += REPLACE(@body_where, 'cached_time', 'creation_time') ;
    
    SET @sql += @body_order + @nl + @nl + @nl;

    IF @sort_order = 'compiles'
    BEGIN
        SET @sql = REPLACE(@sql, '#sortable#', 'creation_time');
    END
END


IF (@query_filter = 'all' AND (SELECT COUNT(*) FROM #only_query_hashes) = 0)
   OR (LEFT(@query_filter, 3) = 'pro')
BEGIN
    SET @sql += @insert_list;
    SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Stored Procedure') ;

    SET @sql += REPLACE(@body, '#view#', 'dm_exec_procedure_stats') ; 
    SET @sql += @body_where ;

    IF @ignore_system_db = 1
       SET @sql += ' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') ' + @nl ;

    SET @sql += @body_order + @nl + @nl + @nl ;
END



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
IF (@use_triggers_anyway = 1 OR @v >= 11)
   AND (SELECT COUNT(*) FROM #only_query_hashes) = 0
   AND (@query_filter = 'all')
BEGIN
   RAISERROR (N'Adding SQL to collect trigger stats.',0,1) WITH NOWAIT;

   /* Trigger level information from the plan cache */
   SET @sql += @insert_list ;

   SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Trigger') ;

   SET @sql += REPLACE(@body, '#view#', 'dm_exec_trigger_stats') ;

   SET @sql += @body_where ;

   IF @ignore_system_db = 1
      SET @sql += ' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') ' + @nl ;
   
   SET @sql += @body_order + @nl + @nl + @nl ;
END



DECLARE @sort NVARCHAR(MAX);

SELECT @sort = CASE @sort_order WHEN 'cpu' THEN 'total_worker_time'
                                WHEN 'reads' THEN 'total_logical_reads'
                                WHEN 'writes' THEN 'total_logical_writes'
                                WHEN 'duration' THEN 'total_elapsed_time'
                                WHEN 'executions' THEN 'execution_count'
                                WHEN 'compiles' THEN 'cached_time'
                                /* And now the averages */
                                WHEN 'avg cpu' THEN 'total_worker_time / execution_count'
                                WHEN 'avg reads' THEN 'total_logical_reads / execution_count'
                                WHEN 'avg writes' THEN 'total_logical_writes / execution_count'
                                WHEN 'avg duration' THEN 'total_elapsed_time / execution_count'
                                WHEN 'avg executions' THEN 'CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
               END ;

SELECT @sql = REPLACE(@sql, '#sortable#', @sort);

SET @sql += N'
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
         FROM    ##bou_BlitzCacheProcs) AS x
WHERE x.rn = 1
OPTION (RECOMPILE);
';

SELECT @sort = CASE @sort_order WHEN 'cpu' THEN 'TotalCPU'
                                WHEN 'reads' THEN 'TotalReads'
                                WHEN 'writes' THEN 'TotalWrites'
                                WHEN 'duration' THEN 'TotalDuration'
                                WHEN 'executions' THEN 'ExecutionCount'
                                WHEN 'compiles' THEN 'PlanCreationTime'
                                WHEN 'avg cpu' THEN 'TotalCPU / ExecutionCount'
                                WHEN 'avg reads' THEN 'TotalReads / ExecutionCount'
                                WHEN 'avg writes' THEN 'TotalWrites / ExecutionCount'
                                WHEN 'avg duration' THEN 'TotalDuration / ExecutionCount'
                                WHEN 'avg executions' THEN 'CASE WHEN ExecutionCount = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * ExecutionCount / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
               END ;

SELECT @sql = REPLACE(@sql, '#sortable#', @sort);

IF @reanalyze = 0
BEGIN
    RAISERROR('Collecting execution plan information.', 0, 1) WITH NOWAIT;

    EXEC sp_executesql @sql, N'@top INT, @min_duration INT', @top, @duration_filter_i;
END



/* Compute the total CPU, etc across our active set of the plan cache.
 * Yes, there's a flaw - this doesn't include anything outside of our @top
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
                ELSE CAST((1.00 * ExecutionCount / DATEDIFF(mi, PlanCreationTime, LastExecutionTime)) AS money)
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
OPTION (RECOMPILE) ;



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
                ELSE CAST((1.00 * ExecutionCount / DATEDIFF(mi, PlanCreationTime, LastExecutionTime)) AS money)
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
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
        q.n.query('.') AS statement
INTO    #statements
FROM    ##bou_BlitzCacheProcs p
        CROSS APPLY p.QueryPlan.nodes('//p:StmtSimple') AS q(n) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
        q.n.query('.') AS query_plan
INTO    #query_plan
FROM    #statements p
        CROSS APPLY p.statement.nodes('//p:QueryPlan') AS q(n) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
SELECT  QueryHash ,
        SqlHandle ,
        q.n.query('.') AS relop
INTO    #relop
FROM    #query_plan p
        CROSS APPLY p.query_plan.nodes('//p:RelOp') AS q(n) ;



-- high level plan stuff
UPDATE  ##bou_BlitzCacheProcs
SET     NumberOfDistinctPlans = distinct_plan_count,
        NumberOfPlans = number_of_plans ,
        plan_multiple_plans = CASE WHEN distinct_plan_count < number_of_plans THEN 1 END
FROM (
        SELECT  COUNT(DISTINCT QueryHash) AS distinct_plan_count,
                COUNT(QueryHash) AS number_of_plans,
                QueryHash
        FROM    ##bou_BlitzCacheProcs
        GROUP BY QueryHash
) AS x
WHERE ##bou_BlitzCacheProcs.QueryHash = x.QueryHash
OPTION (RECOMPILE) ;

-- statement level checks
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET     QueryPlanCost = CASE WHEN QueryType LIKE '%Stored Procedure%' THEN
                                statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float')
                             ELSE
                                statement.value('sum(/p:StmtSimple[xs:hexBinary(substring(@QueryPlanHash, 3)) = xs:hexBinary(sql:column("QueryPlanHash"))]/@StatementSubTreeCost)', 'float')
                        END ,
        compile_timeout = CASE WHEN statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1 THEN 1 END ,
        compile_memory_limit_exceeded = CASE WHEN statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1 THEN 1 END ,
        unmatched_index_count = statement.value('count(//p:UnmatchedIndexes/Parameterization/Object)', 'int') ,
        is_trivial = CASE WHEN statement.exist('/p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1 THEN 1 END ,
        unparameterized_query = CASE WHEN statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 1 AND
                                          statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList/p:ColumnReference') = 0 THEN 1
                                     WHEN statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 0 AND
                                          statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/*/p:RelOp/descendant::p:ScalarOperator/p:Identifier/p:ColumnReference[contains(@Column, "@")]') = 1 THEN 1
                                END
FROM    #statements s
WHERE   s.QueryHash = ##bou_BlitzCacheProcs.QueryHash
OPTION (RECOMPILE);

-- query level checks
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  ##bou_BlitzCacheProcs
SET     missing_index_count = query_plan.value('count(/p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int') ,
        SerialDesiredMemory = query_plan.value('sum(/p:QueryPlan/p:MemoryGrantInfo/@SerialDesiredMemory)', 'float') ,
        SerialRequiredMemory = query_plan.value('sum(/p:QueryPlan/p:MemoryGrantInfo/@SerialRequiredMemory)', 'float'),
        CachedPlanSize = query_plan.value('sum(/p:QueryPlan/@CachedPlanSize)', 'float') ,
        CompileTime = query_plan.value('sum(/p:QueryPlan/@CompileTime)', 'float') ,
        CompileCPU = query_plan.value('sum(/p:QueryPlan/@CompileCPU)', 'float') ,
        CompileMemory = query_plan.value('sum(/p:QueryPlan/@CompileMemory)', 'float') ,
        implicit_conversions = CASE WHEN QueryPlan.exist('/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') = 1 THEN 1 END ,
        plan_warnings = CASE WHEN QueryPlan.value('count(/p:QueryPlan/p:Warnings)', 'int') > 0 THEN 1 END
FROM    #query_plan qp
WHERE   qp.QueryHash = ##bou_BlitzCacheProcs.QueryHash
OPTION (RECOMPILE);

-- operator level checks
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE p
SET    busy_loops = CASE WHEN (x.estimated_executions / 100.0) > x.estimated_rows THEN 1 END ,
       tvf_join = CASE WHEN x.tvf_join = 1 THEN 1 END ,
       warning_no_join_predicate = CASE WHEN x.no_join_warning = 1 THEN 1 END
FROM   ##bou_BlitzCacheProcs p
       JOIN (
            SELECT qs.SqlHandle,
                   relop.value('sum(/p:RelOp/@EstimateRows)', 'float') AS estimated_rows ,
                   relop.value('sum(/p:RelOp/@EstimateRewinds)', 'float') + relop.value('sum(/p:RelOp/@EstimateRebinds)', 'float') + 1.0 AS estimated_executions ,
                   relop.exist('/p:RelOp[contains(@LogicalOp, "Join")]/*/p:RelOp[(@LogicalOp[.="Table-valued function"])]') AS tvf_join,
                   relop.exist('/p:RelOp/p:Warnings[(@NoJoinPredicate[.="1"])]') AS no_join_warning
            FROM   #relop qs
       ) AS x ON p.SqlHandle = x.SqlHandle
OPTION (RECOMPILE);

IF @v >= 12
BEGIN
    RAISERROR('Checking for downlevel cardinality estimators being used on SQL Server 2014.', 0, 1) WITH NOWAIT;

    WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
    UPDATE  p
    SET     downlevel_estimator = CASE WHEN statement.value('min(//p:StmtSimple/@CardinalityEstimationModelVersion)', 'int') < (@v * 10) THEN 1 END
    FROM    ##bou_BlitzCacheProcs p
            JOIN #statements s ON p.QueryHash = s.QueryHash ;
END ;

/* END Testing using XML nodes to speed up processing */

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET NumberOfDistinctPlans = distinct_plan_count,
    NumberOfPlans = number_of_plans,
    QueryPlanCost = CASE WHEN QueryType LIKE '%Stored Procedure%' THEN
        QueryPlan.value('sum(//p:StmtSimple/@StatementSubTreeCost)', 'float')
        ELSE
        QueryPlan.value('sum(//p:StmtSimple[xs:hexBinary(substring(@QueryPlanHash, 3)) = xs:hexBinary(sql:column("QueryPlanHash"))]/@StatementSubTreeCost)', 'float')
        END,
    missing_index_count = QueryPlan.value('count(//p:MissingIndexGroup)', 'int') ,
    unmatched_index_count = QueryPlan.value('count(//p:UnmatchedIndexes/p:Parameterization/p:Object)', 'int') ,
    plan_multiple_plans = CASE WHEN distinct_plan_count < number_of_plans THEN 1 END ,
    is_trivial = CASE WHEN QueryPlan.exist('//p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1 THEN 1 END ,
    SerialDesiredMemory = QueryPlan.value('sum(//p:MemoryGrantInfo/@SerialDesiredMemory)', 'float') ,
    SerialRequiredMemory = QueryPlan.value('sum(//p:MemoryGrantInfo/@SerialRequiredMemory)', 'float'),
    CachedPlanSize = QueryPlan.value('sum(//p:QueryPlan/@CachedPlanSize)', 'float') ,
    CompileTime = QueryPlan.value('sum(//p:QueryPlan/@CompileTime)', 'float') ,
    CompileCPU = QueryPlan.value('sum(//p:QueryPlan/@CompileCPU)', 'float') ,
    CompileMemory = QueryPlan.value('sum(//p:QueryPlan/@CompileMemory)', 'float')
FROM (
SELECT COUNT(DISTINCT QueryHash) AS distinct_plan_count,
       COUNT(QueryHash) AS number_of_plans,
       QueryHash
FROM   ##bou_BlitzCacheProcs
GROUP BY QueryHash
) AS x
WHERE ##bou_BlitzCacheProcs.QueryHash = x.QueryHash
OPTION (RECOMPILE) ;


/* Update to grab stored procedure name for individual statements */
UPDATE  p
SET     QueryType = QueryType + ' (parent ' +
                    + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
                    + '.'
                    + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + ')'
FROM    ##bou_BlitzCacheProcs p
        JOIN sys.dm_exec_procedure_stats s ON p.SqlHandle = s.sql_handle
WHERE   QueryType = 'Statement'



IF @skip_analysis = 1
    GOTO Results ;



/* Set configuration values */
DECLARE @execution_threshold INT = 1000 ,
        @parameter_sniffing_warning_pct TINYINT = 30,
        /* This is in average reads */
        @parameter_sniffing_io_threshold BIGINT = 100000 ,
        @ctp_threshold_pct TINYINT = 10,
        @long_running_query_warning_seconds BIGINT = 300 * 1000 ;

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'frequent execution threshold' = LOWER(parameter_name))
BEGIN
    SELECT @execution_threshold = CAST(value AS INT)
    FROM   #configuration
    WHERE  'frequent execution threshold' = LOWER(parameter_name) ;

    SET @msg = ' Setting "frequent execution threshold" to ' + CAST(@execution_threshold AS VARCHAR(10)) ;

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'parameter sniffing variance percent' = LOWER(parameter_name))
BEGIN
    SELECT @parameter_sniffing_warning_pct = CAST(value AS TINYINT)
    FROM   #configuration
    WHERE  'parameter sniffing variance percent' = LOWER(parameter_name) ;

    SET @msg = ' Setting "parameter sniffing variance percent" to ' + CAST(@parameter_sniffing_warning_pct AS VARCHAR(3)) ;

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'parameter sniffing io threshold' = LOWER(parameter_name))
BEGIN
    SELECT @parameter_sniffing_io_threshold = CAST(value AS BIGINT)
    FROM   #configuration
    WHERE 'parameter sniffing io threshold' = LOWER(parameter_name) ;

    SET @msg = ' Setting "parameter sniffing io threshold" to ' + CAST(@parameter_sniffing_io_threshold AS VARCHAR(10));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'cost threshold for parallelism warning' = LOWER(parameter_name))
BEGIN
    SELECT @ctp_threshold_pct = CAST(value AS TINYINT)
    FROM   #configuration
    WHERE 'cost threshold for parallelism warning' = LOWER(parameter_name) ;

    SET @msg = ' Setting "cost threshold for parallelism warning" to ' + CAST(@ctp_threshold_pct AS VARCHAR(3));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END

IF EXISTS (SELECT 1/0 FROM #configuration WHERE 'long running query warning (seconds)' = LOWER(parameter_name))
BEGIN
    SELECT @long_running_query_warning_seconds = CAST(value * 1000 AS BIGINT)
    FROM   #configuration
    WHERE 'long running query warning (seconds)' = LOWER(parameter_name) ;

    SET @msg = ' Setting "long running query warning (seconds)" to ' + CAST(@long_running_query_warning_seconds AS VARCHAR(10));

    RAISERROR(@msg, 0, 1) WITH NOWAIT;
END

DECLARE @ctp INT ;

SELECT  @ctp = CAST(value AS INT)
FROM    sys.configurations
WHERE   name = 'cost threshold for parallelism'
OPTION (RECOMPILE);



/* Update to populate checks columns */
RAISERROR('Checking for query level SQL Server issues.', 0, 1) WITH NOWAIT;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE ##bou_BlitzCacheProcs
SET    frequent_execution = CASE WHEN ExecutionsPerMinute > @execution_threshold THEN 1 END ,
       parameter_sniffing = CASE WHEN AverageReads > @parameter_sniffing_io_threshold
                                      AND min_worker_time < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN AverageReads > @parameter_sniffing_io_threshold
                                      AND max_worker_time > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageCPU) THEN 1
                                 WHEN AverageReads > @parameter_sniffing_io_threshold
                                      AND MinReturnedRows < ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1
                                 WHEN AverageReads > @parameter_sniffing_io_threshold
                                      AND MaxReturnedRows > ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * AverageReturnedRows) THEN 1 END ,
       near_parallel = CASE WHEN QueryPlanCost BETWEEN @ctp * (1 - (@ctp_threshold_pct / 100.0)) AND @ctp THEN 1 END,
       long_running = CASE WHEN AverageDuration > @long_running_query_warning_seconds THEN 1
                           WHEN max_worker_time > @long_running_query_warning_seconds THEN 1
                           WHEN max_elapsed_time > @long_running_query_warning_seconds THEN 1 END ;



RAISERROR('Checking for forced parameterization and cursors.', 0, 1) WITH NOWAIT;

/* Set options checks */
UPDATE p
SET    is_forced_parameterized = CASE WHEN (CAST(pa.value AS INT) & 131072 = 131072) THEN 1
                                      END ,
       is_forced_plan = CASE WHEN (CAST(pa.value AS INT) & 131072 = 131072) THEN 1
                             WHEN (CAST(pa.value AS INT) & 4 = 4) THEN 1 
                             END ,
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
WHERE  pa.attribute = 'set_options' ;



/* Cursor checks */
UPDATE p
SET    is_cursor = CASE WHEN CAST(pa.value AS INT) <> 0 THEN 1 END
FROM   ##bou_BlitzCacheProcs p
       CROSS APPLY sys.dm_exec_plan_attributes(p.PlanHandle) pa
WHERE  pa.attribute LIKE '%cursor%' ;







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
                  CASE WHEN is_cursor = 1 THEN ', Cursor' ELSE '' END +
                  CASE WHEN is_parallel = 1 THEN ', Parallel' ELSE '' END +
                  CASE WHEN near_parallel = 1 THEN ', Nearly Parallel' ELSE '' END +
                  CASE WHEN frequent_execution = 1 THEN ', Frequent Execution' ELSE '' END +
                  CASE WHEN plan_warnings = 1 THEN ', Plan Warnings' ELSE '' END +
                  CASE WHEN parameter_sniffing = 1 THEN ', Parameter Sniffing' ELSE '' END +
                  CASE WHEN long_running = 1 THEN ', Long Running Query' ELSE '' END +
                  CASE WHEN downlevel_estimator = 1 THEN ', Downlevel CE' ELSE '' END +
                  CASE WHEN implicit_conversions = 1 THEN ', Implicit Conversions' ELSE '' END +
                  CASE WHEN tempdb_spill = 1 THEN ', TempDB Spills' ELSE '' END +
                  CASE WHEN tvf_join = 1 THEN ', Function Join' ELSE '' END +
                  CASE WHEN plan_multiple_plans = 1 THEN ', Multiple Plans' ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END 
                  , 2, 200000) ;












Results:
IF @output_database_name IS NOT NULL
   AND @output_schema_name IS NOT NULL
   AND @output_table_name IS NOT NULL
BEGIN
    RAISERROR('Writing results to table.', 0, 1) WITH NOWAIT;

    /* send results to a table */
    DECLARE @insert_sql NVARCHAR(MAX) = N'' ;

    SET @insert_sql = 'USE '
        + @output_database_name
        + '; IF EXISTS(SELECT * FROM '
        + @output_database_name
        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
        + @output_schema_name
        + ''') AND NOT EXISTS (SELECT * FROM '
        + @output_database_name
        + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
        + @output_schema_name + ''' AND QUOTENAME(TABLE_NAME) = '''
        + @output_table_name + ''') CREATE TABLE '
        + @output_schema_name + '.'
        + @output_table_name
        + N'(ID bigint NOT NULL IDENTITY(1,1),
          ServerName nvarchar(256),
          Version nvarchar(256),
          QueryType nvarchar(256),
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
          LastExecutionTime datetime,
          PlanHandle varbinary(64),
          SqlHandle varbinary(64),
          QueryHash binary(8),
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
          SampleTime DATETIME DEFAULT(GETDATE())
          CONSTRAINT [PK_' +CAST(NEWID() AS NCHAR(36)) + '] PRIMARY KEY CLUSTERED(ID))';

    EXEC sp_executesql @insert_sql ;

    SET @insert_sql =N' IF EXISTS(SELECT * FROM '
          + @output_database_name
          + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
          + @output_schema_name + N''') '
          + 'INSERT '
          + @output_database_name + '.'
          + @output_schema_name + '.'
          + @output_table_name
          + N' (ServerName, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
          + N' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
          + N' SerialRequiredMemory, SerialDesiredMemory) '
          + N'SELECT TOP (@top) '
          + QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), N'''') + N', '
          + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)), N'''') + ', '
          + N' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
          + N' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
          + N' SerialRequiredMemory, SerialDesiredMemory '
          + N' FROM ##bou_BlitzCacheProcs '
          
    SELECT @insert_sql += N' ORDER BY ' + CASE @sort_order WHEN 'cpu' THEN ' TotalCPU '
                                                    WHEN 'reads' THEN ' TotalReads '
                                                    WHEN 'writes' THEN ' TotalWrites '
                                                    WHEN 'duration' THEN ' TotalDuration '
                                                    WHEN 'executions' THEN ' ExecutionCount '
                                                    WHEN 'compiles' THEN ' PlanCreationTime '
                                                    WHEN 'avg cpu' THEN 'AverageCPU'
                                                    WHEN 'avg reads' THEN 'AverageReads'
                                                    WHEN 'avg writes' THEN 'AverageWrites'
                                                    WHEN 'avg duration' THEN 'AverageDuration'
                                                    WHEN 'avg executions' THEN 'ExecutionsPerMinute'
                                                    END + N' DESC '

    SET @insert_sql += N' OPTION (RECOMPILE) ; '    
    
    EXEC sp_executesql @insert_sql, N'@top INT', @top;

    RETURN
END
ELSE IF @export_to_excel = 1
BEGIN
    RAISERROR('Displaying results with Excel formatting (no plans).', 0, 1) WITH NOWAIT;

    /* excel output */
    UPDATE ##bou_BlitzCacheProcs
    SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),' ','<>'),'><',''),'<>',' '), 1, 32000);

    SET @sql = N'
    SELECT  TOP (@top)
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
            NumberOfPlans,
            NumberOfDistinctPlans,
            PlanCreationTime AS [Created At],
            LastExecutionTime AS [Last Execution],
            StatementStartOffset,
            StatementEndOffset,
            QueryHash,
            QueryPlanHash,
            COALESCE(SetOptions, '''') AS [SET Options]
    FROM    ##bou_BlitzCacheProcs
    WHERE   1 = 1 ' + @nl

    SELECT @sql += N' ORDER BY ' + CASE @sort_order WHEN 'cpu' THEN ' TotalCPU '
                              WHEN 'reads' THEN ' TotalReads '
                              WHEN 'writes' THEN ' TotalWrites '
                              WHEN 'duration' THEN ' TotalDuration '
                              WHEN 'executions' THEN ' ExecutionCount '
                              WHEN 'compiles' THEN ' PlanCreationTime '
                              WHEN 'avg cpu' THEN 'AverageCPU'
                              WHEN 'avg reads' THEN 'AverageReads'
                              WHEN 'avg writes' THEN 'AverageWrites'
                              WHEN 'avg duration' THEN 'AverageDuration'
                              WHEN 'avg executions' THEN 'ExecutionsPerMinute'
                              END + N' DESC '

    SET @sql += N' OPTION (RECOMPILE) ; '

    EXEC sp_executesql @sql, N'@top INT', @top ;
END

IF @hide_summary = 0
BEGIN
    IF @reanalyze = 0
    BEGIN
        RAISERROR('Building query plan summary data.', 0, 1) WITH NOWAIT;

        /* Build summary data */
        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE frequent_execution =1)
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
                  )
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
                  )
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    3,
                    5,
                    'Parameterization',
                    'Forced Plans',
                    'http://brentozar.com/blitzcache/forced-plans/',
                    'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

        /* Cursors */
        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  is_cursor = 1
                  )
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
                   WHERE  is_forced_parameterized = 1
                  )
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
                  )
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
                  )
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
                  )
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
                  )
            INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (@@SPID,
                    9,
                    50,
                    'Performance',
                    'Long Running Queries',
                    'http://brentozar.com/blitzcache/long-running-queries/',
                    'Long running queries have beend found. These are queries with an average duration longer than '
                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 AS VARCHAR(5))
                    + ' second(s). These queries should be investigated for additional tuning options') ;

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs p
                   WHERE  p.missing_index_count > 0)
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
                  )
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
                  )
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
                   WHERE  tempdb_spill = 1
                  )
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                15,
                10,
                'Performance',
                'TempDB Spills',
                'http://brentozar.com/blitzcache/tempdb-spills/',
                'TempDB spills detected. Queries are unable to allocate enough memory to proceed normally.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  busy_loops = 1)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                16,
                10,
                'Performance',
                'Frequently executed operators',
                'http://brentozar.com/blitzcache/busy-loops/',
                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  tvf_join = 1)
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
                   WHERE  compile_timeout = 1)
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
                   WHERE  compile_memory_limit_exceeded = 1)
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
                   WHERE  warning_no_join_predicate = 1)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                20,
                10,
                'Execution Plans',
                'No join predicate',
                'http://brentozar.com/blitzcache/no-join-predicate/',
                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  plan_multiple_plans = 1)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                21,
                200,
                'Execution Plans',
                'Multiple execution plans',
                'http://brentozar.com/blitzcache/multiple-plans/',
                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count/');

        IF EXISTS (SELECT 1/0
                   FROM   ##bou_BlitzCacheProcs
                   WHERE  unmatched_index_count > 0)
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
                   WHERE  unparameterized_query = 1)
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
                   WHERE  is_trivial = 1)
        INSERT INTO ##bou_BlitzCacheResults (SPID, CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (@@SPID,
                24,
                100,
                'Execution Plans',
                'Trivial Plans',
                'http://brentozar.com/blitzcache/trivial-plans',
                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');
    END            
    
    IF @export_to_excel = 1
        RETURN

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
    ORDER BY Priority ASC
    OPTION (RECOMPILE);
END



RAISERROR('Displaying analysis of plan cache.', 0, 1) WITH NOWAIT;

DECLARE @columns NVARCHAR(MAX) = N'' ;

IF LOWER(@results) = 'narrow'
BEGIN
    SET @columns = N' DatabaseName AS [Database],
    QueryPlanCost AS [Cost],
    QueryText AS [Query Text],
    QueryType AS [Query Type],
    Warnings AS [Warnings],
    ExecutionCount AS [# Executions],
    AverageCPU AS [Average CPU (ms)],
    AverageDuration AS [Average Duration (ms)],
    AverageReads AS [Average Reads],
    AverageWrites AS [Average Writes],
    AverageReturnedRows AS [Average Rows Returned],
    PlanCreationTime AS [Created At],
    LastExecutionTime AS [Last Execution],
    QueryPlan AS [Query] ';
END
ELSE IF LOWER(@results) = 'simple'
BEGIN
    SET @columns = N' DatabaseName AS [Database],
    QueryPlanCost AS [Cost],
    QueryText AS [Query Text],
    QueryType AS [Query Type],
    Warnings AS [Warnings],
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
    PlanCreationTime AS [Created At],
    LastExecutionTime AS [Last Execution],
    QueryPlan AS [Query Plan],
    COALESCE(SetOptions, '''') AS [SET Options] ';
END
ELSE
BEGIN
    SET @columns = N' DatabaseName AS [Database],
        QueryText AS [Query Text],
        QueryType AS [Query Type],
        Warnings AS [Warnings], ' + @nl

    IF LOWER(@results) = 'opserver1'
    BEGIN
        SET @columns += '        SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN '', 20'' ELSE '''' END +
                  CASE WHEN compile_timeout = 1 THEN '', 18'' ELSE '''' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN '', 19'' ELSE '''' END +
                  CASE WHEN busy_loops = 1 THEN '', 16'' ELSE '''' END +
                  CASE WHEN is_forced_plan = 1 THEN '', 3'' ELSE '''' END +
                  CASE WHEN is_forced_parameterized = 1 THEN '', 5'' ELSE '''' END +
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
                  CASE WHEN tempdb_spill = 1 THEN '', 15'' ELSE '''' END +
                  CASE WHEN tvf_join = 1 THEN '', 17'' ELSE '''' END +
                  CASE WHEN plan_multiple_plans = 1 THEN '', 21'' ELSE '''' END +
                  CASE WHEN unmatched_index_count > 0 THEN '', 22'', ELSE '''' END + 
                  CASE WHEN unparameterized_query > 0 THEN '', 23'', ELSE '''' END + 
                  Case WHEN is_trivial = 1 THEN '', 24'', ELSE '''' END
                  , 2, 200000) AS opserver_warning , ' + @nl ;
    END
    
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
        NumberOfPlans AS [# Plans],
        NumberOfDistinctPlans AS [# Distinct Plans],
        PlanCreationTime AS [Created At],
        LastExecutionTime AS [Last Execution],
        QueryPlanCost AS [Query Plan Cost],
        QueryPlan AS [Query Plan],
        CachedPlanSize AS [Cached Plan Size (KB)],
        CompileTime AS [Compile Time (ms)],
        CompileCPU AS [Compile CPU (ms)],
        CompileMemory AS [Compile memory (KB)],
        COALESCE(SetOptions, '''') AS [SET Options],
        PlanHandle AS [Plan Handle],
        SqlHandle AS [SQL Handle],
        QueryHash AS [Query Hash],
        QueryPlanHash AS [Query Plan Hash],
        StatementStartOffset,
        StatementEndOffset ';
END



SET @sql = N'
SELECT  TOP (@top) ' + @columns + @nl + N'
FROM    ##bou_BlitzCacheProcs
WHERE   SPID = @spid ' + @nl

SELECT @sql += N' ORDER BY ' + CASE @sort_order WHEN 'cpu' THEN ' TotalCPU '
                                                WHEN 'reads' THEN ' TotalReads '
                                                WHEN 'writes' THEN ' TotalWrites '
                                                WHEN 'duration' THEN ' TotalDuration '
                                                WHEN 'executions' THEN ' ExecutionCount '
                                                WHEN 'compiles' THEN ' PlanCreationTime '
                                                WHEN 'avg cpu' THEN 'AverageCPU'
                                                WHEN 'avg reads' THEN 'AverageReads'
                                                WHEN 'avg writes' THEN 'AverageWrites'
                                                WHEN 'avg duration' THEN 'AverageDuration'
                                                WHEN 'avg executions' THEN 'ExecutionsPerMinute'
                               END + N' DESC '
SET @sql += N' OPTION (RECOMPILE) ; '

EXEC sp_executesql @sql, N'@top INT, @spid INT', @top, @@SPID ;


GO
