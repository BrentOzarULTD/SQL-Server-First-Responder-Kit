IF OBJECT_ID('dbo.sp_BlitzCache') IS NULL 
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzCache AS RETURN 0;')
GO

ALTER PROCEDURE dbo.sp_BlitzCache
    @get_help BIT = 0,
    @top INT = 50, 
    @sort_order VARCHAR(10) = 'CPU',
    @use_triggers_anyway BIT = NULL,
    @export_to_excel BIT = 0,
    @output_database_name NVARCHAR(128) = NULL ,
    @output_schema_name NVARCHAR(256) = NULL ,
    @output_table_name NVARCHAR(256) = NULL ,
    @duration_filter DECIMAL(38,4) = NULL,
    @whole_cache BIT = 0 /* This will forcibly set @top to 2,147,483,647 */

/******************************************
(C) 2014, Brent Ozar Unlimited. 
See http://BrentOzar.com/go/eula for the End User Licensing Agreement.


HOW TO USE THIS:

Don't just hit F5. This script has 3 separate parts:
Step 1. Populate a temp table
Step 2. Report on the temp table with analysis 
Step 3 (optional). Excel-friendly copy/paste version of #2.


Description: Displays a server level view of the SQL Server plan cache.

Output: One result set is presented that contains data from the statement, 
procedure, and trigger stats DMVs.

To learn more, visit http://www.brentozar.com/responder/get-top-resource-consuming-queries/ 
where you can download new versions for free, watch training videos on
how it works, get more info on the findings, and more. To contribute 
code and see your name in the change log, email your improvements & 
ideas to help@brentozar.com.


KNOWN ISSUES:
- This query will not run on SQL Server 2005.
- SQL Server 2008 and 2008R2 have a bug in trigger stats (see below).

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
           N'Data processing and display order. @sort_order will still be used, even when preparing output for a table or for excel. Possible values are: "CPU", "Reads", "Writes", "Duration", "Executions".'
           
    UNION ALL
    SELECT N'@use_triggers_anyway',
           N'BIT',
           N'On SQL Server 2008R2 and earlier, trigger execution count is wildly incorrect. If you still want to see relative execution count of triggers, then you can force sp_BlitzCache to include this information.'
           
    UNION ALL
    SELECT N'@export_to_excel',
           N'BIT',
           N'Prepare output for exporting to Excel. Newlines and additional whitespace are removed from query text and the execution plan is not displayed.'
           
    UNION ALL
    SELECT N'@output_database_name',
           N'NVARCHAR(128)',
           N'The output database. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@output_schema_name',
           N'NVARCHAR(256)',
           N'Output schema. If this does not exist SQL Server will divide by zero and everything will fall apart.'

    UNION ALL
    SELECT N'@output_table_name',
           N'NVARCHAR(256)',
           N'Output table. If this does not exist, it will be created for you.'

    UNION ALL
    SELECT N'@duration_filter',
           N'DECIMAL(38,4)',
           N'Filters queries with an average duration (seconds) less than @duration_filter.'

    UNION ALL
    SELECT N'@whole_cache',
           N'BIT',
           N'This forces sp_BlitzCache to examine the entire plan cache. Be careful running this on servers with a lot of memory or a large execution plan cache.' ;


           
    /* Column definitions */
    SELECT N'# Executions' AS [Column Name],
           N'BIGINT' AS [Data Type],
           N'The number of executions of this particular query. This is computed across statements, procedures, and triggers and aggregated by the SQL handle.' AS [Column Description]

    UNION ALL
    SELECT N'Executions / Minute',
           N'MONEY',
           N'Number of executions per minute for this SQL handle. This is calculated for the life of the current plan. Plan life is the last execution time minus the plan creation time.'

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
           N'Total CPU time, reported in microseconds, that was consumed by all executions of this query since the last compilation.'

    UNION ALL
    SELECT N'Avg CPU',
           N'BIGINT',
           N'Average CPU time, reported in microseconds, consumed by each execution of this query since the last compilation.'

    UNION ALL
    SELECT N'CPU Weight',
           N'MONEY',
           N'An arbitrary metric of total "CPU-ness". A weight of 2 is "one more" than a weight of 1.'


    UNION ALL
    SELECT N'Total Duration',
           N'BIGINT',
           N'Total elapsed time, reported in microseconds, consumed by all executions of this query since last compilation.'

    UNION ALL
    SELECT N'Avg Duration',
           N'BIGINT',
           N'Average elapsed time, reported in microseconds, consumed by each execution of this query since the last compilation.'

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
           N'Average logical writes performed by each exuection this query since last compilation.'

    UNION ALL
    SELECT N'Write Weight',
           N'MONEY',
           N'An arbitrary metric of "Write-ness". A weight of 2 is "one more" than a weight of 1.'

    UNION ALL
    SELECT N'Query Type',
           N'NVARCHAR(256)',
           N'The type of query being examined. This can be "Procedure", "Statement", or "Trigger".' + NCHAR(13) + NCHAR(10)
             + N'If the first character of the Query Type column is an asterisk, this query has a parallel plan.'

    UNION ALL
    SELECT N'Query Text',
           N'NVARCHAR(4000)',
           N'The text of the query. This may be truncated by either SQL Server or by sp_BlitzCache for display purposes.'

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
    RETURN
END

DECLARE @duration_filter_i INT;

/* Change duration from seconds to microseconds */
IF @duration_filter IS NOT NULL
  SET @duration_filter_i = CAST((@duration_filter * 1000.0 * 1000.0) AS INT)

SET @sort_order = LOWER(@sort_order);

IF @sort_order NOT IN ('cpu', 'reads', 'writes', 'duration', 'executions')
  SET @sort_order = 'cpu';

SELECT @output_database_name = QUOTENAME(@output_database_name),
       @output_schema_name   = QUOTENAME(@output_schema_name),
       @output_table_name    = QUOTENAME(@output_table_name)



IF OBJECT_ID('tempdb..#p') IS NOT NULL
    DROP TABLE #p;

IF OBJECT_ID('tempdb..#procs') IS NOT NULL
    DROP TABLE #procs;

IF OBJECT_ID ('tempdb..#checkversion') IS NOT NULL
    DROP TABLE #checkversion;

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
    maj_version AS SUBSTRING(version, 1,CHARINDEX('.', version) + 1 ),
    build AS PARSENAME(CONVERT(varchar(32), version), 2)
);

-- TODO: Add columns from main query to #procs
CREATE TABLE #procs (
    QueryType nvarchar(256),
    DatabaseName sysname,
    AverageCPU bigint,
    AverageCPUPerMinute money,
    TotalCPU bigint,
    PercentCPUByType money,
    PercentCPU money,
    AverageDuration bigint,
    TotalDuration bigint,
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
    NumberOfDistinctPlans int
);

DECLARE @sql nvarchar(MAX) = N'',
        @insert_list nvarchar(MAX) = N'',
        @plans_triggers_select_list nvarchar(MAX) = N'',
        @body nvarchar(MAX) = N'',
        @nl nvarchar(2) = NCHAR(13) + NCHAR(10),
        @q nvarchar(1) = N'''',
        @pv varchar(20),
        @pos tinyint,
        @v decimal(6,2),
        @build int;


INSERT INTO #checkversion (version) 
SELECT CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));
 

SELECT @v = maj_version ,
       @build = build 
FROM   #checkversion ;

SET @insert_list += N'
INSERT INTO #procs (QueryType, DatabaseName, AverageCPU, TotalCPU, AverageCPUPerMinute, PercentCPUByType, PercentDurationByType, 
                    PercentReadsByType, PercentExecutionsByType, AverageDuration, TotalDuration, AverageReads, TotalReads, ExecutionCount,
                    ExecutionsPerMinute, TotalWrites, AverageWrites, PercentWritesByType, WritesPerMinute, PlanCreationTime, 
                    LastExecutionTime, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, 
                    LastReturnedRows, QueryText, QueryPlan, TotalWorkerTimeForType, TotalElapsedTimeForType, TotalReadsForType, 
                    TotalExecutionCountForType, TotalWritesForType, SqlHandle, PlanHandle, QueryHash, QueryPlanHash) ' ;

SET @body += N'
FROM   (SELECT *,
               CAST((CASE WHEN DATEDIFF(second, cached_time, GETDATE()) > 0 And execution_count > 1
                          THEN DATEDIFF(second, cached_time, GETDATE()) / 60.0
                          ELSE NULL END) as MONEY) as age_minutes, 
               CAST((CASE WHEN DATEDIFF(second, cached_time, last_execution_time) > 0 And execution_count > 1
                          THEN DATEDIFF(second, cached_time, last_execution_time) / 60.0
                          ELSE Null END) as MONEY) as age_minutes_lifetime
        FROM   sys.#view#) AS qs
       CROSS JOIN(SELECT SUM(execution_count) AS t_TotalExecs,
                         SUM(total_elapsed_time) AS t_TotalElapsed, 
                         SUM(total_worker_time) AS t_TotalWorker,
                         SUM(total_logical_reads) AS t_TotalReads,
                         SUM(total_logical_writes) AS t_TotalWrites
                  FROM   sys.#view#) AS t
       CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
       CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
       CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE  pa.attribute = ' + QUOTENAME('dbid', @q) + @nl

IF @duration_filter IS NOT NULL
  SET @body += N'       AND total_elapsed_time / execution_count > @min_duration ' + @nl

SET @body += N'ORDER BY #sortable# DESC
OPTION(RECOMPILE);'

SET @plans_triggers_select_list += N'
SELECT TOP (@top)
       CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN ''* '' ELSE '''' END 
            + ''#query_type#'' 
            + COALESCE('': '' + OBJECT_NAME(qs.object_id, qs.database_id),'''') AS QueryType,
       COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), ''-- N/A --'') AS DatabaseName,
       total_worker_time / execution_count AS AvgCPU ,
       total_worker_time AS TotalCPU ,
       CASE WHEN total_worker_time = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST(total_worker_time / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time)) AS MONEY) 
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
       CASE WHEN t.t_TotalExecs = 0 THEN 0
            ELSE CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY)
            END AS PercentExecutionsByType,
       total_elapsed_time / execution_count AS AvgDuration , 
       total_elapsed_time AS TotalDuration ,
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
       NULL AS QueryPlanHash '


SET @sql += @insert_list;

SET @sql += N'
SELECT TOP (@top)
       CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN ''* '' ELSE '''' END
            + ''Statement'' AS QueryType,
       COALESCE(DB_NAME(CAST(pa.value AS INT)), ''-- N/A --'') AS DatabaseName,
       total_worker_time / execution_count AS AvgCPU ,
       total_worker_time AS TotalCPU ,
       CASE WHEN total_worker_time = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST(total_worker_time / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time)) AS MONEY) 
            END AS AverageCPUPerMinute ,
       CAST(ROUND(100.00 * total_worker_time / t.t_TotalWorker, 2) AS MONEY) AS PercentCPUByType,
       CAST(ROUND(100.00 * total_elapsed_time / t.t_TotalElapsed, 2) AS MONEY) AS PercentDurationByType, 
       CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY) AS PercentReadsByType,
       CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY) AS PercentExecutionsByType,
       total_elapsed_time / execution_count AS AvgDuration , 
       total_elapsed_time AS TotalDuration ,
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

IF (@v > 11) OR (@v >= 10.5 AND @build >= 2500)
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
       qs.query_plan_hash AS QueryPlanHash '

SET @sql += REPLACE(REPLACE(@body, '#view#', 'dm_exec_query_stats'), 'cached_time', 'creation_time') ;
SET @sql += @nl + @nl;



SET @sql += @insert_list;
SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Stored Procedure') ;

SET @sql += REPLACE(@body, '#view#', 'dm_exec_procedure_stats') ;
SET @sql += @nl + @nl;



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
IF @use_triggers_anyway = 1 OR @v >= 11
BEGIN
   /* Trigger level information from the plan cache */
   SET @sql += @insert_list ;

   SET @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Trigger') ;

   SET @sql += REPLACE(@body, '#view#', 'dm_exec_trigger_stats') ;
END




DECLARE @sort NVARCHAR(30);

SELECT @sort = CASE @sort_order WHEN 'cpu' THEN 'total_worker_time'
                                WHEN 'reads' THEN 'total_logical_reads'
                                WHEN 'writes' THEN 'total_logical_writes'
                                WHEN 'duration' THEN 'total_elapsed_time'
                                WHEN 'executions' THEN 'execution_count'
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
         FROM    #procs) AS x
WHERE x.rn = 1
';

SELECT @sort = CASE @sort_order WHEN 'cpu' THEN 'TotalCPU'
                                WHEN 'reads' THEN 'TotalReads'
                                WHEN 'writes' THEN 'TotalWrites'
                                WHEN 'duration' THEN 'TotalDuration'
                                WHEN 'executions' THEN 'ExecutionCount'
               END ;

SELECT @sql = REPLACE(@sql, '#sortable#', @sort);



EXEC sp_executesql @sql, N'@top INT, @min_duration INT', @top, @duration_filter_i;



/* Compute the total CPU, etc across our active set of the plan cache.
 * Yes, there's a flaw - this doesn't include anything outside of our @top 
 * metric.
 */
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
UPDATE #procs
SET     PercentCPU = y.PercentCPU,
        PercentDuration = y.PercentDuration,
        PercentReads = y.PercentReads,
        PercentExecutions = y.PercentExecutions,
        ExecutionsPerMinute = y.ExecutionsPerMinute,
        /* Strip newlines and tabs. Tabs are replaced with multiple spaces
           so that the later whitespace trim will completely eliminate them
         */
        QueryText = REPLACE(REPLACE(REPLACE(QueryText, @cr, ' '), @lf, ' '), @tab, '  ')
FROM (
    SELECT  PlanHandle,
            CAST((100. * TotalCPU) / @total_cpu AS MONEY) AS PercentCPU,
            CAST((100. * TotalDuration) / @total_duration AS MONEY) AS PercentDuration,
            CAST((100. * TotalReads) / @total_reads AS MONEY) AS PercentReads,
            CAST((100. * ExecutionCount) / @total_execution_count AS MONEY) AS PercentExecutions,
            CASE  DATEDIFF(mi, PlanCreationTime, LastExecutionTime)
                WHEN 0 THEN 0
                ELSE CAST((1.00 * ExecutionCount / DATEDIFF(mi, PlanCreationTime, LastExecutionTime)) AS money) 
            END AS ExecutionsPerMinute
    FROM (
        SELECT  PlanHandle,
                TotalCPU,
                TotalDuration,
                TotalReads,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
        FROM    #procs
        WHERE   PlanHandle IS NOT NULL
        GROUP BY PlanHandle,
                TotalCPU,
                TotalDuration,
                TotalReads,
                ExecutionCount,
                PlanCreationTime,
                LastExecutionTime
    ) AS x
) AS y
WHERE #procs.PlanHandle = y.PlanHandle
      AND #procs.PlanHandle IS NOT NULL
OPTION (RECOMPILE) ;



UPDATE #procs
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
            CAST((100. * TotalCPU) / @total_cpu AS MONEY) AS PercentCPU,
            CAST((100. * TotalDuration) / @total_duration AS MONEY) AS PercentDuration,
            CAST((100. * TotalReads) / @total_reads AS MONEY) AS PercentReads,
            CAST((100. * TotalWrites) / @total_writes AS MONEY) AS PercentWrites,
            CAST((100. * ExecutionCount) / @total_execution_count AS MONEY) AS PercentExecutions,
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
        FROM    #procs
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
WHERE   #procs.SqlHandle = y.SqlHandle
        AND #procs.QueryHash = y.QueryHash
        AND #procs.DatabaseName = y.DatabaseName
        AND #procs.PlanHandle IS NULL
OPTION (RECOMPILE) ;


UPDATE #procs
SET NumberOfDistinctPlans = distinct_plan_count,
    NumberOfPlans = number_of_plans
FROM (
SELECT COUNT(DISTINCT QueryHash) AS distinct_plan_count,
       COUNT(QueryHash) AS number_of_plans,
       QueryHash
FROM   #procs
GROUP BY QueryHash
) AS x 
WHERE #procs.QueryHash = x.QueryHash
OPTION (RECOMPILE) ;











IF @output_database_name IS NOT NULL
   AND @output_schema_name IS NOT NULL
   AND @output_schema_name IS NOT NULL
BEGIN
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
          DatabaseName sysname,
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
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans) '
          + N'SELECT '
          + QUOTENAME(CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)), N'''') + N', '
          + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)), N'''') + ', '
          + N' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
          + N' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
          + N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, PlanHandle, SqlHandle, QueryHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans '
          + N' FROM #procs OPTION (RECOMPILE) '
    EXEC sp_executesql @insert_sql;

END
ELSE IF @export_to_excel = 1
BEGIN
    /* excel output */
    UPDATE #procs
    SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),' ','<>'),'><',''),'<>',' '), 1, 100);

    SET @sql = N'
    SELECT  ExecutionCount,
            ExecutionsPerMinute AS [Executions / Minute],
            PercentExecutions AS [Execution Weight],
            PercentExecutionsByType AS [% Executions (Type)],    
            QueryType AS [Query Type],
            DatabaseName AS [Database Name],
            QueryText,
            TotalCPU AS [Total CPU],
            AverageCPU AS [Avg CPU],
            PercentCPU AS [CPU Weight],
            PercentCPUByType AS [% CPU (Type)],
            TotalDuration AS [Total Duration],
            AverageDuration AS [Avg Duration],
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
            StatementEndOffset
    FROM    #procs 
    WHERE   1 = 1 ' + @nl

    SELECT @sql += N' ORDER BY ' + CASE @sort_order WHEN 'cpu' THEN ' TotalCPU '
                              WHEN 'reads' THEN ' TotalReads '
                              WHEN 'writes' THEN ' TotalWrites '
                              WHEN 'duration' THEN ' TotalDuration '
                              WHEN 'executions' THEN ' ExecutionCount '
                              END + N' DESC '
    
    SET @sql += N' OPTION (RECOMPILE) ; '

    EXEC sp_executesql @sql, N'@min_duration INT', @duration_filter_i;
END
ELSE
BEGIN
    /* Default behavior is to display all results */
    SET @sql = N'
    SELECT  ExecutionCount AS [# Executions],
            ExecutionsPerMinute AS [Executions / Minute],
            PercentExecutions AS [Execution Weight],
            DatabaseName AS [Database],
            TotalCPU AS [Total CPU],
            AverageCPU AS [Avg CPU],
            PercentCPU AS [CPU Weight],
            TotalDuration AS [Total Duration],
            AverageDuration AS [Avg Duration],
            PercentDuration AS [Duration Weight],
            TotalReads AS [Total Reads],
            AverageReads AS [Average Reads],
            PercentReads AS [Read Weight],
            TotalWrites AS [Total Writes],
            AverageWrites AS [Average Writes],
            PercentWrites AS [Write Weight],
            QueryType AS [Query Type],
            QueryText AS [Query Text], 
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
            QueryPlan AS [Query Plan],
            PlanHandle AS [Plan Handle],
            SqlHandle AS [SQL Handle],
            QueryHash AS [Query Hash],
            StatementStartOffset,
            StatementEndOffset
    FROM    #procs
    WHERE   1 = 1 ' + @nl

    SELECT @sql += N' ORDER BY ' + CASE @sort_order WHEN 'cpu' THEN ' TotalCPU '
                              WHEN 'reads' THEN ' TotalReads '
                              WHEN 'writes' THEN ' TotalWrites '
                              WHEN 'duration' THEN ' TotalDuration '
                              WHEN 'executions' THEN ' ExecutionCount '
                              END + N' DESC '
    SET @sql += N' OPTION (RECOMPILE) ; '

    EXEC sp_executesql @sql, N'@min_duration INT', @duration_filter_i;
END


GO




