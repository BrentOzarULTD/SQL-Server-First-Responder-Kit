SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

DECLARE @msg NVARCHAR(MAX) = N''

IF  (
	SELECT PARSENAME(CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)
	) < 13
BEGIN
	SELECT @msg = 'Sorry, sp_BlitzQS doesn''t work on versions of SQL prior to 2016.' + REPLICATE(CHAR(13), 7933)
	PRINT @msg
	RETURN
END

IF OBJECT_ID('dbo.sp_BlitzQS') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzQS AS RETURN 0;')
GO

ALTER PROCEDURE dbo.sp_BlitzQS
    @Help BIT = 0,
    @DatabaseName NVARCHAR(128) = NULL ,
    @Top INT = 3,
	@StartDate DATETIME2 = NULL,
	@EndDate DATETIME2 = NULL,
    @MinimumExecutionCount INT = NULL,
    @DurationFilter DECIMAL(38,4) = NULL ,
    @StoredProcName NVARCHAR(128) = NULL,
	@Failed BIT = 0,
    @QueryFilter NVARCHAR(10) = 'ALL' ,
    @ExportToExcel BIT = 0,
    @HideSummary BIT = 0 ,
	@SkipXML BIT = 0,
	@Debug BIT = 0,
	@VersionDate DATETIME = NULL OUTPUT
WITH RECOMPILE
AS
BEGIN /*First BEGIN*/

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Version VARCHAR(30);
SET @Version = '1.0';
SET @VersionDate = '20170601';

DECLARE /*Variables for the variable Gods*/
		@msg NVARCHAR(MAX) = N'',
		@sql_select NVARCHAR(MAX) = N'',
		@sql_where NVARCHAR(MAX) = N'',
		@duration_filter_ms DECIMAL(38,4) = (@DurationFilter * 1000.),
		@execution_threshold INT = 1000,
		@parameter_sniffing_warning_pct TINYINT = 30,
        @parameter_sniffing_io_threshold BIGINT = 100000 ,
        @ctp_threshold_pct TINYINT = 10,
        @long_running_query_warning_seconds BIGINT = 300 * 1000 ,
		@memory_grant_warning_percent INT = 10,
		@ctp INT,
		@min_memory_per_query INT,
		@cr NVARCHAR(1) = NCHAR(13),
		@lf NVARCHAR(1) = NCHAR(10),
		@tab NVARCHAR(1) = NCHAR(9),
		@sp_params NVARCHAR(MAX) = N'@sp_Top INT, @sp_StartDate DATETIME2, @sp_EndDate DATETIME2, @sp_MinimumExecutionCount INT, @sp_MinDuration INT, @sp_StoredProcName NVARCHAR(128)'


SELECT  @ctp = NULLIF(CAST(value AS INT), 0)
FROM    sys.configurations
WHERE   name = 'cost threshold for parallelism'
OPTION (RECOMPILE);

SELECT @min_memory_per_query = CONVERT(INT, c.value)
FROM   sys.configurations AS c
WHERE  c.name = 'min memory per query (KB)';


/*Help section.*/
IF @Help = 1
	BEGIN
	SELECT 'You have requested assistance. It will arrive as soon as humanly possible.' AS [Take four red capsules, help is on the way];

	PRINT '
	sp_BlitzQS from http://FirstResponderKit.org
		
	This script displays your most resource-intensive queries from the Query Store,
	and points to ways you can tune these queries to make them faster.
	
	
	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.
	
	Known limitations of this version:
	 - This query will not run on SQL Server versions less than 2016.

	Unknown limitations of this version:
	 - Could be tickling
	
	Changes - for the full list of improvements and fixes in this version, see:
	https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	
	
	MIT License
	
	Copyright (c) 2016 Brent Ozar Unlimited
	
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
	'
	RETURN;

END

/*Making sure your version is copasetic*/
IF  (
	SELECT PARSENAME(CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)
	) < 13
BEGIN
	SELECT @msg = 'Sorry, sp_BlitzQS doesn''t work on versions of SQL prior to 2016.' + REPLICATE(CHAR(13), 7933)
	PRINT @msg
	RETURN
END

/*Making sure at least one database uses QS*/
IF  (
	SELECT COUNT(*)
	FROM sys.databases AS d
	WHERE d.is_query_store_on = 1
	AND user_access_desc='MULTI_USER'
	AND state_desc = 'ONLINE'
	AND name NOT IN ('master', 'model', 'msdb', 'tempdb', '32767') 
	AND is_distributor = 0
	) = 0
BEGIN
	SELECT @msg = 'You don''t currently have any databases with QS enabled.' + REPLICATE(CHAR(13), 7933)
	PRINT @msg
	RETURN
END

/*Making sure your databases are using QDS.*/
RAISERROR('Checking database validity', 0, 1) WITH NOWAIT;

SET @DatabaseName = LTRIM(RTRIM(@DatabaseName));

BEGIN	
	
	/*Did you set @DatabaseName?*/
	RAISERROR('Making sure @DatabaseName isn''t NULL', 0, 1) WITH	NOWAIT;
	IF (@DatabaseName IS NULL)
	BEGIN
	   RAISERROR('@DatabaseName cannot be NULL', 0, 1) WITH	NOWAIT;
	   RETURN;
	END

	/*Does the database exist?*/
	RAISERROR('Making sure @DatabaseName exists', 0, 1) WITH	NOWAIT;
	IF ((DB_ID(@DatabaseName)) IS NULL)
	BEGIN
	   RAISERROR('The @DatabaseName you specified does not exist. Please check the name and try again.', 0, 1) WITH	NOWAIT;
	   RETURN;
	END

	/*Is it online?*/
	RAISERROR('Making sure databasename is online', 0, 1) WITH	NOWAIT;
	IF (DATABASEPROPERTYEX(@DatabaseName, 'Status')) <> 'ONLINE'
	BEGIN
	   RAISERROR('The @DatabaseName you specified is not readable. Please check the name and try again. Better yet, check your server.', 0, 1);
	   RETURN;
	END

	/*Does it have Query Store enabled?*/
	RAISERROR('Making sure @DatabaseName has Query Store enabled', 0, 1) WITH	NOWAIT;
	IF 	
		((DB_ID(@DatabaseName)) IS NOT NULL AND @DatabaseName <> '')
	AND		
		( 
			SELECT DB_NAME(d.database_id)
			FROM sys.databases AS d
			WHERE d.is_query_store_on = 1
			AND user_access_desc='MULTI_USER'
			AND state_desc = 'ONLINE'
			AND DB_NAME(d.database_id) = @DatabaseName
		) IS NULL
	BEGIN
	   RAISERROR('The @DatabaseName you specified does not have the Query Store enabled. Please check the name or settings, and try again.', 0, 1) WITH	NOWAIT;
	   RETURN;
	END

END

/*Making sure top is set to something if NULL*/
IF ( @Top IS NULL )
   BEGIN
         SET @Top = 3;
   END;

SET @QueryFilter = LOWER(@QueryFilter);

IF LEFT(@QueryFilter, 3) NOT IN ('all', 'sta', 'pro')
  BEGIN
  RAISERROR(N'Invalid query filter chosen. Reverting to all.', 0, 1) WITH NOWAIT;
  SET @QueryFilter = 'all';
  END

/*
These are the temp tables we use
*/


/*
This one holds the grouped data that helps use figure out which periods to examine
*/

RAISERROR(N'Creating temp tables', 0, 1) WITH NOWAIT;

DROP TABLE IF EXISTS #grouped_interval;

CREATE TABLE #grouped_interval
(
    flate_date DATE NULL,
    start_range DATETIME NULL,
    end_range DATETIME NULL,
    total_avg_duration_ms DECIMAL(38, 2) NULL,
    total_avg_cpu_time_ms DECIMAL(38, 2) NULL,
    total_avg_logical_io_reads_mb DECIMAL(38, 2) NULL,
    total_avg_physical_io_reads_mb DECIMAL(38, 2) NULL,
    total_avg_logical_io_writes_mb DECIMAL(38, 2) NULL,
    total_avg_query_max_used_memory_mb DECIMAL(38, 2) NULL,
    total_rowcount DECIMAL(38, 2) NULL,
    total_count_executions BIGINT NULL,
	INDEX ix_dates CLUSTERED (start_range, end_range)
);


/*
These are the plans we focus on based on what we find in the grouped intervals
*/
DROP TABLE IF EXISTS #working_plans;

CREATE TABLE #working_plans
(
    plan_id BIGINT,
    query_id BIGINT,
	pattern NVARCHAR(256),
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


/*
These are the gathered metrics we get from query store to generate some warnings and help you find your worst offenders
*/
DROP TABLE IF EXISTS #working_metrics

CREATE TABLE #working_metrics (
    database_name NVARCHAR(256),
	plan_id BIGINT,
    query_id BIGINT,
	/*these columns are from query_store_query*/
	proc_or_function_name NVARCHAR(256),
	batch_sql_handle VARBINARY(64),
	query_hash BINARY(8),
	query_parameterization_type_desc NVARCHAR(256),
	parameter_sniffing_symptoms NVARCHAR(4000),
	count_compiles BIGINT,
	avg_compile_duration DECIMAL(38,2),
	last_compile_duration DECIMAL(38,2),
	avg_bind_duration DECIMAL(38,2),
	last_bind_duration DECIMAL(38,2),
	avg_bind_cpu_time DECIMAL(38,2),
	last_bind_cpu_time DECIMAL(38,2),
	avg_optimize_duration DECIMAL(38,2),
	last_optimize_duration DECIMAL(38,2),
	avg_optimize_cpu_time DECIMAL(38,2),
	last_optimize_cpu_time DECIMAL(38,2),
	avg_compile_memory_kb DECIMAL(38,2),
	last_compile_memory_kb DECIMAL(38,2),
	/*These come from query_store_runtime_stats*/
	execution_type_desc NVARCHAR(128),
	first_execution_time DATETIME2,
	last_execution_time DATETIME2,
	count_executions BIGINT,
	avg_duration DECIMAL(38,2) ,
	last_duration DECIMAL(38,2),
	min_duration DECIMAL(38,2),
	max_duration DECIMAL(38,2),
	avg_cpu_time DECIMAL(38,2),
	last_cpu_time DECIMAL(38,2),
	min_cpu_time DECIMAL(38,2),
	max_cpu_time DECIMAL(38,2),
	avg_logical_io_reads DECIMAL(38,2),
	last_logical_io_reads DECIMAL(38,2),
	min_logical_io_reads DECIMAL(38,2),
	max_logical_io_reads DECIMAL(38,2),
	avg_logical_io_writes DECIMAL(38,2),
	last_logical_io_writes DECIMAL(38,2),
	min_logical_io_writes DECIMAL(38,2),
	max_logical_io_writes DECIMAL(38,2),
	avg_physical_io_reads DECIMAL(38,2),
	last_physical_io_reads DECIMAL(38,2),
	min_physical_io_reads DECIMAL(38,2),
	max_physical_io_reads DECIMAL(38,2),
	avg_clr_time DECIMAL(38,2),
	last_clr_time DECIMAL(38,2),
	min_clr_time DECIMAL(38,2),
	max_clr_time DECIMAL(38,2),
	avg_dop BIGINT,
	last_dop BIGINT,
	min_dop BIGINT,
	max_dop BIGINT,
	avg_query_max_used_memory DECIMAL(38,2),
	last_query_max_used_memory DECIMAL(38,2),
	min_query_max_used_memory DECIMAL(38,2),
	max_query_max_used_memory DECIMAL(38,2),
	avg_rowcount DECIMAL(38,2),
	last_rowcount DECIMAL(38,2),
	min_rowcount DECIMAL(38,2),
	max_rowcount  DECIMAL(38,2),
	total_compile_duration AS avg_compile_duration * count_compiles,
	total_bind_duration AS avg_bind_duration * count_compiles,
	total_bind_cpu_time AS avg_bind_cpu_time * count_compiles,
	total_optimize_duration AS avg_optimize_duration * count_compiles,
	total_optimize_cpu_time AS avg_optimize_cpu_time * count_compiles,
	total_compile_memory_kb AS avg_compile_memory_kb * count_compiles,
	total_duration AS avg_duration * count_executions,
	total_cpu_time AS avg_cpu_time * count_executions,
	total_logical_io_reads AS avg_logical_io_reads * count_executions,
	total_logical_io_writes AS avg_logical_io_writes * count_executions,
	total_physical_io_reads AS avg_physical_io_reads * count_executions,
	total_clr_time AS avg_clr_time * count_executions,
	total_query_max_used_memory AS avg_query_max_used_memory * count_executions,
	total_rowcount AS avg_rowcount * count_executions,
	xpm AS NULLIF(count_executions, 0) / NULLIF(DATEDIFF(MINUTE, first_execution_time, last_execution_time), 0),
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


/*
This is where we store some additional metrics, along with the query plan and text
*/
DROP TABLE IF EXISTS #working_plan_text;

CREATE TABLE #working_plan_text (
	database_name NVARCHAR(256),
    plan_id BIGINT,
    query_id BIGINT,
	/*These are from query_store_plan*/
	plan_group_id BIGINT,
	engine_version NVARCHAR(64),
	compatibility_level INT,
	query_plan_hash BINARY(8),
	query_plan_xml XML,
	is_online_index_plan BIT,
	is_trivial_plan BIT,
	is_parallel_plan BIT,
	is_forced_plan BIT,
	is_natively_compiled BIT,
	force_failure_count BIGINT,
	last_force_failure_reason_desc NVARCHAR(256),
	count_compiles BIGINT,
	initial_compile_start_time DATETIME2,
	last_compile_start_time DATETIME2,
	last_execution_time DATETIME2,
	avg_compile_duration DECIMAL(38,2),
	last_compile_duration BIGINT,
	/*These are from query_store_query*/
	query_sql_text NVARCHAR(MAX),
	statement_sql_handle VARBINARY(64),
	is_part_of_encrypted_module BIT,
	has_restricted_text BIT,
	/*This is from query_context_settings*/
	context_settings NVARCHAR(512),
	/*This is from #working_plans*/
	pattern NVARCHAR(512)
	INDEX ix_ids CLUSTERED (plan_id, query_id)
); 


/*
This is where we store warnings that we generate from the XML and metrics
*/
DROP TABLE IF EXISTS #working_warnings;

CREATE TABLE #working_warnings(
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	proc_or_function_name NVARCHAR(256),
	plan_multiple_plans BIT,
    is_forced_plan BIT,
    is_forced_parameterized BIT,
    is_cursor BIT,
	is_optimistic_cursor BIT,
	is_forward_only_cursor BIT,
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
    long_running BIT,
    downlevel_estimator BIT,
    implicit_conversions BIT,
    tvf_estimate BIT,
    compile_timeout BIT,
    compile_memory_limit_exceeded BIT,
    warning_no_join_predicate BIT,
    query_cost FLOAT,
    missing_index_count INT,
    unmatched_index_count INT,
    is_trivial BIT,
	trace_flags_session NVARCHAR(1000),
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
	op_name NVARCHAR(100) NULL,
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
    warnings NVARCHAR(4000)
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


/*
The next three tables hold plan XML parsed out to different degrees 
*/
DROP TABLE IF EXISTS #statements;

CREATE TABLE #statements (
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	statement XML,
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


DROP TABLE IF EXISTS #query_plan;

CREATE TABLE #query_plan (
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	query_plan XML,
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


DROP TABLE IF EXISTS #relop;

CREATE TABLE #relop (
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	relop XML,
	INDEX ix_ids CLUSTERED (plan_id, query_id)
);


DROP TABLE IF EXISTS #plan_cost;

CREATE TABLE #plan_cost (
	query_plan_cost DECIMAL(18,2),
	sql_handle VARBINARY(64)
);


DROP TABLE IF EXISTS #stats_agg;

CREATE TABLE #stats_agg (
sql_handle VARBINARY(64),
last_update DATETIME2,
modification_count DECIMAL(38,2),
sampling_percent DECIMAL(38,2),
[statistics] NVARCHAR(256),
[table] NVARCHAR(256),
[schema] NVARCHAR(256),
[database] NVARCHAR(256)
);


DROP TABLE IF EXISTS #trace_flags;

CREATE TABLE #trace_flags (
	sql_handle VARBINARY(54),
	global_trace_flags NVARCHAR(4000),
	session_trace_flags NVARCHAR(4000)
);


DROP TABLE IF EXISTS #warning_results	

CREATE TABLE #warning_results (
    ID INT IDENTITY(1,1),
    CheckID INT,
    Priority TINYINT,
    FindingsGroup NVARCHAR(50),
    Finding NVARCHAR(200),
    URL NVARCHAR(200),
    Details NVARCHAR(4000) 
);

/*Sets up WHERE clause that gets used quite a bit*/

--Date stuff
--If they're both NULL, we'll just look at the last 7 days
IF (@StartDate IS NULL AND @EndDate IS NULL)
	BEGIN
	RAISERROR(N'@StartDate and @EndDate are NULL, checking last 7 days', 0, 1) WITH NOWAIT;
	SET @sql_where += ' AND qsrs.last_execution_time >= DATEADD(DAY, -7, DATEDIFF(DAY, 0, SYSDATETIME() ))
					  '
	END
--Hey, that's nice of me
IF @StartDate IS NOT NULL
	BEGIN 
	RAISERROR(N'Setting start date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time >= @sp_StartDate 
					   '
	END 
--Alright, sensible
IF @EndDate IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting end date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time < @sp_EndDate 
					   '
    END
--C'mon, why would you do that?
IF (@StartDate IS NULL AND @EndDate IS NOT NULL)
	BEGIN 
	RAISERROR(N'Setting reasonable start date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time < DATEADD(DAY, -7, @sp_EndDate) 
					   '
    END
--Jeez, abusive
IF (@StartDate IS NOT NULL AND @EndDate IS NULL)
	BEGIN 
	RAISERROR(N'Setting reasonable end date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time < DATEADD(DAY, 7, @sp_StartDate) 
					   '
    END

--I care about minimum execution counts
IF @MinimumExecutionCount IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting execution filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.count_executions >= @sp_MinimumExecutionCount 
					   '
    END

--You care about stored proc names
IF @StoredProcName IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting stored proc filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND object_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N')) = @sp_StoredProcName 
					   '
    END

--I will always love you, but hopefully this query will eventually end
IF @DurationFilter IS NOT NULL
    BEGIN 
	RAISERROR(N'Setting duration filter', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND (qsrs.avg_duration / 1000.) >= @sp_MinDuration 
					    ' 
	END 

--I don't know why you'd go looking for failed queries, but hey
IF (@Failed = 0 OR @Failed IS NULL)
    BEGIN 
	RAISERROR(N'Setting failed query filter to 0', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsrs.execution_type = 0 
					    ' 
	END 
IF (@Failed = 1)
    BEGIN 
	RAISERROR(N'Setting failed query filter to 3, 4', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsrs.execution_type IN (3, 4) 
					    ' 
	END  

--These query filters allow you to look for everything, statements, or stored procs
IF (LOWER(@QueryFilter) = N'all')
    BEGIN 
	RAISERROR(N'No query filter necessary filter', 0, 1) WITH NOWAIT;
	SET  @sql_where += N'' 
	END 
IF (LOWER(@QueryFilter) LIKE N'sta%')
    BEGIN 
	RAISERROR(N'Looking for statements only', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsq.object_id IS NULL
						  OR qsq.object_id = 0
					    ' 
	END 
IF (LOWER(@QueryFilter) LIKE N'pro%')
    BEGIN 
	RAISERROR(N'Looking for procs only', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND object_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N')) IS NOT NULL
					    ' 
	END 


IF @Debug = 1
	RAISERROR(N'Starting WHERE clause:', 0, 1) WITH NOWAIT;
	PRINT @sql_where

IF @ExportToExcel = 1	
	BEGIN
	RAISERROR(N'Exporting to Excel, hiding summary', 0, 1) WITH NOWAIT;
	SET @HideSummary = 1
	END

/*
This is our grouped interval query.

By default, it looks at queries: 
	In the last 7 days
	That aren't system queries
	That have a query plan (some won't, if nested level is > 128, along with other reasons)
	And haven't failed
	This stuff, along with some other options, will be configurable in the stored proc

*/

RAISERROR(N'Gathering intervals', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
SELECT   CONVERT(DATE, qsrs.last_execution_time) AS flate_date,
         MIN(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time), 0)) AS start_range,
         MAX(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time) + 1, 0)) AS end_range,
         SUM(qsrs.avg_duration / 1000.) / SUM(qsrs.count_executions) AS total_avg_duration_ms,
         SUM(qsrs.avg_cpu_time / 1000.) / SUM(qsrs.count_executions) AS total_avg_cpu_time_ms,
         SUM((qsrs.avg_logical_io_reads * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_reads_mb,
         SUM((qsrs.avg_physical_io_reads* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_physical_io_reads_mb,
         SUM((qsrs.avg_logical_io_writes* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_writes_mb,
         SUM(( qsrs.avg_query_max_used_memory * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_query_max_used_memory_mb,
         SUM(qsrs.avg_rowcount) AS total_rowcount,
         SUM(qsrs.count_executions) AS total_count_executions
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = qsrs.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
WHERE    1 = 1
       AND qsq.is_internal_query = 0
	   AND qsp.query_plan IS NOT NULL
	  '

SET @sql_select += @sql_where

SET @sql_select += 
			N'GROUP BY CONVERT(DATE, qsrs.last_execution_time)
					OPTION(RECOMPILE);
			'

IF @Debug = 1
	PRINT @sql_select

INSERT #grouped_interval WITH (TABLOCK)
		( flate_date, start_range, end_range, total_avg_duration_ms, 
		  total_avg_cpu_time_ms, total_avg_logical_io_reads_mb, total_avg_physical_io_reads_mb, 
		  total_avg_logical_io_writes_mb, total_avg_query_max_used_memory_mb, total_rowcount, total_count_executions )
EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;


/*
The next group of queries looks at plans in the ranges we found in the grouped interval query

We take the highest value from each metric (duration, cpu, etc) and find the top plans by that metric in the range

They insert into the #working_plans table
*/

/*Get longest duration plans*/

RAISERROR(N'Gathering longest duration plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH duration_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_duration_ms DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
         qsp.plan_id, qsp.query_id, ''duration''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     duration_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
	AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_duration DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*Get longest cpu plans*/

RAISERROR(N'Gathering highest cpu plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH cpu_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_cpu_time_ms DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''cpu''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     cpu_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_cpu_time DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*Get highest logical read plans*/

RAISERROR(N'Gathering highest logical read plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH logical_reads_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_logical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''logical reads''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     logical_reads_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_logical_io_reads DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*Get highest physical read plans*/

RAISERROR(N'Gathering highest physical read plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH physical_read_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_physical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''physical reads''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     physical_read_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_physical_io_reads DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;


/*Get highest logical write plans*/

RAISERROR(N'Gathering highest write plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH logical_writes_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_logical_io_writes_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''writes''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     logical_writes_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_logical_io_writes DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;


/*Get highest memory use plans*/

RAISERROR(N'Gathering highest memory use plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH memory_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_query_max_used_memory_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''memory''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     memory_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_query_max_used_memory DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;


/*Get highest memory use plans*/

RAISERROR(N'Gathering highest row count plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_rowcount DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''rows''
FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'ORDER BY qsrs.avg_rowcount DESC
					OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;


/*
This rolls up the different patterns we find before deduplicating.

The point of this is so we know if a query was gathered by one or more of the search queries

*/

RAISERROR(N'Updating patterns', 0, 1) WITH NOWAIT;

WITH patterns AS (
SELECT wp.plan_id, wp.query_id,
	   pattern_path = STUFF((SELECT DISTINCT N', ' + wp2.pattern
									FROM #working_plans AS wp2
									WHERE wp.plan_id = wp2.plan_id
									AND wp.query_id = wp2.query_id
									FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'')									
FROM #working_plans AS wp
)
UPDATE wp
SET wp.pattern = patterns.pattern_path
FROM #working_plans AS wp
JOIN patterns
ON  wp.plan_id = patterns.plan_id
AND wp.query_id = patterns.query_id
OPTION(RECOMPILE);


/*
This dedupes out results so we don't double-work the same plan
*/

RAISERROR(N'Deduplicating gathered plans', 0, 1) WITH NOWAIT;

WITH dedupe AS (
SELECT * , ROW_NUMBER() OVER (PARTITION BY wp.plan_id ORDER BY wp.plan_id) AS dupes
FROM #working_plans AS wp
)
DELETE dedupe
WHERE dedupe.dupes > 1
OPTION(RECOMPILE);



/*
This gathers data for the #working_metrics table
*/

RAISERROR(N'Collecting worker metrics', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
SELECT ' + QUOTENAME(@DatabaseName, '''') + N' AS database_name, wp.plan_id, wp.query_id,
       object_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N')) AS proc_or_function_name,
	   qsq.batch_sql_handle, qsq.query_hash, qsq.query_parameterization_type_desc, qsq.count_compiles, qsq.avg_compile_duration, 
	   qsq.last_compile_duration, qsq.avg_bind_duration, qsq.last_bind_duration, qsq.avg_bind_cpu_time, qsq.last_bind_cpu_time, 
	   qsq.avg_optimize_duration, qsq.last_optimize_duration, qsq.avg_optimize_cpu_time, qsq.last_optimize_cpu_time, qsq.avg_compile_memory_kb, 
	   qsq.last_compile_memory_kb, qsrs.execution_type_desc, qsrs.first_execution_time, qsrs.last_execution_time, qsrs.count_executions, 
	   qsrs.avg_duration, qsrs.last_duration, qsrs.min_duration, qsrs.max_duration, qsrs.avg_cpu_time, qsrs.last_cpu_time, qsrs.min_cpu_time, 
	   qsrs.max_cpu_time, qsrs.avg_logical_io_reads, qsrs.last_logical_io_reads, qsrs.min_logical_io_reads, qsrs.max_logical_io_reads, 
	   qsrs.avg_logical_io_writes, qsrs.last_logical_io_writes, qsrs.min_logical_io_writes, qsrs.max_logical_io_writes, qsrs.avg_physical_io_reads, 
	   qsrs.last_physical_io_reads, qsrs.min_physical_io_reads, qsrs.max_physical_io_reads, qsrs.avg_clr_time, qsrs.last_clr_time, qsrs.min_clr_time, 
	   qsrs.max_clr_time, qsrs.avg_dop, qsrs.last_dop, qsrs.min_dop, qsrs.max_dop, qsrs.avg_query_max_used_memory, qsrs.last_query_max_used_memory, 
	   qsrs.min_query_max_used_memory, qsrs.max_query_max_used_memory, qsrs.avg_rowcount, qsrs.last_rowcount, qsrs.min_rowcount, qsrs.max_rowcount
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

INSERT #working_metrics WITH (TABLOCK)
		( database_name, plan_id, query_id, 
		  proc_or_function_name, 
		  batch_sql_handle, query_hash, query_parameterization_type_desc, count_compiles, 
		  avg_compile_duration, last_compile_duration, avg_bind_duration, last_bind_duration, avg_bind_cpu_time, last_bind_cpu_time, avg_optimize_duration, 
		  last_optimize_duration, avg_optimize_cpu_time, last_optimize_cpu_time, avg_compile_memory_kb, last_compile_memory_kb, execution_type_desc, 
		  first_execution_time, last_execution_time, count_executions, avg_duration, last_duration, min_duration, max_duration, avg_cpu_time, last_cpu_time, 
		  min_cpu_time, max_cpu_time, avg_logical_io_reads, last_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, 
		  last_logical_io_writes, min_logical_io_writes, max_logical_io_writes, avg_physical_io_reads, last_physical_io_reads, min_physical_io_reads, 
		  max_physical_io_reads, avg_clr_time, last_clr_time, min_clr_time, max_clr_time, avg_dop, last_dop, min_dop, max_dop, avg_query_max_used_memory, 
		  last_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory, avg_rowcount, last_rowcount, min_rowcount, max_rowcount )

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*
This gathers data for the #working_plan_text table
*/

RAISERROR(N'Gathering working plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
SELECT ' + QUOTENAME(@DatabaseName, '''') + N' AS database_name,  wp.plan_id, wp.query_id,
	   qsp.plan_group_id, qsp.engine_version, qsp.compatibility_level, qsp.query_plan_hash, TRY_CONVERT(XML, qsp.query_plan), qsp.is_online_index_plan, qsp.is_trivial_plan, 
	   qsp.is_parallel_plan, qsp.is_forced_plan, qsp.is_natively_compiled, qsp.force_failure_count, qsp.last_force_failure_reason_desc, qsp.count_compiles, 
	   qsp.initial_compile_start_time, qsp.last_compile_start_time, qsp.last_execution_time, qsp.avg_compile_duration, qsp.last_compile_duration, 
	   qsqt.query_sql_text, qsqt.statement_sql_handle, qsqt.is_part_of_encrypted_module, qsqt.has_restricted_text
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

INSERT #working_plan_text WITH (TABLOCK)
		( database_name, plan_id, query_id, 
		  plan_group_id, engine_version, compatibility_level, query_plan_hash, query_plan_xml, is_online_index_plan, is_trivial_plan, 
		  is_parallel_plan, is_forced_plan, is_natively_compiled, force_failure_count, last_force_failure_reason_desc, count_compiles, 
		  initial_compile_start_time, last_compile_start_time, last_execution_time, avg_compile_duration, last_compile_duration, 
		  query_sql_text, statement_sql_handle, is_part_of_encrypted_module, has_restricted_text )

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*
This gets us context settings for our queries and adds it to the #working_plan_text table
*/

RAISERROR(N'Gathering context settings', 0, 1) WITH NOWAIT;

UPDATE wp
SET wp.context_settings = SUBSTRING(
					    CASE WHEN (CAST(qcs.set_options AS INT) & 1 = 1) THEN ', ANSI_PADDING' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8 = 8) THEN ', CONCAT_NULL_YIELDS_NULL' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 16 = 16) THEN ', ANSI_WARNINGS' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 32 = 32) THEN ', ANSI_NULLS' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 64 = 64) THEN ', QUOTED_IDENTIFIER' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 4096 = 4096) THEN ', ARITH_ABORT' ELSE '' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8192 = 8191) THEN ', NUMERIC_ROUNDABORT' ELSE '' END 
					    , 2, 200000)
FROM #working_plan_text wp
JOIN   StackOverflow.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   StackOverflow.sys.query_context_settings AS qcs
ON qcs.context_settings_id = qsq.context_settings_id
OPTION(RECOMPILE);


/*
This adds the patterns we found from each interval to the #working_plan_text table
*/

RAISERROR(N'Add patterns to working plans', 0, 1) WITH NOWAIT;

UPDATE wpt
SET wpt.pattern = wp.pattern
FROM #working_plans AS wp
JOIN #working_plan_text AS wpt
ON wpt.plan_id = wp.plan_id
AND wpt.query_id = wp.query_id
OPTION(RECOMPILE);

/*This cleans up query text a bit*/

RAISERROR(N'Clean awkward characters from query text', 0, 1) WITH NOWAIT;

UPDATE b
SET b.query_sql_text = REPLACE(REPLACE(REPLACE(query_sql_text, @cr, ' '), @lf, ' '), @tab, '  ')
FROM #working_plan_text AS b

IF (@SkipXML = 0)
BEGIN

/*
This sets up the #working_warnings table with the IDs we're interested in so we can tie warnings back to them 
*/

RAISERROR(N'Populate working warnings table with gathered plans', 0, 1) WITH NOWAIT;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
SELECT wp.plan_id, wp.query_id, qsq.query_hash, qsqt.statement_sql_handle, wm.proc_or_function_name
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
JOIN #working_metrics wm
ON wm.plan_id = wp.plan_id
   AND wm.query_id = wp.query_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select +=  N'OPTION(RECOMPILE);
					'

IF @Debug = 1
	PRINT @sql_select

INSERT #working_warnings  WITH (TABLOCK)
	( plan_id, query_id, query_hash, sql_handle, proc_or_function_name )
EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*
This looks for queries in the query stores that we picked up from an internal that have multiple plans in cache

This and several of the following queries all replaced XML parsing to find plan attributes. Sweet.

Thanks, Query Store
*/

RAISERROR(N'Checking for multiple plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
SET @sql_select += N'
UPDATE ww
SET ww.plan_multiple_plans = 1
FROM #working_warnings AS ww
JOIN 
(
SELECT wp.query_id, COUNT(qsp.plan_id) AS  plans
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	AND qsqt.query_sql_text NOT LIKE ''(@_msparam_0%''
	'

SET @sql_select += @sql_where

SET @sql_select += N'GROUP BY wp.query_id
					HAVING COUNT(qsp.plan_id) > 1
					) AS x
					ON ww.query_id = x.query_id
					OPTION(RECOMPILE);
					'
IF @Debug = 1
	PRINT @sql_select

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName;

/*
This looks for forced plans
*/

RAISERROR(N'Checking for forced plans', 0, 1) WITH NOWAIT;

UPDATE ww
SET    ww.is_forced_plan = 1
FROM   #working_warnings AS ww
JOIN   #working_plan_text AS wp
ON ww.plan_id = wp.plan_id
   AND ww.query_id = wp.query_id
   AND wp.is_forced_plan = 1
OPTION(RECOMPILE);


/*
This looks for forced parameterization
*/

RAISERROR(N'Checking for forced parameterization', 0, 1) WITH NOWAIT;

UPDATE ww
SET    ww.is_forced_parameterized = 1
FROM   #working_warnings AS ww
JOIN   #working_metrics AS wm
ON ww.plan_id = wm.plan_id
   AND ww.query_id = wm.query_id
   AND wm.query_parameterization_type_desc = 'Forced'
OPTION(RECOMPILE);


/*
This looks for unparameterized queries
*/

RAISERROR(N'Checking for unparameterized plans', 0, 1) WITH NOWAIT;

UPDATE ww
SET    ww.unparameterized_query = 1
FROM   #working_warnings AS ww
JOIN   #working_metrics AS wm
ON ww.plan_id = wm.plan_id
   AND ww.query_id = wm.query_id
   AND wm.query_parameterization_type_desc = 'None'
   AND ww.proc_or_function_name IS NOT NULL
OPTION(RECOMPILE);


/*
This looks for cursors
*/
UPDATE ww
SET    ww.is_cursor = 1
FROM   #working_warnings AS ww
JOIN   #working_plan_text AS wp
ON ww.plan_id = wp.plan_id
   AND ww.query_id = wp.query_id
   AND wp.plan_group_id > 0
OPTION(RECOMPILE);


/*
This looks for parallel plans
*/
UPDATE ww
SET    ww.is_parallel = 1
FROM   #working_warnings AS ww
JOIN   #working_plan_text AS wp
ON ww.plan_id = wp.plan_id
   AND ww.query_id = wp.query_id
   AND wp.is_parallel_plan = 1
OPTION(RECOMPILE);

/*This looks for old CE*/
UPDATE w
SET w.downlevel_estimator = 1
FROM #working_warnings AS w
JOIN #working_plan_text AS wpt
ON w.plan_id = wpt.plan_id
AND w.query_id = wpt.query_id
/*PLEASE DON'T TELL ANYONE I DID THIS*/
WHERE PARSENAME(wpt.engine_version, 4) < PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4);
/*NO SERIOUSLY THIS IS A HORRIBLE IDEA*/


/*This looks for trivial plans*/
UPDATE w
SET w.is_trivial = 1
FROM #working_warnings AS w
JOIN #working_plan_text AS wpt
ON w.plan_id = wpt.plan_id
AND w.query_id = wpt.query_id
AND wpt.is_trivial_plan = 1;



/*
This parses the XML from our top plans into smaller chunks for easier consumption
*/
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, statement )	
	SELECT ww.plan_id, ww.query_id, ww.query_hash, ww.sql_handle, q.n.query('.') AS statement
	FROM #working_warnings AS ww
	JOIN #working_plan_text AS wp
	ON ww.plan_id = wp.plan_id
	AND ww.query_id = wp.query_id
    CROSS APPLY wp.query_plan_xml.nodes('//p:StmtSimple') AS q(n) 
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, statement )
	SELECT ww.plan_id, ww.query_id, ww.query_hash, ww.sql_handle, q.n.query('.') AS statement
	FROM #working_warnings AS ww
	JOIN #working_plan_text AS wp
	ON ww.plan_id = wp.plan_id
	AND ww.query_id = wp.query_id
    CROSS APPLY wp.query_plan_xml.nodes('//p:StmtCursor') AS q(n) 
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #query_plan WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, query_plan )
SELECT  s.plan_id, s.query_id, s.query_hash, s.sql_handle, q.n.query('.') AS query_plan
FROM    #statements AS s
        CROSS APPLY s.statement.nodes('//p:QueryPlan') AS q(n) 
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #relop WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, relop)
SELECT  qp.plan_id, qp.query_id, qp.query_hash, qp.sql_handle, q.n.query('.') AS relop
FROM    #query_plan qp
        CROSS APPLY qp.query_plan.nodes('//p:RelOp') AS q(n) 
OPTION (RECOMPILE) ;


-- statement level checks

RAISERROR(N'Performing compile timeout checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_timeout = 1 
FROM    #statements s
JOIN #working_warnings AS b
ON  s.query_hash = b.query_hash
WHERE statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
OPTION (RECOMPILE);


RAISERROR(N'Performing compile memory limit exceeded checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     compile_memory_limit_exceeded = 1 
FROM    #statements s
JOIN #working_warnings AS b
ON  s.query_hash = b.query_hash
WHERE statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1
OPTION (RECOMPILE);


RAISERROR(N'Performing index DML checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
index_dml AS (
	SELECT	s.query_hash,	
			index_dml = CASE WHEN statement.exist('//p:StmtSimple/@StatementType[.="CREATE INDEX"]') = 1 THEN 1
							 WHEN statement.exist('//p:StmtSimple/@StatementType[.="DROP INDEX"]') = 1 THEN 1
							 END
	FROM    #statements s
			)
	UPDATE b
		SET b.index_dml = i.index_dml
	FROM #working_warnings AS b
	JOIN index_dml i
	ON i.query_hash = b.query_hash
	WHERE i.index_dml = 1
	OPTION (RECOMPILE);

RAISERROR(N'Performing table DML checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
table_dml AS (
	SELECT s.query_hash,			
		   table_dml = CASE WHEN statement.exist('//p:StmtSimple/@StatementType[.="CREATE TABLE"]') = 1 THEN 1
							WHEN statement.exist('//p:StmtSimple/@StatementType[.="DROP OBJECT"]') = 1 THEN 1
							END
		 FROM #statements AS s
		 )
	UPDATE b
		SET b.table_dml = t.table_dml
	FROM #working_warnings AS b
	JOIN table_dml t
	ON t.query_hash = b.query_hash
	WHERE t.table_dml = 1
	OPTION (RECOMPILE);


/*Begin plan cost calculations*/
RAISERROR(N'Gathering statement costs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #plan_cost WITH (TABLOCK)
	( query_plan_cost, sql_handle )
SELECT  DISTINCT
		statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') query_plan_cost,
		s.sql_handle
FROM    #statements s
OUTER APPLY s.statement.nodes('/p:StmtSimple') AS q(n)
WHERE statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') > 0
OPTION (RECOMPILE);


RAISERROR(N'Updating statement costs', 0, 1) WITH NOWAIT;
WITH pc AS (
	SELECT SUM(DISTINCT pc.query_plan_cost) AS queryplancostsum, pc.sql_handle
	FROM #plan_cost AS pc
	GROUP BY pc.sql_handle
	)
	UPDATE b
		SET b.query_cost = ISNULL(pc.queryplancostsum, 0)
		FROM  #working_warnings AS b
		JOIN pc
		ON pc.sql_handle = b.sql_handle
	OPTION (RECOMPILE);


/*End plan cost calculations*/

RAISERROR(N'Checking for plan warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET plan_warnings = 1
FROM    #query_plan qp
JOIN #working_warnings b
ON  qp.sql_handle = b.sql_handle
AND query_plan.exist('/p:QueryPlan/p:Warnings') = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking for implicit conversion', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET b.implicit_conversions = 1
FROM    #query_plan qp
JOIN #working_warnings b
ON  qp.sql_handle = b.sql_handle
AND query_plan.exist('/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') = 1
OPTION (RECOMPILE);

RAISERROR(N'Checking for operator warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.sql_handle,
	   c.n.exist('//p:Warnings[(@NoJoinPredicate[.="1"])]') AS warning_no_join_predicate,
	   c.n.exist('//p:ColumnsWithNoStatistics') AS no_stats_warning ,
	   c.n.exist('//p:Warnings') AS relop_warnings
FROM #relop AS r
CROSS APPLY r.relop.nodes('/p:RelOp/p:Warnings') AS c(n)
)
UPDATE b
SET	   b.warning_no_join_predicate = x.warning_no_join_predicate,
	   b.no_stats_warning = x.no_stats_warning,
	   b.relop_warnings = x.relop_warnings
FROM #working_warnings b
JOIN x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);

RAISERROR(N'Checking for table variables', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.sql_handle,
	   c.n.value('substring(@Table, 2, 1)','VARCHAR(100)') AS first_char
FROM   #relop r
CROSS APPLY r.relop.nodes('//p:Object') AS c(n)
)
UPDATE b
SET	   b.is_table_variable = CASE WHEN x.first_char = '@' THEN 1 END
FROM #working_warnings b
JOIN x ON x.sql_handle = b.sql_handle
JOIN #working_metrics AS wm
ON b.plan_id = wm.plan_id
AND b.query_id = wm.query_id
AND wm.batch_sql_handle IS NOT NULL
OPTION (RECOMPILE);


RAISERROR(N'Checking for functions', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.sql_handle,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS function_count,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[@IsClrFunction = "1"]))', 'INT') AS clr_function_count
FROM   #relop r
CROSS APPLY r.relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
)
UPDATE b
SET	   b.function_count = x.function_count,
	   b.clr_function_count = x.clr_function_count
FROM #working_warnings b
JOIN x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);

RAISERROR(N'Checking for expensive key lookups', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.key_lookup_cost = x.key_lookup_cost
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float') AS key_lookup_cost
FROM   #relop r
WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE) ;

RAISERROR(N'Checking for expensive remote queries', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET remote_query_cost = x.remote_query_cost
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float') AS remote_query_cost
FROM   #relop r
WHERE [relop].exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE) ;

RAISERROR(N'Checking for expensive sorts', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET sort_cost = (x.sort_io + x.sort_cpu) 
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   relop.value('sum(/p:RelOp/@EstimateIO)', 'float') AS sort_io,
	   relop.value('sum(/p:RelOp/@EstimateCPU)', 'float') AS sort_cpu
FROM   #relop r
WHERE [relop].exist('/p:RelOp[(@PhysicalOp[.="Sort"])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE) ;

RAISERROR(N'Checking for icky cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_optimistic_cursor =  CASE WHEN n.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1 THEN 1 END,
	b.is_forward_only_cursor = CASE WHEN n.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1 THEN 1 ELSE 0 END
FROM #working_warnings b
JOIN #statements AS s
ON b.sql_handle = s.sql_handle
AND b.is_cursor = 1
CROSS APPLY s.statement.nodes('/p:StmtCursor') AS n(fn)
OPTION (RECOMPILE) ;


RAISERROR(N'Checking for bad scans and plan forcing', 0, 1) WITH NOWAIT;
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET 
b.is_table_scan = x.is_table_scan,
b.backwards_scan = x.backwards_scan,
b.forced_index = x.forced_index,
b.forced_seek = x.forced_seek,
b.forced_scan = x.forced_scan
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   0 AS is_table_scan,
	   q.n.exist('@ScanDirection[.="BACKWARD"]') AS backwards_scan,
	   q.n.value('@ForcedIndex', 'bit') AS forced_index,
	   q.n.value('@ForceSeek', 'bit') AS forced_seek,
	   q.n.value('@ForceScan', 'bit') AS forced_scan
FROM   #relop r
CROSS APPLY r.relop.nodes('//p:IndexScan') AS q(n)
UNION ALL
SELECT 
       r.sql_handle,
	   1 AS is_table_scan,
	   q.n.exist('@ScanDirection[.="BACKWARD"]') AS backwards_scan,
	   q.n.value('@ForcedIndex', 'bit') AS forced_index,
	   q.n.value('@ForceSeek', 'bit') AS forced_seek,
	   q.n.value('@ForceScan', 'bit') AS forced_scan
FROM   #relop r
CROSS APPLY r.relop.nodes('//p:TableScan') AS q(n)
) AS x ON b.sql_handle = x.sql_handle
OPTION (RECOMPILE);


RAISERROR(N'Checking for computed columns that reference scalar UDFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_computed_scalar = x.computed_column_function
FROM #working_warnings b
JOIN (
SELECT r.sql_handle,
	   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS computed_column_function
FROM   #relop r
CROSS APPLY r.relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
WHERE n.fn.exist('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ColumnReference[(@ComputedColumn[.="1"])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);


RAISERROR(N'Checking for filters that reference scalar UDFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_computed_filter = x.filter_function
FROM #working_warnings b
JOIN (
SELECT 
r.sql_handle, 
c.n.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))', 'INT') AS filter_function
FROM #relop AS r
CROSS APPLY r.relop.nodes('/p:RelOp/p:Filter/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator/p:UserDefinedFunction') c(n) 
) x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);

RAISERROR(N'Checking modification queries that hit lots of indexes', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),	
IndexOps AS 
(
	SELECT 
	r.sql_handle,
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
		SELECT	ios.sql_handle,
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
		GROUP BY ios.sql_handle) 
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
FROM #working_warnings AS b
JOIN iops ON  iops.sql_handle = b.sql_handle
OPTION(RECOMPILE);

RAISERROR(N'Checking for Spatial index use', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_spatial = x.is_spatial
FROM #working_warnings AS b
JOIN (
SELECT r.sql_handle,
	   1 AS is_spatial
FROM   #relop r
CROSS APPLY r.relop.nodes('/p:RelOp//p:Object') n(fn)
WHERE n.fn.exist('(@IndexKind[.="Spatial"])') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);


RAISERROR(N'Checking for forced serialization', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET b.is_forced_serial = 1
FROM #query_plan qp
JOIN #working_warnings AS b
ON    qp.sql_handle = b.sql_handle
AND b.is_parallel IS NULL
AND qp.query_plan.exist('/p:QueryPlan/@NonParallelPlanReason') = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET columnstore_row_mode = x.is_row_mode
FROM #working_warnings AS b
JOIN (
SELECT 
       r.sql_handle,
	   relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') AS is_row_mode
FROM   #relop r
WHERE [relop].exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE) ;


RAISERROR('Checking for row level security only', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET  b.is_row_level = 1
FROM #working_warnings b
JOIN #statements s 
ON s.query_hash = b.query_hash 
WHERE statement.exist('/p:StmtSimple/@SecurityPolicyApplied[.="true"]') = 1
OPTION (RECOMPILE) ;

IF (PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)) >= 14

BEGIN

RAISERROR('Gathering stats information', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #stats_agg WITH (TABLOCK)
	(sql_handle, last_update, modification_count, sampling_percent, [statistics], [table], [schema], [database])
SELECT qp.sql_handle,
	   x.c.value('@LastUpdate', 'DATETIME2(7)') AS LastUpdate,
	   x.c.value('@ModificationCount', 'INT') AS ModificationCount,
	   x.c.value('@SamplingPercent', 'FLOAT') AS SamplingPercent,
	   x.c.value('@Statistics', 'NVARCHAR(256)') AS [Statistics], 
	   x.c.value('@Table', 'NVARCHAR(256)') AS [Table], 
	   x.c.value('@Schema', 'NVARCHAR(256)') AS [Schema], 
	   x.c.value('@Database', 'NVARCHAR(256)') AS [Database]
FROM #query_plan AS qp
CROSS APPLY qp.query_plan.nodes('//p:OptimizerStatsUsage/p:StatisticsInfo') x (c)


RAISERROR('Checking for stale stats', 0, 1) WITH NOWAIT;
WITH  stale_stats AS (
	SELECT sa.sql_handle
	FROM #stats_agg AS sa
	GROUP BY sa.sql_handle
	HAVING MAX(sa.last_update) <= DATEADD(DAY, -7, SYSDATETIME())
	AND AVG(sa.modification_count) >= 100000
)
UPDATE b
SET b.stale_stats = 1
FROM #working_warnings AS b
JOIN stale_stats os
ON b.sql_handle = os.sql_handle
OPTION (RECOMPILE) ;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
aj AS (
	SELECT r.sql_handle
	FROM #relop AS r
	CROSS APPLY r.relop.nodes('//p:RelOp') x(c)
	WHERE x.c.exist('@IsAdaptive[.=1]') = 1
)
UPDATE b
SET b.is_adaptive = 1
FROM #working_warnings AS b
JOIN aj
ON b.sql_handle = aj.sql_handle
OPTION (RECOMPILE);

RAISERROR(N'Performing query level checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET     missing_index_count = query_plan.value('count(//p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int') ,
		unmatched_index_count = query_plan.value('count(//p:QueryPlan/p:UnmatchedIndexes/p:Parameterization/p:Object)', 'int') 
FROM    #query_plan qp
JOIN #working_warnings AS b
ON b.query_hash = qp.query_hash
OPTION (RECOMPILE);

END 

RAISERROR(N'Filling in object nam column where NULL', 0, 1) WITH NOWAIT;
UPDATE wm
SET wm.proc_or_function_name = N'Statement'
FROM #working_metrics AS wm
WHERE proc_or_function_name IS NULL

RAISERROR(N'Trace flag checks', 0, 1) WITH NOWAIT;
;WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, tf_pretty AS (
SELECT  qp.sql_handle,
		q.n.value('@Value', 'INT') AS trace_flag,
		q.n.value('@Scope', 'VARCHAR(10)') AS scope
FROM    #query_plan qp
CROSS APPLY qp.query_plan.nodes('/p:QueryPlan/p:TraceFlags/p:TraceFlag') AS q(n)
)
INSERT #trace_flags WITH (TABLOCK)
		(sql_handle, global_trace_flags, session_trace_flags )
SELECT DISTINCT tf1.sql_handle ,
    STUFF((
          SELECT DISTINCT ', ' + CONVERT(VARCHAR(5), tf2.trace_flag)
          FROM  tf_pretty AS tf2 
          WHERE tf1.sql_handle = tf2.sql_handle 
		  AND tf2.scope = 'Global'
        FOR XML PATH(N'')), 1, 2, N''
      ) AS global_trace_flags,
    STUFF((
          SELECT DISTINCT ', ' + CONVERT(VARCHAR(5), tf2.trace_flag)
          FROM  tf_pretty AS tf2 
          WHERE tf1.sql_handle = tf2.sql_handle 
		  AND tf2.scope = 'Session'
        FOR XML PATH(N'')), 1, 2, N''
      ) AS session_trace_flags
FROM tf_pretty AS tf1
OPTION (RECOMPILE);

UPDATE b
SET    b.trace_flags_session = tf.session_trace_flags
FROM   #working_warnings AS b
JOIN #trace_flags tf 
ON tf.sql_handle = b.sql_handle 
OPTION(RECOMPILE);

RAISERROR(N'This dumb thing', 0, 1) WITH NOWAIT;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET    frequent_execution = CASE WHEN wm.xpm > @execution_threshold THEN 1 END ,
	   near_parallel = CASE WHEN b.query_cost BETWEEN @ctp * (1 - (@ctp_threshold_pct / 100.0)) AND @ctp THEN 1 END,
	   long_running = CASE WHEN wm.avg_duration > @long_running_query_warning_seconds THEN 1
						   WHEN wm.max_duration > @long_running_query_warning_seconds THEN 1
                           WHEN wm.avg_cpu_time > @long_running_query_warning_seconds THEN 1
                           WHEN wm.max_cpu_time > @long_running_query_warning_seconds THEN 1 END,
	   is_key_lookup_expensive = CASE WHEN b.query_cost > (@ctp / 2) AND key_lookup_cost >= b.query_cost * .5 THEN 1 END,
	   is_sort_expensive = CASE WHEN b.query_cost > (@ctp / 2) AND sort_cost >= b.query_cost * .5 THEN 1 END,
	   is_remote_query_expensive = CASE WHEN remote_query_cost >= b.query_cost * .05 THEN 1 END,
	   long_running_low_cpu = CASE WHEN wm.avg_duration > wm.avg_cpu_time * 4 THEN 1 END,
	   low_cost_high_cpu = CASE WHEN b.query_cost < @ctp AND wm.avg_cpu_time > 500. AND b.query_cost * 10 < wm.avg_cpu_time THEN 1 END
FROM #working_warnings AS b
JOIN #working_metrics AS wm
ON b.plan_id = wm.plan_id
AND b.query_id = wm.query_id
OPTION (RECOMPILE) ;

END

UPDATE b
SET parameter_sniffing_symptoms = 
	SUBSTRING(  
				/*Duration*/
				CASE WHEN (min_duration / 1000.) * 10000 < (avg_duration / 1000.) THEN ', Fast sometimes' ELSE '' END +
				CASE WHEN (max_duration / 1000.) > (avg_duration / 1000.) * 10000 THEN ', Slow sometimes' ELSE '' END +
				CASE WHEN (last_duration / 1000.) * 10000 < (avg_duration / 1000.)  THEN ', Fast last run' ELSE '' END +
				CASE WHEN (last_duration / 1000.) > (avg_duration / 1000.) * 10000 THEN ', Slow last run' ELSE '' END +
				/*CPU*/
				CASE WHEN (min_cpu_time / 1000. / avg_dop) * 10000 < (avg_cpu_time / 1000. / avg_dop) THEN ', Low CPU sometimes' ELSE '' END +
				CASE WHEN (max_cpu_time / 1000. / max_dop) > (avg_cpu_time / 1000. / avg_dop) * 10000 THEN ', High CPU sometimes' ELSE '' END +
				CASE WHEN (last_cpu_time / 1000. / last_dop) * 10000 < (avg_cpu_time / 1000. / avg_dop)  THEN ', Low CPU last run' ELSE '' END +
				CASE WHEN (last_cpu_time / 1000. / last_dop) > (avg_cpu_time / 1000. / avg_dop) * 10000 THEN ', High CPU last run' ELSE '' END +
				/*Logical Reads*/
				CASE WHEN (min_logical_io_reads * 8 / 1024.) * 10000 < (avg_logical_io_reads * 8 / 1024.) THEN ', Low reads sometimes' ELSE '' END +
				CASE WHEN (max_logical_io_reads * 8 / 1024.) > (avg_logical_io_reads * 8 / 1024.) * 10000 THEN ', High reads sometimes' ELSE '' END +
				CASE WHEN (last_logical_io_reads * 8 / 1024.) * 10000 < (avg_logical_io_reads * 8 / 1024.)  THEN ', Low reads last run' ELSE '' END +
				CASE WHEN (last_logical_io_reads * 8 / 1024.) > (avg_logical_io_reads * 8 / 1024.) * 10000 THEN ', High reads last run' ELSE '' END +
				/*Logical Writes*/
				CASE WHEN (min_logical_io_writes * 8 / 1024.) * 10000 < (avg_logical_io_writes * 8 / 1024.) THEN ', Low writes sometimes' ELSE '' END +
				CASE WHEN (max_logical_io_writes * 8 / 1024.) > (avg_logical_io_writes * 8 / 1024.) * 10000 THEN ', High writes sometimes' ELSE '' END +
				CASE WHEN (last_logical_io_writes * 8 / 1024.) * 10000 < (avg_logical_io_writes * 8 / 1024.)  THEN ', Low writes last run' ELSE '' END +
				CASE WHEN (last_logical_io_writes * 8 / 1024.) > (avg_logical_io_writes * 8 / 1024.) * 10000 THEN ', High writes last run' ELSE '' END +
				/*Physical Reads*/
				CASE WHEN (min_physical_io_reads * 8 / 1024.) * 10000 < (avg_physical_io_reads * 8 / 1024.) THEN ', Low physical reads sometimes' ELSE '' END +
				CASE WHEN (max_physical_io_reads * 8 / 1024.) > (avg_physical_io_reads * 8 / 1024.) * 10000 THEN ', High physical reads sometimes' ELSE '' END +
				CASE WHEN (last_physical_io_reads * 8 / 1024.) * 10000 < (avg_physical_io_reads * 8 / 1024.)  THEN ', Low physical reads last run' ELSE '' END +
				CASE WHEN (last_physical_io_reads * 8 / 1024.) > (avg_physical_io_reads * 8 / 1024.) * 10000 THEN ', High physical reads last run' ELSE '' END +
				/*Memory*/
				CASE WHEN (min_query_max_used_memory * 8 / 1024.) * 10000 < (avg_query_max_used_memory * 8 / 1024.) THEN ', Low memory sometimes' ELSE '' END +
				CASE WHEN (max_query_max_used_memory * 8 / 1024.) > (avg_query_max_used_memory * 8 / 1024.) * 10000 THEN ', High memory sometimes' ELSE '' END +
				CASE WHEN (last_query_max_used_memory * 8 / 1024.) * 10000 < (avg_query_max_used_memory * 8 / 1024.)  THEN ', Low memory last run' ELSE '' END +
				CASE WHEN (last_query_max_used_memory * 8 / 1024.) > (avg_query_max_used_memory * 8 / 1024.) * 10000 THEN ', High memory last run' ELSE '' END +
				/*Duration*/
				CASE WHEN min_rowcount  * 10000 < avg_rowcount THEN ', Fast sometimes' ELSE '' END +
				CASE WHEN max_rowcount  > avg_rowcount * 10000 THEN ', Slow sometimes' ELSE '' END +
				CASE WHEN last_rowcount * 10000 < avg_rowcount  THEN ', Fast last run' ELSE '' END +
				CASE WHEN last_rowcount > avg_rowcount * 10000 THEN ', Slow last run' ELSE '' END +
				/*DOP*/
				CASE WHEN min_dop = 1 THEN ', Serial sometimes' ELSE '' END +
				CASE WHEN max_dop  > 1 THEN ', Parallel sometimes' ELSE '' END +
				CASE WHEN last_dop = 1  THEN ', Serial last run' ELSE '' END +
				CASE WHEN last_dop > 1 THEN ', Parallel last run' ELSE '' END 
	, 2, 200000) 
FROM #working_metrics AS b


IF @SkipXML = 0
BEGIN 

RAISERROR('Populating Warnings column', 0, 1) WITH NOWAIT;
/* Populate warnings */
UPDATE b
SET    b.warnings = SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN ', No Join Predicate' ELSE '' END +
                  CASE WHEN compile_timeout = 1 THEN ', Compilation Timeout' ELSE '' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN ', Compile Memory Limit Exceeded' ELSE '' END +
                  CASE WHEN is_forced_plan = 1 THEN ', Forced Plan' ELSE '' END +
                  CASE WHEN is_forced_parameterized = 1 THEN ', Forced Parameterization' ELSE '' END +
                  CASE WHEN unparameterized_query = 1 THEN ', Unparameterized Query' ELSE '' END +
                  CASE WHEN missing_index_count > 0 THEN ', Missing Indexes (' + CAST(missing_index_count AS NVARCHAR(3)) + ')' ELSE '' END +
                  CASE WHEN unmatched_index_count > 0 THEN ', Unmatched Indexes (' + CAST(unmatched_index_count AS NVARCHAR(3)) + ')' ELSE '' END +                  
                  CASE WHEN is_cursor = 1 THEN ', Cursor' 
							+ CASE WHEN is_optimistic_cursor = 1 THEN ' with optimistic' ELSE '' END
							+ CASE WHEN is_forward_only_cursor = 0 THEN ' not forward only' ELSE '' END							
				  ELSE '' END +
                  CASE WHEN is_parallel = 1 THEN ', Parallel' ELSE '' END +
                  CASE WHEN near_parallel = 1 THEN ', Nearly Parallel' ELSE '' END +
                  CASE WHEN frequent_execution = 1 THEN ', Frequent Execution' ELSE '' END +
                  CASE WHEN plan_warnings = 1 THEN ', Plan Warnings' ELSE '' END +
                  CASE WHEN parameter_sniffing = 1 THEN ', Parameter Sniffing' ELSE '' END +
                  CASE WHEN long_running = 1 THEN ', Long Running Query' ELSE '' END +
                  CASE WHEN downlevel_estimator = 1 THEN ', Downlevel CE' ELSE '' END +
                  CASE WHEN implicit_conversions = 1 THEN ', Implicit Conversions' ELSE '' END +
                  CASE WHEN plan_multiple_plans = 1 THEN ', Multiple Plans' ELSE '' END +
                  CASE WHEN is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session ELSE '' END +
				  CASE WHEN is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), function_count) + ' function(s)' ELSE '' END + 
				  CASE WHEN clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), clr_function_count) + ' CLR function(s)' ELSE '' END + 
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
				  CASE WHEN is_adaptive = 1 THEN + ', Adaptive Joins' ELSE '' END				   
                  , 2, 200000) 
FROM #working_warnings b
OPTION (RECOMPILE) ;

 
RAISERROR('Checking for plans with no warnings', 0, 1) WITH NOWAIT;	

UPDATE b
SET b.warnings = 'No warnings detected.'
FROM #working_warnings AS b
WHERE b.warnings = '' OR b.warnings IS NULL
OPTION (RECOMPILE);
END

IF (@Failed = 0 AND @ExportToExcel = 0 AND @SkipXML = 0)
BEGIN

SELECT wpt.database_name, ww.query_cost, wpt.query_sql_text, wm.proc_or_function_name, wpt.query_plan_xml, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.min_query_max_used_memory, wm.max_query_max_used_memory,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
ON wpt.plan_id = ww.plan_id
AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
ON wpt.plan_id = wm.plan_id
AND wpt.query_id = wm.query_id
ORDER BY ww.query_cost DESC
OPTION(RECOMPILE);

END

IF (@Failed = 1 AND @ExportToExcel = 0 AND @SkipXML = 0)
BEGIN

SELECT wpt.database_name, ww.query_cost, wpt.query_sql_text, wm.proc_or_function_name, wpt.query_plan_xml, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.last_force_failure_reason_desc, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.min_query_max_used_memory, wm.max_query_max_used_memory,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
ON wpt.plan_id = ww.plan_id
AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
ON wpt.plan_id = wm.plan_id
AND wpt.query_id = wm.query_id
ORDER BY ww.query_cost DESC
OPTION(RECOMPILE);

END

IF (@ExportToExcel = 1 AND @SkipXML = 0)
BEGIN

UPDATE #working_plan_text
SET query_sql_text = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(query_sql_text)),' ','<>'),'><',''),'<>',' '), 1, 31000)
OPTION(RECOMPILE);

SELECT wpt.database_name, ww.query_cost, wpt.query_sql_text, wm.proc_or_function_name, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.last_force_failure_reason_desc, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.min_query_max_used_memory, wm.max_query_max_used_memory,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
ON wpt.plan_id = ww.plan_id
AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
ON wpt.plan_id = wm.plan_id
AND wpt.query_id = wm.query_id
ORDER BY ww.query_cost DESC
OPTION(RECOMPILE);

END

IF (@ExportToExcel = 0 AND @SkipXML = 1)
BEGIN
SELECT wpt.database_name, wpt.query_sql_text, wpt.query_plan_xml, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.min_query_max_used_memory, wm.max_query_max_used_memory,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings
FROM #working_plan_text AS wpt
JOIN #working_metrics AS wm
ON wpt.plan_id = wm.plan_id
AND wpt.query_id = wm.query_id
ORDER BY wm.avg_cpu_time DESC
OPTION(RECOMPILE);
END


IF (@ExportToExcel = 0 AND @HideSummary = 0 AND @SkipXML = 0)
BEGIN
        RAISERROR('Building query plan summary data.', 0, 1) WITH NOWAIT;

        /* Build summary data */
        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE frequent_execution = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    1,
                    100,
                    'Execution Pattern',
                    'Frequently Executed Queries',
                    'http://brentozar.com/blitzcache/frequently-executed-queries/',
                    'Queries are being executed more than '
                    + CAST (@execution_threshold AS VARCHAR(5))
                    + ' times per minute. This can put additional load on the server, even when queries are lightweight.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  parameter_sniffing = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    2,
                    50,
                    'Parameterization',
                    'Parameter Sniffing',
                    'http://brentozar.com/blitzcache/parameter-sniffing/',
                    'There are signs of parameter sniffing (wide variance in rows return or time to execute). Investigate query patterns and tune code appropriately.') ;

        /* Forced execution plans */
        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_forced_plan = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    3,
                    5,
                    'Parameterization',
                    'Forced Plans',
                    'http://brentozar.com/blitzcache/forced-plans/',
                    'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_cursor = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    4,
                    200,
                    'Cursors',
                    'Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are cursors in the plan cache. This is neither good nor bad, but it is a thing. Cursors are weird in SQL Server.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_cursor = 1
				   AND is_optimistic_cursor = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    4,
                    200,
                    'Cursors',
                    'Optimistic Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are optimistic cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_cursor = 1
				   AND is_forward_only_cursor = 0
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    4,
                    200,
                    'Cursors',
                    'Non-forward Only Cursors',
                    'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are non-forward only cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_forced_parameterized = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    5,
                    50,
                    'Parameterization',
                    'Forced Parameterization',
                    'http://brentozar.com/blitzcache/forced-parameterization/',
                    'Execution plans have been compiled with forced parameterization.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_parallel = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    6,
                    200,
                    'Execution Plans',
                    'Parallelism',
                    'http://brentozar.com/blitzcache/parallel-plans-detected/',
                    'Parallel plans detected. These warrant investigation, but are neither good nor bad.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  near_parallel = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    7,
                    200,
                    'Execution Plans',
                    'Nearly Parallel',
                    'http://brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                    'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  plan_warnings = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    8,
                    50,
                    'Execution Plans',
                    'Query Plan Warnings',
                    'http://brentozar.com/blitzcache/query-plan-warnings/',
                    'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  long_running = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    9,
                    50,
                    'Performance',
                    'Long Running Queries',
                    'http://brentozar.com/blitzcache/long-running-queries/',
                    'Long running queries have been found. These are queries with an average duration longer than '
                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 AS VARCHAR(5))
                    + ' second(s). These queries should be investigated for additional tuning options.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.missing_index_count > 0
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    10,
                    50,
                    'Performance',
                    'Missing Index Request',
                    'http://brentozar.com/blitzcache/missing-index-request/',
                    'Queries found with missing indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.downlevel_estimator = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    13,
                    200,
                    'Cardinality',
                    'Legacy Cardinality Estimator in Use',
                    'http://brentozar.com/blitzcache/legacy-cardinality-estimator/',
                    'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

        IF EXISTS (SELECT 1/0
                   FROM #working_warnings p
                   WHERE implicit_conversions = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    14,
                    50,
                    'Performance',
                    'Implicit Conversions',
                    'http://brentozar.com/go/implicit',
                    'One or more queries are comparing two fields that are not of the same data type.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  compile_timeout = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                18,
                50,
                'Execution Plans',
                'Compilation timeout',
                'http://brentozar.com/blitzcache/compilation-timeout/',
                'Query compilation timed out for one or more queries. SQL Server did not find a plan that meets acceptable performance criteria in the time allotted so the best guess was returned. There is a very good chance that this plan isn''t even below average - it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  compile_memory_limit_exceeded = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                19,
                50,
                'Execution Plans',
                'Compilation memory limit exceeded',
                'http://brentozar.com/blitzcache/compile-memory-limit-exceeded/',
                'The optimizer has a limited amount of memory available. One or more queries are complex enough that SQL Server was unable to allocate enough memory to fully optimize the query. A best fit plan was found, and it''s probably terrible.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  warning_no_join_predicate = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                20,
                10,
                'Execution Plans',
                'No join predicate',
                'http://brentozar.com/blitzcache/no-join-predicate/',
                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  plan_multiple_plans = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                21,
                200,
                'Execution Plans',
                'Multiple execution plans',
                'http://brentozar.com/blitzcache/multiple-plans/',
                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  unmatched_index_count > 0
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                22,
                100,
                'Performance',
                'Unmatched indexes',
                'http://brentozar.com/blitzcache/unmatched-indexes',
                'An index could have been used, but SQL Server chose not to use it - likely due to parameterization and filtered indexes.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  unparameterized_query = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                23,
                100,
                'Parameterization',
                'Unparameterized queries',
                'http://brentozar.com/blitzcache/unparameterized-queries',
                'Unparameterized queries found. These could be ad hoc queries, data exploration, or queries using "OPTIMIZE FOR UNKNOWN".');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_trivial = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                24,
                100,
                'Execution Plans',
                'Trivial Plans',
                'http://brentozar.com/blitzcache/trivial-plans',
                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');
    
        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_forced_serial= 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    25,
                    10,
                    'Execution Plans',
                    'Forced Serialization',
                    'http://www.brentozar.com/blitzcache/forced-serialization/',
                    'Something in your plan is forcing a serial query. Further investigation is needed if this is not by design.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_key_lookup_expensive= 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    26,
                    100,
                    'Execution Plans',
                    'Expensive Key Lookups',
                    'http://www.brentozar.com/blitzcache/expensive-key-lookups/',
                    'There''s a key lookup in your plan that costs >=50% of the total plan cost.') ;	

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_remote_query_expensive= 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    28,
                    100,
                    'Execution Plans',
                    'Expensive Remote Query',
                    'http://www.brentozar.com/blitzcache/expensive-remote-query/',
                    'There''s a remote query in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.trace_flags_session IS NOT NULL
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    29,
                    100,
                    'Trace Flags',
                    'Session Level Trace Flags Enabled',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'Someone is enabling session level Trace Flags in a query.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_unused_grant IS NOT NULL
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    30,
                    100,
                    'Unused memory grants',
                    'Queries are asking for more memory than they''re using',
                    'https://www.brentozar.com/blitzcache/unused-memory-grants/',
                    'Queries have large unused memory grants. This can cause concurrency issues, if queries are waiting a long time to get memory to run.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.function_count > 0
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    31,
                    100,
                    'Compute Scalar That References A Function',
                    'This could be trouble if you''re using Scalar Functions or MSTVFs',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'Both of these will force queries to run serially, run at least once per row, and may result in poor cardinality estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.clr_function_count > 0
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    32,
                    100,
                    'Compute Scalar That References A CLR Function',
                    'This could be trouble if your CLR functions perform data access',
                    'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                    'May force queries to run serially, run at least once per row, and may result in poor cardinlity estimates.') ;


        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_table_variable = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    33,
                    100,
                    'Table Variables detected',
                    'Beware nasty side effects',
                    'https://www.brentozar.com/blitzcache/table-variables/',
                    'All modifications are single threaded, and selects have really low row estimates.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.no_stats_warning = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    35,
                    100,
                    'Columns with no statistics',
                    'Poor cardinality estimates may ensue',
                    'https://www.brentozar.com/blitzcache/columns-no-statistics/',
                    'Sometimes this happens with indexed views, other times because auto create stats is turned off.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.relop_warnings = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    36,
                    100,
                    'Operator Warnings',
                    'SQL is throwing operator level plan warnings',
                    'http://brentozar.com/blitzcache/query-plan-warnings/',
                    'Check the plan for more details.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_table_scan = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    37,
                    100,
                    'Table Scans',
                    'Your database has HEAPs',
                    'https://www.brentozar.com/archive/2012/05/video-heaps/',
                    'This may not be a problem. Run sp_BlitzIndex for more information.') ;
        
		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.backwards_scan = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    38,
                    100,
                    'Backwards Scans',
                    'Indexes are being read backwards',
                    'https://www.brentozar.com/blitzcache/backwards-scans/',
                    'This isn''t always a problem. They can cause serial zones in plans, and may need an index to match sort order.') ;

		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.forced_index = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    39,
                    100,
                    'Index forcing',
                    'Someone is using hints to force index usage',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans, and will prevent missing index requests.') ;

		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.forced_seek = 1
				   OR p.forced_scan = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    40,
                    100,
                    'Seek/Scan forcing',
                    'Someone is using hints to force index seeks/scans',
                    'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                    'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.') ;

		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.columnstore_row_mode = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    41,
                    100,
                    'ColumnStore indexes operating in Row Mode',
                    'Batch Mode is optimal for ColumnStore indexes',
                    'https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/',
                    'ColumnStore indexes operating in Row Mode indicate really poor query choices.') ;

		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.is_computed_scalar = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    42,
                    50,
                    'Computed Columns Referencing Scalar UDFs',
                    'This makes a whole lot of stuff run serially',
                    'https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/',
                    'This can cause a whole mess of bad serializartion problems.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_sort_expensive = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     43,
                     100,
                     'Execution Plans',
                     'Expensive Sort',
                     'http://www.brentozar.com/blitzcache/expensive-sorts/',
                     'There''s a sort in your plan that costs >=50% of the total plan cost.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_computed_filter = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     44,
                     50,
                     'Filters Referencing Scalar UDFs',
                     'This forces serialization',
                     'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                     'Someone put a Scalar UDF in the WHERE clause!') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.index_ops >= 5
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     45,
                     100,
                     'Many Indexes Modified',
                     'Write Queries Are Hitting >= 5 Indexes',
                     'No URL yet',
                     'This can cause lots of hidden I/O -- Run sp_BlitzIndex for more information.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_row_level = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     46,
                     100,
                     'Plan Confusion',
                     'Row Level Security is in use',
                     'No URL yet',
                     'You may see a lot of confusing junk in your query plan.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_spatial = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     47,
                     200,
                     'Spatial Abuse',
                     'You hit a Spatial Index',
                     'No URL yet',
                     'Purely informational.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.index_dml = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     48,
                     150,
                     'Index DML',
                     'Indexes were created or dropped',
                     'No URL yet',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.table_dml = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     49,
                     150,
                     'Table DML',
                     'Tables were created or dropped',
                     'No URL yet',
                     'This can cause recompiles and stuff.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.long_running_low_cpu = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     50,
                     150,
                     'Long Running Low CPU',
                     'You have a query that runs for much longer than it uses CPU',
                     'No URL yet',
                     'This can be a sign of blocking, linked servers, or poor client application code (ASYNC_NETWORK_IO).') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.low_cost_high_cpu = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     51,
                     150,
                     'Low Cost Query With High CPU',
                     'You have a low cost query that uses a lot of CPU',
                     'No URL yet',
                     'This can be a sign of functions or Dynamic SQL that calls black-box code.') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.stale_stats = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     52,
                     150,
                     'Biblical Statistics',
                     'Statistics used in queries are >7 days old with >100k modifications',
                     'No URL yet',
                     'Ever heard of updating statistics?') ;

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_adaptive = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     53,
                     150,
                     'Adaptive joins',
                     'This is pretty cool -- you''re living in the future.',
                     'No URL yet',
                     'Joe Sack rules.') ;					 


				INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT 
				
				999,
				200,
				'Database Level Statistics',
				'The database ' + sa.[database] + ' last had a stats update on '  + CONVERT(NVARCHAR(10), CONVERT(DATE, MAX(sa.last_update))) + ' and has ' + CONVERT(NVARCHAR(10), AVG(sa.modification_count)) + ' modifications on average.' AS [Finding],
				'' AS URL,
				'Consider updating statistics more frequently,' AS [Details]
				FROM #stats_agg AS sa
				GROUP BY sa.[database]
				HAVING MAX(sa.last_update) <= DATEADD(DAY, -7, SYSDATETIME())
				AND AVG(sa.modification_count) >= 100000

		
        IF EXISTS (SELECT 1/0
                   FROM   #trace_flags AS tf 
                   WHERE  tf.global_trace_flags IS NOT NULL
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    1000,
                    255,
                    'Global Trace Flags Enabled',
                    'You have Global Trace Flags enabled on your server',
                    'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                    'You have the following Global Trace Flags enabled: ' + (SELECT TOP 1 tf.global_trace_flags FROM #trace_flags AS tf WHERE tf.global_trace_flags IS NOT NULL)) ;


        IF NOT EXISTS (SELECT 1/0
					   FROM   #warning_results AS bcr
                       WHERE  bcr.Priority = 2147483647
				      )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    2147483647,
                    255,
                    'Thanks for using sp_BlitzCache!' ,
                    'From Your Community Volunteers',
                    'http://FirstResponderKit.org',
                    'We hope you found this tool useful. Current version: ' + @Version + ' released on ' + CONVERT(NVARCHAR(30), @VersionDate) + '.') ;
	

	
    SELECT  Priority,
            FindingsGroup,
            Finding,
            URL,
            Details,
            CheckID
    FROM    #warning_results 
    GROUP BY Priority,
            FindingsGroup,
            Finding,
            URL,
            Details,
            CheckID
    ORDER BY Priority ASC, CheckID ASC
    OPTION (RECOMPILE);

END	

IF @Debug = 1	
BEGIN
--Table content debugging

SELECT '#working_metrics' AS table_name, *
FROM #working_metrics AS wm
OPTION(RECOMPILE);

SELECT '#working_plan_text' AS table_name, *
FROM #working_plan_text AS wpt
OPTION(RECOMPILE);

SELECT '#working_warnings' AS table_name, *
FROM #working_warnings AS ww
OPTION(RECOMPILE);

SELECT '#statements' AS table_name, *
FROM #statements AS s
OPTION(RECOMPILE);

SELECT '#query_plan' AS table_name, *
FROM #query_plan AS qp
OPTION(RECOMPILE);

SELECT '#relop' AS table_name, *
FROM #relop AS r
OPTION(RECOMPILE);

SELECT '#plan_cost' AS table_name,  * 
FROM #plan_cost AS pc
OPTION(RECOMPILE);

END 

/*
Ways to run this thing

--Debug
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Debug = 1

--Get the top 1
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @Debug = 1

--Use a StartDate												 
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170527', @Debug = 1
				
--Use an EndDate												 
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @EndDate = '20170527', @Debug = 1
				
--Use Both												 
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170526', @EndDate = '20170527', @Debug = 1

--Set a minimum execution count												 
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @MinimumExecutionCount = 10, @Debug = 1

Set a duration minimum
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @DurationFilter = 5, @Debug = 1

--Look for a stored procedure name (that doesn't exist!)
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'blah', @Debug = 1

--Look for a stored procedure name that does (at least On My Computer®)
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'UserReportExtended', @Debug = 1

--Look for failed queries
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @Failed = 1, @Debug = 1

--Look for statements 
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @QueryFilter = 'sta', @Debug = 1

--Look for stored procs
EXEC sp_BlitzQS @DatabaseName = 'StackOverflow', @Top = 1, @QueryFilter = 'pro', @Debug = 1

*/



END;