SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

DECLARE @msg NVARCHAR(MAX) = N'';

	-- Must be a compatible, on-prem version of SQL (2016+)
IF  (	(SELECT CONVERT(NVARCHAR(128), SERVERPROPERTY ('EDITION'))) <> 'SQL Azure' 
	AND (SELECT PARSENAME(CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)) < 13 
	)
	-- or Azure Database (not Azure Data Warehouse), running at database compat level 130+
OR	(	(SELECT CONVERT(NVARCHAR(128), SERVERPROPERTY ('EDITION'))) = 'SQL Azure'
	AND (SELECT SERVERPROPERTY ('ENGINEEDITION')) NOT IN (5,8)
	AND (SELECT [compatibility_level] FROM sys.databases WHERE [name] = DB_NAME()) < 130
	)
BEGIN
	SELECT @msg = N'Sorry, sp_BlitzQueryStore doesn''t work on versions of SQL prior to 2016, or Azure Database compatibility < 130.' + REPLICATE(CHAR(13), 7933);
	PRINT @msg;
	RETURN;
END;

IF OBJECT_ID('dbo.sp_BlitzQueryStore') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzQueryStore AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_BlitzQueryStore
    @Help BIT = 0,
    @DatabaseName NVARCHAR(128) = NULL ,
    @Top INT = 3,
	@StartDate DATETIME2 = NULL,
	@EndDate DATETIME2 = NULL,
    @MinimumExecutionCount INT = NULL,
    @DurationFilter DECIMAL(38,4) = NULL ,
    @StoredProcName NVARCHAR(128) = NULL,
	@Failed BIT = 0,
	@PlanIdFilter INT = NULL,
	@QueryIdFilter INT = NULL,
    @ExportToExcel BIT = 0,
    @HideSummary BIT = 0 ,
	@SkipXML BIT = 0,
	@Debug BIT = 0,
	@ExpertMode BIT = 0,
	@Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
WITH RECOMPILE
AS
BEGIN /*First BEGIN*/

SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT @Version = '8.10', @VersionDate = '20220718';
IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;


DECLARE /*Variables for the variable Gods*/
		@msg NVARCHAR(MAX) = N'', --Used to format RAISERROR messages in some places
		@sql_select NVARCHAR(MAX) = N'', --Used to hold SELECT statements for dynamic SQL
		@sql_where NVARCHAR(MAX) = N'', -- Used to hold WHERE clause for dynamic SQL
		@duration_filter_ms DECIMAL(38,4) = (@DurationFilter * 1000.), --We accept Duration in seconds, but we filter in milliseconds (this is grandfathered from sp_BlitzCache)
		@execution_threshold INT = 1000, --Threshold at which we consider a query to be frequently executed
        @ctp_threshold_pct TINYINT = 10, --Percentage of CTFP at which we consider a query to be near parallel
        @long_running_query_warning_seconds BIGINT = 300 * 1000 ,--Number of seconds (converted to milliseconds) at which a query is considered long running
		@memory_grant_warning_percent INT = 10,--Percent of memory grant used compared to what's granted; used to trigger unused memory grant warning
		@ctp INT,--Holds the CTFP value for the server
		@min_memory_per_query INT,--Holds the server configuration value for min memory per query
		@cr NVARCHAR(1) = NCHAR(13),--Special character
		@lf NVARCHAR(1) = NCHAR(10),--Special character
		@tab NVARCHAR(1) = NCHAR(9),--Special character
		@error_severity INT,--Holds error info for try/catch blocks
		@error_state INT,--Holds error info for try/catch blocks
		@sp_params NVARCHAR(MAX) = N'@sp_Top INT, @sp_StartDate DATETIME2, @sp_EndDate DATETIME2, @sp_MinimumExecutionCount INT, @sp_MinDuration INT, @sp_StoredProcName NVARCHAR(128), @sp_PlanIdFilter INT, @sp_QueryIdFilter INT',--Holds parameters used in dynamic SQL
		@is_azure_db BIT = 0, --Are we using Azure? I'm not. You might be. That's cool.
		@compatibility_level TINYINT = 0, --Some functionality (T-SQL) isn't available in lower compat levels. We can use this to weed out those issues as we go.
		@log_size_mb DECIMAL(38,2) = 0,
		@avg_tempdb_data_file DECIMAL(38,2) = 0;

/*Grabs CTFP setting*/
SELECT  @ctp = NULLIF(CAST(value AS INT), 0)
FROM    sys.configurations
WHERE   name = N'cost threshold for parallelism'
OPTION (RECOMPILE);

/*Grabs min query memory setting*/
SELECT @min_memory_per_query = CONVERT(INT, c.value)
FROM   sys.configurations AS c
WHERE  c.name = N'min memory per query (KB)'
OPTION (RECOMPILE);

/*Check if this is Azure first*/
IF (SELECT CONVERT(NVARCHAR(128), SERVERPROPERTY ('EDITION'))) <> 'SQL Azure'
    BEGIN 
        /*Grabs log size for datbase*/
        SELECT @log_size_mb = AVG(((mf.size * 8) / 1024.))
        FROM sys.master_files AS mf
        WHERE mf.database_id = DB_ID(@DatabaseName)
        AND mf.type_desc = 'LOG';
        
        /*Grab avg tempdb file size*/
        SELECT @avg_tempdb_data_file = AVG(((mf.size * 8) / 1024.))
        FROM sys.master_files AS mf
        WHERE mf.database_id = DB_ID('tempdb')
        AND mf.type_desc = 'ROWS';
    END;

/*Help section*/

IF @Help = 1
	BEGIN
	
	SELECT N'You have requested assistance. It will arrive as soon as humanly possible.' AS [Take four red capsules, help is on the way];

	PRINT N'
	sp_BlitzQueryStore from http://FirstResponderKit.org
		
	This script displays your most resource-intensive queries from the Query Store,
	and points to ways you can tune these queries to make them faster.
	
	
	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.
	
	Known limitations of this version:
	 - This query will not run on SQL Server versions less than 2016.
	 - This query will not run on Azure Databases with compatibility less than 130.
	 - This query will not run on Azure Data Warehouse.

	Unknown limitations of this version:
	 - Could be tickling
	
	
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

END;

/*Making sure your version is copasetic*/
IF  ( (SELECT CONVERT(NVARCHAR(128), SERVERPROPERTY ('EDITION'))) = 'SQL Azure' )
	BEGIN
		SET @is_azure_db = 1;

		IF	(	(SELECT SERVERPROPERTY ('ENGINEEDITION')) NOT IN (5,8)
			OR	(SELECT [compatibility_level] FROM sys.databases WHERE [name] = DB_NAME()) < 130 
			)
		BEGIN
			SELECT @msg = N'Sorry, sp_BlitzQueryStore doesn''t work on Azure Data Warehouse, or Azure Databases with DB compatibility < 130.' + REPLICATE(CHAR(13), 7933);
			PRINT @msg;
			RETURN;
		END;
	END;
ELSE IF  ( (SELECT PARSENAME(CONVERT(NVARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4) ) < 13 )
	BEGIN
		SELECT @msg = N'Sorry, sp_BlitzQueryStore doesn''t work on versions of SQL prior to 2016.' + REPLICATE(CHAR(13), 7933);
		PRINT @msg;
		RETURN;
	END;

/*Making sure at least one database uses QS*/
IF  (	SELECT COUNT(*)
		FROM sys.databases AS d
		WHERE d.is_query_store_on = 1
		AND d.user_access_desc='MULTI_USER'
		AND d.state_desc = 'ONLINE'
		AND d.name NOT IN ('master', 'model', 'msdb', 'tempdb', '32767') 
		AND d.is_distributor = 0 ) = 0
	BEGIN
		SELECT @msg = N'You don''t currently have any databases with Query Store enabled.' + REPLICATE(CHAR(13), 7933);
		PRINT @msg;
		RETURN;
	END;
									
/*Making sure your databases are using QDS.*/
RAISERROR('Checking database validity', 0, 1) WITH NOWAIT;

IF (@is_azure_db = 1)
	SET @DatabaseName = DB_NAME();
ELSE
BEGIN	

	/*If we're on Azure we don't need to check all this @DatabaseName stuff...*/

	SET @DatabaseName = LTRIM(RTRIM(@DatabaseName));

	/*Did you set @DatabaseName?*/
	RAISERROR('Making sure [%s] isn''t NULL', 0, 1, @DatabaseName) WITH	NOWAIT;
	IF (@DatabaseName IS NULL)
	BEGIN
		RAISERROR('@DatabaseName cannot be NULL', 0, 1) WITH NOWAIT;
		RETURN;
	END;

	/*Does the database exist?*/
	RAISERROR('Making sure [%s] exists', 0, 1, @DatabaseName) WITH NOWAIT;
	IF ((DB_ID(@DatabaseName)) IS NULL)
	BEGIN
		RAISERROR('The @DatabaseName you specified ([%s]) does not exist. Please check the name and try again.', 0, 1, @DatabaseName) WITH	NOWAIT;
		RETURN;
	END;

	/*Is it online?*/
	RAISERROR('Making sure [%s] is online', 0, 1, @DatabaseName) WITH NOWAIT;
	IF (DATABASEPROPERTYEX(@DatabaseName, 'Collation')) IS NULL
	BEGIN
		RAISERROR('The @DatabaseName you specified ([%s]) is not readable. Please check the name and try again. Better yet, check your server.', 0, 1, @DatabaseName);
		RETURN;
	END;
END;

/*Does it have Query Store enabled?*/
RAISERROR('Making sure [%s] has Query Store enabled', 0, 1, @DatabaseName) WITH NOWAIT;
IF 	

	( SELECT [d].[name]
		FROM [sys].[databases] AS d
		WHERE [d].[is_query_store_on] = 1
		AND [d].[user_access_desc]='MULTI_USER'
		AND [d].[state_desc] = 'ONLINE'
		AND [d].[database_id] = (SELECT database_id FROM sys.databases WHERE name = @DatabaseName)
	) IS NULL
BEGIN
	RAISERROR('The @DatabaseName you specified ([%s]) does not have the Query Store enabled. Please check the name or settings, and try again.', 0, 1, @DatabaseName) WITH	NOWAIT;
	RETURN;
END;

/*Check database compat level*/

RAISERROR('Checking database compatibility level', 0, 1) WITH NOWAIT;

SELECT @compatibility_level = d.compatibility_level
FROM sys.databases AS d
WHERE d.name = @DatabaseName;

RAISERROR('The @DatabaseName you specified ([%s])is running in compatibility level ([%d]).', 0, 1, @DatabaseName, @compatibility_level) WITH NOWAIT;


/*Making sure top is set to something if NULL*/
IF ( @Top IS NULL )
   BEGIN
       SET @Top = 3;
   END;

/*
This section determines if you have the Query Store wait stats DMV
*/

RAISERROR('Checking for query_store_wait_stats', 0, 1) WITH NOWAIT;

DECLARE @ws_out INT,
		@waitstats BIT,
		@ws_sql NVARCHAR(MAX) = N'SELECT @i_out = COUNT(*) FROM ' + QUOTENAME(@DatabaseName) + N'.sys.all_objects WHERE name = ''query_store_wait_stats'' OPTION (RECOMPILE);',
		@ws_params NVARCHAR(MAX) = N'@i_out INT OUTPUT';

EXEC sys.sp_executesql @ws_sql, @ws_params, @i_out = @ws_out OUTPUT;

SELECT @waitstats = CASE @ws_out WHEN 0 THEN 0 ELSE 1 END;

SET @msg = N'Wait stats DMV ' + CASE @waitstats 
									WHEN 0 THEN N' does not exist, skipping.'
									WHEN 1 THEN N' exists, will analyze.'
							   END;
RAISERROR(@msg, 0, 1) WITH NOWAIT;

/*
This section determines if you have some additional columns present in 2017, in case they get back ported.
*/

RAISERROR('Checking for new columns in query_store_runtime_stats', 0, 1) WITH NOWAIT;

DECLARE @nc_out INT,
		@new_columns BIT,
		@nc_sql NVARCHAR(MAX) = N'SELECT @i_out = COUNT(*) 
							      FROM ' + QUOTENAME(@DatabaseName) + N'.sys.all_columns AS ac
								  WHERE OBJECT_NAME(object_id) = ''query_store_runtime_stats''
								  AND ac.name IN (
								  ''avg_num_physical_io_reads'',
								  ''last_num_physical_io_reads'',
								  ''min_num_physical_io_reads'',
								  ''max_num_physical_io_reads'',
								  ''avg_log_bytes_used'',
								  ''last_log_bytes_used'',
								  ''min_log_bytes_used'',
								  ''max_log_bytes_used'',
								  ''avg_tempdb_space_used'',
								  ''last_tempdb_space_used'',
								  ''min_tempdb_space_used'',
								  ''max_tempdb_space_used''
								  ) OPTION (RECOMPILE);',
		@nc_params NVARCHAR(MAX) = N'@i_out INT OUTPUT';

EXEC sys.sp_executesql @nc_sql, @ws_params, @i_out = @nc_out OUTPUT;

SELECT @new_columns = CASE @nc_out WHEN 12 THEN 1 ELSE 0 END;

SET @msg = N'New query_store_runtime_stats columns ' + CASE @new_columns 
									WHEN 0 THEN N' do not exist, skipping.'
									WHEN 1 THEN N' exist, will analyze.'
							   END;
RAISERROR(@msg, 0, 1) WITH NOWAIT;

 
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
    flat_date DATE NULL,
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
	total_avg_log_bytes_mb DECIMAL(38, 2) NULL,
	total_avg_tempdb_space DECIMAL(38, 2) NULL,
    total_max_duration_ms DECIMAL(38, 2) NULL,
    total_max_cpu_time_ms DECIMAL(38, 2) NULL,
    total_max_logical_io_reads_mb DECIMAL(38, 2) NULL,
    total_max_physical_io_reads_mb DECIMAL(38, 2) NULL,
    total_max_logical_io_writes_mb DECIMAL(38, 2) NULL,
    total_max_query_max_used_memory_mb DECIMAL(38, 2) NULL,
	total_max_log_bytes_mb DECIMAL(38, 2) NULL,
	total_max_tempdb_space DECIMAL(38, 2) NULL,
	INDEX gi_ix_dates CLUSTERED (start_range, end_range)
);


/*
These are the plans we focus on based on what we find in the grouped intervals
*/
DROP TABLE IF EXISTS #working_plans;

CREATE TABLE #working_plans
(
    plan_id BIGINT,
    query_id BIGINT,
	pattern NVARCHAR(258),
	INDEX wp_ix_ids CLUSTERED (plan_id, query_id)
);


/*
These are the gathered metrics we get from query store to generate some warnings and help you find your worst offenders
*/
DROP TABLE IF EXISTS #working_metrics;

CREATE TABLE #working_metrics 
(
    database_name NVARCHAR(258),
	plan_id BIGINT,
    query_id BIGINT,
    query_id_all_plan_ids VARCHAR(8000),
	/*these columns are from query_store_query*/
	proc_or_function_name NVARCHAR(258),
	batch_sql_handle VARBINARY(64),
	query_hash BINARY(8),
	query_parameterization_type_desc NVARCHAR(258),
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
	/*These are 2017 only, AFAIK*/
	avg_num_physical_io_reads DECIMAL(38,2),
	last_num_physical_io_reads DECIMAL(38,2),
	min_num_physical_io_reads DECIMAL(38,2),
	max_num_physical_io_reads DECIMAL(38,2),
	avg_log_bytes_used DECIMAL(38,2),
	last_log_bytes_used DECIMAL(38,2),
	min_log_bytes_used DECIMAL(38,2),
	max_log_bytes_used DECIMAL(38,2),
	avg_tempdb_space_used DECIMAL(38,2),
	last_tempdb_space_used DECIMAL(38,2),
	min_tempdb_space_used DECIMAL(38,2),
	max_tempdb_space_used DECIMAL(38,2),
	/*These are computed columns to make some stuff easier down the line*/
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
	total_num_physical_io_reads AS avg_num_physical_io_reads * count_executions,
	total_log_bytes_used AS avg_log_bytes_used * count_executions,
	total_tempdb_space_used AS avg_tempdb_space_used * count_executions,
	xpm AS NULLIF(count_executions, 0) / NULLIF(DATEDIFF(MINUTE, first_execution_time, last_execution_time), 0),
    percent_memory_grant_used AS CONVERT(MONEY, ISNULL(NULLIF(( max_query_max_used_memory * 1.00 ), 0) / NULLIF(min_query_max_used_memory, 0), 0) * 100.),
	INDEX wm_ix_ids CLUSTERED (plan_id, query_id, query_hash)
);


/*
This is where we store some additional metrics, along with the query plan and text
*/
DROP TABLE IF EXISTS #working_plan_text;

CREATE TABLE #working_plan_text 
(
	database_name NVARCHAR(258),
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
	last_force_failure_reason_desc NVARCHAR(258),
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
	pattern NVARCHAR(512),
	top_three_waits NVARCHAR(MAX),
	INDEX wpt_ix_ids CLUSTERED (plan_id, query_id, query_plan_hash)
); 


/*
This is where we store warnings that we generate from the XML and metrics
*/
DROP TABLE IF EXISTS #working_warnings;

CREATE TABLE #working_warnings 
(
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	proc_or_function_name NVARCHAR(258),
	plan_multiple_plans BIT,
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
	is_slow_plan BIT,
	is_compile_more BIT,
	index_spool_cost FLOAT,
	index_spool_rows FLOAT,
	is_spool_expensive BIT,
	is_spool_more_rows BIT,
	estimated_rows FLOAT,
	is_bad_estimate BIT, 
	is_big_log BIT,
	is_big_tempdb BIT,
	is_paul_white_electric BIT,
	is_row_goal BIT,
	is_mstvf BIT,
	is_mm_join BIT,
    is_nonsargable BIT,
	busy_loops BIT,
	tvf_join BIT,
	implicit_conversion_info XML,
	cached_execution_parameters XML,
	missing_indexes XML,
    warnings NVARCHAR(4000)
	INDEX ww_ix_ids CLUSTERED (plan_id, query_id, query_hash, sql_handle)
);


DROP TABLE IF EXISTS #working_wait_stats;

CREATE TABLE #working_wait_stats
(
    plan_id BIGINT,
	wait_category TINYINT,
	wait_category_desc NVARCHAR(258),
	total_query_wait_time_ms BIGINT,
	avg_query_wait_time_ms	 DECIMAL(38, 2),
	last_query_wait_time_ms	BIGINT,
	min_query_wait_time_ms	BIGINT,
	max_query_wait_time_ms	BIGINT,
	wait_category_mapped AS CASE wait_category
								WHEN 0  THEN N'UNKNOWN'
								WHEN 1  THEN N'SOS_SCHEDULER_YIELD'
								WHEN 2  THEN N'THREADPOOL'
								WHEN 3  THEN N'LCK_M_%'
								WHEN 4  THEN N'LATCH_%'
								WHEN 5  THEN N'PAGELATCH_%'
								WHEN 6  THEN N'PAGEIOLATCH_%'
								WHEN 7  THEN N'RESOURCE_SEMAPHORE_QUERY_COMPILE'
								WHEN 8  THEN N'CLR%, SQLCLR%'
								WHEN 9  THEN N'DBMIRROR%'
								WHEN 10 THEN N'XACT%, DTC%, TRAN_MARKLATCH_%, MSQL_XACT_%, TRANSACTION_MUTEX'
								WHEN 11 THEN N'SLEEP_%, LAZYWRITER_SLEEP, SQLTRACE_BUFFER_FLUSH, SQLTRACE_INCREMENTAL_FLUSH_SLEEP, SQLTRACE_WAIT_ENTRIES, FT_IFTS_SCHEDULER_IDLE_WAIT, XE_DISPATCHER_WAIT, REQUEST_FOR_DEADLOCK_SEARCH, LOGMGR_QUEUE, ONDEMAND_TASK_QUEUE, CHECKPOINT_QUEUE, XE_TIMER_EVENT'
								WHEN 12 THEN N'PREEMPTIVE_%'
								WHEN 13 THEN N'BROKER_% (but not BROKER_RECEIVE_WAITFOR)'
								WHEN 14 THEN N'LOGMGR, LOGBUFFER, LOGMGR_RESERVE_APPEND, LOGMGR_FLUSH, LOGMGR_PMM_LOG, CHKPT, WRITELOG'
								WHEN 15 THEN N'ASYNC_NETWORK_IO, NET_WAITFOR_PACKET, PROXY_NETWORK_IO, EXTERNAL_SCRIPT_NETWORK_IOF'
								WHEN 16 THEN N'CXPACKET, EXCHANGE, CXCONSUMER'
								WHEN 17 THEN N'RESOURCE_SEMAPHORE, CMEMTHREAD, CMEMPARTITIONED, EE_PMOLOCK, MEMORY_ALLOCATION_EXT, RESERVED_MEMORY_ALLOCATION_EXT, MEMORY_GRANT_UPDATE'
								WHEN 18 THEN N'WAITFOR, WAIT_FOR_RESULTS, BROKER_RECEIVE_WAITFOR'
								WHEN 19 THEN N'TRACEWRITE, SQLTRACE_LOCK, SQLTRACE_FILE_BUFFER, SQLTRACE_FILE_WRITE_IO_COMPLETION, SQLTRACE_FILE_READ_IO_COMPLETION, SQLTRACE_PENDING_BUFFER_WRITERS, SQLTRACE_SHUTDOWN, QUERY_TRACEOUT, TRACE_EVTNOTIFF'
								WHEN 20 THEN N'FT_RESTART_CRAWL, FULLTEXT GATHERER, MSSEARCH, FT_METADATA_MUTEX, FT_IFTSHC_MUTEX, FT_IFTSISM_MUTEX, FT_IFTS_RWLOCK, FT_COMPROWSET_RWLOCK, FT_MASTER_MERGE, FT_PROPERTYLIST_CACHE, FT_MASTER_MERGE_COORDINATOR, PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC'
								WHEN 21 THEN N'ASYNC_IO_COMPLETION, IO_COMPLETION, BACKUPIO, WRITE_COMPLETION, IO_QUEUE_LIMIT, IO_RETRY'
								WHEN 22 THEN N'SE_REPL_%, REPL_%, HADR_% (but not HADR_THROTTLE_LOG_RATE_GOVERNOR), PWAIT_HADR_%, REPLICA_WRITES, FCB_REPLICA_WRITE, FCB_REPLICA_READ, PWAIT_HADRSIM'
								WHEN 23 THEN N'LOG_RATE_GOVERNOR, POOL_LOG_RATE_GOVERNOR, HADR_THROTTLE_LOG_RATE_GOVERNOR, INSTANCE_LOG_RATE_GOVERNOR'
							END,
    INDEX wws_ix_ids CLUSTERED ( plan_id)
);


/*
The next three tables hold plan XML parsed out to different degrees 
*/
DROP TABLE IF EXISTS #statements;

CREATE TABLE #statements 
(
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	statement XML,
    is_cursor BIT
	INDEX s_ix_ids CLUSTERED (plan_id, query_id, query_hash, sql_handle)
);


DROP TABLE IF EXISTS #query_plan;

CREATE TABLE #query_plan 
(
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	query_plan XML,
	INDEX qp_ix_ids CLUSTERED (plan_id, query_id, query_hash, sql_handle)
);


DROP TABLE IF EXISTS #relop;

CREATE TABLE #relop 
(
    plan_id BIGINT,
    query_id BIGINT,
	query_hash BINARY(8),
	sql_handle VARBINARY(64),
	relop XML,
	INDEX ix_ids CLUSTERED (plan_id, query_id, query_hash, sql_handle)
);


DROP TABLE IF EXISTS #plan_cost;

CREATE TABLE #plan_cost 
(
	query_plan_cost DECIMAL(38,2),
	sql_handle VARBINARY(64),
	plan_id INT,
	INDEX px_ix_ids CLUSTERED (sql_handle, plan_id)
);


DROP TABLE IF EXISTS #est_rows;

CREATE TABLE #est_rows 
(
	estimated_rows DECIMAL(38,2),
	query_hash BINARY(8),
	INDEX px_ix_ids CLUSTERED (query_hash)
);


DROP TABLE IF EXISTS #stats_agg;

CREATE TABLE #stats_agg
(
    sql_handle VARBINARY(64),
    last_update DATETIME2,
    modification_count BIGINT,
    sampling_percent DECIMAL(38, 2),
    [statistics] NVARCHAR(258),
    [table] NVARCHAR(258),
    [schema] NVARCHAR(258),
    [database] NVARCHAR(258),
	INDEX sa_ix_ids CLUSTERED (sql_handle)
);


DROP TABLE IF EXISTS #trace_flags;

CREATE TABLE #trace_flags 
(
	sql_handle VARBINARY(54),
	global_trace_flags NVARCHAR(4000),
	session_trace_flags NVARCHAR(4000),
	INDEX tf_ix_ids CLUSTERED (sql_handle)
);


DROP TABLE IF EXISTS #warning_results;	

CREATE TABLE #warning_results 
(
    ID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    CheckID INT,
    Priority TINYINT,
    FindingsGroup NVARCHAR(50),
    Finding NVARCHAR(200),
    URL NVARCHAR(200),
    Details NVARCHAR(4000)
);

/*These next three tables hold information about implicit conversion and cached parameters */
DROP TABLE IF EXISTS #stored_proc_info;	

CREATE TABLE #stored_proc_info
(
	sql_handle VARBINARY(64),
    query_hash BINARY(8),
    variable_name NVARCHAR(258),
    variable_datatype NVARCHAR(258),
	converted_column_name NVARCHAR(258),
    compile_time_value NVARCHAR(258),
    proc_name NVARCHAR(1000),
    column_name NVARCHAR(4000),
    converted_to NVARCHAR(258),
	set_options NVARCHAR(1000)
	INDEX tf_ix_ids CLUSTERED (sql_handle, query_hash)
);

DROP TABLE IF EXISTS #variable_info;

CREATE TABLE #variable_info
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    proc_name NVARCHAR(1000),
    variable_name NVARCHAR(258),
    variable_datatype NVARCHAR(258),
    compile_time_value NVARCHAR(258),
	INDEX vif_ix_ids CLUSTERED (sql_handle, query_hash)
);

DROP TABLE IF EXISTS #conversion_info;

CREATE TABLE #conversion_info
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    proc_name NVARCHAR(128),
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
    convert_implicit_charindex AS CHARINDEX('=CONVERT_IMPLICIT', expression),
	INDEX cif_ix_ids CLUSTERED (sql_handle, query_hash)
);

/* These tables support the Missing Index details clickable*/


DROP TABLE IF EXISTS #missing_index_xml;

CREATE TABLE #missing_index_xml
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    index_xml XML,
	INDEX mix_ix_ids CLUSTERED (sql_handle, query_hash)
);

DROP TABLE IF EXISTS #missing_index_schema;

CREATE TABLE #missing_index_schema
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
    index_xml XML,
	INDEX mis_ix_ids CLUSTERED (sql_handle, query_hash)
);


DROP TABLE IF EXISTS #missing_index_usage;

CREATE TABLE #missing_index_usage
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	usage NVARCHAR(128),
    index_xml XML,
	INDEX miu_ix_ids CLUSTERED (sql_handle, query_hash)
);

DROP TABLE IF EXISTS #missing_index_detail;

CREATE TABLE #missing_index_detail
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
    usage NVARCHAR(128),
    column_name NVARCHAR(128),
	INDEX mid_ix_ids CLUSTERED (sql_handle, query_hash)
);


DROP TABLE IF EXISTS #missing_index_pretty;

CREATE TABLE #missing_index_pretty
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	equality NVARCHAR(MAX),
	inequality NVARCHAR(MAX),
	[include] NVARCHAR(MAX),
	is_spool BIT,
	details AS N'/* '
	           + CHAR(10) 
			   + CASE is_spool 
			          WHEN 0 
					  THEN N'The Query Processor estimates that implementing the '
					  ELSE N'We estimate that implementing the '
				 END  
			   + CONVERT(NVARCHAR(30), impact)
			   + '%.'
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
			   + N'*/',
	INDEX mip_ix_ids CLUSTERED (sql_handle, query_hash)
);

DROP TABLE IF EXISTS #index_spool_ugly;

CREATE TABLE #index_spool_ugly
(
    query_hash BINARY(8),
    sql_handle VARBINARY(64),
    impact FLOAT,
    database_name NVARCHAR(128),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(128),
	equality NVARCHAR(MAX),
	inequality NVARCHAR(MAX),
	[include] NVARCHAR(MAX),
	INDEX isu_ix_ids CLUSTERED (sql_handle, query_hash)
);


/*Sets up WHERE clause that gets used quite a bit*/

--Date stuff
--If they're both NULL, we'll just look at the last 7 days
IF (@StartDate IS NULL AND @EndDate IS NULL)
	BEGIN
	RAISERROR(N'@StartDate and @EndDate are NULL, checking last 7 days', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time >= DATEADD(DAY, -7, DATEDIFF(DAY, 0, SYSDATETIME() ))
					  ';
	END;

--Hey, that's nice of me
IF @StartDate IS NOT NULL
	BEGIN 
	RAISERROR(N'Setting start date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time >= @sp_StartDate 
					   ';
	END; 

--Alright, sensible
IF @EndDate IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting end date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time < @sp_EndDate 
					   ';
    END;

--C'mon, why would you do that?
IF (@StartDate IS NULL AND @EndDate IS NOT NULL)
	BEGIN 
	RAISERROR(N'Setting reasonable start date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time >= DATEADD(DAY, -7, @sp_EndDate) 
					   ';
    END;

--Jeez, abusive
IF (@StartDate IS NOT NULL AND @EndDate IS NULL)
	BEGIN 
	RAISERROR(N'Setting reasonable end date filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.last_execution_time < DATEADD(DAY, 7, @sp_StartDate) 
					   ';
    END;

--I care about minimum execution counts
IF @MinimumExecutionCount IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting execution filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND qsrs.count_executions >= @sp_MinimumExecutionCount 
					   ';
    END;

--You care about stored proc names
IF @StoredProcName IS NOT NULL 
	BEGIN 
	RAISERROR(N'Setting stored proc filter', 0, 1) WITH NOWAIT;
	SET @sql_where += N' AND object_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N')) = @sp_StoredProcName 
					   ';
    END;

--I will always love you, but hopefully this query will eventually end
IF @DurationFilter IS NOT NULL
    BEGIN 
	RAISERROR(N'Setting duration filter', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND (qsrs.avg_duration / 1000.) >= @sp_MinDuration 
					    '; 
	END; 

--I don't know why you'd go looking for failed queries, but hey
IF (@Failed = 0 OR @Failed IS NULL)
    BEGIN 
	RAISERROR(N'Setting failed query filter to 0', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsrs.execution_type = 0 
					    '; 
	END; 
IF (@Failed = 1)
    BEGIN 
	RAISERROR(N'Setting failed query filter to 3, 4', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsrs.execution_type IN (3, 4) 
					    '; 
	END;  

/*Filtering for plan_id or query_id*/
IF (@PlanIdFilter IS NOT NULL)
    BEGIN 
	RAISERROR(N'Setting plan_id filter', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsp.plan_id = @sp_PlanIdFilter 
					    '; 
	END; 

IF (@QueryIdFilter IS NOT NULL)
    BEGIN 
	RAISERROR(N'Setting query_id filter', 0, 1) WITH NOWAIT;
	SET  @sql_where += N' AND qsq.query_id = @sp_QueryIdFilter 
					    '; 
	END; 

IF @Debug = 1
	RAISERROR(N'Starting WHERE clause:', 0, 1) WITH NOWAIT;
	PRINT @sql_where;

IF @sql_where IS NULL
    BEGIN
        RAISERROR(N'@sql_where is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

IF (@ExportToExcel = 1 OR @SkipXML = 1)	
	BEGIN
	RAISERROR(N'Exporting to Excel or skipping XML, hiding summary', 0, 1) WITH NOWAIT;
	SET @HideSummary = 1;
	END;

IF @StoredProcName IS NOT NULL
	BEGIN 
	
	DECLARE @sql NVARCHAR(MAX);
	DECLARE @out INT;
	DECLARE @proc_params NVARCHAR(MAX) = N'@sp_StartDate DATETIME2, @sp_EndDate DATETIME2, @sp_MinimumExecutionCount INT, @sp_MinDuration INT, @sp_StoredProcName NVARCHAR(128), @sp_PlanIdFilter INT, @sp_QueryIdFilter INT, @i_out INT OUTPUT';
	
	
	SET @sql = N'SELECT @i_out = COUNT(*) 
				 FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
				 JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
				 ON qsp.plan_id = qsrs.plan_id
				 JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
				 ON qsq.query_id = qsp.query_id
				 WHERE    1 = 1
				        AND qsq.is_internal_query = 0
				 	    AND qsp.query_plan IS NOT NULL 
				 ';
	
	SET @sql += @sql_where;

	EXEC sys.sp_executesql @sql, 
						   @proc_params, 
						   @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter, @i_out = @out OUTPUT;
	
	IF @out = 0
		BEGIN	

		SET @msg = N'We couldn''t find the Stored Procedure ' + QUOTENAME(@StoredProcName) + N' in the Query Store views for ' + QUOTENAME(@DatabaseName) + N' between ' + CONVERT(NVARCHAR(30), ISNULL(@StartDate, DATEADD(DAY, -7, DATEDIFF(DAY, 0, SYSDATETIME() ))) ) + N' and ' + CONVERT(NVARCHAR(30), ISNULL(@EndDate, SYSDATETIME())) +
					 '. Try removing schema prefixes or adjusting dates. If it was executed from a different database context, try searching there instead.';
		RAISERROR(@msg, 0, 1) WITH NOWAIT;

		SELECT @msg AS [Blue Flowers, Blue Flowers, Blue Flowers];
	
		RETURN;
	
		END; 
	
	END;




/*
This is our grouped interval query.

By default, it looks at queries: 
	In the last 7 days
	That aren't system queries
	That have a query plan (some won't, if nested level is > 128, along with other reasons)
	And haven't failed
	This stuff, along with some other options, will be configurable in the stored proc

*/

IF @sql_where IS NOT NULL
BEGIN TRY
	BEGIN

	RAISERROR(N'Populating temp tables', 0, 1) WITH NOWAIT;

RAISERROR(N'Gathering intervals', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
SELECT   CONVERT(DATE, qsrs.last_execution_time) AS flat_date,
         MIN(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time), 0)) AS start_range,
         MAX(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time) + 1, 0)) AS end_range,
         SUM(qsrs.avg_duration / 1000.) / SUM(qsrs.count_executions) AS total_avg_duration_ms,
         SUM(qsrs.avg_cpu_time / 1000.) / SUM(qsrs.count_executions) AS total_avg_cpu_time_ms,
         SUM((qsrs.avg_logical_io_reads * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_reads_mb,
         SUM((qsrs.avg_physical_io_reads* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_physical_io_reads_mb,
         SUM((qsrs.avg_logical_io_writes* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_writes_mb,
         SUM((qsrs.avg_query_max_used_memory * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_query_max_used_memory_mb,
         SUM(qsrs.avg_rowcount) AS total_rowcount,
         SUM(qsrs.count_executions) AS total_count_executions,
         SUM(qsrs.max_duration / 1000.) AS total_max_duration_ms,
         SUM(qsrs.max_cpu_time / 1000.) AS total_max_cpu_time_ms,
         SUM((qsrs.max_logical_io_reads * 8 ) / 1024.) AS total_max_logical_io_reads_mb,
         SUM((qsrs.max_physical_io_reads* 8 ) / 1024.) AS total_max_physical_io_reads_mb,
         SUM((qsrs.max_logical_io_writes* 8 ) / 1024.) AS total_max_logical_io_writes_mb,
         SUM((qsrs.max_query_max_used_memory * 8 ) / 1024.)  AS total_max_query_max_used_memory_mb         ';
		 IF @new_columns = 1
			BEGIN
				SET @sql_select += N',
									 SUM((qsrs.avg_log_bytes_used) / 1048576.) / SUM(qsrs.count_executions) AS total_avg_log_bytes_mb,
									 SUM(qsrs.avg_tempdb_space_used) /  SUM(qsrs.count_executions) AS total_avg_tempdb_space,
                                     SUM((qsrs.max_log_bytes_used) / 1048576.) AS total_max_log_bytes_mb,
		                             SUM(qsrs.max_tempdb_space_used) AS total_max_tempdb_space
									 ';
			END;
		IF @new_columns = 0
			BEGIN
				SET @sql_select += N',
									NULL AS total_avg_log_bytes_mb, 
									NULL AS total_avg_tempdb_space,
                                    NULL AS total_max_log_bytes_mb,
                                    NULL AS total_max_tempdb_space
									';
			END;


SET @sql_select += N'FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
					 JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
					 ON qsp.plan_id = qsrs.plan_id
					 JOIN     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
					 ON qsq.query_id = qsp.query_id
					 WHERE  1 = 1
					        AND qsq.is_internal_query = 0
					 	    AND qsp.query_plan IS NOT NULL
					 	  ';


SET @sql_select += @sql_where;

SET @sql_select += 
			N'GROUP BY CONVERT(DATE, qsrs.last_execution_time)
					OPTION (RECOMPILE);
			';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

INSERT #grouped_interval WITH (TABLOCK)
		( flat_date, start_range, end_range, total_avg_duration_ms, 
		  total_avg_cpu_time_ms, total_avg_logical_io_reads_mb, total_avg_physical_io_reads_mb, 
		  total_avg_logical_io_writes_mb, total_avg_query_max_used_memory_mb, total_rowcount, 
		  total_count_executions, total_max_duration_ms, total_max_cpu_time_ms, total_max_logical_io_reads_mb,
          total_max_physical_io_reads_mb, total_max_logical_io_writes_mb, total_max_query_max_used_memory_mb,
          total_avg_log_bytes_mb, total_avg_tempdb_space, total_max_log_bytes_mb, total_max_tempdb_space )                      

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*
The next group of queries looks at plans in the ranges we found in the grouped interval query

We take the highest value from each metric (duration, cpu, etc) and find the top plans by that metric in the range

They insert into the #working_plans table
*/



/*Get longest duration plans*/

RAISERROR(N'Gathering longest duration plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
         qsp.plan_id, qsp.query_id, ''avg duration''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_duration DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH duration_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_duration_ms DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
         qsp.plan_id, qsp.query_id, ''max duration''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_duration DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get longest cpu plans*/

RAISERROR(N'Gathering highest cpu plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg cpu''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_cpu_time DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH cpu_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_cpu_time_ms DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max cpu''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_cpu_time DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest logical read plans*/

RAISERROR(N'Gathering highest logical read plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg logical reads''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_logical_io_reads DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH logical_reads_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_logical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max logical reads''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_logical_io_reads DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest physical read plans*/

RAISERROR(N'Gathering highest physical read plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg physical reads''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_physical_io_reads DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH physical_read_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_physical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max physical reads''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_physical_io_reads DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest logical write plans*/

RAISERROR(N'Gathering highest write plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg writes''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_logical_io_writes DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH logical_writes_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_logical_io_writes_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max writes''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_logical_io_writes DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest memory use plans*/

RAISERROR(N'Gathering highest memory use plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg memory''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_query_max_used_memory DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH memory_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_query_max_used_memory_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max memory''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_query_max_used_memory DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest row count plans*/

RAISERROR(N'Gathering highest row count plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
		 qsp.plan_id, qsp.query_id, ''avg rows''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_rowcount DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


IF @new_columns = 1
BEGIN

RAISERROR(N'Gathering new 2017 new column info...', 0, 1) WITH NOWAIT;

/*Get highest log byte count plans*/

RAISERROR(N'Gathering highest log byte use plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_log_bytes_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''avg log bytes''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_log_bytes_used DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;

RAISERROR(N'Gathering highest log byte use plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_log_bytes_mb DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max log bytes''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_log_bytes_used DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


/*Get highest tempdb use plans*/

RAISERROR(N'Gathering highest tempdb use plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_tempdb_space DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''avg tempdb space''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.avg_tempdb_space_used DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1 
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_tempdb_space DESC )
INSERT #working_plans WITH (TABLOCK) 
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top ) 
		 qsp.plan_id, qsp.query_id, ''max tempdb space''
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'ORDER BY qsrs.max_tempdb_space_used DESC
					OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;


END;


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
OPTION (RECOMPILE);


/*
This dedupes our results so we hopefully don't double-work the same plan
*/

RAISERROR(N'Deduplicating gathered plans', 0, 1) WITH NOWAIT;

WITH dedupe AS (
SELECT * , ROW_NUMBER() OVER (PARTITION BY wp.plan_id ORDER BY wp.plan_id) AS dupes
FROM #working_plans AS wp
)
DELETE dedupe
WHERE dedupe.dupes > 1
OPTION (RECOMPILE);

SET @msg = N'Removed ' + CONVERT(NVARCHAR(10), @@ROWCOUNT) + N' duplicate plan_ids.';
RAISERROR(@msg, 0, 1) WITH NOWAIT;


/*
This gathers data for the #working_metrics table
*/


RAISERROR(N'Collecting worker metrics', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
SELECT ' + QUOTENAME(@DatabaseName, '''') + N' AS database_name, wp.plan_id, wp.query_id,
       QUOTENAME(object_schema_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N'))) + ''.'' +
	   QUOTENAME(object_name(qsq.object_id, DB_ID(' + QUOTENAME(@DatabaseName, '''') + N'))) AS proc_or_function_name,
	   qsq.batch_sql_handle, qsq.query_hash, qsq.query_parameterization_type_desc, qsq.count_compiles, 
	   (qsq.avg_compile_duration / 1000.), 
	   (qsq.last_compile_duration / 1000.), 
	   (qsq.avg_bind_duration / 1000.), 
	   (qsq.last_bind_duration / 1000.), 
	   (qsq.avg_bind_cpu_time / 1000.), 
	   (qsq.last_bind_cpu_time / 1000.), 
	   (qsq.avg_optimize_duration / 1000.), 
	   (qsq.last_optimize_duration / 1000.), 
	   (qsq.avg_optimize_cpu_time / 1000.), 
	   (qsq.last_optimize_cpu_time / 1000.), 
	   (qsq.avg_compile_memory_kb / 1024.), 
	   (qsq.last_compile_memory_kb / 1024.), 
	   qsrs.execution_type_desc, qsrs.first_execution_time, qsrs.last_execution_time, qsrs.count_executions, 
	   (qsrs.avg_duration / 1000.), 
	   (qsrs.last_duration / 1000.),
	   (qsrs.min_duration / 1000.), 
	   (qsrs.max_duration / 1000.), 
	   (qsrs.avg_cpu_time / 1000.), 
	   (qsrs.last_cpu_time / 1000.), 
	   (qsrs.min_cpu_time / 1000.), 
	   (qsrs.max_cpu_time / 1000.), 
	   ((qsrs.avg_logical_io_reads * 8 ) / 1024.), 
	   ((qsrs.last_logical_io_reads * 8 ) / 1024.), 
	   ((qsrs.min_logical_io_reads * 8 ) / 1024.), 
	   ((qsrs.max_logical_io_reads * 8 ) / 1024.), 
	   ((qsrs.avg_logical_io_writes * 8 ) / 1024.), 
	   ((qsrs.last_logical_io_writes * 8 ) / 1024.), 
	   ((qsrs.min_logical_io_writes * 8 ) / 1024.), 
	   ((qsrs.max_logical_io_writes * 8 ) / 1024.), 
	   ((qsrs.avg_physical_io_reads * 8 ) / 1024.), 
	   ((qsrs.last_physical_io_reads * 8 ) / 1024.), 
	   ((qsrs.min_physical_io_reads * 8 ) / 1024.), 
	   ((qsrs.max_physical_io_reads * 8 ) / 1024.), 
	   (qsrs.avg_clr_time / 1000.), 
	   (qsrs.last_clr_time / 1000.), 
	   (qsrs.min_clr_time / 1000.), 
	   (qsrs.max_clr_time / 1000.), 
	   qsrs.avg_dop, qsrs.last_dop, qsrs.min_dop, qsrs.max_dop, 
	   ((qsrs.avg_query_max_used_memory * 8 ) / 1024.), 
	   ((qsrs.last_query_max_used_memory * 8 ) / 1024.), 
	   ((qsrs.min_query_max_used_memory * 8 ) / 1024.), 
	   ((qsrs.max_query_max_used_memory * 8 ) / 1024.), 
	   qsrs.avg_rowcount, qsrs.last_rowcount, qsrs.min_rowcount, qsrs.max_rowcount,';
		
		IF @new_columns = 1
			BEGIN
			SET @sql_select += N'
			qsrs.avg_num_physical_io_reads, qsrs.last_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,
			(qsrs.avg_log_bytes_used / 100000000),
			(qsrs.last_log_bytes_used / 100000000),
			(qsrs.min_log_bytes_used / 100000000),
			(qsrs.max_log_bytes_used / 100000000),
			((qsrs.avg_tempdb_space_used * 8 ) / 1024.),
			((qsrs.last_tempdb_space_used * 8 ) / 1024.),
			((qsrs.min_tempdb_space_used * 8 ) / 1024.),
			((qsrs.max_tempdb_space_used * 8 ) / 1024.)
			';
			END;	
		IF @new_columns = 0
			BEGIN
			SET @sql_select += N'
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL
			';
			END;
SET @sql_select +=
N'FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
AND qsp.query_id = wp.query_id
JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

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
		  last_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory, avg_rowcount, last_rowcount, min_rowcount, max_rowcount,
		  /* 2017 only columns */
		  avg_num_physical_io_reads, last_num_physical_io_reads, min_num_physical_io_reads, max_num_physical_io_reads,
		  avg_log_bytes_used, last_log_bytes_used, min_log_bytes_used, max_log_bytes_used,
		  avg_tempdb_space_used, last_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used )

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;



/*This just helps us classify our queries*/
UPDATE #working_metrics
SET proc_or_function_name = N'Statement'
WHERE proc_or_function_name IS NULL
OPTION(RECOMPILE);

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
    WITH patterns AS (
         SELECT query_id, planid_path = STUFF((SELECT DISTINCT N'', '' + RTRIM(qsp2.plan_id)
             									FROM ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp2
             									WHERE qsp.query_id = qsp2.query_id
             									FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 2, N'''')
         FROM ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_plan AS qsp
    )
    UPDATE wm
    SET wm.query_id_all_plan_ids = patterns.planid_path
    FROM #working_metrics AS wm
    JOIN patterns
    ON  wm.query_id = patterns.query_id
    OPTION (RECOMPILE);
'

EXEC sys.sp_executesql  @stmt = @sql_select;

/*
This gathers data for the #working_plan_text table
*/


RAISERROR(N'Gathering working plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
SELECT ' + QUOTENAME(@DatabaseName, '''') + N' AS database_name,  wp.plan_id, wp.query_id,
	   qsp.plan_group_id, qsp.engine_version, qsp.compatibility_level, qsp.query_plan_hash, TRY_CONVERT(XML, qsp.query_plan), qsp.is_online_index_plan, qsp.is_trivial_plan, 
	   qsp.is_parallel_plan, qsp.is_forced_plan, qsp.is_natively_compiled, qsp.force_failure_count, qsp.last_force_failure_reason_desc, qsp.count_compiles, 
	   qsp.initial_compile_start_time, qsp.last_compile_start_time, qsp.last_execution_time, 
	   (qsp.avg_compile_duration / 1000.), 
	   (qsp.last_compile_duration / 1000.), 
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

INSERT #working_plan_text WITH (TABLOCK)
		( database_name, plan_id, query_id, 
		  plan_group_id, engine_version, compatibility_level, query_plan_hash, query_plan_xml, is_online_index_plan, is_trivial_plan, 
		  is_parallel_plan, is_forced_plan, is_natively_compiled, force_failure_count, last_force_failure_reason_desc, count_compiles, 
		  initial_compile_start_time, last_compile_start_time, last_execution_time, avg_compile_duration, last_compile_duration, 
		  query_sql_text, statement_sql_handle, is_part_of_encrypted_module, has_restricted_text )

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;



/*
This gets us context settings for our queries and adds it to the #working_plan_text table
*/

RAISERROR(N'Gathering context settings', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
UPDATE wp
SET wp.context_settings = SUBSTRING(
					    CASE WHEN (CAST(qcs.set_options AS INT) & 1 = 1) THEN '', ANSI_PADDING'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8 = 8) THEN '', CONCAT_NULL_YIELDS_NULL'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 16 = 16) THEN '', ANSI_WARNINGS'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 32 = 32) THEN '', ANSI_NULLS'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 64 = 64) THEN '', QUOTED_IDENTIFIER'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 4096 = 4096) THEN '', ARITH_ABORT'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8192 = 8192) THEN '', NUMERIC_ROUNDABORT'' ELSE '''' END 
					    , 2, 200000)
FROM #working_plan_text wp
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@DatabaseName) + N'.sys.query_context_settings AS qcs
ON qcs.context_settings_id = qsq.context_settings_id
OPTION (RECOMPILE);
';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select;


/*This adds the patterns we found from each interval to the #working_plan_text table*/

RAISERROR(N'Add patterns to working plans', 0, 1) WITH NOWAIT;

UPDATE wpt
SET wpt.pattern = wp.pattern
FROM #working_plans AS wp
JOIN #working_plan_text AS wpt
ON wpt.plan_id = wp.plan_id
AND wpt.query_id = wp.query_id
OPTION (RECOMPILE);

/*This cleans up query text a bit*/

RAISERROR(N'Clean awkward characters from query text', 0, 1) WITH NOWAIT;

UPDATE b
SET b.query_sql_text = REPLACE(REPLACE(REPLACE(b.query_sql_text, @cr, ' '), @lf, ' '), @tab, '  ')
FROM #working_plan_text AS b
OPTION (RECOMPILE);


/*This populates #working_wait_stats when available*/

IF @waitstats = 1

	BEGIN
	
	RAISERROR(N'Collecting wait stats info', 0, 1) WITH NOWAIT;
	
	
		SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
		SET @sql_select += N'
		SELECT   qws.plan_id,
		         qws.wait_category,
		         qws.wait_category_desc,
		         SUM(qws.total_query_wait_time_ms) AS total_query_wait_time_ms,
		         SUM(qws.avg_query_wait_time_ms) AS avg_query_wait_time_ms,
		         SUM(qws.last_query_wait_time_ms) AS last_query_wait_time_ms,
		         SUM(qws.min_query_wait_time_ms) AS min_query_wait_time_ms,
		         SUM(qws.max_query_wait_time_ms) AS max_query_wait_time_ms
		FROM     ' + QUOTENAME(@DatabaseName) + N'.sys.query_store_wait_stats qws
		JOIN #working_plans AS wp
		ON qws.plan_id = wp.plan_id
		GROUP BY qws.plan_id, qws.wait_category, qws.wait_category_desc
		HAVING SUM(qws.min_query_wait_time_ms) >= 5
		OPTION (RECOMPILE);
		';
		
		IF @Debug = 1
			PRINT @sql_select;
		
		IF @sql_select IS NULL
		    BEGIN
		        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
		        RETURN;
		    END;
		
		INSERT #working_wait_stats WITH (TABLOCK)
				( plan_id, wait_category, wait_category_desc, total_query_wait_time_ms, avg_query_wait_time_ms, last_query_wait_time_ms, min_query_wait_time_ms, max_query_wait_time_ms )
		
		EXEC sys.sp_executesql  @stmt = @sql_select;
	
	
	/*This updates #working_plan_text with the top three waits from the wait stats DMV*/
	
	RAISERROR(N'Update working_plan_text with top three waits', 0, 1) WITH NOWAIT;
	
	
		UPDATE wpt
		SET wpt.top_three_waits = x.top_three_waits 
		FROM #working_plan_text AS wpt
		JOIN (
			SELECT wws.plan_id,
				   top_three_waits = STUFF((SELECT TOP 3 N', ' + wws2.wait_category_desc + N' (' + CONVERT(NVARCHAR(20), SUM(CONVERT(BIGINT, wws2.avg_query_wait_time_ms))) + N' ms) '
												FROM #working_wait_stats AS wws2
												WHERE wws.plan_id = wws2.plan_id
												GROUP BY wws2.wait_category_desc
												ORDER BY SUM(wws2.avg_query_wait_time_ms) DESC
												FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'')							
			FROM #working_wait_stats AS wws
			GROUP BY wws.plan_id
		) AS x 
		ON x.plan_id = wpt.plan_id
		OPTION (RECOMPILE);

END;

/*End wait stats population*/

UPDATE #working_plan_text
SET top_three_waits = CASE 
						WHEN @waitstats = 0 
                        THEN N'The query store waits stats DMV is not available'
						ELSE N'No Significant waits detected!'
						END
WHERE top_three_waits IS NULL
OPTION(RECOMPILE);

END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure populating temp tables.', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;

IF (@SkipXML = 0)
BEGIN TRY 
BEGIN

/*
This sets up the #working_warnings table with the IDs we're interested in so we can tie warnings back to them 
*/

RAISERROR(N'Populate working warnings table with gathered plans', 0, 1) WITH NOWAIT;


SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
SET @sql_select += N'
SELECT DISTINCT wp.plan_id, wp.query_id, qsq.query_hash, qsqt.statement_sql_handle
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
	';

SET @sql_select += @sql_where;

SET @sql_select +=  N'OPTION (RECOMPILE);
					';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

INSERT #working_warnings  WITH (TABLOCK)
	( plan_id, query_id, query_hash, sql_handle )
EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;

/*
This looks for queries in the query stores that we picked up from an internal that have multiple plans in cache

This and several of the following queries all replaced XML parsing to find plan attributes. Sweet.

Thanks, Query Store
*/

RAISERROR(N'Populating object name in #working_warnings', 0, 1) WITH NOWAIT;
UPDATE w
SET    w.proc_or_function_name = ISNULL(wm.proc_or_function_name, N'Statement')
FROM   #working_warnings AS w
JOIN   #working_metrics AS wm
ON w.plan_id = wm.plan_id
   AND w.query_id = wm.query_id
OPTION (RECOMPILE);


RAISERROR(N'Checking for multiple plans', 0, 1) WITH NOWAIT;

SET @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
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
	';

SET @sql_select += @sql_where;

SET @sql_select += 
N'GROUP BY wp.query_id
  HAVING COUNT(qsp.plan_id) > 1
) AS x
    ON ww.query_id = x.query_id
OPTION (RECOMPILE);
';

IF @Debug = 1
	PRINT @sql_select;

IF @sql_select IS NULL
    BEGIN
        RAISERROR(N'@sql_select is NULL', 0, 1) WITH NOWAIT;
        RETURN;
    END;

EXEC sys.sp_executesql  @stmt = @sql_select, 
						@params = @sp_params,
						@sp_Top = @Top, @sp_StartDate = @StartDate, @sp_EndDate = @EndDate, @sp_MinimumExecutionCount = @MinimumExecutionCount, @sp_MinDuration = @duration_filter_ms, @sp_StoredProcName = @StoredProcName, @sp_PlanIdFilter = @PlanIdFilter, @sp_QueryIdFilter = @QueryIdFilter;

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
OPTION (RECOMPILE);


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
OPTION (RECOMPILE);


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
   AND ww.proc_or_function_name = 'Statement'
OPTION (RECOMPILE);


/*
This looks for cursors
*/

RAISERROR(N'Checking for cursors', 0, 1) WITH NOWAIT;
UPDATE ww
SET    ww.is_cursor = 1
FROM   #working_warnings AS ww
JOIN   #working_plan_text AS wp
ON ww.plan_id = wp.plan_id
   AND ww.query_id = wp.query_id
   AND wp.plan_group_id > 0
OPTION (RECOMPILE);


UPDATE ww
SET    ww.is_cursor = 1
FROM   #working_warnings AS ww
JOIN   #working_plan_text AS wp
ON ww.plan_id = wp.plan_id
   AND ww.query_id = wp.query_id
WHERE ww.query_hash = 0x0000000000000000
OR wp.query_plan_hash = 0x0000000000000000
OPTION (RECOMPILE);

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
OPTION (RECOMPILE);

/*This looks for old CE*/

RAISERROR(N'Checking for legacy CE', 0, 1) WITH NOWAIT;

UPDATE w
SET w.downlevel_estimator = 1
FROM #working_warnings AS w
JOIN #working_plan_text AS wpt
ON w.plan_id = wpt.plan_id
AND w.query_id = wpt.query_id
/*PLEASE DON'T TELL ANYONE I DID THIS*/
WHERE PARSENAME(wpt.engine_version, 4) < PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)
OPTION (RECOMPILE);
/*NO SERIOUSLY THIS IS A HORRIBLE IDEA*/


/*Plans that compile 2x more than they execute*/

RAISERROR(N'Checking for plans that compile 2x more than they execute', 0, 1) WITH NOWAIT;

UPDATE ww
SET    ww.is_compile_more = 1
FROM   #working_warnings AS ww
JOIN   #working_metrics AS wm
ON ww.plan_id = wm.plan_id
   AND ww.query_id = wm.query_id
   AND wm.count_compiles > (wm.count_executions * 2)
OPTION (RECOMPILE);

/*Plans that compile 2x more than they execute*/

RAISERROR(N'Checking for plans that take more than 5 seconds to bind, compile, or optimize', 0, 1) WITH NOWAIT;

UPDATE ww
SET    ww.is_slow_plan = 1
FROM   #working_warnings AS ww
JOIN   #working_metrics AS wm
ON ww.plan_id = wm.plan_id
   AND ww.query_id = wm.query_id
   AND (wm.avg_bind_duration > 5000
		OR 
		wm.avg_compile_duration > 5000
		OR
		wm.avg_optimize_duration > 5000
		OR 
		wm.avg_optimize_cpu_time > 5000)
OPTION (RECOMPILE);



/*
This parses the XML from our top plans into smaller chunks for easier consumption
*/

RAISERROR(N'Begin XML nodes parsing', 0, 1) WITH NOWAIT;

RAISERROR(N'Inserting #statements', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, statement, is_cursor )	
	SELECT ww.plan_id, ww.query_id, ww.query_hash, ww.sql_handle, q.n.query('.') AS statement, 0 AS is_cursor
	FROM #working_warnings AS ww
	JOIN #working_plan_text AS wp
	ON ww.plan_id = wp.plan_id
	AND ww.query_id = wp.query_id
    CROSS APPLY wp.query_plan_xml.nodes('//p:StmtSimple') AS q(n) 
OPTION (RECOMPILE);

RAISERROR(N'Inserting parsed cursor XML to #statements', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #statements WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, statement, is_cursor )
	SELECT ww.plan_id, ww.query_id, ww.query_hash, ww.sql_handle, q.n.query('.') AS statement, 1 AS is_cursor
	FROM #working_warnings AS ww
	JOIN #working_plan_text AS wp
	ON ww.plan_id = wp.plan_id
	AND ww.query_id = wp.query_id
    CROSS APPLY wp.query_plan_xml.nodes('//p:StmtCursor') AS q(n) 
OPTION (RECOMPILE);

RAISERROR(N'Inserting to #query_plan', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #query_plan WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, query_plan )
SELECT  s.plan_id, s.query_id, s.query_hash, s.sql_handle, q.n.query('.') AS query_plan
FROM    #statements AS s
        CROSS APPLY s.statement.nodes('//p:QueryPlan') AS q(n) 
OPTION (RECOMPILE);

RAISERROR(N'Inserting to #relop', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #relop WITH (TABLOCK) ( plan_id, query_id, query_hash, sql_handle, relop)
SELECT  qp.plan_id, qp.query_id, qp.query_hash, qp.sql_handle, q.n.query('.') AS relop
FROM    #query_plan qp
        CROSS APPLY qp.query_plan.nodes('//p:RelOp') AS q(n) 
OPTION (RECOMPILE);


-- statement level checks

RAISERROR(N'Performing compile timeout checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     b.compile_timeout = 1 
FROM    #statements s
JOIN #working_warnings AS b
ON  s.query_hash = b.query_hash
WHERE s.statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
OPTION (RECOMPILE);


RAISERROR(N'Performing compile memory limit exceeded checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET     b.compile_memory_limit_exceeded = 1 
FROM    #statements s
JOIN #working_warnings AS b
ON  s.query_hash = b.query_hash
WHERE s.statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing index DML checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
index_dml AS (
	SELECT	s.query_hash,	
			index_dml = CASE WHEN s.statement.exist('//p:StmtSimple/@StatementType[.="CREATE INDEX"]') = 1 THEN 1
							 WHEN s.statement.exist('//p:StmtSimple/@StatementType[.="DROP INDEX"]') = 1 THEN 1
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
		   table_dml = CASE WHEN s.statement.exist('//p:StmtSimple/@StatementType[.="CREATE TABLE"]') = 1 THEN 1
							WHEN s.statement.exist('//p:StmtSimple/@StatementType[.="DROP OBJECT"]') = 1 THEN 1
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
END;


RAISERROR(N'Gathering trivial plans', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
UPDATE b
SET b.is_trivial = 1
FROM #working_warnings AS b
JOIN (
SELECT  s.sql_handle
FROM    #statements AS s
JOIN    (   SELECT  r.sql_handle
            FROM    #relop AS r
            WHERE   r.relop.exist('//p:RelOp[contains(@LogicalOp, "Scan")]') = 1 ) AS r
    ON r.sql_handle = s.sql_handle
WHERE   s.statement.exist('//p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1
) AS s
ON b.sql_handle = s.sql_handle
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Gathering row estimates', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
INSERT #est_rows (query_hash, estimated_rows)
SELECT DISTINCT 
		CONVERT(BINARY(8), RIGHT('0000000000000000' + SUBSTRING(c.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18), 16), 2) AS query_hash,
		c.n.value('(/p:StmtSimple/@StatementEstRows)[1]', 'FLOAT') AS estimated_rows
FROM   #statements AS s
CROSS APPLY s.statement.nodes('/p:StmtSimple') AS c(n)
WHERE  c.n.exist('/p:StmtSimple[@StatementEstRows > 0]') = 1;

	UPDATE b
		SET b.estimated_rows = er.estimated_rows
	FROM #working_warnings AS b
	JOIN #est_rows er
	ON er.query_hash = b.query_hash
	OPTION (RECOMPILE);
END;


/*Begin plan cost calculations*/
RAISERROR(N'Gathering statement costs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #plan_cost WITH (TABLOCK)
	( query_plan_cost, sql_handle, plan_id )
SELECT  DISTINCT
		s.statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') query_plan_cost,
		s.sql_handle,
		s.plan_id
FROM    #statements s
OUTER APPLY s.statement.nodes('/p:StmtSimple') AS q(n)
WHERE s.statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') > 0
OPTION (RECOMPILE);


RAISERROR(N'Updating statement costs', 0, 1) WITH NOWAIT;
WITH pc AS (
	SELECT SUM(DISTINCT pc.query_plan_cost) AS queryplancostsum, pc.sql_handle, pc.plan_id
	FROM #plan_cost AS pc
	GROUP BY pc.sql_handle, pc.plan_id
	)
	UPDATE b
		SET b.query_cost = ISNULL(pc.queryplancostsum, 0)
		FROM  #working_warnings AS b
		JOIN pc
		ON pc.sql_handle = b.sql_handle
		AND pc.plan_id = b.plan_id
OPTION (RECOMPILE);


/*End plan cost calculations*/


RAISERROR(N'Checking for plan warnings', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET b.plan_warnings = 1
FROM    #query_plan qp
JOIN #working_warnings b
ON  qp.sql_handle = b.sql_handle
AND qp.query_plan.exist('/p:QueryPlan/p:Warnings') = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking for implicit conversion', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET b.implicit_conversions = 1
FROM    #query_plan qp
JOIN #working_warnings b
ON  qp.sql_handle = b.sql_handle
AND qp.query_plan.exist('/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') = 1
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Performing busy loops checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE p
SET    busy_loops = CASE WHEN (x.estimated_executions / 100.0) > x.estimated_rows THEN 1 END 
FROM   #working_warnings p
       JOIN (
            SELECT qs.sql_handle,
                   relop.value('sum(/p:RelOp/@EstimateRows)', 'float') AS estimated_rows ,
                   relop.value('sum(/p:RelOp/@EstimateRewinds)', 'float') + relop.value('sum(/p:RelOp/@EstimateRebinds)', 'float') + 1.0 AS estimated_executions 
            FROM   #relop qs
       ) AS x ON p.sql_handle = x.sql_handle
OPTION (RECOMPILE);
END; 


RAISERROR(N'Performing TVF join check', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE p
SET    p.tvf_join = CASE WHEN x.tvf_join = 1 THEN 1 END
FROM   #working_warnings p
       JOIN (
			SELECT r.sql_handle,
				   1 AS tvf_join
			FROM #relop AS r
			WHERE r.relop.exist('//p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
			AND   r.relop.exist('//p:RelOp[contains(@LogicalOp, "Join")]') = 1
       ) AS x ON p.sql_handle = x.sql_handle
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
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
END; 


RAISERROR(N'Checking for table variables', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
, x AS (
SELECT r.sql_handle,
	   c.n.value('substring(@Table, 2, 1)','VARCHAR(100)') AS first_char
FROM   #relop r
CROSS APPLY r.relop.nodes('//p:Object') AS c(n)
)
UPDATE b
SET	   b.is_table_variable = 1
FROM #working_warnings b
JOIN x ON x.sql_handle = b.sql_handle
JOIN #working_metrics AS wm
ON b.plan_id = wm.plan_id
AND b.query_id = wm.query_id
AND wm.batch_sql_handle IS NOT NULL
WHERE x.first_char = '@'
OPTION (RECOMPILE);


IF @ExpertMode > 0
BEGIN
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
END;


RAISERROR(N'Checking for expensive key lookups', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.key_lookup_cost = x.key_lookup_cost
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   MAX(r.relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS key_lookup_cost
FROM   #relop r
WHERE r.relop.exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
GROUP BY r.sql_handle
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);


RAISERROR(N'Checking for expensive remote queries', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.remote_query_cost = x.remote_query_cost
FROM #working_warnings b
JOIN (
SELECT 
       r.sql_handle,
	   MAX(r.relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) AS remote_query_cost
FROM   #relop r
WHERE r.relop.exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
GROUP BY r.sql_handle
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);


RAISERROR(N'Checking for expensive sorts', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET sort_cost = y.max_sort_cost 
FROM #working_warnings b
JOIN (
	SELECT x.sql_handle, MAX((x.sort_io + x.sort_cpu)) AS max_sort_cost
	FROM (
		SELECT 
		       qs.sql_handle,
			   relop.value('sum(/p:RelOp/@EstimateIO)', 'float') AS sort_io,
			   relop.value('sum(/p:RelOp/@EstimateCPU)', 'float') AS sort_cpu
		FROM   #relop qs
		WHERE [relop].exist('/p:RelOp[(@PhysicalOp[.="Sort"])]') = 1
		) AS x
	GROUP BY x.sql_handle
	) AS y
ON  b.sql_handle = y.sql_handle
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
FROM #working_warnings b
JOIN #statements AS s
ON b.sql_handle = s.sql_handle
CROSS APPLY s.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE n1.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1
AND s.is_cursor = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking if cursor is Forward Only', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_forward_only_cursor = 1
FROM #working_warnings b
JOIN #statements AS s
ON b.sql_handle = s.sql_handle
CROSS APPLY s.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE n1.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1
AND s.is_cursor = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking if cursor is Fast Forward', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_fast_forward_cursor = 1
FROM #working_warnings b
JOIN #statements AS s
ON b.sql_handle = s.sql_handle
CROSS APPLY s.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE n1.fn.exist('//p:CursorPlan/@CursorActualType[.="FastForward"]') = 1
AND s.is_cursor = 1
OPTION (RECOMPILE);


RAISERROR(N'Checking for Dynamic cursors', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_cursor_dynamic =  1
FROM #working_warnings b
JOIN #statements AS s
ON b.sql_handle = s.sql_handle
CROSS APPLY s.statement.nodes('/p:StmtCursor') AS n1(fn)
WHERE n1.fn.exist('//p:CursorPlan/@CursorActualType[.="Dynamic"]') = 1
AND s.is_cursor = 1
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
END;


IF @ExpertMode > 0
BEGIN
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
END;


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


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking modification queries that hit lots of indexes', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),	
IndexOps AS 
(
	SELECT 
	r.query_hash,
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
		SELECT	ios.query_hash,
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
		GROUP BY ios.query_hash) 
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
JOIN iops ON  iops.query_hash = b.query_hash
OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
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
END;

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


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.columnstore_row_mode = x.is_row_mode
FROM #working_warnings AS b
JOIN (
SELECT 
       r.sql_handle,
	   r.relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') AS is_row_mode
FROM   #relop r
WHERE r.relop.exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
) AS x ON x.sql_handle = b.sql_handle
OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
RAISERROR('Checking for row level security only', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET  b.is_row_level = 1
FROM #working_warnings b
JOIN #statements s 
ON s.query_hash = b.query_hash 
WHERE s.statement.exist('/p:StmtSimple/@SecurityPolicyApplied[.="true"]') = 1
OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
RAISERROR('Checking for wonky Index Spools', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES (
    'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
, selects
AS ( SELECT s.plan_id, s.query_id
     FROM   #statements AS s
     WHERE  s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 )
, spools
AS ( SELECT DISTINCT r.plan_id,
       r.query_id,
	   c.n.value('@EstimateRows', 'FLOAT') AS estimated_rows,
       c.n.value('@EstimateIO', 'FLOAT') AS estimated_io,
       c.n.value('@EstimateCPU', 'FLOAT') AS estimated_cpu,
       c.n.value('@EstimateRebinds', 'FLOAT') AS estimated_rebinds
FROM   #relop AS r
JOIN   selects AS s
ON s.plan_id = r.plan_id
   AND s.query_id = r.query_id
CROSS APPLY r.relop.nodes('/p:RelOp') AS c(n)
WHERE  r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1
)
UPDATE ww
		SET ww.index_spool_rows = sp.estimated_rows,
			ww.index_spool_cost = ((sp.estimated_io * sp.estimated_cpu) * CASE WHEN sp.estimated_rebinds < 1 THEN 1 ELSE sp.estimated_rebinds END)

FROM #working_warnings ww
JOIN spools sp
ON ww.plan_id = sp.plan_id
AND ww.query_id = sp.query_id
OPTION (RECOMPILE);
END;


IF (PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)) >= 14
OR ((PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)) = 13
	AND PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 2) >= 5026)

BEGIN

RAISERROR(N'Beginning 2017 and 2016 SP2 specfic checks', 0, 1) WITH NOWAIT;

IF @ExpertMode > 0
BEGIN
RAISERROR('Gathering stats information', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
INSERT #stats_agg WITH (TABLOCK)
	(sql_handle, last_update, modification_count, sampling_percent, [statistics], [table], [schema], [database])
SELECT qp.sql_handle,
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
OPTION (RECOMPILE);
END;


IF (PARSENAME(CONVERT(VARCHAR(128), SERVERPROPERTY ('PRODUCTVERSION')), 4)) >= 14
	AND @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for Adaptive Joins', 0, 1) WITH NOWAIT;
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
END; 


IF @ExpertMode > 0
BEGIN;
RAISERROR(N'Checking for Row Goals', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
row_goals AS(
SELECT qs.query_hash
FROM   #relop qs
WHERE relop.value('sum(/p:RelOp/@EstimateRowsWithoutRowGoal)', 'float') > 0
)
UPDATE b
SET b.is_row_goal = 1
FROM #working_warnings b
JOIN row_goals
ON b.query_hash = row_goals.query_hash
OPTION (RECOMPILE);
END;

END; 


RAISERROR(N'Performing query level checks', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE  b
SET     b.missing_index_count = query_plan.value('count(//p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int') ,
		b.unmatched_index_count = CASE WHEN is_trivial <> 1 THEN query_plan.value('count(//p:QueryPlan/p:UnmatchedIndexes/p:Parameterization/p:Object)', 'int') END
FROM    #query_plan qp
JOIN #working_warnings AS b
ON b.query_hash = qp.query_hash
OPTION (RECOMPILE);


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
          SELECT DISTINCT N', ' + CONVERT(NVARCHAR(5), tf2.trace_flag)
          FROM  tf_pretty AS tf2 
          WHERE tf1.sql_handle = tf2.sql_handle 
		  AND tf2.scope = 'Global'
        FOR XML PATH(N'')), 1, 2, N''
      ) AS global_trace_flags,
    STUFF((
          SELECT DISTINCT N', ' + CONVERT(NVARCHAR(5), tf2.trace_flag)
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
OPTION (RECOMPILE);


RAISERROR(N'Checking for MSTVFs', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_mstvf = 1
FROM #relop AS r
JOIN #working_warnings AS b
ON b.sql_handle = r.sql_handle
WHERE  r.relop.exist('/p:RelOp[(@EstimateRows="100" or @EstimateRows="1") and @LogicalOp="Table-valued function"]') = 1
OPTION (RECOMPILE);

IF @ExpertMode > 0
BEGIN
RAISERROR(N'Checking for many to many merge joins', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET b.is_mm_join = 1
FROM #relop AS r
JOIN #working_warnings AS b
ON b.sql_handle = r.sql_handle
WHERE  r.relop.exist('/p:RelOp/p:Merge/@ManyToMany[.="1"]') = 1
OPTION (RECOMPILE);
END;


IF @ExpertMode > 0
BEGIN
RAISERROR(N'Is Paul White Electric?', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p),
is_paul_white_electric AS (
SELECT 1 AS [is_paul_white_electric], 
r.sql_handle
FROM #relop AS r
CROSS APPLY r.relop.nodes('//p:RelOp') c(n)
WHERE c.n.exist('@PhysicalOp[.="Switch"]') = 1
)
UPDATE b
SET    b.is_paul_white_electric = ipwe.is_paul_white_electric
FROM   #working_warnings AS b
JOIN is_paul_white_electric ipwe 
ON ipwe.sql_handle = b.sql_handle 
OPTION (RECOMPILE);
END;



RAISERROR(N'Checking for non-sargable predicates', 0, 1) WITH NOWAIT;
WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
, nsarg
    AS (   SELECT       r.query_hash, 1 AS fn, 0 AS jo, 0 AS lk
           FROM         #relop AS r
           CROSS APPLY  r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator') AS ca(x)
           WHERE        (   ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName') = 1
                            OR     ca.x.exist('//p:ScalarOperator/p:IF') = 1 )
           UNION ALL
           SELECT       r.query_hash, 0 AS fn, 1 AS jo, 0 AS lk
           FROM         #relop AS r
           CROSS APPLY  r.relop.nodes('/p:RelOp//p:ScalarOperator') AS ca(x)
           WHERE        r.relop.exist('/p:RelOp[contains(@LogicalOp, "Join")]') = 1
                        AND ca.x.exist('//p:ScalarOperator[contains(@ScalarString, "Expr")]') = 1
           UNION ALL
           SELECT       r.query_hash, 0 AS fn, 0 AS jo, 1 AS lk
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
                    nsarg.query_hash
           FROM     nsarg
           WHERE    nsarg.fn = 1
                    OR nsarg.jo = 1
                    OR nsarg.lk = 1 )
UPDATE  b
SET     b.is_nonsargable = 1
FROM    d_nsarg AS d
JOIN    #working_warnings AS b
    ON b.query_hash = d.query_hash
OPTION ( RECOMPILE );


        RAISERROR(N'Getting information about implicit conversions and stored proc parameters', 0, 1) WITH NOWAIT;

		RAISERROR(N'Getting variable info', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #variable_info ( query_hash, sql_handle, proc_name, variable_name, variable_datatype, compile_time_value )
		SELECT      DISTINCT
		            qp.query_hash,
		            qp.sql_handle,
		            b.proc_or_function_name AS proc_name,
		            q.n.value('@Column', 'NVARCHAR(258)') AS variable_name,
		            q.n.value('@ParameterDataType', 'NVARCHAR(258)') AS variable_datatype,
		            q.n.value('@ParameterCompiledValue', 'NVARCHAR(258)') AS compile_time_value
		FROM        #query_plan AS qp
           JOIN     #working_warnings AS b
           ON (b.query_hash = qp.query_hash AND b.proc_or_function_name = 'adhoc')
		   OR (b.sql_handle = qp.sql_handle AND b.proc_or_function_name <> 'adhoc')
		CROSS APPLY qp.query_plan.nodes('//p:QueryPlan/p:ParameterList/p:ColumnReference') AS q(n)
		OPTION (RECOMPILE);

		RAISERROR(N'Getting conversion info', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #conversion_info ( query_hash, sql_handle, proc_name, expression )
		SELECT      DISTINCT
		            qp.query_hash,
		            qp.sql_handle,
		            b.proc_or_function_name AS proc_name,
		            qq.c.value('@Expression', 'NVARCHAR(4000)') AS expression
		FROM        #query_plan AS qp
		   JOIN     #working_warnings AS b
           ON (b.query_hash = qp.query_hash AND b.proc_or_function_name = 'adhoc')
		   OR (b.sql_handle = qp.sql_handle AND b.proc_or_function_name <> 'adhoc')
		CROSS APPLY qp.query_plan.nodes('//p:QueryPlan/p:Warnings/p:PlanAffectingConvert') AS qq(c)
		WHERE       qq.c.exist('@ConvertIssue[.="Seek Plan"]') = 1
		            AND b.implicit_conversions = 1
		OPTION (RECOMPILE);

		RAISERROR(N'Parsing conversion info', 0, 1) WITH NOWAIT;
		INSERT #stored_proc_info ( sql_handle, query_hash, proc_name, variable_name, variable_datatype, converted_column_name, column_name, converted_to, compile_time_value )
		SELECT ci.sql_handle,
		       ci.query_hash,
		       ci.proc_name,
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

		RAISERROR(N'Updating variables inserted procs', 0, 1) WITH NOWAIT;
		UPDATE sp
		SET sp.variable_datatype = vi.variable_datatype,
			sp.compile_time_value = vi.compile_time_value
		FROM   #stored_proc_info AS sp
		JOIN #variable_info AS vi
		ON (sp.proc_name = 'adhoc' AND sp.query_hash = vi.query_hash)
		OR 	(sp.proc_name <> 'adhoc' AND sp.sql_handle = vi.sql_handle)
		AND sp.variable_name = vi.variable_name
		OPTION (RECOMPILE);
		
		
		RAISERROR(N'Inserting variables for other procs', 0, 1) WITH NOWAIT;
		INSERT #stored_proc_info 
				( sql_handle, query_hash, variable_name, variable_datatype, compile_time_value, proc_name )
		SELECT vi.sql_handle, vi.query_hash, vi.variable_name, vi.variable_datatype, vi.compile_time_value, vi.proc_name
		FROM #variable_info AS vi
		WHERE NOT EXISTS
		(
			SELECT * 
			FROM   #stored_proc_info AS sp
			WHERE (sp.proc_name = 'adhoc' AND sp.query_hash = vi.query_hash)
			OR 	(sp.proc_name <> 'adhoc' AND sp.sql_handle = vi.sql_handle)
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
														  CHARINDEX(')', s.compile_time_value) - 1 - CHARINDEX('(', s.compile_time_value)
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
				SELECT  x.sql_handle,
						N'SET ANSI_NULLS ' + CASE WHEN [ANSI_NULLS] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
						N'SET ANSI_PADDING ' + CASE WHEN [ANSI_PADDING] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
						N'SET ANSI_WARNINGS ' + CASE WHEN [ANSI_WARNINGS] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
						N'SET ARITHABORT ' + CASE WHEN [ARITHABORT] = 'true' THEN N'ON ' ELSE N' OFF ' END + NCHAR(10) +
						N'SET CONCAT_NULL_YIELDS_NULL ' + CASE WHEN [CONCAT_NULL_YIELDS_NULL] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
						N'SET NUMERIC_ROUNDABORT ' + CASE WHEN [NUMERIC_ROUNDABORT] = 'true' THEN N'ON ' ELSE N'OFF ' END + NCHAR(10) +
						N'SET QUOTED_IDENTIFIER ' + CASE WHEN [QUOTED_IDENTIFIER] = 'true' THEN N'ON ' ELSE N'OFF ' + NCHAR(10) END AS [ansi_set_options]
				FROM (
					SELECT
						s.sql_handle,
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
		) AS set_options ON set_options.sql_handle = s.sql_handle
		OPTION(RECOMPILE);


		RAISERROR(N'Updating conversion XML', 0, 1) WITH NOWAIT;
		WITH precheck AS (
		SELECT spi.sql_handle,
			   spi.proc_name,
					(SELECT CASE WHEN spi.proc_name <> 'Statement' 
						   THEN N'The stored procedure ' + spi.proc_name 
						   ELSE N'This ad hoc statement' 
					  END
					+ N' had the following implicit conversions: '
					+ CHAR(10)
					+ STUFF((
						SELECT DISTINCT 
								@cr + @lf
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
						WHERE spi.sql_handle = spi2.sql_handle
						FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
					AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
					AS implicit_conversion_info
		FROM #stored_proc_info AS spi
		GROUP BY spi.sql_handle, spi.proc_name
		)
		UPDATE b
        SET    b.implicit_conversion_info = pk.implicit_conversion_info
        FROM   #working_warnings AS b
        JOIN   precheck AS pk
        ON pk.sql_handle = b.sql_handle
        OPTION (RECOMPILE);

		RAISERROR(N'Updating cached parameter XML for procs', 0, 1) WITH NOWAIT;
		WITH precheck AS (
		SELECT spi.sql_handle,
			   spi.proc_name,
			   (SELECT set_options
					+ @cr + @lf
					+ @cr + @lf
					+ N'EXEC ' 
					+ spi.proc_name 
					+ N' '
					+ STUFF((
						SELECT DISTINCT N', ' 
								+ CASE WHEN spi2.variable_name <> N'**no_variable**' AND spi2.compile_time_value <> N'**idk_man**'
										THEN spi2.variable_name + N' = '
										ELSE @cr + @lf + N' We could not find any cached parameter values for this stored proc. ' 
								  END
								+ CASE WHEN spi2.variable_name = N'**no_variable**' OR spi2.compile_time_value = N'**idk_man**'
									   THEN @cr + @lf + N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
									   WHEN spi2.compile_time_value = N'NULL' 
									   THEN spi2.compile_time_value 
									   ELSE RTRIM(spi2.compile_time_value)
								  END
						FROM #stored_proc_info AS spi2
						WHERE spi.sql_handle = spi2.sql_handle
						AND spi2.proc_name <> N'Statement'
						FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
					AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
					AS cached_execution_parameters
		FROM #stored_proc_info AS spi
		GROUP BY spi.sql_handle, spi.proc_name, set_options
		) 
		UPDATE b
		SET b.cached_execution_parameters = pk.cached_execution_parameters
        FROM   #working_warnings AS b
        JOIN   precheck AS pk
        ON pk.sql_handle = b.sql_handle
		WHERE b.proc_or_function_name <> N'Statement'
        OPTION (RECOMPILE);


	RAISERROR(N'Updating cached parameter XML for statements', 0, 1) WITH NOWAIT;
	WITH precheck AS (
	SELECT spi.sql_handle,
			   spi.proc_name,
		   (SELECT 
				set_options
				+ @cr + @lf
				+ @cr + @lf
				+ N' See QueryText column for full query text'
				+ @cr + @lf
				+ @cr + @lf
				+ STUFF((
					SELECT DISTINCT N', ' 
							+ CASE WHEN spi2.variable_name <> N'**no_variable**' AND spi2.compile_time_value <> N'**idk_man**'
									THEN spi2.variable_name + N' = '
									ELSE + @cr + @lf + N' We could not find any cached parameter values for this stored proc. ' 
							  END
							+ CASE WHEN spi2.variable_name = N'**no_variable**' OR spi2.compile_time_value = N'**idk_man**'
								   THEN + @cr + @lf + N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
								   WHEN spi2.compile_time_value = N'NULL' 
								   THEN spi2.compile_time_value 
								   ELSE RTRIM(spi2.compile_time_value)
							  END
					FROM #stored_proc_info AS spi2
					WHERE spi.sql_handle = spi2.sql_handle
					AND spi2.proc_name = N'Statement'
					AND spi2.variable_name NOT LIKE N'%msparam%'
					FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
				AS [processing-instruction(ClickMe)] FOR XML PATH(''), TYPE )
				AS cached_execution_parameters
	FROM #stored_proc_info AS spi
	GROUP BY spi.sql_handle, spi.proc_name, spi.set_options
	) 
	UPDATE b
	SET b.cached_execution_parameters = pk.cached_execution_parameters
    FROM   #working_warnings AS b
    JOIN   precheck AS pk
    ON pk.sql_handle = b.sql_handle
	WHERE b.proc_or_function_name = N'Statement'
    OPTION (RECOMPILE);


RAISERROR(N'Filling in implicit conversion info', 0, 1) WITH NOWAIT;
UPDATE b
SET    b.implicit_conversion_info = CASE WHEN b.implicit_conversion_info IS NULL 
									OR CONVERT(NVARCHAR(MAX), b.implicit_conversion_info) = N''
									THEN N'<?NoNeedToClickMe -- N/A --?>'
                                    ELSE b.implicit_conversion_info
                                    END,
       b.cached_execution_parameters = CASE WHEN b.cached_execution_parameters IS NULL 
									   OR CONVERT(NVARCHAR(MAX), b.cached_execution_parameters) = N''
									   THEN N'<?NoNeedToClickMe -- N/A --?>'
                                       ELSE b.cached_execution_parameters
                                       END
FROM   #working_warnings AS b
OPTION (RECOMPILE);

/*End implicit conversion and parameter info*/

/*Begin Missing Index*/
IF EXISTS ( SELECT 1/0 
            FROM #working_warnings AS ww 
            WHERE ww.missing_index_count > 0
		    OR ww.index_spool_cost > 0
		    OR ww.index_spool_rows > 0 )
		   
		BEGIN	
	
		RAISERROR(N'Inserting to #missing_index_xml', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT 	#missing_index_xml
		SELECT qp.query_hash,
		       qp.sql_handle,
		       c.mg.value('@Impact', 'FLOAT') AS Impact,
			   c.mg.query('.') AS cmg
		FROM   #query_plan AS qp
		CROSS APPLY qp.query_plan.nodes('//p:MissingIndexes/p:MissingIndexGroup') AS c(mg)
		WHERE  qp.query_hash IS NOT NULL
		AND c.mg.value('@Impact', 'FLOAT') > 70.0
		OPTION (RECOMPILE);

		RAISERROR(N'Inserting to #missing_index_schema', 0, 1) WITH NOWAIT;		
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_schema
		SELECT mix.query_hash, mix.sql_handle, mix.impact,
		       c.mi.value('@Database', 'NVARCHAR(128)'),
		       c.mi.value('@Schema', 'NVARCHAR(128)'),
		       c.mi.value('@Table', 'NVARCHAR(128)'),
			   c.mi.query('.')
		FROM #missing_index_xml AS mix
		CROSS APPLY mix.index_xml.nodes('//p:MissingIndex') AS c(mi)
		OPTION (RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_usage', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_usage
		SELECT ms.query_hash, ms.sql_handle, ms.impact, ms.database_name, ms.schema_name, ms.table_name,
				c.cg.value('@Usage', 'NVARCHAR(128)'),
				c.cg.query('.')
		FROM #missing_index_schema ms
		CROSS APPLY ms.index_xml.nodes('//p:ColumnGroup') AS c(cg)
		OPTION (RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_detail', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p )
		INSERT #missing_index_detail
		SELECT miu.query_hash,
		       miu.sql_handle,
		       miu.impact,
		       miu.database_name,
		       miu.schema_name,
		       miu.table_name,
		       miu.usage,
		       c.c.value('@Name', 'NVARCHAR(128)')
		FROM #missing_index_usage AS miu
		CROSS APPLY miu.index_xml.nodes('//p:Column') AS c(c)
		OPTION (RECOMPILE);
		
		RAISERROR(N'Inserting to #missing_index_pretty', 0, 1) WITH NOWAIT;
		INSERT #missing_index_pretty
		SELECT DISTINCT m.query_hash, m.sql_handle, m.impact, m.database_name, m.schema_name, m.table_name
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'EQUALITY'
						 AND m.query_hash = m2.query_hash
						 AND m.sql_handle = m2.sql_handle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS equality
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'INEQUALITY'
						 AND m.query_hash = m2.query_hash
						 AND m.sql_handle = m2.sql_handle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS inequality
		, STUFF((   SELECT DISTINCT N', ' + ISNULL(m2.column_name, '') AS column_name
		                 FROM   #missing_index_detail AS m2
		                 WHERE  m2.usage = 'INCLUDE'
						 AND m.query_hash = m2.query_hash
						 AND m.sql_handle = m2.sql_handle
						 AND m.impact = m2.impact
						 AND m.database_name = m2.database_name
						 AND m.schema_name = m2.schema_name
						 AND m.table_name = m2.table_name
		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS [include],
		0 AS is_spool
		FROM #missing_index_detail AS m
		GROUP BY m.query_hash, m.sql_handle, m.impact, m.database_name, m.schema_name, m.table_name
		OPTION (RECOMPILE);

		RAISERROR(N'Inserting to #index_spool_ugly', 0, 1) WITH NOWAIT;
		WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
		INSERT #index_spool_ugly 
		        (query_hash, sql_handle, impact, database_name, schema_name, table_name, equality, inequality, include)
		SELECT r.query_hash, 
		       r.sql_handle,                                                                               
		       (c.n.value('@EstimateIO', 'FLOAT') + (c.n.value('@EstimateCPU', 'FLOAT'))) 
					/ ( 1 * NULLIF(ww.query_cost, 0)) * 100 AS impact, 
			   o.n.value('@Database', 'NVARCHAR(128)') AS output_database, 
			   o.n.value('@Schema', 'NVARCHAR(128)') AS output_schema, 
			   o.n.value('@Table', 'NVARCHAR(128)') AS output_table, 
		       k.n.value('@Column', 'NVARCHAR(128)') AS range_column,
			   e.n.value('@Column', 'NVARCHAR(128)') AS expression_column,
			   o.n.value('@Column', 'NVARCHAR(128)') AS output_column
		FROM #relop AS r
		JOIN #working_warnings AS ww
		ON ww.query_hash = r.query_hash
		CROSS APPLY r.relop.nodes('/p:RelOp') AS c(n)
		CROSS APPLY r.relop.nodes('/p:RelOp/p:OutputList/p:ColumnReference') AS o(n)
		OUTER APPLY r.relop.nodes('/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeColumns/p:ColumnReference') AS k(n)
		OUTER APPLY r.relop.nodes('/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeExpressions/p:ColumnReference') AS e(n)
		WHERE  r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1
		
		RAISERROR(N'Inserting to spools to #missing_index_pretty', 0, 1) WITH NOWAIT;
		INSERT #missing_index_pretty
			(query_hash, sql_handle, impact, database_name, schema_name, table_name, equality, inequality, include, is_spool)
		SELECT DISTINCT 
		       isu.query_hash,
		       isu.sql_handle,
		       isu.impact,
		       isu.database_name,
		       isu.schema_name,
		       isu.table_name
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.equality, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.equality IS NOT NULL
			   						 AND isu.query_hash = isu2.query_hash
			   						 AND isu.sql_handle = isu2.sql_handle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS equality
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.inequality, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.inequality IS NOT NULL
			   						 AND isu.query_hash = isu2.query_hash
			   						 AND isu.sql_handle = isu2.sql_handle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS inequality
			   , STUFF((   SELECT DISTINCT N', ' + ISNULL(isu2.include, '') AS column_name
			   		                 FROM   #index_spool_ugly AS isu2
			   		                 WHERE  isu2.include IS NOT NULL
			   						 AND isu.query_hash = isu2.query_hash
			   						 AND isu.sql_handle = isu2.sql_handle
			   						 AND isu.impact = isu2.impact
			   						 AND isu.database_name = isu2.database_name
			   						 AND isu.schema_name = isu2.schema_name
			   						 AND isu.table_name = isu2.table_name
			   		                 FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS include,
			   1 AS is_spool
		FROM #index_spool_ugly AS isu


		RAISERROR(N'Updating missing index information', 0, 1) WITH NOWAIT;
		WITH missing AS (
		SELECT DISTINCT
		       mip.query_hash,
		       mip.sql_handle, 
			   N'<MissingIndexes><![CDATA['
			   + CHAR(10) + CHAR(13)
			   + STUFF((   SELECT CHAR(10) + CHAR(13) + ISNULL(mip2.details, '') AS details
		                   FROM   #missing_index_pretty AS mip2
						   WHERE mip.query_hash = mip2.query_hash
						   AND mip.sql_handle = mip2.sql_handle
						   GROUP BY mip2.details
		                   ORDER BY MAX(mip2.impact) DESC
						   FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') 
			   + CHAR(10) + CHAR(13)
			   + N']]></MissingIndexes>'  
			   AS full_details
		FROM #missing_index_pretty AS mip
		GROUP BY mip.query_hash, mip.sql_handle, mip.impact
						)
		UPDATE ww
			SET ww.missing_indexes = m.full_details
		FROM #working_warnings AS ww
		JOIN missing AS m
		ON m.sql_handle = ww.sql_handle
		OPTION (RECOMPILE);

	RAISERROR(N'Filling in missing index blanks', 0, 1) WITH NOWAIT;
	UPDATE ww
	SET ww.missing_indexes = 
		CASE WHEN ww.missing_indexes IS NULL 
			 THEN '<?NoNeedToClickMe -- N/A --?>' 
			 ELSE ww.missing_indexes 
		END
	FROM #working_warnings AS ww
	OPTION (RECOMPILE);

END
/*End Missing Index*/

RAISERROR(N'General query dispositions: frequent executions, long running, etc.', 0, 1) WITH NOWAIT;

WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)
UPDATE b
SET    b.frequent_execution = CASE WHEN wm.xpm > @execution_threshold THEN 1 END ,
	   b.near_parallel = CASE WHEN b.query_cost BETWEEN @ctp * (1 - (@ctp_threshold_pct / 100.0)) AND @ctp THEN 1 END,
	   b.long_running = CASE WHEN wm.avg_duration > @long_running_query_warning_seconds THEN 1
						     WHEN wm.max_duration > @long_running_query_warning_seconds THEN 1
                             WHEN wm.avg_cpu_time > @long_running_query_warning_seconds THEN 1
                             WHEN wm.max_cpu_time > @long_running_query_warning_seconds THEN 1 END,
	   b.is_key_lookup_expensive = CASE WHEN b.query_cost >= (@ctp / 2) AND b.key_lookup_cost >= b.query_cost * .5 THEN 1 END,
	   b.is_sort_expensive = CASE WHEN b.query_cost >= (@ctp / 2) AND b.sort_cost >= b.query_cost * .5 THEN 1 END,
	   b.is_remote_query_expensive = CASE WHEN b.remote_query_cost >= b.query_cost * .05 THEN 1 END,
	   b.is_unused_grant = CASE WHEN percent_memory_grant_used <= @memory_grant_warning_percent AND min_query_max_used_memory > @min_memory_per_query THEN 1 END,
	   b.long_running_low_cpu = CASE WHEN wm.avg_duration > wm.avg_cpu_time * 4 AND avg_cpu_time < 500. THEN 1 END,
	   b.low_cost_high_cpu = CASE WHEN b.query_cost < 10 AND wm.avg_cpu_time > 5000. THEN 1 END,
	   b.is_spool_expensive = CASE WHEN b.query_cost > (@ctp / 2) AND b.index_spool_cost >= b.query_cost * .1 THEN 1 END,
	   b.is_spool_more_rows = CASE WHEN b.index_spool_rows >= wm.min_rowcount THEN 1 END,
	   b.is_bad_estimate = CASE WHEN wm.avg_rowcount > 0 AND (b.estimated_rows * 1000 < wm.avg_rowcount OR b.estimated_rows > wm.avg_rowcount * 1000) THEN 1 END,
	   b.is_big_log = CASE WHEN wm.avg_log_bytes_used >= (@log_size_mb / 2.) THEN 1 END,
	   b.is_big_tempdb = CASE WHEN wm.avg_tempdb_space_used >= (@avg_tempdb_data_file / 2.) THEN 1 END
FROM #working_warnings AS b
JOIN #working_metrics AS wm
ON b.plan_id = wm.plan_id
AND b.query_id = wm.query_id
JOIN #working_plan_text AS wpt
ON b.plan_id = wpt.plan_id
AND b.query_id = wpt.query_id
OPTION (RECOMPILE);


RAISERROR('Populating Warnings column', 0, 1) WITH NOWAIT;
/* Populate warnings */
UPDATE b
SET    b.warnings = SUBSTRING(
                  CASE WHEN b.warning_no_join_predicate = 1 THEN ', No Join Predicate' ELSE '' END +
                  CASE WHEN b.compile_timeout = 1 THEN ', Compilation Timeout' ELSE '' END +
                  CASE WHEN b.compile_memory_limit_exceeded = 1 THEN ', Compile Memory Limit Exceeded' ELSE '' END +
                  CASE WHEN b.is_forced_plan = 1 THEN ', Forced Plan' ELSE '' END +
                  CASE WHEN b.is_forced_parameterized = 1 THEN ', Forced Parameterization' ELSE '' END +
                  CASE WHEN b.unparameterized_query = 1 THEN ', Unparameterized Query' ELSE '' END +
                  CASE WHEN b.missing_index_count > 0 THEN ', Missing Indexes (' + CAST(b.missing_index_count AS NVARCHAR(3)) + ')' ELSE '' END +
                  CASE WHEN b.unmatched_index_count > 0 THEN ', Unmatched Indexes (' + CAST(b.unmatched_index_count AS NVARCHAR(3)) + ')' ELSE '' END +                  
                  CASE WHEN b.is_cursor = 1 THEN ', Cursor' 
							+ CASE WHEN b.is_optimistic_cursor = 1 THEN '; optimistic' ELSE '' END
							+ CASE WHEN b.is_forward_only_cursor = 0 THEN '; not forward only' ELSE '' END
							+ CASE WHEN b.is_cursor_dynamic = 1 THEN '; dynamic' ELSE '' END
                            + CASE WHEN b.is_fast_forward_cursor = 1 THEN '; fast forward' ELSE '' END			
				  ELSE '' END +
                  CASE WHEN b.is_parallel = 1 THEN ', Parallel' ELSE '' END +
                  CASE WHEN b.near_parallel = 1 THEN ', Nearly Parallel' ELSE '' END +
                  CASE WHEN b.frequent_execution = 1 THEN ', Frequent Execution' ELSE '' END +
                  CASE WHEN b.plan_warnings = 1 THEN ', Plan Warnings' ELSE '' END +
                  CASE WHEN b.parameter_sniffing = 1 THEN ', Parameter Sniffing' ELSE '' END +
                  CASE WHEN b.long_running = 1 THEN ', Long Running Query' ELSE '' END +
                  CASE WHEN b.downlevel_estimator = 1 THEN ', Downlevel CE' ELSE '' END +
                  CASE WHEN b.implicit_conversions = 1 THEN ', Implicit Conversions' ELSE '' END +
                  CASE WHEN b.plan_multiple_plans = 1 THEN ', Multiple Plans' ELSE '' END +
                  CASE WHEN b.is_trivial = 1 THEN ', Trivial Plans' ELSE '' END +
				  CASE WHEN b.is_forced_serial = 1 THEN ', Forced Serialization' ELSE '' END +
				  CASE WHEN b.is_key_lookup_expensive = 1 THEN ', Expensive Key Lookup' ELSE '' END +
				  CASE WHEN b.is_remote_query_expensive = 1 THEN ', Expensive Remote Query' ELSE '' END + 
				  CASE WHEN b.trace_flags_session IS NOT NULL THEN ', Session Level Trace Flag(s) Enabled: ' + b.trace_flags_session ELSE '' END +
				  CASE WHEN b.is_unused_grant = 1 THEN ', Unused Memory Grant' ELSE '' END +
				  CASE WHEN b.function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), b.function_count) + ' function(s)' ELSE '' END + 
				  CASE WHEN b.clr_function_count > 0 THEN ', Calls ' + CONVERT(VARCHAR(10), b.clr_function_count) + ' CLR function(s)' ELSE '' END + 
				  CASE WHEN b.is_table_variable = 1 THEN ', Table Variables' ELSE '' END +
				  CASE WHEN b.no_stats_warning = 1 THEN ', Columns With No Statistics' ELSE '' END +
				  CASE WHEN b.relop_warnings = 1 THEN ', Operator Warnings' ELSE '' END  + 
				  CASE WHEN b.is_table_scan = 1 THEN ', Table Scans' ELSE '' END  + 
				  CASE WHEN b.backwards_scan = 1 THEN ', Backwards Scans' ELSE '' END  + 
				  CASE WHEN b.forced_index = 1 THEN ', Forced Indexes' ELSE '' END  + 
				  CASE WHEN b.forced_seek = 1 THEN ', Forced Seeks' ELSE '' END  + 
				  CASE WHEN b.forced_scan = 1 THEN ', Forced Scans' ELSE '' END  +
				  CASE WHEN b.columnstore_row_mode = 1 THEN ', ColumnStore Row Mode ' ELSE '' END +
				  CASE WHEN b.is_computed_scalar = 1 THEN ', Computed Column UDF ' ELSE '' END  +
				  CASE WHEN b.is_sort_expensive = 1 THEN ', Expensive Sort' ELSE '' END +
				  CASE WHEN b.is_computed_filter = 1 THEN ', Filter UDF' ELSE '' END +
				  CASE WHEN b.index_ops >= 5 THEN ', >= 5 Indexes Modified' ELSE '' END +
				  CASE WHEN b.is_row_level = 1 THEN ', Row Level Security' ELSE '' END + 
				  CASE WHEN b.is_spatial = 1 THEN ', Spatial Index' ELSE '' END + 
				  CASE WHEN b.index_dml = 1 THEN ', Index DML' ELSE '' END +
				  CASE WHEN b.table_dml = 1 THEN ', Table DML' ELSE '' END +
				  CASE WHEN b.low_cost_high_cpu = 1 THEN ', Low Cost High CPU' ELSE '' END + 
				  CASE WHEN b.long_running_low_cpu = 1 THEN + ', Long Running With Low CPU' ELSE '' END +
				  CASE WHEN b.stale_stats = 1 THEN + ', Statistics used have > 100k modifications in the last 7 days' ELSE '' END +
				  CASE WHEN b.is_adaptive = 1 THEN + ', Adaptive Joins' ELSE '' END	+
				  CASE WHEN b.is_spool_expensive = 1 THEN + ', Expensive Index Spool' ELSE '' END +
				  CASE WHEN b.is_spool_more_rows = 1 THEN + ', Large Index Row Spool' ELSE '' END +
				  CASE WHEN b.is_bad_estimate = 1 THEN + ', Row estimate mismatch' ELSE '' END +
				  CASE WHEN b.is_big_log = 1 THEN + ', High log use' ELSE '' END +
				  CASE WHEN b.is_big_tempdb = 1 THEN ', High tempdb use' ELSE '' END +
				  CASE WHEN b.is_paul_white_electric = 1 THEN ', SWITCH!' ELSE '' END + 
				  CASE WHEN b.is_row_goal = 1 THEN ', Row Goals' ELSE '' END + 
				  CASE WHEN b.is_mstvf = 1 THEN ', MSTVFs' ELSE '' END + 
				  CASE WHEN b.is_mm_join = 1 THEN ', Many to Many Merge' ELSE '' END + 
                  CASE WHEN b.is_nonsargable = 1 THEN ', non-SARGables' ELSE '' END
                  , 2, 200000) 
FROM #working_warnings b
OPTION (RECOMPILE);


END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure generating warnings.', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;


BEGIN TRY 
BEGIN 

RAISERROR(N'Checking for parameter sniffing symptoms', 0, 1) WITH NOWAIT;

UPDATE b
SET b.parameter_sniffing_symptoms = 
CASE WHEN b.count_executions < 2 THEN 'Too few executions to compare (< 2).'
	ELSE													
	SUBSTRING(  
				/*Duration*/
				CASE WHEN (b.min_duration * 100) < (b.avg_duration) THEN ', Fast sometimes' ELSE '' END +
				CASE WHEN (b.max_duration) > (b.avg_duration * 100) THEN ', Slow sometimes' ELSE '' END +
				CASE WHEN (b.last_duration * 100) < (b.avg_duration)  THEN ', Fast last run' ELSE '' END +
				CASE WHEN (b.last_duration) > (b.avg_duration * 100) THEN ', Slow last run' ELSE '' END +
				/*CPU*/
				CASE WHEN (b.min_cpu_time / b.avg_dop) * 100 < (b.avg_cpu_time / b.avg_dop) THEN ', Low CPU sometimes' ELSE '' END +
				CASE WHEN (b.max_cpu_time / b.max_dop) > (b.avg_cpu_time / b.avg_dop) * 100 THEN ', High CPU sometimes' ELSE '' END +
				CASE WHEN (b.last_cpu_time / b.last_dop) * 100 < (b.avg_cpu_time / b.avg_dop)  THEN ', Low CPU last run' ELSE '' END +
				CASE WHEN (b.last_cpu_time / b.last_dop) > (b.avg_cpu_time / b.avg_dop) * 100 THEN ', High CPU last run' ELSE '' END +
				/*Logical Reads*/
				CASE WHEN (b.min_logical_io_reads * 100) < (b.avg_logical_io_reads) THEN ', Low reads sometimes' ELSE '' END +
				CASE WHEN (b.max_logical_io_reads) > (b.avg_logical_io_reads * 100) THEN ', High reads sometimes' ELSE '' END +
				CASE WHEN (b.last_logical_io_reads * 100) < (b.avg_logical_io_reads)  THEN ', Low reads last run' ELSE '' END +
				CASE WHEN (b.last_logical_io_reads) > (b.avg_logical_io_reads * 100) THEN ', High reads last run' ELSE '' END +
				/*Logical Writes*/
				CASE WHEN (b.min_logical_io_writes * 100) < (b.avg_logical_io_writes) THEN ', Low writes sometimes' ELSE '' END +
				CASE WHEN (b.max_logical_io_writes) > (b.avg_logical_io_writes * 100) THEN ', High writes sometimes' ELSE '' END +
				CASE WHEN (b.last_logical_io_writes * 100) < (b.avg_logical_io_writes)  THEN ', Low writes last run' ELSE '' END +
				CASE WHEN (b.last_logical_io_writes) > (b.avg_logical_io_writes * 100) THEN ', High writes last run' ELSE '' END +
				/*Physical Reads*/
				CASE WHEN (b.min_physical_io_reads * 100) < (b.avg_physical_io_reads) THEN ', Low physical reads sometimes' ELSE '' END +
				CASE WHEN (b.max_physical_io_reads) > (b.avg_physical_io_reads * 100) THEN ', High physical reads sometimes' ELSE '' END +
				CASE WHEN (b.last_physical_io_reads * 100) < (b.avg_physical_io_reads)  THEN ', Low physical reads last run' ELSE '' END +
				CASE WHEN (b.last_physical_io_reads) > (b.avg_physical_io_reads * 100) THEN ', High physical reads last run' ELSE '' END +
				/*Memory*/
				CASE WHEN (b.min_query_max_used_memory * 100) < (b.avg_query_max_used_memory) THEN ', Low memory sometimes' ELSE '' END +
				CASE WHEN (b.max_query_max_used_memory) > (b.avg_query_max_used_memory * 100) THEN ', High memory sometimes' ELSE '' END +
				CASE WHEN (b.last_query_max_used_memory * 100) < (b.avg_query_max_used_memory)  THEN ', Low memory last run' ELSE '' END +
				CASE WHEN (b.last_query_max_used_memory) > (b.avg_query_max_used_memory * 100) THEN ', High memory last run' ELSE '' END +
				/*Duration*/
				CASE WHEN b.min_rowcount * 100 < b.avg_rowcount THEN ', Low row count sometimes' ELSE '' END +
				CASE WHEN b.max_rowcount > b.avg_rowcount * 100 THEN ', High row count sometimes' ELSE '' END +
				CASE WHEN b.last_rowcount * 100 < b.avg_rowcount  THEN ', Low row count run' ELSE '' END +
				CASE WHEN b.last_rowcount > b.avg_rowcount * 100 THEN ', High row count last run' ELSE '' END +
				/*DOP*/
				CASE WHEN b.min_dop <> b.max_dop THEN ', Serial sometimes' ELSE '' END +
				CASE WHEN b.min_dop <> b.max_dop AND b.last_dop = 1  THEN ', Serial last run' ELSE '' END +
				CASE WHEN b.min_dop <> b.max_dop AND b.last_dop > 1 THEN ', Parallel last run' ELSE '' END +
				/*tempdb*/
				CASE WHEN b.min_tempdb_space_used * 100 < b.avg_tempdb_space_used THEN ', Low tempdb sometimes' ELSE '' END +
				CASE WHEN b.max_tempdb_space_used > b.avg_tempdb_space_used * 100 THEN ', High tempdb sometimes' ELSE '' END +
				CASE WHEN b.last_tempdb_space_used * 100 < b.avg_tempdb_space_used  THEN ', Low tempdb run' ELSE '' END +
				CASE WHEN b.last_tempdb_space_used > b.avg_tempdb_space_used * 100 THEN ', High tempdb last run' ELSE '' END +
				/*tlog*/
				CASE WHEN b.min_log_bytes_used * 100 < b.avg_log_bytes_used THEN ', Low log use sometimes' ELSE '' END +
				CASE WHEN b.max_log_bytes_used > b.avg_log_bytes_used * 100 THEN ', High log use sometimes' ELSE '' END +
				CASE WHEN b.last_log_bytes_used * 100 < b.avg_log_bytes_used  THEN ', Low log use run' ELSE '' END +
				CASE WHEN b.last_log_bytes_used > b.avg_log_bytes_used * 100 THEN ', High log use last run' ELSE '' END 
	, 2, 200000) 
	END
FROM #working_metrics AS b
OPTION (RECOMPILE);

END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure analyzing parameter sniffing', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;

BEGIN TRY 

BEGIN 

IF (@Failed = 0 AND @ExportToExcel = 0 AND @SkipXML = 0)
BEGIN

RAISERROR(N'Returning regular results', 0, 1) WITH NOWAIT;

WITH x AS (
SELECT wpt.database_name, ww.query_cost, wm.plan_id, wm.query_id, wm.query_id_all_plan_ids, wpt.query_sql_text, wm.proc_or_function_name, wpt.query_plan_xml, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.top_three_waits, ww.missing_indexes, ww.implicit_conversion_info, ww.cached_execution_parameters, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes, wm.total_rowcount, wm.avg_rowcount,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.total_tempdb_space_used, wm.avg_tempdb_space_used,
	   wm.total_log_bytes_used, wm.avg_log_bytes_used, wm.total_num_physical_io_reads, wm.avg_num_physical_io_reads,
	   wm.first_execution_time, wm.last_execution_time, wpt.last_force_failure_reason_desc, wpt.context_settings, ROW_NUMBER() OVER (PARTITION BY wm.plan_id, wm.query_id, wm.last_execution_time ORDER BY wm.plan_id) AS rn
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
	ON wpt.plan_id = ww.plan_id
	AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
	ON wpt.plan_id = wm.plan_id
	AND wpt.query_id = wm.query_id
)
SELECT *
FROM x
WHERE x.rn = 1
ORDER BY x.last_execution_time
OPTION (RECOMPILE);

END;

IF (@Failed = 1 AND @ExportToExcel = 0 AND @SkipXML = 0)
BEGIN

RAISERROR(N'Returning results for failed queries', 0, 1) WITH NOWAIT;

WITH x AS (
SELECT wpt.database_name, ww.query_cost, wm.plan_id, wm.query_id, wm.query_id_all_plan_ids, wpt.query_sql_text, wm.proc_or_function_name, wpt.query_plan_xml, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.last_force_failure_reason_desc, wpt.top_three_waits, ww.missing_indexes, ww.implicit_conversion_info, ww.cached_execution_parameters, 
	   wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes, wm.total_rowcount, wm.avg_rowcount,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.total_tempdb_space_used, wm.avg_tempdb_space_used,
	   wm.total_log_bytes_used, wm.avg_log_bytes_used, wm.total_num_physical_io_reads, wm.avg_num_physical_io_reads,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings, ROW_NUMBER() OVER (PARTITION BY wm.plan_id, wm.query_id, wm.last_execution_time ORDER BY wm.plan_id) AS rn
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
	ON wpt.plan_id = ww.plan_id
	AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
	ON wpt.plan_id = wm.plan_id
	AND wpt.query_id = wm.query_id
)
SELECT *
FROM x
WHERE x.rn = 1
ORDER BY x.last_execution_time
OPTION (RECOMPILE);

END;

IF (@ExportToExcel = 1 AND @SkipXML = 0)
BEGIN

RAISERROR(N'Returning results for Excel export', 0, 1) WITH NOWAIT;

UPDATE #working_plan_text
SET query_sql_text = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(query_sql_text)),' ','<>'),'><',''),'<>',' '), 1, 31000)
OPTION (RECOMPILE);

WITH x AS (
SELECT wpt.database_name, ww.query_cost, wm.plan_id, wm.query_id, wm.query_id_all_plan_ids, wpt.query_sql_text, wm.proc_or_function_name, ww.warnings, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.last_force_failure_reason_desc, wpt.top_three_waits, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes, wm.total_rowcount, wm.avg_rowcount,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.total_tempdb_space_used, wm.avg_tempdb_space_used,
	   wm.total_log_bytes_used, wm.avg_log_bytes_used, wm.total_num_physical_io_reads, wm.avg_num_physical_io_reads,
	   wm.first_execution_time, wm.last_execution_time, wpt.context_settings, ROW_NUMBER() OVER (PARTITION BY wm.plan_id, wm.query_id, wm.last_execution_time ORDER BY wm.plan_id) AS rn
FROM #working_plan_text AS wpt
JOIN #working_warnings AS ww
	ON wpt.plan_id = ww.plan_id
	AND wpt.query_id = ww.query_id
JOIN #working_metrics AS wm
	ON wpt.plan_id = wm.plan_id
	AND wpt.query_id = wm.query_id
)
SELECT *
FROM x
WHERE x.rn = 1
ORDER BY x.last_execution_time
OPTION (RECOMPILE);

END;

IF (@ExportToExcel = 0 AND @SkipXML = 1)
BEGIN

RAISERROR(N'Returning results for skipped XML', 0, 1) WITH NOWAIT;

WITH x AS (
SELECT wpt.database_name, wm.plan_id, wm.query_id, wm.query_id_all_plan_ids, wpt.query_sql_text, wpt.query_plan_xml, wpt.pattern, 
	   wm.parameter_sniffing_symptoms, wpt.top_three_waits, wm.count_executions, wm.count_compiles, wm.total_cpu_time, wm.avg_cpu_time,
	   wm.total_duration, wm.avg_duration, wm.total_logical_io_reads, wm.avg_logical_io_reads,
	   wm.total_physical_io_reads, wm.avg_physical_io_reads, wm.total_logical_io_writes, wm.avg_logical_io_writes, wm.total_rowcount, wm.avg_rowcount,
	   wm.total_query_max_used_memory, wm.avg_query_max_used_memory, wm.total_tempdb_space_used, wm.avg_tempdb_space_used,
	   wm.total_log_bytes_used, wm.avg_log_bytes_used, wm.total_num_physical_io_reads, wm.avg_num_physical_io_reads,
	   wm.first_execution_time, wm.last_execution_time, wpt.last_force_failure_reason_desc, wpt.context_settings, ROW_NUMBER() OVER (PARTITION BY wm.plan_id, wm.query_id, wm.last_execution_time ORDER BY wm.plan_id) AS rn
FROM #working_plan_text AS wpt
JOIN #working_metrics AS wm
	ON wpt.plan_id = wm.plan_id
	AND wpt.query_id = wm.query_id
)
SELECT *
FROM x
WHERE x.rn = 1
ORDER BY x.last_execution_time
OPTION (RECOMPILE);

END;

END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure returning results', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;

BEGIN TRY 
BEGIN 

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
                    'https://www.brentozar.com/blitzcache/frequently-executed-queries/',
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
                    'https://www.brentozar.com/blitzcache/parameter-sniffing/',
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
                    'https://www.brentozar.com/blitzcache/forced-plans/',
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
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
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
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
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
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'There are non-forward only cursors in the plan cache, which can harm performance.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_cursor = 1
				   AND is_cursor_dynamic = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (4,
                    200,
                    'Cursors',
                    'Dynamic Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Dynamic Cursors inhibit parallelism!.');

		IF EXISTS (SELECT 1/0
                   FROM   #working_warnings
                   WHERE  is_cursor = 1
				   AND is_fast_forward_cursor = 1
                    )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (4,
                    200,
                    'Cursors',
                    'Fast Forward Cursors',
                    'https://www.brentozar.com/blitzcache/cursors-found-slow-queries/',
                    'Fast forward cursors inhibit parallelism!.');
					
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
                    'https://www.brentozar.com/blitzcache/forced-parameterization/',
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
                    'https://www.brentozar.com/blitzcache/parallel-plans-detected/',
                    'Parallel plans detected. These warrant investigation, but are neither good nor bad.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.near_parallel = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    7,
                    200,
                    'Execution Plans',
                    'Nearly Parallel',
                    'https://www.brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                    'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.plan_warnings = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    8,
                    50,
                    'Execution Plans',
                    'Query Plan Warnings',
                    'https://www.brentozar.com/blitzcache/query-plan-warnings/',
                    'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  p.long_running = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    9,
                    50,
                    'Performance',
                    'Long Running Queries',
                    'https://www.brentozar.com/blitzcache/long-running-queries/',
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
                    'https://www.brentozar.com/blitzcache/missing-index-request/',
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
                    'https://www.brentozar.com/blitzcache/legacy-cardinality-estimator/',
                    'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

        IF EXISTS (SELECT 1/0
                   FROM #working_warnings p
                   WHERE p.implicit_conversions = 1
				   )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    14,
                    50,
                    'Performance',
                    'Implicit Conversions',
                    'https://www.brentozar.com/go/implicit',
                    'One or more queries are comparing two fields that are not of the same data type.') ;

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  busy_loops = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                16,
                100,
                'Performance',
                'Busy Loops',
                'https://www.brentozar.com/blitzcache/busy-loops/',
                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

        IF EXISTS (SELECT 1/0
                   FROM   #working_warnings p
                   WHERE  tvf_join = 1
				   )
        INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
        VALUES (
                17,
                50,
                'Performance',
                'Joining to table valued functions',
                'https://www.brentozar.com/blitzcache/tvf-join/',
                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

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
                'https://www.brentozar.com/blitzcache/compilation-timeout/',
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
                'https://www.brentozar.com/blitzcache/compile-memory-limit-exceeded/',
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
                'https://www.brentozar.com/blitzcache/no-join-predicate/',
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
                'https://www.brentozar.com/blitzcache/multiple-plans/',
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
                'https://www.brentozar.com/blitzcache/unmatched-indexes',
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
                'https://www.brentozar.com/blitzcache/unparameterized-queries',
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
                'https://www.brentozar.com/blitzcache/trivial-plans',
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
                    'https://www.brentozar.com/blitzcache/forced-serialization/',
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
                    'https://www.brentozar.com/blitzcache/expensive-key-lookups/',
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
                    'https://www.brentozar.com/blitzcache/expensive-remote-query/',
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
                    'https://www.brentozar.com/blitzcache/query-plan-warnings/',
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
                     'https://www.brentozar.com/blitzcache/expensive-sorts/',
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
                     'https://www.brentozar.com/blitzcache/many-indexes-modified/',
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
                     'https://www.brentozar.com/blitzcache/row-level-security/',
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
                     'https://www.brentozar.com/blitzcache/spatial-indexes/',
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
                     'https://www.brentozar.com/blitzcache/index-dml/',
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
                     'https://www.brentozar.com/blitzcache/table-dml/',
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
                     'https://www.brentozar.com/blitzcache/long-running-low-cpu/',
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
                     'https://www.brentozar.com/blitzcache/low-cost-high-cpu/',
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
                     'https://www.brentozar.com/blitzcache/stale-statistics/',
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
                     'https://www.brentozar.com/blitzcache/adaptive-joins/',
                     'Joe Sack rules.') ;					 

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_spool_expensive = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     54,
                     150,
                     'Expensive Index Spool',
                     'You have an index spool, this is usually a sign that there''s an index missing somewhere.',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;	

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_spool_more_rows = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     55,
                     150,
                     'Index Spools Many Rows',
                     'You have an index spool that spools more rows than the query returns',
                     'https://www.brentozar.com/blitzcache/eager-index-spools/',
                     'Check operator predicates and output for index definition guidance') ;	

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_bad_estimate = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     56,
                     100,
                     'Potentially bad cardinality estimates',
                     'Estimated rows are different from average rows by a factor of 10000',
                     'https://www.brentozar.com/blitzcache/bad-estimates/',
                     'This may indicate a performance problem if mismatches occur regularly') ;					

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_big_log = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     57,
                     100,
                     'High transaction log use',
                     'This query on average uses more than half of the transaction log',
                     'http://michaeljswart.com/2014/09/take-care-when-scripting-batches/',
                     'This is probably a sign that you need to start batching queries') ;
					 		
        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_big_tempdb = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     58,
                     100,
                     'High tempdb use',
                     'This query uses more than half of a data file on average',
                     'No URL yet',
                     'You should take a look at tempdb waits to see if you''re having problems') ;
					 
        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_row_goal = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (59,
                     200,
                     'Row Goals',
                     'This query had row goals introduced',
                     'https://www.brentozar.com/archive/2018/01/sql-server-2017-cu3-adds-optimizer-row-goal-information-query-plans/',
                     'This can be good or bad, and should be investigated for high read queries') ;						 	

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_mstvf = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     60,
                     100,
                     'MSTVFs',
                     'These have many of the same problems scalar UDFs have',
                     'https://www.brentozar.com/blitzcache/tvf-join/',
					 'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_mstvf = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (61,
                     100,
                     'Many to Many Merge',
                     'These use secret worktables that could be doing lots of reads',
                     'https://www.brentozar.com/archive/2018/04/many-mysteries-merge-joins/',
					 'Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');

        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_nonsargable = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (62,
                     50,
                     'Non-SARGable queries',
                     'Queries may be using',
                     'https://www.brentozar.com/blitzcache/non-sargable-predicates/',
					 'Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');
					
        IF EXISTS (SELECT 1/0
                    FROM   #working_warnings p
                    WHERE  p.is_paul_white_electric = 1
  					)
             INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
             VALUES (
                     998,
                     200,
                     'Is Paul White Electric?',
                     'This query has a Switch operator in it!',
                     'https://www.sql.kiwi/2013/06/hello-operator-my-switch-is-bored.html',
                     'You should email this query plan to Paul: SQLkiwi at gmail dot com') ;
					 
					 						 					
				
				INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT 				
				999,
				200,
				'Database Level Statistics',
				'The database ' + sa.[database] + ' last had a stats update on '  + CONVERT(NVARCHAR(10), CONVERT(DATE, MAX(sa.last_update))) + ' and has ' + CONVERT(NVARCHAR(10), AVG(sa.modification_count)) + ' modifications on average.' AS Finding,
				'https://www.brentozar.com/blitzcache/stale-statistics/' AS URL,
				'Consider updating statistics more frequently,' AS Details
				FROM #stats_agg AS sa
				GROUP BY sa.[database]
				HAVING MAX(sa.last_update) <= DATEADD(DAY, -7, SYSDATETIME())
				AND AVG(sa.modification_count) >= 100000;

		
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


			/*
			Return worsts
			*/
			WITH worsts AS (
			SELECT gi.flat_date,
			       gi.start_range,
			       gi.end_range,
			       gi.total_avg_duration_ms,
			       gi.total_avg_cpu_time_ms,
			       gi.total_avg_logical_io_reads_mb,
			       gi.total_avg_physical_io_reads_mb,
			       gi.total_avg_logical_io_writes_mb,
			       gi.total_avg_query_max_used_memory_mb,
			       gi.total_rowcount,
				   gi.total_avg_log_bytes_mb,
				   gi.total_avg_tempdb_space,
                   gi.total_max_duration_ms, 
                   gi.total_max_cpu_time_ms, 
                   gi.total_max_logical_io_reads_mb,
                   gi.total_max_physical_io_reads_mb, 
                   gi.total_max_logical_io_writes_mb, 
                   gi.total_max_query_max_used_memory_mb,
                   gi.total_max_log_bytes_mb, 
                   gi.total_max_tempdb_space,
				   CONVERT(NVARCHAR(20), gi.flat_date) AS worst_date,
				   CASE WHEN DATEPART(HOUR, gi.start_range) = 0 THEN ' midnight '
						WHEN DATEPART(HOUR, gi.start_range) <= 12 THEN CONVERT(NVARCHAR(3), DATEPART(HOUR, gi.start_range)) + 'am '
						WHEN DATEPART(HOUR, gi.start_range) > 12 THEN CONVERT(NVARCHAR(3), DATEPART(HOUR, gi.start_range) -12) + 'pm '
						END AS worst_start_time,
				   CASE WHEN DATEPART(HOUR, gi.end_range) = 0 THEN ' midnight '
						WHEN DATEPART(HOUR, gi.end_range) <= 12 THEN CONVERT(NVARCHAR(3), DATEPART(HOUR, gi.end_range)) + 'am '
						WHEN DATEPART(HOUR, gi.end_range) > 12 THEN CONVERT(NVARCHAR(3),  DATEPART(HOUR, gi.end_range) -12) + 'pm '
						END AS worst_end_time
			FROM   #grouped_interval AS gi
			), /*averages*/
				duration_worst AS (
			SELECT TOP 1 'Your worst avg duration range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_duration_ms DESC
				), 
				cpu_worst AS (
			SELECT TOP 1 'Your worst avg cpu range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_cpu_time_ms DESC
				), 
				logical_reads_worst AS (
			SELECT TOP 1 'Your worst avg logical read range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_logical_io_reads_mb DESC
				), 
				physical_reads_worst AS (
			SELECT TOP 1 'Your worst avg physical read range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_physical_io_reads_mb DESC
				), 
				logical_writes_worst AS (
			SELECT TOP 1 'Your worst avg logical write range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_logical_io_writes_mb DESC
				), 
				memory_worst AS (
			SELECT TOP 1 'Your worst avg memory range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_query_max_used_memory_mb DESC
				), 
				rowcount_worst AS (
			SELECT TOP 1 'Your worst avg row count range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_rowcount DESC
				), 
				logbytes_worst AS (
			SELECT TOP 1 'Your worst avg log bytes range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_log_bytes_mb DESC
				), 
				tempdb_worst AS (
			SELECT TOP 1 'Your worst avg tempdb range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_avg_tempdb_space DESC
				)/*maxes*/, 
				max_duration_worst AS (
			SELECT TOP 1 'Your worst max duration range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_duration_ms DESC
				), 
				max_cpu_worst AS (
			SELECT TOP 1 'Your worst max cpu range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_cpu_time_ms DESC
				), 
				max_logical_reads_worst AS (
			SELECT TOP 1 'Your worst max logical read range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_logical_io_reads_mb DESC
				), 
				max_physical_reads_worst AS (
			SELECT TOP 1 'Your worst max physical read range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_physical_io_reads_mb DESC
				), 
				max_logical_writes_worst AS (
			SELECT TOP 1 'Your worst max logical write range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_logical_io_writes_mb DESC
				), 
				max_memory_worst AS (
			SELECT TOP 1 'Your worst max memory range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_query_max_used_memory_mb DESC
				), 
				max_logbytes_worst AS (
			SELECT TOP 1 'Your worst max log bytes range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_log_bytes_mb DESC
				), 
				max_tempdb_worst AS (
			SELECT TOP 1 'Your worst max tempdb range was on ' + worsts.worst_date + ' between ' + worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' AS msg
			FROM worsts
			ORDER BY worsts.total_max_tempdb_space DESC
				)
			INSERT #warning_results ( CheckID, Priority, FindingsGroup, Finding, URL, Details )
			/*averages*/
            SELECT 1002, 255, 'Worsts', 'Worst Avg Duration', 'N/A', duration_worst.msg
			FROM duration_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg CPU', 'N/A', cpu_worst.msg
			FROM cpu_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg Logical Reads', 'N/A', logical_reads_worst.msg
			FROM logical_reads_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg Physical Reads', 'N/A', physical_reads_worst.msg
			FROM physical_reads_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg Logical Writes', 'N/A', logical_writes_worst.msg
			FROM logical_writes_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg Memory', 'N/A', memory_worst.msg
			FROM memory_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Row Counts', 'N/A', rowcount_worst.msg
			FROM rowcount_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg Log Bytes', 'N/A', logbytes_worst.msg
			FROM logbytes_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Avg tempdb', 'N/A', tempdb_worst.msg
			FROM tempdb_worst
            UNION ALL
            /*maxes*/
            SELECT 1002, 255, 'Worsts', 'Worst Max Duration', 'N/A', max_duration_worst.msg
			FROM max_duration_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max CPU', 'N/A', max_cpu_worst.msg
			FROM max_cpu_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max Logical Reads', 'N/A', max_logical_reads_worst.msg
			FROM max_logical_reads_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max Physical Reads', 'N/A', max_physical_reads_worst.msg
			FROM max_physical_reads_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max Logical Writes', 'N/A', max_logical_writes_worst.msg
			FROM max_logical_writes_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max Memory', 'N/A', max_memory_worst.msg
			FROM max_memory_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max Log Bytes', 'N/A', max_logbytes_worst.msg
			FROM max_logbytes_worst
			UNION ALL
			SELECT 1002, 255, 'Worsts', 'Worst Max tempdb', 'N/A', max_tempdb_worst.msg
			FROM max_tempdb_worst
			OPTION (RECOMPILE);


        IF NOT EXISTS (SELECT 1/0
					   FROM   #warning_results AS bcr
                       WHERE  bcr.Priority = 2147483646
				      )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (2147483646,
                    255,
                    'Need more help?' ,
                    'Paste your plan on the internet!',
                    'http://pastetheplan.com',
                    'This makes it easy to share plans and post them to Q&A sites like https://dba.stackexchange.com/!') ;


        IF NOT EXISTS (SELECT 1/0
					   FROM   #warning_results AS bcr
                       WHERE  bcr.Priority = 2147483647
				      )
            INSERT INTO #warning_results (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (
                    2147483647,
                    255,
                    'Thanks for using sp_BlitzQueryStore!' ,
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
    ORDER BY Priority ASC, FindingsGroup, Finding, CheckID ASC
    OPTION (RECOMPILE);



END;	

END;
END TRY
BEGIN CATCH
        RAISERROR (N'Failure returning warnings', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;

IF @Debug = 1	

BEGIN TRY 

BEGIN

RAISERROR(N'Returning debugging data from temp tables', 0, 1) WITH NOWAIT;

--Table content debugging

SELECT '#working_metrics' AS table_name, *
FROM #working_metrics AS wm
OPTION (RECOMPILE);

SELECT '#working_plan_text' AS table_name, *
FROM #working_plan_text AS wpt
OPTION (RECOMPILE);

SELECT '#working_warnings' AS table_name, *
FROM #working_warnings AS ww
OPTION (RECOMPILE);

SELECT '#working_wait_stats' AS table_name, *
FROM #working_wait_stats wws
OPTION (RECOMPILE);

SELECT '#grouped_interval' AS table_name, *
FROM #grouped_interval
OPTION (RECOMPILE);

SELECT '#working_plans' AS table_name, *
FROM #working_plans
OPTION (RECOMPILE);

SELECT '#stats_agg' AS table_name, *
FROM #stats_agg
OPTION (RECOMPILE);

SELECT '#trace_flags' AS table_name, *
FROM #trace_flags
OPTION (RECOMPILE);

SELECT '#statements' AS table_name, *
FROM #statements AS s
OPTION (RECOMPILE);

SELECT '#query_plan' AS table_name, *
FROM #query_plan AS qp
OPTION (RECOMPILE);

SELECT '#relop' AS table_name, *
FROM #relop AS r
OPTION (RECOMPILE);

SELECT '#plan_cost' AS table_name,  * 
FROM #plan_cost AS pc
OPTION (RECOMPILE);

SELECT '#est_rows' AS table_name,  * 
FROM #est_rows AS er
OPTION (RECOMPILE);

SELECT '#stored_proc_info' AS table_name, *
FROM #stored_proc_info AS spi
OPTION(RECOMPILE);

SELECT '#conversion_info' AS table_name, *
FROM #conversion_info AS ci
OPTION ( RECOMPILE );

SELECT '#variable_info' AS table_name, *
FROM #variable_info AS vi
OPTION ( RECOMPILE );

SELECT '#missing_index_xml' AS table_name, *
FROM   #missing_index_xml
OPTION ( RECOMPILE );

SELECT '#missing_index_schema' AS table_name, *
FROM   #missing_index_schema
OPTION ( RECOMPILE );

SELECT '#missing_index_usage' AS table_name, *
FROM   #missing_index_usage
OPTION ( RECOMPILE );

SELECT '#missing_index_detail' AS table_name, *
FROM   #missing_index_detail
OPTION ( RECOMPILE );

SELECT '#missing_index_pretty' AS table_name, *
FROM   #missing_index_pretty
OPTION ( RECOMPILE );

END; 

END TRY
BEGIN CATCH
        RAISERROR (N'Failure returning debug temp tables', 0,1) WITH NOWAIT;

        IF @sql_select IS NOT NULL
        BEGIN
            SET @msg = N'Last @sql_select: ' + @sql_select;
            RAISERROR(@msg, 0, 1) WITH NOWAIT;
        END;

        SELECT    @msg = @DatabaseName + N' database failed to process. ' + ERROR_MESSAGE(), @error_severity = ERROR_SEVERITY(), @error_state = ERROR_STATE();
        RAISERROR (@msg, @error_severity, @error_state) WITH NOWAIT;
        
        
        WHILE @@TRANCOUNT > 0 
            ROLLBACK;

        RETURN;
END CATCH;

/*
Ways to run this thing

--Debug
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Debug = 1

--Get the top 1
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @Debug = 1

--Use a StartDate												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170527'
				
--Use an EndDate												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @EndDate = '20170527'
				
--Use Both												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170526', @EndDate = '20170527'

--Set a minimum execution count												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @MinimumExecutionCount = 10

--Set a duration minimum
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @DurationFilter = 5

--Look for a stored procedure name (that doesn't exist!)
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'blah'

--Look for a stored procedure name that does (at least On My Computer®)
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'UserReportExtended'

--Look for failed queries
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @Failed = 1

--Filter by plan_id
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @PlanIdFilter = 3356

--Filter by query_id
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @QueryIdFilter = 2958

*/

END;

GO 
