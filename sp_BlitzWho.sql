IF OBJECT_ID('dbo.sp_BlitzWho') IS NULL
	EXEC ('CREATE PROCEDURE dbo.sp_BlitzWho AS RETURN 0;')
GO

ALTER PROCEDURE dbo.sp_BlitzWho 
	@Help TINYINT = 0 ,
	@ShowSleepingSPIDs TINYINT = 0,
	@ExpertMode BIT = 0,
	@Debug BIT = 0,
	@OutputDatabaseName NVARCHAR(256) = NULL ,
	@OutputSchemaName NVARCHAR(256) = NULL ,
	@OutputTableName NVARCHAR(256) = NULL ,
	@OutputTableRetentionDays TINYINT = 3 ,
	@MinElapsedSeconds INT = 0 ,
	@MinCPUTime INT = 0 ,
	@MinLogicalReads INT = 0 ,
	@MinPhysicalReads INT = 0 ,
	@MinWrites INT = 0 ,
	@MinTempdbMB INT = 0 ,
	@MinRequestedMemoryKB INT = 0 ,
	@MinBlockingSeconds INT = 0 ,
	@CheckDateOverride DATETIMEOFFSET = NULL,
	@ShowActualParameters BIT = 0,
	@GetOuterCommand BIT = 0,
	@GetLiveQueryPlan BIT = 0,
	@Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0,
	@SortOrder NVARCHAR(256) = N'elapsed time'
AS
BEGIN
	SET NOCOUNT ON;
	SET STATISTICS XML OFF;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	SELECT @Version = '8.10', @VersionDate = '20220718';
    
	IF(@VersionCheckMode = 1)
	BEGIN
		RETURN;
	END;



	IF @Help = 1
	BEGIN
		PRINT '
sp_BlitzWho from http://FirstResponderKit.org

This script gives you a snapshot of everything currently executing on your SQL Server.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
 - Outputting to table is only supported with SQL Server 2012 and higher.
 - If @OutputDatabaseName and @OutputSchemaName are populated, the database and
   schema must already exist. We will not create them, only the table.
   
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

/* Get the major and minor build numbers */
DECLARE  @ProductVersion NVARCHAR(128)
		,@ProductVersionMajor DECIMAL(10,2)
		,@ProductVersionMinor DECIMAL(10,2)
		,@Platform NVARCHAR(8) /* Azure or NonAzure are acceptable */ = (SELECT CASE WHEN @@VERSION LIKE '%Azure%' THEN N'Azure' ELSE N'NonAzure' END AS [Platform])
		,@EnhanceFlag BIT = 0
		,@BlockingCheck NVARCHAR(MAX)
		,@StringToSelect NVARCHAR(MAX)
		,@StringToExecute NVARCHAR(MAX)
		,@OutputTableCleanupDate DATE
		,@SessionWaits BIT = 0
		,@SessionWaitsSQL NVARCHAR(MAX) = 
						 N'LEFT JOIN ( SELECT DISTINCT
												wait.session_id ,
												( SELECT TOP  5 waitwait.wait_type + N'' (''
												     + CAST(MAX(waitwait.wait_time_ms) AS NVARCHAR(128))
												     + N'' ms), ''
												 FROM   sys.dm_exec_session_wait_stats AS waitwait
												 WHERE  waitwait.session_id = wait.session_id
												 GROUP BY  waitwait.wait_type
												 HAVING SUM(waitwait.wait_time_ms) > 5
												 ORDER BY 1												 
												 FOR
												 XML PATH('''') ) AS session_wait_info
										FROM sys.dm_exec_session_wait_stats AS wait ) AS wt2
						ON   s.session_id = wt2.session_id
						LEFT JOIN sys.dm_exec_query_stats AS session_stats
						ON   r.sql_handle = session_stats.sql_handle
								AND r.plan_handle = session_stats.plan_handle
						  AND r.statement_start_offset = session_stats.statement_start_offset
						  AND r.statement_end_offset = session_stats.statement_end_offset' 
		,@ObjectFullName NVARCHAR(2000)
		,@OutputTableNameQueryStats_View NVARCHAR(256)
		,@LineFeed NVARCHAR(MAX) /* Had to set as MAX up from 10 as it was truncating the view creation*/;

/* Let's get @SortOrder set to lower case here for comparisons later */
SET @SortOrder = REPLACE(LOWER(@SortOrder), N' ', N'_');

SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @ProductVersionMajor = SUBSTRING(@ProductVersion, 1,CHARINDEX('.', @ProductVersion) + 1 ),
    @ProductVersionMinor = PARSENAME(CONVERT(VARCHAR(32), @ProductVersion), 2)

SELECT
	@OutputTableNameQueryStats_View = QUOTENAME(@OutputTableName + '_Deltas'),
	@OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
	@OutputSchemaName = QUOTENAME(@OutputSchemaName),
	@OutputTableName = QUOTENAME(@OutputTableName),
	@LineFeed = CHAR(13) + CHAR(10);

IF @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL
  AND EXISTS ( SELECT *
      FROM   sys.databases
      WHERE  QUOTENAME([name]) = @OutputDatabaseName)
	 BEGIN
	 SET @ExpertMode = 1; /* Force ExpertMode when we're logging to table */

	 /* Create the table if it doesn't exist */
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
	  + N'(';
	SET @StringToExecute = @StringToExecute + N'
	ID INT IDENTITY(1,1) NOT NULL,
	ServerName NVARCHAR(128) NOT NULL,
	CheckDate DATETIMEOFFSET NOT NULL,
	[elapsed_time] [varchar](41) NULL,
	[session_id] [smallint] NOT NULL,
	[database_name] [nvarchar](128) NULL,
	[query_text] [nvarchar](max) NULL,
	[outer_command] NVARCHAR(4000) NULL,
	[query_plan] [xml] NULL,
	[live_query_plan] [xml] NULL,
	[cached_parameter_info] [nvarchar](max) NULL,
	[live_parameter_info] [nvarchar](max) NULL,
	[query_cost] [float] NULL,
	[status] [nvarchar](30) NOT NULL,
	[wait_info] [nvarchar](max) NULL,
	[wait_resource] [nvarchar](max) NULL,
	[top_session_waits] [nvarchar](max) NULL,
	[blocking_session_id] [smallint] NULL,
	[open_transaction_count] [int] NULL,
	[is_implicit_transaction] [int] NOT NULL,
	[nt_domain] [nvarchar](128) NULL,
	[host_name] [nvarchar](128) NULL,
	[login_name] [nvarchar](128) NOT NULL,
	[nt_user_name] [nvarchar](128) NULL,
	[program_name] [nvarchar](128) NULL,
	[fix_parameter_sniffing] [nvarchar](150) NULL,
	[client_interface_name] [nvarchar](32) NULL,
	[login_time] [datetime] NOT NULL,
	[start_time] [datetime] NULL,
	[request_time] [datetime] NULL,
	[request_cpu_time] [int] NULL,
	[request_logical_reads] [bigint] NULL,
	[request_writes] [bigint] NULL,
	[request_physical_reads] [bigint] NULL,
	[session_cpu] [int] NOT NULL,
	[session_logical_reads] [bigint] NOT NULL,
	[session_physical_reads] [bigint] NOT NULL,
	[session_writes] [bigint] NOT NULL,
	[tempdb_allocations_mb] [decimal](38, 2) NULL,
	[memory_usage] [int] NOT NULL,
	[estimated_completion_time] [bigint] NULL,
	[percent_complete] [real] NULL,
	[deadlock_priority] [int] NULL,
	[transaction_isolation_level] [varchar](33) NOT NULL,
	[degree_of_parallelism] [smallint] NULL,
	[last_dop] [bigint] NULL,
	[min_dop] [bigint] NULL,
	[max_dop] [bigint] NULL,
	[last_grant_kb] [bigint] NULL,
	[min_grant_kb] [bigint] NULL,
	[max_grant_kb] [bigint] NULL,
	[last_used_grant_kb] [bigint] NULL,
	[min_used_grant_kb] [bigint] NULL,
	[max_used_grant_kb] [bigint] NULL,
	[last_ideal_grant_kb] [bigint] NULL,
	[min_ideal_grant_kb] [bigint] NULL,
	[max_ideal_grant_kb] [bigint] NULL,
	[last_reserved_threads] [bigint] NULL,
	[min_reserved_threads] [bigint] NULL,
	[max_reserved_threads] [bigint] NULL,
	[last_used_threads] [bigint] NULL,
	[min_used_threads] [bigint] NULL,
	[max_used_threads] [bigint] NULL,
	[grant_time] [varchar](20) NULL,
	[requested_memory_kb] [bigint] NULL,
	[grant_memory_kb] [bigint] NULL,
	[is_request_granted] [varchar](39) NOT NULL,
	[required_memory_kb] [bigint] NULL,
	[query_memory_grant_used_memory_kb] [bigint] NULL,
	[ideal_memory_kb] [bigint] NULL,
	[is_small] [bit] NULL,
	[timeout_sec] [int] NULL,
	[resource_semaphore_id] [smallint] NULL,
	[wait_order] [varchar](20) NULL,
	[wait_time_ms] [varchar](20) NULL,
	[next_candidate_for_memory_grant] [varchar](3) NOT NULL,
	[target_memory_kb] [bigint] NULL,
	[max_target_memory_kb] [varchar](30) NULL,
	[total_memory_kb] [bigint] NULL,
	[available_memory_kb] [bigint] NULL,
	[granted_memory_kb] [bigint] NULL,
	[query_resource_semaphore_used_memory_kb] [bigint] NULL,
	[grantee_count] [int] NULL,
	[waiter_count] [int] NULL,
	[timeout_error_count] [bigint] NULL,
	[forced_grant_count] [varchar](30) NULL,
	[workload_group_name] [sysname] NULL,
	[resource_pool_name] [sysname] NULL,
	[context_info] [varchar](128) NULL,
	[query_hash] [binary](8) NULL,
	[query_plan_hash] [binary](8) NULL,
	[sql_handle] [varbinary] (64) NULL,
	[plan_handle] [varbinary] (64) NULL,
	[statement_start_offset] INT NULL,
	[statement_end_offset] INT NULL,
	JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
	PRIMARY KEY CLUSTERED (ID ASC));';
	IF @Debug = 1
		BEGIN
			PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 0, 8000))
			PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 8000, 16000))
		END
	EXEC(@StringToExecute);

	/* If the table doesn't have the new JoinKey computed column, add it. See Github #2162. */
	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
	SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
		WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
		ALTER TABLE ' + @ObjectFullName + N' ADD JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50));';
	EXEC(@StringToExecute);

	/* If the table doesn't have the new cached_parameter_info computed column, add it. See Github #2842. */
	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
	SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
		WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''cached_parameter_info'')
		ALTER TABLE ' + @ObjectFullName + N' ADD cached_parameter_info NVARCHAR(MAX) NULL;';
	EXEC(@StringToExecute);

	/* If the table doesn't have the new live_parameter_info computed column, add it. See Github #2842. */
	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
	SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
		WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''live_parameter_info'')
		ALTER TABLE ' + @ObjectFullName + N' ADD live_parameter_info NVARCHAR(MAX) NULL;';
	EXEC(@StringToExecute);

	/* If the table doesn't have the new outer_command column, add it. See Github #2887. */
	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
	SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
		WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''outer_command'')
		ALTER TABLE ' + @ObjectFullName + N' ADD outer_command NVARCHAR(4000) NULL;';
	EXEC(@StringToExecute);

	/* If the table doesn't have the new wait_resource column, add it. See Github #2970. */
	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
	SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
		WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''wait_resource'')
		ALTER TABLE ' + @ObjectFullName + N' ADD wait_resource NVARCHAR(MAX) NULL;';
	EXEC(@StringToExecute);

	/* Delete history older than @OutputTableRetentionDays */
	SET @OutputTableCleanupDate = CAST( (DATEADD(DAY, -1 * @OutputTableRetentionDays, GETDATE() ) ) AS DATE);
	SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
		+ @OutputDatabaseName
		+ N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
		+ @OutputSchemaName + N''') DELETE '
		+ @OutputDatabaseName + '.'
		+ @OutputSchemaName + '.'
		+ @OutputTableName
		+ N' WHERE ServerName = @SrvName AND CheckDate < @CheckDate;';
	IF @Debug = 1
		BEGIN
			PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 0, 8000))
			PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 8000, 16000))
		END
	EXEC sp_executesql @StringToExecute,
		N'@SrvName NVARCHAR(128), @CheckDate date',
		@@SERVERNAME, @OutputTableCleanupDate;

	SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNameQueryStats_View;

        /* Create the view */
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = N'USE '
                + @OutputDatabaseName
                + N'; EXEC (''CREATE VIEW '
                + @OutputSchemaName + '.'
                + @OutputTableNameQueryStats_View + N' AS ' + @LineFeed
				+ N'WITH MaxQueryDuration AS ' + @LineFeed
				+ N'( ' + @LineFeed
				+ N'    SELECT ' + @LineFeed
				+ N'        MIN([ID]) AS [MinID], ' + @LineFeed
				+ N'		MAX([ID]) AS [MaxID] ' + @LineFeed
				+ N'    FROM ' + @OutputSchemaName + '.' + @OutputTableName + '' + @LineFeed
				+ N'    GROUP BY [ServerName], ' + @LineFeed
				+ N'    [session_id], ' + @LineFeed
				+ N'    [database_name], ' + @LineFeed
				+ N'    [request_time], ' + @LineFeed
				+ N'    [start_time], ' + @LineFeed
				+ N'    [sql_handle] ' + @LineFeed
				+ N') ' + @LineFeed
				+ N'SELECT ' + @LineFeed
				+ N'    [ID], ' + @LineFeed
				+ N'    [ServerName], ' + @LineFeed
				+ N'    [CheckDate], ' + @LineFeed
				+ N'    [elapsed_time], ' + @LineFeed 
				+ N'    [session_id], ' + @LineFeed 
				+ N'    [database_name], ' + @LineFeed 
				+ N'    [query_text_snippet], ' + @LineFeed 
				+ N'    [query_plan], ' + @LineFeed 
				+ N'    [live_query_plan], ' + @LineFeed 
				+ N'    [query_cost], ' + @LineFeed 
				+ N'    [status], ' + @LineFeed 
				+ N'    [wait_info], ' + @LineFeed 
				+ N'    [wait_resource], ' + @LineFeed 
				+ N'    [top_session_waits], ' + @LineFeed 
				+ N'    [blocking_session_id], ' + @LineFeed 
				+ N'    [open_transaction_count], ' + @LineFeed 
				+ N'    [is_implicit_transaction], ' + @LineFeed 
				+ N'    [nt_domain], ' + @LineFeed 
				+ N'    [host_name], ' + @LineFeed 
				+ N'    [login_name], ' + @LineFeed 
				+ N'    [nt_user_name], ' + @LineFeed 
				+ N'    [program_name], ' + @LineFeed 
				+ N'    [fix_parameter_sniffing], ' + @LineFeed 
				+ N'    [client_interface_name], ' + @LineFeed 
				+ N'    [login_time], ' + @LineFeed 
				+ N'    [start_time], ' + @LineFeed 
				+ N'    [request_time], ' + @LineFeed 
				+ N'    [request_cpu_time], ' + @LineFeed 
				+ N'    [degree_of_parallelism], ' + @LineFeed 
				+ N'    [request_logical_reads], ' + @LineFeed 
				+ N'    [Logical_Reads_MB], ' + @LineFeed 
				+ N'    [request_writes], ' + @LineFeed 
				+ N'    [Logical_Writes_MB], ' + @LineFeed 
				+ N'    [request_physical_reads], ' + @LineFeed 
				+ N'    [Physical_reads_MB], ' + @LineFeed 
				+ N'    [session_cpu], ' + @LineFeed 
				+ N'    [session_logical_reads], ' + @LineFeed 
				+ N'    [session_logical_reads_MB], ' + @LineFeed 
				+ N'    [session_physical_reads], ' + @LineFeed 
				+ N'    [session_physical_reads_MB], ' + @LineFeed 
				+ N'    [session_writes], ' + @LineFeed 
				+ N'    [session_writes_MB], ' + @LineFeed 
				+ N'    [tempdb_allocations_mb], ' + @LineFeed 
				+ N'    [memory_usage], ' + @LineFeed 
				+ N'    [estimated_completion_time], ' + @LineFeed 
				+ N'    [percent_complete], ' + @LineFeed 
				+ N'    [deadlock_priority], ' + @LineFeed 
				+ N'    [transaction_isolation_level], ' + @LineFeed 
				+ N'    [last_dop], ' + @LineFeed 
				+ N'    [min_dop], ' + @LineFeed 
				+ N'    [max_dop], ' + @LineFeed 
				+ N'    [last_grant_kb], ' + @LineFeed 
				+ N'    [min_grant_kb], ' + @LineFeed 
				+ N'    [max_grant_kb], ' + @LineFeed 
				+ N'    [last_used_grant_kb], ' + @LineFeed 
				+ N'    [min_used_grant_kb], ' + @LineFeed 
				+ N'    [max_used_grant_kb], ' + @LineFeed 
				+ N'    [last_ideal_grant_kb], ' + @LineFeed 
				+ N'    [min_ideal_grant_kb], ' + @LineFeed 
				+ N'    [max_ideal_grant_kb], ' + @LineFeed 
				+ N'    [last_reserved_threads], ' + @LineFeed 
				+ N'    [min_reserved_threads], ' + @LineFeed 
				+ N'    [max_reserved_threads], ' + @LineFeed 
				+ N'    [last_used_threads], ' + @LineFeed 
				+ N'    [min_used_threads], ' + @LineFeed 
				+ N'    [max_used_threads], ' + @LineFeed 
				+ N'    [grant_time], ' + @LineFeed 
				+ N'    [requested_memory_kb], ' + @LineFeed 
				+ N'    [grant_memory_kb], ' + @LineFeed 
				+ N'    [is_request_granted], ' + @LineFeed 
				+ N'    [required_memory_kb], ' + @LineFeed 
				+ N'    [query_memory_grant_used_memory_kb], ' + @LineFeed 
				+ N'    [ideal_memory_kb], ' + @LineFeed 
				+ N'    [is_small], ' + @LineFeed 
				+ N'    [timeout_sec], ' + @LineFeed 
				+ N'    [resource_semaphore_id], ' + @LineFeed 
				+ N'    [wait_order], ' + @LineFeed 
				+ N'    [wait_time_ms], ' + @LineFeed 
				+ N'    [next_candidate_for_memory_grant], ' + @LineFeed 
				+ N'    [target_memory_kb], ' + @LineFeed 
				+ N'    [max_target_memory_kb], ' + @LineFeed 
				+ N'    [total_memory_kb], ' + @LineFeed 
				+ N'    [available_memory_kb], ' + @LineFeed 
				+ N'    [granted_memory_kb], ' + @LineFeed 
				+ N'    [query_resource_semaphore_used_memory_kb], ' + @LineFeed 
				+ N'    [grantee_count], ' + @LineFeed 
				+ N'    [waiter_count], ' + @LineFeed 
				+ N'    [timeout_error_count], ' + @LineFeed 
				+ N'    [forced_grant_count], ' + @LineFeed 
				+ N'    [workload_group_name], ' + @LineFeed 
				+ N'    [resource_pool_name], ' + @LineFeed 
				+ N'    [context_info], ' + @LineFeed 
				+ N'    [query_hash], ' + @LineFeed 
				+ N'    [query_plan_hash], ' + @LineFeed 
				+ N'    [sql_handle], ' + @LineFeed 
				+ N'    [plan_handle], ' + @LineFeed 
				+ N'    [statement_start_offset], ' + @LineFeed 
				+ N'    [statement_end_offset] ' + @LineFeed
				+ N'    FROM ' + @LineFeed
				+ N'        ( ' + @LineFeed
				+ N'            SELECT ' + @LineFeed
				+ N'                   [ID], ' + @LineFeed
				+ N'			       [ServerName], ' + @LineFeed 
				+ N'			       [CheckDate], ' + @LineFeed 
				+ N'			       [elapsed_time], ' + @LineFeed 
				+ N'			       [session_id], ' + @LineFeed 
				+ N'			       [database_name], ' + @LineFeed 
				+ N'			       /* Truncate the query text to aid performance of painting the rows in SSMS */ ' + @LineFeed
				+ N'			       CAST([query_text] AS NVARCHAR(1000)) AS [query_text_snippet], ' + @LineFeed 
				+ N'			       [query_plan], ' + @LineFeed 
				+ N'			       [live_query_plan], ' + @LineFeed 
				+ N'			       [query_cost], ' + @LineFeed 
				+ N'			       [status], ' + @LineFeed 
				+ N'			       [wait_info], ' + @LineFeed 
				+ N'			       [wait_resource], ' + @LineFeed 
				+ N'			       [top_session_waits], ' + @LineFeed 
				+ N'			       [blocking_session_id], ' + @LineFeed 
				+ N'			       [open_transaction_count], ' + @LineFeed 
				+ N'			       [is_implicit_transaction], ' + @LineFeed 
				+ N'			       [nt_domain], ' + @LineFeed 
				+ N'			       [host_name], ' + @LineFeed 
				+ N'			       [login_name], ' + @LineFeed 
				+ N'			       [nt_user_name], ' + @LineFeed 
				+ N'			       [program_name], ' + @LineFeed 
				+ N'			       [fix_parameter_sniffing], ' + @LineFeed 
				+ N'			       [client_interface_name], ' + @LineFeed 
				+ N'			       [login_time], ' + @LineFeed 
				+ N'			       [start_time], ' + @LineFeed 
				+ N'			       [request_time], ' + @LineFeed 
				+ N'			       [request_cpu_time], ' + @LineFeed 
				+ N'			       [degree_of_parallelism], ' + @LineFeed 
				+ N'			       [request_logical_reads], ' + @LineFeed 
				+ N'			       ((CAST([request_logical_reads] AS DECIMAL(38,2))* 8)/ 1024) [Logical_Reads_MB], ' + @LineFeed 
				+ N'			       [request_writes], ' + @LineFeed 
				+ N'			       ((CAST([request_writes] AS DECIMAL(38,2))* 8)/ 1024) [Logical_Writes_MB], ' + @LineFeed 
				+ N'			       [request_physical_reads], ' + @LineFeed 
				+ N'			       ((CAST([request_physical_reads] AS DECIMAL(38,2))* 8)/ 1024) [Physical_reads_MB], ' + @LineFeed 
				+ N'			       [session_cpu], ' + @LineFeed 
				+ N'			       [session_logical_reads], ' + @LineFeed 
				+ N'			       ((CAST([session_logical_reads] AS DECIMAL(38,2))* 8)/ 1024) [session_logical_reads_MB], ' + @LineFeed 
				+ N'			       [session_physical_reads], ' + @LineFeed 
				+ N'			       ((CAST([session_physical_reads] AS DECIMAL(38,2))* 8)/ 1024) [session_physical_reads_MB], ' + @LineFeed 
				+ N'			       [session_writes], ' + @LineFeed 
				+ N'			       ((CAST([session_writes] AS DECIMAL(38,2))* 8)/ 1024) [session_writes_MB], ' + @LineFeed 
				+ N'			       [tempdb_allocations_mb], ' + @LineFeed 
				+ N'			       [memory_usage], ' + @LineFeed 
				+ N'			       [estimated_completion_time], ' + @LineFeed 
				+ N'			       [percent_complete], ' + @LineFeed 
				+ N'			       [deadlock_priority], ' + @LineFeed 
				+ N'			       [transaction_isolation_level], ' + @LineFeed 
				+ N'			       [last_dop], ' + @LineFeed 
				+ N'			       [min_dop], ' + @LineFeed 
				+ N'			       [max_dop], ' + @LineFeed 
				+ N'			       [last_grant_kb], ' + @LineFeed 
				+ N'			       [min_grant_kb], ' + @LineFeed 
				+ N'			       [max_grant_kb], ' + @LineFeed 
				+ N'			       [last_used_grant_kb], ' + @LineFeed 
				+ N'			       [min_used_grant_kb], ' + @LineFeed 
				+ N'			       [max_used_grant_kb], ' + @LineFeed 
				+ N'			       [last_ideal_grant_kb], ' + @LineFeed 
				+ N'			       [min_ideal_grant_kb], ' + @LineFeed 
				+ N'			       [max_ideal_grant_kb], ' + @LineFeed 
				+ N'			       [last_reserved_threads], ' + @LineFeed 
				+ N'			       [min_reserved_threads], ' + @LineFeed 
				+ N'			       [max_reserved_threads], ' + @LineFeed 
				+ N'			       [last_used_threads], ' + @LineFeed 
				+ N'			       [min_used_threads], ' + @LineFeed 
				+ N'			       [max_used_threads], ' + @LineFeed 
				+ N'			       [grant_time], ' + @LineFeed 
				+ N'			       [requested_memory_kb], ' + @LineFeed 
				+ N'			       [grant_memory_kb], ' + @LineFeed 
				+ N'			       [is_request_granted], ' + @LineFeed 
				+ N'			       [required_memory_kb], ' + @LineFeed 
				+ N'			       [query_memory_grant_used_memory_kb], ' + @LineFeed 
				+ N'			       [ideal_memory_kb], ' + @LineFeed 
				+ N'			       [is_small], ' + @LineFeed 
				+ N'			       [timeout_sec], ' + @LineFeed 
				+ N'			       [resource_semaphore_id], ' + @LineFeed 
				+ N'			       [wait_order], ' + @LineFeed 
				+ N'			       [wait_time_ms], ' + @LineFeed 
				+ N'			       [next_candidate_for_memory_grant], ' + @LineFeed 
				+ N'			       [target_memory_kb], ' + @LineFeed 
				+ N'			       [max_target_memory_kb], ' + @LineFeed 
				+ N'			       [total_memory_kb], ' + @LineFeed 
				+ N'			       [available_memory_kb], ' + @LineFeed 
				+ N'			       [granted_memory_kb], ' + @LineFeed 
				+ N'			       [query_resource_semaphore_used_memory_kb], ' + @LineFeed 
				+ N'			       [grantee_count], ' + @LineFeed 
				+ N'			       [waiter_count], ' + @LineFeed 
				+ N'			       [timeout_error_count], ' + @LineFeed 
				+ N'			       [forced_grant_count], ' + @LineFeed 
				+ N'			       [workload_group_name], ' + @LineFeed 
				+ N'			       [resource_pool_name], ' + @LineFeed 
				+ N'			       [context_info], ' + @LineFeed 
				+ N'			       [query_hash], ' + @LineFeed 
				+ N'			       [query_plan_hash], ' + @LineFeed 
				+ N'			       [sql_handle], ' + @LineFeed 
				+ N'			       [plan_handle], ' + @LineFeed 
				+ N'			       [statement_start_offset], ' + @LineFeed 
				+ N'			       [statement_end_offset] ' + @LineFeed
				+ N'            FROM ' + @OutputSchemaName + '.' + @OutputTableName + '' + @LineFeed 
				+ N'        ) AS [BlitzWho] ' + @LineFeed
				+ N'INNER JOIN [MaxQueryDuration] ON [BlitzWho].[ID] = [MaxQueryDuration].[MaxID]; ' + @LineFeed
				+ N''');'

			IF @Debug = 1
				BEGIN
					PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 0, 8000))
					PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 8000, 16000))
				END

				EXEC(@StringToExecute);
            END;

 END

 IF OBJECT_ID('tempdb..#WhoReadableDBs') IS NOT NULL 
	DROP TABLE #WhoReadableDBs;

CREATE TABLE #WhoReadableDBs 
(
database_id INT
);

IF EXISTS (SELECT * FROM sys.all_objects o WHERE o.name = 'dm_hadr_database_replica_states')
BEGIN
	RAISERROR('Checking for Read intent databases to exclude',0,0) WITH NOWAIT;

	EXEC('INSERT INTO #WhoReadableDBs (database_id) SELECT DBs.database_id FROM sys.databases DBs INNER JOIN sys.availability_replicas Replicas ON DBs.replica_id = Replicas.replica_id WHERE replica_server_name NOT IN (SELECT DISTINCT primary_replica FROM sys.dm_hadr_availability_group_states States) AND Replicas.secondary_role_allow_connections_desc = ''READ_ONLY'' AND replica_server_name = @@SERVERNAME;');
END

SELECT @BlockingCheck = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
   
						SET LOCK_TIMEOUT 1000; /* To avoid blocking on live query plans. See Github issue #2907. */
						DECLARE @blocked TABLE 
								(
								    dbid SMALLINT NOT NULL,
								    last_batch DATETIME NOT NULL,
								    open_tran SMALLINT NOT NULL,
								    sql_handle BINARY(20) NOT NULL,
								    session_id SMALLINT NOT NULL,
								    blocking_session_id SMALLINT NOT NULL,
								    lastwaittype NCHAR(32) NOT NULL,
								    waittime BIGINT NOT NULL,
								    cpu INT NOT NULL,
								    physical_io BIGINT NOT NULL,
								    memusage INT NOT NULL
								); 
						
						INSERT @blocked ( dbid, last_batch, open_tran, sql_handle, session_id, blocking_session_id, lastwaittype, waittime, cpu, physical_io, memusage )
						SELECT
							sys1.dbid, sys1.last_batch, sys1.open_tran, sys1.sql_handle, 
							sys2.spid AS session_id, sys2.blocked AS blocking_session_id, sys2.lastwaittype, sys2.waittime, sys2.cpu, sys2.physical_io, sys2.memusage
						FROM sys.sysprocesses AS sys1
						JOIN sys.sysprocesses AS sys2
						ON sys1.spid = sys2.blocked;
						'+CASE
							WHEN (@GetOuterCommand = 1 AND (NOT EXISTS(SELECT 1 FROM sys.all_objects WHERE [name] = N'dm_exec_input_buffer'))) THEN N'
						DECLARE @session_id SMALLINT;
						DECLARE @Sessions TABLE 
						(
							session_id INT
						);

						DECLARE @inputbuffer TABLE 
						(
						ID INT IDENTITY(1,1),
						session_id INT,
						event_type	NVARCHAR(30),
						parameters	SMALLINT,
						event_info	NVARCHAR(4000)
						);

						DECLARE inputbuffer_cursor
						
						CURSOR LOCAL FAST_FORWARD
						FOR 
						SELECT session_id
						FROM sys.dm_exec_sessions
						WHERE session_id <> @@SPID
						AND is_user_process = 1;
						
						OPEN inputbuffer_cursor;
						
						FETCH NEXT FROM inputbuffer_cursor INTO @session_id;
						
						WHILE (@@FETCH_STATUS = 0)
						BEGIN;
							BEGIN TRY;

								INSERT INTO @inputbuffer ([event_type],[parameters],[event_info])
								EXEC sp_executesql
									N''DBCC INPUTBUFFER(@session_id) WITH NO_INFOMSGS;'',
									N''@session_id SMALLINT'',
									@session_id;
						
								UPDATE @inputbuffer 
								SET session_id = @session_id 
								WHERE ID = SCOPE_IDENTITY();

							END TRY
							BEGIN CATCH
								RAISERROR(''DBCC inputbuffer failed for session %d'',0,0,@session_id) WITH NOWAIT;
							END CATCH;
						
							FETCH NEXT FROM inputbuffer_cursor INTO @session_id
						
						END;
						
						CLOSE inputbuffer_cursor;
						DEALLOCATE inputbuffer_cursor;'
						ELSE N''
						END+
						N' 

						DECLARE @LiveQueryPlans TABLE
						(
							Session_Id INT NOT NULL,
							Query_Plan XML NOT NULL
						);

						'
IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_exec_query_statistics_xml') AND name = 'query_plan' AND @GetLiveQueryPlan=1)
BEGIN
	SET @BlockingCheck = @BlockingCheck + N'
							INSERT INTO @LiveQueryPlans
							SELECT	s.session_id, query_plan 
							FROM	sys.dm_exec_sessions AS s
							CROSS APPLY sys.dm_exec_query_statistics_xml(s.session_id)
							WHERE	s.session_id <> @@SPID;';
END


IF @ProductVersionMajor > 9 and @ProductVersionMajor < 11
BEGIN
    /* Think of the StringToExecute as starting with this, but we'll set this up later depending on whether we're doing an insert or a select:
    SELECT @StringToExecute = N'SELECT  GETDATE() AS run_date ,
    */
    SET @StringToExecute = N'COALESCE( RIGHT(''00'' + CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), RIGHT(''00'' + CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
			       s.session_id ,
						    COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid), ''N/A'') AS database_name,
			       ISNULL(SUBSTRING(dest.text,
			            ( query_stats.statement_start_offset / 2 ) + 1,
			            ( ( CASE query_stats.statement_end_offset
			               WHEN -1 THEN DATALENGTH(dest.text)
			               ELSE query_stats.statement_end_offset
			             END - query_stats.statement_start_offset )
			              / 2 ) + 1), dest.text) AS query_text ,
						  '+CASE
								WHEN @GetOuterCommand = 1 THEN N'CAST(event_info AS NVARCHAR(4000)) AS outer_command,'
							ELSE N''
							END+N'
			       derp.query_plan ,
						    qmg.query_cost ,										   		   
						    s.status ,
							CASE
								WHEN s.status <> ''sleeping'' THEN COALESCE(wt.wait_info, RTRIM(blocked.lastwaittype) + '' ('' + CONVERT(VARCHAR(10), blocked.waittime) + '')'' ) 
								ELSE NULL
							END AS wait_info ,																					
							r.wait_resource ,
						    CASE WHEN r.blocking_session_id <> 0 AND blocked.session_id IS NULL 
							       THEN r.blocking_session_id
							       WHEN r.blocking_session_id <> 0 AND s.session_id <> blocked.blocking_session_id 
							       THEN blocked.blocking_session_id
								   WHEN r.blocking_session_id = 0 AND s.session_id = blocked.session_id 
								   THEN blocked.blocking_session_id
								   WHEN r.blocking_session_id <> 0 AND s.session_id = blocked.blocking_session_id 
							       THEN r.blocking_session_id
							       ELSE NULL 
						      END AS blocking_session_id,
			       COALESCE(r.open_transaction_count, blocked.open_tran) AS open_transaction_count ,
						    CASE WHEN EXISTS (  SELECT 1 
               FROM sys.dm_tran_active_transactions AS tat
               JOIN sys.dm_tran_session_transactions AS tst
               ON tst.transaction_id = tat.transaction_id
               WHERE tat.name = ''implicit_transaction''
               AND s.session_id = tst.session_id 
               )  THEN 1 
            ELSE 0 
          END AS is_implicit_transaction ,
					     s.nt_domain ,
			       s.host_name ,
			       s.login_name ,
			       s.nt_user_name ,'
		IF @Platform = 'NonAzure'
		BEGIN
		SET @StringToExecute +=
				   N'program_name = COALESCE((
					SELECT REPLACE(program_name,Substring(program_name,30,34),''"''+j.name+''"'') 
					FROM msdb.dbo.sysjobs j WHERE Substring(program_name,32,32) = CONVERT(char(32),CAST(j.job_id AS binary(16)),2)
					),s.program_name)'
		END
		ELSE
		BEGIN
		SET @StringToExecute += N's.program_name'
		END
						
    IF @ExpertMode = 1
    BEGIN
    SET @StringToExecute += 
			   N',
						''DBCC FREEPROCCACHE ('' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + '');'' AS fix_parameter_sniffing,						      		
			   s.client_interface_name ,
			   s.login_time ,
			   r.start_time ,
			   qmg.request_time ,
						COALESCE(r.cpu_time, s.cpu_time) AS request_cpu_time,
			   COALESCE(r.logical_reads, s.logical_reads) AS request_logical_reads,
			   COALESCE(r.writes, s.writes) AS request_writes,
			   COALESCE(r.reads, s.reads) AS request_physical_reads ,
			   s.cpu_time AS session_cpu,
			   s.logical_reads AS session_logical_reads,
			   s.reads AS session_physical_reads ,
			   s.writes AS session_writes,					
						tempdb_allocations.tempdb_allocations_mb,
			   s.memory_usage ,
			   r.estimated_completion_time , 	
						r.percent_complete , 
			   r.deadlock_priority ,
			   CASE 
			     WHEN s.transaction_isolation_level = 0 THEN ''Unspecified''
			     WHEN s.transaction_isolation_level = 1 THEN ''Read Uncommitted''
			     WHEN s.transaction_isolation_level = 2 AND EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME(r.database_id) AND is_read_committed_snapshot_on = 1) THEN ''Read Committed Snapshot Isolation''
			     WHEN s.transaction_isolation_level = 2 THEN ''Read Committed''
			     WHEN s.transaction_isolation_level = 3 THEN ''Repeatable Read''
			     WHEN s.transaction_isolation_level = 4 THEN ''Serializable''
			     WHEN s.transaction_isolation_level = 5 THEN ''Snapshot''
			     ELSE ''WHAT HAVE YOU DONE?''
			   END AS transaction_isolation_level ,				
						qmg.dop AS degree_of_parallelism ,
			   COALESCE(CAST(qmg.grant_time AS VARCHAR(20)), ''N/A'') AS grant_time ,
			   qmg.requested_memory_kb ,
			   qmg.granted_memory_kb AS grant_memory_kb,
			   CASE WHEN qmg.grant_time IS NULL THEN ''N/A''
        WHEN qmg.requested_memory_kb < qmg.granted_memory_kb
			     THEN ''Query Granted Less Than Query Requested''
			     ELSE ''Memory Request Granted''
			   END AS is_request_granted ,
			   qmg.required_memory_kb ,
			   qmg.used_memory_kb AS query_memory_grant_used_memory_kb,
			   qmg.ideal_memory_kb ,
			   qmg.is_small ,
			   qmg.timeout_sec ,
			   qmg.resource_semaphore_id ,
			   COALESCE(CAST(qmg.wait_order AS VARCHAR(20)), ''N/A'') AS wait_order ,
			   COALESCE(CAST(qmg.wait_time_ms AS VARCHAR(20)),
			      ''N/A'') AS wait_time_ms ,
			   CASE qmg.is_next_candidate
			     WHEN 0 THEN ''No''
			     WHEN 1 THEN ''Yes''
			     ELSE ''N/A''
			   END AS next_candidate_for_memory_grant ,
			   qrs.target_memory_kb ,
			   COALESCE(CAST(qrs.max_target_memory_kb AS VARCHAR(20)),
			      ''Small Query Resource Semaphore'') AS max_target_memory_kb ,
			   qrs.total_memory_kb ,
			   qrs.available_memory_kb ,
			   qrs.granted_memory_kb ,
			   qrs.used_memory_kb AS query_resource_semaphore_used_memory_kb,
			   qrs.grantee_count ,
			   qrs.waiter_count ,
			   qrs.timeout_error_count ,
			   COALESCE(CAST(qrs.forced_grant_count AS VARCHAR(20)),
			      ''Small Query Resource Semaphore'') AS forced_grant_count,
						wg.name AS workload_group_name , 
						rp.name AS resource_pool_name,
 						CONVERT(VARCHAR(128), r.context_info)  AS context_info
						'
	END /* IF @ExpertMode = 1 */
				
    SET @StringToExecute += 			 
	    N'FROM sys.dm_exec_sessions AS s
			 '+
			 CASE
				WHEN @GetOuterCommand = 1 THEN CASE
													WHEN EXISTS(SELECT 1 FROM sys.all_objects WHERE [name] = N'dm_exec_input_buffer') THEN N'OUTER APPLY sys.dm_exec_input_buffer (s.session_id, 0) AS ib'
													ELSE N'LEFT JOIN @inputbuffer ib ON s.session_id = ib.session_id'
												END
				ELSE N''
			 END+N'
			 LEFT JOIN sys.dm_exec_requests AS r
			 ON   r.session_id = s.session_id
			 LEFT JOIN ( SELECT DISTINCT
			      wait.session_id ,
			      ( SELECT waitwait.wait_type + N'' (''
			         + CAST(MAX(waitwait.wait_duration_ms) AS NVARCHAR(128))
			         + N'' ms) ''
			        FROM   sys.dm_os_waiting_tasks AS waitwait
			        WHERE  waitwait.session_id = wait.session_id
			        GROUP BY  waitwait.wait_type
			        ORDER BY  SUM(waitwait.wait_duration_ms) DESC
			      FOR
			        XML PATH('''') ) AS wait_info
			    FROM sys.dm_os_waiting_tasks AS wait ) AS wt
			 ON   s.session_id = wt.session_id
			 LEFT JOIN sys.dm_exec_query_stats AS query_stats
			 ON   r.sql_handle = query_stats.sql_handle
						AND r.plan_handle = query_stats.plan_handle
			   AND r.statement_start_offset = query_stats.statement_start_offset
			   AND r.statement_end_offset = query_stats.statement_end_offset
			 LEFT JOIN sys.dm_exec_query_memory_grants qmg
			 ON   r.session_id = qmg.session_id
						AND r.request_id = qmg.request_id
			 LEFT JOIN sys.dm_exec_query_resource_semaphores qrs
			 ON   qmg.resource_semaphore_id = qrs.resource_semaphore_id
					 AND qmg.pool_id = qrs.pool_id
				LEFT JOIN sys.resource_governor_workload_groups wg 
				ON 		s.group_id = wg.group_id
				LEFT JOIN sys.resource_governor_resource_pools rp 
				ON		wg.pool_id = rp.pool_id
				OUTER APPLY (
								SELECT TOP 1
								b.dbid, b.last_batch, b.open_tran, b.sql_handle, 
								b.session_id, b.blocking_session_id, b.lastwaittype, b.waittime
								FROM @blocked b
								WHERE (s.session_id = b.session_id
										OR s.session_id = b.blocking_session_id)
							) AS blocked				
				OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, blocked.sql_handle)) AS dest
			 OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS derp
				OUTER APPLY (
						SELECT CONVERT(DECIMAL(38,2), SUM( (((tsu.user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8) / 1024.)) ) AS tempdb_allocations_mb
						FROM sys.dm_db_task_space_usage tsu
						WHERE tsu.request_id = r.request_id
						AND tsu.session_id = r.session_id
						AND tsu.session_id = s.session_id
				) as tempdb_allocations
			 WHERE s.session_id <> @@SPID 
				AND s.host_name IS NOT NULL
				'
				+ CASE WHEN @ShowSleepingSPIDs = 0 THEN
						N' AND COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid)) IS NOT NULL'
					  WHEN @ShowSleepingSPIDs = 1 THEN
						N' OR COALESCE(r.open_transaction_count, blocked.open_tran) >= 1'
					 ELSE N'' END;
END /* IF @ProductVersionMajor > 9 and @ProductVersionMajor < 11 */

IF @ProductVersionMajor >= 11 
    BEGIN
    SELECT @EnhanceFlag = 
	     CASE WHEN @ProductVersionMajor = 11 AND @ProductVersionMinor >= 6020 THEN 1
		      WHEN @ProductVersionMajor = 12 AND @ProductVersionMinor >= 5000 THEN 1
		      WHEN @ProductVersionMajor = 13 AND	@ProductVersionMinor >= 1601 THEN 1
			     WHEN @ProductVersionMajor > 13 THEN 1
		      ELSE 0 
	     END


    IF OBJECT_ID('sys.dm_exec_session_wait_stats') IS NOT NULL
    BEGIN
	    SET @SessionWaits = 1
    END

    /* Think of the StringToExecute as starting with this, but we'll set this up later depending on whether we're doing an insert or a select:
    SELECT @StringToExecute = N'SELECT  GETDATE() AS run_date ,
    */
    SELECT @StringToExecute = N'COALESCE( RIGHT(''00'' + CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), RIGHT(''00'' + CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400), 2) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
			       s.session_id ,
						    COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid), ''N/A'') AS database_name,
			       ISNULL(SUBSTRING(dest.text,
			            ( query_stats.statement_start_offset / 2 ) + 1,
			            ( ( CASE query_stats.statement_end_offset
			               WHEN -1 THEN DATALENGTH(dest.text)
			               ELSE query_stats.statement_end_offset
			             END - query_stats.statement_start_offset )
			              / 2 ) + 1), dest.text) AS query_text ,
						  '+CASE
								WHEN @GetOuterCommand = 1 THEN N'CAST(event_info AS NVARCHAR(4000)) AS outer_command,'
							ELSE N''
							END+N'
			       derp.query_plan ,
				   CAST(COALESCE(qs_live.Query_Plan, ' + CASE WHEN @GetLiveQueryPlan=1 
				   		THEN '''<?No live query plan available. To turn on live plans, see https://www.BrentOzar.com/go/liveplans ?>'''
						ELSE '''<?Live Query Plans were not retrieved. Set @GetLiveQueryPlan=1 to try and retrieve Live Query Plans ?>'''
						END
					+') AS XML
				   
				   
				   ) AS live_query_plan ,
					STUFF((SELECT DISTINCT N'', '' + Node.Data.value(''(@Column)[1]'', ''NVARCHAR(4000)'') + N'' {'' + Node.Data.value(''(@ParameterDataType)[1]'', ''NVARCHAR(4000)'') + N''}: '' + Node.Data.value(''(@ParameterCompiledValue)[1]'', ''NVARCHAR(4000)'')
						FROM derp.query_plan.nodes(''/*:ShowPlanXML/*:BatchSequence/*:Batch/*:Statements/*:StmtSimple/*:QueryPlan/*:ParameterList/*:ColumnReference'') AS Node(Data)
						FOR XML PATH('''')), 1,2,'''')
						AS Cached_Parameter_Info,
						'
	IF @ShowActualParameters = 1
	BEGIN
		SELECT @StringToExecute = @StringToExecute + N'qs_live.Live_Parameter_Info as Live_Parameter_Info,'
	END

	SELECT @StringToExecute = @StringToExecute + N'
			       qmg.query_cost ,
			       s.status ,
					CASE
						WHEN s.status <> ''sleeping'' THEN COALESCE(wt.wait_info, RTRIM(blocked.lastwaittype) + '' ('' + CONVERT(VARCHAR(10), blocked.waittime) + '')'' ) 
						ELSE NULL
					END AS wait_info ,
					r.wait_resource ,'
						    +
						    CASE @SessionWaits
							     WHEN 1 THEN + N'SUBSTRING(wt2.session_wait_info, 0, LEN(wt2.session_wait_info) ) AS top_session_waits ,'
							     ELSE N' NULL AS top_session_waits ,'
						    END
						    +																	
						    N'CASE WHEN r.blocking_session_id <> 0 AND blocked.session_id IS NULL 
							       THEN r.blocking_session_id
							       WHEN r.blocking_session_id <> 0 AND s.session_id <> blocked.blocking_session_id 
							       THEN blocked.blocking_session_id
								   WHEN r.blocking_session_id = 0 AND s.session_id = blocked.session_id 
								   THEN blocked.blocking_session_id
								   WHEN r.blocking_session_id <> 0 AND s.session_id = blocked.blocking_session_id 
							       THEN r.blocking_session_id
							       ELSE NULL 
						      END AS blocking_session_id,
			       COALESCE(r.open_transaction_count, blocked.open_tran) AS open_transaction_count ,
						    CASE WHEN EXISTS (  SELECT 1 
               FROM sys.dm_tran_active_transactions AS tat
               JOIN sys.dm_tran_session_transactions AS tst
               ON tst.transaction_id = tat.transaction_id
               WHERE tat.name = ''implicit_transaction''
               AND s.session_id = tst.session_id 
               )  THEN 1 
            ELSE 0 
          END AS is_implicit_transaction ,
					     s.nt_domain ,
			       s.host_name ,
			       s.login_name ,
			       s.nt_user_name ,'
		IF @Platform = 'NonAzure'
		BEGIN
		SET @StringToExecute +=
				   N'program_name = COALESCE((
					SELECT REPLACE(program_name,Substring(program_name,30,34),''"''+j.name+''"'') 
					FROM msdb.dbo.sysjobs j WHERE Substring(program_name,32,32) = CONVERT(char(32),CAST(j.job_id AS binary(16)),2)
					),s.program_name)'
		END
		ELSE
		BEGIN
		SET @StringToExecute += N's.program_name'
		END

    IF @ExpertMode = 1 /* We show more columns in expert mode, so the SELECT gets longer */
    BEGIN
        SET @StringToExecute += 						
	        N', ''DBCC FREEPROCCACHE ('' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + '');'' AS fix_parameter_sniffing,						      		
        s.client_interface_name ,
        s.login_time ,
        r.start_time ,		
        qmg.request_time ,										
		        COALESCE(r.cpu_time, s.cpu_time) AS request_cpu_time,
        COALESCE(r.logical_reads, s.logical_reads) AS request_logical_reads,
        COALESCE(r.writes, s.writes) AS request_writes,
        COALESCE(r.reads, s.reads) AS request_physical_reads ,
        s.cpu_time AS session_cpu,
        s.logical_reads AS session_logical_reads,
        s.reads AS session_physical_reads ,
        s.writes AS session_writes,
		        tempdb_allocations.tempdb_allocations_mb,
        s.memory_usage ,
        r.estimated_completion_time , 
		        r.percent_complete , 
        r.deadlock_priority ,
		        CASE 
	        WHEN s.transaction_isolation_level = 0 THEN ''Unspecified''
	        WHEN s.transaction_isolation_level = 1 THEN ''Read Uncommitted''
			WHEN s.transaction_isolation_level = 2 AND EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME(r.database_id) AND is_read_committed_snapshot_on = 1) THEN ''Read Committed Snapshot Isolation''
			WHEN s.transaction_isolation_level = 2 THEN ''Read Committed''
	        WHEN s.transaction_isolation_level = 3 THEN ''Repeatable Read''
	        WHEN s.transaction_isolation_level = 4 THEN ''Serializable''
	        WHEN s.transaction_isolation_level = 5 THEN ''Snapshot''
	        ELSE ''WHAT HAVE YOU DONE?''
        END AS transaction_isolation_level ,
		        qmg.dop AS degree_of_parallelism ,						'
		        + 
		        CASE @EnhanceFlag
				        WHEN 1 THEN N'query_stats.last_dop,
        query_stats.min_dop,
        query_stats.max_dop,
        query_stats.last_grant_kb,
        query_stats.min_grant_kb,
        query_stats.max_grant_kb,
        query_stats.last_used_grant_kb,
        query_stats.min_used_grant_kb,
        query_stats.max_used_grant_kb,
        query_stats.last_ideal_grant_kb,
        query_stats.min_ideal_grant_kb,
        query_stats.max_ideal_grant_kb,
        query_stats.last_reserved_threads,
        query_stats.min_reserved_threads,
        query_stats.max_reserved_threads,
        query_stats.last_used_threads,
        query_stats.min_used_threads,
        query_stats.max_used_threads,'
				        ELSE N' NULL AS last_dop,
        NULL AS min_dop,
        NULL AS max_dop,
        NULL AS last_grant_kb,
        NULL AS min_grant_kb,
        NULL AS max_grant_kb,
        NULL AS last_used_grant_kb,
        NULL AS min_used_grant_kb,
        NULL AS max_used_grant_kb,
        NULL AS last_ideal_grant_kb,
        NULL AS min_ideal_grant_kb,
        NULL AS max_ideal_grant_kb,
        NULL AS last_reserved_threads,
        NULL AS min_reserved_threads,
        NULL AS max_reserved_threads,
        NULL AS last_used_threads,
        NULL AS min_used_threads,
        NULL AS max_used_threads,'
		        END 

        SET @StringToExecute += 						
		        N'
        COALESCE(CAST(qmg.grant_time AS VARCHAR(20)), ''Memory Not Granted'') AS grant_time ,
        qmg.requested_memory_kb ,
        qmg.granted_memory_kb AS grant_memory_kb,
        CASE WHEN qmg.grant_time IS NULL THEN ''N/A''
        WHEN qmg.requested_memory_kb < qmg.granted_memory_kb
	        THEN ''Query Granted Less Than Query Requested''
	        ELSE ''Memory Request Granted''
        END AS is_request_granted ,
        qmg.required_memory_kb ,
        qmg.used_memory_kb AS query_memory_grant_used_memory_kb,
        qmg.ideal_memory_kb ,
        qmg.is_small ,
        qmg.timeout_sec ,
        qmg.resource_semaphore_id ,
        COALESCE(CAST(qmg.wait_order AS VARCHAR(20)), ''N/A'') AS wait_order ,
        COALESCE(CAST(qmg.wait_time_ms AS VARCHAR(20)),
	        ''N/A'') AS wait_time_ms ,
        CASE qmg.is_next_candidate
	        WHEN 0 THEN ''No''
	        WHEN 1 THEN ''Yes''
	        ELSE ''N/A''
        END AS next_candidate_for_memory_grant ,
        qrs.target_memory_kb ,
        COALESCE(CAST(qrs.max_target_memory_kb AS VARCHAR(20)),
	        ''Small Query Resource Semaphore'') AS max_target_memory_kb ,
        qrs.total_memory_kb ,
        qrs.available_memory_kb ,
        qrs.granted_memory_kb ,
        qrs.used_memory_kb AS query_resource_semaphore_used_memory_kb,
        qrs.grantee_count ,
        qrs.waiter_count ,
        qrs.timeout_error_count ,
        COALESCE(CAST(qrs.forced_grant_count AS VARCHAR(20)),
        ''Small Query Resource Semaphore'') AS forced_grant_count,
        wg.name AS workload_group_name, 
        rp.name AS resource_pool_name,
        CONVERT(VARCHAR(128), r.context_info)  AS context_info,
        r.query_hash, r.query_plan_hash, r.sql_handle, r.plan_handle, r.statement_start_offset, r.statement_end_offset '
    END /* IF @ExpertMode = 1 */
					
    SET @StringToExecute += 	
	    N' FROM sys.dm_exec_sessions AS s'+
			 CASE
				WHEN @GetOuterCommand = 1 THEN CASE
													WHEN EXISTS(SELECT 1 FROM sys.all_objects WHERE [name] = N'dm_exec_input_buffer') THEN N'
		OUTER APPLY sys.dm_exec_input_buffer (s.session_id, 0) AS ib'
													ELSE N'
		LEFT JOIN @inputbuffer ib ON s.session_id = ib.session_id'
											   END
				ELSE N''
			 END+N'
	    LEFT JOIN sys.dm_exec_requests AS r
					    ON   r.session_id = s.session_id
	    LEFT JOIN ( SELECT DISTINCT
						    wait.session_id ,
						    ( SELECT waitwait.wait_type + N'' (''
							    + CAST(MAX(waitwait.wait_duration_ms) AS NVARCHAR(128))
							    + N'' ms) ''
						    FROM   sys.dm_os_waiting_tasks AS waitwait
						    WHERE  waitwait.session_id = wait.session_id
						    GROUP BY  waitwait.wait_type
						    ORDER BY  SUM(waitwait.wait_duration_ms) DESC
						    FOR
						    XML PATH('''') ) AS wait_info
					    FROM sys.dm_os_waiting_tasks AS wait ) AS wt
					    ON   s.session_id = wt.session_id
	    LEFT JOIN sys.dm_exec_query_stats AS query_stats
	    ON   r.sql_handle = query_stats.sql_handle
			    AND r.plan_handle = query_stats.plan_handle
		    AND r.statement_start_offset = query_stats.statement_start_offset
		    AND r.statement_end_offset = query_stats.statement_end_offset
	    '
	    +
	    CASE @SessionWaits
			    WHEN 1 THEN @SessionWaitsSQL
			    ELSE N''
	    END
	    + 
	    N'
	    LEFT JOIN sys.dm_exec_query_memory_grants qmg
	    ON   r.session_id = qmg.session_id
			    AND r.request_id = qmg.request_id
	    LEFT JOIN sys.dm_exec_query_resource_semaphores qrs
	    ON   qmg.resource_semaphore_id = qrs.resource_semaphore_id
			    AND qmg.pool_id = qrs.pool_id
	    LEFT JOIN sys.resource_governor_workload_groups wg 
	    ON 		s.group_id = wg.group_id
	    LEFT JOIN sys.resource_governor_resource_pools rp 
	    ON		wg.pool_id = rp.pool_id
	    OUTER APPLY (
			    SELECT TOP 1
			    b.dbid, b.last_batch, b.open_tran, b.sql_handle, 
			    b.session_id, b.blocking_session_id, b.lastwaittype, b.waittime
			    FROM @blocked b
			    WHERE (s.session_id = b.session_id
					    OR s.session_id = b.blocking_session_id)
		    ) AS blocked
	    OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, blocked.sql_handle)) AS dest
	    OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS derp
	    OUTER APPLY (
			    SELECT CONVERT(DECIMAL(38,2), SUM( (((tsu.user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8) / 1024.)) ) AS tempdb_allocations_mb
			    FROM sys.dm_db_task_space_usage tsu
			    WHERE tsu.request_id = r.request_id
			    AND tsu.session_id = r.session_id
			    AND tsu.session_id = s.session_id
	    ) as tempdb_allocations

		OUTER APPLY (
			SELECT TOP 1 Query_Plan,
			STUFF((SELECT DISTINCT N'', '' + Node.Data.value(''(@Column)[1]'', ''NVARCHAR(4000)'') + N'' {'' + Node.Data.value(''(@ParameterDataType)[1]'', ''NVARCHAR(4000)'') + N''}: '' + Node.Data.value(''(@ParameterCompiledValue)[1]'', ''NVARCHAR(4000)'') + N'' (Actual: '' + Node.Data.value(''(@ParameterRuntimeValue)[1]'', ''NVARCHAR(4000)'') + N'')''
					FROM q.Query_Plan.nodes(''/*:ShowPlanXML/*:BatchSequence/*:Batch/*:Statements/*:StmtSimple/*:QueryPlan/*:ParameterList/*:ColumnReference'') AS Node(Data)
					FOR XML PATH('''')), 1,2,'''')
					AS Live_Parameter_Info
			FROM @LiveQueryPlans q
			WHERE (s.session_id = q.Session_Id)

		) AS qs_live

	    WHERE s.session_id <> @@SPID 
	    AND s.host_name IS NOT NULL
		AND r.database_id NOT IN (SELECT database_id FROM #WhoReadableDBs)
	    '
	    + CASE WHEN @ShowSleepingSPIDs = 0 THEN
			    N' AND COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid)) IS NOT NULL'
			    WHEN @ShowSleepingSPIDs = 1 THEN
			    N' OR COALESCE(r.open_transaction_count, blocked.open_tran) >= 1'
			    ELSE N'' END;


END /* IF @ProductVersionMajor >= 11  */

IF (@MinElapsedSeconds + @MinCPUTime + @MinLogicalReads + @MinPhysicalReads + @MinWrites + @MinTempdbMB + @MinRequestedMemoryKB + @MinBlockingSeconds) > 0
	BEGIN
	/* They're filtering for something, so set up a where clause that will let any (not all combined) of the min triggers work: */
	SET @StringToExecute += N' AND (1 = 0 ';
	IF @MinElapsedSeconds > 0
		SET @StringToExecute += N' OR ABS(COALESCE(r.total_elapsed_time,0)) / 1000 >= ' + CAST(@MinElapsedSeconds AS NVARCHAR(20));
	IF @MinCPUTime > 0
		SET @StringToExecute += N' OR COALESCE(r.cpu_time, s.cpu_time,0) / 1000 >= ' + CAST(@MinCPUTime AS NVARCHAR(20));
	IF @MinLogicalReads > 0
		SET @StringToExecute += N' OR COALESCE(r.logical_reads, s.logical_reads,0) >= ' + CAST(@MinLogicalReads AS NVARCHAR(20));
	IF @MinPhysicalReads > 0
		SET @StringToExecute += N' OR COALESCE(s.reads,0) >= ' + CAST(@MinPhysicalReads AS NVARCHAR(20));
	IF @MinWrites > 0
		SET @StringToExecute += N' OR COALESCE(r.writes, s.writes,0) >= ' + CAST(@MinWrites AS NVARCHAR(20));
	IF @MinTempdbMB > 0
		SET @StringToExecute += N' OR COALESCE(tempdb_allocations.tempdb_allocations_mb,0) >= ' + CAST(@MinTempdbMB AS NVARCHAR(20));
	IF @MinRequestedMemoryKB > 0
		SET @StringToExecute += N' OR COALESCE(qmg.requested_memory_kb,0) >= ' + CAST(@MinRequestedMemoryKB AS NVARCHAR(20));
	/* Blocking is a little different - we're going to return ALL of the queries if we meet the blocking threshold. */
	IF @MinBlockingSeconds > 0
		SET @StringToExecute += N' OR (SELECT SUM(waittime / 1000) FROM @blocked) >= ' + CAST(@MinBlockingSeconds AS NVARCHAR(20));
	SET @StringToExecute += N' ) ';
	END

SET @StringToExecute +=
		N' ORDER BY ' + CASE	WHEN @SortOrder = 'session_id'		THEN '[session_id] DESC'
								WHEN @SortOrder = 'query_cost'		THEN '[query_cost] DESC'
								WHEN @SortOrder = 'database_name'		THEN '[database_name] ASC'
								WHEN @SortOrder = 'open_transaction_count'		THEN '[open_transaction_count] DESC'
								WHEN @SortOrder = 'is_implicit_transaction'		THEN '[is_implicit_transaction] DESC'
								WHEN @SortOrder = 'login_name'		THEN '[login_name] ASC'
								WHEN @SortOrder = 'program_name'		THEN '[program_name] ASC'
								WHEN @SortOrder = 'client_interface_name'		THEN '[client_interface_name] ASC'
								WHEN @SortOrder = 'request_cpu_time'		THEN 'COALESCE(r.cpu_time, s.cpu_time) DESC'
								WHEN @SortOrder = 'request_logical_reads'		THEN 'COALESCE(r.logical_reads, s.logical_reads) DESC'
								WHEN @SortOrder = 'request_writes'		THEN 'COALESCE(r.writes, s.writes) DESC'
								WHEN @SortOrder = 'request_physical_reads'		THEN 'COALESCE(r.reads, s.reads) DESC '
								WHEN @SortOrder = 'session_cpu'		THEN 's.cpu_time DESC'
								WHEN @SortOrder = 'session_logical_reads'		THEN 's.logical_reads DESC'
								WHEN @SortOrder = 'session_physical_reads'		THEN 's.reads DESC'
								WHEN @SortOrder = 'session_writes'		THEN 's.writes DESC'
								WHEN @SortOrder = 'tempdb_allocations_mb'		THEN '[tempdb_allocations_mb] DESC'
								WHEN @SortOrder = 'memory_usage'		THEN '[memory_usage] DESC'
								WHEN @SortOrder = 'deadlock_priority'		THEN 'r.deadlock_priority DESC'
								WHEN @SortOrder = 'transaction_isolation_level'		THEN 'r.[transaction_isolation_level] DESC'
								WHEN @SortOrder = 'requested_memory_kb' THEN '[requested_memory_kb] DESC'
								WHEN @SortOrder = 'grant_memory_kb' THEN 'qmg.granted_memory_kb DESC'
								WHEN @SortOrder = 'grant' THEN 'qmg.granted_memory_kb DESC'
								WHEN @SortOrder = 'query_memory_grant_used_memory_kb' THEN 'qmg.used_memory_kb DESC'
								WHEN @SortOrder = 'ideal_memory_kb' THEN '[ideal_memory_kb] DESC'
								WHEN @SortOrder = 'workload_group_name' THEN 'wg.name ASC'
								WHEN @SortOrder = 'resource_pool_name' THEN 'rp.name ASC'
								ELSE '[elapsed_time] DESC'
						END + '
		';


IF @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL
  AND EXISTS ( SELECT *
      FROM   sys.databases
      WHERE  QUOTENAME([name]) = @OutputDatabaseName)
	BEGIN
	SET @StringToExecute = N'USE '
	  + @OutputDatabaseName + N'; '
	  + @BlockingCheck + 
	  + ' INSERT INTO '
	  + @OutputSchemaName + N'.'
	  + @OutputTableName
	  + N'(ServerName
	,CheckDate
	,[elapsed_time]
	,[session_id]
	,[database_name]
	,[query_text]'
	+ CASE WHEN @GetOuterCommand = 1 THEN N',[outer_command]' ELSE N'' END + N'
	,[query_plan]'
    + CASE WHEN @ProductVersionMajor >= 11 THEN N',[live_query_plan]' ELSE N'' END 
	+ CASE WHEN @ProductVersionMajor >= 11 THEN N',[cached_parameter_info]'  ELSE N'' END
	+ CASE WHEN @ProductVersionMajor >= 11 AND @ShowActualParameters = 1 THEN N',[Live_Parameter_Info]' ELSE N'' END + N'
	,[query_cost]
	,[status]
	,[wait_info]
	,[wait_resource]'
    + CASE WHEN @ProductVersionMajor >= 11 THEN N',[top_session_waits]' ELSE N'' END + N'
	,[blocking_session_id]
	,[open_transaction_count]
	,[is_implicit_transaction]
	,[nt_domain]
	,[host_name]
	,[login_name]
	,[nt_user_name]
	,[program_name]
	,[fix_parameter_sniffing]
	,[client_interface_name]
	,[login_time]
	,[start_time]
	,[request_time]
	,[request_cpu_time]
	,[request_logical_reads]
	,[request_writes]
	,[request_physical_reads]
	,[session_cpu]
	,[session_logical_reads]
	,[session_physical_reads]
	,[session_writes]
	,[tempdb_allocations_mb]
	,[memory_usage]
	,[estimated_completion_time]
	,[percent_complete]
	,[deadlock_priority]
	,[transaction_isolation_level]
	,[degree_of_parallelism]'
    + CASE WHEN @ProductVersionMajor >= 11 THEN N'
	,[last_dop]
	,[min_dop]
	,[max_dop]
	,[last_grant_kb]
	,[min_grant_kb]
	,[max_grant_kb]
	,[last_used_grant_kb]
	,[min_used_grant_kb]
	,[max_used_grant_kb]
	,[last_ideal_grant_kb]
	,[min_ideal_grant_kb]
	,[max_ideal_grant_kb]
	,[last_reserved_threads]
	,[min_reserved_threads]
	,[max_reserved_threads]
	,[last_used_threads]
	,[min_used_threads]
	,[max_used_threads]' ELSE N'' END + N'
	,[grant_time]
	,[requested_memory_kb]
	,[grant_memory_kb]
	,[is_request_granted]
	,[required_memory_kb]
	,[query_memory_grant_used_memory_kb]
	,[ideal_memory_kb]
	,[is_small]
	,[timeout_sec]
	,[resource_semaphore_id]
	,[wait_order]
	,[wait_time_ms]
	,[next_candidate_for_memory_grant]
	,[target_memory_kb]
	,[max_target_memory_kb]
	,[total_memory_kb]
	,[available_memory_kb]
	,[granted_memory_kb]
	,[query_resource_semaphore_used_memory_kb]
	,[grantee_count]
	,[waiter_count]
	,[timeout_error_count]
	,[forced_grant_count]
	,[workload_group_name]
	,[resource_pool_name]
	,[context_info]'
    + CASE WHEN @ProductVersionMajor >= 11 THEN N'
	,[query_hash]
	,[query_plan_hash]
	,[sql_handle]
	,[plan_handle]
	,[statement_start_offset]
	,[statement_end_offset]' ELSE N'' END + N'
) 
	SELECT @@SERVERNAME, COALESCE(@CheckDateOverride, SYSDATETIMEOFFSET()) AS CheckDate , '
	+ @StringToExecute;
	END
ELSE
	SET @StringToExecute = @BlockingCheck + N' SELECT  GETDATE() AS run_date , ' + @StringToExecute;

/* If the server has > 50GB of memory, add a max grant hint to avoid getting a giant grant */
IF (@ProductVersionMajor = 11 AND @ProductVersionMinor >= 6020)
	OR (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 5000 )
	OR (@ProductVersionMajor >= 13 )
	AND 50000000 < (SELECT cntr_value 			
						FROM sys.dm_os_performance_counters 
						WHERE object_name LIKE '%:Memory Manager%'
						AND counter_name LIKE 'Target Server Memory (KB)%')
	BEGIN
		SET @StringToExecute = @StringToExecute + N' OPTION (MAX_GRANT_PERCENT = 1, RECOMPILE) ';
	END
ELSE
	BEGIN
		SET @StringToExecute = @StringToExecute + N' OPTION (RECOMPILE) ';
	END

/* Be good: */
SET @StringToExecute = @StringToExecute + N' ; ';


IF @Debug = 1
	BEGIN
		PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 0, 8000))
		PRINT CONVERT(VARCHAR(8000), SUBSTRING(@StringToExecute, 8000, 16000))
	END

EXEC sp_executesql @StringToExecute,
	N'@CheckDateOverride DATETIMEOFFSET',
	@CheckDateOverride;

END
GO 
