IF OBJECT_ID('dbo.sp_kill') IS NULL
	EXEC ('CREATE PROCEDURE dbo.sp_kill AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_kill
	@ExecuteKills VARCHAR(1) = 'N',
	@SPID INT = NULL,
	@LoginName NVARCHAR(256) = NULL,
	@AppName NVARCHAR(256) = NULL,
	@DatabaseName NVARCHAR(256) = NULL,
	@HostName NVARCHAR(256) = NULL,
	@LeadBlockers VARCHAR(1) = NULL,
	@ReadOnly VARCHAR(1) = NULL,
	@OrderBy VARCHAR(20) = 'duration',
	@SPIDState VARCHAR(1) = NULL,
	@OmitLogin NVARCHAR(256) = NULL,
	@HasOpenTran VARCHAR(1) = NULL,
	@RequestsOlderThanSeconds INT = NULL,
	@OutputDatabaseName NVARCHAR(256) = NULL,
	@OutputSchemaName NVARCHAR(256) = NULL,
	@OutputTableName NVARCHAR(256) = NULL,
	@Help BIT = 0,
	@Debug BIT = 0,
	@Version VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
	@VersionCheckMode BIT = 0,
	@EmergencyMode TINYINT = NULL
WITH RECOMPILE
AS
BEGIN
	SET NOCOUNT ON;
	SET STATISTICS XML OFF;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	/*
	sp_kill from http://FirstResponderKit.org

	This script helps you kill queries during a performance emergency.

	It collects all running session data, recommends which queries to kill
	(with reasons), optionally executes the kills, frees plan cache entries
	for killed queries, and logs everything to an output table.

	This is NOT targeted at DBAs who want to pull on the latex gloves to do
	careful analysis first. This IS targeted at people who would otherwise
	just restart the whole server.

	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	MIT License

	Copyright (c) Brent Ozar Unlimited

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
	*/

	SELECT @Version = '8.32', @VersionDate = '20260407';

	IF(@VersionCheckMode = 1)
	BEGIN
		RETURN;
	END;

	IF @Help = 1
	BEGIN
		PRINT '
sp_kill from http://FirstResponderKit.org

This script helps you kill queries during a performance emergency.

It collects all running session data, recommends which queries to kill
(with reasons), optionally executes the kills, frees plan cache entries
for killed queries, and logs everything.

Parameters:
  @ExecuteKills VARCHAR(1) = ''N''
    Y = actually kill the recommended sessions
    N = just show recommendations and log (default)

  @SPID INT = NULL
    Target a specific session ID to kill.

  @LoginName NVARCHAR(256) = NULL
    Kill sessions belonging to this login.

  @AppName NVARCHAR(256) = NULL
    Kill sessions from this application (supports LIKE wildcards).

  @DatabaseName NVARCHAR(256) = NULL
    Kill sessions using this database.

  @HostName NVARCHAR(256) = NULL
    Kill sessions from this host.

  @LeadBlockers VARCHAR(1) = NULL
    Y = kill only lead blockers (sessions blocking others but not blocked themselves).

  @ReadOnly VARCHAR(1) = NULL
    Y = only kill read-only queries (SELECT with no writes).

  @OrderBy VARCHAR(20) = ''duration''
    Sort order for kill recommendations: duration, cpu, reads, writes, tempdb, transactions.

  @SPIDState VARCHAR(1) = NULL
    S = only sleeping sessions, R = only running sessions.

  @OmitLogin NVARCHAR(256) = NULL
    Exclude sessions belonging to this login from kill recommendations.

  @HasOpenTran VARCHAR(1) = NULL
    Y = only target sessions with open transactions.

  @RequestsOlderThanSeconds INT = NULL
    Only target sessions whose last request started at least this many seconds ago.

  @OutputDatabaseName, @OutputSchemaName, @OutputTableName
    Optional 3-part name for persistent logging table. Will be ignored when @EmergencyMode > 0

  @Help BIT = 0
    Show this help text.

  @Debug BIT = 0
    Show diagnostic messages during execution.

  @EmergencyMode TINYINT = NULL
    1 = Skip nearly all optional parts of the processing to minimize work performed by the procedure, but show the sql_text column
	2 = Skip all optional parts of processing including the sql_text column

Example usage:
  -- Just show recommendations, no killing:
  EXEC sp_kill;

  -- Show recommendations as quickly as possible by limiting data returned, designed to run on extremely busy servers
  EXEC sp_kill @EmergencyMode = 1;	

  -- Kill everything (except system sessions, rollbacks, and your own session):
  EXEC sp_kill @ExecuteKills = ''Y'';

  -- Kill all lead blockers:
  EXEC sp_kill @LeadBlockers = ''Y'', @ExecuteKills = ''Y'';

  -- Kill a specific session:
  EXEC sp_kill @SPID = 55, @ExecuteKills = ''Y'';

  -- Kill sleeping sessions with open transactions older than 10 seconds:
  EXEC sp_kill @HasOpenTran = ''Y'', @SPIDState = ''S'',
    @RequestsOlderThanSeconds = 10, @ExecuteKills = ''Y'';

  -- Just show what we would kill for a specific login, sorted by CPU:
  EXEC sp_kill @LoginName = ''DOMAIN\TroublesomeUser'', @OrderBy = ''cpu'';

MIT License

Copyright (c) Brent Ozar Unlimited

For more info, visit http://FirstResponderKit.org
';
		RETURN;
	END; /* @Help = 1 */

	/*-------------------------------------------------------
	  Section 2: Variable Declarations & Platform Detection
	-------------------------------------------------------*/
	DECLARE @AzureSQLDB BIT = (SELECT CASE WHEN SERVERPROPERTY('EngineEdition') = 5 THEN 1 ELSE 0 END)
		,@HasLiveQueryPlan BIT = 0
		,@StringToExecute NVARCHAR(MAX)
		,@ObjectFullName NVARCHAR(2000)
		,@OurSessionID INT = @@SPID
		,@CheckDate DATETIMEOFFSET = SYSDATETIMEOFFSET()
		,@CallerLoginName NVARCHAR(128) = ORIGINAL_LOGIN()
		,@CallerHostName NVARCHAR(128) = HOST_NAME()
		,@ParametersUsed NVARCHAR(4000)
		,@KillCount INT = 0
		,@TotalKills INT = 0
		,@CurrentSPID INT
		,@CurrentPlanHandle VARBINARY(64)
		,@CurrentSqlHandle VARBINARY(64)
		,@CurrentStartTime DATETIME
		,@CurrentLastRequestStartTime DATETIME
		,@KillSQL NVARCHAR(100)
		,@FreeCacheSQL NVARCHAR(200)
		,@KillStartTime DATETIME
		,@KillEndTime DATETIME
		,@KillError NVARCHAR(4000)
		,@msg NVARCHAR(400)
		,@LineFeed NVARCHAR(10) = CHAR(13) + CHAR(10)
		,@BlockedSessionCount INT;

	/* Check for live query plan support - if the DMV exists, it's supported */
	IF EXISTS (SELECT 1 FROM sys.all_objects WHERE [name] = N'dm_exec_query_statistics_xml')
		SET @HasLiveQueryPlan = 1;

	/* Build @ParametersUsed string (escape single quotes in all string params) */
	SET @ParametersUsed = N'EXEC sp_kill';
	IF @ExecuteKills <> 'N' SET @ParametersUsed = @ParametersUsed + N' @ExecuteKills = ''' + REPLACE(@ExecuteKills, N'''', N'''''') + N'''';
	IF @SPID IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @SPID = ' + CAST(@SPID AS NVARCHAR(10));
	IF @LoginName IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @LoginName = N''' + REPLACE(@LoginName, N'''', N'''''') + N'''';
	IF @AppName IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @AppName = N''' + REPLACE(@AppName, N'''', N'''''') + N'''';
	IF @DatabaseName IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @DatabaseName = N''' + REPLACE(@DatabaseName, N'''', N'''''') + N'''';
	IF @HostName IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @HostName = N''' + REPLACE(@HostName, N'''', N'''''') + N'''';
	IF @LeadBlockers IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @LeadBlockers = ''' + REPLACE(@LeadBlockers, N'''', N'''''') + N'''';
	IF @ReadOnly IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @ReadOnly = ''' + REPLACE(@ReadOnly, N'''', N'''''') + N'''';
	IF @OrderBy <> 'duration' SET @ParametersUsed = @ParametersUsed + N', @OrderBy = N''' + REPLACE(@OrderBy, N'''', N'''''') + N'''';
	IF @SPIDState IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @SPIDState = ''' + REPLACE(@SPIDState, N'''', N'''''') + N'''';
	IF @OmitLogin IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @OmitLogin = N''' + REPLACE(@OmitLogin, N'''', N'''''') + N'''';
	IF @HasOpenTran IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @HasOpenTran = ''' + REPLACE(@HasOpenTran, N'''', N'''''') + N'''';
	IF @RequestsOlderThanSeconds IS NOT NULL SET @ParametersUsed = @ParametersUsed + N', @RequestsOlderThanSeconds = ' + CAST(@RequestsOlderThanSeconds AS NVARCHAR(10));

	/*-------------------------------------------------------
	  Section 3: Parameter Validation
	-------------------------------------------------------*/
	IF @ExecuteKills NOT IN ('Y', 'N')
	BEGIN
		RAISERROR('@ExecuteKills must be Y or N.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @LeadBlockers IS NOT NULL AND @LeadBlockers <> 'Y'
	BEGIN
		RAISERROR('@LeadBlockers must be Y or NULL.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @ReadOnly IS NOT NULL AND @ReadOnly <> 'Y'
	BEGIN
		RAISERROR('@ReadOnly must be Y or NULL.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @SPIDState IS NOT NULL AND @SPIDState NOT IN ('S', 'R')
	BEGIN
		RAISERROR('@SPIDState must be S (sleeping), R (running), or NULL (both).', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @HasOpenTran IS NOT NULL AND @HasOpenTran <> 'Y'
	BEGIN
		RAISERROR('@HasOpenTran must be Y or NULL.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @RequestsOlderThanSeconds IS NOT NULL AND @RequestsOlderThanSeconds < 0
	BEGIN
		RAISERROR('@RequestsOlderThanSeconds must be >= 0.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	SET @OrderBy = LOWER(@OrderBy);
	IF @OrderBy NOT IN ('duration', 'cpu', 'reads', 'writes', 'tempdb', 'transactions')
	BEGIN
		RAISERROR('@OrderBy must be one of: duration, cpu, reads, writes, tempdb, transactions.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @EmergencyMode IS NOT NULL AND @EmergencyMode NOT IN (1, 2)
	BEGIN
		RAISERROR('@EmergencyMode must be NULL, 1, or 2.', 11, 1) WITH NOWAIT;
		RETURN;
	END;

	/*-------------------------------------------------------
	  Section 4: Output Table Name Sanitization
	-------------------------------------------------------*/
	SELECT
		@OutputDatabaseName = QUOTENAME(PARSENAME(@OutputDatabaseName, 1)),
		@OutputSchemaName = ISNULL(QUOTENAME(PARSENAME(@OutputSchemaName, 1)), QUOTENAME(PARSENAME(@OutputTableName, 2))),
		@OutputTableName = QUOTENAME(PARSENAME(@OutputTableName, 1));

	IF @OutputTableName IS NOT NULL AND (@OutputDatabaseName IS NULL OR @OutputSchemaName IS NULL)
	BEGIN
		IF @OutputDatabaseName IS NULL AND @AzureSQLDB = 1
		BEGIN
			SET @OutputDatabaseName = QUOTENAME(DB_NAME());
		END;
		IF @OutputSchemaName IS NULL AND @OutputDatabaseName = QUOTENAME(DB_NAME())
		BEGIN
			SET @OutputSchemaName = QUOTENAME(SCHEMA_NAME());
		END;
	END;

	/*-------------------------------------------------------
	  Section 5: Temp Table Creation
	-------------------------------------------------------*/
	CREATE TABLE #hitlist (
		Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
		ServerName NVARCHAR(128) NULL,
		CheckDate DATETIMEOFFSET NULL,
		CallerLoginName NVARCHAR(128) NULL,
		CallerHostName NVARCHAR(128) NULL,
		ParametersUsed NVARCHAR(4000) NULL,
		OrderByNum INT NULL,
		KillCommand NVARCHAR(100) NULL,
		KillRecommended BIT DEFAULT 0,
		KillStartedTime DATETIME NULL,
		KillEndedTime DATETIME NULL,
		KillError NVARCHAR(4000) NULL,
		Reason NVARCHAR(4000) NULL,
		session_id INT NOT NULL,
		[status] NVARCHAR(30) NULL,
		blocking_session_id INT NULL,
		wait_type NVARCHAR(60) NULL,
		wait_time_ms BIGINT NULL,
		wait_resource NVARCHAR(256) NULL,
		open_transaction_count INT NULL,
		is_implicit_transaction BIT NULL,
		cpu_time_ms BIGINT NULL,
		reads BIGINT NULL,
		writes BIGINT NULL,
		logical_reads BIGINT NULL,
		memory_grant_mb DECIMAL(38,2) NULL,
		tempdb_allocations_mb DECIMAL(38,2) NULL,
		login_name NVARCHAR(128) NULL,
		host_name NVARCHAR(128) NULL,
		program_name NVARCHAR(256) NULL,
		database_name NVARCHAR(128) NULL,
		start_time DATETIME NULL,
		last_request_start_time DATETIME NULL,
		last_request_end_time DATETIME NULL,
		command NVARCHAR(32) NULL,
		sql_text NVARCHAR(MAX) NULL,
		sql_handle VARBINARY(64) NULL,
		plan_handle VARBINARY(64) NULL,
		query_plan XML NULL,
		live_query_plan XML NULL,
		transaction_isolation_level NVARCHAR(50) NULL,
		is_read_only BIT NULL,
		FreeProcCacheCommand NVARCHAR(200) NULL
	);

	/*-------------------------------------------------------
	  Section 6: Main DMV Collection Query
	-------------------------------------------------------*/
	IF @Debug = 1
		RAISERROR('Collecting session data from DMVs...', 0, 1) WITH NOWAIT;

	-- NOTE: when adding a new column to #hitlist please consider if the @EmergencyMode input parameter should prevent its population
	SET @StringToExecute = CAST(N'' AS NVARCHAR(MAX)) + N'
	INSERT INTO #hitlist (
		ServerName, CheckDate, CallerLoginName, CallerHostName, ParametersUsed,
		KillCommand, session_id, [status], blocking_session_id,
		wait_type, wait_time_ms, wait_resource,
		open_transaction_count, is_implicit_transaction,
		cpu_time_ms, reads, writes, logical_reads,		
		login_name, host_name, program_name, database_name,
		start_time, last_request_start_time, last_request_end_time,
		command, sql_handle, plan_handle,		
		transaction_isolation_level, is_read_only, FreeProcCacheCommand'
		-- when @EmergencyMode = 2 skip all expensive columns, both in terms of this query and time needed to render the result in SSMS
		-- when @EmergencyMode = 1 skip some expensive columns but populate the sql_text column
		-- when @EmergencyMode IS NULL populate all columns
		+ CASE WHEN @EmergencyMode = 1 THEN N',
		sql_text'
		WHEN @EmergencyMode IS NULL THEN N',
		sql_text, memory_grant_mb, tempdb_allocations_mb, query_plan, live_query_plan'
		ELSE N'' END + N'
	)
	SELECT
		@@SERVERNAME AS ServerName,
		@CheckDate AS CheckDate,
		@CallerLoginName AS CallerLoginName,
		@CallerHostName AS CallerHostName,
		@ParametersUsed AS ParametersUsed,
		N''KILL '' + CAST(s.session_id AS NVARCHAR(10)) + N'';'' AS KillCommand,
		s.session_id,
		COALESCE(r.[status], s.[status]) AS [status],
		r.blocking_session_id,
		r.wait_type,
		r.wait_time AS wait_time_ms,
		r.wait_resource,
		COALESCE(r.open_transaction_count, s.open_transaction_count, 0) AS open_transaction_count,
		CASE WHEN EXISTS (
			SELECT 1
			FROM sys.dm_tran_active_transactions AS tat
			JOIN sys.dm_tran_session_transactions AS tst ON tst.transaction_id = tat.transaction_id
			WHERE tat.name = ''implicit_transaction''
			AND s.session_id = tst.session_id
		) THEN 1 ELSE 0 END AS is_implicit_transaction,
		COALESCE(r.cpu_time, s.cpu_time) AS cpu_time_ms,
		COALESCE(r.reads, s.reads) AS reads,
		COALESCE(r.writes, s.writes) AS writes,
		COALESCE(r.logical_reads, s.logical_reads) AS logical_reads,		
		s.login_name,
		s.host_name,
		s.program_name,
		COALESCE(DB_NAME(r.database_id), DB_NAME(s.database_id)) AS database_name,
		r.start_time,
		s.last_request_start_time,
		s.last_request_end_time,
		r.command,
		COALESCE(r.sql_handle, c.most_recent_sql_handle) AS sql_handle,
		r.plan_handle,';		

	SET @StringToExecute = @StringToExecute
		+ N'
		CASE
			WHEN s.transaction_isolation_level = 0 THEN ''Unspecified''
			WHEN s.transaction_isolation_level = 1 THEN ''Read Uncommitted''
			WHEN s.transaction_isolation_level = 2 THEN ''Read Committed''
			WHEN s.transaction_isolation_level = 3 THEN ''Repeatable Read''
			WHEN s.transaction_isolation_level = 4 THEN ''Serializable''
			WHEN s.transaction_isolation_level = 5 THEN ''Snapshot''
			ELSE ''Unknown''
		END AS transaction_isolation_level,
		CASE
			WHEN COALESCE(r.writes, s.writes, 0) = 0
				AND (r.command IS NULL OR r.command IN (''SELECT'', ''FETCH'', ''RECEIVE''))
				AND COALESCE(r.open_transaction_count, s.open_transaction_count, 0) <= 1
			THEN 1 ELSE 0
		END AS is_read_only,
		CASE WHEN r.plan_handle IS NOT NULL
			THEN N''DBCC FREEPROCCACHE ('' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + N'');''
			ELSE NULL
		END AS FreeProcCacheCommand'
		+ CASE WHEN @EmergencyMode IS NULL OR @EmergencyMode = 1 THEN N',
		ISNULL(SUBSTRING(dest.text,
			(r.statement_start_offset / 2) + 1,
			((CASE r.statement_end_offset
				WHEN -1 THEN DATALENGTH(dest.text)
				ELSE r.statement_end_offset
			END - r.statement_start_offset) / 2) + 1), dest.text) AS sql_text'
		ELSE N'' END + 
		CASE WHEN @EmergencyMode IS NULL THEN N',
		CAST(qmg.granted_memory_kb / 1024.0 AS DECIMAL(38,2)) AS memory_grant_mb,
		tempdb_allocations.tempdb_allocations_mb,
		derp.query_plan,'
		+ CASE WHEN @HasLiveQueryPlan = 1 THEN N'
		lqp.query_plan AS live_query_plan' ELSE N'
		NULL AS live_query_plan' END
		ELSE N'' END + N'
	FROM sys.dm_exec_sessions AS s
	LEFT JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
	LEFT JOIN sys.dm_exec_connections AS c ON s.session_id = c.session_id'
	+ CASE WHEN @EmergencyMode IS NULL THEN N'
	LEFT JOIN sys.dm_exec_query_memory_grants AS qmg ON r.session_id = qmg.session_id AND r.request_id = qmg.request_id
	OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS derp
	OUTER APPLY (
		SELECT CONVERT(DECIMAL(38,2), SUM(((((tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count)
			+ (tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count)) * 8) / 1024.))) AS tempdb_allocations_mb
		FROM sys.dm_db_task_space_usage tsu
		WHERE tsu.request_id = r.request_id
		AND tsu.session_id = r.session_id
		AND tsu.session_id = s.session_id
	) AS tempdb_allocations' ELSE N'' END
	+ CASE WHEN @EmergencyMode IS NULL OR @EmergencyMode = 1 THEN N'
	OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, c.most_recent_sql_handle)) AS dest' ELSE N'' END
	+ CASE WHEN @HasLiveQueryPlan = 1 AND @EmergencyMode IS NULL THEN N'
	OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) AS lqp' ELSE N'' END
	+ N'
	WHERE s.session_id <> @OurSessionID
		AND s.session_id > 50
		AND s.is_user_process = 1
	OPTION (MAXDOP 1);';

	IF @Debug = 1
		RAISERROR('Executing DMV collection query...', 0, 1) WITH NOWAIT;

	EXEC sp_executesql @StringToExecute,
		N'@CheckDate DATETIMEOFFSET, @CallerLoginName NVARCHAR(128), @CallerHostName NVARCHAR(128), @ParametersUsed NVARCHAR(4000), @OurSessionID INT',
		@CheckDate = @CheckDate,
		@CallerLoginName = @CallerLoginName,
		@CallerHostName = @CallerHostName,
		@ParametersUsed = @ParametersUsed,
		@OurSessionID = @OurSessionID;

	IF @Debug = 1
	BEGIN
		SET @msg = N'Collected ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + N' sessions.';
		RAISERROR(@msg, 0, 1) WITH NOWAIT;
	END;

	/*-------------------------------------------------------
	  Section 7: Kill Recommendation Logic
	-------------------------------------------------------*/
	IF @Debug = 1
		RAISERROR('Applying kill recommendation logic...', 0, 1) WITH NOWAIT;

	/* 7a: Mark rollbacks as not killable */
	UPDATE #hitlist
	SET KillRecommended = 0,
		Reason = N'Session is rolling back - not safe to kill.'
	WHERE [status] = 'rollback';

	/* 7b: If @SPID is specified, only recommend that one */
	IF @SPID IS NOT NULL
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM #hitlist WHERE session_id = @SPID)
		BEGIN
			RAISERROR('The specified @SPID %d was not found among active user sessions.', 11, 1, @SPID) WITH NOWAIT;
			/* Still return results so user can see what IS running */
		END
		ELSE IF EXISTS (SELECT 1 FROM #hitlist WHERE session_id = @SPID AND [status] = 'rollback')
		BEGIN
			RAISERROR('The specified @SPID %d is currently rolling back and cannot be killed.', 11, 1, @SPID) WITH NOWAIT;
		END
		ELSE
		BEGIN
			UPDATE #hitlist
			SET KillRecommended = 1,
				Reason = N'Targeted by @SPID parameter.'
			WHERE session_id = @SPID
			AND [status] <> 'rollback';
		END;
	END
	/* 7c: Otherwise, apply all filters as AND conditions in a single pass.
	   A session must match ALL specified filters to be recommended for kill.
	   With no filters, @ExecuteKills = 'Y' kills everything;
	   @ExecuteKills = 'N' just shows sessions without recommending. */
	ELSE
	BEGIN
		/* When in display mode with no filters, just show sessions without recommending kills */
		IF @ExecuteKills = 'N'
			AND @LoginName IS NULL
			AND @AppName IS NULL
			AND @DatabaseName IS NULL
			AND @HostName IS NULL
			AND @LeadBlockers IS NULL
			AND @ReadOnly IS NULL
			AND @SPIDState IS NULL
			AND @HasOpenTran IS NULL
			AND @RequestsOlderThanSeconds IS NULL
			AND @OmitLogin IS NULL
		BEGIN
			/* No filters in display mode - nothing to recommend */
			IF @Debug = 1
				RAISERROR('No filters specified in display mode - showing all sessions without recommendations.', 0, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			/* Recommend sessions that match ALL specified filters */
			UPDATE h
			SET KillRecommended = 1
			FROM #hitlist h
			WHERE [status] <> 'rollback'
			AND (@LoginName IS NULL OR login_name = @LoginName)
			AND (@AppName IS NULL OR program_name LIKE @AppName)
			AND (@DatabaseName IS NULL OR database_name = @DatabaseName)
			AND (@HostName IS NULL OR host_name = @HostName)
			AND (@ReadOnly IS NULL OR is_read_only = 1)
			AND (@SPIDState IS NULL OR (@SPIDState = 'S' AND [status] = 'sleeping') OR (@SPIDState = 'R' AND [status] = 'running'))
			AND (@HasOpenTran IS NULL OR ISNULL(open_transaction_count, 0) > 0)
			AND (@RequestsOlderThanSeconds IS NULL OR last_request_start_time <= DATEADD(SECOND, -@RequestsOlderThanSeconds, GETDATE()))
			AND (@OmitLogin IS NULL OR login_name <> @OmitLogin)
			AND (@LeadBlockers IS NULL OR (
				EXISTS (SELECT 1 FROM #hitlist blocked WHERE blocked.blocking_session_id = h.session_id)
				AND (h.blocking_session_id IS NULL OR h.blocking_session_id = 0)
			));
		END;
	END; /* End of filter logic */

	/* 7d: Build Reason strings for recommended kills */
	/* Lead blockers */
	UPDATE t
	SET Reason = N'Lead blocker: blocking '
		+ CAST(blocked_stats.cnt AS NVARCHAR(10))
		+ N' session(s) for '
		+ CAST(ISNULL(blocked_stats.max_wait_time_ms / 1000, 0) AS NVARCHAR(20))
		+ N' seconds.'
	FROM #hitlist t
	CROSS APPLY (
		SELECT
			COUNT(*) AS cnt,
			MAX(ISNULL(blocked.wait_time_ms, 0)) AS max_wait_time_ms
		FROM #hitlist blocked
		WHERE blocked.blocking_session_id = t.session_id
	) blocked_stats
	WHERE t.KillRecommended = 1
	AND blocked_stats.cnt > 0
	AND (t.blocking_session_id IS NULL OR t.blocking_session_id = 0)
	AND (t.Reason IS NULL OR t.Reason = N'Targeted by @SPID parameter.');

	/* Sleeping with open transactions */
	UPDATE #hitlist
	SET Reason = N'Sleeping session with '
		+ CAST(open_transaction_count AS NVARCHAR(10))
		+ N' open transaction(s), idle for '
		+ CAST(ISNULL(DATEDIFF(SECOND, last_request_end_time, GETDATE()), 0) AS NVARCHAR(10))
		+ N' seconds.'
	WHERE KillRecommended = 1
	AND [status] = 'sleeping'
	AND open_transaction_count > 0
	AND Reason IS NULL;

	/* Long-running read-only */
	UPDATE #hitlist
	SET Reason = N'Long-running read-only query: running for '
		+ CAST(ISNULL(DATEDIFF(SECOND, COALESCE(start_time, last_request_start_time), GETDATE()), 0) AS NVARCHAR(10))
		+ N' seconds'
		+ CASE WHEN memory_grant_mb IS NOT NULL AND memory_grant_mb > 0
			THEN N', using ' + CAST(memory_grant_mb AS NVARCHAR(20)) + N' MB memory grant'
			ELSE N'' END
		+ N'.'
	WHERE KillRecommended = 1
	AND is_read_only = 1
	AND [status] <> 'sleeping'
	AND Reason IS NULL;

	/* Long-running write queries */
	UPDATE #hitlist
	SET Reason = N'Long-running query: running for '
		+ CAST(ISNULL(DATEDIFF(SECOND, COALESCE(start_time, last_request_start_time), GETDATE()), 0) AS NVARCHAR(10))
		+ N' seconds, writes: ' + CAST(ISNULL(writes, 0) AS NVARCHAR(20))
		+ CASE WHEN memory_grant_mb IS NOT NULL AND memory_grant_mb > 0
			THEN N', memory: ' + CAST(memory_grant_mb AS NVARCHAR(20)) + N' MB'
			ELSE N'' END
		+ N'.'
	WHERE KillRecommended = 1
	AND is_read_only = 0
	AND [status] <> 'sleeping'
	AND Reason IS NULL;

	/* Catch-all for anything that matched filters but wasn't categorized above */
	UPDATE #hitlist
	SET Reason = N'Matched filter criteria.'
	WHERE KillRecommended = 1
	AND Reason IS NULL;

	/* Clear KillCommand for sessions we're NOT recommending to kill */
	UPDATE #hitlist
	SET KillCommand = NULL,
		FreeProcCacheCommand = NULL
	WHERE KillRecommended = 0;

	/*-------------------------------------------------------
	  Section 8: OrderByNum Assignment
	-------------------------------------------------------*/
	;WITH cte AS (
		SELECT Id, OrderByNum,
			ROW_NUMBER() OVER (ORDER BY
				CASE WHEN @OrderBy = 'duration' THEN CAST(ISNULL(DATEDIFF(SECOND, COALESCE(start_time, last_request_start_time), GETDATE()), 0) AS BIGINT) END DESC,
				CASE WHEN @OrderBy = 'cpu' THEN ISNULL(cpu_time_ms, 0) END DESC,
				CASE WHEN @OrderBy = 'reads' THEN ISNULL(reads, 0) + ISNULL(logical_reads, 0) END DESC,
				CASE WHEN @OrderBy = 'writes' THEN ISNULL(writes, 0) END DESC,
				CASE WHEN @OrderBy = 'tempdb' THEN CAST(ISNULL(tempdb_allocations_mb, 0) AS BIGINT) END DESC,
				CASE WHEN @OrderBy = 'transactions' THEN ISNULL(open_transaction_count, 0) END DESC
			) AS rn
		FROM #hitlist
		WHERE KillRecommended = 1
	)
	UPDATE cte SET OrderByNum = rn;

	/*-------------------------------------------------------
	  Section 9: Rollback Warning
	-------------------------------------------------------*/
	IF EXISTS (SELECT 1 FROM #hitlist WHERE [status] = 'rollback')
	BEGIN
		DECLARE @RollbackCount INT;
		SELECT @RollbackCount = COUNT(*) FROM #hitlist WHERE [status] = 'rollback';
		SET @msg = N'WARNING: ' + CAST(@RollbackCount AS NVARCHAR(10))
			+ N' session(s) are currently rolling back. These will not be killed.';
		RAISERROR(@msg, 0, 1) WITH NOWAIT;
	END;

	/*-------------------------------------------------------
	  Section 10: Persistent Output Table
	-------------------------------------------------------*/
	IF @EmergencyMode IS NULL AND @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL
		AND EXISTS (SELECT * FROM sys.databases WHERE QUOTENAME([name]) = @OutputDatabaseName)
	BEGIN
		IF @Debug = 1
			RAISERROR('Creating/updating persistent output table...', 0, 1) WITH NOWAIT;

		IF @AzureSQLDB = 1
			SET @ObjectFullName = @OutputSchemaName + N'.' + @OutputTableName;
		ELSE
			SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' + @OutputTableName;

		/* Create table if it doesn't exist */
		IF @AzureSQLDB = 1
			SET @StringToExecute = N'
				IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''' + @OutputSchemaName + N''')
				AND NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''' + @OutputSchemaName + N''' AND QUOTENAME(TABLE_NAME) = ''' + @OutputTableName + N''')
				CREATE TABLE ' + @ObjectFullName + N' (';
		ELSE
			SET @StringToExecute = N'
				IF EXISTS(SELECT * FROM ' + @OutputDatabaseName + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''' + @OutputSchemaName + N''')
				AND NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''' + @OutputSchemaName + N''' AND QUOTENAME(TABLE_NAME) = ''' + @OutputTableName + N''')
				CREATE TABLE ' + @ObjectFullName + N' (';

		SET @StringToExecute = @StringToExecute + N'
				Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
				ServerName NVARCHAR(128) NULL,
				CheckDate DATETIMEOFFSET NULL,
				CallerLoginName NVARCHAR(128) NULL,
				CallerHostName NVARCHAR(128) NULL,
				ParametersUsed NVARCHAR(4000) NULL,
				OrderByNum INT NULL,
				KillCommand NVARCHAR(100) NULL,
				KillRecommended BIT NULL,
				KillStartedTime DATETIME NULL,
				KillEndedTime DATETIME NULL,
				KillError NVARCHAR(4000) NULL,
				Reason NVARCHAR(4000) NULL,
				session_id INT NOT NULL,
				[status] NVARCHAR(30) NULL,
				blocking_session_id INT NULL,
				wait_type NVARCHAR(60) NULL,
				wait_time_ms BIGINT NULL,
				wait_resource NVARCHAR(256) NULL,
				open_transaction_count INT NULL,
				is_implicit_transaction BIT NULL,
				cpu_time_ms BIGINT NULL,
				reads BIGINT NULL,
				writes BIGINT NULL,
				logical_reads BIGINT NULL,
				memory_grant_mb DECIMAL(38,2) NULL,
				tempdb_allocations_mb DECIMAL(38,2) NULL,
				login_name NVARCHAR(128) NULL,
				host_name NVARCHAR(128) NULL,
				program_name NVARCHAR(256) NULL,
				database_name NVARCHAR(128) NULL,
				start_time DATETIME NULL,
				last_request_start_time DATETIME NULL,
				last_request_end_time DATETIME NULL,
				command NVARCHAR(32) NULL,
				sql_text NVARCHAR(MAX) NULL,
				sql_handle VARBINARY(64) NULL,
				plan_handle VARBINARY(64) NULL,
				query_plan XML NULL,
				live_query_plan XML NULL,
				transaction_isolation_level NVARCHAR(50) NULL,
				is_read_only BIT NULL,
				FreeProcCacheCommand NVARCHAR(200) NULL
			);';

		EXEC(@StringToExecute);

		/* Insert data into persistent table */
		SET @StringToExecute = N'INSERT INTO ' + @ObjectFullName + N' (
				ServerName, CheckDate, CallerLoginName, CallerHostName, ParametersUsed,
				OrderByNum, KillCommand, KillRecommended, KillStartedTime, KillEndedTime, KillError,
				Reason, session_id, [status], blocking_session_id, wait_type, wait_time_ms, wait_resource,
				open_transaction_count, is_implicit_transaction, cpu_time_ms, reads, writes, logical_reads,
				memory_grant_mb, tempdb_allocations_mb, login_name, host_name, program_name, database_name,
				start_time, last_request_start_time, last_request_end_time, command, sql_text,
				sql_handle, plan_handle, query_plan, live_query_plan,
				transaction_isolation_level, is_read_only, FreeProcCacheCommand
			)
			SELECT
				ServerName, CheckDate, CallerLoginName, CallerHostName, ParametersUsed,
				OrderByNum, KillCommand, KillRecommended, KillStartedTime, KillEndedTime, KillError,
				Reason, session_id, [status], blocking_session_id, wait_type, wait_time_ms, wait_resource,
				open_transaction_count, is_implicit_transaction, cpu_time_ms, reads, writes, logical_reads,
				memory_grant_mb, tempdb_allocations_mb, login_name, host_name, program_name, database_name,
				start_time, last_request_start_time, last_request_end_time, command, sql_text,
				sql_handle, plan_handle, query_plan, live_query_plan,
				transaction_isolation_level, is_read_only, FreeProcCacheCommand
			FROM #hitlist
			ORDER BY KillRecommended DESC, ISNULL(OrderByNum, 2147483647), session_id;';

		EXEC(@StringToExecute);
	END;

	/*-------------------------------------------------------
	  Section 11: Kill Execution Loop
	-------------------------------------------------------*/
	IF @ExecuteKills = 'Y'
	BEGIN
		SELECT @TotalKills = COUNT(*) FROM #hitlist WHERE KillRecommended = 1;

		IF @TotalKills = 0
		BEGIN
			RAISERROR('No sessions matched the targeting filters. Nothing to kill.', 0, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @msg = N'Preparing to kill ' + CAST(@TotalKills AS NVARCHAR(10)) + N' session(s)...';
			RAISERROR(@msg, 0, 1) WITH NOWAIT;

			DECLARE kill_cursor CURSOR LOCAL FAST_FORWARD FOR
				SELECT session_id, plan_handle, sql_handle, start_time, last_request_start_time
				FROM #hitlist
				WHERE KillRecommended = 1
				ORDER BY OrderByNum;

			OPEN kill_cursor;

			FETCH NEXT FROM kill_cursor INTO @CurrentSPID, @CurrentPlanHandle, @CurrentSqlHandle, @CurrentStartTime, @CurrentLastRequestStartTime;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @KillCount = @KillCount + 1;
				SET @KillError = NULL;
				SET @KillStartTime = GETDATE();

				SET @msg = N'Killing query ' + CAST(@KillCount AS NVARCHAR(10))
					+ N' of ' + CAST(@TotalKills AS NVARCHAR(10))
					+ N' (SPID ' + CAST(@CurrentSPID AS NVARCHAR(10)) + N')...';
				RAISERROR(@msg, 0, 1) WITH NOWAIT;

				/* Verify the session still has the same query we captured */
				IF NOT EXISTS (
					SELECT 1 FROM sys.dm_exec_sessions s
					LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
					WHERE s.session_id = @CurrentSPID
					AND (
						/* Running session: match sql_handle and start_time */
						(r.sql_handle = @CurrentSqlHandle AND r.start_time = @CurrentStartTime)
						OR
						/* Sleeping session: match last_request_start_time */
						(r.session_id IS NULL AND s.last_request_start_time = @CurrentLastRequestStartTime)
						OR
						/* Session with no captured sql_handle (e.g. sleeping with no prior request info) */
						(@CurrentSqlHandle IS NULL AND @CurrentStartTime IS NULL AND s.last_request_start_time = @CurrentLastRequestStartTime)
					)
				)
				BEGIN
					SET @KillError = N'SKIPPED: Session query changed since capture - original query may have completed.';
					SET @msg = N'Skipping SPID ' + CAST(@CurrentSPID AS NVARCHAR(10)) + N': query changed since capture.';
					RAISERROR(@msg, 0, 1) WITH NOWAIT;
				END
				ELSE
				BEGIN
					/* Execute the kill */
					BEGIN TRY
						SET @KillSQL = N'KILL ' + CAST(@CurrentSPID AS NVARCHAR(10)) + N';';
						EXEC(@KillSQL);
					END TRY
					BEGIN CATCH
						SET @KillError = ERROR_MESSAGE();
						SET @msg = N'Error killing SPID ' + CAST(@CurrentSPID AS NVARCHAR(10)) + N': ' + @KillError;
						RAISERROR(@msg, 0, 1) WITH NOWAIT;
					END CATCH;

					/* Free plan from cache */
					IF @KillError IS NULL AND @CurrentPlanHandle IS NOT NULL
					BEGIN
						BEGIN TRY
							SET @FreeCacheSQL = N'DBCC FREEPROCCACHE (' + CONVERT(NVARCHAR(128), @CurrentPlanHandle, 1) + N');';
							EXEC(@FreeCacheSQL);
						END TRY
						BEGIN CATCH
							/* Plan may already be gone, ignore */
							IF @Debug = 1
							BEGIN
								SET @msg = N'Could not free plan cache for SPID ' + CAST(@CurrentSPID AS NVARCHAR(10)) + N': ' + ERROR_MESSAGE();
								RAISERROR(@msg, 0, 1) WITH NOWAIT;
							END;
						END CATCH;
					END;
				END;

				SET @KillEndTime = GETDATE();

				/* Update temp table with kill results */
				UPDATE #hitlist
				SET KillStartedTime = @KillStartTime,
					KillEndedTime = @KillEndTime,
					KillError = @KillError
				WHERE session_id = @CurrentSPID;

				FETCH NEXT FROM kill_cursor INTO @CurrentSPID, @CurrentPlanHandle, @CurrentSqlHandle, @CurrentStartTime, @CurrentLastRequestStartTime;
			END;

			CLOSE kill_cursor;
			DEALLOCATE kill_cursor;

			SET @msg = N'Kill loop complete. Processed ' + CAST(@KillCount AS NVARCHAR(10)) + N' session(s).';
			RAISERROR(@msg, 0, 1) WITH NOWAIT;

			/* Update persistent output table with kill results if it exists */
			IF @EmergencyMode IS NULL AND @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL
				AND EXISTS (SELECT * FROM sys.databases WHERE QUOTENAME([name]) = @OutputDatabaseName)
			BEGIN
				IF @AzureSQLDB = 1
						SET @ObjectFullName = @OutputSchemaName + N'.' + @OutputTableName;
					ELSE
						SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' + @OutputTableName;
				SET @StringToExecute = N'UPDATE ot
					SET ot.KillStartedTime = t.KillStartedTime,
						ot.KillEndedTime = t.KillEndedTime,
						ot.KillError = t.KillError
					FROM ' + @ObjectFullName + N' ot
					INNER JOIN #hitlist t ON ot.session_id = t.session_id
						AND ot.CheckDate = t.CheckDate
						AND ot.ServerName = t.ServerName
					WHERE t.KillRecommended = 1;';
				EXEC(@StringToExecute);
			END;
		END;
	END;

	/*-------------------------------------------------------
	  Section 12: Return Results
	-------------------------------------------------------*/
	SELECT
		Id, ServerName, CheckDate, CallerLoginName, CallerHostName, ParametersUsed,
		OrderByNum, KillCommand, KillRecommended,
		KillStartedTime, KillEndedTime, KillError,
		Reason, session_id, [status], blocking_session_id,
		wait_type, wait_time_ms, wait_resource,
		open_transaction_count, is_implicit_transaction,
		cpu_time_ms, reads, writes, logical_reads,
		memory_grant_mb, tempdb_allocations_mb,
		login_name, host_name, program_name, database_name,
		start_time, last_request_start_time, last_request_end_time,
		command, sql_text, sql_handle, plan_handle,
		query_plan, live_query_plan,
		transaction_isolation_level, is_read_only, FreeProcCacheCommand
	FROM #hitlist
	ORDER BY KillRecommended DESC, ISNULL(OrderByNum, 2147483647), session_id;

END;
GO
