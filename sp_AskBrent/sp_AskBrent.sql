USE [master];
GO

IF OBJECT_ID('master.dbo.sp_AskBrent') IS NOT NULL 
    DROP PROC dbo.sp_AskBrent;
GO

CREATE PROCEDURE [dbo].[sp_AskBrent]
    @Question NVARCHAR(MAX) = NULL ,
    @OutputType VARCHAR(20) = 'TABLE' ,
	@ExpertMode TINYINT = 0,
    @OutputDatabaseName NVARCHAR(128) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
    @Version INT = NULL OUTPUT
AS 
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	/*
	sp_AskBrent (TM) v1 - July 11, 2013
    
	(C) 2013, Brent Ozar Unlimited. 
	See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

	Sure, the server needs tuning - but why is it slow RIGHT NOW?
	sp_AskBrent performs quick checks for things like:

	* Blocking queries that have been running a long time
	* Backups and restores
	* Transactions that are rolling back
	* Long-running end user queries waiting on slow workstations

	To learn more, visit http://www.BrentOzar.com/askbrent/ where you can download
	new versions for free, watch training videos on how it works, get more info on
	the findings, and more.  To contribute code and see your name in the change
	log, email your improvements & checks to Help@BrentOzar.com.

	Known limitations of this version:
	 - No support for SQL Server 2000 or compatibility mode 80.

	Unknown limitations of this version:
	 - None. Like Zombo.com, the only limit is yourself.

    Changes in v1 - July 11, 2013
	 - Initial bug-filled release. We purposely left extra errors on here so
	   you could email bug reports to Help@BrentOzar.com, thereby increasing
	   your self-esteem. It's all about you.
	*/


	/*
	We start by creating #AskBrentResults. It's a temp table that will storef
	the results from our checks. Throughout the rest of this stored procedure,
	we're running a series of checks looking for dangerous things inside the SQL
	Server. When we find a problem, we insert rows into #BlitzResults. At the
	end, we return these results to the end user.

	#AskBrentResults has a CheckID field, but there's no Check table. As we do
	checks, we insert data into this table, and we manually put in the CheckID.
	We (Brent Ozar Unlimited) maintain a list of the checks by ID#. You can
	download that from http://www.BrentOzar.com/askbrent/documentation/ if you
	want to build a tool that relies on the output of sp_AskBrent.
	*/
	DECLARE @StringToExecute NVARCHAR(4000),
		@OurSessionID INT = @@SPID,
		@LineFeed NVARCHAR(10) = CHAR(13)+CHAR(10),
		@StockWarningHeader NVARCHAR(500),
		@StockWarningFooter NVARCHAR(100),
		@StockDetailsHeader NVARCHAR(100),
		@StockDetailsFooter NVARCHAR(100),
		@StartSampleTime DATETIME = GETDATE(),
		@FinishSampleTime DATETIME = DATEADD(ss, 10, GETDATE());

    IF OBJECT_ID('tempdb..#AskBrentResults') IS NOT NULL 
        DROP TABLE #AskBrentResults;
    CREATE TABLE #AskBrentResults
        (
          ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
          CheckID INT NOT NULL,
          Priority TINYINT NOT NULL,
          FindingsGroup VARCHAR(50) NOT NULL,
          Finding VARCHAR(200) NOT NULL,
          URL VARCHAR(200) NOT NULL,
          Details NVARCHAR(4000) NOT NULL,
		  HowToStopIt [XML] NULL,
          QueryPlan [XML] NULL,
          QueryText NVARCHAR(MAX) NULL,
		  StartTime DATETIME NULL,
		  LoginName NVARCHAR(128) NULL,
		  NTUserName NVARCHAR(128) NULL,
		  OriginalLoginName NVARCHAR(128) NULL,
		  ProgramName NVARCHAR(128) NULL,
		  HostName NVARCHAR(128) NULL,
		  DatabaseID INT NULL,
		  DatabaseName NVARCHAR(128) NULL,
		  OpenTransactionCount INT NULL
        );

    IF OBJECT_ID('tempdb..#WaitStatsBaseline') IS NOT NULL 
        DROP TABLE #WaitStatsBaseline;
	CREATE TABLE #WaitStatsBaseline (wait_type NVARCHAR(60), waiting_tasks_count BIGINT, wait_time_ms BIGINT, signal_wait_time_ms BIGINT, recorded_date DATETIME);

	SET @StockWarningHeader = '<?ClickToSeeCommmand -- ' + @LineFeed + @LineFeed 
	    + 'WARNING: Running this command may result in data loss or an outage.' + @LineFeed
		+ 'This tool is meant as a shortcut to help generate scripts for DBAs.' + @LineFeed
		+ 'It is not a substitute for database training and experience.' + @LineFeed
		+ 'Now, having said that, here''s the script:' + @LineFeed + @LineFeed;

	SELECT @StockWarningFooter = @LineFeed + @LineFeed + '-- ?>',
		@StockDetailsHeader = '<?ClickToSeeDetails -- ' + @LineFeed,
		@StockDetailsFooter = @LineFeed + ' -- ?>';

	/* Populate #WaitStatsBaseline with our current sys.dm_os_wait_stats. We'll revisit this after doing our checks. */
	INSERT INTO #WaitStatsBaseline (wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, recorded_date)
	SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, GETDATE()
	FROM sys.dm_os_wait_stats
	WHERE waiting_tasks_count > 0 OR wait_time_ms > 0;


/* Maintenance Tasks Running - Backup Running - CheckID 1 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 1 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'Backup Running' AS Finding,
	'http://BrentOzar.com/go/backups' AS URL,
	@StockDetailsHeader + 'Backup of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM sys.master_files WHERE database_id = db.resource_database_id) + 'GB) is ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	pl.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	db.[resource_database_id] AS DatabaseID,
	DB_NAME(db.resource_database_id) AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
INNER JOIN (
SELECT DISTINCT request_session_id, resource_database_id
FROM    sys.dm_tran_locks
WHERE resource_type = N'DATABASE'
AND     request_mode = N'S'
AND     request_status = N'GRANT'
AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
WHERE r.command LIKE 'BACKUP%';


/* If there's a backup running, add details explaining how long full backup has been taking in the last month. */
UPDATE #AskBrentResults
SET Details = Details + ' Over the last 60 days, the full backup usually takes ' + CAST((SELECT AVG(DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date)) FROM msdb.dbo.backupset bs WHERE abr.DatabaseName = bs.database_name AND bs.type = 'D' AND bs.backup_start_date > DATEADD(dd, -60, GETDATE()) AND bs.backup_finish_date IS NOT NULL) AS NVARCHAR(100)) + ' minutes.'
FROM #AskBrentResults abr
WHERE abr.CheckID = 1 AND EXISTS (SELECT * FROM msdb.dbo.backupset bs WHERE bs.type = 'D' AND bs.backup_start_date > DATEADD(dd, -60, GETDATE()) AND bs.backup_finish_date IS NOT NULL AND abr.DatabaseName = bs.database_name AND DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date) > 1)



/* Maintenance Tasks Running - DBCC Running - CheckID 2 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 2 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'DBCC Running' AS Finding,
	'http://BrentOzar.com/go/dbcc' AS URL,
	@StockDetailsHeader + 'Corruption check of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM sys.master_files WHERE database_id = db.resource_database_id) + 'GB) has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	pl.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	db.[resource_database_id] AS DatabaseID,
	DB_NAME(db.resource_database_id) AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
INNER JOIN (SELECT DISTINCT l.request_session_id, l.resource_database_id
FROM    sys.dm_tran_locks l
INNER JOIN sys.databases d ON l.resource_database_id = d.database_id
WHERE l.resource_type = N'DATABASE'
AND     l.request_mode = N'S'
AND    l.request_status = N'GRANT'
AND    l.request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
WHERE r.command LIKE 'DBCC%';


/* Maintenance Tasks Running - Restore Running - CheckID 3 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 3 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'Restore Running' AS Finding,
	'http://BrentOzar.com/go/backups' AS URL,
	@StockDetailsHeader + 'Restore of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM sys.master_files WHERE database_id = db.resource_database_id) + 'GB) is ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	pl.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	db.[resource_database_id] AS DatabaseID,
	DB_NAME(db.resource_database_id) AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
INNER JOIN (
SELECT DISTINCT request_session_id, resource_database_id
FROM    sys.dm_tran_locks
WHERE resource_type = N'DATABASE'
AND     request_mode = N'S'
AND     request_status = N'GRANT'
AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
WHERE r.command LIKE 'RESTORE%';


/* SQL Server Internal Maintenance - Database File Growing - CheckID 4 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 4 AS CheckID,
	1 AS Priority,
	'SQL Server Internal Maintenance' AS FindingGroup,
	'Database File Growing' AS Finding,
	'http://BrentOzar.com/go/backups' AS URL,
	@StockDetailsHeader + 'SQL Server is waiting for Windows to provide storage space for a database restore, a data file growth, or a log file growth. This task has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '.' + @LineFeed + 'Check the query plan (expert mode) to identify the database involved.' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	pl.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	NULL AS DatabaseID,
	NULL AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_os_waiting_tasks t
INNER JOIN sys.dm_exec_connections c ON t.session_id = c.session_id
INNER JOIN sys.dm_exec_requests r ON t.session_id = r.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
WHERE t.wait_type = 'PREEMPTIVE_OS_WRITEFILEGATHER'


/* Query Problems - Long-Running Query Blocking Others - CheckID 5 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 5 AS CheckID,
	1 AS Priority,
	'Query Problems' AS FindingGroup,
	'Long-Running Query Blocking Others' AS Finding,
	'http://BrentOzar.com/go/blocking' AS URL,
	@StockDetailsHeader + 'Query in ' + DB_NAME(db.resource_database_id) + ' has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' + @LineFeed + @LineFeed
		+ CAST(COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(rBlocker.sql_handle)),
		(SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY ::fn_get_sql(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id), '') AS NVARCHAR(2000)) AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(tBlocked.blocking_session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	(SELECT TOP 1 query_plan FROM sys.dm_exec_query_plan(rBlocker.plan_handle)) AS QueryPlan,
	COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(rBlocker.sql_handle)),
		(SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY ::fn_get_sql(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id)) AS QueryText,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	db.[resource_database_id] AS DatabaseID,
	DB_NAME(db.resource_database_id) AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_exec_sessions s
INNER JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
INNER JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
INNER JOIN sys.dm_os_waiting_tasks tBlocked ON tBlocked.session_id = s.session_id
INNER JOIN (
SELECT DISTINCT request_session_id, resource_database_id
FROM    sys.dm_tran_locks
WHERE resource_type = N'DATABASE'
AND     request_mode = N'S'
AND     request_status = N'GRANT'
AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
LEFT OUTER JOIN sys.dm_exec_requests rBlocker ON tBlocked.blocking_session_id = rBlocker.session_id
  WHERE NOT EXISTS (SELECT * FROM sys.dm_os_waiting_tasks tBlocker WHERE tBlocker.session_id = tBlocked.blocking_session_id AND tBlocker.blocking_session_id IS NOT NULL)
  AND s.last_request_start_time < DATEADD(SECOND, -30, GETDATE())





/* End of checks. If we haven't waited ten seconds, wait. */
IF GETDATE() < @FinishSampleTime
	WAITFOR TIME @FinishSampleTime;


/* Compare the current wait stats to the sample we took ten seconds ago, and insert the top 10 waits. */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
SELECT TOP 10 6 AS CheckID,
	200 AS Priority,
	'Wait Stats' AS FindingGroup,
	wNow.wait_type AS Finding,
	N'http://BrentOzar.com/waits/' + wNow.wait_type AS URL,
	@StockDetailsHeader + 'For ' + CAST(((wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) / 1000) AS NVARCHAR(100)) + ' seconds over the last 10 seconds, SQL Server was waiting on this particular bottleneck.' + @LineFeed + @LineFeed AS Details,
	CAST(@StockWarningHeader + 'See the URL for more details on how to mitigate this wait type.' + @StockWarningFooter AS XML) AS HowToStopIt
FROM sys.dm_os_wait_stats wNow
LEFT OUTER JOIN #WaitStatsBaseline wBase ON wNow.wait_type = wBase.wait_type
WHERE wNow.wait_time_ms > (wBase.wait_time_ms + 5000) /* Only look for things we've actually waited on for 5 seconds or more */
AND wNow.wait_type NOT IN ('REQUEST_FOR_DEADLOCK_SEARCH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP','SQLTRACE_BUFFER_FLUSH',
'LAZYWRITER_SLEEP','XE_TIMER_EVENT','XE_DISPATCHER_WAIT','FT_IFTS_SCHEDULER_IDLE_WAIT','LOGMGR_QUEUE','CHECKPOINT_QUEUE',
'BROKER_TO_FLUSH','BROKER_TASK_STOP','BROKER_EVENTHANDLER','BROKER_TRANSMITTER','SLEEP_TASK','WAITFOR','DBMIRROR_DBM_MUTEX',
'DBMIRROR_EVENTS_QUEUE','DBMIRRORING_CMD','DISPATCHER_QUEUE_SEMAPHORE','BROKER_RECEIVE_WAITFOR','CLR_AUTO_EVENT',
'DIRTY_PAGE_POLL','CLR_SEMAPHORE','HADR_FILESTREAM_IOMGR_IOCOMPLETION','ONDEMAND_TASK_QUEUE','FT_IFTSHC_MUTEX',
'CLR_MANUAL_EVENT','SP_SERVER_DIAGNOSTICS_SLEEP','DBMIRROR_WORKER_QUEUE','DBMIRROR_DBM_EVENT')
ORDER BY (wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) DESC


/* If we didn't find anything, apologize. */
IF NOT EXISTS (SELECT * FROM #AskBrentResults)
	BEGIN
	
            INSERT  INTO #AskBrentResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details
                    )
            VALUES  ( -1 ,
                      255 ,
                      'No Problems Found' ,
                      'From Brent Ozar Unlimited' ,
                      'http://www.BrentOzar.com/askbrent/' ,
                      @StockDetailsHeader + 'Try running our more in-depth checks: http://www.BrentOzar.com/blitz/' + @LineFeed + 'or there may not be an unusual SQL Server performance problem. ' + @StockDetailsFooter
                    );
	
	END /*IF NOT EXISTS (SELECT * FROM #AskBrentResults) */
	ELSE /* We found stuff, so add credits */
	BEGIN
		/* Close out the XML field Details by adding a footer */
		UPDATE #AskBrentResults
		  SET Details = Details + @StockDetailsFooter;

			/* Add credits for the nice folks who put so much time into building and maintaining this for free: */                    
            INSERT  INTO #AskBrentResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details
                    )
            VALUES  ( -1 ,
                      255 ,
                      'Thanks!' ,
                      'From Brent Ozar Unlimited' ,
                      'http://www.BrentOzar.com/askbrent/' ,
                      '<?Thanks --' + @LineFeed + 'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com. ' + @LineFeed + '-- ?>'
                    );

            SET @Version = 1;
            INSERT  INTO #AskBrentResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details

                    )
            VALUES  ( -1 ,
                      0 ,
                      'sp_AskBrent (TM) v1 July 11 2013' ,
                      'From Brent Ozar Unlimited' ,
                      'http://www.BrentOzar.com/askbrent/' ,
                      '<?Thanks --' + @LineFeed + 'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.' + @LineFeed + ' -- ?>'
                    );

	END /* ELSE  We found stuff, so add credits */

			/* @OutputTableName lets us export the results to a permanent table */
            IF @OutputDatabaseName IS NOT NULL
                AND @OutputSchemaName IS NOT NULL
                AND @OutputTableName IS NOT NULL
                AND EXISTS ( SELECT *
                             FROM   sys.databases
                             WHERE  QUOTENAME([name]) = @OutputDatabaseName) 
                BEGIN
                    SET @StringToExecute = 'USE '
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
                        + ' (ID INT IDENTITY(1,1) NOT NULL, 
							ServerName NVARCHAR(128), 
							CheckDate DATETIME, 
							AskBrentVersion INT,
							Priority TINYINT ,
							FindingsGroup VARCHAR(50) ,
							Finding VARCHAR(200) ,
							URL VARCHAR(200) ,
							Details [XML] ,
							HowToStopIt [XML] ,
							QueryPlan [XML] NULL ,
							CheckID INT ,
							CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'

                    EXEC(@StringToExecute);
                    SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
                        + @OutputDatabaseName
                        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                        + @OutputSchemaName + ''') INSERT '
                        + @OutputDatabaseName + '.'
                        + @OutputSchemaName + '.'
                        + @OutputTableName
                        + ' (ServerName, CheckDate, AskBrentVersion, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, CheckID) SELECT '''
                        + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
                        + ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
                        + ', Priority, FindingsGroup, Finding, URL, Details, QueryPlan, CheckID FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';
                    EXEC(@StringToExecute);
                END
			ELSE IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
				BEGIN
					SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
                        + @OutputTableName
                        + ''') IS NOT NULL) DROP TABLE ' + @OutputTableName + ';'
						+ 'CREATE TABLE '
                        + @OutputTableName
                        + ' (ID INT IDENTITY(1,1) NOT NULL, 
							ServerName NVARCHAR(128), 
							CheckDate DATETIME, 
							AskBrentVersion INT,
							Priority TINYINT ,
							FindingsGroup VARCHAR(50) ,
							Finding VARCHAR(200) ,
							URL VARCHAR(200) ,
							Details [XML] ,
							HowToStopIt [XML] ,
							QueryPlan [XML] NULL ,
							QueryText NVARCHAR(MAX) NULL ,
							CheckID INT ,
							CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'
                        + ' INSERT '
                        + @OutputTableName
                        + ' (ServerName, CheckDate, AskBrentVersion, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, CheckID) SELECT '''
                        + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
                        + ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
                        + ', Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, CheckID FROM #AskBrentResults ORDER BY Priority , FindingsGroup , Finding , Details';
                    EXEC(@StringToExecute);
				END
			ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
				BEGIN
					RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0)
				END


            DECLARE @separator AS VARCHAR(1);
            IF @OutputType = 'RSV' 
                SET @separator = CHAR(31);
            ELSE 
                SET @separator = ',';

            IF @OutputType = 'COUNT' 
                BEGIN
                    SELECT  COUNT(*) AS Warnings
                    FROM    #AskBrentResults
                END
            ELSE 
                IF @OutputType IN ( 'CSV', 'RSV' ) 
                    BEGIN
			
                        SELECT  Result = CAST([Priority] AS NVARCHAR(100))
                                + @separator + CAST(CheckID AS NVARCHAR(100))
                                + @separator + COALESCE([FindingsGroup],
                                                        '(N/A)') + @separator
                                + COALESCE([Finding], '(N/A)') + @separator
                                + COALESCE(DatabaseName, '(N/A)') + @separator
                                + COALESCE([URL], '(N/A)') + @separator
                                + COALESCE([Details], '(N/A)')
                        FROM    #AskBrentResults
                        ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                Details;
                    END
                ELSE IF @ExpertMode = 0
                    BEGIN
                        SELECT  [Priority] ,
                                [FindingsGroup] ,
                                [Finding] ,
                                [URL] ,
                                CAST([Details] AS [XML]) AS Details,
								[HowToStopIt]
                        FROM    #AskBrentResults
						ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                ID;
                    END
                ELSE IF @ExpertMode = 1
                    BEGIN
                        SELECT  [Priority] ,
                                [FindingsGroup] ,
                                [Finding] ,
                                [URL] ,
                                CAST([Details] AS [XML]) AS Details,
								[HowToStopIt] ,
								[CheckID] ,
								[StartTime],
								[LoginName],
								[NTUserName],
								[OriginalLoginName],
								[ProgramName],
								[HostName],
								[DatabaseID],
								[DatabaseName],
								[OpenTransactionCount],
								[QueryPlan],
								[QueryText]
                        FROM    #AskBrentResults
						ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                ID;
                    END

            DROP TABLE #AskBrentResults;

    SET NOCOUNT OFF;
GO


EXEC dbo.sp_AskBrent @ExpertMode = 1;
