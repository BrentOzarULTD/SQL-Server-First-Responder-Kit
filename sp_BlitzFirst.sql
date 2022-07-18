IF OBJECT_ID('dbo.sp_BlitzFirst') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzFirst AS RETURN 0;');
GO


ALTER PROCEDURE [dbo].[sp_BlitzFirst]
    @LogMessage NVARCHAR(4000) = NULL ,
    @Help TINYINT = 0 ,
    @AsOf DATETIMEOFFSET = NULL ,
    @ExpertMode TINYINT = 0 ,
    @Seconds INT = 5 ,
    @OutputType VARCHAR(20) = 'TABLE' ,
    @OutputServerName NVARCHAR(256) = NULL ,
    @OutputDatabaseName NVARCHAR(256) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
    @OutputTableNameFileStats NVARCHAR(256) = NULL ,
    @OutputTableNamePerfmonStats NVARCHAR(256) = NULL ,
    @OutputTableNameWaitStats NVARCHAR(256) = NULL ,
    @OutputTableNameBlitzCache NVARCHAR(256) = NULL ,
    @OutputTableNameBlitzWho NVARCHAR(256) = NULL ,
    @OutputTableRetentionDays TINYINT = 7 ,
    @OutputXMLasNVARCHAR TINYINT = 0 ,
    @FilterPlansByDatabase VARCHAR(MAX) = NULL ,
    @CheckProcedureCache TINYINT = 0 ,
    @CheckServerInfo TINYINT = 1 ,
    @FileLatencyThresholdMS INT = 100 ,
    @SinceStartup TINYINT = 0 ,
    @ShowSleepingSPIDs TINYINT = 0 ,
    @BlitzCacheSkipAnalysis BIT = 1 ,
	@MemoryGrantThresholdPct DECIMAL(5,2) = 15.00,
    @LogMessageCheckID INT = 38,
    @LogMessagePriority TINYINT = 1,
    @LogMessageFindingsGroup VARCHAR(50) = 'Logged Message',
    @LogMessageFinding VARCHAR(200) = 'Logged from sp_BlitzFirst',
    @LogMessageURL VARCHAR(200) = '',
    @LogMessageCheckDate DATETIMEOFFSET = NULL,
    @Debug BIT = 0,
	@Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
    WITH EXECUTE AS CALLER, RECOMPILE
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
sp_BlitzFirst from http://FirstResponderKit.org
	
This script gives you a prioritized list of why your SQL Server is slow right now.

This is not an overall health check - for that, check out sp_Blitz.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000. It
   may work just fine on 2005, and if it does, hug your parents. Just don''t
   file support issues if it breaks.
 - If a temp table called #CustomPerfmonCounters exists for any other session,
   but not our session, this stored proc will fail with an error saying the
   temp table #CustomPerfmonCounters does not exist.
 - @OutputServerName is not functional yet.
 - If @OutputDatabaseName, SchemaName, TableName, etc are quoted with brackets,
   the write to table may silently fail. Look, I never said I was good at this.

Unknown limitations of this version:
 - None. Like Zombo.com, the only limit is yourself.

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
RETURN;
END;    /* @Help = 1 */

RAISERROR('Setting up configuration variables',10,1) WITH NOWAIT;
DECLARE @StringToExecute NVARCHAR(MAX),
    @ParmDefinitions NVARCHAR(4000),
    @Parm1 NVARCHAR(4000),
    @OurSessionID INT,
    @LineFeed NVARCHAR(10),
    @StockWarningHeader NVARCHAR(MAX) = N'',
    @StockWarningFooter NVARCHAR(MAX) = N'',
    @StockDetailsHeader NVARCHAR(MAX) = N'',
    @StockDetailsFooter NVARCHAR(MAX) = N'',
    @StartSampleTime DATETIMEOFFSET,
    @FinishSampleTime DATETIMEOFFSET,
	@FinishSampleTimeWaitFor DATETIME,
	@AsOf1 DATETIMEOFFSET,
	@AsOf2 DATETIMEOFFSET,
    @ServiceName sysname,
    @OutputTableNameFileStats_View NVARCHAR(256),
    @OutputTableNamePerfmonStats_View NVARCHAR(256),
    @OutputTableNamePerfmonStatsActuals_View NVARCHAR(256),
    @OutputTableNameWaitStats_View NVARCHAR(256),
    @OutputTableNameWaitStats_Categories NVARCHAR(256),
	@OutputTableCleanupDate DATE,
    @ObjectFullName NVARCHAR(2000),
	@BlitzWho NVARCHAR(MAX) = N'EXEC dbo.sp_BlitzWho @ShowSleepingSPIDs = ' + CONVERT(NVARCHAR(1), @ShowSleepingSPIDs) + N';',
    @BlitzCacheMinutesBack INT,
    @UnquotedOutputServerName NVARCHAR(256) = @OutputServerName ,
    @UnquotedOutputDatabaseName NVARCHAR(256) = @OutputDatabaseName ,
    @UnquotedOutputSchemaName NVARCHAR(256) = @OutputSchemaName ,
    @LocalServerName NVARCHAR(128) = CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)),
	@dm_exec_query_statistics_xml BIT = 0,
	@total_cpu_usage BIT = 0,
	@get_thread_time_ms NVARCHAR(MAX) = N'',
	@thread_time_ms FLOAT = 0;

/* Sanitize our inputs */
SELECT
    @OutputTableNameFileStats_View = QUOTENAME(@OutputTableNameFileStats + '_Deltas'),
    @OutputTableNamePerfmonStats_View = QUOTENAME(@OutputTableNamePerfmonStats + '_Deltas'),
    @OutputTableNamePerfmonStatsActuals_View = QUOTENAME(@OutputTableNamePerfmonStats + '_Actuals'),
    @OutputTableNameWaitStats_View = QUOTENAME(@OutputTableNameWaitStats + '_Deltas'),
    @OutputTableNameWaitStats_Categories = QUOTENAME(@OutputTableNameWaitStats + '_Categories');

SELECT
    @OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
    @OutputSchemaName = QUOTENAME(@OutputSchemaName),
    @OutputTableName = QUOTENAME(@OutputTableName),
    @OutputTableNameFileStats = QUOTENAME(@OutputTableNameFileStats),
    @OutputTableNamePerfmonStats = QUOTENAME(@OutputTableNamePerfmonStats),
    @OutputTableNameWaitStats = QUOTENAME(@OutputTableNameWaitStats),
	@OutputTableCleanupDate = CAST( (DATEADD(DAY, -1 * @OutputTableRetentionDays, GETDATE() ) ) AS DATE),
    /* @OutputTableNameBlitzCache = QUOTENAME(@OutputTableNameBlitzCache),  We purposely don't sanitize this because sp_BlitzCache will */
    /* @OutputTableNameBlitzWho = QUOTENAME(@OutputTableNameBlitzWho),  We purposely don't sanitize this because sp_BlitzWho will */
    @LineFeed = CHAR(13) + CHAR(10),
    @OurSessionID = @@SPID,
    @OutputType                     = UPPER(@OutputType);

IF(@OutputType = 'NONE' AND (@OutputTableName IS NULL OR @OutputSchemaName IS NULL OR @OutputDatabaseName IS NULL))
BEGIN
    RAISERROR('This procedure should be called with a value for all @Output* parameters, as @OutputType is set to NONE',12,1);
    RETURN;
END;

IF UPPER(@OutputType) LIKE 'TOP 10%' SET @OutputType = 'Top10';
IF @OutputType = 'Top10' SET @SinceStartup = 1;

/* Logged Message  - CheckID 38 */
IF @LogMessage IS NOT NULL
    BEGIN

    RAISERROR('Saving LogMessage to table',10,1) WITH NOWAIT;

    /* Try to set the output table parameters if they don't exist */
    IF @OutputSchemaName IS NULL AND @OutputTableName IS NULL AND @OutputDatabaseName IS NULL
        BEGIN
            SET @OutputSchemaName = N'[dbo]';
            SET @OutputTableName = N'[BlitzFirst]';

            /* Look for the table in the current database */
            SELECT TOP 1 @OutputDatabaseName = QUOTENAME(TABLE_CATALOG)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'BlitzFirst';

            IF @OutputDatabaseName IS NULL AND EXISTS (SELECT * FROM sys.databases WHERE name = 'DBAtools')
                SET @OutputDatabaseName = '[DBAtools]';

        END;

    IF @OutputDatabaseName IS NULL OR @OutputSchemaName IS NULL OR @OutputTableName IS NULL
            OR NOT EXISTS ( SELECT *
                     FROM   sys.databases
                     WHERE  QUOTENAME([name]) = @OutputDatabaseName)
		BEGIN
        RAISERROR('We have a hard time logging a message without a valid @OutputDatabaseName, @OutputSchemaName, and @OutputTableName to log it to.', 0, 1) WITH NOWAIT;
		RETURN;
        END;
    IF @LogMessageCheckDate IS NULL
        SET @LogMessageCheckDate = SYSDATETIMEOFFSET();
    SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
        + @OutputDatabaseName
        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
        + @OutputSchemaName + ''') INSERT '
        + @OutputDatabaseName + '.'
        + @OutputSchemaName + '.'
        + @OutputTableName
        + ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, Details, URL) VALUES( '
        + ' @SrvName, @LogMessageCheckDate, @LogMessageCheckID, @LogMessagePriority, @LogMessageFindingsGroup, @LogMessageFinding, @LogMessage, @LogMessageURL)';

    EXECUTE sp_executesql @StringToExecute,
        N'@SrvName NVARCHAR(128), @LogMessageCheckID INT, @LogMessagePriority TINYINT, @LogMessageFindingsGroup VARCHAR(50), @LogMessageFinding VARCHAR(200), @LogMessage NVARCHAR(4000), @LogMessageCheckDate DATETIMEOFFSET, @LogMessageURL VARCHAR(200)',
        @LocalServerName, @LogMessageCheckID, @LogMessagePriority, @LogMessageFindingsGroup, @LogMessageFinding, @LogMessage, @LogMessageCheckDate, @LogMessageURL;

    RAISERROR('LogMessage saved to table. We have made a note of your activity. Keep up the good work.',10,1) WITH NOWAIT;

    RETURN;
    END;

IF @SinceStartup = 1
    SELECT @Seconds = 0, @ExpertMode = 1;


IF @OutputType = 'SCHEMA'
BEGIN
    SELECT FieldList = '[Priority] TINYINT, [FindingsGroup] VARCHAR(50), [Finding] VARCHAR(200), [URL] VARCHAR(200), [Details] NVARCHAR(4000), [HowToStopIt] NVARCHAR(MAX), [QueryPlan] XML, [QueryText] NVARCHAR(MAX)';

END;
ELSE IF @AsOf IS NOT NULL AND @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL
BEGIN
    /* They want to look into the past. */
        SET @AsOf1= DATEADD(mi, -15, @AsOf);
		SET @AsOf2= DATEADD(mi, +15, @AsOf);

        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') SELECT CheckDate, [Priority], [FindingsGroup], [Finding], [URL], CAST([Details] AS [XML]) AS Details,'
            + '[HowToStopIt], [CheckID], [StartTime], [LoginName], [NTUserName], [OriginalLoginName], [ProgramName], [HostName], [DatabaseID],'
            + '[DatabaseName], [OpenTransactionCount], [QueryPlan], [QueryText] FROM '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableName
            + ' WHERE CheckDate >= @AsOf1'
            + ' AND CheckDate <= @AsOf2'
            + ' /*ORDER BY CheckDate, Priority , FindingsGroup , Finding , Details*/;';
		EXEC sp_executesql @StringToExecute,
			N'@AsOf1 DATETIMEOFFSET, @AsOf2 DATETIMEOFFSET',
			@AsOf1, @AsOf2


END; /* IF @AsOf IS NOT NULL AND @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL */
ELSE IF @LogMessage IS NULL /* IF @OutputType = 'SCHEMA' */
BEGIN
    /* What's running right now? This is the first and last result set. */
    IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'
    BEGIN
		IF OBJECT_ID('master.dbo.sp_BlitzWho') IS NULL AND OBJECT_ID('dbo.sp_BlitzWho') IS NULL
		BEGIN
			PRINT N'sp_BlitzWho is not installed in the current database_files.  You can get a copy from http://FirstResponderKit.org';
		END;
		ELSE
		BEGIN
			EXEC (@BlitzWho);
		END;
    END; /* IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'   -   What's running right now? This is the first and last result set. */

    /* Set start/finish times AFTER sp_BlitzWho runs. For more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2244 */
    IF @Seconds = 0 AND SERVERPROPERTY('Edition') = 'SQL Azure'
        WITH WaitTimes AS (
            SELECT wait_type, wait_time_ms,
                NTILE(3) OVER(ORDER BY wait_time_ms) AS grouper
                FROM sys.dm_os_wait_stats w
                WHERE wait_type IN ('DIRTY_PAGE_POLL','HADR_FILESTREAM_IOMGR_IOCOMPLETION','LAZYWRITER_SLEEP',
                                    'LOGMGR_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH','XE_TIMER_EVENT')
        )
        SELECT @StartSampleTime = DATEADD(mi, AVG(-wait_time_ms / 1000 / 60), SYSDATETIMEOFFSET()), @FinishSampleTime = SYSDATETIMEOFFSET()
            FROM WaitTimes
            WHERE grouper = 2;
    ELSE IF @Seconds = 0 AND SERVERPROPERTY('Edition') <> 'SQL Azure'
        SELECT @StartSampleTime = DATEADD(MINUTE,DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()),create_date) , @FinishSampleTime = SYSDATETIMEOFFSET()
            FROM sys.databases
            WHERE database_id = 2;
    ELSE
        SELECT @StartSampleTime = SYSDATETIMEOFFSET(),
                @FinishSampleTime = DATEADD(ss, @Seconds, SYSDATETIMEOFFSET()),
                @FinishSampleTimeWaitFor = DATEADD(ss, @Seconds, GETDATE());


    IF EXISTS
	   (

	       SELECT
		       1/0
		   FROM sys.all_columns AS ac
		   WHERE ac.object_id = OBJECT_ID('sys.dm_os_schedulers')
		   AND   ac.name = 'total_cpu_usage_ms'

	   )
	BEGIN

	    SELECT
		    @total_cpu_usage = 1,
			@get_thread_time_ms +=
			    N'
			     SELECT 
				     @thread_time_ms = 
				         CONVERT
					     (
					         FLOAT,
				             SUM(s.total_cpu_usage_ms)
					     )
                 FROM sys.dm_os_schedulers AS s
                 WHERE  s.status = ''VISIBLE ONLINE''
                 AND    s.is_online = 1
				 OPTION(RECOMPILE);
			     ';

	END
	ELSE
	BEGIN
	    SELECT
		    @total_cpu_usage = 0,
			@get_thread_time_ms +=
			    N'
			     SELECT 
				     @thread_time_ms = 
				         CONVERT
					     (
					         FLOAT,
				             SUM(s.total_worker_time / 1000.)
					     )
                 FROM sys.dm_exec_query_stats AS s
                 OPTION(RECOMPILE);
			     ';
	END

    RAISERROR('Now starting diagnostic analysis',10,1) WITH NOWAIT;

    /*
    We start by creating #BlitzFirstResults. It's a temp table that will store
    the results from our checks. Throughout the rest of this stored procedure,
    we're running a series of checks looking for dangerous things inside the SQL
    Server. When we find a problem, we insert rows into the temp table. At the
    end, we return these results to the end user.

    #BlitzFirstResults has a CheckID field, but there's no Check table. As we do
    checks, we insert data into this table, and we manually put in the CheckID.
    We (Brent Ozar Unlimited) maintain a list of the checks by ID#. You can
    download that from http://FirstResponderKit.org if you want to build
    a tool that relies on the output of sp_BlitzFirst.
    */


    IF OBJECT_ID('tempdb..#BlitzFirstResults') IS NOT NULL
        DROP TABLE #BlitzFirstResults;
    CREATE TABLE #BlitzFirstResults
        (
          ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
          CheckID INT NOT NULL,
          Priority TINYINT NOT NULL,
          FindingsGroup VARCHAR(50) NOT NULL,
          Finding VARCHAR(200) NOT NULL,
          URL VARCHAR(200) NULL,
          Details NVARCHAR(MAX) NULL,
          HowToStopIt NVARCHAR(MAX) NULL,
          QueryPlan [XML] NULL,
          QueryText NVARCHAR(MAX) NULL,
          StartTime DATETIMEOFFSET NULL,
          LoginName NVARCHAR(128) NULL,
          NTUserName NVARCHAR(128) NULL,
          OriginalLoginName NVARCHAR(128) NULL,
          ProgramName NVARCHAR(128) NULL,
          HostName NVARCHAR(128) NULL,
          DatabaseID INT NULL,
          DatabaseName NVARCHAR(128) NULL,
          OpenTransactionCount INT NULL,
          QueryStatsNowID INT NULL,
          QueryStatsFirstID INT NULL,
          PlanHandle VARBINARY(64) NULL,
          DetailsInt INT NULL,
          QueryHash BINARY(8)
        );

    IF OBJECT_ID('tempdb..#WaitStats') IS NOT NULL
        DROP TABLE #WaitStats;
    CREATE TABLE #WaitStats (
	    Pass TINYINT NOT NULL,
		wait_type NVARCHAR(60),
		wait_time_ms BIGINT,
		thread_time_ms FLOAT,
		signal_wait_time_ms BIGINT,
		waiting_tasks_count BIGINT,
		SampleTime datetimeoffset
		);

    IF OBJECT_ID('tempdb..#FileStats') IS NOT NULL
        DROP TABLE #FileStats;
    CREATE TABLE #FileStats (
        ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
        Pass TINYINT NOT NULL,
        SampleTime DATETIMEOFFSET NOT NULL,
        DatabaseID INT NOT NULL,
        FileID INT NOT NULL,
        DatabaseName NVARCHAR(256) ,
        FileLogicalName NVARCHAR(256) ,
        TypeDesc NVARCHAR(60) ,
        SizeOnDiskMB BIGINT ,
        io_stall_read_ms BIGINT ,
        num_of_reads BIGINT ,
        bytes_read BIGINT ,
        io_stall_write_ms BIGINT ,
        num_of_writes BIGINT ,
        bytes_written BIGINT,
        PhysicalName NVARCHAR(520) ,
        avg_stall_read_ms INT ,
        avg_stall_write_ms INT
    );

    IF OBJECT_ID('tempdb..#QueryStats') IS NOT NULL
        DROP TABLE #QueryStats;
    CREATE TABLE #QueryStats (
        ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
        Pass INT NOT NULL,
        SampleTime DATETIMEOFFSET NOT NULL,
        [sql_handle] VARBINARY(64),
        statement_start_offset INT,
        statement_end_offset INT,
        plan_generation_num BIGINT,
        plan_handle VARBINARY(64),
        execution_count BIGINT,
        total_worker_time BIGINT,
        total_physical_reads BIGINT,
        total_logical_writes BIGINT,
        total_logical_reads BIGINT,
        total_clr_time BIGINT,
        total_elapsed_time BIGINT,
        creation_time DATETIMEOFFSET,
        query_hash BINARY(8),
        query_plan_hash BINARY(8),
        Points TINYINT
    );

    IF OBJECT_ID('tempdb..#PerfmonStats') IS NOT NULL
        DROP TABLE #PerfmonStats;
    CREATE TABLE #PerfmonStats (
        ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
        Pass TINYINT NOT NULL,
        SampleTime DATETIMEOFFSET NOT NULL,
        [object_name] NVARCHAR(128) NOT NULL,
        [counter_name] NVARCHAR(128) NOT NULL,
        [instance_name] NVARCHAR(128) NULL,
        [cntr_value] BIGINT NULL,
        [cntr_type] INT NOT NULL,
        [value_delta] BIGINT NULL,
        [value_per_second] DECIMAL(18,2) NULL
    );

    IF OBJECT_ID('tempdb..#PerfmonCounters') IS NOT NULL
        DROP TABLE #PerfmonCounters;
    CREATE TABLE #PerfmonCounters (
        ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
        [object_name] NVARCHAR(128) NOT NULL,
        [counter_name] NVARCHAR(128) NOT NULL,
        [instance_name] NVARCHAR(128) NULL
    );

    IF OBJECT_ID('tempdb..#FilterPlansByDatabase') IS NOT NULL
        DROP TABLE #FilterPlansByDatabase;
    CREATE TABLE #FilterPlansByDatabase (DatabaseID INT PRIMARY KEY CLUSTERED);

	IF OBJECT_ID ('tempdb..#checkversion') IS NOT NULL
		DROP TABLE #checkversion;
	CREATE TABLE #checkversion (
		version NVARCHAR(128),
		common_version AS SUBSTRING(version, 1, CHARINDEX('.', version) + 1 ),
		major AS PARSENAME(CONVERT(VARCHAR(32), version), 4),
		minor AS PARSENAME(CONVERT(VARCHAR(32), version), 3),
		build AS PARSENAME(CONVERT(VARCHAR(32), version), 2),
		revision AS PARSENAME(CONVERT(VARCHAR(32), version), 1)
	);

    IF OBJECT_ID('tempdb..##WaitCategories') IS NULL
		BEGIN
			/* We reuse this one by default rather than recreate it every time. */
			CREATE TABLE ##WaitCategories
			(
				WaitType NVARCHAR(60) PRIMARY KEY CLUSTERED,
				WaitCategory NVARCHAR(128) NOT NULL,
				Ignorable BIT DEFAULT 0
			);
		END; /* IF OBJECT_ID('tempdb..##WaitCategories') IS NULL */

	IF 527 > (SELECT COALESCE(SUM(1),0) FROM ##WaitCategories)
		BEGIN
		    TRUNCATE TABLE ##WaitCategories;
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('ASYNC_IO_COMPLETION','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('ASYNC_NETWORK_IO','Network IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BACKUPIO','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_CONNECTION_RECEIVE_TASK','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_DISPATCHER','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_ENDPOINT_STATE_MUTEX','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_EVENTHANDLER','Service Broker',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_FORWARDER','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_INIT','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_MASTERSTART','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_RECEIVE_WAITFOR','User Wait',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_REGISTERALLENDPOINTS','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_SERVICE','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_SHUTDOWN','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_START','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TASK_SHUTDOWN','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TASK_STOP','Service Broker',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TASK_SUBMIT','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TO_FLUSH','Service Broker',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TRANSMISSION_OBJECT','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TRANSMISSION_TABLE','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TRANSMISSION_WORK','Service Broker',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('BROKER_TRANSMITTER','Service Broker',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CHECKPOINT_QUEUE','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CHKPT','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_AUTO_EVENT','SQL CLR',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_CRST','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_JOIN','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_MANUAL_EVENT','SQL CLR',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_MEMORY_SPY','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_MONITOR','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_RWLOCK_READER','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_RWLOCK_WRITER','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_SEMAPHORE','SQL CLR',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLR_TASK_START','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CLRHOST_STATE_ACCESS','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CMEMPARTITIONED','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CMEMTHREAD','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CXPACKET','Parallelism',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('CXCONSUMER','Parallelism',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRROR_DBM_EVENT','Mirroring',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRROR_DBM_MUTEX','Mirroring',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRROR_EVENTS_QUEUE','Mirroring',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRROR_SEND','Mirroring',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRROR_WORKER_QUEUE','Mirroring',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DBMIRRORING_CMD','Mirroring',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DIRTY_PAGE_POLL','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DIRTY_PAGE_TABLE_LOCK','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DISPATCHER_QUEUE_SEMAPHORE','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DPT_ENTRY_LOCK','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC_ABORT_REQUEST','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC_RESOLVE','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC_STATE','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC_TMDOWN_REQUEST','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTC_WAITFOR_OUTCOME','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCNEW_ENLIST','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCNEW_PREPARE','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCNEW_RECOVERY','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCNEW_TM','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCNEW_TRANSACTION_ENLISTMENT','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('DTCPNTSYNC','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('EE_PMOLOCK','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('EXCHANGE','Parallelism',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('EXTERNAL_SCRIPT_NETWORK_IOF','Network IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FCB_REPLICA_READ','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FCB_REPLICA_WRITE','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_COMPROWSET_RWLOCK','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_IFTS_RWLOCK','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_IFTS_SCHEDULER_IDLE_WAIT','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_IFTSHC_MUTEX','Full Text Search',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_IFTSISM_MUTEX','Full Text Search',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_MASTER_MERGE','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_MASTER_MERGE_COORDINATOR','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_METADATA_MUTEX','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_PROPERTYLIST_CACHE','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FT_RESTART_CRAWL','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('FULLTEXT GATHERER','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_AG_MUTEX','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_AR_CRITICAL_SECTION_ENTRY','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_AR_MANAGER_MUTEX','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_AR_UNLOAD_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_ARCONTROLLER_NOTIFICATIONS_SUBSCRIBER_LIST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_BACKUP_BULK_LOCK','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_BACKUP_QUEUE','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_CLUSAPI_CALL','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_COMPRESSED_CACHE_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_CONNECTIVITY_INFO','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DATABASE_FLOW_CONTROL','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DATABASE_VERSIONING_STATE','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DATABASE_WAIT_FOR_RECOVERY','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DATABASE_WAIT_FOR_RESTART','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DB_COMMAND','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DB_OP_COMPLETION_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DB_OP_START_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DBR_SUBSCRIBER','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DBR_SUBSCRIBER_FILTER_LIST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DBSEEDING','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DBSEEDING_LIST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_DBSTATECHANGE_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FABRIC_CALLBACK','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_BLOCK_FLUSH','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_FILE_CLOSE','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_FILE_REQUEST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_IOMGR','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_IOMGR_IOCOMPLETION','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_MANAGER','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_FILESTREAM_PREPROC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_GROUP_COMMIT','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_LOGCAPTURE_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_LOGCAPTURE_WAIT','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_LOGPROGRESS_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_NOTIFICATION_DEQUEUE','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_NOTIFICATION_WORKER_EXCLUSIVE_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_NOTIFICATION_WORKER_STARTUP_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_NOTIFICATION_WORKER_TERMINATION_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_PARTNER_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_READ_ALL_NETWORKS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_RECOVERY_WAIT_FOR_CONNECTION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_RECOVERY_WAIT_FOR_UNDO','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_REPLICAINFO_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_CANCELLATION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_FILE_LIST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_LIMIT_BACKUPS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_SYNC_COMPLETION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_TIMEOUT_TASK','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SEEDING_WAIT_FOR_COMPLETION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SYNC_COMMIT','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_SYNCHRONIZING_THROTTLE','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TDS_LISTENER_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TDS_LISTENER_SYNC_PROCESSING','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_THROTTLE_LOG_RATE_GOVERNOR','Log Rate Governor',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TIMER_TASK','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TRANSPORT_DBRLIST','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TRANSPORT_FLOW_CONTROL','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_TRANSPORT_SESSION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_WORK_POOL','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_WORK_QUEUE','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('HADR_XRF_STACK_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('INSTANCE_LOG_RATE_GOVERNOR','Log Rate Governor',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('IO_COMPLETION','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('IO_QUEUE_LIMIT','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('IO_RETRY','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_DT','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_EX','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_KP','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_NL','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_SH','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LATCH_UP','Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LAZYWRITER_SLEEP','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_BU','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_BU_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_BU_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IS_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IS_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IU','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IU_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IU_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IX','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IX_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_IX_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_NL','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_NL_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_NL_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_S','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_S_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_S_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_U','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_U_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_U_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_X','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_X_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RIn_X_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_S','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_S_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_S_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_U','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_U_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RS_U_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_S','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_S_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_S_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_U','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_U_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_U_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_X','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_X_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_RX_X_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_S','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_S_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_S_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_M','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_M_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_M_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_S','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_S_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SCH_S_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIU','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIU_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIU_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIX','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIX_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_SIX_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_U','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_U_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_U_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_UIX','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_UIX_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_UIX_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_X','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_X_ABORT_BLOCKERS','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LCK_M_X_LOW_PRIORITY','Lock',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOG_RATE_GOVERNOR','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGBUFFER','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGMGR','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGMGR_FLUSH','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGMGR_PMM_LOG','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGMGR_QUEUE','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('LOGMGR_RESERVE_APPEND','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('MEMORY_ALLOCATION_EXT','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('MEMORY_GRANT_UPDATE','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('MSQL_XACT_MGR_MUTEX','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('MSQL_XACT_MUTEX','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('MSSEARCH','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('NET_WAITFOR_PACKET','Network IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('ONDEMAND_TASK_QUEUE','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_DT','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_EX','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_KP','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_NL','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_SH','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGEIOLATCH_UP','Buffer IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_DT','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_EX','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_KP','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_NL','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_SH','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PAGELATCH_UP','Buffer Latch',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_DRAIN_WORKER','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_FLOW_CONTROL','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_LOG_CACHE','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_TRAN_LIST','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_TRAN_TURN','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_WORKER_SYNC','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PARALLEL_REDO_WORKER_WAIT_WORK','Replication',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('POOL_LOG_RATE_GOVERNOR','Log Rate Governor',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('POPULATE_LOCK_ORDINALS','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_ABR','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CLOSEBACKUPMEDIA','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CLOSEBACKUPTAPE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CLOSEBACKUPVDIDEVICE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CLUSAPI_CLUSTERRESOURCECONTROL','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_COCREATEINSTANCE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_COGETCLASSOBJECT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_CREATEACCESSOR','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_DELETEROWS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_GETCOMMANDTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_GETDATA','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_GETNEXTROWS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_GETRESULT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_GETROWSBYBOOKMARK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBFLUSH','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBLOCKREGION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBREADAT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBSETSIZE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBSTAT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBUNLOCKREGION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_LBWRITEAT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_QUERYINTERFACE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_RELEASE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_RELEASEACCESSOR','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_RELEASEROWS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_RELEASESESSION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_RESTARTPOSITION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_SEQSTRMREAD','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_SEQSTRMREADANDWRITE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_SETDATAFAILURE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_SETPARAMETERINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_SETPARAMETERPROPERTIES','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMLOCKREGION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMSEEKANDREAD','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMSEEKANDWRITE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMSETSIZE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMSTAT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_COM_STRMUNLOCKREGION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CONSOLEWRITE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_CREATEPARAM','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DEBUG','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSADDLINK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSLINKEXISTCHECK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSLINKHEALTHCHECK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSREMOVELINK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSREMOVEROOT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSROOTFOLDERCHECK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSROOTINIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DFSROOTSHARECHECK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_ABORT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_ABORTREQUESTDONE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_BEGINTRANSACTION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_COMMITREQUESTDONE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_ENLIST','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_DTC_PREPAREREQUESTDONE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_FILESIZEGET','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_FSAOLEDB_ABORTTRANSACTION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_FSAOLEDB_COMMITTRANSACTION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_FSAOLEDB_STARTTRANSACTION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_FSRECOVER_UNCONDITIONALUNDO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_GETRMINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_HADR_LEASE_MECHANISM','Preemptive',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_HTTP_EVENT_WAIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_HTTP_REQUEST','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_LOCKMONITOR','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_MSS_RELEASE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_ODBCOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLE_UNINIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_ABORTORCOMMITTRAN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_ABORTTRAN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_GETDATASOURCE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_GETLITERALINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_GETPROPERTIES','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_GETPROPERTYINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_GETSCHEMALOCK','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_JOINTRANSACTION','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_RELEASE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDB_SETPROPERTIES','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OLEDBOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_ACCEPTSECURITYCONTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_ACQUIRECREDENTIALSHANDLE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_AUTHENTICATIONOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_AUTHORIZATIONOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_AUTHZGETINFORMATIONFROMCONTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_AUTHZINITIALIZECONTEXTFROMSID','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_AUTHZINITIALIZERESOURCEMANAGER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_BACKUPREAD','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CLOSEHANDLE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CLUSTEROPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_COMOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_COMPLETEAUTHTOKEN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_COPYFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CREATEDIRECTORY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CREATEFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CRYPTACQUIRECONTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CRYPTIMPORTKEY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_CRYPTOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DECRYPTMESSAGE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DELETEFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DELETESECURITYCONTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DEVICEIOCONTROL','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DEVICEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DIRSVC_NETWORKOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DISCONNECTNAMEDPIPE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DOMAINSERVICESOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DSGETDCNAME','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_DTCOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_ENCRYPTMESSAGE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FILEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FINDFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FLUSHFILEBUFFERS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FORMATMESSAGE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FREECREDENTIALSHANDLE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_FREELIBRARY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GENERICOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETADDRINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETCOMPRESSEDFILESIZE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETDISKFREESPACE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETFILEATTRIBUTES','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETFILESIZE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETFINALFILEPATHBYHANDLE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETLONGPATHNAME','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETPROCADDRESS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETVOLUMENAMEFORVOLUMEMOUNTPOINT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_GETVOLUMEPATHNAME','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_INITIALIZESECURITYCONTEXT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_LIBRARYOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_LOADLIBRARY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_LOGONUSER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_LOOKUPACCOUNTSID','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_MESSAGEQUEUEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_MOVEFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETGROUPGETUSERS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETLOCALGROUPGETMEMBERS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETUSERGETGROUPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETUSERGETLOCALGROUPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETUSERMODALSGET','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICYFREE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_OPENDIRECTORY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_PDH_WMI_INIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_PIPEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_PROCESSOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_QUERYREGISTRY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_QUERYSECURITYCONTEXTTOKEN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_REMOVEDIRECTORY','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_REPORTEVENT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_REVERTTOSELF','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_RSFXDEVICEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SECURITYOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SERVICEOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SETENDOFFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SETFILEPOINTER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SETFILEVALIDDATA','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SETNAMEDSECURITYINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SQLCLROPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_SQMLAUNCH','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_VERIFYSIGNATURE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_VERIFYTRUST','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_VSSOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_WAITFORSINGLEOBJECT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_WINSOCKOPS','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_WRITEFILE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_WRITEFILEGATHER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_OS_WSASETLASTERROR','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_REENLIST','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_RESIZELOG','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_ROLLFORWARDREDO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_ROLLFORWARDUNDO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SB_STOPENDPOINT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SERVER_STARTUP','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SETRMINFO','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SHAREDMEM_GETDATA','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SNIOPEN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SOSHOST','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SOSTESTING','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_SP_SERVER_DIAGNOSTICS','Preemptive',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_STARTRM','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_STREAMFCB_CHECKPOINT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_STREAMFCB_RECOVER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_STRESSDRIVER','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_TESTING','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_TRANSIMPORT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_UNMARSHALPROPAGATIONTOKEN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_VSS_CREATESNAPSHOT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_VSS_CREATEVOLUMESNAPSHOT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_CALLBACKEXECUTE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_CX_FILE_OPEN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_CX_HTTP_CALL','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_DISPATCHER','Preemptive',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_ENGINEINIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_GETTARGETSTATE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_SESSIONCOMMIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_TARGETFINALIZE','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_TARGETINIT','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XE_TIMERRUN','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PREEMPTIVE_XETESTING','Preemptive',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_ACTION_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_CLUSTER_INTEGRATION','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_FAILOVER_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_JOIN','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_OFFLINE_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_ONLINE_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_POST_ONLINE_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_SERVER_READY_CONNECTIONS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADR_WORKITEM_COMPLETED','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_HADRSIM','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC','Full Text Search',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('QDS_ASYNC_QUEUE','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('QDS_PERSIST_TASK_MAIN_LOOP_SLEEP','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('QDS_SHUTDOWN_QUEUE','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('QUERY_TRACEOUT','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REDO_THREAD_PENDING_WORK','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_CACHE_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_HISTORYCACHE_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_SCHEMA_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_TRANFSINFO_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_TRANHASHTABLE_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPL_TRANTEXTINFO_ACCESS','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REPLICA_WRITES','Replication',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('REQUEST_FOR_DEADLOCK_SEARCH','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('RESERVED_MEMORY_ALLOCATION_EXT','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('RESOURCE_SEMAPHORE','Memory',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('RESOURCE_SEMAPHORE_QUERY_COMPILE','Compilation',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_BPOOL_FLUSH','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_BUFFERPOOL_HELPLW','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_DBSTARTUP','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_DCOMSTARTUP','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_MASTERDBREADY','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_MASTERMDREADY','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_MASTERUPGRADED','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_MEMORYPOOL_ALLOCATEPAGES','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_MSDBSTARTUP','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_RETRY_VIRTUALALLOC','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_SYSTEMTASK','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_TASK','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_TEMPDBSTARTUP','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SLEEP_WORKSPACE_ALLOCATEPAGE','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SOS_SCHEDULER_YIELD','CPU',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SOS_WORK_DISPATCHER','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SP_SERVER_DIAGNOSTICS_SLEEP','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLCLR_APPDOMAIN','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLCLR_ASSEMBLY','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLCLR_DEADLOCK_DETECTION','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLCLR_QUANTUM_PUNISHMENT','SQL CLR',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_BUFFER_FLUSH','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_FILE_BUFFER','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_FILE_READ_IO_COMPLETION','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_FILE_WRITE_IO_COMPLETION','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_INCREMENTAL_FLUSH_SLEEP','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_PENDING_BUFFER_WRITERS','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_SHUTDOWN','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('SQLTRACE_WAIT_ENTRIES','Idle',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('THREADPOOL','Worker Thread',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRACE_EVTNOTIF','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRACEWRITE','Tracing',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_DT','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_EX','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_KP','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_NL','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_SH','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRAN_MARKLATCH_UP','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('TRANSACTION_MUTEX','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('UCS_SESSION_REGISTRATION','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('WAIT_FOR_RESULTS','User Wait',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('WAIT_XTP_OFFLINE_CKPT_NEW_LOG','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('WAITFOR','User Wait',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('WRITE_COMPLETION','Other Disk IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('WRITELOG','Tran Log IO',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XACT_OWN_TRANSACTION','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XACT_RECLAIM_SESSION','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XACTLOCKINFO','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XACTWORKSPACE_MUTEX','Transaction',0);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XE_DISPATCHER_WAIT','Idle',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XE_LIVE_TARGET_TVF','Other',1);
			INSERT INTO ##WaitCategories(WaitType, WaitCategory, Ignorable) VALUES ('XE_TIMER_EVENT','Idle',1);
		END; /* IF SELECT SUM(1) FROM ##WaitCategories <> 527 */



    IF OBJECT_ID('tempdb..#MasterFiles') IS NOT NULL
        DROP TABLE #MasterFiles;
    CREATE TABLE #MasterFiles (database_id INT, file_id INT, type_desc NVARCHAR(50), name NVARCHAR(255), physical_name NVARCHAR(255), size BIGINT);
    /* Azure SQL Database doesn't have sys.master_files, so we have to build our own. */
    IF ((SERVERPROPERTY('Edition')) = 'SQL Azure' 
         AND (OBJECT_ID('sys.master_files') IS NULL))
        SET @StringToExecute = 'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT DB_ID(), file_id, type_desc, name, physical_name, size FROM sys.database_files;';
    ELSE
        SET @StringToExecute = 'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT database_id, file_id, type_desc, name, physical_name, size FROM sys.master_files;';
    EXEC(@StringToExecute);

    IF @FilterPlansByDatabase IS NOT NULL
        BEGIN
        IF UPPER(LEFT(@FilterPlansByDatabase,4)) = 'USER'
            BEGIN
            INSERT INTO #FilterPlansByDatabase (DatabaseID)
            SELECT database_id
                FROM sys.databases
                WHERE [name] NOT IN ('master', 'model', 'msdb', 'tempdb');
            END;
        ELSE
            BEGIN
            SET @FilterPlansByDatabase = @FilterPlansByDatabase + ','
            ;WITH a AS
                (
                SELECT CAST(1 AS BIGINT) f, CHARINDEX(',', @FilterPlansByDatabase) t, 1 SEQ
                UNION ALL
                SELECT t + 1, CHARINDEX(',', @FilterPlansByDatabase, t + 1), SEQ + 1
                FROM a
                WHERE CHARINDEX(',', @FilterPlansByDatabase, t + 1) > 0
                )
            INSERT #FilterPlansByDatabase (DatabaseID)
                SELECT DISTINCT db.database_id
                FROM a
                INNER JOIN sys.databases db ON LTRIM(RTRIM(SUBSTRING(@FilterPlansByDatabase, a.f, a.t - a.f))) = db.name
                WHERE SUBSTRING(@FilterPlansByDatabase, f, t - f) IS NOT NULL
                OPTION (MAXRECURSION 0);
            END;
        END;

	IF OBJECT_ID('tempdb..#ReadableDBs') IS NOT NULL 
		DROP TABLE #ReadableDBs;
	CREATE TABLE #ReadableDBs (
	database_id INT
	);

	IF EXISTS (SELECT * FROM sys.all_objects o WHERE o.name = 'dm_hadr_database_replica_states')
    BEGIN
		RAISERROR('Checking for Read intent databases to exclude',0,0) WITH NOWAIT;

        SET @StringToExecute = 'INSERT INTO #ReadableDBs (database_id) SELECT DBs.database_id FROM sys.databases DBs INNER JOIN sys.availability_replicas Replicas ON DBs.replica_id = Replicas.replica_id WHERE replica_server_name NOT IN (SELECT DISTINCT primary_replica FROM sys.dm_hadr_availability_group_states States) AND Replicas.secondary_role_allow_connections_desc = ''READ_ONLY'' AND replica_server_name = @@SERVERNAME;';
        EXEC(@StringToExecute);
		
	END

    DECLARE	@v DECIMAL(6,2),
		@build INT,
		@memGrantSortSupported BIT = 1;

	RAISERROR (N'Determining SQL Server version.',0,1) WITH NOWAIT;

	INSERT INTO #checkversion (version)
	SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))
	OPTION (RECOMPILE);


	SELECT @v = common_version ,
			@build = build
	FROM   #checkversion
	OPTION (RECOMPILE);

	IF (@v < 11)
	OR (@v = 11 AND @build < 6020) 
	OR (@v = 12 AND @build < 5000) 
	OR (@v = 13 AND @build < 1601)
		SET @memGrantSortSupported = 0;

	IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_exec_query_statistics_xml')
		AND ((@v = 13 AND @build >= 5337)	/* This DMF causes assertion errors: https://support.microsoft.com/en-us/help/4490136/fix-assertion-error-occurs-when-you-use-sys-dm-exec-query-statistics-x */
				OR (@v = 14 AND @build >= 3162) 
				OR (@v >= 15)
				OR (@v <= 12)) /* Azure */
		SET @dm_exec_query_statistics_xml = 1;


    SET @StockWarningHeader = '<?ClickToSeeCommmand -- ' + @LineFeed + @LineFeed
        + 'WARNING: Running this command may result in data loss or an outage.' + @LineFeed
        + 'This tool is meant as a shortcut to help generate scripts for DBAs.' + @LineFeed
        + 'It is not a substitute for database training and experience.' + @LineFeed
        + 'Now, having said that, here''s the details:' + @LineFeed + @LineFeed;

    SELECT @StockWarningFooter = @StockWarningFooter + @LineFeed + @LineFeed + '-- ?>',
           @StockDetailsHeader = @StockDetailsHeader + '<?ClickToSeeDetails -- ' + @LineFeed,
           @StockDetailsFooter = @StockDetailsFooter + @LineFeed + ' -- ?>';

    /* Get the instance name to use as a Perfmon counter prefix. */
    IF CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) = 'SQL Azure'
        SELECT TOP 1 @ServiceName = LEFT(object_name, (CHARINDEX(':', object_name) - 1))
        FROM sys.dm_os_performance_counters;
    ELSE
        BEGIN
        SET @StringToExecute = 'INSERT INTO #PerfmonStats(object_name, Pass, SampleTime, counter_name, cntr_type) SELECT CASE WHEN @@SERVICENAME = ''MSSQLSERVER'' THEN ''SQLServer'' ELSE ''MSSQL$'' + @@SERVICENAME END, 0, SYSDATETIMEOFFSET(), ''stuffing'', 0 ;';
        EXEC(@StringToExecute);
        SELECT @ServiceName = object_name FROM #PerfmonStats;
        DELETE #PerfmonStats;
        END;

    /* Build a list of queries that were run in the last 10 seconds.
       We're looking for the death-by-a-thousand-small-cuts scenario
       where a query is constantly running, and it doesn't have that
       big of an impact individually, but it has a ton of impact
       overall. We're going to build this list, and then after we
       finish our @Seconds sample, we'll compare our plan cache to
       this list to see what ran the most. */

    /* Populate #QueryStats. SQL 2005 doesn't have query hash or query plan hash. */
    IF @CheckProcedureCache = 1 
	BEGIN
		RAISERROR('@CheckProcedureCache = 1, capturing first pass of plan cache',10,1) WITH NOWAIT;
		IF @@VERSION LIKE 'Microsoft SQL Server 2005%'
			BEGIN
			IF @FilterPlansByDatabase IS NULL
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()));';
				END;
			ELSE
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
												CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
												INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()))
												AND attr.attribute = ''dbid'';';
				END;
			END;
		ELSE
			BEGIN
			IF @FilterPlansByDatabase IS NULL
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()));';
				END;
			ELSE
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
											INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()))
												AND attr.attribute = ''dbid'';';
				END;
			END;
		EXEC(@StringToExecute);

		/* Get the totals for the entire plan cache */
		INSERT INTO #QueryStats (Pass, SampleTime, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time)
		SELECT -1 AS Pass, SYSDATETIMEOFFSET(), SUM(execution_count), SUM(total_worker_time), SUM(total_physical_reads), SUM(total_logical_writes), SUM(total_logical_reads), SUM(total_clr_time), SUM(total_elapsed_time), MIN(creation_time)
			FROM sys.dm_exec_query_stats qs;
    END; /*IF @CheckProcedureCache = 1 */


    IF EXISTS (SELECT *
                    FROM tempdb.sys.all_objects obj
                    INNER JOIN tempdb.sys.all_columns col1 ON obj.object_id = col1.object_id AND col1.name = 'object_name'
                    INNER JOIN tempdb.sys.all_columns col2 ON obj.object_id = col2.object_id AND col2.name = 'counter_name'
                    INNER JOIN tempdb.sys.all_columns col3 ON obj.object_id = col3.object_id AND col3.name = 'instance_name'
                    WHERE obj.name LIKE '%CustomPerfmonCounters%')
        BEGIN
        SET @StringToExecute = 'INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) SELECT [object_name],[counter_name],[instance_name] FROM #CustomPerfmonCounters';
        EXEC(@StringToExecute);
        END;
    ELSE
        BEGIN
        /* Add our default Perfmon counters */
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Forwarded Records/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Page compression attempts/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Page Splits/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Skipped Ghosted Records/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Table Lock Escalations/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Worktables Created/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Group','Active Hadr Threads','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Bytes Received from Replica/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Bytes Sent to Replica/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Bytes Sent to Transport/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Flow Control Time (ms/sec)','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Flow Control/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Resent Messages/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Availability Replica','Sends to Replica/sec','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Page life expectancy', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Page reads/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Page writes/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Readahead pages/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Target pages', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Total pages', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Active Transactions','_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Database Flow Control Delay', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Database Flow Controls/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Group Commit Time', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Group Commits/Sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Log Apply Pending Queue', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Log Apply Ready Queue', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Log Compression Cache misses/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Log remaining for undo', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Log Send Queue', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Recovery Queue', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Redo blocked/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Redo Bytes Remaining', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Database Replica','Redone Bytes/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Log Bytes Flushed/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Log Growths', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Log Pool LogWriter Pushes/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Log Shrinks', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Transactions/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','Write Transactions/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Databases','XTP Memory Used (KB)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Exec Statistics','Distributed Query', 'Execs in progress');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Exec Statistics','DTC calls', 'Execs in progress');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Exec Statistics','Extended Procedures', 'Execs in progress');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Exec Statistics','OLEDB calls', 'Execs in progress');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','Active Temp Tables', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','Logins/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','Logouts/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','Mars Deadlocks', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','Processes blocked', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Number of Deadlocks/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Memory Manager','Memory Grants Pending', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Errors','Errors/sec', '_Total');
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Batch Requests/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Forced Parameterizations/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Guided plan executions/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','SQL Attention rate', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','SQL Compilations/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','SQL Re-Compilations/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Workload Group Stats','Query optimizations/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Workload Group Stats','Suboptimal plans/sec',NULL);
        /* Below counters added by Jefferson Elias */
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Worktables From Cache Base',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Worktables From Cache Ratio',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Database pages',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Free pages',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Stolen pages',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Memory Manager','Granted Workspace Memory (KB)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Memory Manager','Maximum Workspace Memory (KB)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Memory Manager','Target Server Memory (KB)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Memory Manager','Total Server Memory (KB)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Buffer cache hit ratio',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Buffer cache hit ratio base',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Checkpoint pages/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Free list stalls/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Lazy writes/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Auto-Param Attempts/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Failed Auto-Params/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Safe Auto-Params/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','Unsafe Auto-Params/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Workfiles Created/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':General Statistics','User Connections',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Latches','Average Latch Wait Time (ms)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Latches','Average Latch Wait Time Base',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Latches','Latch Waits/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Latches','Total Latch Wait Time (ms)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Average Wait Time (ms)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Average Wait Time Base',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Lock Requests/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Lock Timeouts/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Lock Wait Time (ms)',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Locks','Lock Waits/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Transactions','Longest Transaction Running Time',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Full Scans/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Access Methods','Index Searches/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Buffer Manager','Page lookups/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':Cursor Manager by Type','Active cursors',NULL);
        /* Below counters are for In-Memory OLTP (Hekaton), which have a different naming convention.
           And yes, they actually hard-coded the version numbers into the counters, and SQL 2019 still says 2017, oddly.
           For why, see: https://connect.microsoft.com/SQLServer/feedback/details/817216/xtp-perfmon-counters-should-appear-under-sql-server-perfmon-counter-group
        */
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Cursors','Expired rows removed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Cursors','Expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Garbage Collection','Rows processed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP IO Governor','Io Issued/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Phantom Processor','Phantom expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Phantom Processor','Phantom rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Transaction Log','Log bytes written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Transaction Log','Log records written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Transactions','Transactions aborted by user/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Transactions','Transactions aborted/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2014 XTP Transactions','Transactions created/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Cursors','Expired rows removed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Cursors','Expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Garbage Collection','Rows processed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP IO Governor','Io Issued/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Phantom Processor','Phantom expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Phantom Processor','Phantom rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Transaction Log','Log bytes written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Transaction Log','Log records written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Transactions','Transactions aborted by user/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Transactions','Transactions aborted/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2016 XTP Transactions','Transactions created/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Cursors','Expired rows removed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Cursors','Expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Garbage Collection','Rows processed/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP IO Governor','Io Issued/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Phantom Processor','Phantom expired rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Phantom Processor','Phantom rows touched/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Transaction Log','Log bytes written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Transaction Log','Log records written/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Transactions','Transactions aborted by user/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Transactions','Transactions aborted/sec',NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES ('SQL Server 2017 XTP Transactions','Transactions created/sec',NULL);
        END;


    IF @total_cpu_usage IN (0, 1)
	BEGIN
	    EXEC sys.sp_executesql
		    @get_thread_time_ms,
			N'@thread_time_ms FLOAT OUTPUT',
			@thread_time_ms OUTPUT;		    
	END

    /* Populate #FileStats, #PerfmonStats, #WaitStats with DMV data.
        After we finish doing our checks, we'll take another sample and compare them. */
	RAISERROR('Capturing first pass of wait stats, perfmon counters, file stats',10,1) WITH NOWAIT;
	SET @StringToExecute = N'
		INSERT #WaitStats(Pass, SampleTime, wait_type, wait_time_ms, thread_time_ms, signal_wait_time_ms, waiting_tasks_count)
			SELECT 
			x.Pass, 
			x.SampleTime, 
			x.wait_type, 
			SUM(x.sum_wait_time_ms) AS sum_wait_time_ms, 
			CASE @Seconds
			     WHEN 0
			     THEN 0
			     ELSE @thread_time_ms
			END AS thread_time_ms,
			SUM(x.sum_signal_wait_time_ms) AS sum_signal_wait_time_ms, 
			SUM(x.sum_waiting_tasks) AS sum_waiting_tasks
			FROM (
			SELECT  
					1 AS Pass,
					CASE @Seconds WHEN 0 THEN @StartSampleTime ELSE SYSDATETIMEOFFSET() END AS SampleTime,
					owt.wait_type,
					CASE @Seconds WHEN 0 THEN 0 ELSE SUM(owt.wait_duration_ms) OVER (PARTITION BY owt.wait_type, owt.session_id)
						 - CASE WHEN @Seconds = 0 THEN 0 ELSE (@Seconds * 1000) END END AS sum_wait_time_ms,
					0 AS sum_signal_wait_time_ms,
					0 AS sum_waiting_tasks
				FROM    sys.dm_os_waiting_tasks owt
				WHERE owt.session_id > 50
				AND owt.wait_duration_ms >= CASE @Seconds WHEN 0 THEN 0 ELSE @Seconds * 1000 END
			UNION ALL
			SELECT
				   1 AS Pass,
				   CASE @Seconds WHEN 0 THEN @StartSampleTime ELSE SYSDATETIMEOFFSET() END AS SampleTime,
				   os.wait_type,
				   CASE @Seconds WHEN 0 THEN 0 ELSE SUM(os.wait_time_ms) OVER (PARTITION BY os.wait_type) END AS sum_wait_time_ms,
				   CASE @Seconds WHEN 0 THEN 0 ELSE SUM(os.signal_wait_time_ms) OVER (PARTITION BY os.wait_type ) END AS sum_signal_wait_time_ms,
				   CASE @Seconds WHEN 0 THEN 0 ELSE SUM(os.waiting_tasks_count) OVER (PARTITION BY os.wait_type) END AS sum_waiting_tasks ';

	IF SERVERPROPERTY('Edition') = 'SQL Azure'
		SET @StringToExecute = @StringToExecute + N' FROM sys.dm_db_wait_stats os ';
	ELSE
		SET @StringToExecute = @StringToExecute + N' FROM sys.dm_os_wait_stats os ';

	SET @StringToExecute = @StringToExecute + N'
		) x
		   WHERE NOT EXISTS 
		   (
                SELECT *
				FROM ##WaitCategories AS wc
				WHERE wc.WaitType = x.wait_type
				AND wc.Ignorable = 1
		   )
		GROUP BY x.Pass, x.SampleTime, x.wait_type
		ORDER BY sum_wait_time_ms DESC;'
		
		EXEC sys.sp_executesql
	        @StringToExecute,
          N'@StartSampleTime DATETIMEOFFSET,
	        @Seconds INT,
	        @thread_time_ms FLOAT',
	        @StartSampleTime,
	        @Seconds,
	        @thread_time_ms;
		
    WITH w AS
	(
	    SELECT
		    total_waits =
			    CONVERT
				(
				    FLOAT,
			        SUM(ws.wait_time_ms)
				)
		FROM #WaitStats AS ws
		WHERE Pass = 1
	)
    UPDATE ws
	    SET	ws.thread_time_ms += w.total_waits
	FROM #WaitStats AS ws
	CROSS JOIN w
	WHERE ws.Pass = 1
	OPTION(RECOMPILE);
	
    INSERT INTO #FileStats (Pass, SampleTime, DatabaseID, FileID, DatabaseName, FileLogicalName, SizeOnDiskMB, io_stall_read_ms ,
        num_of_reads, [bytes_read] , io_stall_write_ms,num_of_writes, [bytes_written], PhysicalName, TypeDesc)
    SELECT
        1 AS Pass,
        CASE @Seconds WHEN 0 THEN @StartSampleTime ELSE SYSDATETIMEOFFSET() END AS SampleTime,
        mf.[database_id],
        mf.[file_id],
        DB_NAME(vfs.database_id) AS [db_name],
        mf.name + N' [' + mf.type_desc COLLATE SQL_Latin1_General_CP1_CI_AS + N']' AS file_logical_name ,
        CAST(( ( vfs.size_on_disk_bytes / 1024.0 ) / 1024.0 ) AS INT) AS size_on_disk_mb ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.io_stall_read_ms END ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.num_of_reads END ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.[num_of_bytes_read] END ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.io_stall_write_ms END ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.num_of_writes END ,
        CASE @Seconds WHEN 0 THEN 0 ELSE vfs.[num_of_bytes_written] END ,
        mf.physical_name,
        mf.type_desc
    FROM sys.dm_io_virtual_file_stats (NULL, NULL) AS vfs
    INNER JOIN #MasterFiles AS mf ON vfs.file_id = mf.file_id
        AND vfs.database_id = mf.database_id
    WHERE vfs.num_of_reads > 0
        OR vfs.num_of_writes > 0;

    INSERT INTO #PerfmonStats (Pass, SampleTime, [object_name],[counter_name],[instance_name],[cntr_value],[cntr_type])
    SELECT         1 AS Pass,
        CASE @Seconds WHEN 0 THEN @StartSampleTime ELSE SYSDATETIMEOFFSET() END AS SampleTime, RTRIM(dmv.object_name), RTRIM(dmv.counter_name), RTRIM(dmv.instance_name), CASE @Seconds WHEN 0 THEN 0 ELSE dmv.cntr_value END, dmv.cntr_type
        FROM #PerfmonCounters counters
        INNER JOIN sys.dm_os_performance_counters dmv ON counters.counter_name COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.counter_name) COLLATE SQL_Latin1_General_CP1_CI_AS
            AND counters.[object_name] COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.[object_name]) COLLATE SQL_Latin1_General_CP1_CI_AS
            AND (counters.[instance_name] IS NULL OR counters.[instance_name] COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.[instance_name]) COLLATE SQL_Latin1_General_CP1_CI_AS);

	/* For Github #2743: */
	CREATE TABLE #TempdbOperationalStats (object_id BIGINT PRIMARY KEY CLUSTERED,
		forwarded_fetch_count BIGINT);
	INSERT INTO #TempdbOperationalStats (object_id, forwarded_fetch_count)
		SELECT object_id, forwarded_fetch_count
		FROM tempdb.sys.dm_db_index_operational_stats(DB_ID('tempdb'), NULL, NULL, NULL) os
	WHERE os.database_id = DB_ID('tempdb')
		AND os.forwarded_fetch_count > 100;


	/* If they want to run sp_BlitzWho and export to table, go for it. */
	IF @OutputTableNameBlitzWho IS NOT NULL
		AND @OutputDatabaseName IS NOT NULL
		AND @OutputSchemaName IS NOT NULL
		AND EXISTS ( SELECT *
						FROM   sys.databases
						WHERE  QUOTENAME([name]) = @OutputDatabaseName)
	BEGIN
		RAISERROR('Logging sp_BlitzWho to table',10,1) WITH NOWAIT;
		EXEC sp_BlitzWho @OutputDatabaseName = @UnquotedOutputDatabaseName, @OutputSchemaName = @UnquotedOutputSchemaName, @OutputTableName = @OutputTableNameBlitzWho, @CheckDateOverride = @StartSampleTime, @OutputTableRetentionDays = @OutputTableRetentionDays;
	END

	RAISERROR('Beginning investigatory queries',10,1) WITH NOWAIT;


    /* Maintenance Tasks Running - Backup Running - CheckID 1 */
    IF @Seconds > 0
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 1',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, QueryHash)
		SELECT 1 AS CheckID,
		    1 AS Priority,
		    'Maintenance Tasks Running' AS FindingGroup,
		    'Backup Running' AS Finding,
		    'https://www.brentozar.com/askbrent/backups/' AS URL,
		    'Backup of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM #MasterFiles WHERE database_id = db.resource_database_id) + 'GB) ' + @LineFeed
		        + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' + @LineFeed
			    + CASE WHEN COALESCE(s.nt_username, s.loginame) IS NOT NULL THEN (' Login: ' + COALESCE(s.nt_username, s.loginame) + ' ') ELSE '' END AS Details,
		    'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' AS HowToStopIt,
		    pl.query_plan AS QueryPlan,
		    r.start_time AS StartTime,
		    s.loginame AS LoginName,
		    s.nt_username AS NTUserName,
		    s.[program_name] AS ProgramName,
		    s.[hostname] AS HostName,
		    db.[resource_database_id] AS DatabaseID,
		    DB_NAME(db.resource_database_id) AS DatabaseName,
		    0 AS OpenTransactionCount,
		    r.query_hash
		FROM sys.dm_exec_requests r
		INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
		INNER JOIN sys.sysprocesses AS s ON r.session_id = s.spid AND s.ecid = 0
		INNER JOIN
		(
		    SELECT DISTINCT
			    t.request_session_id,
				t.resource_database_id
		    FROM sys.dm_tran_locks AS t
		    WHERE t.resource_type = N'DATABASE'
		    AND   t.request_mode = N'S'
		    AND   t.request_status = N'GRANT'
		    AND   t.request_owner_type = N'SHARED_TRANSACTION_WORKSPACE'
		) AS db ON s.spid = db.request_session_id AND s.dbid = db.resource_database_id
		CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
		WHERE r.command LIKE 'BACKUP%'
		AND r.start_time <= DATEADD(minute, -5, GETDATE())
		AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);
	END

    /* If there's a backup running, add details explaining how long full backup has been taking in the last month. */
    IF @Seconds > 0 AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) <> 'SQL Azure'
    BEGIN
        SET @StringToExecute = 'UPDATE #BlitzFirstResults SET Details = Details + '' Over the last 60 days, the full backup usually takes '' + CAST((SELECT AVG(DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date)) FROM msdb.dbo.backupset bs WHERE abr.DatabaseName = bs.database_name AND bs.type = ''D'' AND bs.backup_start_date > DATEADD(dd, -60, SYSDATETIMEOFFSET()) AND bs.backup_finish_date IS NOT NULL) AS NVARCHAR(100)) + '' minutes.'' FROM #BlitzFirstResults abr WHERE abr.CheckID = 1 AND EXISTS (SELECT * FROM msdb.dbo.backupset bs WHERE bs.type = ''D'' AND bs.backup_start_date > DATEADD(dd, -60, SYSDATETIMEOFFSET()) AND bs.backup_finish_date IS NOT NULL AND abr.DatabaseName = bs.database_name AND DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date) > 1)';
        EXEC(@StringToExecute);
    END;


    /* Maintenance Tasks Running - DBCC CHECK* Running - CheckID 2 */
    IF @Seconds > 0 AND EXISTS(SELECT * FROM sys.dm_exec_requests WHERE command LIKE 'DBCC%')
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 2',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, QueryHash)
		SELECT 2 AS CheckID,
		    1 AS Priority,
		    'Maintenance Tasks Running' AS FindingGroup,
		    'DBCC CHECK* Running' AS Finding,
		    'https://www.brentozar.com/askbrent/dbcc/' AS URL,
		    'Corruption check of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM #MasterFiles WHERE database_id = db.resource_database_id) + 'GB) has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
		    'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' AS HowToStopIt,
		    pl.query_plan AS QueryPlan,
		    r.start_time AS StartTime,
		    s.login_name AS LoginName,
		    s.nt_user_name AS NTUserName,
		    s.[program_name] AS ProgramName,
		    s.[host_name] AS HostName,
		    db.[resource_database_id] AS DatabaseID,
		    DB_NAME(db.resource_database_id) AS DatabaseName,
		    0 AS OpenTransactionCount,
		    r.query_hash
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
		OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) pl
		OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
		WHERE r.command LIKE 'DBCC%'
		AND CAST(t.text AS NVARCHAR(4000)) NOT LIKE '%dm_db_index_physical_stats%'
		AND CAST(t.text AS NVARCHAR(4000)) NOT LIKE '%ALTER INDEX%'
		AND CAST(t.text AS NVARCHAR(4000)) NOT LIKE '%fileproperty%'
		AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);
	END

    /* Maintenance Tasks Running - Restore Running - CheckID 3 */
    IF @Seconds > 0
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 3',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, QueryHash)
		SELECT 3 AS CheckID,
		    1 AS Priority,
		    'Maintenance Tasks Running' AS FindingGroup,
		    'Restore Running' AS Finding,
		    'https://www.brentozar.com/askbrent/backups/' AS URL,
		    'Restore of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM #MasterFiles WHERE database_id = db.resource_database_id) + 'GB) is ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
		    'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' AS HowToStopIt,
		    pl.query_plan AS QueryPlan,
		    r.start_time AS StartTime,
		    s.login_name AS LoginName,
		    s.nt_user_name AS NTUserName,
		    s.[program_name] AS ProgramName,
		    s.[host_name] AS HostName,
		    db.[resource_database_id] AS DatabaseID,
		    DB_NAME(db.resource_database_id) AS DatabaseName,
		    0 AS OpenTransactionCount,
		    r.query_hash
		FROM sys.dm_exec_requests r
		INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
		INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
		INNER JOIN (
		SELECT DISTINCT request_session_id, resource_database_id
		FROM    sys.dm_tran_locks
		WHERE resource_type = N'DATABASE'
		AND     request_mode = N'S'
		AND     request_status = N'GRANT') AS db ON s.session_id = db.request_session_id
		CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
		WHERE r.command LIKE 'RESTORE%'
		AND s.program_name <> 'SQL Server Log Shipping'
		AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);
	END

    /* SQL Server Internal Maintenance - Database File Growing - CheckID 4 */
    IF @Seconds > 0
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 4',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
		SELECT 4 AS CheckID,
		    1 AS Priority,
		    'SQL Server Internal Maintenance' AS FindingGroup,
		    'Database File Growing' AS Finding,
		    'https://www.brentozar.com/go/instant' AS URL,
		    'SQL Server is waiting for Windows to provide storage space for a database restore, a data file growth, or a log file growth. This task has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '.' + @LineFeed + 'Check the query plan (expert mode) to identify the database involved.' AS Details,
		    'Unfortunately, you can''t stop this, but you can prevent it next time. Check out https://www.brentozar.com/go/instant for details.' AS HowToStopIt,
		    pl.query_plan AS QueryPlan,
		    r.start_time AS StartTime,
		    s.login_name AS LoginName,
		    s.nt_user_name AS NTUserName,
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
		AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);
	END

    /* Query Problems - Long-Running Query Blocking Others - CheckID 5 */
    IF SERVERPROPERTY('Edition') <> 'SQL Azure' AND @Seconds > 0 AND EXISTS(SELECT * FROM sys.dm_os_waiting_tasks WHERE wait_type LIKE 'LCK%' AND wait_duration_ms > 30000)
    BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 5',10,1) WITH NOWAIT;
		END

        SET @StringToExecute = N'INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, QueryHash)
            SELECT 5 AS CheckID,
                1 AS Priority,
                ''Query Problems'' AS FindingGroup,
                ''Long-Running Query Blocking Others'' AS Finding,
                ''https://www.brentozar.com/go/blocking'' AS URL,
                ''Query in '' + COALESCE(DB_NAME(COALESCE((SELECT TOP 1 dbid FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 t.dbid FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) t WHERE spBlocker.spid = tBlocked.blocking_session_id))), ''(Unknown)'') + '' has a last request start time of '' + CAST(s.last_request_start_time AS NVARCHAR(100)) + ''. Query follows: ' 
					+ @LineFeed + @LineFeed + 
					'''+ CAST(COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id), '''') AS NVARCHAR(2000)) AS Details,
                ''KILL '' + CAST(tBlocked.blocking_session_id AS NVARCHAR(100)) + '';'' AS HowToStopIt,
                (SELECT TOP 1 query_plan FROM sys.dm_exec_query_plan(r.plan_handle)) AS QueryPlan,
                COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id)) AS QueryText,
                r.start_time AS StartTime,
                s.login_name AS LoginName,
                s.nt_user_name AS NTUserName,
                s.[program_name] AS ProgramName,
                s.[host_name] AS HostName,
                r.[database_id] AS DatabaseID,
                DB_NAME(r.database_id) AS DatabaseName,
                0 AS OpenTransactionCount,
                r.query_hash
            FROM sys.dm_os_waiting_tasks tBlocked
	        INNER JOIN sys.dm_exec_sessions s ON tBlocked.blocking_session_id = s.session_id
            LEFT OUTER JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
            INNER JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
            WHERE tBlocked.wait_type LIKE ''LCK%'' AND tBlocked.wait_duration_ms > 30000
              /* And the blocking session ID is not blocked by anyone else: */
              AND NOT EXISTS(SELECT * FROM sys.dm_os_waiting_tasks tBlocking WHERE s.session_id = tBlocking.session_id AND tBlocking.session_id <> tBlocking.blocking_session_id AND tBlocking.blocking_session_id IS NOT NULL)
			  AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);';
		EXECUTE sp_executesql @StringToExecute;
    END;
	
    /* Query Problems - Plan Cache Erased Recently - CheckID 7 */
    IF DATEADD(mi, -15, SYSDATETIME()) < (SELECT TOP 1 creation_time FROM sys.dm_exec_query_stats ORDER BY creation_time)
    BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 7',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
        SELECT TOP 1 7 AS CheckID,
            50 AS Priority,
            'Query Problems' AS FindingGroup,
            'Plan Cache Erased Recently' AS Finding,
            'https://www.brentozar.com/askbrent/plan-cache-erased-recently/' AS URL,
            'The oldest query in the plan cache was created at ' + CAST(creation_time AS NVARCHAR(50)) + '. ' + @LineFeed + @LineFeed
                + 'This indicates that someone ran DBCC FREEPROCCACHE at that time,' + @LineFeed
                + 'Giving SQL Server temporary amnesia. Now, as queries come in,' + @LineFeed
                + 'SQL Server has to use a lot of CPU power in order to build execution' + @LineFeed
                + 'plans and put them in cache again. This causes high CPU loads.' AS Details,
            'Find who did that, and stop them from doing it again.' AS HowToStopIt
        FROM sys.dm_exec_query_stats
        ORDER BY creation_time;
    END;


    /* Query Problems - Sleeping Query with Open Transactions - CheckID 8 */
    IF @Seconds > 0
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 8',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount)
		SELECT 8 AS CheckID,
		    50 AS Priority,
		    'Query Problems' AS FindingGroup,
		    'Sleeping Query with Open Transactions' AS Finding,
		    'https://www.brentozar.com/askbrent/sleeping-query-with-open-transactions/' AS URL,
		    'Database: ' + DB_NAME(db.resource_database_id) + @LineFeed + 'Host: ' + s.hostname + @LineFeed + 'Program: ' + s.[program_name] + @LineFeed + 'Asleep with open transactions and locks since ' + CAST(s.last_batch AS NVARCHAR(100)) + '. ' AS Details,
		    'KILL ' + CAST(s.spid AS NVARCHAR(100)) + ';' AS HowToStopIt,
		    s.last_batch AS StartTime,
		    s.loginame AS LoginName,
		    s.nt_username AS NTUserName,
		    s.[program_name] AS ProgramName,
		    s.hostname AS HostName,
		    db.[resource_database_id] AS DatabaseID,
		    DB_NAME(db.resource_database_id) AS DatabaseName,
		    (SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(c.most_recent_sql_handle)) AS QueryText,
		    s.open_tran AS OpenTransactionCount
		FROM sys.sysprocesses s
		INNER JOIN sys.dm_exec_connections c ON s.spid = c.session_id
		INNER JOIN (
		SELECT DISTINCT request_session_id, resource_database_id
		FROM    sys.dm_tran_locks
		WHERE resource_type = N'DATABASE'
		AND     request_mode = N'S'
		AND     request_status = N'GRANT'
		AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.spid = db.request_session_id
		WHERE s.status = 'sleeping'
		AND s.open_tran > 0
		AND s.last_batch < DATEADD(ss, -10, SYSDATETIME())
		AND EXISTS(SELECT * FROM sys.dm_tran_locks WHERE request_session_id = s.spid
		AND NOT (resource_type = N'DATABASE' AND request_mode = N'S' AND request_status = N'GRANT' AND request_owner_type = N'SHARED_TRANSACTION_WORKSPACE'));
	END

    /*Query Problems - Clients using implicit transactions - CheckID 37 */
    IF @Seconds > 0 
		AND ( @@VERSION NOT LIKE 'Microsoft SQL Server 2005%'
		AND	  @@VERSION NOT LIKE 'Microsoft SQL Server 2008%'
		AND	  @@VERSION NOT LIKE 'Microsoft SQL Server 2008 R2%' )
     BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 37',10,1) WITH NOWAIT;
		END

        SET @StringToExecute = N'INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount)
		SELECT  37 AS CheckId,
		        50 AS Priority,
		        ''Query Problems'' AS FindingsGroup,
		        ''Implicit Transactions'',
		        ''https://www.brentozar.com/go/ImplicitTransactions/'' AS URL,
		        ''Database: '' + DB_NAME(s.database_id)  + '' '' + CHAR(13) + CHAR(10) +
				''Host: '' + s.[host_name]  + '' '' + CHAR(13) + CHAR(10) +
				''Program: '' + s.[program_name]  + '' '' + CHAR(13) + CHAR(10) +
				CONVERT(NVARCHAR(10), s.open_transaction_count) + 
				'' open transactions since: '' + 
				CONVERT(NVARCHAR(30), tat.transaction_begin_time) + ''. '' 
					AS Details,
				''Run sp_BlitzWho and check the is_implicit_transaction column to spot the culprits.
If one of them is a lead blocker, consider killing that query.'' AS HowToStopit,
		        tat.transaction_begin_time,
		        s.login_name,
		        s.nt_user_name,
		        s.program_name,
		        s.host_name,
		        s.database_id,
		        DB_NAME(s.database_id) AS DatabaseName,
		        NULL AS Querytext,
		        s.open_transaction_count AS OpenTransactionCount
		FROM    sys.dm_tran_active_transactions AS tat
		LEFT JOIN sys.dm_tran_session_transactions AS tst
		ON tst.transaction_id = tat.transaction_id
		LEFT JOIN sys.dm_exec_sessions AS s
		ON s.session_id = tst.session_id
		WHERE tat.name = ''implicit_transaction'';
		'
		EXECUTE sp_executesql @StringToExecute;
    END;

    /* Query Problems - Query Rolling Back - CheckID 9 */
    IF @Seconds > 0
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 9',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, QueryHash)
		SELECT 9 AS CheckID,
		    1 AS Priority,
		    'Query Problems' AS FindingGroup,
		    'Query Rolling Back' AS Finding,
		    'https://www.brentozar.com/askbrent/rollback/' AS URL,
		    'Rollback started at ' + CAST(r.start_time AS NVARCHAR(100)) + ', is ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete.' AS Details,
		    'Unfortunately, you can''t stop this. Whatever you do, don''t restart the server in an attempt to fix it - SQL Server will keep rolling back.' AS HowToStopIt,
		    r.start_time AS StartTime,
		    s.login_name AS LoginName,
		    s.nt_user_name AS NTUserName,
		    s.[program_name] AS ProgramName,
		    s.[host_name] AS HostName,
		    db.[resource_database_id] AS DatabaseID,
		    DB_NAME(db.resource_database_id) AS DatabaseName,
		    (SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(c.most_recent_sql_handle)) AS QueryText,
		    r.query_hash
		FROM sys.dm_exec_sessions s
		INNER JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
		INNER JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
		LEFT OUTER JOIN (
		    SELECT DISTINCT request_session_id, resource_database_id
		    FROM    sys.dm_tran_locks
		    WHERE resource_type = N'DATABASE'
		    AND     request_mode = N'S'
		    AND     request_status = N'GRANT'
		    AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
		WHERE r.status = 'rollback';
	END

	IF @Seconds > 0
	BEGIN
    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
    SELECT
        47 AS CheckId,
    	50 AS Priority,
    	'Query Problems' AS FindingsGroup,
    	'High Percentage Of Runnable Queries' AS Finding, 
    	'https://erikdarlingdata.com/go/RunnableQueue/' AS URL, 
    	'On the ' 
    	+ CASE WHEN y.pass = 1 
    	       THEN '1st' 
    		   ELSE '2nd'
    	   END
    	+ ' pass, '
    	+ RTRIM(y.runnable_pct)
    	+ '% of your queries were waiting to get on a CPU to run. '
    	+ ' This can indicate CPU pressure.'
    FROM
    (
        SELECT 
            1 AS pass,
            x.total, 
        	x.runnable,
            CONVERT(decimal(5,2),
                (
                    x.runnable / 
                        (1. * NULLIF(x.total, 0))
                )
            ) * 100. AS runnable_pct
        FROM 
        (
            SELECT 
                COUNT_BIG(*) AS total, 
                SUM(CASE WHEN status = 'runnable' 
        		         THEN 1 
        				 ELSE 0 
        		    END) AS runnable
            FROM sys.dm_exec_requests
            WHERE session_id > 50
        ) AS x
    ) AS y
    WHERE y.runnable_pct > 20.;
	END

    /* Server Performance - Too Much Free Memory - CheckID 34 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 34',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 34 AS CheckID,
        50 AS Priority,
        'Server Performance' AS FindingGroup,
        'Too Much Free Memory' AS Finding,
        'https://www.brentozar.com/go/freememory' AS URL,
		CAST((CAST(cFree.cntr_value AS BIGINT) / 1024 / 1024 ) AS NVARCHAR(100)) + N'GB of free memory inside SQL Server''s buffer pool,' + @LineFeed + ' which is ' + CAST((CAST(cTotal.cntr_value AS BIGINT) / 1024 / 1024) AS NVARCHAR(100)) + N'GB. You would think lots of free memory would be good, but check out the URL for more information.' AS Details,
        'Run sp_BlitzCache @SortOrder = ''memory grant'' to find queries with huge memory grants and tune them.' AS HowToStopIt
		FROM sys.dm_os_performance_counters cFree
		INNER JOIN sys.dm_os_performance_counters cTotal ON cTotal.object_name LIKE N'%Memory Manager%'
			AND cTotal.counter_name = N'Total Server Memory (KB)                                                                                                        '
		WHERE cFree.object_name LIKE N'%Memory Manager%'
			AND cFree.counter_name = N'Free Memory (KB)                                                                                                                '
			AND CAST(cFree.cntr_value AS BIGINT) > 20480000000
			AND CAST(cTotal.cntr_value AS BIGINT) * .3 <= CAST(cFree.cntr_value AS BIGINT)
            AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Standard%';

    /* Server Performance - Target Memory Lower Than Max - CheckID 35 */
	IF SERVERPROPERTY('Edition') <> 'SQL Azure'
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 35',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
		SELECT 35 AS CheckID,
			10 AS Priority,
			'Server Performance' AS FindingGroup,
			'Target Memory Lower Than Max' AS Finding,
			'https://www.brentozar.com/go/target' AS URL,
			N'Max server memory is ' + CAST(cMax.value_in_use AS NVARCHAR(50)) + N' MB but target server memory is only ' + CAST((CAST(cTarget.cntr_value AS BIGINT) / 1024) AS NVARCHAR(50)) + N' MB,' + @LineFeed
				+ N'indicating that SQL Server may be under external memory pressure or max server memory may be set too high.' AS Details,
			'Investigate what OS processes are using memory, and double-check the max server memory setting.' AS HowToStopIt
			FROM sys.configurations cMax
			INNER JOIN sys.dm_os_performance_counters cTarget ON cTarget.object_name LIKE N'%Memory Manager%'
				AND cTarget.counter_name = N'Target Server Memory (KB)                                                                                                       '
			WHERE cMax.name = 'max server memory (MB)'
				AND CAST(cMax.value_in_use AS BIGINT) >= 1.5 * (CAST(cTarget.cntr_value AS BIGINT) / 1024)
				AND CAST(cMax.value_in_use AS BIGINT) < 2147483647 /* Not set to default of unlimited */
				AND CAST(cTarget.cntr_value AS BIGINT) < .8 * (SELECT available_physical_memory_kb FROM sys.dm_os_sys_memory); /* Target memory less than 80% of physical memory (in case they set max too high) */
	END

    /* Server Info - Database Size, Total GB - CheckID 21 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 21',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
    SELECT 21 AS CheckID,
        251 AS Priority,
        'Server Info' AS FindingGroup,
        'Database Size, Total GB' AS Finding,
        CAST(SUM (CAST(size AS BIGINT)*8./1024./1024.) AS VARCHAR(100)) AS Details,
        SUM (CAST(size AS BIGINT))*8./1024./1024. AS DetailsInt,
        'https://www.brentozar.com/askbrent/' AS URL
    FROM #MasterFiles
    WHERE database_id > 4;

    /* Server Info - Database Count - CheckID 22 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 22',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
    SELECT 22 AS CheckID,
        251 AS Priority,
        'Server Info' AS FindingGroup,
        'Database Count' AS Finding,
        CAST(SUM(1) AS VARCHAR(100)) AS Details,
        SUM (1) AS DetailsInt,
        'https://www.brentozar.com/askbrent/' AS URL
    FROM sys.databases
    WHERE database_id > 4;

	/* Server Info - Memory Grants pending - CheckID 39 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 39',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
    SELECT 39 AS CheckID,
        50 AS Priority,
		'Server Performance' AS FindingGroup,
        'Memory Grants Pending' AS Finding,
		CAST(PendingGrants.Details AS NVARCHAR(50)) AS Details,
		PendingGrants.DetailsInt,
		'https://www.brentozar.com/blitz/memory-grants/' AS URL
	FROM 
	(
		SELECT 
		COUNT(1) AS Details,
		COUNT(1) AS DetailsInt
	FROM sys.dm_exec_query_memory_grants AS Grants
	WHERE queue_id IS NOT NULL
	) AS PendingGrants
	WHERE PendingGrants.Details > 0;

	/* Server Info - Memory Grant/Workspace info - CheckID 40 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 40',10,1) WITH NOWAIT;
	END

	DECLARE @MaxWorkspace BIGINT
	SET @MaxWorkspace = (SELECT CAST(cntr_value AS BIGINT)/1024 FROM #PerfmonStats WHERE counter_name = N'Maximum Workspace Memory (KB)')
	
	IF (@MaxWorkspace IS NULL
	    OR @MaxWorkspace = 0)
	BEGIN
		SET @MaxWorkspace = 1
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
    SELECT 40 AS CheckID,
        251 AS Priority,
        'Server Info' AS FindingGroup,
        'Memory Grant/Workspace info' AS Finding,
		+ 'Grants Outstanding: ' + CAST((SELECT COUNT(*) FROM sys.dm_exec_query_memory_grants WHERE queue_id IS NULL) AS NVARCHAR(50)) + @LineFeed
		+ 'Total Granted(MB): ' + CAST(ISNULL(SUM(Grants.granted_memory_kb) / 1024, 0) AS NVARCHAR(50)) + @LineFeed
		+ 'Total WorkSpace(MB): ' + CAST(ISNULL(@MaxWorkspace, 0) AS NVARCHAR(50)) + @LineFeed  
		+ 'Granted workspace: ' + CAST(ISNULL((CAST(SUM(Grants.granted_memory_kb) / 1024 AS MONEY)
		                              / CAST(@MaxWorkspace AS MONEY)) * 100, 0) AS NVARCHAR(50)) + '%' + @LineFeed
		+ 'Oldest Grant in seconds: ' + CAST(ISNULL(DATEDIFF(SECOND, MIN(Grants.request_time), GETDATE()), 0) AS NVARCHAR(50)) AS Details,
		(SELECT COUNT(*) FROM sys.dm_exec_query_memory_grants WHERE queue_id IS NULL) AS DetailsInt,
		'https://www.brentozar.com/askbrent/' AS URL
	FROM sys.dm_exec_query_memory_grants AS Grants;

	/* Query Problems - Queries with high memory grants - CheckID 46 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 46',10,1) WITH NOWAIT;
	END

	INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, URL, QueryText, QueryPlan)
	SELECT 46 AS CheckID,
	    100 AS Priority,
	    'Query Problems' AS FindingGroup,
	    'Query with a memory grant exceeding '
		+CAST(@MemoryGrantThresholdPct AS NVARCHAR(15))
		+'%' AS Finding,
		'Granted size: '+ CAST(CAST(Grants.granted_memory_kb / 1024 AS INT) AS NVARCHAR(50))
		+N'MB '
		 + @LineFeed
		+N'Granted pct of max workspace: ' 
		+ CAST(ISNULL((CAST(Grants.granted_memory_kb / 1024 AS MONEY)
		                              / CAST(@MaxWorkspace AS MONEY)) * 100, 0) AS NVARCHAR(50)) + '%' 
		+ @LineFeed
		+N'SQLHandle: '
		+CONVERT(NVARCHAR(128),Grants.[sql_handle],1),
		'https://www.brentozar.com/memory-grants-sql-servers-public-toilet/' AS URL,
		SQLText.[text],
		QueryPlan.query_plan
	FROM sys.dm_exec_query_memory_grants AS Grants
	OUTER APPLY sys.dm_exec_sql_text(Grants.[sql_handle]) AS SQLText
	OUTER APPLY sys.dm_exec_query_plan(Grants.[plan_handle]) AS QueryPlan
	WHERE Grants.granted_memory_kb > ((@MemoryGrantThresholdPct/100.00)*(@MaxWorkspace*1024));

    /* Query Problems - Memory Leak in USERSTORE_TOKENPERM Cache - CheckID 45 */
    IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_os_memory_clerks') AND name = 'pages_kb')
        BEGIN
			IF (@Debug = 1)
			BEGIN
				RAISERROR('Running CheckID 45',10,1) WITH NOWAIT;
			END

        /* SQL 2012+ version */
        SET @StringToExecute = N'
        INSERT  INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, URL)
        SELECT 45 AS CheckID,
                50 AS Priority,
                ''Query Problems'' AS FindingsGroup,
                ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
                N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) 
                    + N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
                AS details,
                ''https://www.BrentOzar.com/go/userstore'' AS URL
        FROM sys.dm_os_memory_clerks
        HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / SUM(pages_kb) >= 0.1					
            AND SUM(pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
        EXEC sp_executesql @StringToExecute;
        END
    ELSE
        BEGIN
        /* Antiques Roadshow SQL 2008R2 - version */
			IF (@Debug = 1)
			BEGIN
				RAISERROR('Running CheckID 45 (Legacy version)',10,1) WITH NOWAIT;
			END

        SET @StringToExecute = N'
        INSERT  INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, URL)
        SELECT 45 AS CheckID,
                50 AS Priority,
                ''Performance'' AS FindingsGroup,
                ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
                N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) 
                    + N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
                AS details,
                ''https://www.BrentOzar.com/go/userstore'' AS URL
        FROM sys.dm_os_memory_clerks
        HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / SUM(single_pages_kb + multi_pages_kb) >= 0.1
            AND SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
        EXEC sp_executesql @StringToExecute;
        END



    IF @Seconds > 0
    BEGIN

    IF EXISTS ( SELECT 1/0
                FROM sys.all_objects AS ao
                WHERE ao.name = 'dm_exec_query_profiles' )
    BEGIN

        IF EXISTS( SELECT 1/0
                   FROM sys.dm_exec_requests AS r
                   JOIN sys.dm_exec_sessions AS s
                       ON r.session_id = s.session_id
                   WHERE s.host_name IS NOT NULL
                   AND r.total_elapsed_time > 5000 )
			BEGIN

                   SET @StringToExecute = N'
                   DECLARE @bad_estimate TABLE 
                     ( 
                       session_id INT, 
                       request_id INT, 
                       estimate_inaccuracy BIT 
                     );
                   
                   INSERT @bad_estimate ( session_id, request_id, estimate_inaccuracy )
                   SELECT x.session_id, 
                          x.request_id, 
                          x.estimate_inaccuracy
                   FROM (
                         SELECT deqp.session_id,
                                deqp.request_id,
                                CASE WHEN deqp.row_count > ( deqp.estimate_row_count * 10000 )
                                     THEN 1
                                     ELSE 0
                                END AS estimate_inaccuracy
                         FROM   sys.dm_exec_query_profiles AS deqp
						 WHERE deqp.session_id <> @@SPID
                   ) AS x
                   WHERE x.estimate_inaccuracy = 1
                   GROUP BY x.session_id, 
                            x.request_id, 
                            x.estimate_inaccuracy;
                   
                   DECLARE @parallelism_skew TABLE
                     (
                       session_id INT, 
                       request_id INT, 
                       parallelism_skew BIT     
                     );
                   
                   INSERT @parallelism_skew ( session_id, request_id, parallelism_skew )
                   SELECT y.session_id,
                          y.request_id,
                          y.parallelism_skew
                   FROM (
                         SELECT x.session_id, 
                                x.request_id, 
                                x.node_id, 
                                x.thread_id, 
                                x.row_count, 
                                x.sum_node_rows, 
                                x.node_dop,
                                x.sum_node_rows / x.node_dop AS even_distribution,
                                x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) AS skew_percent,
                                CASE 
                                    WHEN x.row_count > 10000
                                    AND x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) > 2.
                                    THEN 1
                                    WHEN x.row_count > 10000
                                    AND x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) < 0.5
                                    THEN 1
                                    ELSE 0 
                         	   END AS parallelism_skew
                         FROM (
                         	       SELECT deqp.session_id,
                                              deqp.request_id,
                                              deqp.node_id,
                                              deqp.thread_id,
                         	       	   deqp.row_count,
                         	       	   SUM(deqp.row_count) 
                         	       		OVER ( PARTITION BY deqp.session_id,
                                                                   deqp.request_id,
                         	       		                    deqp.node_id
                         	       			   ORDER BY deqp.row_count
                         	       			   ROWS BETWEEN UNBOUNDED PRECEDING 
                         	       			   AND UNBOUNDED FOLLOWING ) 
                         	       			   AS sum_node_rows,
                         	       	   COUNT(*) 
                         	       		OVER ( PARTITION BY deqp.session_id,
                                                                   deqp.request_id,
                         	       		                    deqp.node_id
                         	       			   ORDER BY deqp.row_count
                         	       			   ROWS BETWEEN UNBOUNDED PRECEDING 
                         	       			   AND UNBOUNDED FOLLOWING ) 
                         	       			   AS node_dop
                         	       FROM sys.dm_exec_query_profiles AS deqp
                         	       WHERE deqp.thread_id > 0
								   AND deqp.session_id <> @@SPID
                         	       AND EXISTS 
                         	       	(
                         	       		SELECT 1/0
                         	       		FROM   sys.dm_exec_query_profiles AS deqp2
                         	       		WHERE deqp.session_id = deqp2.session_id
                         	       		AND   deqp.node_id = deqp2.node_id
                         	       		AND   deqp2.thread_id > 0
                         	       		GROUP BY deqp2.session_id, deqp2.node_id
                         	       		HAVING COUNT(deqp2.node_id) > 1
                         	       	)
                         	   ) AS x
                         ) AS y
                   WHERE y.parallelism_skew = 1
                   GROUP BY y.session_id, 
                            y.request_id, 
                            y.parallelism_skew;
                   
                   /* Queries in dm_exec_query_profiles showing signs of poor cardinality estimates - CheckID 42 */
				   IF (@Debug = 1)
				   BEGIN
				        RAISERROR(''Running CheckID 42'',10,1) WITH NOWAIT;
				   END

                   INSERT INTO #BlitzFirstResults 
                   (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount, QueryHash, QueryPlan)
                   SELECT 42 AS CheckID,
                          100 AS Priority,
                          ''Query Performance'' AS FindingsGroup,
                          ''Queries with 10000x cardinality misestimations'' AS Findings,
                          ''https://www.brentozar.com/go/skewedup'' AS URL,
                          ''The query on SPID '' 
                              + RTRIM(b.session_id) 
                              + '' has been running for ''
                              + RTRIM(r.total_elapsed_time / 1000)
                              + '' seconds,  with a large cardinality misestimate'' AS Details,
                          ''No quick fix here: time to dig into the actual execution plan. '' AS HowToStopIt,
                          r.start_time,
                          s.login_name,
                          s.nt_user_name,
                          s.program_name,
                          s.host_name,
                          r.database_id,
                          DB_NAME(r.database_id),
                          dest.text,
                          s.open_transaction_count,
                          r.query_hash, ';

				IF @dm_exec_query_statistics_xml = 1
					SET @StringToExecute = @StringToExecute + N' COALESCE(qs_live.query_plan, qp.query_plan) AS query_plan ';
				ELSE
					SET @StringToExecute = @StringToExecute + N' qp.query_plan ';

				SET @StringToExecute = @StringToExecute + N'
                  FROM @bad_estimate AS b
                  JOIN sys.dm_exec_requests AS r
                  ON r.session_id = b.session_id
                  AND r.request_id = b.request_id
                  JOIN sys.dm_exec_sessions AS s
                  ON s.session_id = b.session_id
                  CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS dest
				  CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp ';

				IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_exec_query_statistics_xml')
					SET @StringToExecute = @StringToExecute + N' OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live ';
				  
				  
				SET @StringToExecute = @StringToExecute + N';

                   /* Queries in dm_exec_query_profiles showing signs of unbalanced parallelism - CheckID 43 */
				   IF (@Debug = 1)
				   BEGIN
				        RAISERROR(''Running CheckID 43'',10,1) WITH NOWAIT;
				   END

                   INSERT INTO #BlitzFirstResults 
                   (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount, QueryHash, QueryPlan)
                   SELECT 43 AS CheckID,
                          100 AS Priority,
                          ''Query Performance'' AS FindingsGroup,
                          ''Queries with 10000x skewed parallelism'' AS Findings,
                          ''https://www.brentozar.com/go/skewedup'' AS URL,
                          ''The query on SPID '' 
                              + RTRIM(p.session_id) 
                              + '' has been running for ''
                              + RTRIM(r.total_elapsed_time / 1000)
                              + '' seconds,  with a parallel threads doing uneven work.'' AS Details,
                          ''No quick fix here: time to dig into the actual execution plan. '' AS HowToStopIt,
                          r.start_time,
                          s.login_name,
                          s.nt_user_name,
                          s.program_name,
                          s.host_name,
                          r.database_id,
                          DB_NAME(r.database_id),
                          dest.text,
                          s.open_transaction_count,
                          r.query_hash, ';

				IF @dm_exec_query_statistics_xml = 1
					SET @StringToExecute = @StringToExecute + N' COALESCE(qs_live.query_plan, qp.query_plan) AS query_plan ';
				ELSE
					SET @StringToExecute = @StringToExecute + N' qp.query_plan ';

				SET @StringToExecute = @StringToExecute + N'
                  FROM @parallelism_skew AS p
                  JOIN sys.dm_exec_requests AS r
                  ON r.session_id = p.session_id
                  AND r.request_id = p.request_id
                  JOIN sys.dm_exec_sessions AS s
                  ON s.session_id = p.session_id
                  CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS dest
				  CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp ';

				IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_exec_query_statistics_xml')
					SET @StringToExecute = @StringToExecute + N' OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live ';
				  
				  
				SET @StringToExecute = @StringToExecute + N';';

	          EXECUTE sp_executesql @StringToExecute, N'@Debug BIT',@Debug = @Debug;
			END
   
        END
    END

    /* Server Performance - High CPU Utilization - CheckID 24 */
    IF @Seconds < 30
        BEGIN
        /* If we're waiting less than 30 seconds, run this check now rather than wait til the end.
           We get this data from the ring buffers, and it's only updated once per minute, so might
           as well get it now - whereas if we're checking 30+ seconds, it might get updated by the
           end of our sp_BlitzFirst session. */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 24',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
        SELECT 24, 50, 'Server Performance', 'High CPU Utilization', CAST(100 - SystemIdle AS NVARCHAR(20)) + N'%.', 100 - SystemIdle, 'https://www.brentozar.com/go/cpu'
            FROM (
                SELECT record,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
                FROM (
                    SELECT TOP 1 CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%'
                    ORDER BY timestamp DESC) AS rb
            ) AS y
            WHERE 100 - SystemIdle >= 50;

        /* CPU Utilization - CheckID 23 */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 23',10,1) WITH NOWAIT;
		END

        IF SERVERPROPERTY('Edition') <> 'SQL Azure'
			WITH y
				AS
				 (
					 SELECT      CONVERT(VARCHAR(5), 100 - ca.c.value('.', 'INT')) AS system_idle,
								 CONVERT(VARCHAR(30), rb.event_date) AS event_date,
								 CONVERT(VARCHAR(8000), rb.record) AS record,
								 event_date as event_date_raw
					 FROM
								 (   SELECT CONVERT(XML, dorb.record) AS record,
											DATEADD(ms, -( ts.ms_ticks - dorb.timestamp ), GETDATE()) AS event_date
									 FROM   sys.dm_os_ring_buffers AS dorb
									 CROSS JOIN
											( SELECT dosi.ms_ticks FROM sys.dm_os_sys_info AS dosi ) AS ts
									 WHERE  dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
									 AND    record LIKE '%<SystemHealth>%' ) AS rb
					 CROSS APPLY rb.record.nodes('/Record/SchedulerMonitorEvent/SystemHealth/SystemIdle') AS ca(c)
				 )
			INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL, HowToStopIt)
			SELECT TOP 1 
					23, 
					250, 
					'Server Info', 
					'CPU Utilization', 
					y.system_idle + N'%. Ring buffer details: ' + CAST(y.record AS NVARCHAR(4000)), 
					y.system_idle	, 
					'https://www.brentozar.com/go/cpu',
					STUFF(( SELECT TOP 2147483647
							  CHAR(10) + CHAR(13)
							+ y2.system_idle 
							+ '% ON ' 
							+ y2.event_date 
							+ ' Ring buffer details:  '
							+ y2.record
					FROM   y AS y2
					ORDER BY y2.event_date_raw DESC
					FOR XML PATH(N''), TYPE ).value(N'.[1]', N'VARCHAR(MAX)'), 1, 1, N'') AS query
			FROM   y
			ORDER BY y.event_date_raw DESC;

		
		/* Highlight if non SQL processes are using >25% CPU - CheckID 28 */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 28',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
	    SELECT 28,	50,	'Server Performance', 'High CPU Utilization - Not SQL', CONVERT(NVARCHAR(100),100 - (y.SQLUsage + y.SystemIdle)) + N'% - Other Processes (not SQL Server) are using this much CPU. This may impact on the performance of your SQL Server instance', 100 - (y.SQLUsage + y.SystemIdle), 'https://www.brentozar.com/go/cpu'
            FROM (
                SELECT record,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
					,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLUsage
                FROM (
                    SELECT TOP 1 CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%'
                    ORDER BY timestamp DESC) AS rb
            ) AS y
            WHERE 100 - (y.SQLUsage + y.SystemIdle) >= 25;
		
        END; /* IF @Seconds < 30 */

    /* Query Problems - Statistics Updated Recently - CheckID 44 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 44',10,1) WITH NOWAIT;
	END

	IF 20 >= (SELECT COUNT(*) FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb'))
		AND @Seconds > 0
	BEGIN
		CREATE TABLE #UpdatedStats (HowToStopIt NVARCHAR(4000), RowsForSorting BIGINT);
		IF EXISTS(SELECT * FROM sys.all_objects WHERE name = 'dm_db_stats_properties')
		BEGIN
			/* We don't want to hang around to obtain locks */
			SET LOCK_TIMEOUT 0;

			IF SERVERPROPERTY('Edition') <> 'SQL Azure'
				SET @StringToExecute = N'USE [?];' + @LineFeed;
			ELSE
				SET @StringToExecute = N'';

            SET @StringToExecute = @StringToExecute + 'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SET LOCK_TIMEOUT 1000;' + @LineFeed +
                                    'BEGIN TRY' + @LineFeed +
                                    '    INSERT INTO #UpdatedStats(HowToStopIt, RowsForSorting)' + @LineFeed +
                                    '    SELECT HowToStopIt = ' + @LineFeed +
                                    '                QUOTENAME(DB_NAME()) + N''.'' +' + @LineFeed +
                                    '                QUOTENAME(SCHEMA_NAME(obj.schema_id)) + N''.'' +' + @LineFeed +
                                    '                QUOTENAME(obj.name) +' + @LineFeed +
                                    '                N'' statistic '' + QUOTENAME(stat.name) + ' + @LineFeed +
                                    '                N'' was updated on '' + CONVERT(NVARCHAR(50), sp.last_updated, 121) + N'','' + ' + @LineFeed +
                                    '                N'' had '' + CAST(sp.rows AS NVARCHAR(50)) + N'' rows, with '' +' + @LineFeed +
                                    '                CAST(sp.rows_sampled AS NVARCHAR(50)) + N'' rows sampled,'' +  ' + @LineFeed +
                                    '                N'' producing '' + CAST(sp.steps AS NVARCHAR(50)) + N'' steps in the histogram.'',' + @LineFeed +
                                    '        sp.rows' + @LineFeed +
                                    '    FROM sys.objects AS obj WITH (NOLOCK)' + @LineFeed +
                                    '    INNER JOIN sys.stats AS stat WITH (NOLOCK) ON stat.object_id = obj.object_id  ' + @LineFeed +
                                    '    CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  ' + @LineFeed +
                                    '    WHERE sp.last_updated > DATEADD(MI, -15, GETDATE())' + @LineFeed +
                                    '    AND obj.is_ms_shipped = 0' + @LineFeed +
                                    '    AND ''[?]'' <> ''[tempdb]'';' + @LineFeed +
                                    'END TRY' + @LineFeed +
                                    'BEGIN CATCH' + @LineFeed +
                                    '    IF (ERROR_NUMBER() = 1222)' + @LineFeed +
                                    '    BEGIN ' + @LineFeed +
                                    '        INSERT INTO #UpdatedStats(HowToStopIt, RowsForSorting)' + @LineFeed +
                                    '        SELECT HowToStopIt = ' + @LineFeed +
                                    '                    QUOTENAME(DB_NAME()) +' + @LineFeed +
                                    '                    N'' No information could be retrieved as the lock timeout was exceeded,''+' + @LineFeed +
                                    '                    N''  this is likely due to an Index operation in Progress'',' + @LineFeed +
                                    '            -1' + @LineFeed +
                                    '    END' + @LineFeed +
                                    '    ELSE' + @LineFeed +
                                    '    BEGIN' + @LineFeed +
                                    '        INSERT INTO #UpdatedStats(HowToStopIt, RowsForSorting)' + @LineFeed +
                                    '        SELECT HowToStopIt = ' + @LineFeed +
                                    '                    QUOTENAME(DB_NAME()) +' + @LineFeed +
                                    '                    N'' No information could be retrieved as a result of error: ''+' + @LineFeed +
                                    '                    CAST(ERROR_NUMBER() AS NVARCHAR(10)) +' + @LineFeed +
                                    '                    N'' with message: ''+' + @LineFeed +
                                    '                    CAST(ERROR_MESSAGE() AS NVARCHAR(128)),' + @LineFeed +
                                    '            -1' + @LineFeed +
                                    '    END' + @LineFeed +
                                    'END CATCH'                          
                                    ;

			IF SERVERPROPERTY('Edition') <> 'SQL Azure'
	            EXEC sp_MSforeachdb @StringToExecute;
			ELSE
				EXEC(@StringToExecute);

			/* Set timeout back to a default value of -1 */
			SET LOCK_TIMEOUT -1;
		END;
		
		/* We mark timeout exceeded with a -1 so only show these IF there is statistics info that succeeded */
		IF EXISTS (SELECT * FROM #UpdatedStats WHERE RowsForSorting > -1)
			INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
			SELECT 44 AS CheckId,
					50 AS Priority,
					'Query Problems' AS FindingGroup,
					'Statistics Updated Recently' AS Finding,
					'https://www.brentozar.com/go/stats' AS URL,
					'In the last 15 minutes, statistics were updated. To see which ones, click the HowToStopIt column.' + @LineFeed + @LineFeed
						+ 'This effectively clears the plan cache for queries that involve these tables,' + @LineFeed
						+ 'which thereby causes parameter sniffing: those queries are now getting brand new' + @LineFeed
						+ 'query plans based on whatever parameters happen to call them next.' + @LineFeed + @LineFeed
						+ 'Be on the lookout for sudden parameter sniffing issues after this time range.',
					HowToStopIt = (SELECT (SELECT HowToStopIt + NCHAR(10))
						FROM #UpdatedStats
						ORDER BY RowsForSorting DESC
						FOR XML PATH(''));

	END

	RAISERROR('Finished running investigatory queries',10,1) WITH NOWAIT;


    /* End of checks. If we haven't waited @Seconds seconds, wait. */
    IF DATEADD(SECOND,1,SYSDATETIMEOFFSET()) < @FinishSampleTime
        BEGIN
        RAISERROR('Waiting to match @Seconds parameter',10,1) WITH NOWAIT;
        WAITFOR TIME @FinishSampleTimeWaitFor;
        END;

    IF @total_cpu_usage IN (0, 1)
	BEGIN
	    EXEC sys.sp_executesql
		    @get_thread_time_ms,
			N'@thread_time_ms FLOAT OUTPUT',
			@thread_time_ms OUTPUT;		    
	END

	RAISERROR('Capturing second pass of wait stats, perfmon counters, file stats',10,1) WITH NOWAIT;
    /* Populate #FileStats, #PerfmonStats, #WaitStats with DMV data. In a second, we'll compare these. */
	SET @StringToExecute = N'
		INSERT #WaitStats(Pass, SampleTime, wait_type, wait_time_ms, thread_time_ms, signal_wait_time_ms, waiting_tasks_count)
			SELECT 
			x.Pass, 
			x.SampleTime, 
			x.wait_type, 
			SUM(x.sum_wait_time_ms) AS sum_wait_time_ms, 
			@thread_time_ms AS thread_time_ms,
			SUM(x.sum_signal_wait_time_ms) AS sum_signal_wait_time_ms, 
			SUM(x.sum_waiting_tasks) AS sum_waiting_tasks
			FROM (
			SELECT  
					2 AS Pass,
					SYSDATETIMEOFFSET() AS SampleTime,
					owt.wait_type,
					SUM(owt.wait_duration_ms) OVER (PARTITION BY owt.wait_type, owt.session_id)
						 - CASE WHEN @Seconds = 0 THEN 0 ELSE (@Seconds * 1000) END AS sum_wait_time_ms,
					0 AS sum_signal_wait_time_ms,
					CASE @Seconds WHEN 0 THEN 0 ELSE 1 END AS sum_waiting_tasks
				FROM    sys.dm_os_waiting_tasks owt
				WHERE owt.session_id > 50
				AND owt.wait_duration_ms >= CASE @Seconds WHEN 0 THEN 0 ELSE @Seconds * 1000 END
			UNION ALL
			SELECT
				   2 AS Pass,
				   SYSDATETIMEOFFSET() AS SampleTime,
				   os.wait_type,
				   SUM(os.wait_time_ms) OVER (PARTITION BY os.wait_type) AS sum_wait_time_ms,
				   SUM(os.signal_wait_time_ms) OVER (PARTITION BY os.wait_type ) AS sum_signal_wait_time_ms,
				   SUM(os.waiting_tasks_count) OVER (PARTITION BY os.wait_type) AS sum_waiting_tasks ';

	IF SERVERPROPERTY('Edition') = 'SQL Azure'
		SET @StringToExecute = @StringToExecute + N' FROM sys.dm_db_wait_stats os ';
	ELSE
		SET @StringToExecute = @StringToExecute + N' FROM sys.dm_os_wait_stats os ';

	SET @StringToExecute = @StringToExecute + N'
		) x
		   WHERE NOT EXISTS 
		   (
                SELECT *
				FROM ##WaitCategories AS wc
				WHERE wc.WaitType = x.wait_type
				AND wc.Ignorable = 1
		   )
		GROUP BY x.Pass, x.SampleTime, x.wait_type
		ORDER BY sum_wait_time_ms DESC;';
		
		EXEC sys.sp_executesql
	        @StringToExecute,
          N'@StartSampleTime DATETIMEOFFSET,
	        @Seconds INT,
	        @thread_time_ms FLOAT',
	        @StartSampleTime,
	        @Seconds,
	        @thread_time_ms;

    WITH w AS
	(
	    SELECT
		    total_waits =
			    CONVERT
				(
				    FLOAT,
			        SUM(ws.wait_time_ms)
				)
		FROM #WaitStats AS ws
		WHERE Pass = 2
	)
    UPDATE ws
	    SET	ws.thread_time_ms += w.total_waits
	FROM #WaitStats AS ws
	CROSS JOIN w
	WHERE ws.Pass = 2
	OPTION(RECOMPILE);
	

    INSERT INTO #FileStats (Pass, SampleTime, DatabaseID, FileID, DatabaseName, FileLogicalName, SizeOnDiskMB, io_stall_read_ms ,
        num_of_reads, [bytes_read] , io_stall_write_ms,num_of_writes, [bytes_written], PhysicalName, TypeDesc, avg_stall_read_ms, avg_stall_write_ms)
    SELECT         2 AS Pass,
        SYSDATETIMEOFFSET() AS SampleTime,
        mf.[database_id],
        mf.[file_id],
        DB_NAME(vfs.database_id) AS [db_name],
        mf.name + N' [' + mf.type_desc COLLATE SQL_Latin1_General_CP1_CI_AS + N']' AS file_logical_name ,
        CAST(( ( vfs.size_on_disk_bytes / 1024.0 ) / 1024.0 ) AS INT) AS size_on_disk_mb ,
        vfs.io_stall_read_ms ,
        vfs.num_of_reads ,
        vfs.[num_of_bytes_read],
        vfs.io_stall_write_ms ,
        vfs.num_of_writes ,
        vfs.[num_of_bytes_written],
        mf.physical_name,
        mf.type_desc,
        0,
        0
    FROM sys.dm_io_virtual_file_stats (NULL, NULL) AS vfs
    INNER JOIN #MasterFiles AS mf ON vfs.file_id = mf.file_id
        AND vfs.database_id = mf.database_id
    WHERE vfs.num_of_reads > 0
        OR vfs.num_of_writes > 0;

    INSERT INTO #PerfmonStats (Pass, SampleTime, [object_name],[counter_name],[instance_name],[cntr_value],[cntr_type])
    SELECT         2 AS Pass,
        SYSDATETIMEOFFSET() AS SampleTime,
        RTRIM(dmv.object_name), RTRIM(dmv.counter_name), RTRIM(dmv.instance_name), dmv.cntr_value, dmv.cntr_type
        FROM #PerfmonCounters counters
        INNER JOIN sys.dm_os_performance_counters dmv ON counters.counter_name COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.counter_name) COLLATE SQL_Latin1_General_CP1_CI_AS
            AND counters.[object_name] COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.[object_name]) COLLATE SQL_Latin1_General_CP1_CI_AS
            AND (counters.[instance_name] IS NULL OR counters.[instance_name] COLLATE SQL_Latin1_General_CP1_CI_AS = RTRIM(dmv.[instance_name]) COLLATE SQL_Latin1_General_CP1_CI_AS);

    /* Set the latencies and averages. We could do this with a CTE, but we're not ambitious today. */
    UPDATE fNow
    SET avg_stall_read_ms = ((fNow.io_stall_read_ms - fBase.io_stall_read_ms) / (fNow.num_of_reads - fBase.num_of_reads))
    FROM #FileStats fNow
    INNER JOIN #FileStats fBase ON fNow.DatabaseID = fBase.DatabaseID AND fNow.FileID = fBase.FileID AND fNow.SampleTime > fBase.SampleTime AND fNow.num_of_reads > fBase.num_of_reads AND fNow.io_stall_read_ms > fBase.io_stall_read_ms
    WHERE (fNow.num_of_reads - fBase.num_of_reads) > 0;

    UPDATE fNow
    SET avg_stall_write_ms = ((fNow.io_stall_write_ms - fBase.io_stall_write_ms) / (fNow.num_of_writes - fBase.num_of_writes))
    FROM #FileStats fNow
    INNER JOIN #FileStats fBase ON fNow.DatabaseID = fBase.DatabaseID AND fNow.FileID = fBase.FileID AND fNow.SampleTime > fBase.SampleTime AND fNow.num_of_writes > fBase.num_of_writes AND fNow.io_stall_write_ms > fBase.io_stall_write_ms
    WHERE (fNow.num_of_writes - fBase.num_of_writes) > 0;

    UPDATE pNow
        SET [value_delta] = pNow.cntr_value - pFirst.cntr_value,
            [value_per_second] = ((1.0 * pNow.cntr_value - pFirst.cntr_value) / DATEDIFF(ss, pFirst.SampleTime, pNow.SampleTime))
        FROM #PerfmonStats pNow
            INNER JOIN #PerfmonStats pFirst ON pFirst.[object_name] = pNow.[object_name] AND pFirst.counter_name = pNow.counter_name AND (pFirst.instance_name = pNow.instance_name OR (pFirst.instance_name IS NULL AND pNow.instance_name IS NULL))
                AND pNow.ID > pFirst.ID
        WHERE  DATEDIFF(ss, pFirst.SampleTime, pNow.SampleTime) > 0;


    /* Query Stats - If we're within 10 seconds of our projected finish time, do the plan cache analysis. - CheckID 18 */
    IF DATEDIFF(ss, @FinishSampleTime, SYSDATETIMEOFFSET()) > 10 AND @CheckProcedureCache = 1
        BEGIN
			IF (@Debug = 1)
			BEGIN
				RAISERROR('Running CheckID 18',10,1) WITH NOWAIT;
			END

            INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            VALUES (18, 210, 'Query Stats', 'Plan Cache Analysis Skipped', 'https://www.brentozar.com/go/topqueries',
                'Due to excessive load, the plan cache analysis was skipped. To override this, use @ExpertMode = 1.');

        END;
    ELSE IF @CheckProcedureCache = 1
        BEGIN


		RAISERROR('@CheckProcedureCache = 1, capturing second pass of plan cache',10,1) WITH NOWAIT;

        /* Populate #QueryStats. SQL 2005 doesn't have query hash or query plan hash. */
		IF @@VERSION LIKE 'Microsoft SQL Server 2005%'
			BEGIN
			IF @FilterPlansByDatabase IS NULL
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= @StartSampleTimeText;';
				END;
			ELSE
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
												CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
												INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= @StartSampleTimeText
												AND attr.attribute = ''dbid'';';
				END;
			END;
		ELSE
			BEGIN
			IF @FilterPlansByDatabase IS NULL
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= @StartSampleTimeText';
				END;
			ELSE
				BEGIN
				SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
											INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= @StartSampleTimeText
												AND attr.attribute = ''dbid'';';
				END;
			END;
		/* Old version pre-2016/06/13:
        IF @@VERSION LIKE 'Microsoft SQL Server 2005%'
            SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
                                        SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
                                        FROM sys.dm_exec_query_stats qs
                                        WHERE qs.last_execution_time >= @StartSampleTimeText;';
        ELSE
            SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
                                        SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
                                        FROM sys.dm_exec_query_stats qs
                                        WHERE qs.last_execution_time >= @StartSampleTimeText;';
		*/
        SET @ParmDefinitions = N'@StartSampleTimeText NVARCHAR(100)';
        SET @Parm1 = CONVERT(NVARCHAR(100), CAST(@StartSampleTime AS DATETIME), 127);

        EXECUTE sp_executesql @StringToExecute, @ParmDefinitions, @StartSampleTimeText = @Parm1;

		RAISERROR('@CheckProcedureCache = 1, totaling up plan cache metrics',10,1) WITH NOWAIT;

        /* Get the totals for the entire plan cache */
        INSERT INTO #QueryStats (Pass, SampleTime, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time)
        SELECT 0 AS Pass, SYSDATETIMEOFFSET(), SUM(execution_count), SUM(total_worker_time), SUM(total_physical_reads), SUM(total_logical_writes), SUM(total_logical_reads), SUM(total_clr_time), SUM(total_elapsed_time), MIN(creation_time)
            FROM sys.dm_exec_query_stats qs;


		RAISERROR('@CheckProcedureCache = 1, so analyzing execution plans',10,1) WITH NOWAIT;
        /*
        Pick the most resource-intensive queries to review. Update the Points field
        in #QueryStats - if a query is in the top 10 for logical reads, CPU time,
        duration, or execution, add 1 to its points.
        */
        WITH qsTop AS (
        SELECT TOP 10 qsNow.ID
        FROM #QueryStats qsNow
          INNER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
        WHERE qsNow.total_elapsed_time > qsFirst.total_elapsed_time
            AND qsNow.Pass = 2
            AND qsNow.total_elapsed_time - qsFirst.total_elapsed_time > 1000000 /* Only queries with over 1 second of runtime */
        ORDER BY (qsNow.total_elapsed_time - COALESCE(qsFirst.total_elapsed_time, 0)) DESC)
        UPDATE #QueryStats
            SET Points = Points + 1
            FROM #QueryStats qs
            INNER JOIN qsTop ON qs.ID = qsTop.ID;

        WITH qsTop AS (
        SELECT TOP 10 qsNow.ID
        FROM #QueryStats qsNow
          INNER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
        WHERE qsNow.total_logical_reads > qsFirst.total_logical_reads
            AND qsNow.Pass = 2
            AND qsNow.total_logical_reads - qsFirst.total_logical_reads > 1000 /* Only queries with over 1000 reads */
        ORDER BY (qsNow.total_logical_reads - COALESCE(qsFirst.total_logical_reads, 0)) DESC)
        UPDATE #QueryStats
            SET Points = Points + 1
            FROM #QueryStats qs
            INNER JOIN qsTop ON qs.ID = qsTop.ID;

        WITH qsTop AS (
        SELECT TOP 10 qsNow.ID
        FROM #QueryStats qsNow
          INNER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
        WHERE qsNow.total_worker_time > qsFirst.total_worker_time
            AND qsNow.Pass = 2
            AND qsNow.total_worker_time - qsFirst.total_worker_time > 1000000 /* Only queries with over 1 second of worker time */
        ORDER BY (qsNow.total_worker_time - COALESCE(qsFirst.total_worker_time, 0)) DESC)
        UPDATE #QueryStats
            SET Points = Points + 1
            FROM #QueryStats qs
            INNER JOIN qsTop ON qs.ID = qsTop.ID;

        WITH qsTop AS (
        SELECT TOP 10 qsNow.ID
        FROM #QueryStats qsNow
          INNER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
        WHERE qsNow.execution_count > qsFirst.execution_count
            AND qsNow.Pass = 2
            AND (qsNow.total_elapsed_time - qsFirst.total_elapsed_time > 1000000 /* Only queries with over 1 second of runtime */
                OR qsNow.total_logical_reads - qsFirst.total_logical_reads > 1000 /* Only queries with over 1000 reads */
                OR qsNow.total_worker_time - qsFirst.total_worker_time > 1000000 /* Only queries with over 1 second of worker time */)
        ORDER BY (qsNow.execution_count - COALESCE(qsFirst.execution_count, 0)) DESC)
        UPDATE #QueryStats
            SET Points = Points + 1
            FROM #QueryStats qs
            INNER JOIN qsTop ON qs.ID = qsTop.ID;

        /* Query Stats - Most Resource-Intensive Queries - CheckID 17 */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 17',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, QueryStatsNowID, QueryStatsFirstID, PlanHandle, QueryHash)
        SELECT 17, 210, 'Query Stats', 'Most Resource-Intensive Queries', 'https://www.brentozar.com/go/topqueries',
            'Query stats during the sample:' + @LineFeed +
            'Executions: ' + CAST(qsNow.execution_count - (COALESCE(qsFirst.execution_count, 0)) AS NVARCHAR(100)) + @LineFeed +
            'Elapsed Time: ' + CAST(qsNow.total_elapsed_time - (COALESCE(qsFirst.total_elapsed_time, 0)) AS NVARCHAR(100)) + @LineFeed +
            'CPU Time: ' + CAST(qsNow.total_worker_time - (COALESCE(qsFirst.total_worker_time, 0)) AS NVARCHAR(100)) + @LineFeed +
            'Logical Reads: ' + CAST(qsNow.total_logical_reads - (COALESCE(qsFirst.total_logical_reads, 0)) AS NVARCHAR(100)) + @LineFeed +
            'Logical Writes: ' + CAST(qsNow.total_logical_writes - (COALESCE(qsFirst.total_logical_writes, 0)) AS NVARCHAR(100)) + @LineFeed +
            'CLR Time: ' + CAST(qsNow.total_clr_time - (COALESCE(qsFirst.total_clr_time, 0)) AS NVARCHAR(100)) + @LineFeed +
            @LineFeed + @LineFeed + 'Query stats since ' + CONVERT(NVARCHAR(100), qsNow.creation_time ,121) + @LineFeed +
            'Executions: ' + CAST(qsNow.execution_count AS NVARCHAR(100)) +
                    CASE qsTotal.execution_count WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.execution_count / qsTotal.execution_count AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            'Elapsed Time: ' + CAST(qsNow.total_elapsed_time AS NVARCHAR(100)) +
                    CASE qsTotal.total_elapsed_time WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.total_elapsed_time / qsTotal.total_elapsed_time AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            'CPU Time: ' + CAST(qsNow.total_worker_time AS NVARCHAR(100)) +
                    CASE qsTotal.total_worker_time WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.total_worker_time / qsTotal.total_worker_time AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            'Logical Reads: ' + CAST(qsNow.total_logical_reads AS NVARCHAR(100)) +
                    CASE qsTotal.total_logical_reads WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.total_logical_reads / qsTotal.total_logical_reads AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            'Logical Writes: ' + CAST(qsNow.total_logical_writes AS NVARCHAR(100)) +
                    CASE qsTotal.total_logical_writes WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.total_logical_writes / qsTotal.total_logical_writes AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            'CLR Time: ' + CAST(qsNow.total_clr_time AS NVARCHAR(100)) +
                    CASE qsTotal.total_clr_time WHEN 0 THEN '' ELSE (' - Percent of Server Total: ' + CAST(CAST(100.0 * qsNow.total_clr_time / qsTotal.total_clr_time AS DECIMAL(6,2)) AS NVARCHAR(100)) + '%') END + @LineFeed +
            --@LineFeed + @LineFeed + 'Query hash: ' + CAST(qsNow.query_hash AS NVARCHAR(100)) + @LineFeed +
            --@LineFeed + @LineFeed + 'Query plan hash: ' + CAST(qsNow.query_plan_hash AS NVARCHAR(100)) +
            @LineFeed AS Details,
            'See the URL for tuning tips on why this query may be consuming resources.' AS HowToStopIt,
            qp.query_plan,
            QueryText = SUBSTRING(st.text,
                 (qsNow.statement_start_offset / 2) + 1,
                 ((CASE qsNow.statement_end_offset
                   WHEN -1 THEN DATALENGTH(st.text)
                   ELSE qsNow.statement_end_offset
                   END - qsNow.statement_start_offset) / 2) + 1),
            qsNow.ID AS QueryStatsNowID,
            qsFirst.ID AS QueryStatsFirstID,
            qsNow.plan_handle AS PlanHandle,
            qsNow.query_hash
            FROM #QueryStats qsNow
                INNER JOIN #QueryStats qsTotal ON qsTotal.Pass = 0
                LEFT OUTER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
                CROSS APPLY sys.dm_exec_sql_text(qsNow.sql_handle) AS st
                CROSS APPLY sys.dm_exec_query_plan(qsNow.plan_handle) AS qp
            WHERE qsNow.Points > 0 AND st.text IS NOT NULL AND qp.query_plan IS NOT NULL;

            UPDATE #BlitzFirstResults
                SET DatabaseID = CAST(attr.value AS INT),
                DatabaseName = DB_NAME(CAST(attr.value AS INT))
            FROM #BlitzFirstResults
                CROSS APPLY sys.dm_exec_plan_attributes(#BlitzFirstResults.PlanHandle) AS attr
            WHERE attr.attribute = 'dbid';


        END; /* IF DATEDIFF(ss, @FinishSampleTime, SYSDATETIMEOFFSET()) > 10 AND @CheckProcedureCache = 1 */


	RAISERROR('Analyzing changes between first and second passes of DMVs',10,1) WITH NOWAIT;

    /* Wait Stats - CheckID 6 */
    /* Compare the current wait stats to the sample we took at the start, and insert the top 10 waits. */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 6',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, DetailsInt)
    SELECT TOP 10 6 AS CheckID,
        200 AS Priority,
        'Wait Stats' AS FindingGroup,
        wNow.wait_type AS Finding, /* IF YOU CHANGE THIS, STUFF WILL BREAK. Other checks look for wait type names in the Finding field. See checks 11, 12 as example. */
        N'https://www.sqlskills.com/help/waits/' + LOWER(wNow.wait_type) + '/' AS URL,
        'For ' + CAST(((wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) / 1000) AS NVARCHAR(100)) + ' seconds over the last ' + CASE @Seconds WHEN 0 THEN (CAST(DATEDIFF(dd,@StartSampleTime,@FinishSampleTime) AS NVARCHAR(10)) + ' days') ELSE (CAST(@Seconds AS NVARCHAR(10)) + ' seconds') END + ', SQL Server was waiting on this particular bottleneck.' + @LineFeed + @LineFeed AS Details,
        'See the URL for more details on how to mitigate this wait type.' AS HowToStopIt,
        ((wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) / 1000) AS DetailsInt
    FROM #WaitStats wNow
    LEFT OUTER JOIN #WaitStats wBase ON wNow.wait_type = wBase.wait_type AND wNow.SampleTime > wBase.SampleTime
    WHERE wNow.wait_time_ms > (wBase.wait_time_ms + (.5 * (DATEDIFF(ss,@StartSampleTime,@FinishSampleTime)) * 1000)) /* Only look for things we've actually waited on for half of the time or more */
    ORDER BY (wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) DESC;

    /* Server Performance - Poison Wait Detected - CheckID 30 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 30',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, DetailsInt)
    SELECT 30 AS CheckID,
        10 AS Priority,
        'Server Performance' AS FindingGroup,
        'Poison Wait Detected: ' + wNow.wait_type AS Finding,
        N'https://www.brentozar.com/go/poison/#' + wNow.wait_type AS URL,
        'For ' + CAST(((wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) / 1000) AS NVARCHAR(100)) + ' seconds over the last ' + CASE @Seconds WHEN 0 THEN (CAST(DATEDIFF(dd,@StartSampleTime,@FinishSampleTime) AS NVARCHAR(10)) + ' days') ELSE (CAST(@Seconds AS NVARCHAR(10)) + ' seconds') END + ', SQL Server was waiting on this particular bottleneck.' + @LineFeed + @LineFeed AS Details,
        'See the URL for more details on how to mitigate this wait type.' AS HowToStopIt,
        ((wNow.wait_time_ms - COALESCE(wBase.wait_time_ms,0)) / 1000) AS DetailsInt
    FROM #WaitStats wNow
    LEFT OUTER JOIN #WaitStats wBase ON wNow.wait_type = wBase.wait_type AND wNow.SampleTime > wBase.SampleTime
    WHERE wNow.wait_type IN ('IO_QUEUE_LIMIT', 'IO_RETRY', 'LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR', 'PREEMPTIVE_DEBUG', 'RESMGR_THROTTLED', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE','SE_REPL_CATCHUP_THROTTLE','SE_REPL_COMMIT_ACK','SE_REPL_COMMIT_TURN','SE_REPL_ROLLBACK_ACK','SE_REPL_SLOW_SECONDARY_THROTTLE','THREADPOOL') 
	  AND wNow.wait_time_ms > (wBase.wait_time_ms + 1000);


    /* Server Performance - Slow Data File Reads - CheckID 11 */
	IF EXISTS (SELECT * FROM #BlitzFirstResults WHERE Finding LIKE 'PAGEIOLATCH%')
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 11',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, DatabaseID, DatabaseName)
		SELECT TOP 10 11 AS CheckID,
			50 AS Priority,
			'Server Performance' AS FindingGroup,
			'Slow Data File Reads' AS Finding,
			'https://www.brentozar.com/go/slow/' AS URL,
			'Your server is experiencing PAGEIOLATCH% waits due to slow data file reads. This file is one of the reasons why.' + @LineFeed
				+ 'File: ' + fNow.PhysicalName + @LineFeed
				+ 'Number of reads during the sample: ' + CAST((fNow.num_of_reads - fBase.num_of_reads) AS NVARCHAR(20)) + @LineFeed
				+ 'Seconds spent waiting on storage for these reads: ' + CAST(((fNow.io_stall_read_ms - fBase.io_stall_read_ms) / 1000.0) AS NVARCHAR(20)) + @LineFeed
				+ 'Average read latency during the sample: ' + CAST(((fNow.io_stall_read_ms - fBase.io_stall_read_ms) / (fNow.num_of_reads - fBase.num_of_reads) ) AS NVARCHAR(20)) + ' milliseconds' + @LineFeed
				+ 'Microsoft guidance for data file read speed: 20ms or less.' + @LineFeed + @LineFeed AS Details,
			'See the URL for more details on how to mitigate this wait type.' AS HowToStopIt,
			fNow.DatabaseID,
			fNow.DatabaseName
		FROM #FileStats fNow
		INNER JOIN #FileStats fBase ON fNow.DatabaseID = fBase.DatabaseID AND fNow.FileID = fBase.FileID AND fNow.SampleTime > fBase.SampleTime AND fNow.num_of_reads > fBase.num_of_reads AND fNow.io_stall_read_ms > (fBase.io_stall_read_ms + 1000)
		WHERE (fNow.io_stall_read_ms - fBase.io_stall_read_ms) / (fNow.num_of_reads - fBase.num_of_reads) >= @FileLatencyThresholdMS
			AND fNow.TypeDesc = 'ROWS'
		ORDER BY (fNow.io_stall_read_ms - fBase.io_stall_read_ms) / (fNow.num_of_reads - fBase.num_of_reads) DESC;
	END;	

    /* Server Performance - Slow Log File Writes - CheckID 12 */
	IF EXISTS (SELECT * FROM #BlitzFirstResults WHERE Finding LIKE 'WRITELOG%')
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 12',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, DatabaseID, DatabaseName)
		SELECT TOP 10 12 AS CheckID,
			50 AS Priority,
			'Server Performance' AS FindingGroup,
			'Slow Log File Writes' AS Finding,
			'https://www.brentozar.com/go/slow/' AS URL,
			'Your server is experiencing WRITELOG waits due to slow log file writes. This file is one of the reasons why.' + @LineFeed
				+ 'File: ' + fNow.PhysicalName + @LineFeed
				+ 'Number of writes during the sample: ' + CAST((fNow.num_of_writes - fBase.num_of_writes) AS NVARCHAR(20)) + @LineFeed
				+ 'Seconds spent waiting on storage for these writes: ' + CAST(((fNow.io_stall_write_ms - fBase.io_stall_write_ms) / 1000.0) AS NVARCHAR(20)) + @LineFeed
				+ 'Average write latency during the sample: ' + CAST(((fNow.io_stall_write_ms - fBase.io_stall_write_ms) / (fNow.num_of_writes - fBase.num_of_writes) ) AS NVARCHAR(20)) + ' milliseconds' + @LineFeed
				+ 'Microsoft guidance for log file write speed: 3ms or less.' + @LineFeed + @LineFeed AS Details,
			'See the URL for more details on how to mitigate this wait type.' AS HowToStopIt,
			fNow.DatabaseID,
			fNow.DatabaseName
		FROM #FileStats fNow
		INNER JOIN #FileStats fBase ON fNow.DatabaseID = fBase.DatabaseID AND fNow.FileID = fBase.FileID AND fNow.SampleTime > fBase.SampleTime AND fNow.num_of_writes > fBase.num_of_writes AND fNow.io_stall_write_ms > (fBase.io_stall_write_ms + 1000)
		WHERE (fNow.io_stall_write_ms - fBase.io_stall_write_ms) / (fNow.num_of_writes - fBase.num_of_writes) >= @FileLatencyThresholdMS
			AND fNow.TypeDesc = 'LOG'
		ORDER BY (fNow.io_stall_write_ms - fBase.io_stall_write_ms) / (fNow.num_of_writes - fBase.num_of_writes) DESC;
	END;


    /* SQL Server Internal Maintenance - Log File Growing - CheckID 13 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 13',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 13 AS CheckID,
        1 AS Priority,
        'SQL Server Internal Maintenance' AS FindingGroup,
        'Log File Growing' AS Finding,
        'https://www.brentozar.com/askbrent/file-growing/' AS URL,
        'Number of growths during the sample: ' + CAST(ps.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'Determined by sampling Perfmon counter ' + ps.object_name + ' - ' + ps.counter_name + @LineFeed AS Details,
        'Pre-grow data and log files during maintenance windows so that they do not grow during production loads. See the URL for more details.'  AS HowToStopIt
    FROM #PerfmonStats ps
    WHERE ps.Pass = 2
        AND object_name = @ServiceName + ':Databases'
        AND counter_name = 'Log Growths'
        AND value_delta > 0;


    /* SQL Server Internal Maintenance - Log File Shrinking - CheckID 14 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 14',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 14 AS CheckID,
        1 AS Priority,
        'SQL Server Internal Maintenance' AS FindingGroup,
        'Log File Shrinking' AS Finding,
        'https://www.brentozar.com/askbrent/file-shrinking/' AS URL,
        'Number of shrinks during the sample: ' + CAST(ps.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'Determined by sampling Perfmon counter ' + ps.object_name + ' - ' + ps.counter_name + @LineFeed AS Details,
        'Pre-grow data and log files during maintenance windows so that they do not grow during production loads. See the URL for more details.' AS HowToStopIt
    FROM #PerfmonStats ps
    WHERE ps.Pass = 2
        AND object_name = @ServiceName + ':Databases'
        AND counter_name = 'Log Shrinks'
        AND value_delta > 0;

    /* Query Problems - Compilations/Sec High - CheckID 15 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 15',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 15 AS CheckID,
        50 AS Priority,
        'Query Problems' AS FindingGroup,
        'Compilations/Sec High' AS Finding,
        'https://www.brentozar.com/askbrent/compilations/' AS URL,
        'Number of batch requests during the sample: ' + CAST(ps.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'Number of compilations during the sample: ' + CAST(psComp.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'For OLTP environments, Microsoft recommends that 90% of batch requests should hit the plan cache, and not be compiled from scratch. We are exceeding that threshold.' + @LineFeed AS Details,
        'To find the queries that are compiling, start with:' + @LineFeed
            + 'sp_BlitzCache @SortOrder = ''recent compilations''' + @LineFeed
            + 'If dynamic SQL or non-parameterized strings are involved, consider enabling Forced Parameterization. See the URL for more details.' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name = @ServiceName + ':SQL Statistics' AND psComp.counter_name = 'SQL Compilations/sec' AND psComp.value_delta > 0
    WHERE ps.Pass = 2
        AND ps.object_name = @ServiceName + ':SQL Statistics'
        AND ps.counter_name = 'Batch Requests/sec'
        AND psComp.value_delta > 75 /* Because sp_BlitzFirst does around 50 compilations and re-compilations */
        AND (psComp.value_delta > (10 * @Seconds) OR psComp.value_delta > ps.value_delta) /* Either doing 10 compilations per second, or more compilations than queries */
        AND (psComp.value_delta * 10) > ps.value_delta; /* Compilations are more than 10% of batch requests per second */

    /* Query Problems - Re-Compilations/Sec High - CheckID 16 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 16',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 16 AS CheckID,
        50 AS Priority,
        'Query Problems' AS FindingGroup,
        'Re-Compilations/Sec High' AS Finding,
        'https://www.brentozar.com/askbrent/recompilations/' AS URL,
        'Number of batch requests during the sample: ' + CAST(ps.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'Number of recompilations during the sample: ' + CAST(psComp.value_delta AS NVARCHAR(20)) + @LineFeed
            + 'More than 10% of our queries are being recompiled. This is typically due to statistics changing on objects.' + @LineFeed AS Details,
        'To find the queries that are being forced to recompile, start with:' + @LineFeed
            + 'sp_BlitzCache @SortOrder = ''recent compilations''' + @LineFeed
            + 'Examine those plans to find out which objects are changing so quickly that they hit the stats update threshold. See the URL for more details.' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name = @ServiceName + ':SQL Statistics' AND psComp.counter_name = 'SQL Re-Compilations/sec' AND psComp.value_delta > 0
    WHERE ps.Pass = 2
        AND ps.object_name = @ServiceName + ':SQL Statistics'
        AND ps.counter_name = 'Batch Requests/sec'
        AND psComp.value_delta > 75 /* Because sp_BlitzFirst does around 50 compilations and re-compilations */
        AND (psComp.value_delta > (10 * @Seconds) OR psComp.value_delta > ps.value_delta) /* Either doing 10 recompilations per second, or more recompilations than queries */
        AND (psComp.value_delta * 10) > ps.value_delta; /* Recompilations are more than 10% of batch requests per second */

    /* Table Problems - Forwarded Fetches/Sec High - CheckID 29 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 29',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 29 AS CheckID,
        40 AS Priority,
        'Table Problems' AS FindingGroup,
        'Forwarded Fetches/Sec High' AS Finding,
        'https://www.brentozar.com/go/fetch/' AS URL,
        CAST(ps.value_delta AS NVARCHAR(20)) + ' forwarded fetches (from SQLServer:Access Methods counter)' + @LineFeed
            + 'Check your heaps: they need to be rebuilt, or they need a clustered index applied.' + @LineFeed AS Details,
        'Rebuild your heaps. If you use Ola Hallengren maintenance scripts, those do not rebuild heaps by default: https://www.brentozar.com/archive/2016/07/fix-forwarded-records/' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name = @ServiceName + ':Access Methods' AND psComp.counter_name = 'Forwarded Records/sec' AND psComp.value_delta > 100
    WHERE ps.Pass = 2
        AND ps.object_name = @ServiceName + ':Access Methods'
        AND ps.counter_name = 'Forwarded Records/sec'
        AND ps.value_delta > (100 * @Seconds); /* Ignore servers sitting idle */

	/* Check for temp objects with high forwarded fetches.
		This has to be done as dynamic SQL because we have to execute OBJECT_NAME inside TempDB. */
	IF @@ROWCOUNT > 0
		BEGIN
		SET @StringToExecute = N'
		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
		SELECT TOP 10 29 AS CheckID,
			40 AS Priority,
			''Table Problems'' AS FindingGroup,
			''Forwarded Fetches/Sec High: TempDB Object'' AS Finding,
			''https://www.brentozar.com/go/fetch/'' AS URL,
			CAST(COALESCE(os.forwarded_fetch_count,0) - COALESCE(os_prior.forwarded_fetch_count,0) AS NVARCHAR(20)) + '' forwarded fetches on '' +
				CASE WHEN OBJECT_NAME(os.object_id) IS NULL THEN ''an unknown table ''
				WHEN LEN(OBJECT_NAME(os.object_id)) < 50 THEN ''a table variable, internal identifier '' + OBJECT_NAME(os.object_id)
				ELSE ''a temp table '' + OBJECT_NAME(os.object_id)
				END AS Details,
			''Look through your source code to find the object creating these objects, and tune the creation and population to reduce fetches. See the URL for details.'' AS HowToStopIt
		FROM tempdb.sys.dm_db_index_operational_stats(DB_ID(''tempdb''), NULL, NULL, NULL) os
			LEFT OUTER JOIN #TempdbOperationalStats os_prior ON os.object_id = os_prior.object_id
				AND os.forwarded_fetch_count > os_prior.forwarded_fetch_count
		WHERE os.database_id = DB_ID(''tempdb'')
			AND os.forwarded_fetch_count - COALESCE(os_prior.forwarded_fetch_count,0) > 100
		ORDER BY os.forwarded_fetch_count DESC;'

		EXECUTE sp_executesql @StringToExecute;
		END

    /* In-Memory OLTP - Garbage Collection in Progress - CheckID 31 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 31',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 31 AS CheckID,
        50 AS Priority,
        'In-Memory OLTP' AS FindingGroup,
        'Garbage Collection in Progress' AS Finding,
        'https://www.brentozar.com/go/garbage/' AS URL,
        CAST(ps.value_delta AS NVARCHAR(50)) + ' rows processed (from SQL Server YYYY XTP Garbage Collection:Rows processed/sec counter)'  + @LineFeed 
            + 'This can happen due to memory pressure (causing In-Memory OLTP to shrink its footprint) or' + @LineFeed
            + 'due to transactional workloads that constantly insert/delete data.' AS Details,
        'Sadly, you cannot choose when garbage collection occurs. This is one of the many gotchas of Hekaton. Learn more: http://nedotter.com/archive/2016/04/row-version-lifecycle-for-in-memory-oltp/' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name LIKE '%XTP Garbage Collection' AND psComp.counter_name = 'Rows processed/sec' AND psComp.value_delta > 100
    WHERE ps.Pass = 2
        AND ps.object_name LIKE '%XTP Garbage Collection'
        AND ps.counter_name = 'Rows processed/sec'
        AND ps.value_delta > (100 * @Seconds); /* Ignore servers sitting idle */

    /* In-Memory OLTP - Transactions Aborted - CheckID 32 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 32',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 32 AS CheckID,
        100 AS Priority,
        'In-Memory OLTP' AS FindingGroup,
        'Transactions Aborted' AS Finding,
        'https://www.brentozar.com/go/aborted/' AS URL,
        CAST(ps.value_delta AS NVARCHAR(50)) + ' transactions aborted (from SQL Server YYYY XTP Transactions:Transactions aborted/sec counter)'  + @LineFeed 
            + 'This may indicate that data is changing, or causing folks to retry their transactions, thereby increasing load.' AS Details,
        'Dig into your In-Memory OLTP transactions to figure out which ones are failing and being retried.' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name LIKE '%XTP Transactions' AND psComp.counter_name = 'Transactions aborted/sec' AND psComp.value_delta > 100
    WHERE ps.Pass = 2
        AND ps.object_name LIKE '%XTP Transactions'
        AND ps.counter_name = 'Transactions aborted/sec'
        AND ps.value_delta > (10 * @Seconds); /* Ignore servers sitting idle */

    /* Query Problems - Suboptimal Plans/Sec High - CheckID 33 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 33',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
    SELECT 32 AS CheckID,
        100 AS Priority,
        'Query Problems' AS FindingGroup,
        'Suboptimal Plans/Sec High' AS Finding,
        'https://www.brentozar.com/go/suboptimal/' AS URL,
        CAST(ps.value_delta AS NVARCHAR(50)) + ' plans reported in the ' + CAST(ps.instance_name AS NVARCHAR(100)) + ' workload group (from Workload GroupStats:Suboptimal plans/sec counter)'  + @LineFeed 
            + 'Even if you are not using Resource Governor, it still tracks information about user queries, memory grants, etc.' AS Details,
        'Check out sp_BlitzCache to get more information about recent queries, or try sp_BlitzWho to see currently running queries.' AS HowToStopIt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats psComp ON psComp.Pass = 2 AND psComp.object_name = @ServiceName + ':Workload GroupStats' AND psComp.counter_name = 'Suboptimal plans/sec' AND psComp.value_delta > 100
    WHERE ps.Pass = 2
        AND ps.object_name = @ServiceName + ':Workload GroupStats' 
        AND ps.counter_name = 'Suboptimal plans/sec'
        AND ps.value_delta > (10 * @Seconds); /* Ignore servers sitting idle */

    /* Azure Performance - Database is Maxed Out - CheckID 41 */
    IF SERVERPROPERTY('Edition') = 'SQL Azure'
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 41',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
        SELECT 41 AS CheckID,
            10 AS Priority,
            'Azure Performance' AS FindingGroup,
            'Database is Maxed Out' AS Finding,
            'https://www.brentozar.com/go/maxedout' AS URL,
            N'At ' + CONVERT(NVARCHAR(100), s.end_time ,121) + N', your database approached (or hit) your DTU limits:' + @LineFeed
                + N'Average CPU percent: ' + CAST(avg_cpu_percent AS NVARCHAR(50)) + @LineFeed
                + N'Average data IO percent: ' + CAST(avg_data_io_percent AS NVARCHAR(50)) + @LineFeed
                + N'Average log write percent: ' + CAST(avg_log_write_percent AS NVARCHAR(50)) + @LineFeed
                + N'Max worker percent: ' + CAST(max_worker_percent AS NVARCHAR(50)) + @LineFeed
                + N'Max session percent: ' + CAST(max_session_percent AS NVARCHAR(50)) AS Details,
            'Tune your queries or indexes with sp_BlitzCache or sp_BlitzIndex, or consider upgrading to a higher DTU level.' AS HowToStopIt
        FROM sys.dm_db_resource_stats s
        WHERE s.end_time >= DATEADD(MI, -5, GETDATE())
          AND (avg_cpu_percent > 90
               OR avg_data_io_percent >= 90
               OR avg_log_write_percent >=90
               OR max_worker_percent >= 90
               OR max_session_percent >= 90);
	END

    /* Server Info - Batch Requests per Sec - CheckID 19 */
	IF (@Debug = 1)
	BEGIN
		RAISERROR('Running CheckID 19',10,1) WITH NOWAIT;
	END

    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, DetailsInt)
    SELECT 19 AS CheckID,
        250 AS Priority,
        'Server Info' AS FindingGroup,
        'Batch Requests per Sec' AS Finding,
        'https://www.brentozar.com/go/measure' AS URL,
        CAST(CAST(ps.value_delta AS MONEY) / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS NVARCHAR(20)) AS Details,
        ps.value_delta / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS DetailsInt
    FROM #PerfmonStats ps
        INNER JOIN #PerfmonStats ps1 ON ps.object_name = ps1.object_name AND ps.counter_name = ps1.counter_name AND ps1.Pass = 1
    WHERE ps.Pass = 2
        AND ps.object_name = @ServiceName + ':SQL Statistics'
        AND ps.counter_name = 'Batch Requests/sec';


        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','SQL Compilations/sec', NULL);
        INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) VALUES (@ServiceName + ':SQL Statistics','SQL Re-Compilations/sec', NULL);

    /* Server Info - SQL Compilations/sec - CheckID 25 */
    IF @ExpertMode = 1
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 25',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, DetailsInt)
		SELECT 25 AS CheckID,
		    250 AS Priority,
		    'Server Info' AS FindingGroup,
		    'SQL Compilations per Sec' AS Finding,
		    'https://www.brentozar.com/go/measure' AS URL,
		    CAST(ps.value_delta / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS NVARCHAR(20)) AS Details,
		    ps.value_delta / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS DetailsInt
		FROM #PerfmonStats ps
		    INNER JOIN #PerfmonStats ps1 ON ps.object_name = ps1.object_name AND ps.counter_name = ps1.counter_name AND ps1.Pass = 1
		WHERE ps.Pass = 2
		    AND ps.object_name = @ServiceName + ':SQL Statistics'
		    AND ps.counter_name = 'SQL Compilations/sec';
	END

    /* Server Info - SQL Re-Compilations/sec - CheckID 26 */
    IF @ExpertMode = 1
	BEGIN
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 26',10,1) WITH NOWAIT;
		END

		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, DetailsInt)
		SELECT 26 AS CheckID,
		    250 AS Priority,
		    'Server Info' AS FindingGroup,
		    'SQL Re-Compilations per Sec' AS Finding,
		    'https://www.brentozar.com/go/measure' AS URL,
		    CAST(ps.value_delta / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS NVARCHAR(20)) AS Details,
		    ps.value_delta / (DATEDIFF(ss, ps1.SampleTime, ps.SampleTime)) AS DetailsInt
		FROM #PerfmonStats ps
		    INNER JOIN #PerfmonStats ps1 ON ps.object_name = ps1.object_name AND ps.counter_name = ps1.counter_name AND ps1.Pass = 1
		WHERE ps.Pass = 2
		    AND ps.object_name = @ServiceName + ':SQL Statistics'
		    AND ps.counter_name = 'SQL Re-Compilations/sec';
	END

    /* Server Info - Wait Time per Core per Sec - CheckID 20 */
    IF @Seconds > 0
    BEGIN;
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 20',10,1) WITH NOWAIT;
		END;

        WITH waits1(SampleTime, waits_ms) AS (SELECT SampleTime, SUM(ws1.wait_time_ms) FROM #WaitStats ws1 WHERE ws1.Pass = 1 GROUP BY SampleTime),
        waits2(SampleTime, waits_ms) AS (SELECT SampleTime, SUM(ws2.wait_time_ms) FROM #WaitStats ws2 WHERE ws2.Pass = 2 GROUP BY SampleTime),
        cores(cpu_count) AS (SELECT SUM(1) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE' AND is_online = 1)
        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, DetailsInt)
        SELECT 20 AS CheckID,
            250 AS Priority,
            'Server Info' AS FindingGroup,
            'Wait Time per Core per Sec' AS Finding,
            'https://www.brentozar.com/go/measure' AS URL,
            CAST((CAST(waits2.waits_ms - waits1.waits_ms AS MONEY)) / 1000 / i.cpu_count / DATEDIFF(ss, waits1.SampleTime, waits2.SampleTime) AS NVARCHAR(20)) AS Details,
            (waits2.waits_ms - waits1.waits_ms) / 1000 / i.cpu_count / DATEDIFF(ss, waits1.SampleTime, waits2.SampleTime) AS DetailsInt
        FROM cores i
          CROSS JOIN waits1
          CROSS JOIN waits2;
    END;

	IF @Seconds > 0
	BEGIN
    INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
    SELECT
        47 AS CheckId,
    	50 AS Priority,
    	'Query Problems' AS FindingsGroup,
    	'High Percentage Of Runnable Queries' AS Finding, 
    	'https://erikdarlingdata.com/go/RunnableQueue/' AS URL, 
    	'On the ' 
    	+ CASE WHEN y.pass = 1 
    	       THEN '1st' 
    		   ELSE '2nd'
    	   END
    	+ ' pass, '
    	+ RTRIM(y.runnable_pct)
    	+ '% of your queries were waiting to get on a CPU to run. '
    	+ ' This can indicate CPU pressure.'
    FROM
    (
        SELECT 
            2 AS pass,
            x.total, 
        	x.runnable,
            CONVERT(decimal(5,2),
                (
                    x.runnable / 
                        (1. * NULLIF(x.total, 0))
                )
            ) * 100. AS runnable_pct
        FROM 
        (
            SELECT 
                COUNT_BIG(*) AS total, 
                SUM(CASE WHEN status = 'runnable' 
        		         THEN 1 
        				 ELSE 0 
        		    END) AS runnable
            FROM sys.dm_exec_requests
            WHERE session_id > 50
        ) AS x
    ) AS y
    WHERE y.runnable_pct > 20.;
	END

    /* If we're waiting 30+ seconds, run these checks at the end.
    We get this data from the ring buffers, and it's only updated once per minute, so might
    as well get it now - whereas if we're checking 30+ seconds, it might get updated by the
    end of our sp_BlitzFirst session. */
    IF @Seconds >= 30
    BEGIN
        /* Server Performance - High CPU Utilization CheckID 24 */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 24',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
        SELECT 24, 50, 'Server Performance', 'High CPU Utilization', CAST(100 - SystemIdle AS NVARCHAR(20)) + N'%. Ring buffer details: ' + CAST(record AS NVARCHAR(4000)), 100 - SystemIdle, 'https://www.brentozar.com/go/cpu'
            FROM (
                SELECT record,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
                FROM (
                    SELECT TOP 1 CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%'
                    ORDER BY timestamp DESC) AS rb
            ) AS y
            WHERE 100 - SystemIdle >= 50;

        /* Server Performance - CPU Utilization CheckID 23 */
		IF (@Debug = 1)
		BEGIN
			RAISERROR('Running CheckID 23',10,1) WITH NOWAIT;
		END

        INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, DetailsInt, URL)
        SELECT 23, 250, 'Server Info', 'CPU Utilization', CAST(100 - SystemIdle AS NVARCHAR(20)) + N'%. Ring buffer details: ' + CAST(record AS NVARCHAR(4000)), 100 - SystemIdle, 'https://www.brentozar.com/go/cpu'
            FROM (
                SELECT record,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
                FROM (
                    SELECT TOP 1 CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%'
                    ORDER BY timestamp DESC) AS rb
            ) AS y;

	END; /* IF @Seconds >= 30 */

	IF /* Let people on <2016 know about the thread time column */
	(
	    @Seconds > 0
		AND @total_cpu_usage = 0
	)
	BEGIN
	    INSERT INTO
		    #BlitzFirstResults
		(
		    CheckID,
			Priority,
			FindingsGroup,
			Finding,
			Details,
			URL
		)
		SELECT
		    48,
			254,
			N'Informational',
			N'Thread Time comes from the plan cache in versions earlier than 2016, and is not as reliable',
			N'The oldest plan in your cache is from ' +
			CONVERT(nvarchar(30), MIN(s.creation_time)) +
			N' and your server was last restarted on ' +
			CONVERT(nvarchar(30), MAX(o.sqlserver_start_time)),
			N'https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-schedulers-transact-sql'
        FROM sys.dm_exec_query_stats AS s
        CROSS JOIN sys.dm_os_sys_info AS o
        OPTION(RECOMPILE);
	END /* Let people on <2016 know about the thread time column */

    /* If we didn't find anything, apologize. */
    IF NOT EXISTS (SELECT * FROM #BlitzFirstResults WHERE Priority < 250)
    BEGIN

        INSERT  INTO #BlitzFirstResults
                ( CheckID ,
                  Priority ,
                  FindingsGroup ,
                  Finding ,
                  URL ,
                  Details
                )
        VALUES  ( -1 ,
                  1 ,
                  'No Problems Found' ,
                  'From Your Community Volunteers' ,
                  'http://FirstResponderKit.org/' ,
                  'Try running our more in-depth checks with sp_Blitz, or there may not be an unusual SQL Server performance problem. '
                );

    END; /*IF NOT EXISTS (SELECT * FROM #BlitzFirstResults) */

        /* Add credits for the nice folks who put so much time into building and maintaining this for free: */
        INSERT  INTO #BlitzFirstResults
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
                  'From Your Community Volunteers' ,
                  'http://FirstResponderKit.org/' ,
                  'To get help or add your own contributions, join us at http://FirstResponderKit.org.'
                );

        INSERT  INTO #BlitzFirstResults
                ( CheckID ,
                  Priority ,
                  FindingsGroup ,
                  Finding ,
                  URL ,
                  Details

                )
        VALUES  ( -1 ,
                  0 ,
                  'sp_BlitzFirst ' + CAST(CONVERT(DATETIMEOFFSET, @VersionDate, 102) AS VARCHAR(100)),
                  'From Your Community Volunteers' ,
                  'http://FirstResponderKit.org/' ,
                  'We hope you found this tool useful.'
                );

                /* Outdated sp_BlitzFirst - sp_BlitzFirst is Over 6 Months Old - CheckID 27 */
				IF (@Debug = 1)
				BEGIN
					RAISERROR('Running CheckID 27',10,1) WITH NOWAIT;
				END

                IF DATEDIFF(MM, @VersionDate, SYSDATETIMEOFFSET()) > 6
                    BEGIN
                        INSERT  INTO #BlitzFirstResults
                                ( CheckID ,
                                    Priority ,
                                    FindingsGroup ,
                                    Finding ,
                                    URL ,
                                    Details
                                )
                                SELECT 27 AS CheckID ,
                                        0 AS Priority ,
                                        'Outdated sp_BlitzFirst' AS FindingsGroup ,
                                        'sp_BlitzFirst is Over 6 Months Old' AS Finding ,
                                        'http://FirstResponderKit.org/' AS URL ,
                                        'Some things get better with age, like fine wine and your T-SQL. However, sp_BlitzFirst is not one of those things - time to go download the current one.' AS Details;
                    END;

    IF @CheckServerInfo = 0 /* Github #1680 */
        BEGIN
        DELETE #BlitzFirstResults
          WHERE FindingsGroup = 'Server Info';
        END

    RAISERROR('Analysis finished, outputting results',10,1) WITH NOWAIT;


    /* If they want to run sp_BlitzCache and export to table, go for it. */
    IF @OutputTableNameBlitzCache IS NOT NULL
        AND @OutputDatabaseName IS NOT NULL
        AND @OutputSchemaName IS NOT NULL
        AND EXISTS ( SELECT *
                     FROM   sys.databases
                     WHERE  QUOTENAME([name]) = @OutputDatabaseName)
    BEGIN


		RAISERROR('Calling sp_BlitzCache',10,1) WITH NOWAIT;


        /* If they have an newer version of sp_BlitzCache that supports @MinutesBack and @CheckDateOverride */
        IF EXISTS (SELECT * FROM sys.objects o 
                        INNER JOIN sys.parameters pMB ON o.object_id = pMB.object_id AND pMB.name = '@MinutesBack'
                        INNER JOIN sys.parameters pCDO ON o.object_id = pCDO.object_id AND pCDO.name = '@CheckDateOverride'
                        WHERE o.name = 'sp_BlitzCache')
            BEGIN
                /* Get the most recent sp_BlitzCache execution before this one - don't use sp_BlitzFirst because user logs are added in there at any time */
                SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
                    + @OutputDatabaseName
                    + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
                    + @OutputSchemaName + ''' AND QUOTENAME(TABLE_NAME) = '''
                    + QUOTENAME(@OutputTableNameBlitzCache) + ''') SELECT TOP 1 @BlitzCacheMinutesBack = DATEDIFF(MI,CheckDate,SYSDATETIMEOFFSET()) FROM '
                    + @OutputDatabaseName + '.'
                    + @OutputSchemaName + '.'
                    + QUOTENAME(@OutputTableNameBlitzCache)
                    + ' WHERE ServerName = ''' + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)) + ''' ORDER BY CheckDate DESC;';
                EXEC sp_executesql @StringToExecute, N'@BlitzCacheMinutesBack INT OUTPUT', @BlitzCacheMinutesBack OUTPUT;

                /* If there's no data, let's just analyze the last 15 minutes of the plan cache */
                IF @BlitzCacheMinutesBack IS NULL OR @BlitzCacheMinutesBack < 1 OR @BlitzCacheMinutesBack > 60
                    SET @BlitzCacheMinutesBack = 15;

                IF(@OutputType = 'NONE')
				BEGIN 
                    EXEC sp_BlitzCache
                        @OutputDatabaseName = @UnquotedOutputDatabaseName,
                        @OutputSchemaName = @UnquotedOutputSchemaName,
                        @OutputTableName = @OutputTableNameBlitzCache,
                        @CheckDateOverride = @StartSampleTime,
                        @SortOrder = 'all',
                        @SkipAnalysis = @BlitzCacheSkipAnalysis,
                        @MinutesBack = @BlitzCacheMinutesBack,
                        @Debug = @Debug,
					    @OutputType = @OutputType
				    ;
				END;
				ELSE
				BEGIN
				
				    EXEC sp_BlitzCache
                        @OutputDatabaseName = @UnquotedOutputDatabaseName,
                        @OutputSchemaName = @UnquotedOutputSchemaName,
                        @OutputTableName = @OutputTableNameBlitzCache,
                        @CheckDateOverride = @StartSampleTime,
                        @SortOrder = 'all',
                        @SkipAnalysis = @BlitzCacheSkipAnalysis,
                        @MinutesBack = @BlitzCacheMinutesBack,
                        @Debug = @Debug
				    ;
				END;

                /* Delete history older than @OutputTableRetentionDays */
                SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
                    + @OutputDatabaseName
                    + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                    + @OutputSchemaName + ''') DELETE '
                    + @OutputDatabaseName + '.'
                    + @OutputSchemaName + '.'
                    + QUOTENAME(@OutputTableNameBlitzCache)
                    + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate;';
                EXEC sp_executesql @StringToExecute,
					N'@SrvName NVARCHAR(128), @CheckDate date',
					@LocalServerName, @OutputTableCleanupDate;


            END;

        ELSE 
            BEGIN
                /* No sp_BlitzCache found, or it's outdated - CheckID 36 */
				IF (@Debug = 1)
				BEGIN
					RAISERROR('Running CheckID 36',10,1) WITH NOWAIT;
				END

                INSERT  INTO #BlitzFirstResults
                        ( CheckID ,
                            Priority ,
                            FindingsGroup ,
                            Finding ,
                            URL ,
                            Details
                        )
                        SELECT 36 AS CheckID ,
                                0 AS Priority ,
                                'Outdated or Missing sp_BlitzCache' AS FindingsGroup ,
                                'Update Your sp_BlitzCache' AS Finding ,
                                'http://FirstResponderKit.org/' AS URL ,
                                'You passed in @OutputTableNameBlitzCache, but we need a newer version of sp_BlitzCache in master or the current database.' AS Details;
            END;

    	RAISERROR('sp_BlitzCache Finished',10,1) WITH NOWAIT;

    END; /* End running sp_BlitzCache */

    /* @OutputTableName lets us export the results to a permanent table */
    IF @OutputDatabaseName IS NOT NULL
        AND @OutputSchemaName IS NOT NULL
        AND @OutputTableName IS NOT NULL
        AND @OutputTableName NOT LIKE '#%'
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
                CheckDate DATETIMEOFFSET,
                CheckID INT NOT NULL,
                Priority TINYINT NOT NULL,
                FindingsGroup VARCHAR(50) NOT NULL,
                Finding VARCHAR(200) NOT NULL,
                URL VARCHAR(200) NOT NULL,
                Details NVARCHAR(4000) NULL,
                HowToStopIt [XML] NULL,
                QueryPlan [XML] NULL,
                QueryText NVARCHAR(MAX) NULL,
                StartTime DATETIMEOFFSET NULL,
                LoginName NVARCHAR(128) NULL,
                NTUserName NVARCHAR(128) NULL,
                OriginalLoginName NVARCHAR(128) NULL,
                ProgramName NVARCHAR(128) NULL,
                HostName NVARCHAR(128) NULL,
                DatabaseID INT NULL,
                DatabaseName NVARCHAR(128) NULL,
                OpenTransactionCount INT NULL,
                DetailsInt INT NULL,
                QueryHash BINARY(8) NULL,
                JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
                PRIMARY KEY CLUSTERED (ID ASC));';

        EXEC(@StringToExecute);

        /* If the table doesn't have the new QueryHash column, add it. See Github #2162. */
        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
        SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
            WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''QueryHash'')
            ALTER TABLE ' + @ObjectFullName + N' ADD QueryHash BINARY(8) NULL;';
        EXEC(@StringToExecute);

        /* If the table doesn't have the new JoinKey computed column, add it. See Github #2164. */
        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
        SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
            WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
            ALTER TABLE ' + @ObjectFullName + N' ADD JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50));';
        EXEC(@StringToExecute);

        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') INSERT '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableName
            + ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt, QueryHash) SELECT '
            + ' @SrvName, @CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, LEFT(Details,4000), HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt, QueryHash FROM #BlitzFirstResults ORDER BY Priority , FindingsGroup , Finding , Details';
		
		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;

        /* Delete history older than @OutputTableRetentionDays */
        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') DELETE '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableName
            + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';
		
		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate date',
			@LocalServerName, @OutputTableCleanupDate;

    END;
    ELSE IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
    BEGIN
        SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
            + @OutputTableName
            + ''') IS NULL) CREATE TABLE '
            + @OutputTableName
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                CheckID INT NOT NULL,
                Priority TINYINT NOT NULL,
                FindingsGroup VARCHAR(50) NOT NULL,
                Finding VARCHAR(200) NOT NULL,
                URL VARCHAR(200) NOT NULL,
                Details NVARCHAR(4000) NULL,
                HowToStopIt [XML] NULL,
                QueryPlan [XML] NULL,
                QueryText NVARCHAR(MAX) NULL,
                StartTime DATETIMEOFFSET NULL,
                LoginName NVARCHAR(128) NULL,
                NTUserName NVARCHAR(128) NULL,
                OriginalLoginName NVARCHAR(128) NULL,
                ProgramName NVARCHAR(128) NULL,
                HostName NVARCHAR(128) NULL,
                DatabaseID INT NULL,
                DatabaseName NVARCHAR(128) NULL,
                OpenTransactionCount INT NULL,
                DetailsInt INT NULL,
                QueryHash BINARY(8) NULL,
                JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
                PRIMARY KEY CLUSTERED (ID ASC));'
            + ' INSERT '
            + @OutputTableName
            + ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt) SELECT '
            + ' @SrvName, @CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, LEFT(Details,4000), HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt FROM #BlitzFirstResults ORDER BY Priority , FindingsGroup , Finding , Details';
		
		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;
    END;
    ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
    BEGIN
        RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
    END;

    /* @OutputTableNameFileStats lets us export the results to a permanent table */
    IF @OutputDatabaseName IS NOT NULL
        AND @OutputSchemaName IS NOT NULL
        AND @OutputTableNameFileStats IS NOT NULL
        AND @OutputTableNameFileStats NOT LIKE '#%'
        AND EXISTS ( SELECT *
                     FROM   sys.databases
                     WHERE  QUOTENAME([name]) = @OutputDatabaseName)
    BEGIN
        /* Create the table */
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
            + @OutputTableNameFileStats + ''') CREATE TABLE '
            + @OutputSchemaName + '.'
            + @OutputTableNameFileStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                DatabaseID INT NOT NULL,
                FileID INT NOT NULL,
                DatabaseName NVARCHAR(256) ,
                FileLogicalName NVARCHAR(256) ,
                TypeDesc NVARCHAR(60) ,
                SizeOnDiskMB BIGINT ,
                io_stall_read_ms BIGINT ,
                num_of_reads BIGINT ,
                bytes_read BIGINT ,
                io_stall_write_ms BIGINT ,
                num_of_writes BIGINT ,
                bytes_written BIGINT,
                PhysicalName NVARCHAR(520) ,
                PRIMARY KEY CLUSTERED (ID ASC));';

		EXEC(@StringToExecute);

        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNameFileStats_View;

        /* If the view exists without the most recently added columns, drop it. See Github #2162. */
        IF OBJECT_ID(@ObjectFullName) IS NOT NULL
            BEGIN
            SET @StringToExecute = N'USE ' + @OutputDatabaseName + N'; IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @OutputSchemaName + N'.' + @OutputTableNameFileStats_View + N';';

            EXEC(@StringToExecute);
            END

        /* Create the view */
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = 'USE '
                + @OutputDatabaseName
                + '; EXEC (''CREATE VIEW '
                + @OutputSchemaName + '.'
                + @OutputTableNameFileStats_View + ' AS ' + @LineFeed
                + 'WITH RowDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ' + @LineFeed
                + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' + @LineFeed
                + '                [CheckDate]' + @LineFeed
                + '        FROM ' + @OutputSchemaName + '.' + @OutputTableNameFileStats + '' + @LineFeed
                + '        GROUP BY [ServerName], [CheckDate]' + @LineFeed
                + '),' + @LineFeed
                + 'CheckDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ThisDate.CheckDate,' + @LineFeed
                + '               LastDate.CheckDate as PreviousCheckDate' + @LineFeed
                + '        FROM RowDates ThisDate' + @LineFeed
                + '        JOIN RowDates LastDate' + @LineFeed
                + '        ON ThisDate.ID = LastDate.ID + 1' + @LineFeed
                + ')' + @LineFeed
                + '     SELECT f.ServerName,' + @LineFeed
                + '            f.CheckDate,' + @LineFeed
                + '            f.DatabaseID,' + @LineFeed
                + '            f.DatabaseName,' + @LineFeed
                + '            f.FileID,' + @LineFeed
                + '            f.FileLogicalName,' + @LineFeed
                + '            f.TypeDesc,' + @LineFeed
                + '            f.PhysicalName,' + @LineFeed
                + '            f.SizeOnDiskMB,' + @LineFeed
                + '            DATEDIFF(ss, fPrior.CheckDate, f.CheckDate) AS ElapsedSeconds,' + @LineFeed
                + '            (f.SizeOnDiskMB - fPrior.SizeOnDiskMB) AS SizeOnDiskMBgrowth,' + @LineFeed
                + '            (f.io_stall_read_ms - fPrior.io_stall_read_ms) AS io_stall_read_ms,' + @LineFeed
                + '            io_stall_read_ms_average = CASE' + @LineFeed
                + '                                           WHEN(f.num_of_reads - fPrior.num_of_reads) = 0' + @LineFeed
                + '                                           THEN 0' + @LineFeed
                + '                                           ELSE(f.io_stall_read_ms - fPrior.io_stall_read_ms) /     (f.num_of_reads   -           fPrior.num_of_reads)' + @LineFeed
                + '                                       END,' + @LineFeed
                + '            (f.num_of_reads - fPrior.num_of_reads) AS num_of_reads,' + @LineFeed
                + '            (f.bytes_read - fPrior.bytes_read) / 1024.0 / 1024.0 AS megabytes_read,' + @LineFeed
                + '            (f.io_stall_write_ms - fPrior.io_stall_write_ms) AS io_stall_write_ms,' + @LineFeed
                + '            io_stall_write_ms_average = CASE' + @LineFeed
                + '                                            WHEN(f.num_of_writes - fPrior.num_of_writes) = 0' + @LineFeed
                + '                                            THEN 0' + @LineFeed
                + '                                            ELSE(f.io_stall_write_ms - fPrior.io_stall_write_ms) /         (f.num_of_writes   -       fPrior.num_of_writes)' + @LineFeed
                + '                                        END,' + @LineFeed
                + '            (f.num_of_writes - fPrior.num_of_writes) AS num_of_writes,' + @LineFeed
                + '            (f.bytes_written - fPrior.bytes_written) / 1024.0 / 1024.0 AS megabytes_written, ' + @LineFeed
                + '            f.ServerName + CAST(f.CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + '     FROM   ' + @OutputSchemaName + '.' + @OutputTableNameFileStats + ' f' + @LineFeed
                + '            INNER HASH JOIN CheckDates DATES ON f.CheckDate = DATES.CheckDate' + @LineFeed
                + '            INNER JOIN ' + @OutputSchemaName + '.' + @OutputTableNameFileStats + ' fPrior ON f.ServerName =                 fPrior.ServerName' + @LineFeed
                + '                                                              AND f.DatabaseID = fPrior.DatabaseID' +     @LineFeed
                + '                                                              AND f.FileID = fPrior.FileID' + @LineFeed
                + '                                                              AND fPrior.CheckDate =   DATES.PreviousCheckDate'   +           @LineFeed
                + '' + @LineFeed
                + '     WHERE  f.num_of_reads >= fPrior.num_of_reads' + @LineFeed
                + '            AND f.num_of_writes >= fPrior.num_of_writes' + @LineFeed
                + '            AND DATEDIFF(MI, fPrior.CheckDate, f.CheckDate) BETWEEN 1 AND 60;'')'

			EXEC(@StringToExecute);
            END;


        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') INSERT '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNameFileStats
            + ' (ServerName, CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName) SELECT '
            + ' @SrvName, @CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName FROM #FileStats WHERE Pass = 2';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;

        /* Delete history older than @OutputTableRetentionDays */
        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') DELETE '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNameFileStats
            + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate date',
			@LocalServerName, @OutputTableCleanupDate;

    END;
    ELSE IF (SUBSTRING(@OutputTableNameFileStats, 2, 2) = '##')
    BEGIN
        SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
            + @OutputTableNameFileStats
            + ''') IS NULL) CREATE TABLE '
            + @OutputTableNameFileStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                DatabaseID INT NOT NULL,
                FileID INT NOT NULL,
                DatabaseName NVARCHAR(256) ,
                FileLogicalName NVARCHAR(256) ,
                TypeDesc NVARCHAR(60) ,
                SizeOnDiskMB BIGINT ,
                io_stall_read_ms BIGINT ,
                num_of_reads BIGINT ,
                bytes_read BIGINT ,
                io_stall_write_ms BIGINT ,
                num_of_writes BIGINT ,
                bytes_written BIGINT,
                PhysicalName NVARCHAR(520) ,
                DetailsInt INT NULL,
                PRIMARY KEY CLUSTERED (ID ASC));'
            + ' INSERT '
            + @OutputTableNameFileStats
            + ' (ServerName, CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName) SELECT '
            + ' @SrvName, @CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName FROM #FileStats WHERE Pass = 2';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;
    END;
    ELSE IF (SUBSTRING(@OutputTableNameFileStats, 2, 1) = '#')
    BEGIN
        RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
    END;


    /* @OutputTableNamePerfmonStats lets us export the results to a permanent table */
    IF @OutputDatabaseName IS NOT NULL
        AND @OutputSchemaName IS NOT NULL
        AND @OutputTableNamePerfmonStats IS NOT NULL
        AND @OutputTableNamePerfmonStats NOT LIKE '#%'
        AND EXISTS ( SELECT *
                     FROM   sys.databases
                     WHERE  QUOTENAME([name]) = @OutputDatabaseName)
    BEGIN
        /* Create the table */
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
            + @OutputTableNamePerfmonStats + ''') CREATE TABLE '
            + @OutputSchemaName + '.'
            + @OutputTableNamePerfmonStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                [object_name] NVARCHAR(128) NOT NULL,
                [counter_name] NVARCHAR(128) NOT NULL,
                [instance_name] NVARCHAR(128) NULL,
                [cntr_value] BIGINT NULL,
                [cntr_type] INT NOT NULL,
                [value_delta] BIGINT NULL,
                [value_per_second] DECIMAL(18,2) NULL,
                PRIMARY KEY CLUSTERED (ID ASC));';

		EXEC(@StringToExecute);

        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNamePerfmonStats_View;

        /* If the view exists without the most recently added columns, drop it. See Github #2162. */
        IF OBJECT_ID(@ObjectFullName) IS NOT NULL
            BEGIN
            SET @StringToExecute = N'USE ' + @OutputDatabaseName + N'; IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @OutputSchemaName + N'.' + @OutputTableNamePerfmonStats_View + N';';

            EXEC(@StringToExecute);
            END

        /* Create the view */
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = 'USE '
                + @OutputDatabaseName
                + '; EXEC (''CREATE VIEW '
                + @OutputSchemaName + '.'
                + @OutputTableNamePerfmonStats_View + ' AS ' + @LineFeed
                + 'WITH RowDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ' + @LineFeed
                + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' + @LineFeed
                + '                [CheckDate]' + @LineFeed
                + '        FROM ' + @OutputSchemaName + '.' +@OutputTableNamePerfmonStats + '' + @LineFeed
                + '        GROUP BY [ServerName], [CheckDate]' + @LineFeed
                + '),' + @LineFeed
                + 'CheckDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ThisDate.CheckDate,' + @LineFeed
                + '               LastDate.CheckDate as PreviousCheckDate' + @LineFeed
                + '        FROM RowDates ThisDate' + @LineFeed
                + '        JOIN RowDates LastDate' + @LineFeed
                + '        ON ThisDate.ID = LastDate.ID + 1' + @LineFeed
                + ')' + @LineFeed
                + 'SELECT' + @LineFeed
                + '       pMon.[ServerName]' + @LineFeed
                + '      ,pMon.[CheckDate]' + @LineFeed
                + '      ,pMon.[object_name]' + @LineFeed
                + '      ,pMon.[counter_name]' + @LineFeed
                + '      ,pMon.[instance_name]' + @LineFeed
                + '      ,DATEDIFF(SECOND,pMonPrior.[CheckDate],pMon.[CheckDate]) AS ElapsedSeconds' + @LineFeed
                + '      ,pMon.[cntr_value]' + @LineFeed
                + '      ,pMon.[cntr_type]' + @LineFeed
                + '      ,(pMon.[cntr_value] - pMonPrior.[cntr_value]) AS cntr_delta' + @LineFeed
                + '      ,(pMon.cntr_value - pMonPrior.cntr_value) * 1.0 / DATEDIFF(ss, pMonPrior.CheckDate, pMon.CheckDate) AS cntr_delta_per_second' + @LineFeed
                + '      ,pMon.ServerName + CAST(pMon.CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + '  FROM ' + @OutputSchemaName + '.' +@OutputTableNamePerfmonStats + ' pMon' + @LineFeed
                + '  INNER HASH JOIN CheckDates Dates' + @LineFeed
                + '  ON Dates.CheckDate = pMon.CheckDate' + @LineFeed
                + '  JOIN ' + @OutputSchemaName + '.' +@OutputTableNamePerfmonStats + ' pMonPrior' + @LineFeed
                + '  ON  Dates.PreviousCheckDate = pMonPrior.CheckDate' + @LineFeed
                + '      AND pMon.[ServerName]    = pMonPrior.[ServerName]   ' + @LineFeed
                + '      AND pMon.[object_name]   = pMonPrior.[object_name]  ' + @LineFeed
                + '      AND pMon.[counter_name]  = pMonPrior.[counter_name] ' + @LineFeed
                + '      AND pMon.[instance_name] = pMonPrior.[instance_name]' + @LineFeed
                + '    WHERE DATEDIFF(MI, pMonPrior.CheckDate, pMon.CheckDate) BETWEEN 1 AND 60;'')'

			EXEC(@StringToExecute);
            END

        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNamePerfmonStatsActuals_View;

        /* If the view exists without the most recently added columns, drop it. See Github #2162. */
        IF OBJECT_ID(@ObjectFullName) IS NOT NULL
            BEGIN
            SET @StringToExecute = N'USE ' + @OutputDatabaseName + N'; IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @OutputSchemaName + N'.' + @OutputTableNamePerfmonStatsActuals_View + N';';

            EXEC(@StringToExecute);
            END

        /* Create the second view */
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = 'USE '
                + @OutputDatabaseName
                + '; EXEC (''CREATE VIEW '
                + @OutputSchemaName + '.'
                + @OutputTableNamePerfmonStatsActuals_View + ' AS ' + @LineFeed
                + 'WITH PERF_AVERAGE_BULK AS' + @LineFeed
                + '(' + @LineFeed
                + '    SELECT ServerName,' + @LineFeed
                + '           object_name,' + @LineFeed
                + '           instance_name,' + @LineFeed
                + '           counter_name,' + @LineFeed
                + '           CASE WHEN CHARINDEX(''''('''', counter_name) = 0 THEN counter_name ELSE LEFT (counter_name, CHARINDEX(''''('''',counter_name)-1) END    AS   counter_join,' + @LineFeed
                + '           CheckDate,' + @LineFeed
                + '           cntr_delta' + @LineFeed
                + '    FROM   ' + @OutputSchemaName + '.' + @OutputTableNamePerfmonStats_View + @LineFeed
                + '    WHERE  cntr_type IN(1073874176)' + @LineFeed
                + '    AND cntr_delta <> 0' + @LineFeed
                + '),' + @LineFeed
                + 'PERF_LARGE_RAW_BASE AS' + @LineFeed
                + '(' + @LineFeed
                + '    SELECT ServerName,' + @LineFeed
                + '           object_name,' + @LineFeed
                + '           instance_name,' + @LineFeed
                + '           LEFT(counter_name, CHARINDEX(''''BASE'''', UPPER(counter_name))-1) AS counter_join,' + @LineFeed
                + '           CheckDate,' + @LineFeed
                + '           cntr_delta' + @LineFeed
                + '    FROM   ' + @OutputSchemaName + '.' + @OutputTableNamePerfmonStats_View + '' + @LineFeed
                + '    WHERE  cntr_type IN(1073939712)' + @LineFeed
                + '    AND cntr_delta <> 0' + @LineFeed
                + '),' + @LineFeed
                + 'PERF_AVERAGE_FRACTION AS' + @LineFeed
                + '(' + @LineFeed
                + '    SELECT ServerName,' + @LineFeed
                + '           object_name,' + @LineFeed
                + '           instance_name,' + @LineFeed
                + '           counter_name,' + @LineFeed
                + '           counter_name AS counter_join,' + @LineFeed
                + '           CheckDate,' + @LineFeed
                + '           cntr_delta' + @LineFeed
                + '    FROM   ' + @OutputSchemaName + '.' + @OutputTableNamePerfmonStats_View + '' + @LineFeed
                + '    WHERE  cntr_type IN(537003264)' + @LineFeed
                + '    AND cntr_delta <> 0' + @LineFeed
                + '),' + @LineFeed
                + 'PERF_COUNTER_BULK_COUNT AS' + @LineFeed
                + '(' + @LineFeed
                + '    SELECT ServerName,' + @LineFeed
                + '           object_name,' + @LineFeed
                + '           instance_name,' + @LineFeed
                + '           counter_name,' + @LineFeed
                + '           CheckDate,' + @LineFeed
                + '           cntr_delta / ElapsedSeconds AS cntr_value' + @LineFeed
                + '    FROM   ' + @OutputSchemaName + '.' + @OutputTableNamePerfmonStats_View + '' + @LineFeed
                + '    WHERE  cntr_type IN(272696576, 272696320)' + @LineFeed
                + '    AND cntr_delta <> 0' + @LineFeed
                + '),' + @LineFeed
                + 'PERF_COUNTER_RAWCOUNT AS' + @LineFeed
                + '(' + @LineFeed
                + '    SELECT ServerName,' + @LineFeed
                + '           object_name,' + @LineFeed
                + '           instance_name,' + @LineFeed
                + '           counter_name,' + @LineFeed
                + '           CheckDate,' + @LineFeed
                + '           cntr_value' + @LineFeed
                + '    FROM   ' + @OutputSchemaName + '.' + @OutputTableNamePerfmonStats_View + '' + @LineFeed
                + '    WHERE  cntr_type IN(65792, 65536)' + @LineFeed
                + ')' + @LineFeed
                + '' + @LineFeed
                + 'SELECT NUM.ServerName,' + @LineFeed
                + '       NUM.object_name,' + @LineFeed
                + '       NUM.counter_name,' + @LineFeed
                + '       NUM.instance_name,' + @LineFeed
                + '       NUM.CheckDate,' + @LineFeed
                + '       NUM.cntr_delta / DEN.cntr_delta AS cntr_value,' + @LineFeed
                + '       NUM.ServerName + CAST(NUM.CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + '       ' + @LineFeed
                + 'FROM   PERF_AVERAGE_BULK AS NUM' + @LineFeed
                + '       JOIN PERF_LARGE_RAW_BASE AS DEN ON NUM.counter_join = DEN.counter_join' + @LineFeed
                + '                                          AND NUM.CheckDate = DEN.CheckDate' + @LineFeed
                + '                                          AND NUM.ServerName = DEN.ServerName' + @LineFeed
                + '                                          AND NUM.object_name = DEN.object_name' + @LineFeed
                + '                                          AND NUM.instance_name = DEN.instance_name' + @LineFeed
                + '                                          AND DEN.cntr_delta <> 0' + @LineFeed
                + '' + @LineFeed
                + 'UNION ALL' + @LineFeed
                + '' + @LineFeed
                + 'SELECT NUM.ServerName,' + @LineFeed
                + '       NUM.object_name,' + @LineFeed
                + '       NUM.counter_name,' + @LineFeed
                + '       NUM.instance_name,' + @LineFeed
                + '       NUM.CheckDate,' + @LineFeed
                + '       CAST((CAST(NUM.cntr_delta as DECIMAL(19)) / DEN.cntr_delta) as decimal(23,3))  AS cntr_value,' +         @LineFeed
                + '       NUM.ServerName + CAST(NUM.CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + 'FROM   PERF_AVERAGE_FRACTION AS NUM' + @LineFeed
                + '       JOIN PERF_LARGE_RAW_BASE AS DEN ON NUM.counter_join = DEN.counter_join' + @LineFeed
                + '                                          AND NUM.CheckDate = DEN.CheckDate' + @LineFeed
                + '                                          AND NUM.ServerName = DEN.ServerName' + @LineFeed
                + '                                          AND NUM.object_name = DEN.object_name' + @LineFeed
                + '                                          AND NUM.instance_name = DEN.instance_name' + @LineFeed
                + '                                          AND DEN.cntr_delta <> 0' + @LineFeed
                + 'UNION ALL' + @LineFeed
                + '' + @LineFeed
                + 'SELECT ServerName,' + @LineFeed
                + '       object_name,' + @LineFeed
                + '       counter_name,' + @LineFeed
                + '       instance_name,' + @LineFeed
                + '       CheckDate,' + @LineFeed
                + '       cntr_value,' + @LineFeed
                + '       ServerName + CAST(CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + 'FROM   PERF_COUNTER_BULK_COUNT' + @LineFeed
                + '' + @LineFeed
                + 'UNION ALL' + @LineFeed
                + '' + @LineFeed
                + 'SELECT ServerName,' + @LineFeed
                + '       object_name,' + @LineFeed
                + '       counter_name,' + @LineFeed
                + '       instance_name,' + @LineFeed
                + '       CheckDate,' + @LineFeed
                + '       cntr_value,' + @LineFeed
                + '       ServerName + CAST(CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + 'FROM   PERF_COUNTER_RAWCOUNT;'')';

			EXEC(@StringToExecute);
            END;


        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') INSERT '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNamePerfmonStats
            + ' (ServerName, CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second) SELECT '
            + ' @SrvName, @CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second FROM #PerfmonStats WHERE Pass = 2';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;

        /* Delete history older than @OutputTableRetentionDays */
        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') DELETE '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNamePerfmonStats
            + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate date',
			@LocalServerName, @OutputTableCleanupDate;



    END;
    ELSE IF (SUBSTRING(@OutputTableNamePerfmonStats, 2, 2) = '##')
    BEGIN
        SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
            + @OutputTableNamePerfmonStats
            + ''') IS NULL) CREATE TABLE '
            + @OutputTableNamePerfmonStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                [object_name] NVARCHAR(128) NOT NULL,
                [counter_name] NVARCHAR(128) NOT NULL,
                [instance_name] NVARCHAR(128) NULL,
                [cntr_value] BIGINT NULL,
                [cntr_type] INT NOT NULL,
                [value_delta] BIGINT NULL,
                [value_per_second] DECIMAL(18,2) NULL,
                PRIMARY KEY CLUSTERED (ID ASC));'
            + ' INSERT '
            + @OutputTableNamePerfmonStats
            + ' (ServerName, CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second) SELECT '
            + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
            + ' @SrvName, @CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second FROM #PerfmonStats WHERE Pass = 2';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;
    END;
    ELSE IF (SUBSTRING(@OutputTableNamePerfmonStats, 2, 1) = '#')
    BEGIN
        RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
    END;


    /* @OutputTableNameWaitStats lets us export the results to a permanent table */
    IF @OutputDatabaseName IS NOT NULL
        AND @OutputSchemaName IS NOT NULL
        AND @OutputTableNameWaitStats IS NOT NULL
        AND @OutputTableNameWaitStats NOT LIKE '#%'
        AND EXISTS ( SELECT *
                     FROM   sys.databases
                     WHERE  QUOTENAME([name]) = @OutputDatabaseName)
    BEGIN
        /* Create the table */
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
            + @OutputTableNameWaitStats + ''') ' + @LineFeed
			+ 'BEGIN' + @LineFeed
			+ 'CREATE TABLE '
            + @OutputSchemaName + '.'
            + @OutputTableNameWaitStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                wait_type NVARCHAR(60),
                wait_time_ms BIGINT,
                signal_wait_time_ms BIGINT,
                waiting_tasks_count BIGINT ,
                PRIMARY KEY CLUSTERED (ID));' + @LineFeed
			+ 'CREATE NONCLUSTERED INDEX IX_ServerName_wait_type_CheckDate_Includes ON ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats + @LineFeed
			+ '(ServerName, wait_type, CheckDate) INCLUDE (wait_time_ms, signal_wait_time_ms, waiting_tasks_count);' + @LineFeed
			+ 'END';

        EXEC(@StringToExecute);

        /* Create the wait stats category table */
        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNameWaitStats_Categories;
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = 'USE '
                + @OutputDatabaseName
                + '; EXEC (''CREATE TABLE '
                + @OutputSchemaName + '.'
                + @OutputTableNameWaitStats_Categories + ' (WaitType NVARCHAR(60) PRIMARY KEY CLUSTERED, WaitCategory NVARCHAR(128) NOT NULL, Ignorable BIT DEFAULT 0);'')';

			EXEC(@StringToExecute);
            END;

		/* Make sure the wait stats category table has the current number of rows */
		SET @StringToExecute = 'USE '
            + @OutputDatabaseName
            + '; EXEC (''IF (SELECT COALESCE(SUM(1),0) FROM ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats_Categories + ') <> (SELECT COALESCE(SUM(1),0) FROM ##WaitCategories)' + @LineFeed
			+ 'BEGIN ' + @LineFeed
			+ 'TRUNCATE TABLE '  + @OutputSchemaName + '.' + @OutputTableNameWaitStats_Categories + @LineFeed
			+ 'INSERT INTO ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats_Categories + ' (WaitType, WaitCategory, Ignorable) SELECT WaitType, WaitCategory, Ignorable FROM ##WaitCategories;' + @LineFeed
			+ 'END'')';

		EXEC(@StringToExecute);


        SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableNameWaitStats_View;

        /* If the view exists without the most recently added columns, drop it. See Github #2162. */
        IF OBJECT_ID(@ObjectFullName) IS NOT NULL
            BEGIN
            SET @StringToExecute = N'USE ' + @OutputDatabaseName + N'; IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
                WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @OutputSchemaName + N'.' + @OutputTableNameWaitStats_View + N';';

            EXEC(@StringToExecute);
            END


        /* Create the wait stats view */
        IF OBJECT_ID(@ObjectFullName) IS NULL
            BEGIN
            SET @StringToExecute = 'USE '
                + @OutputDatabaseName
                + '; EXEC (''CREATE VIEW '
                + @OutputSchemaName + '.'
                + @OutputTableNameWaitStats_View + ' AS ' + @LineFeed
                + 'WITH RowDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ' + @LineFeed
                + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' + @LineFeed
                + '                [CheckDate]' + @LineFeed
                + '        FROM ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats + @LineFeed
                + '        GROUP BY [ServerName], [CheckDate]' + @LineFeed
                + '),' + @LineFeed
                + 'CheckDates as' + @LineFeed
                + '(' + @LineFeed
                + '        SELECT ThisDate.CheckDate,' + @LineFeed
                + '               LastDate.CheckDate as PreviousCheckDate' + @LineFeed
                + '        FROM RowDates ThisDate' + @LineFeed
                + '        JOIN RowDates LastDate' + @LineFeed
                + '        ON ThisDate.ID = LastDate.ID + 1' + @LineFeed
                + ')' + @LineFeed
                + 'SELECT w.ServerName, w.CheckDate, w.wait_type, COALESCE(wc.WaitCategory, ''''Other'''') AS WaitCategory, COALESCE(wc.Ignorable,0) AS Ignorable' + @LineFeed
                + ', DATEDIFF(ss, wPrior.CheckDate, w.CheckDate) AS ElapsedSeconds' + @LineFeed
                + ', (w.wait_time_ms - wPrior.wait_time_ms) AS wait_time_ms_delta' + @LineFeed
                + ', (w.wait_time_ms - wPrior.wait_time_ms) / 60000.0 AS wait_time_minutes_delta' + @LineFeed
                + ', (w.wait_time_ms - wPrior.wait_time_ms) / 1000.0 / DATEDIFF(ss, wPrior.CheckDate, w.CheckDate) AS wait_time_minutes_per_minute' + @LineFeed
                + ', (w.signal_wait_time_ms - wPrior.signal_wait_time_ms) AS signal_wait_time_ms_delta' + @LineFeed
                + ', (w.waiting_tasks_count - wPrior.waiting_tasks_count) AS waiting_tasks_count_delta' + @LineFeed
                + ', w.ServerName + CAST(w.CheckDate AS NVARCHAR(50)) AS JoinKey' + @LineFeed
                + 'FROM ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats + ' w' + @LineFeed
                + 'INNER HASH JOIN CheckDates Dates' + @LineFeed
                + 'ON Dates.CheckDate = w.CheckDate' + @LineFeed
                + 'INNER JOIN ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats + ' wPrior ON w.ServerName = wPrior.ServerName AND w.wait_type = wPrior.wait_type AND Dates.PreviousCheckDate = wPrior.CheckDate' + @LineFeed
			 + 'LEFT OUTER JOIN ' + @OutputSchemaName + '.' + @OutputTableNameWaitStats_Categories + ' wc ON w.wait_type = wc.WaitType' + @LineFeed
                + 'WHERE DATEDIFF(MI, wPrior.CheckDate, w.CheckDate) BETWEEN 1 AND 60' + @LineFeed
                + 'AND [w].[wait_time_ms] >= [wPrior].[wait_time_ms];'')'

			EXEC(@StringToExecute);
            END;


        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') INSERT '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNameWaitStats
            + ' (ServerName, CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count) SELECT '
            + ' @SrvName, @CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count FROM #WaitStats WHERE Pass = 2 AND wait_time_ms > 0 AND waiting_tasks_count > 0';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;

        /* Delete history older than @OutputTableRetentionDays */
        SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
            + @OutputDatabaseName
            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
            + @OutputSchemaName + ''') DELETE '
            + @OutputDatabaseName + '.'
            + @OutputSchemaName + '.'
            + @OutputTableNameWaitStats
            + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate date',
			@LocalServerName, @OutputTableCleanupDate;

    END;
    ELSE IF (SUBSTRING(@OutputTableNameWaitStats, 2, 2) = '##')
    BEGIN
        SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
            + @OutputTableNameWaitStats
            + ''') IS NULL) CREATE TABLE '
            + @OutputTableNameWaitStats
            + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                wait_type NVARCHAR(60),
                wait_time_ms BIGINT,
                signal_wait_time_ms BIGINT,
                waiting_tasks_count BIGINT ,
                PRIMARY KEY CLUSTERED (ID ASC));'
            + ' INSERT '
            + @OutputTableNameWaitStats
            + ' (ServerName, CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count) SELECT '
            + ' @SrvName, @CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count FROM #WaitStats WHERE Pass = 2 AND wait_time_ms > 0 AND waiting_tasks_count > 0';

		EXEC sp_executesql @StringToExecute,
			N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
			@LocalServerName, @StartSampleTime;
    END;
    ELSE IF (SUBSTRING(@OutputTableNameWaitStats, 2, 1) = '#')
    BEGIN
        RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
    END;




    DECLARE @separator AS VARCHAR(1);
    IF @OutputType = 'RSV'
        SET @separator = CHAR(31);
    ELSE
        SET @separator = ',';

    IF @OutputType = 'COUNT' AND @SinceStartup = 0
    BEGIN
        SELECT  COUNT(*) AS Warnings
        FROM    #BlitzFirstResults;
    END;
    ELSE
        IF @OutputType = 'Opserver1' AND @SinceStartup = 0
        BEGIN

            SELECT  r.[Priority] ,
                    r.[FindingsGroup] ,
                    r.[Finding] ,
                    r.[URL] ,
                    r.[Details],
                    r.[HowToStopIt] ,
                    r.[CheckID] ,
                    r.[StartTime],
                    r.[LoginName],
                    r.[NTUserName],
                    r.[OriginalLoginName],
                    r.[ProgramName],
                    r.[HostName],
                    r.[DatabaseID],
                    r.[DatabaseName],
                    r.[OpenTransactionCount],
                    r.[QueryPlan],
                    r.[QueryText],
                    qsNow.plan_handle AS PlanHandle,
                    qsNow.sql_handle AS SqlHandle,
                    qsNow.statement_start_offset AS StatementStartOffset,
                    qsNow.statement_end_offset AS StatementEndOffset,
                    [Executions] = qsNow.execution_count - (COALESCE(qsFirst.execution_count, 0)),
                    [ExecutionsPercent] = CAST(100.0 * (qsNow.execution_count - (COALESCE(qsFirst.execution_count, 0))) / (qsTotal.execution_count - qsTotalFirst.execution_count) AS DECIMAL(6,2)),
                    [Duration] = qsNow.total_elapsed_time - (COALESCE(qsFirst.total_elapsed_time, 0)),
                    [DurationPercent] = CAST(100.0 * (qsNow.total_elapsed_time - (COALESCE(qsFirst.total_elapsed_time, 0))) / (qsTotal.total_elapsed_time - qsTotalFirst.total_elapsed_time) AS DECIMAL(6,2)),
                    [CPU] = qsNow.total_worker_time - (COALESCE(qsFirst.total_worker_time, 0)),
                    [CPUPercent] = CAST(100.0 * (qsNow.total_worker_time - (COALESCE(qsFirst.total_worker_time, 0))) / (qsTotal.total_worker_time - qsTotalFirst.total_worker_time) AS DECIMAL(6,2)),
                    [Reads] = qsNow.total_logical_reads - (COALESCE(qsFirst.total_logical_reads, 0)),
                    [ReadsPercent] = CAST(100.0 * (qsNow.total_logical_reads - (COALESCE(qsFirst.total_logical_reads, 0))) / (qsTotal.total_logical_reads - qsTotalFirst.total_logical_reads) AS DECIMAL(6,2)),
                    [PlanCreationTime] = CONVERT(NVARCHAR(100), qsNow.creation_time ,121),
                    [TotalExecutions] = qsNow.execution_count,
                    [TotalExecutionsPercent] = CAST(100.0 * qsNow.execution_count / qsTotal.execution_count AS DECIMAL(6,2)),
                    [TotalDuration] = qsNow.total_elapsed_time,
                    [TotalDurationPercent] = CAST(100.0 * qsNow.total_elapsed_time / qsTotal.total_elapsed_time AS DECIMAL(6,2)),
                    [TotalCPU] = qsNow.total_worker_time,
                    [TotalCPUPercent] = CAST(100.0 * qsNow.total_worker_time / qsTotal.total_worker_time AS DECIMAL(6,2)),
                    [TotalReads] = qsNow.total_logical_reads,
                    [TotalReadsPercent] = CAST(100.0 * qsNow.total_logical_reads / qsTotal.total_logical_reads AS DECIMAL(6,2)),
                    r.[DetailsInt]
            FROM    #BlitzFirstResults r
                LEFT OUTER JOIN #QueryStats qsTotal ON qsTotal.Pass = 0
                LEFT OUTER JOIN #QueryStats qsTotalFirst ON qsTotalFirst.Pass = -1
                LEFT OUTER JOIN #QueryStats qsNow ON r.QueryStatsNowID = qsNow.ID
                LEFT OUTER JOIN #QueryStats qsFirst ON r.QueryStatsFirstID = qsFirst.ID
            ORDER BY r.Priority ,
                    r.FindingsGroup ,
                    CASE
                        WHEN r.CheckID = 6 THEN DetailsInt
                        ELSE 0
                    END DESC,
                    r.Finding,
                    r.ID;
        END;
        ELSE IF @OutputType IN ( 'CSV', 'RSV' ) AND @SinceStartup = 0
        BEGIN

            SELECT  Result = CAST([Priority] AS NVARCHAR(100))
                    + @separator + CAST(CheckID AS NVARCHAR(100))
                    + @separator + COALESCE([FindingsGroup],
                                            '(N/A)') + @separator
                    + COALESCE([Finding], '(N/A)') + @separator
                    + COALESCE(DatabaseName, '(N/A)') + @separator
                    + COALESCE([URL], '(N/A)') + @separator
                    + COALESCE([Details], '(N/A)')
            FROM    #BlitzFirstResults
            ORDER BY Priority ,
                    FindingsGroup ,
                    CASE
                        WHEN CheckID = 6 THEN DetailsInt
                        ELSE 0
                    END DESC,
                    Finding,
                    Details;
        END;
        ELSE IF @OutputType = 'Top10'
            BEGIN
                /* Measure waits in hours */
                ;WITH max_batch AS (
                    SELECT MAX(SampleTime) AS SampleTime
                    FROM #WaitStats
                )
                SELECT TOP 10
                    CAST(DATEDIFF(mi,wd1.SampleTime, wd2.SampleTime) / 60.0 AS DECIMAL(18,1)) AS [Hours Sample],
					CAST(c.[Total Thread Time (Seconds)] / 60. / 60. AS DECIMAL(18,1)) AS [Thread Time (Hours)],
                    wd1.wait_type,
					COALESCE(wcat.WaitCategory, 'Other') AS wait_category,
                    CAST(c.[Wait Time (Seconds)] / 60. / 60. AS DECIMAL(18,1)) AS [Wait Time (Hours)],
					CAST((wd2.wait_time_ms - wd1.wait_time_ms) / 1000.0 / cores.cpu_count / DATEDIFF(ss, wd1.SampleTime, wd2.SampleTime) AS DECIMAL(18,1)) AS [Per Core Per Hour],
                    (wd2.waiting_tasks_count - wd1.waiting_tasks_count) AS [Number of Waits],
                    CASE WHEN (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    THEN
                        CAST((wd2.wait_time_ms-wd1.wait_time_ms)/
                            (1.0*(wd2.waiting_tasks_count - wd1.waiting_tasks_count)) AS NUMERIC(12,1))
                    ELSE 0 END AS [Avg ms Per Wait]
                FROM  max_batch b
                JOIN #WaitStats wd2 ON
                    wd2.SampleTime =b.SampleTime
                JOIN #WaitStats wd1 ON
                    wd1.wait_type=wd2.wait_type AND
                    wd2.SampleTime > wd1.SampleTime
                CROSS APPLY (SELECT SUM(1) AS cpu_count FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE' AND is_online = 1) AS cores
                CROSS APPLY (SELECT
                    CAST((wd2.wait_time_ms-wd1.wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Wait Time (Seconds)],
                    CAST((wd2.signal_wait_time_ms - wd1.signal_wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Signal Wait Time (Seconds)],
					CAST((wd2.thread_time_ms)/1000. AS DECIMAL(18,1)) AS [Total Thread Time (Seconds)]
					) AS c
				LEFT OUTER JOIN ##WaitCategories wcat ON wd1.wait_type = wcat.WaitType
                WHERE (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    AND wd2.wait_time_ms-wd1.wait_time_ms > 0
                ORDER BY [Wait Time (Seconds)] DESC;
        END;
        ELSE IF @ExpertMode = 0 AND @OutputType <> 'NONE' AND @OutputXMLasNVARCHAR = 0 AND @SinceStartup = 0
        BEGIN
            SELECT  [Priority] ,
                    [FindingsGroup] ,
                    [Finding] ,
                    [URL] ,
                    CAST(@StockDetailsHeader + [Details] + @StockDetailsFooter AS XML) AS Details,
                    CAST(@StockWarningHeader + HowToStopIt + @StockWarningFooter AS XML) AS HowToStopIt,
                    [QueryText],
                    [QueryPlan]
            FROM    #BlitzFirstResults
            WHERE (@Seconds > 0 OR (Priority IN (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
            ORDER BY Priority ,
                    FindingsGroup ,
                    CASE
                        WHEN CheckID = 6 THEN DetailsInt
                        ELSE 0
                    END DESC,
                    Finding,
                    ID,
					CAST(Details AS NVARCHAR(4000));
        END;
        ELSE IF @OutputType <> 'NONE' AND @OutputXMLasNVARCHAR = 1 AND @SinceStartup = 0
        BEGIN
            SELECT  [Priority] ,
                    [FindingsGroup] ,
                    [Finding] ,
                    [URL] ,
                    CAST(LEFT(@StockDetailsHeader + [Details] + @StockDetailsFooter,32000) AS TEXT) AS Details,
                    CAST(LEFT([HowToStopIt],32000) AS TEXT) AS HowToStopIt,
                    CAST([QueryText] AS NVARCHAR(MAX)) AS QueryText,
                    CAST([QueryPlan] AS NVARCHAR(MAX)) AS QueryPlan
            FROM    #BlitzFirstResults
            WHERE (@Seconds > 0 OR (Priority IN (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
            ORDER BY Priority ,
                    FindingsGroup ,
                    CASE
                        WHEN CheckID = 6 THEN DetailsInt
                        ELSE 0
                    END DESC,
                    Finding,
                    ID,
					CAST(Details AS NVARCHAR(4000));
        END;
        ELSE IF @ExpertMode = 1 AND @OutputType <> 'NONE'
        BEGIN
            IF @SinceStartup = 0
                SELECT  r.[Priority] ,
                        r.[FindingsGroup] ,
                        r.[Finding] ,
                        r.[URL] ,
                        CAST(@StockDetailsHeader + r.[Details] + @StockDetailsFooter AS XML) AS Details,
                        CAST(@StockWarningHeader + r.HowToStopIt + @StockWarningFooter AS XML) AS HowToStopIt,
                        r.[CheckID] ,
                        r.[StartTime],
                        r.[LoginName],
                        r.[NTUserName],
                        r.[OriginalLoginName],
                        r.[ProgramName],
                        r.[HostName],
                        r.[DatabaseID],
                        r.[DatabaseName],
                        r.[OpenTransactionCount],
                        r.[QueryPlan],
                        r.[QueryText],
                        qsNow.plan_handle AS PlanHandle,
                        qsNow.sql_handle AS SqlHandle,
                        qsNow.statement_start_offset AS StatementStartOffset,
                        qsNow.statement_end_offset AS StatementEndOffset,
                        [Executions] = qsNow.execution_count - (COALESCE(qsFirst.execution_count, 0)),
                        [ExecutionsPercent] = CAST(100.0 * (qsNow.execution_count - (COALESCE(qsFirst.execution_count, 0))) / (qsTotal.execution_count - qsTotalFirst.execution_count) AS DECIMAL(6,2)),
                        [Duration] = qsNow.total_elapsed_time - (COALESCE(qsFirst.total_elapsed_time, 0)),
                        [DurationPercent] = CAST(100.0 * (qsNow.total_elapsed_time - (COALESCE(qsFirst.total_elapsed_time, 0))) / (qsTotal.total_elapsed_time - qsTotalFirst.total_elapsed_time) AS DECIMAL(6,2)),
                        [CPU] = qsNow.total_worker_time - (COALESCE(qsFirst.total_worker_time, 0)),
                        [CPUPercent] = CAST(100.0 * (qsNow.total_worker_time - (COALESCE(qsFirst.total_worker_time, 0))) / (qsTotal.total_worker_time - qsTotalFirst.total_worker_time) AS DECIMAL(6,2)),
                        [Reads] = qsNow.total_logical_reads - (COALESCE(qsFirst.total_logical_reads, 0)),
                        [ReadsPercent] = CAST(100.0 * (qsNow.total_logical_reads - (COALESCE(qsFirst.total_logical_reads, 0))) / (qsTotal.total_logical_reads - qsTotalFirst.total_logical_reads) AS DECIMAL(6,2)),
                        [PlanCreationTime] = CONVERT(NVARCHAR(100), qsNow.creation_time ,121),
                        [TotalExecutions] = qsNow.execution_count,
                        [TotalExecutionsPercent] = CAST(100.0 * qsNow.execution_count / qsTotal.execution_count AS DECIMAL(6,2)),
                        [TotalDuration] = qsNow.total_elapsed_time,
                        [TotalDurationPercent] = CAST(100.0 * qsNow.total_elapsed_time / qsTotal.total_elapsed_time AS DECIMAL(6,2)),
                        [TotalCPU] = qsNow.total_worker_time,
                        [TotalCPUPercent] = CAST(100.0 * qsNow.total_worker_time / qsTotal.total_worker_time AS DECIMAL(6,2)),
                        [TotalReads] = qsNow.total_logical_reads,
                        [TotalReadsPercent] = CAST(100.0 * qsNow.total_logical_reads / qsTotal.total_logical_reads AS DECIMAL(6,2)),
                        r.[DetailsInt]
                FROM    #BlitzFirstResults r
                    LEFT OUTER JOIN #QueryStats qsTotal ON qsTotal.Pass = 0
                    LEFT OUTER JOIN #QueryStats qsTotalFirst ON qsTotalFirst.Pass = -1
                    LEFT OUTER JOIN #QueryStats qsNow ON r.QueryStatsNowID = qsNow.ID
                    LEFT OUTER JOIN #QueryStats qsFirst ON r.QueryStatsFirstID = qsFirst.ID
                WHERE (@Seconds > 0 OR (Priority IN (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
                ORDER BY r.Priority ,
                        r.FindingsGroup ,
                        CASE
                            WHEN r.CheckID = 6 THEN DetailsInt
                            ELSE 0
                        END DESC,
                        r.Finding,
                        r.ID,
						CAST(r.Details AS NVARCHAR(4000));

            -------------------------
            --What happened: #WaitStats
            -------------------------
            IF @Seconds = 0
                BEGIN
                /* Measure waits in hours */
                ;WITH max_batch AS (
                    SELECT MAX(SampleTime) AS SampleTime
                    FROM #WaitStats
                )
                SELECT
                    'WAIT STATS' AS Pattern,
                    b.SampleTime AS [Sample Ended],
                    CAST(DATEDIFF(mi,wd1.SampleTime, wd2.SampleTime) / 60. AS DECIMAL(18,1)) AS [Hours Sample],
					CAST(c.[Total Thread Time (Seconds)] / 60. / 60. AS DECIMAL(18,1)) AS [Thread Time (Hours)],
                    wd1.wait_type,
					COALESCE(wcat.WaitCategory, 'Other') AS wait_category,
                    CAST(c.[Wait Time (Seconds)] / 60. / 60. AS DECIMAL(18,1)) AS [Wait Time (Hours)],
                    CAST((wd2.wait_time_ms - wd1.wait_time_ms) / 1000.0 / cores.cpu_count / DATEDIFF(ss, wd1.SampleTime, wd2.SampleTime) AS DECIMAL(18,1)) AS [Per Core Per Hour],
                    CAST(c.[Signal Wait Time (Seconds)] / 60.0 / 60 AS DECIMAL(18,1)) AS [Signal Wait Time (Hours)],
                    CASE WHEN c.[Wait Time (Seconds)] > 0
                     THEN CAST(100.*(c.[Signal Wait Time (Seconds)]/c.[Wait Time (Seconds)]) AS NUMERIC(4,1))
                    ELSE 0 END AS [Percent Signal Waits],
                    (wd2.waiting_tasks_count - wd1.waiting_tasks_count) AS [Number of Waits],
                    CASE WHEN (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    THEN
                        CAST((wd2.wait_time_ms-wd1.wait_time_ms)/
                            (1.0*(wd2.waiting_tasks_count - wd1.waiting_tasks_count)) AS NUMERIC(12,1))
                    ELSE 0 END AS [Avg ms Per Wait],
                    N'https://www.sqlskills.com/help/waits/' + LOWER(wd1.wait_type) + '/' AS URL
                FROM  max_batch b
                JOIN #WaitStats wd2 ON
                    wd2.SampleTime =b.SampleTime
                JOIN #WaitStats wd1 ON
                    wd1.wait_type=wd2.wait_type AND
                    wd2.SampleTime > wd1.SampleTime
                CROSS APPLY (SELECT SUM(1) AS cpu_count FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE' AND is_online = 1) AS cores
                CROSS APPLY (SELECT
                    CAST((wd2.wait_time_ms-wd1.wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Wait Time (Seconds)],
                    CAST((wd2.signal_wait_time_ms - wd1.signal_wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Signal Wait Time (Seconds)],
					CAST((wd2.thread_time_ms)/1000. AS DECIMAL(18,1)) AS [Total Thread Time (Seconds)]
					) AS c
				LEFT OUTER JOIN ##WaitCategories wcat ON wd1.wait_type = wcat.WaitType
                WHERE (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    AND wd2.wait_time_ms-wd1.wait_time_ms > 0
                ORDER BY [Wait Time (Seconds)] DESC;
                END;
            ELSE
                BEGIN
                /* Measure waits in seconds */
                ;WITH max_batch AS (
                    SELECT MAX(SampleTime) AS SampleTime
                    FROM #WaitStats
                )
                SELECT
                    'WAIT STATS' AS Pattern,
                    b.SampleTime AS [Sample Ended],
                    DATEDIFF(ss,wd1.SampleTime, wd2.SampleTime) AS [Seconds Sample],
					c.[Total Thread Time (Seconds)],
                    wd1.wait_type,
					COALESCE(wcat.WaitCategory, 'Other') AS wait_category,
                    c.[Wait Time (Seconds)],
                    CAST((CAST(wd2.wait_time_ms - wd1.wait_time_ms AS MONEY)) / 1000.0 / cores.cpu_count / DATEDIFF(ss, wd1.SampleTime, wd2.SampleTime) AS DECIMAL(18,1)) AS [Per Core Per Second],
                    c.[Signal Wait Time (Seconds)],
                    CASE WHEN c.[Wait Time (Seconds)] > 0
                     THEN CAST(100.*(c.[Signal Wait Time (Seconds)]/c.[Wait Time (Seconds)]) AS NUMERIC(4,1))
                    ELSE 0 END AS [Percent Signal Waits],
                    (wd2.waiting_tasks_count - wd1.waiting_tasks_count) AS [Number of Waits],
                    CASE WHEN (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    THEN
                        CAST((wd2.wait_time_ms-wd1.wait_time_ms)/
                            (1.0*(wd2.waiting_tasks_count - wd1.waiting_tasks_count)) AS NUMERIC(12,1))
                    ELSE 0 END AS [Avg ms Per Wait],
                    N'https://www.sqlskills.com/help/waits/' + LOWER(wd1.wait_type) + '/' AS URL
                FROM  max_batch b
                JOIN #WaitStats wd2 ON
                    wd2.SampleTime =b.SampleTime
                JOIN #WaitStats wd1 ON
                    wd1.wait_type=wd2.wait_type AND
                    wd2.SampleTime > wd1.SampleTime
                CROSS APPLY (SELECT SUM(1) AS cpu_count FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE' AND is_online = 1) AS cores
                CROSS APPLY (SELECT
                    CAST((wd2.wait_time_ms-wd1.wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Wait Time (Seconds)],
                    CAST((wd2.signal_wait_time_ms - wd1.signal_wait_time_ms)/1000. AS DECIMAL(18,1)) AS [Signal Wait Time (Seconds)],
					CAST((wd2.thread_time_ms - wd1.thread_time_ms)/1000. AS DECIMAL(18,1)) AS [Total Thread Time (Seconds)]
					) AS c
				LEFT OUTER JOIN ##WaitCategories wcat ON wd1.wait_type = wcat.WaitType
                WHERE (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                    AND wd2.wait_time_ms-wd1.wait_time_ms > 0
                ORDER BY [Wait Time (Seconds)] DESC;
                END;

            -------------------------
            --What happened: #FileStats
            -------------------------
            WITH readstats AS (
                SELECT 'PHYSICAL READS' AS Pattern,
                ROW_NUMBER() OVER (ORDER BY wd2.avg_stall_read_ms DESC) AS StallRank,
                wd2.SampleTime AS [Sample Time],
                DATEDIFF(ss,wd1.SampleTime, wd2.SampleTime) AS [Sample (seconds)],
                wd1.DatabaseName ,
                wd1.FileLogicalName AS [File Name],
                UPPER(SUBSTRING(wd1.PhysicalName, 1, 2)) AS [Drive] ,
                wd1.SizeOnDiskMB ,
                ( wd2.num_of_reads - wd1.num_of_reads ) AS [# Reads/Writes],
                CASE WHEN wd2.num_of_reads - wd1.num_of_reads > 0
                  THEN CAST(( wd2.bytes_read - wd1.bytes_read)/1024./1024. AS NUMERIC(21,1))
                  ELSE 0
                END AS [MB Read/Written],
                wd2.avg_stall_read_ms AS [Avg Stall (ms)],
                wd1.PhysicalName AS [file physical name]
            FROM #FileStats wd2
                JOIN #FileStats wd1 ON wd2.SampleTime > wd1.SampleTime
                  AND wd1.DatabaseID = wd2.DatabaseID
                  AND wd1.FileID = wd2.FileID
            ),
            writestats AS (
                SELECT
                'PHYSICAL WRITES' AS Pattern,
                ROW_NUMBER() OVER (ORDER BY wd2.avg_stall_write_ms DESC) AS StallRank,
                wd2.SampleTime AS [Sample Time],
                DATEDIFF(ss,wd1.SampleTime, wd2.SampleTime) AS [Sample (seconds)],
                wd1.DatabaseName ,
                wd1.FileLogicalName AS [File Name],
                UPPER(SUBSTRING(wd1.PhysicalName, 1, 2)) AS [Drive] ,
                wd1.SizeOnDiskMB ,
                ( wd2.num_of_writes - wd1.num_of_writes ) AS [# Reads/Writes],
                CASE WHEN wd2.num_of_writes - wd1.num_of_writes > 0
                  THEN CAST(( wd2.bytes_written - wd1.bytes_written)/1024./1024. AS NUMERIC(21,1))
                  ELSE 0
                END AS [MB Read/Written],
                wd2.avg_stall_write_ms AS [Avg Stall (ms)],
                wd1.PhysicalName AS [file physical name]
            FROM #FileStats wd2
                JOIN #FileStats wd1 ON wd2.SampleTime > wd1.SampleTime
                  AND wd1.DatabaseID = wd2.DatabaseID
                  AND wd1.FileID = wd2.FileID
            )
            SELECT
                Pattern, [Sample Time], [Sample (seconds)], [File Name], [Drive],  [# Reads/Writes],[MB Read/Written],[Avg Stall (ms)], [file physical name], [DatabaseName], [StallRank]
            FROM readstats
            WHERE StallRank <=20 AND [MB Read/Written] > 0
            UNION ALL
            SELECT Pattern, [Sample Time], [Sample (seconds)], [File Name], [Drive],  [# Reads/Writes],[MB Read/Written],[Avg Stall (ms)], [file physical name], [DatabaseName], [StallRank]
            FROM writestats
            WHERE StallRank <=20 AND [MB Read/Written] > 0
            ORDER BY Pattern, StallRank;


            -------------------------
            --What happened: #PerfmonStats
            -------------------------

            SELECT 'PERFMON' AS Pattern, pLast.[object_name], pLast.counter_name, pLast.instance_name,
                pFirst.SampleTime AS FirstSampleTime, pFirst.cntr_value AS FirstSampleValue,
                pLast.SampleTime AS LastSampleTime, pLast.cntr_value AS LastSampleValue,
                pLast.cntr_value - pFirst.cntr_value AS ValueDelta,
                ((1.0 * pLast.cntr_value - pFirst.cntr_value) / DATEDIFF(ss, pFirst.SampleTime, pLast.SampleTime)) AS ValuePerSecond
                FROM #PerfmonStats pLast
                    INNER JOIN #PerfmonStats pFirst ON pFirst.[object_name] = pLast.[object_name] AND pFirst.counter_name = pLast.counter_name AND (pFirst.instance_name = pLast.instance_name OR (pFirst.instance_name IS NULL AND pLast.instance_name IS NULL))
                    AND pLast.ID > pFirst.ID
				WHERE pLast.cntr_value <> pFirst.cntr_value
                ORDER BY Pattern, pLast.[object_name], pLast.counter_name, pLast.instance_name;


            -------------------------
            --What happened: #QueryStats
            -------------------------
            IF @CheckProcedureCache = 1
			BEGIN
			
			SELECT qsNow.*, qsFirst.*
            FROM #QueryStats qsNow
              INNER JOIN #QueryStats qsFirst ON qsNow.[sql_handle] = qsFirst.[sql_handle] AND qsNow.statement_start_offset = qsFirst.statement_start_offset AND qsNow.statement_end_offset = qsFirst.statement_end_offset AND qsNow.plan_generation_num = qsFirst.plan_generation_num AND qsNow.plan_handle = qsFirst.plan_handle AND qsFirst.Pass = 1
            WHERE qsNow.Pass = 2;
			END;
			ELSE
			BEGIN
			SELECT 'Plan Cache' AS [Pattern], 'Plan cache not analyzed' AS [Finding], 'Use @CheckProcedureCache = 1 or run sp_BlitzCache for more analysis' AS [More Info], CONVERT(XML, @StockDetailsHeader + 'firstresponderkit.org' + @StockDetailsFooter) AS [Details];
			END;
        END;

    DROP TABLE #BlitzFirstResults;

    /* What's running right now? This is the first and last result set. */
    IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'
    BEGIN
		IF OBJECT_ID('master.dbo.sp_BlitzWho') IS NULL AND OBJECT_ID('dbo.sp_BlitzWho') IS NULL
		BEGIN
			PRINT N'sp_BlitzWho is not installed in the current database_files.  You can get a copy from http://FirstResponderKit.org';
		END;
		ELSE
		BEGIN
			EXEC (@BlitzWho);
		END;
    END; /* IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'   -   What's running right now? This is the first and last result set. */

END; /* IF @LogMessage IS NULL */
END; /* ELSE IF @OutputType = 'SCHEMA' */

SET NOCOUNT OFF;
GO



/* How to run it:
EXEC dbo.sp_BlitzFirst

With extra diagnostic info:
EXEC dbo.sp_BlitzFirst @ExpertMode = 1;

Saving output to tables:
EXEC sp_BlitzFirst
  @OutputDatabaseName = 'DBAtools'
, @OutputSchemaName = 'dbo'
, @OutputTableName = 'BlitzFirst'
, @OutputTableNameFileStats = 'BlitzFirst_FileStats'
, @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats'
, @OutputTableNameWaitStats = 'BlitzFirst_WaitStats'
, @OutputTableNameBlitzCache = 'BlitzCache'
, @OutputTableNameBlitzWho = 'BlitzWho'
, @OutputType = 'none'
*/
