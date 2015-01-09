SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

USE master;
GO

/*
-- Welcome to the beta release of sp_BlitzTrace (TM) -- Version 0.2, released 2015-01-03
-- BrentOzar.com/BlitzTrace
-- Things you can do....

--List running sessions
exec sp_BlitzTrace @Action='start';

--Start a trace for a session. You specify the @SessionID and @TargetPath
exec sp_BlitzTrace @SessionId=52, @TargetPath='S:\XEvents\Traces\', @Action='start';

--Stop a session
exec sp_BlitzTrace @Action='stop';

--Read the results. You can move the files to another server and read there by specifying a @TargetPath.
exec sp_BlitzTrace @Action='read';

--Drop the session. This does NOT delete files created in @TargetPath.
exec sp_BlitzTrace @Action='drop';
*/

IF OBJECT_ID('dbo.sp_BlitzTrace') IS NOT NULL
    DROP PROCEDURE dbo.sp_BlitzTrace
GO
CREATE PROCEDURE dbo.sp_BlitzTrace
    @Debug BIT = 0 , /* 1 prints the statement and won't execute it */
    @SessionId INT = NULL ,
    @Action VARCHAR(5) = NULL ,  /* 'start', 'read', 'stop', 'drop'*/
    @TargetPath VARCHAR(528) = NULL,  /* Required for 'start'. 'Read' will look for a running sp_BlitzTrace session if not specified.*/
    @TraceRecompiles BIT = 1,
    @TraceObjectCreates BIT = 1,
    @TraceParallelism BIT = 1,
    @TraceSortWarnings BIT = 1,
    @TraceStatements BIT = 0,
    @MaxFileSizeMB INT = 256,
    @TraceExecutionPlansAndKillMyPerformance BIT = 0, /* Non-production environments only */
    @MaxRolloverFiles INT = 4,
    @MaxDispatchLatencySeconds INT = 5 /* 0 is unlimited! */

WITH RECOMPILE
AS
    /* (c) 2014 Brent Ozar Unlimited (R) */
    /* See http://BrentOzar.com/go/eula for the End User License Agreement */

    DECLARE @nl NVARCHAR(2) = NCHAR(13) + NCHAR(10) ;

    RAISERROR (N'*******************START HERE*******************',0,1) WITH NOWAIT;
    RAISERROR (N'(c) 2014 Brent Ozar Unlimited (R).',0,1) WITH NOWAIT;
    RAISERROR (N'See http://BrentOzar.com/go/eula for the End User License Agreement.',0,1) WITH NOWAIT;
    RAISERROR (N'Sp_BlitzTrace version 0.1, released on 2014-11-11.',0,1) WITH NOWAIT;
    RAISERROR (N'*****************Let''s Do This!*****************',0,1) WITH NOWAIT;
    RAISERROR (@nl,0,1) WITH NOWAIT;

    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;

BEGIN TRY

    DECLARE @msg NVARCHAR(MAX);
    DECLARE @rowcount INT;
    DECLARE @v decimal(6,2);
    DECLARE @build int;
    DECLARE @datestamp VARCHAR(30);
    DECLARE @TargetPathFull NVARCHAR(MAX);
    DECLARE @filepathXML XML;
    DECLARE @filepath VARCHAR(1024);
    DECLARE @traceexists BIT = 0;
    DECLARE @tracerunning BIT = 0;



    /* Validate parameters */
    SET @msg = CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Validatin'' parameters.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    IF @Action NOT IN (N'start', N'read', N'stop', N'drop')  OR @Action is NULL
    BEGIN
        RAISERROR (N'You need to specify a valid @Action for sp_BlitzTrace: ''start'', ''read'', ''stop'', or ''drop''.',16,1) WITH NOWAIT;
    END

    IF @Action = N'start' AND @SessionId IS NULL
    BEGIN
        SELECT ses.session_id, con.last_read, con.last_write, ses.login_name, ses.host_name, ses.program_name,
            con.connect_time, con.protocol_type, con.encrypt_option,
            con.num_reads, con.num_writes, con.client_net_address,
            req.status, req.command, req.wait_type, req.last_wait_type, req.open_transaction_count
        FROM sys.dm_exec_sessions as ses
        JOIN sys.dm_exec_connections AS con ON
            con.session_id=ses.session_id
        LEFT JOIN sys.dm_exec_requests AS req ON
            con.session_id=req.session_id
        WHERE
            ses.session_id <> @@SPID
        ORDER BY last_read DESC

        RAISERROR (N'sp_BlitzTrace watches just one session, so you have to specify @SessionId. Check out the session list above for some ideas.',16,1) WITH NOWAIT;
    END

    IF @MaxDispatchLatencySeconds > 99
    BEGIN
        RAISERROR (N'@MaxDispatchLatencySeconds must be 99 or less. 5 is the default. 0 is unlimited latency.',16,1) WITH NOWAIT;
    END

    IF @MaxFileSizeMB > 9999
    BEGIN
        RAISERROR (N'@MaxFileSizeMB must be 9999 or smaller - 256MB is the default.',16,1) WITH NOWAIT;
    END

    IF @MaxRolloverFiles > 99
    BEGIN
        RAISERROR (N'@MaxRolloverFiles must be 99 or smaller. 4 is the default.',16,1) WITH NOWAIT;
    END

    IF @TargetPath IS NULL AND @Action = N'start'
    BEGIN
        RAISERROR (N'You gotta give a valid @TargetPath for ''start''.',16,1) WITH NOWAIT;
    END

    IF @TargetPath IS NOT NULL AND @Action=N'start' AND RIGHT(@TargetPath, 1) <> N'\'
    BEGIN
        RAISERROR (N'@TargetPath must be a directory ending in ''\'', like: ''S:\Xevents\''',16,1) WITH NOWAIT;
    END


    IF @TargetPath IS NOT NULL AND @Action='read' AND  ( RIGHT(@TargetPath, 1) <> '\' AND RIGHT(@TargetPath, 4) <> '.xel')
    BEGIN
        RAISERROR (N'To read, @TargetPath must be a directory ending in ''\'', like: ''S:\XEvents\'', or a file ending in .xel, like ''S:\XEvents\sp_BlitzTrace*.xel''',16,1) WITH NOWAIT;
    END

    /* Validate transaction state */
    SET @msg = CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Validatin'' transaction state.'
    RAISERROR (@msg,0,1) WITH NOWAIT;
    IF @@TRANCOUNT > 0
    BEGIN
        RAISERROR (N'@@TRANCOUNT > 0 not supported',16,1) WITH NOWAIT;
    END

    /* Check version */
    SET @msg = CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Determining SQL Server version.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SELECT @v = SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 1,CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') as NVARCHAR(128))) + 1 )

    IF @v < N'11'
    BEGIN
        SET @msg = N'Sad news: most the events sp_BlitzTrace uses are only in SQL Server 2012 and higher-- so it''s not supported on SQL 2008/R2.'
        RAISERROR (@msg,16,1) WITH NOWAIT;
        RETURN;
    END


    /* Get current trace status */
    SELECT
        @traceexists = (CASE WHEN (s.name IS NULL) THEN 0 ELSE 1 END),
        @tracerunning = (CASE WHEN (r.create_time IS NULL) THEN 0 ELSE 1 END)
    FROM sys.server_event_sessions AS s
    LEFT OUTER JOIN sys.dm_xe_sessions AS r ON r.name = s.name
    WHERE s.name=N'sp_BlitzTrace'


    /* We use this to filter sp_BlitzTrace activity out of the results, */
    /* in case you're tracing your own session. */
    /* That's just to make the results less confusing */
    DECLARE @context VARBINARY(128);
    SET @context=CAST('sp_BlitzTrace IS THE BEST' as binary);
    SET CONTEXT_INFO @context;

    IF @Action = N'start'
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Creating extended events trace.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF @traceexists = 1
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- A trace named sp_BlitzTrace already exists'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            IF @tracerunning=1
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- sp_BlitzTrace is running, so stopping it before we recreate: ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = STOP;'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = STOP;

                SET @tracerunning=0;
            END

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Dropping the trace: DROP EVENT SESSION sp_BlitzTrace ON SERVER; '
            RAISERROR (@msg,0,1) WITH NOWAIT;

            DROP EVENT SESSION sp_BlitzTrace ON SERVER;

            SET @traceexists=0;

        END

        IF @traceexists = 0
        BEGIN
            SELECT @datestamp = REPLACE(REPLACE( CONVERT(VARCHAR(26),getdate(),126),':','-'),'.','')
            SET @TargetPathFull=@TargetPath + N'sp_BlitzTrace-' + @datestamp

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Target path = ' + @TargetPathFull;
            RAISERROR (@msg,0,1) WITH NOWAIT;


            DECLARE @dsql NVARCHAR(MAX);
            DECLARE @dsql1 NVARCHAR(MAX)=
                N'CREATE EVENT SESSION sp_BlitzTrace ON SERVER
                ' + case @TraceStatements when 1 then + N'
                ADD EVENT sqlserver.sp_statement_completed (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ADD EVENT sqlserver.sql_statement_completed (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END + case @TraceSortWarnings when 1 then N'
                ADD EVENT sqlserver.sort_warning (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END + case @TraceParallelism when 1 then + N'
                ADD EVENT sqlserver.degree_of_parallelism (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END + N'
                ' + case @TraceObjectCreates when 1 then + N'
                ADD EVENT sqlserver.object_created (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END;

            DECLARE @dsql2 NVARCHAR(MAX)=
                N'ADD EVENT sqlserver.sql_batch_completed (
                    ACTION(sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' + case @TraceRecompiles when 1 then + N'
                ADD EVENT sqlserver.sql_statement_recompile (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END
                + case @TraceExecutionPlansAndKillMyPerformance when 1 then + N'
                ADD EVENT sqlserver.query_post_execution_showplan (
                    ACTION(sqlserver.context_info, sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N'))),
                ' ELSE N'' END + N'
                ADD EVENT sqlserver.rpc_completed (
                    ACTION(sqlserver.sql_text, sqlserver.query_hash, sqlserver.query_plan_hash)
                    WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(6)) + N')))
                ADD TARGET package0.event_file(SET filename=''' + @TargetPathFull + N''',
                    MAX_FILE_SIZE=(' + CAST(@MaxFileSizeMB AS VARCHAR(4)) + N'),
                    MAX_ROLLOVER_FILES = ' + CAST(@MaxRolloverFiles as VARCHAR(2)) + N')
                WITH (
                    MAX_MEMORY = 128 MB,
                    EVENT_RETENTION_MODE = ALLOW_MULTIPLE_EVENT_LOSS,
                    MAX_DISPATCH_LATENCY = ' + CAST(@MaxDispatchLatencySeconds AS NVARCHAR(2)) + N' SECONDS,
                    MEMORY_PARTITION_MODE=NONE,
                    TRACK_CAUSALITY=OFF,
                    STARTUP_STATE=OFF)';

            SET @dsql=@dsql1 + @dsql2;

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Creating trace with dynamic SQL. I REGRET NOTHING!!!'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            IF @Debug=1
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Debug mode, printing but not executing.'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                RAISERROR (@nl,0,1) WITH NOWAIT;

                RAISERROR (@dsql1,0,0) WITH NOWAIT;
                RAISERROR (@dsql2,0,0) WITH NOWAIT;
            END
            ELSE
            BEGIN

                EXEC sp_executesql @dsql;

            END
            SET @traceexists = 1;
        END

        IF @tracerunning = 0
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = START;'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            IF @Debug=0
            BEGIN
                ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = START;

                SET @tracerunning = 1;
            END
            ELSE
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- @Debug=1, not starting trace;'
                RAISERROR (@msg,0,1) WITH NOWAIT;
            END
        END
    END
    IF @Action = 'read'
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Reading, processing, and reporting.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF @traceexists = 0
        BEGIN
            SET @filepath=@TargetPath + N'*.xel';

        END

        /* Figure out the file name for current trace, if it's running */
        if @tracerunning=1
        BEGIN
            SELECT TOP 1 @filepathXML= x.target_data
            FROM sys.dm_xe_sessions as se
            JOIN sys.dm_xe_session_targets as t on
                se.address=t.event_session_address
            CROSS APPLY (SELECT cast(t.target_data as XML) AS target_data) as x
            WHERE se.name=N'sp_BlitzTrace'
            OPTION (RECOMPILE);

            SELECT @filepath=CAST(@filepathXML.query('data(EventFileTarget/File/@name)') AS VARCHAR(1024))
        END
        ELSE /* Figure out the file path for the trace if exists and isn't running */
        BEGIN
            SELECT TOP 1 @filepath=CAST(f.value AS VARCHAR(1024)) + N'*.xel'
            FROM sys.server_event_sessions AS ses
            LEFT OUTER JOIN sys.dm_xe_sessions AS running ON
                running.name = ses.name
            JOIN sys.server_event_session_targets AS t ON
                ses.event_session_id = t.event_session_id
                and t.package = 'package0'
                and t.name='event_file'
                and ses.name='sp_BlitzTrace'
            JOIN sys.dm_xe_objects AS o ON
                t.name = o.name
                AND o.object_type='target'
            JOIN sys.dm_xe_object_columns AS c ON
                t.name = c.object_name AND
                c.column_type = 'customizable' AND
                c.name='filename'
            JOIN sys.server_event_session_fields AS f ON
                t.event_session_id = f.event_session_id AND
                t.target_id = f.object_id
                AND c.name = f.name
        END

        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Using filepath: ' + @filepath
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF @Debug = 0
        BEGIN
            CREATE TABLE #sp_BlitzTraceXML (
                event_data XML NOT NULL
            );

            CREATE TABLE #sp_BlitzTraceEvents (
                event_time DATETIME2 NOT NULL,
                event_type NVARCHAR(256) NOT NULL,
                batch_text VARCHAR(MAX) NULL,
                sql_text VARCHAR(MAX) NULL,
                [statement] VARCHAR(MAX) NULL,
                duration_micros INT NULL,
                cpu_micros INT NULL,
                physical_reads INT NULL,
                logical_reads INT NULL,
                writes INT NULL,
                row_count INT NULL,
                result NVARCHAR(256) NULL,
                dop_statement_type SYSNAME NULL,
                dop INT NULL,
                workspace_memory_grant_kb INT NULL,
                object_id INT NULL,
                object_type sysname NULL,
                object_name sysname NULL,
                ddl_phase sysname NULL,
                recompile_cause sysname NULL,
                sort_warning_type VARCHAR(256) NULL,
                query_operation_node_id INT NULL,
                query_hash varchar(256) NULL,
                query_plan_hash varchar(256) NULL,
                estimated_cost bigint NULL,
                [showplan_xml] XML NULL,
                context_info sysname NULL,
                event_data XML NOT NULL
            )

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Populating #sp_BlitzTraceXML...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            INSERT #sp_BlitzTraceXML (event_data)
            SELECT
                x.event_data
            FROM  sys.fn_xe_file_target_read_file(@filepath,null,null,null) as xet
            CROSS APPLY (SELECT CAST(event_data AS XML) AS event_data) as x
            OUTER APPLY x.event_data.nodes('//event') AS y(n) OPTION (RECOMPILE);

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Started populating #sp_BlitzTraceEvents...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            INSERT #sp_BlitzTraceEvents (event_time, event_type, batch_text, sql_text, [statement], duration_micros, cpu_micros, physical_reads,
            logical_reads, writes, row_count, result, dop_statement_type, dop, workspace_memory_grant_kb, object_id, object_type,
            object_name, ddl_phase, recompile_cause, sort_warning_type, query_operation_node_id, query_hash, query_plan_hash,
            estimated_cost, [showplan_xml], context_info, event_data)
            SELECT
                DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), n.value('@timestamp', 'datetime2')) AS event_time,
                n.value('@name', 'NVARCHAR(max)') AS event_type,
                n.value('(data[@name="batch_text"]/value)[1]', 'varchar(max)') AS batch_text,
                n.value('(action[@name="sql_text"]/value)[1]', 'varchar(max)') AS sql_text,
                n.value('(data[@name="statement"]/value)[1]', 'varchar(max)') AS [statement],
                n.value('(data[@name="duration"]/value)[1]', 'int') AS duration_micros,
                n.value('(data[@name="cpu_time"]/value)[1]', 'int') AS cpu_micros,
                n.value('(data[@name="physical_reads"]/value)[1]', 'int') AS physical_reads,
                n.value('(data[@name="logical_reads"]/value)[1]', 'int') AS logical_reads,
                n.value('(data[@name="writes"]/value)[1]', 'int') AS writes,
                n.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
                n.value('(data[@name="result"]/text)[1]', 'varchar(256)') AS result,

                /* Parallelism */
                n.value('(data[@name="statement_type"]/text)[1]', 'varchar(256)') AS dop_statement_type,
                n.value('(data[@name="dop"]/value)[1]', 'int') AS dop,
                n.value('(data[@name="workspace_memory_grant_kb"]/value)[1]', 'bigint') AS workspace_memory_grant_kb,

                /* Object create comes in pairs, begin and end */
                n.value('(data[@name="object_id"]/value)[1]', 'int') AS [object_id],
                n.value('(data[@name="object_type"]/value)[1]', 'varchar(256)') AS object_type,
                n.value('(data[@name="object_name"]/value)[1]', 'varchar(256)') AS [object_name],
                n.value('(data[@name="ddl_phase"]/value)[1]', 'varchar(256)') AS ddl_phase,

                /* Recompiles */
                n.value('(data[@name="recompile_cause"]/text)[1]', 'varchar(256)') AS recompile_cause,

                /* tempdb spills */
                n.value('(data[@name="sort_warning_type"]/text)[1]', 'varchar(256)') AS sort_warning_type,
                n.value('(data[@name="query_operation_node_id"]/value)[1]', 'varchar(256)') AS query_operation_node_id,

                /* query hash and query plan hash */
                n.value('(action[@name="query_hash"]/value)[1]', 'varchar(256)') AS query_hash,
                n.value('(action[@name="query_plan_hash"]/value)[1]', 'varchar(256)') AS query_plan_hash,

                /* actual execution plan */
                n.value('(data[@name="estimated_cost"]/value)[1]', 'bigint') AS estimated_cost,
                n.query('(data[@name="showplan_xml"]/value/*)[1]') AS [showplan_xml],

                /* Context info, for filtering */
                n.value('(action[@name="context_info"]/value)[1]', 'VARCHAR(MAX)') AS [context_info],
                x.event_data as event_data
            FROM #sp_BlitzTraceXML AS xet
            CROSS APPLY (SELECT CAST(xet.event_data AS XML) AS event_data) as x
            OUTER APPLY x.event_data.nodes('//event') AS y(n)
            OPTION (RECOMPILE);

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Finished populating #sp_BlitzTraceEvents...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            SET @rowcount=@@ROWCOUNT;

            IF @rowcount = 0
            BEGIN
                RAISERROR('No rows found in the trace for these events for your session id',0,1) WITH NOWAIT;
            END
            ELSE
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- ' + CAST(@rowcount AS NVARCHAR(20)) + N' rows inserted into #sp_BlitzTraceEvents.'
                RAISERROR (@msg,0,1) WITH NOWAIT;
            END

            CREATE CLUSTERED INDEX cx_spBlitzTrace on #sp_BlitzTraceEvents (event_type, event_time)

            DELETE FROM #sp_BlitzTraceEvents
            WHERE (@SessionId=@@SPID and [context_info] = '73705f426c69747a5472616365204953205448452042455354')
            OR PATINDEX('%sp_BlitzTrace%',batch_text) <> 0
            OR PATINDEX('%sp_BlitzTrace%',sql_text) <> 0
                OPTION (RECOMPILE);

            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying sql_batch_completed, rpc_completed, sql_statement_completed, sp_statement_completed...'
            RAISERROR (@msg,0,1) WITH NOWAIT;
            /* sql_batch_completed, rpc_completed */
            SELECT
                event_time,
                event_type,
                batch_text,
                [statement],
                sql_text,
                duration_micros,
                cpu_micros,
                physical_reads,
                logical_reads,
                writes,
                row_count,
                result,
                query_hash,
                query_plan_hash,
                event_data
            FROM #sp_BlitzTraceEvents
            WHERE event_type in (N'sql_batch_completed',N'rpc_completed','sql_statement_completed', 'sp_statement_completed')
            ORDER BY 1;

            IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'degree_of_parallelism')
                IS NOT NULL
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying parallelism and memory grant...'
                RAISERROR (@msg,0,1) WITH NOWAIT;
                /* parallelism and memory grant */
                SELECT
                    event_time,
                    event_type,
                    sql_text,
                    dop_statement_type,
                    dop,
                    workspace_memory_grant_kb,
                    query_hash,
                    query_plan_hash,
                    event_data
                FROM #sp_BlitzTraceEvents
                WHERE event_type = 'degree_of_parallelism'
                  and query_hash <> '0'
                ORDER BY 1;
            END

            IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'object_created')
                IS NOT NULL
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying object_created ...'
                RAISERROR (@msg,0,1) WITH NOWAIT;
                /* object_created */
                SELECT
                    event_time,
                    event_type,
                    sql_text,
                    [object_id],
                    [object_name],
                    cpu_micros,
                    ddl_phase,
                    query_hash,
                    query_plan_hash,
                    event_data
                FROM #sp_BlitzTraceEvents
                WHERE event_type = 'object_created'
                ORDER BY 1;
            END

            IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'sql_statement_recompile')
                IS NOT NULL
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying sql_statement_recompile ...'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                /* sql_statement_recompile */
                SELECT
                    event_time,
                    event_type,
                    sql_text,
                    recompile_cause,
                    query_hash,
                    query_plan_hash,
                    event_data
                FROM #sp_BlitzTraceEvents
                WHERE event_type = 'sql_statement_recompile'
                ORDER BY 1;
            END

            IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'sort_warning')
                IS NOT NULL
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying sort_warning ...'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                /* sql_statement_recompile */
                SELECT
                    event_time,
                    event_type,
                    sort_warning_type,
                    query_operation_node_id,
                    query_hash,
                    query_plan_hash,
                    event_data
                FROM #sp_BlitzTraceEvents
                WHERE event_type = 'sort_warning'
                ORDER BY 1;
            END

            IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'query_post_execution_showplan')
                IS NOT NULL
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Querying actual execution plans ...'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                /* sql_statement_recompile */
                SELECT
                    event_time,
                    estimated_cost,
                    [object_name],
                    sql_text,
                    [showplan_xml] as [hope_this_isn't_production],
                    event_data
                FROM #sp_BlitzTraceEvents
                WHERE event_type = 'query_post_execution_showplan'
                ORDER BY 1;
            END

        END
        ELSE /* We're in @Debug=1 mode */
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- @Debug=1, not reading sp_BlitzTrace Extended Events session files;'
            RAISERROR (@msg,0,1) WITH NOWAIT;
        END
    END

    IF @Action = 'stop'
    BEGIN
        IF @tracerunning = 1
        BEGIN
            IF @Debug = 0
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Stopping sp_BlitzTrace Extended Events session.'
                RAISERROR (@msg,0,1) WITH NOWAIT;

                ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = STOP;

                SET @tracerunning = 0;
            END
            ELSE /* @Debug=1 */
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- @Debug=1, sp_BlitzTrace Extended Events session is running but we are NOT stopping it;'
                RAISERROR (@msg,0,1) WITH NOWAIT;
            END
        END
        ELSE
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- No running sp_BlitzTrace Extended Events session to stop.'
            RAISERROR (@msg,16,1) WITH NOWAIT;
        END
    END


    IF @Action = 'drop'
    BEGIN
        IF @traceexists = 1
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Dropping sp_BlitzTrace Extended Events session.'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            IF @Debug=0
            BEGIN
                DROP EVENT SESSION sp_BlitzTrace ON SERVER;

                SET @traceexists=0;
            END
            ELSE /* @Debug=1 */
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- @Debug=1, sp_BlitzTrace Extended Events session exists but we are NOT dropping it.'
                RAISERROR (@msg,0,1) WITH NOWAIT;
            END
        END
        ELSE
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- No sp_BlitzTrace XEvents trace to drop.'
            RAISERROR (@msg,16,1) WITH NOWAIT;
        END
    END

    RAISERROR (@nl,0,1) WITH NOWAIT;
    RAISERROR (N'********************READ ME!********************',0,1) WITH NOWAIT;


    IF @traceexists = 1 and @tracerunning = 0
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Extended Events session sp_BlitzTrace exists, but is stopped.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- To drop sp_BlitzTrace, run: exec dbo.sp_BlitzTrace @Action=''drop'';'
        RAISERROR (@msg,0,1) WITH NOWAIT;

    END
    ELSE IF @traceexists = 1 and @tracerunning = 1
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Extended Events session sp_BlitzTrace exists and is running' +
            CASE WHEN @SessionId is not null
               THEN N' for @SessionId=' + cast(@SessionId as NVARCHAR(5))
               ELSE N''
               END
        RAISERROR (@msg,0,1) WITH NOWAIT;

        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Don''t leave the sp_BlitzTrace session running for long periods!'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- To stop sp_BlitzTrace, run: exec dbo.sp_BlitzTrace @Action=''stop'';'
        RAISERROR (@msg,0,1) WITH NOWAIT;
    END
    ELSE IF @traceexists = 0
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Extended Events session sp_BlitzTrace doesn''t exist, we''re all cleaned up.'
        RAISERROR (@msg,0,1) WITH NOWAIT;
    END

    RAISERROR (N'********************SEE YA********************',0,1) WITH NOWAIT;

    SET CONTEXT_INFO 0x;
END TRY
BEGIN CATCH

    SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Catching error ...'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SELECT
        ERROR_NUMBER() AS ErrorNumber
        ,ERROR_SEVERITY() AS ErrorSeverity
        ,ERROR_STATE() AS ErrorState
        ,ERROR_PROCEDURE() AS ErrorProcedure
        ,ERROR_LINE() AS ErrorLine
        ,ERROR_MESSAGE() AS ErrorMessage;

    /* Re-check trace status */
    SELECT
        @traceexists = (CASE WHEN (s.name IS NULL) THEN 0 ELSE 1 END),
        @tracerunning = (CASE WHEN (r.create_time IS NULL) THEN 0 ELSE 1 END)
    FROM sys.server_event_sessions AS s
    LEFT OUTER JOIN sys.dm_xe_sessions AS r ON r.name = s.name
    WHERE s.name='sp_BlitzTrace'


    IF @Action='start' and @traceexists=1
    BEGIN
        SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- An error occurred starting the trace. Cleaning it up.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        DROP EVENT SESSION sp_BlitzTrace ON SERVER;
    END

    SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + N'- Resetting context and we''re outta here.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SET CONTEXT_INFO 0x;

END CATCH

GO
