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
exec sp_BlitzTrace @SessionId=51, @Seconds=60;
*/

IF OBJECT_ID('dbo.sp_BlitzTrace') IS NOT NULL
    DROP PROCEDURE dbo.sp_BlitzTrace 
GO
CREATE PROCEDURE dbo.sp_BlitzTrace
    @SessionId INT = NULL ,
    @Action VARCHAR(5) = 'timed',  /* 'timed','start', 'stop'*/
    @Seconds INT = 30, /* Timed traces only */
    @Target CHAR(4) = 'file', /*file, ring */
    @TargetPath VARCHAR(528) = 'S:\XEvents\Traces\sp_BlitzTrace' /* This needs to be prettied up.*/
AS
    /* (c) 2014 Brent Ozar Unlimited (R) */
    /* See http://BrentOzar.com/go/eula for the End User License Agreement */
    RAISERROR ('(c) 2014 Brent Ozar Unlimited (R).',0,1) WITH NOWAIT;
    RAISERROR ('See http://BrentOzar.com/go/eula for the End User License Agreement.',0,1) WITH NOWAIT;

    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET XACT_ABORT ON;

    DECLARE @msg NVARCHAR(MAX);
    DECLARE @rowcount INT;
    DECLARE @v decimal(6,2);
    DECLARE @build int;


    /* Validate parameters */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Validatin'' parameters.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    IF @Action NOT IN ('timed','start', 'stop')
    BEGIN
        RAISERROR ('Awk-ward! That''s not a supported action.',0,1) WITH NOWAIT;
        RETURN;
    END

    IF @Action in ('start','timed') AND @SessionId IS NULL
    BEGIN
        RAISERROR ('When starting sp_BlitzTrace, you need to specify @SessionId',0,1) WITH NOWAIT;
        RETURN;
    END

    IF @Target not in ('file', 'ring') 
    BEGIN
        RAISERROR ('Supported targets are ''file'' and ''ring'' (memory)',0,1) WITH NOWAIT;
        RETURN;
    END

    IF @Target = ('file') AND @TargetPath IS NULL
    BEGIN
        RAISERROR ('For file targets, you need to give a valid @TargetPath',0,1) WITH NOWAIT;
        RETURN;
    END

    /* Validate transaction state */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Validatin'' transaction state.'
    RAISERROR (@msg,0,1) WITH NOWAIT;
    IF @@TRANCOUNT > 0
    BEGIN
        RAISERROR ('@@TRANCOUNT > 0 not supported',0,1) WITH NOWAIT;
        RETURN;
    END

    /* Check version */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Determining SQL Server version.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    CREATE TABLE #checkversion (
        version nvarchar(128),
        maj_version AS SUBSTRING(version, 1,CHARINDEX('.', version) + 1 ),
        build AS PARSENAME(CONVERT(varchar(32), version), 2)
    );

    INSERT INTO #checkversion (version)
    SELECT CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))
    OPTION (RECOMPILE);

    SELECT @v = maj_version ,
           @build = build
    FROM   #checkversion
    OPTION (RECOMPILE);

    IF @v < '11'
    BEGIN
        /* Seriously. Not kidding. */
        /* degree_of_parallelism, object_created, sql_batch_completed, */
        /* sql_statement_recompile are all 2012+ only */
        SET @msg = 'Sad news: 90 percent of the events sp_BlitzTrace uses are only in SQL Server 2012 and higher-- so it''s not supported on SQL 2008/R2.'
        RAISERROR (@msg,0,1) WITH NOWAIT;
        RETURN;
    END

BEGIN TRY

    DECLARE	@FinishSampleTime DATETIME;
    IF @Action = 'timed'
    BEGIN
        SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- timed mode: setting @FinishSampleTime to ' + CAST (@FinishSampleTime AS NVARCHAR(128))
        RAISERROR (@msg,0,1) WITH NOWAIT;
        SET	@FinishSampleTime = DATEADD(ss, @Seconds, GETDATE());
    END

    /* We use this to filter sp_BlitzTrace activity out of the results, */
    /* in case you're tracing your own session. */
    /* That's just to make the results less confusing */
    DECLARE @context VARBINARY(128);
    SET @context=CAST('sp_BlitzTrace IS THE BEST' as binary);
    SET CONTEXT_INFO @context;

    IF @Action in ('start', 'timed')
    BEGIN
        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Creating and starting trace.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF (
            SELECT COUNT(*)
            FROM sys.server_event_sessions
            WHERE name='sp_BlitzTrace'
            ) > 0
        BEGIN
            DROP EVENT SESSION sp_BlitzTrace ON SERVER;
        END

        DECLARE @dsql NVARCHAR(MAX)=
            N'CREATE EVENT SESSION sp_BlitzTrace ON SERVER 
            ADD EVENT sqlserver.degree_of_parallelism (
                ACTION(sqlserver.context_info)
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N'))),
            ADD EVENT sqlserver.object_created (
                ACTION(sqlserver.context_info)
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N'))),
            ADD EVENT sqlserver.sql_batch_completed (
                ACTION(sqlserver.context_info)
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N'))),
            ADD EVENT sqlserver.sql_statement_recompile (
                ACTION(sqlserver.context_info)
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N'))), 
            ADD EVENT sqlserver.rpc_completed (
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N')))
' + CASE WHEN @Target=N'file' and @TargetPath is not null THEN + N'
            ADD TARGET package0.event_file(SET filename=''' + @TargetPath + N''',max_file_size=(256))
' ELSE + N'ADD TARGET package0.ring_buffer' END + N'
            WITH (MAX_MEMORY = 128 MB,
                EVENT_RETENTION_MODE = ALLOW_MULTIPLE_EVENT_LOSS,
                MAX_DISPATCH_LATENCY = 5 SECONDS, 
                MEMORY_PARTITION_MODE=NONE,
                TRACK_CAUSALITY=OFF,
                STARTUP_STATE=OFF)';

        RAISERROR ('Create trace dynamic sql',0,0) WITH NOWAIT;
        RAISERROR (@dsql,0,0) WITH NOWAIT;

        EXEC sp_executesql @dsql;

        ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = START;
    END

    IF @Action = 'timed'
    BEGIN
        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Waiting for the sample time to complete...'
        RAISERROR (@msg,0,1) WITH NOWAIT;

	    /* If we haven't waited @Seconds seconds, wait. */
	    IF GETDATE() < @FinishSampleTime
		    WAITFOR TIME @FinishSampleTime;
    END

    IF @Action in ('stop', 'timed')
    BEGIN
        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Reporting, stopping, and deleting trace.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF (
            SELECT COUNT(*)
            FROM sys.server_event_sessions
            WHERE name='sp_BlitzTrace'
            ) = 0
        BEGIN
            RAISERROR ('Whoops, sp_BlitzTrace Extended Events session doesn''t exist!',0,1) WITH NOWAIT;
        END
        ELSE 
        BEGIN
            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Populating #sp_BlitzTraceEvents...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            CREATE TABLE #sp_BlitzTraceEvents (
                event_time DATETIME2 NOT NULL,
                event_type NVARCHAR(MAX) NOT NULL,
                batch_text NVARCHAR(MAX) NULL,
                duration_ms INT NULL,
                cpu_time_ms INT NULL,
                physical_reads INT NULL,
                logical_reads INT NULL,
                writes INT NULL,
                row_count INT NULL,
                dop_statement_type SYSNAME NULL,
                dop INT NULL,
                workspace_memory_grant_kb INT NULL,
                object_id INT NULL,
                object_type sysname NULL,
                object_name sysname NULL,
                ddl_phase sysname NULL,
                recompile_cause sysname NULL,
                context_info sysname NULL
            )

            IF @Target=N'file' and @TargetPath is not null 
            BEGIN
                DECLARE @TargetPathFull NVARCHAR(MAX)=@TargetPath + N'*.xel'

                INSERT #sp_BlitzTraceEvents (event_time, event_type, batch_text, duration_ms, cpu_time_ms, physical_reads,
                logical_reads, writes, row_count, dop_statement_type, dop, workspace_memory_grant_kb, object_id, object_type,
                object_name, ddl_phase, recompile_cause, context_info)
                SELECT  
                    DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), n.value('@timestamp', 'datetime2')) AS event_time,
                    n.value('@name', 'nvarchar(max)') AS event_type,
                    n.value('(data[@name="batch_text"]/value)[1]', 'nvarchar(max)') AS batch_text,
                    n.value('(data[@name="duration"]/value)[1]', 'int') AS duration_ms,
                    n.value('(data[@name="cpu_time"]/value)[1]', 'int') AS cpu_time_ms,
                    n.value('(data[@name="physical_reads"]/value)[1]', 'int') AS physical_reads,
                    n.value('(data[@name="logical_reads"]/value)[1]', 'int') AS logical_reads,
                    n.value('(data[@name="writes"]/value)[1]', 'int') AS writes,
                    n.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
                    /* Parallelism */
                    n.value('(data[@name="statement_type"]/text)[1]', 'sysname') AS dop_statement_type,
                    n.value('(data[@name="dop"]/value)[1]', 'int') AS dop,
                    n.value('(data[@name="workspace_memory_grant_kb"]/value)[1]', 'bigint') AS workspace_memory_grant_kb,
                    /* Object create comes in pairs, begin and end */
                    n.value('(data[@name="object_id"]/value)[1]', 'int') AS [object_id],
                    n.value('(data[@name="object_type"]/value)[1]', 'sysname') AS object_type,
                    n.value('(data[@name="object_name"]/value)[1]', 'sysname') AS [object_name],
                    n.value('(data[@name="ddl_phase"]/value)[1]', 'sysname') AS ddl_phase,
                    /* Recompiles */
                    n.value('(data[@name="recompile_cause"]/value)[1]', 'sysname') AS recompile_cause,
                    /* Context info, for filtering */
                    n.value('(action[@name="context_info"]/value)[1]', 'NVARCHAR(MAX)') AS [context_info]
                    --x.event_data
                FROM  sys.fn_xe_file_target_read_file(@TargetPathFull,null,null,null) as xet
                CROSS APPLY (SELECT CAST(event_data AS XML) AS event_data) as x 
                OUTER APPLY x.event_data.nodes('//event') AS y(n);

            END
            ELSE /* ring buffer */ 
            BEGIN
                INSERT #sp_BlitzTraceEvents (event_time, event_type, batch_text, duration_ms, cpu_time_ms, physical_reads,
                logical_reads, writes, row_count, dop_statement_type, dop, workspace_memory_grant_kb, object_id, object_type,
                object_name, ddl_phase, recompile_cause, context_info)
                SELECT  
                    DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), n.value('@timestamp', 'datetime2')) AS event_time,
                    n.value('@name', 'nvarchar(max)') AS event_type,
                    n.value('(data[@name="batch_text"]/value)[1]', 'nvarchar(max)') AS batch_text,
                    n.value('(data[@name="duration"]/value)[1]', 'int') AS duration_ms,
                    n.value('(data[@name="cpu_time"]/value)[1]', 'int') AS cpu_time_ms,
                    n.value('(data[@name="physical_reads"]/value)[1]', 'int') AS physical_reads,
                    n.value('(data[@name="logical_reads"]/value)[1]', 'int') AS logical_reads,
                    n.value('(data[@name="writes"]/value)[1]', 'int') AS writes,
                    n.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
                    /* Parallelism */
                    n.value('(data[@name="statement_type"]/text)[1]', 'sysname') AS dop_statement_type,
                    n.value('(data[@name="dop"]/value)[1]', 'int') AS dop,
                    n.value('(data[@name="workspace_memory_grant_kb"]/value)[1]', 'bigint') AS workspace_memory_grant_kb,
                    /* Object create comes in pairs, begin and end */
                    n.value('(data[@name="object_id"]/value)[1]', 'int') AS [object_id],
                    n.value('(data[@name="object_type"]/value)[1]', 'sysname') AS object_type,
                    n.value('(data[@name="object_name"]/value)[1]', 'sysname') AS [object_name],
                    n.value('(data[@name="ddl_phase"]/value)[1]', 'sysname') AS ddl_phase,
                    /* Recompiles */
                    n.value('(data[@name="recompile_cause"]/value)[1]', 'sysname') AS recompile_cause,
                    /* Context info, for filtering */
                    n.value('(action[@name="context_info"]/value)[1]', 'NVARCHAR(MAX)') AS [context_info]
                    --x.event_data
                FROM sys.dm_xe_session_targets AS xet
                JOIN sys.dm_xe_sessions AS xe
                    ON xe.address = xet.event_session_address
                    and xe.name = 'sp_BlitzTrace'
                CROSS APPLY (SELECT CAST(xet.target_data AS XML) AS event_data) as x 
                OUTER APPLY x.event_data.nodes('//event') AS y(n);
            END
            set @rowcount=@@ROWCOUNT

            IF @rowcount = 0
            BEGIN
                RAISERROR('No rows found in the trace for these events for your session id',0,1) WITH NOWAIT;
            END
            ELSE 
            BEGIN
                SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + '- ' + CAST(@rowcount AS NVARCHAR(20)) + N' rows inserted into #sp_BlitzTraceEvents.'
                RAISERROR (@msg,0,1) WITH NOWAIT;
            END

            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Querying sql_batch_completed, rpc_completed...'
            RAISERROR (@msg,0,1) WITH NOWAIT;
            /* sql_batch_completed, rpc_completed */
            SELECT  
                event_time,
                event_type,
                batch_text,
                duration_ms,
                cpu_time_ms,
                physical_reads,
                logical_reads,
                writes,
                row_count
            FROM #sp_BlitzTraceEvents
            WHERE event_type in (N'sql_batch_completed',N'rpc_completed')
            AND PATINDEX('%sp_BlitzTrace%',batch_text) = 0
            ORDER BY 1;

            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Querying parallelism and memory grant...'
            RAISERROR (@msg,0,1) WITH NOWAIT;
            /* parallelism and memory grant */
            SELECT  
                event_time,
                event_type,
                dop_statement_type,
                dop,
                workspace_memory_grant_kb, 
                [context_info]
            FROM #sp_BlitzTraceEvents
            WHERE event_type = N'degree_of_parallelism'
            AND NOT (@SessionId=@@SPID and [context_info] = '73705f426c69747a5472616365204953205448452042455354')
            ORDER BY 1;

            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Querying object_created ...'
            RAISERROR (@msg,0,1) WITH NOWAIT;
            /* object_created */
            SELECT  
                event_time,
                event_type,
                [object_id],
                [object_name],
                cpu_time_ms,
                ddl_phase
            FROM #sp_BlitzTraceEvents
            WHERE event_type = N'object_created'
            AND NOT (@SessionId=@@SPID and [context_info] = '73705f426c69747a5472616365204953205448452042455354')
            ORDER BY 1;

            /* sql_statement_recompile */
            SELECT  
                event_time,
                event_type,
                recompile_cause,
                [context_info]
            FROM #sp_BlitzTraceEvents
            WHERE event_type = N'sql_statement_recompile'
            AND NOT (@SessionId=@@SPID and [context_info] = '73705f426c69747a5472616365204953205448452042455354')
            ORDER BY 1;

            SET @msg= 'Stopping and deleting trace.'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            /* Clean up */
            ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = STOP;
            DROP EVENT SESSION sp_BlitzTrace ON SERVER;
        END
    END

    SET @msg= 'Resetting context and we''re outta here.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SET CONTEXT_INFO 0x;
END TRY
BEGIN CATCH
    SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Catching error ...'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SELECT
        ERROR_NUMBER() AS ErrorNumber
        ,ERROR_SEVERITY() AS ErrorSeverity
        ,ERROR_STATE() AS ErrorState
        ,ERROR_PROCEDURE() AS ErrorProcedure
        ,ERROR_LINE() AS ErrorLine
        ,ERROR_MESSAGE() AS ErrorMessage;

END CATCH

GO