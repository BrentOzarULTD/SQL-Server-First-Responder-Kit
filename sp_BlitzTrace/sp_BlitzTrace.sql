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
    @Action VARCHAR(5) = 'timed',  /* 'timed','start', 'read', 'stop'*/
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
    DECLARE @datestamp VARCHAR(30);
    DECLARE @TargetPathFull NVARCHAR(MAX);
    DECLARE @TargetPathRead NVARCHAR(MAX);


    /* Validate parameters */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Validatin'' parameters.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    IF @Action NOT IN ('timed','start', 'read', 'stop')
    BEGIN
        RAISERROR ('Awk-ward! That''s not a supported action.',16,1) WITH NOWAIT;
    END

    IF @Action in ('start','timed') AND @SessionId IS NULL
    BEGIN
        RAISERROR ('When starting sp_BlitzTrace, you need to specify @SessionId',16,1) WITH NOWAIT;
    END

    IF @Target not in ('file', 'ring') 
    BEGIN
        RAISERROR ('Supported targets are ''file'' and ''ring'' (memory)',16,1) WITH NOWAIT;
    END

    IF @Target = ('file') AND @TargetPath IS NULL
    BEGIN
        RAISERROR ('For file targets, you need to give a valid @TargetPath',16,1) WITH NOWAIT;
    END

    IF @Target='file'
    BEGIN
        SELECT @datestamp = REPLACE( CONVERT(VARCHAR(26),getdate(),120),':','-')
        SET @TargetPathFull=@TargetPath + @datestamp +  N'.xel'

        SET @TargetPathRead=@TargetPath + N'*.xel'

        SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- File target is: ' + @TargetPathFull
        RAISERROR (@msg,0,1) WITH NOWAIT;

    END

    /* Validate transaction state */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Validatin'' transaction state.'
    RAISERROR (@msg,0,1) WITH NOWAIT;
    IF @@TRANCOUNT > 0
    BEGIN
        RAISERROR ('@@TRANCOUNT > 0 not supported',16,1) WITH NOWAIT;
    END

    /* Check version */
    SET @msg = CONVERT(nvarchar(30), GETDATE(), 126) + '- Determining SQL Server version.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    SELECT @v = SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)), 1,CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))) + 1 )

    IF @v < '11'
    BEGIN
        /* Seriously. Not kidding. */
        /* degree_of_parallelism, object_created, sql_batch_completed, */
        /* sql_statement_recompile are all 2012+ only */
        SET @msg = 'Sad news: 90 percent of the events sp_BlitzTrace uses are only in SQL Server 2012 and higher-- so it''s not supported on SQL 2008/R2.'
        RAISERROR (@msg,16,1) WITH NOWAIT;
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
            ADD EVENT sqlserver.sort_warning (
                ACTION(sqlserver.context_info)
                WHERE ([sqlserver].[session_id]=('+ CAST(@SessionId as NVARCHAR(3)) + N'))),
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
            ADD TARGET package0.event_file(SET filename=''' + @TargetPathFull + N''',max_file_size=(256))
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

    IF @Action in ('read', 'timed')
    BEGIN
        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Processing and reporting.'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF (
            SELECT COUNT(*)
            FROM sys.server_event_sessions
            WHERE name='sp_BlitzTrace'
            ) = 0
        BEGIN
            RAISERROR ('Whoops, sp_BlitzTrace Extended Events session doesn''t exist!',0,1) WITH NOWAIT;
        END

        CREATE TABLE #sp_BlitzTraceXML (
            event_data XML NOT NULL
        );

        CREATE TABLE #sp_BlitzTraceEvents (
            event_time DATETIME2 NOT NULL,
            event_type NVARCHAR(256) NOT NULL,
            batch_text VARCHAR(MAX) NULL,
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
            sort_warning_type VARCHAR(256) NULL,
            query_operation_node_id INT NULL,
            context_info sysname NULL
        )

        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Populating #sp_BlitzTraceXML...'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        IF @Target=N'file' and @Action in ('read','timed')
        BEGIN

            INSERT #sp_BlitzTraceXML (event_data)
            SELECT  
                x.event_data
            FROM  sys.fn_xe_file_target_read_file(@TargetPathRead,null,null,null) as xet
            CROSS APPLY (SELECT CAST(event_data AS XML) AS event_data) as x 
            OUTER APPLY x.event_data.nodes('//event') AS y(n) OPTION (RECOMPILE);

        END
        ELSE IF @Target='ring' and @Action in ('read','timed')
        BEGIN
            IF (
                SELECT COUNT(*)
                FROM sys.server_event_sessions
                WHERE name='sp_BlitzTrace'
                ) = 0
            BEGIN
                SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Attempting to read from memory, but XEvents trace doesn''t exist. Oops!'
                RAISERROR (@msg,16,1) WITH NOWAIT;
            END

            INSERT #sp_BlitzTraceXML (event_data)
            SELECT
                x.event_data
            FROM sys.dm_xe_session_targets AS xet
            JOIN sys.dm_xe_sessions AS xe
                ON xe.address = xet.event_session_address
                and xe.name = 'sp_BlitzTrace'
            CROSS APPLY (SELECT CAST(xet.target_data AS XML) AS event_data) as x 
            OPTION (RECOMPILE);
        END

        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Started populating #sp_BlitzTraceEvents...'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        INSERT #sp_BlitzTraceEvents (event_time, event_type, batch_text, duration_ms, cpu_time_ms, physical_reads,
        logical_reads, writes, row_count, dop_statement_type, dop, workspace_memory_grant_kb, object_id, object_type,
        object_name, ddl_phase, recompile_cause, sort_warning_type, query_operation_node_id, context_info)
        SELECT  
            DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), n.value('@timestamp', 'datetime2')) AS event_time,
            n.value('@name', 'nvarchar(max)') AS event_type,
            n.value('(data[@name="batch_text"]/value)[1]', 'varchar(max)') AS batch_text,
            n.value('(data[@name="duration"]/value)[1]', 'int') AS duration_ms,
            n.value('(data[@name="cpu_time"]/value)[1]', 'int') AS cpu_time_ms,
            n.value('(data[@name="physical_reads"]/value)[1]', 'int') AS physical_reads,
            n.value('(data[@name="logical_reads"]/value)[1]', 'int') AS logical_reads,
            n.value('(data[@name="writes"]/value)[1]', 'int') AS writes,
            n.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
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
            /* Context info, for filtering */
            n.value('(action[@name="context_info"]/value)[1]', 'VARCHAR(MAX)') AS [context_info]
            --,n.event_data
        FROM #sp_BlitzTraceXML AS xet
        CROSS APPLY (SELECT CAST(xet.event_data AS XML) AS event_data) as x 
        OUTER APPLY x.event_data.nodes('//event') AS y(n)  OPTION (RECOMPILE);

        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Finished populating #sp_BlitzTraceEvents...'
        RAISERROR (@msg,0,1) WITH NOWAIT;

        SET @rowcount=@@ROWCOUNT

        IF @rowcount = 0
        BEGIN
            RAISERROR('No rows found in the trace for these events for your session id',0,1) WITH NOWAIT;
        END
        ELSE 
        BEGIN
            SET @msg= CONVERT(NVARCHAR(30), GETDATE(), 126) + '- ' + CAST(@rowcount AS NVARCHAR(20)) + N' rows inserted into #sp_BlitzTraceEvents.'
            RAISERROR (@msg,0,1) WITH NOWAIT;
        END

        CREATE CLUSTERED INDEX cx_spBlitzTrace on #sp_BlitzTraceEvents (event_type, event_time)

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

        IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'degree_of_parallelism') 
            IS NOT NULL
        BEGIN
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
        END

        IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'object_created') 
            IS NOT NULL
        BEGIN
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
        END

        IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'sql_statement_recompile') 
            IS NOT NULL
        BEGIN
            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Querying sql_statement_recompile ...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

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
        END

        IF (SELECT TOP 1 event_time FROM #sp_BlitzTraceEvents WHERE event_type = N'sort_warning') 
            IS NOT NULL
        BEGIN
            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Querying sort_warning ...'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            /* sql_statement_recompile */
            SELECT  
                event_time,
                event_type,
                sort_warning_type,
                query_operation_node_id
            FROM #sp_BlitzTraceEvents
            WHERE event_type = N'sort_warning'
            AND NOT (@SessionId=@@SPID and [context_info] = '73705f426c69747a5472616365204953205448452042455354')
            ORDER BY 1;
        END

    END

    IF @Action in ('stop', 'timed')
    BEGIN

        IF (
            SELECT COUNT(*)
            FROM sys.server_event_sessions
            WHERE name='sp_BlitzTrace'
            ) > 0
        BEGIN
            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Stopping and dropping XEvents trace.'
            RAISERROR (@msg,0,1) WITH NOWAIT;

            ALTER EVENT SESSION sp_BlitzTrace ON SERVER STATE = STOP;
            DROP EVENT SESSION sp_BlitzTrace ON SERVER;
        END
        ELSE
        BEGIN
            SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- No sp_BlitzTrace XEvents trace to stop and drop.'
            RAISERROR (@msg,16,1) WITH NOWAIT;
        END
    END

    SET @msg= 'Resetting context and we''re outta here.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

    IF (
        SELECT COUNT(*)
        FROM sys.server_event_sessions
        WHERE name='sp_BlitzTrace'
        ) > 0
    BEGIN
        SET @msg= CONVERT(nvarchar(30), GETDATE(), 126) + '- Extended Events Session sp_BlitzTrace exists!'
        RAISERROR (@msg,0,1) WITH NOWAIT;
    END

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

    SET @msg= 'Resetting context and we''re outta here.'
    RAISERROR (@msg,0,1) WITH NOWAIT;

END CATCH

GO