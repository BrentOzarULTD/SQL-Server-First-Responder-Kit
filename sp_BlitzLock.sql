IF OBJECT_ID('dbo.sp_BlitzLock') IS NULL
BEGIN
    EXECUTE ('CREATE PROCEDURE dbo.sp_BlitzLock AS RETURN 0;');
END;
GO

ALTER PROCEDURE
    dbo.sp_BlitzLock
(
    @DatabaseName sysname = NULL,
    @StartDate datetime = NULL,
    @EndDate datetime = NULL,
    @ObjectName nvarchar(1024) = NULL,
    @StoredProcName nvarchar(1024) = NULL,
    @AppName sysname = NULL,
    @HostName sysname = NULL,
    @LoginName sysname = NULL,
    @EventSessionName sysname = N'system_health',
    @TargetSessionType sysname = NULL,
    @VictimsOnly bit = 0,
    @DeadlockType nvarchar(20) = NULL,
    @TargetDatabaseName sysname = NULL,      
    @TargetSchemaName sysname = NULL,        
    @TargetTableName sysname = NULL,         
    @TargetColumnName sysname = NULL,       
    @TargetTimestampColumnName sysname = NULL,
    @Debug bit = 0,
    @Help bit = 0,
    @Version varchar(30) = NULL OUTPUT,
    @VersionDate datetime = NULL OUTPUT,
    @VersionCheckMode bit = 0,
    @OutputDatabaseName sysname = NULL,
    @OutputSchemaName sysname = N'dbo',      /*ditto as below*/
    @OutputTableName sysname = N'BlitzLock', /*put a standard here no need to check later in the script*/
    @ExportToExcel bit = 0
)
WITH RECOMPILE
AS
BEGIN
    SET STATISTICS XML OFF;
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    SELECT @Version = '8.25', @VersionDate = '20250704';

    IF @VersionCheckMode = 1
    BEGIN
        RETURN;
    END;

    IF @Help = 1
    BEGIN
        PRINT N'
    /*
    sp_BlitzLock from http://FirstResponderKit.org

    This script checks for and analyzes deadlocks from the system health session or a custom extended event path

    Variables you can use:

        /*Filtering parameters*/
        @DatabaseName: If you want to filter to a specific database

        @StartDate: The date you want to start searching on, defaults to last 7 days

        @EndDate: The date you want to stop searching on, defaults to current date

        @ObjectName: If you want to filter to a specific able.
                     The object name has to be fully qualified ''Database.Schema.Table''

        @StoredProcName: If you want to search for a single stored proc
                     The proc name has to be fully qualified ''Database.Schema.Sproc''

        @AppName: If you want to filter to a specific application

        @HostName: If you want to filter to a specific host

        @LoginName: If you want to filter to a specific login

        @DeadlockType: Search for regular or parallel deadlocks specifically

        /*Extended Event session details*/
        @EventSessionName: If you want to point this at an XE session rather than the system health session.

        @TargetSessionType: Can be ''ring_buffer'',  ''event_file'', or ''table''. Leave NULL to auto-detect.

        /*Output to a table*/
        @OutputDatabaseName: If you want to output information to a specific database

        @OutputSchemaName: Specify a schema name to output information to a specific Schema

        @OutputTableName: Specify table name to to output information to a specific table

        /*Point at a table containing deadlock XML*/
        @TargetDatabaseName: The database that contains the table with deadlock report XML

        @TargetSchemaName: The schema of the table containing deadlock report XML

        @TargetTableName: The name of the table containing deadlock report XML
       
        @TargetColumnName: The name of the XML column that contains the deadlock report
       
        @TargetTimestampColumnName: The name of the datetime column for filtering by date range (optional)

   
    To learn more, visit http://FirstResponderKit.org where you can download new
    versions for free, watch training videos on how it works, get more info on
    the findings, contribute your own code, and more.

    Known limitations of this version:
     - Only SQL Server 2012 and newer is supported
     - If your tables have weird characters in them (https://en.wikipedia.org/wiki/List_of_xml_and_HTML_character_entity_references) you may get errors trying to parse the xml.
       I took a long look at this one, and:
        1) Trying to account for all the weird places these could crop up is a losing effort.
        2) Replace is slow af on lots of xml.

    Unknown limitations of this version:
     - None.  (If we knew them, they would be known. Duh.)

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

    */';

        RETURN;
    END; /* @Help = 1 */

    /*Declare local variables used in the procudure*/
    DECLARE
        @DatabaseId int =
            DB_ID(@DatabaseName),
        @ProductVersion nvarchar(128) =
            CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),
        @ProductVersionMajor float =
            SUBSTRING
            (
                CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),
                1,
                CHARINDEX('.', CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))) + 1
            ),
        @ProductVersionMinor int =
            PARSENAME
            (
                CONVERT
                (
                    varchar(32),
                    CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))
                ),
                2
            ),
        @ObjectFullName nvarchar(MAX) = N'',
        @Azure bit =
            CASE
                WHEN
                (
                    SELECT
                        CONVERT
                        (
                            integer,
                            SERVERPROPERTY('EngineEdition')
                        )
                ) = 5
                THEN 1
                ELSE 0
            END,
        @MI bit =
            CASE
                WHEN
                (
                    SELECT
                        CONVERT
                        (
                            integer,
                            SERVERPROPERTY('EngineEdition')
                        )
                ) = 8
                THEN 1
                ELSE 0
            END,
        @RDS bit =
            CASE
                WHEN LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS varchar(8000)), 8) <> 'EC2AMAZ-'
                AND  LEFT(CAST(SERVERPROPERTY('MachineName') AS varchar(8000)), 8) <> 'EC2AMAZ-'
                AND  DB_ID('rdsadmin') IS NULL
                THEN 0
                ELSE 1
            END,
        @d varchar(40) = '',
        @StringToExecute nvarchar(4000) = N'',
        @StringToExecuteParams nvarchar(500) = N'',
        @r sysname = NULL,
        @OutputTableFindings nvarchar(100) = N'[BlitzLockFindings]',
        @DeadlockCount int = 0,
        @ServerName sysname = @@SERVERNAME,
        @OutputDatabaseCheck bit = -1,
        @SessionId int = 0,
        @TargetSessionId int = 0,
        @FileName nvarchar(4000) = N'',
        @inputbuf_bom nvarchar(1) = CONVERT(nvarchar(1), 0x0a00, 0),
        @deadlock_result nvarchar(MAX) = N'',
        @StartDateOriginal datetime = @StartDate,
        @EndDateOriginal datetime = @EndDate,
        @StartDateUTC datetime,
        @EndDateUTC datetime,
        @extract_sql nvarchar(MAX),
        @validation_sql nvarchar(MAX),
        @xe bit,
        @xd bit;

    /*Temporary objects used in the procedure*/
    DECLARE
        @sysAssObjId AS table
    (
        database_id int,
        partition_id bigint,
        schema_name sysname,
        table_name sysname
    );

    CREATE TABLE
        #x
    (
        x xml NOT NULL
            DEFAULT N'<x>x</x>'
    );

    CREATE TABLE
        #deadlock_data
    (
        deadlock_xml xml NOT NULL
            DEFAULT N'<x>x</x>'
    );

    CREATE TABLE
        #t
    (
        id int NOT NULL
    );

    CREATE TABLE
        #deadlock_findings
    (
        id int IDENTITY PRIMARY KEY,
        check_id int NOT NULL,
        database_name nvarchar(256),
        object_name nvarchar(1000),
        finding_group nvarchar(100),
        finding nvarchar(4000),
        sort_order bigint
    );

    /*Set these to some sane defaults if NULLs are passed in*/
    /*Normally I'd hate this, but we RECOMPILE everything*/

    SELECT
        @StartDate =
            CASE
                WHEN @StartDate IS NULL
                THEN
                    DATEADD
                    (
                        MINUTE,
                        DATEDIFF
                        (
                            MINUTE,
                            SYSDATETIME(),
                            GETUTCDATE()
                        ),
                        DATEADD
                        (
                            DAY,
                            -7,
                            SYSDATETIME()
                        )
                    )
                ELSE
                    DATEADD
                    (
                        MINUTE,
                        DATEDIFF
                        (
                            MINUTE,
                            SYSDATETIME(),
                            GETUTCDATE()
                        ),
                        @StartDate
                    )
            END,
        @EndDate =
            CASE
                WHEN @EndDate IS NULL
                THEN
                    DATEADD
                    (
                        MINUTE,
                        DATEDIFF
                        (
                            MINUTE,
                            SYSDATETIME(),
                            GETUTCDATE()
                        ),
                        SYSDATETIME()
                    )
                ELSE
                    DATEADD
                    (
                        MINUTE,
                        DATEDIFF
                        (
                            MINUTE,
                            SYSDATETIME(),
                            GETUTCDATE()
                        ),
                        @EndDate
                    )
            END;

    SELECT
        @StartDateUTC = @StartDate,
        @EndDateUTC = @EndDate;

    IF
    (
            @MI = 1
        AND @EventSessionName = N'system_health'
        AND @TargetSessionType IS NULL
    )
    BEGIN
        SET
            @TargetSessionType = N'ring_buffer';
    END;

    IF  ISNULL(@TargetDatabaseName, DB_NAME()) IS NOT NULL
    AND ISNULL(@TargetSchemaName, N'dbo') IS NOT NULL
    AND @TargetTableName IS NOT NULL
    AND @TargetColumnName IS NOT NULL
    BEGIN
        SET @TargetSessionType = N'table';
    END;

    /* Add this after the existing parameter validations */
    IF @TargetSessionType = N'table'
    BEGIN     
        IF @TargetDatabaseName IS NULL
        BEGIN
            SET @TargetDatabaseName = DB_NAME();
        END;

        IF @TargetSchemaName IS NULL
        BEGIN
            SET @TargetSchemaName = N'dbo';
        END;

        IF @TargetTableName IS NULL
        OR @TargetColumnName IS NULL
        BEGIN
            RAISERROR(N'
            When using a table as a source, you must specify @TargetTableName, and @TargetColumnName.
            When @TargetDatabaseName or @TargetSchemaName is NULL, they default to DB_NAME() AND dbo',
            11, 1) WITH NOWAIT;
            RETURN;
        END;
   
        /* Check if target database exists */
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM sys.databases AS d
            WHERE d.name = @TargetDatabaseName
        )
        BEGIN
            RAISERROR(N'The specified @TargetDatabaseName %s does not exist.', 11, 1, @TargetDatabaseName) WITH NOWAIT;
            RETURN;
        END;
   
        /* Use dynamic SQL to validate schema, table, and column existence */
        SET @validation_sql = N'
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
            WHERE s.name = @schema
        )
        BEGIN
            RAISERROR(N''The specified @TargetSchemaName %s does not exist in database %s.'', 11, 1, @schema, @database) WITH NOWAIT;
            RETURN;
        END;
   
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.tables AS t
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE t.name = @table
            AND   s.name = @schema
        )
        BEGIN
            RAISERROR(N''The specified @TargetTableName %s does not exist in schema %s in database %s.'', 11, 1, @table, @schema, @database) WITH NOWAIT;
            RETURN;
        END;
   
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.columns AS c
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.tables AS t
              ON c.object_id = t.object_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE c.name = @column
            AND   t.name = @table
            AND   s.name = @schema
        )
        BEGIN
            RAISERROR(N''The specified @TargetColumnName %s does not exist in table %s.%s in database %s.'', 11, 1, @column, @schema, @table, @database) WITH NOWAIT;
            RETURN;
        END;
   
        /* Validate column is XML type */
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.columns AS c
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.types AS ty
              ON c.user_type_id = ty.user_type_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.tables AS t
              ON c.object_id = t.object_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE c.name = @column
            AND   t.name = @table
            AND   s.name = @schema
            AND   ty.name = N''xml''
        )
        BEGIN
            RAISERROR(N''The specified @TargetColumnName %s must be of XML data type.'', 11, 1, @column) WITH NOWAIT;
            RETURN;
        END;';
   
        /* Validate timestamp_column if specified */
        IF @TargetTimestampColumnName IS NOT NULL
        BEGIN
            SET @validation_sql = @validation_sql + N'
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.columns AS c
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.tables AS t
              ON c.object_id = t.object_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE c.name = @timestamp_column
            AND   t.name = @table
            AND   s.name = @schema
        )
        BEGIN
            RAISERROR(N''The specified @TargetTimestampColumnName %s does not exist in table %s.%s in database %s.'', 11, 1, @timestamp_column, @schema, @table, @database) WITH NOWAIT;
            RETURN;
        END;
   
        /* Validate timestamp column is datetime type */
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM ' + QUOTENAME(@TargetDatabaseName) + N'.sys.columns AS c
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.types AS ty
              ON c.user_type_id = ty.user_type_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.tables AS t
              ON c.object_id = t.object_id
            JOIN ' + QUOTENAME(@TargetDatabaseName) + N'.sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE c.name = @timestamp_column
            AND   t.name = @table
            AND   s.name = @schema
            AND   ty.name LIKE ''%date%''
        )
        BEGIN
            RAISERROR(N''The specified @TargetTimestampColumnName %s must be of datetime data type.'', 11, 1, @timestamp_column) WITH NOWAIT;
            RETURN;
        END;';
        END;
   
        IF @Debug = 1 BEGIN PRINT @validation_sql; END;
       
        EXECUTE sys.sp_executesql
            @validation_sql,
          N'
            @database sysname,
            @schema sysname,
            @table sysname,
            @column sysname,
            @timestamp_column sysname
          ',
            @TargetDatabaseName,
            @TargetSchemaName,
            @TargetTableName,
            @TargetColumnName,
            @TargetTimestampColumnName;
    END;


    IF @Azure = 0
    AND LOWER(@TargetSessionType) <> N'table'
    BEGIN
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM sys.server_event_sessions AS ses
            JOIN sys.dm_xe_sessions AS dxs
              ON dxs.name = ses.name
            WHERE ses.name = @EventSessionName
            AND   dxs.create_time IS NOT NULL
        )
        BEGIN
            RAISERROR('A session with the name %s does not exist or is not currently active.', 11, 1, @EventSessionName) WITH NOWAIT;
            RETURN;
        END;
    END;

    IF @Azure = 1
    AND LOWER(@TargetSessionType) <> N'table'
    BEGIN
        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM sys.database_event_sessions AS ses
            JOIN sys.dm_xe_database_sessions AS dxs
              ON dxs.name = ses.name
            WHERE ses.name = @EventSessionName
            AND   dxs.create_time IS NOT NULL
        )
        BEGIN
            RAISERROR('A session with the name %s does not exist or is not currently active.', 11, 1, @EventSessionName) WITH NOWAIT;
            RETURN;
        END;
    END;

    IF @OutputDatabaseName IS NOT NULL
    BEGIN /*IF databaseName is set, do some sanity checks and put [] around def.*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('@OutputDatabaseName set to %s, checking validity at %s', 0, 1, @OutputDatabaseName, @d) WITH NOWAIT;

        IF NOT EXISTS
        (
            SELECT
                1/0
            FROM sys.databases AS d
            WHERE d.name = @OutputDatabaseName
        ) /*If database is invalid raiserror and set bitcheck*/
        BEGIN
            RAISERROR('Database Name (%s) for output of table is invalid please, Output to Table will not be performed', 0, 1, @OutputDatabaseName) WITH NOWAIT;
            SET @OutputDatabaseCheck = -1; /* -1 invalid/false, 0 = good/true */
        END;
        ELSE
        BEGIN
            SET @OutputDatabaseCheck = 0;

            SELECT
                @StringToExecute =
                    N'SELECT @r = o.name FROM ' +
                    @OutputDatabaseName +
                    N'.sys.objects AS o inner join ' +
                    @OutputDatabaseName +
                    N'.sys.schemas as s on o.schema_id = s.schema_id WHERE o.type_desc = N''USER_TABLE'' AND o.name = ' +
                    QUOTENAME
                    (
                        @OutputTableName,
                        N''''
                    ) +
                    N' AND s.name =''' +
                    @OutputSchemaName +
                    N''';',
                @StringToExecuteParams =
                    N'@r sysname OUTPUT';

            IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
            EXECUTE sys.sp_executesql
                @StringToExecute,
                @StringToExecuteParams,
                @r OUTPUT;

            IF @Debug = 1
            BEGIN
                RAISERROR('@r is set to: %s for schema name %s  and table name %s', 0, 1, @r, @OutputSchemaName, @OutputTableName) WITH NOWAIT;
            END;

            /*protection spells*/
            SELECT
                @ObjectFullName =
                    QUOTENAME(@OutputDatabaseName) +
                    N'.' +
                    QUOTENAME(@OutputSchemaName) +
                    N'.' +
                    QUOTENAME(@OutputTableName),
                @OutputDatabaseName =
                    QUOTENAME(@OutputDatabaseName),
                @OutputTableName =
                    QUOTENAME(@OutputTableName),
                @OutputSchemaName =
                    QUOTENAME(@OutputSchemaName);

            IF (@r IS NOT NULL) /*if it is not null, there is a table, so check for newly added columns*/
            BEGIN
                /* If the table doesn't have the new spid column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''spid'')
                        /*Add spid column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD spid smallint NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /* If the table doesn't have the new wait_resource column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''wait_resource'')
                        /*Add wait_resource column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD wait_resource nvarchar(MAX) NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /* If the table doesn't have the new client option column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''client_option_1'')
                        /*Add wait_resource column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD client_option_1 varchar(500) NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /* If the table doesn't have the new client option column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''client_option_2'')
                        /*Add wait_resource column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD client_option_2 varchar(500) NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /* If the table doesn't have the new lock mode column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''lock_mode'')
                        /*Add wait_resource column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD lock_mode nvarchar(256) NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /* If the table doesn't have the new status column, add it. See Github #3101. */
                SET @StringToExecute =
                        N'IF NOT EXISTS (SELECT 1/0 FROM ' +
                        @OutputDatabaseName +
                        N'.sys.all_columns AS o WHERE o.object_id = (OBJECT_ID(''' +
                        @ObjectFullName +
                        N''')) AND o.name = N''status'')
                        /*Add wait_resource column*/
                        ALTER TABLE ' +
                        @ObjectFullName +
                        N' ADD status nvarchar(256) NULL;';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;
            END;
            ELSE /* end if @r is not null. if it is null there is no table, create it from above execution */
            BEGIN
                SELECT
                    @StringToExecute =
                        N'USE ' +
                        @OutputDatabaseName +
                        N';
                        CREATE TABLE ' +
                        @OutputSchemaName +
                        N'.' +
                        @OutputTableName +
                        N'  (
                                ServerName nvarchar(256),
                                deadlock_type nvarchar(256),
                                event_date datetime,
                                database_name nvarchar(256),
                                spid smallint,
                                deadlock_group nvarchar(256),
                                query xml,
                                object_names xml,
                                isolation_level nvarchar(256),
                                owner_mode nvarchar(256),
                                waiter_mode nvarchar(256),
                                lock_mode nvarchar(256),
                                transaction_count bigint,
                                client_option_1 varchar(500),
                                client_option_2 varchar(500),
                                login_name nvarchar(256),
                                host_name nvarchar(256),
                                client_app nvarchar(1024),
                                wait_time bigint,
                                wait_resource nvarchar(max),
                                priority smallint,
                                log_used bigint,
                                last_tran_started datetime,
                                last_batch_started datetime,
                                last_batch_completed datetime,
                                transaction_name nvarchar(256),
                                status nvarchar(256),
                                owner_waiter_type nvarchar(256),
                                owner_activity nvarchar(256),
                                owner_waiter_activity nvarchar(256),
                                owner_merging nvarchar(256),
                                owner_spilling nvarchar(256),
                                owner_waiting_to_close nvarchar(256),
                                waiter_waiter_type nvarchar(256),
                                waiter_owner_activity nvarchar(256),
                                waiter_waiter_activity nvarchar(256),
                                waiter_merging nvarchar(256),
                                waiter_spilling nvarchar(256),
                                waiter_waiting_to_close nvarchar(256),
                                deadlock_graph xml
                            )';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute;

                /*table created.*/
                SELECT
                    @StringToExecute =
                        N'SELECT @r = o.name FROM ' +
                        @OutputDatabaseName +
                        N'.sys.objects AS o
                          WHERE o.type_desc = N''USER_TABLE''
                          AND o.name = N''BlitzLockFindings''',
                    @StringToExecuteParams =
                        N'@r sysname OUTPUT';

                IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                EXECUTE sys.sp_executesql
                    @StringToExecute,
                    @StringToExecuteParams,
                    @r OUTPUT;

                IF (@r IS NULL) /*if table does not exist*/
                BEGIN
                    SELECT
                        @OutputTableFindings =
                            QUOTENAME(N'BlitzLockFindings'),
                        @StringToExecute =
                            N'USE ' +
                            @OutputDatabaseName +
                            N';
                            CREATE TABLE ' +
                            @OutputSchemaName +
                            N'.' +
                            @OutputTableFindings +
                            N' (
                                   ServerName nvarchar(256),
                                   check_id INT,
                                   database_name nvarchar(256),
                                   object_name nvarchar(1000),
                                   finding_group nvarchar(100),
                                   finding nvarchar(4000)
                               );';

                    IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
                    EXECUTE sys.sp_executesql
                        @StringToExecute;
                END;
            END;

            /*create synonym for deadlockfindings.*/
            IF EXISTS
            (
                SELECT
                    1/0
                FROM sys.objects AS o
                WHERE o.name = N'DeadlockFindings'
                AND   o.type_desc = N'SYNONYM'
            )
            BEGIN
                RAISERROR('Found synonym DeadlockFindings, dropping', 0, 1) WITH NOWAIT;
                DROP SYNONYM dbo.DeadlockFindings;
            END;

            RAISERROR('Creating synonym DeadlockFindings', 0, 1) WITH NOWAIT;
            SET @StringToExecute =
                    N'CREATE SYNONYM dbo.DeadlockFindings FOR ' +
                    @OutputDatabaseName +
                    N'.' +
                    @OutputSchemaName +
                    N'.' +
                    @OutputTableFindings;

            IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
            EXECUTE sys.sp_executesql
                @StringToExecute;

            /*create synonym for deadlock table.*/
            IF EXISTS
            (
                SELECT
                    1/0
                FROM sys.objects AS o
                WHERE o.name = N'DeadLockTbl'
                AND   o.type_desc = N'SYNONYM'
            )
            BEGIN
                RAISERROR('Found synonym DeadLockTbl, dropping', 0, 1) WITH NOWAIT;
                DROP SYNONYM dbo.DeadLockTbl;
            END;

            RAISERROR('Creating synonym DeadLockTbl', 0, 1) WITH NOWAIT;
            SET @StringToExecute =
                    N'CREATE SYNONYM dbo.DeadLockTbl FOR ' +
                    @OutputDatabaseName +
                    N'.' +
                    @OutputSchemaName +
                    N'.' +
                    @OutputTableName;

            IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
            EXECUTE sys.sp_executesql
                @StringToExecute;
        END;
    END;

    /* WITH ROWCOUNT doesn't work on Amazon RDS - see: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2037 */
    IF @RDS = 0
    BEGIN;
        BEGIN TRY;
        RAISERROR('@RDS = 0, updating #t with high row and page counts', 0, 1) WITH NOWAIT;
            UPDATE STATISTICS
                #t
            WITH
                ROWCOUNT  = 9223372036854775807,
                PAGECOUNT = 9223372036854775807;
        END TRY
        BEGIN CATCH;
            /* Misleading error returned, if run without permissions to update statistics the error returned is "Cannot find object".
                    Catching specific error, and returning message with better info. If any other error is returned, then throw as normal */
            IF (ERROR_NUMBER() = 1088)
            BEGIN;
                SET @d = CONVERT(varchar(40), GETDATE(), 109);
                RAISERROR('Cannot run UPDATE STATISTICS on a #temp table without db_owner or sysadmin permissions', 0, 1) WITH NOWAIT;
            END;
            ELSE
            BEGIN;
                THROW;
            END;
        END CATCH;
    END;

    IF @DeadlockType IS NOT NULL
    BEGIN
        SELECT
            @DeadlockType =
                CASE
                    WHEN LOWER(@DeadlockType) LIKE 'regular%'
                    THEN N'Regular Deadlock'
                    WHEN LOWER(@DeadlockType) LIKE N'parallel%'
                    THEN N'Parallel Deadlock'
                    ELSE NULL
                END;
    END;

    /*If @TargetSessionType, we need to figure out if it's ring buffer or event file*/
    /*Azure has differently named views, so  we need to separate. Thanks, Azure.*/

    IF
    (
            @Azure = 0
        AND @TargetSessionType IS NULL
    )
    BEGIN
    RAISERROR('@TargetSessionType is NULL, assigning for non-Azure instance', 0, 1) WITH NOWAIT;

        SELECT TOP (1)
            @TargetSessionType = t.target_name
        FROM sys.dm_xe_sessions AS s
        JOIN sys.dm_xe_session_targets AS t
          ON s.address = t.event_session_address
        WHERE s.name = @EventSessionName
        AND   t.target_name IN (N'event_file', N'ring_buffer')
        ORDER BY t.target_name
        OPTION(RECOMPILE);

    RAISERROR('@TargetSessionType assigned as %s for non-Azure', 0, 1, @TargetSessionType) WITH NOWAIT;
    END;

    IF
    (
            @Azure = 1
        AND @TargetSessionType IS NULL
        AND LOWER(@TargetSessionType) <> N'table'
    )
    BEGIN
    RAISERROR('@TargetSessionType is NULL, assigning for Azure instance', 0, 1) WITH NOWAIT;

        SELECT TOP (1)
            @TargetSessionType = t.target_name
        FROM sys.dm_xe_database_sessions AS s
        JOIN sys.dm_xe_database_session_targets AS t
          ON s.address = t.event_session_address
        WHERE s.name = @EventSessionName
        AND   t.target_name IN (N'event_file', N'ring_buffer')
        ORDER BY t.target_name
        OPTION(RECOMPILE);

    RAISERROR('@TargetSessionType assigned as %s for Azure', 0, 1, @TargetSessionType) WITH NOWAIT;
    END;


    /*The system health stuff gets handled different from user extended events.*/
    /*These next sections deal with user events, dependent on target.*/

    /*If ring buffers*/
    IF
    (
           @TargetSessionType LIKE N'ring%'
       AND @EventSessionName NOT LIKE N'system_health%'
    )
    BEGIN
        IF @Azure = 0
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('@TargetSessionType is ring_buffer, inserting XML for non-Azure at %s', 0, 1, @d) WITH NOWAIT;

            INSERT
                #x WITH(TABLOCKX)
            (
                x
            )
            SELECT
                x = TRY_CAST(t.target_data AS xml)
            FROM sys.dm_xe_session_targets AS t
            JOIN sys.dm_xe_sessions AS s
              ON s.address = t.event_session_address
            WHERE s.name = @EventSessionName
            AND   t.target_name = N'ring_buffer'
            OPTION(RECOMPILE);

            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
        END;

        IF @Azure = 1
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('@TargetSessionType is ring_buffer, inserting XML for Azure at %s', 0, 1, @d) WITH NOWAIT;

            INSERT
                #x WITH(TABLOCKX)
            (
                x
            )
            SELECT
                x = TRY_CAST(t.target_data AS xml)
            FROM sys.dm_xe_database_session_targets AS t
            JOIN sys.dm_xe_database_sessions AS s
              ON s.address = t.event_session_address
            WHERE s.name = @EventSessionName
            AND   t.target_name = N'ring_buffer'
            OPTION(RECOMPILE);

            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
        END;
    END;


    /*If event file*/
    IF
    (
           @TargetSessionType LIKE N'event%'
       AND @EventSessionName NOT LIKE N'system_health%'
    )
    BEGIN
        IF @Azure = 0
        BEGIN
            RAISERROR('@TargetSessionType is event_file, assigning XML for non-Azure', 0, 1) WITH NOWAIT;

            SELECT
                @SessionId = t.event_session_id,
                @TargetSessionId = t.target_id
            FROM sys.server_event_session_targets AS t
            JOIN sys.server_event_sessions AS s
              ON s.event_session_id = t.event_session_id
            WHERE t.name = @TargetSessionType
            AND   s.name = @EventSessionName
            OPTION(RECOMPILE);

            /*We get the file name automatically, here*/
            RAISERROR('Assigning @FileName...', 0, 1) WITH NOWAIT;
            SELECT
                @FileName =
                    CASE
                        WHEN f.file_name LIKE N'%.xel'
                        THEN REPLACE(f.file_name, N'.xel', N'*.xel')
                        ELSE f.file_name + N'*.xel'
                    END
            FROM
            (
                SELECT
                    file_name =
                        CONVERT(nvarchar(4000), f.value)
                FROM sys.server_event_session_fields AS f
                WHERE f.event_session_id = @SessionId
                AND   f.object_id = @TargetSessionId
                AND   f.name = N'filename'
            ) AS f
            OPTION(RECOMPILE);
        END;

        IF @Azure = 1
        BEGIN
            RAISERROR('@TargetSessionType is event_file, assigning XML for Azure', 0, 1) WITH NOWAIT;
            SELECT
                @SessionId =
                    t.event_session_address,
                @TargetSessionId =
                    t.target_name
            FROM sys.dm_xe_database_session_targets t
            JOIN sys.dm_xe_database_sessions s
              ON s.address = t.event_session_address
            WHERE t.target_name = @TargetSessionType
            AND   s.name = @EventSessionName
            OPTION(RECOMPILE);

            /*We get the file name automatically, here*/
            RAISERROR('Assigning @FileName...', 0, 1) WITH NOWAIT;
            SELECT
                @FileName =
                    CASE
                        WHEN f.file_name LIKE N'%.xel'
                        THEN REPLACE(f.file_name, N'.xel', N'*.xel')
                        ELSE f.file_name + N'*.xel'
                    END
            FROM
            (
                SELECT
                    file_name =
                        CONVERT(nvarchar(4000), f.value)
                FROM sys.server_event_session_fields AS f
                WHERE f.event_session_id = @SessionId
                AND   f.object_id = @TargetSessionId
                AND   f.name = N'filename'
            ) AS f
            OPTION(RECOMPILE);
        END;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Reading for event_file %s', 0, 1, @FileName) WITH NOWAIT;

        INSERT
            #x WITH(TABLOCKX)
        (
            x
        )
        SELECT
            x = TRY_CAST(f.event_data AS xml)
        FROM sys.fn_xe_file_target_read_file(@FileName, NULL, NULL, NULL) AS f
        LEFT JOIN #t AS t
          ON 1 = 1
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
    END;

    /*The XML is parsed differently if it comes from the event file or ring buffer*/

    /*If ring buffers*/
    IF
    (
           @TargetSessionType LIKE N'ring%'
       AND @EventSessionName NOT LIKE N'system_health%'
    )
    BEGIN
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Inserting to #deadlock_data for ring buffer data', 0, 1) WITH NOWAIT;

        INSERT
            #deadlock_data WITH(TABLOCKX)
        (
            deadlock_xml
        )
        SELECT
            deadlock_xml =
                e.x.query(N'.')
        FROM #x AS x
        LEFT JOIN #t AS t
          ON 1 = 1
        CROSS APPLY x.x.nodes('/RingBufferTarget/event') AS e(x)
        WHERE
          (
              e.x.exist('@name[ .= "xml_deadlock_report"]') = 1
           OR e.x.exist('@name[ .= "database_xml_deadlock_report"]') = 1
           OR e.x.exist('@name[ .= "xml_deadlock_report_filtered"]') = 1
          )
        AND   e.x.exist('@timestamp[. >= sql:variable("@StartDate") and .< sql:variable("@EndDate")]') = 1
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
    END;

    /*If event file*/
    IF
    (
           @TargetSessionType LIKE N'event_file%'
       AND @EventSessionName NOT LIKE N'system_health%'
    )
    BEGIN
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Inserting to #deadlock_data for event file data', 0, 1) WITH NOWAIT;

        IF @Debug = 1 BEGIN SET STATISTICS XML ON; END;

        INSERT
            #deadlock_data WITH(TABLOCKX)
        (
            deadlock_xml
        )
        SELECT
            deadlock_xml =
                e.x.query('.')
        FROM #x AS x
        LEFT JOIN #t AS t
          ON 1 = 1
        CROSS APPLY x.x.nodes('/event') AS e(x)
        WHERE
          (
              e.x.exist('@name[ .= "xml_deadlock_report"]') = 1
           OR e.x.exist('@name[ .= "database_xml_deadlock_report"]') = 1
           OR e.x.exist('@name[ .= "xml_deadlock_report_filtered"]') = 1
          )
        AND   e.x.exist('@timestamp[. >= sql:variable("@StartDate") and .< sql:variable("@EndDate")]') = 1
        OPTION(RECOMPILE);

        IF @Debug = 1 BEGIN SET STATISTICS XML OFF; END;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
    END;

    /*This section deals with event file*/
    IF
    (
           @TargetSessionType LIKE N'event%'
       AND @EventSessionName LIKE N'system_health%'
    )
    BEGIN
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Grab the initial set of xml to parse at %s', 0, 1, @d) WITH NOWAIT;

        IF @Debug = 1 BEGIN SET STATISTICS XML ON; END;

        SELECT
            xml.deadlock_xml
        INTO #xml
        FROM
        (
            SELECT
                deadlock_xml =
                    TRY_CAST(fx.event_data AS xml)
            FROM sys.fn_xe_file_target_read_file(N'system_health*.xel', NULL, NULL, NULL) AS fx
            LEFT JOIN #t AS t
              ON 1 = 1
            WHERE fx.object_name = N'xml_deadlock_report'
        ) AS xml
        CROSS APPLY xml.deadlock_xml.nodes('/event') AS e(x)
        WHERE 1 = 1
        AND   e.x.exist('@timestamp[. >= sql:variable("@StartDate") and .< sql:variable("@EndDate")]') = 1
        OPTION(RECOMPILE);

        INSERT
            #deadlock_data WITH(TABLOCKX)
        SELECT
            deadlock_xml =
                xml.deadlock_xml
        FROM #xml AS xml
        LEFT JOIN #t AS t
          ON 1 = 1
        WHERE xml.deadlock_xml IS NOT NULL
        OPTION(RECOMPILE);

        IF @Debug = 1 BEGIN SET STATISTICS XML OFF; END;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
    END;

    /* If table target */
    IF LOWER(@TargetSessionType) = N'table'
    BEGIN
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Inserting to #deadlock_data from table source %s', 0, 1, @d) WITH NOWAIT;
   
        /*
        First, we need to heck the XML structure.
        Depending on the data source, the XML could
        contain either the /event or /deadlock nodes.
        When the /event nodes are not present, there
        is no @name attribute to evaluate.
        */

        SELECT
            @extract_sql = N'
        SELECT TOP (1)
            @xe = xe.e.exist(''.''),
            @xd = xd.e.exist(''.'')
        FROM ' +
        QUOTENAME(@TargetDatabaseName) +
        N'.' +
        QUOTENAME(@TargetSchemaName) +
        N'.' +
        QUOTENAME(@TargetTableName) +
        N' AS x
        OUTER APPLY x.' +
        QUOTENAME(@TargetColumnName) +
        N'.nodes(''/event'') AS xe(e)
        OUTER APPLY x.' +
        QUOTENAME(@TargetColumnName) +
        N'.nodes(''/deadlock'') AS xd(e) 
        OPTION(RECOMPILE);
        ';

        IF @Debug = 1 BEGIN PRINT @extract_sql; END;

        EXECUTE sys.sp_executesql
            @extract_sql,
          N'
            @xe bit OUTPUT,
            @xd bit OUTPUT
           ',
           @xe OUTPUT,
           @xd OUTPUT;


        /* Build dynamic SQL to extract the XML  */  
        IF  @xe = 1
        AND @xd IS NULL
        BEGIN
            SET @extract_sql = N'
        SELECT
            deadlock_xml = ' +
            QUOTENAME(@TargetColumnName) +
            N'
        FROM ' +
        QUOTENAME(@TargetDatabaseName) +
        N'.' +
        QUOTENAME(@TargetSchemaName) +
        N'.' +
        QUOTENAME(@TargetTableName) +
        N' AS x
        LEFT JOIN #t AS t
          ON 1 = 1
        CROSS APPLY x.' +
        QUOTENAME(@TargetColumnName) +
        N'.nodes(''/event'') AS e(x)
        WHERE
          (
              e.x.exist(''@name[ .= "xml_deadlock_report"]'') = 1
           OR e.x.exist(''@name[ .= "database_xml_deadlock_report"]'') = 1
           OR e.x.exist(''@name[ .= "xml_deadlock_report_filtered"]'') = 1
          )';
        END;

        IF  @xe IS NULL
        AND @xd = 1
        BEGIN
            SET @extract_sql = N'
        SELECT
            deadlock_xml = ' +
            QUOTENAME(@TargetColumnName) +
            N'
        FROM ' +
        QUOTENAME(@TargetDatabaseName) +
        N'.' +
        QUOTENAME(@TargetSchemaName) +
        N'.' +
        QUOTENAME(@TargetTableName) +
        N' AS x
        LEFT JOIN #t AS t
          ON 1 = 1
        CROSS APPLY x.' +
        QUOTENAME(@TargetColumnName) +
        N'.nodes(''/deadlock'') AS e(x)
        WHERE 1 = 1';
        END;
       
        /* Add timestamp filtering if specified */
        IF @TargetTimestampColumnName IS NOT NULL
        BEGIN
            SET @extract_sql = @extract_sql + N'
        AND x.' + QUOTENAME(@TargetTimestampColumnName) + N' >= @StartDate
        AND x.' + QUOTENAME(@TargetTimestampColumnName) + N'  < @EndDate';
        END;
                   
        /* If no timestamp column but date filtering is needed, handle XML-based filtering when possible */
        IF  @TargetTimestampColumnName IS NULL
        AND @xe = 1
        AND @xd IS NULL
            BEGIN
                SET @extract_sql = @extract_sql + N'
        AND e.x.exist(''@timestamp[. >= sql:variable("@StartDate") and . < sql:variable("@EndDate")]'') = 1';
        END;

		/*Woof*/
		IF  @TargetTimestampColumnName IS NULL
        AND @xe IS NULL
        AND @xd = 1
            BEGIN
                SET @extract_sql = @extract_sql + N'
        AND e.x.exist(''(/deadlock/process-list/process/@lasttranstarted)[. >= sql:variable("@StartDate") and . < sql:variable("@EndDate")]'') = 1';
        END;

        SET @extract_sql += N'
        OPTION(RECOMPILE);
        ';
       
        IF @Debug = 1 BEGIN PRINT @extract_sql; END;
       
        /* Execute the dynamic SQL */
        INSERT
            #deadlock_data
        WITH
            (TABLOCKX)
        (
            deadlock_xml
        )
        EXECUTE sys.sp_executesql
            @extract_sql,
          N'
            @StartDate datetime,
            @EndDate datetime
           ',
            @StartDate,
            @EndDate;
       
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
    END;

        /*Parse process and input buffer xml*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Initial Parse process and input buffer xml %s', 0, 1, @d) WITH NOWAIT;

        SELECT
            d1.deadlock_xml,
            event_date = d1.deadlock_xml.value('(event/@timestamp)[1]', 'datetime2'),
            victim_id = d1.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'nvarchar(256)'),
            is_parallel = d1.deadlock_xml.exist('//deadlock/resource-list/exchangeEvent'),
            is_parallel_batch = d1.deadlock_xml.exist('//deadlock/resource-list/SyncPoint'),
            deadlock_graph = d1.deadlock_xml.query('/event/data/value/deadlock')
        INTO #dd
        FROM #deadlock_data AS d1
        LEFT JOIN #t AS t
          ON 1 = 1
		WHERE @xe = 1
		OR    LOWER(@TargetSessionType) <> N'table'

		UNION ALL

        SELECT
            d1.deadlock_xml,
            event_date = d1.deadlock_xml.value('(/deadlock/process-list/process/@lasttranstarted)[1]', 'datetime2'),
            victim_id = d1.deadlock_xml.value('(/deadlock/victim-list/victimProcess/@id)[1]', 'nvarchar(256)'),
            is_parallel = d1.deadlock_xml.exist('/deadlock/resource-list/exchangeEvent'),
            is_parallel_batch = d1.deadlock_xml.exist('/deadlock/resource-list/SyncPoint'),
            deadlock_graph = d1.deadlock_xml.query('.')
        FROM #deadlock_data AS d1
        LEFT JOIN #t AS t
          ON 1 = 1
		WHERE @xd = 1
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Final Parse process and input buffer xml %s', 0, 1, @d) WITH NOWAIT;

        SELECT
            q.event_date,
            q.victim_id,
            is_parallel =
                CONVERT(bit, q.is_parallel),
            q.deadlock_graph,
            q.id,
            q.spid,
            q.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(q.database_id),
                    N'UNKNOWN'
                ),
            q.current_database_name,
            q.priority,
            q.log_used,
            q.wait_resource,
            q.wait_time,
            q.transaction_name,
            q.last_tran_started,
            q.last_batch_started,
            q.last_batch_completed,
            q.lock_mode,
            q.status,
            q.transaction_count,
            q.client_app,
            q.host_name,
            q.login_name,
            q.isolation_level,
            client_option_1 =
                SUBSTRING
                (
                    CASE WHEN q.clientoption1 & 1 = 1 THEN ', DISABLE_DEF_CNST_CHECK' ELSE '' END +
                    CASE WHEN q.clientoption1 & 2 = 2 THEN ', IMPLICIT_TRANSACTIONS' ELSE '' END +
                    CASE WHEN q.clientoption1 & 4 = 4 THEN ', CURSOR_CLOSE_ON_COMMIT' ELSE '' END +
                    CASE WHEN q.clientoption1 & 8 = 8 THEN ', ANSI_WARNINGS' ELSE '' END +
                    CASE WHEN q.clientoption1 & 16 = 16 THEN ', ANSI_PADDING' ELSE '' END +
                    CASE WHEN q.clientoption1 & 32 = 32 THEN ', ANSI_NULLS' ELSE '' END +
                    CASE WHEN q.clientoption1 & 64 = 64 THEN ', ARITHABORT' ELSE '' END +
                    CASE WHEN q.clientoption1 & 128 = 128 THEN ', ARITHIGNORE' ELSE '' END +
                    CASE WHEN q.clientoption1 & 256 = 256 THEN ', QUOTED_IDENTIFIER' ELSE '' END +
                    CASE WHEN q.clientoption1 & 512 = 512 THEN ', NOCOUNT' ELSE '' END +
                    CASE WHEN q.clientoption1 & 1024 = 1024 THEN ', ANSI_NULL_DFLT_ON' ELSE '' END +
                    CASE WHEN q.clientoption1 & 2048 = 2048 THEN ', ANSI_NULL_DFLT_OFF' ELSE '' END +
                    CASE WHEN q.clientoption1 & 4096 = 4096 THEN ', CONCAT_NULL_YIELDS_NULL' ELSE '' END +
                    CASE WHEN q.clientoption1 & 8192 = 8192 THEN ', NUMERIC_ROUNDABORT' ELSE '' END +
                    CASE WHEN q.clientoption1 & 16384 = 16384 THEN ', XACT_ABORT' ELSE '' END,
                    3,
                    500
                ),
            client_option_2 =
                SUBSTRING
                (
                    CASE WHEN q.clientoption2 & 1024 = 1024 THEN ', DB CHAINING' ELSE '' END +
                    CASE WHEN q.clientoption2 & 2048 = 2048 THEN ', NUMERIC ROUNDABORT' ELSE '' END +
                    CASE WHEN q.clientoption2 & 4096 = 4096 THEN ', ARITHABORT' ELSE '' END +
                    CASE WHEN q.clientoption2 & 8192 = 8192 THEN ', ANSI PADDING' ELSE '' END +
                    CASE WHEN q.clientoption2 & 16384 = 16384 THEN ', ANSI NULL DEFAULT' ELSE '' END +
                    CASE WHEN q.clientoption2 & 65536 = 65536 THEN ', CONCAT NULL YIELDS NULL' ELSE '' END +
                    CASE WHEN q.clientoption2 & 131072 = 131072 THEN ', RECURSIVE TRIGGERS' ELSE '' END +
                    CASE WHEN q.clientoption2 & 1048576 = 1048576 THEN ', DEFAULT TO LOCAL CURSOR' ELSE '' END +
                    CASE WHEN q.clientoption2 & 8388608 = 8388608 THEN ', QUOTED IDENTIFIER' ELSE '' END +
                    CASE WHEN q.clientoption2 & 16777216 = 16777216 THEN ', AUTO CREATE STATISTICS' ELSE '' END +
                    CASE WHEN q.clientoption2 & 33554432 = 33554432 THEN ', CURSOR CLOSE ON COMMIT' ELSE '' END +
                    CASE WHEN q.clientoption2 & 67108864 = 67108864 THEN ', ANSI NULLS' ELSE '' END +
                    CASE WHEN q.clientoption2 & 268435456 = 268435456 THEN ', ANSI WARNINGS' ELSE '' END +
                    CASE WHEN q.clientoption2 & 536870912 = 536870912 THEN ', FULL TEXT ENABLED' ELSE '' END +
                    CASE WHEN q.clientoption2 & 1073741824 = 1073741824 THEN ', AUTO UPDATE STATISTICS' ELSE '' END +
                    CASE WHEN q.clientoption2 & 1469283328 = 1469283328 THEN ', ALL SETTABLE OPTIONS' ELSE '' END,
                    3,
                    500
                ),
            q.process_xml
        INTO #deadlock_process
        FROM
        (
            SELECT
                dd.deadlock_xml,
                event_date =
                    DATEADD
                    (
                        MINUTE,
                        DATEDIFF
                        (
                            MINUTE,
                            GETUTCDATE(),
                            SYSDATETIME()
                        ),
                        dd.event_date
                    ),
                dd.victim_id,
                is_parallel =
                    CONVERT(tinyint, dd.is_parallel) +
                    CONVERT(tinyint, dd.is_parallel_batch),
                dd.deadlock_graph,
                id = ca.dp.value('@id', 'nvarchar(256)'),
                spid = ca.dp.value('@spid', 'smallint'),
                database_id = ca.dp.value('@currentdb', 'bigint'),
                current_database_name = ca.dp.value('@currentdbname', 'nvarchar(256)'),
                priority = ca.dp.value('@priority', 'smallint'),
                log_used = ca.dp.value('@logused', 'bigint'),
                wait_resource = ca.dp.value('@waitresource', 'nvarchar(256)'),
                wait_time = ca.dp.value('@waittime', 'bigint'),
                transaction_name = ca.dp.value('@transactionname', 'nvarchar(256)'),
                last_tran_started = ca.dp.value('@lasttranstarted', 'datetime'),
                last_batch_started = ca.dp.value('@lastbatchstarted', 'datetime'),
                last_batch_completed = ca.dp.value('@lastbatchcompleted', 'datetime'),
                lock_mode = ca.dp.value('@lockMode', 'nvarchar(256)'),
                status = ca.dp.value('@status', 'nvarchar(256)'),
                transaction_count = ca.dp.value('@trancount', 'bigint'),
                client_app = ca.dp.value('@clientapp', 'nvarchar(1024)'),
                host_name = ca.dp.value('@hostname', 'nvarchar(256)'),
                login_name = ca.dp.value('@loginname', 'nvarchar(256)'),
                isolation_level = ca.dp.value('@isolationlevel', 'nvarchar(256)'),
                clientoption1 = ca.dp.value('@clientoption1', 'bigint'),
                clientoption2 = ca.dp.value('@clientoption2', 'bigint'),
                process_xml = ISNULL(ca.dp.query(N'.'), N'')
            FROM #dd AS dd
            CROSS APPLY dd.deadlock_xml.nodes('//deadlock/process-list/process') AS ca(dp)
            WHERE (ca.dp.exist('@currentdb[. = sql:variable("@DatabaseId")]') = 1 OR @DatabaseName IS NULL)
            AND   (ca.dp.exist('@clientapp[. = sql:variable("@AppName")]') = 1 OR @AppName IS NULL)
            AND   (ca.dp.exist('@hostname[. = sql:variable("@HostName")]') = 1 OR @HostName IS NULL)
            AND   (ca.dp.exist('@loginname[. = sql:variable("@LoginName")]') = 1 OR @LoginName IS NULL)
        ) AS q
        LEFT JOIN #t AS t
          ON 1 = 1
        OPTION(RECOMPILE);


        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse execution stack xml*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse execution stack xml %s', 0, 1, @d) WITH NOWAIT;

        SELECT DISTINCT
            dp.id,
            dp.event_date,
            proc_name = ca.dp.value('@procname', 'nvarchar(1024)'),
            sql_handle = ca.dp.value('@sqlhandle', 'nvarchar(131)')
        INTO #deadlock_stack
        FROM #deadlock_process AS dp
        CROSS APPLY dp.process_xml.nodes('//executionStack/frame') AS ca(dp)
        WHERE (ca.dp.exist('@procname[. = sql:variable("@StoredProcName")]') = 1 OR @StoredProcName IS NULL)
        AND    ca.dp.exist('@sqlhandle[ .= "0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"]') = 0
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Grab the full resource list*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);

        RAISERROR('Grab the full resource list %s', 0, 1, @d) WITH NOWAIT;

        SELECT
            event_date =
                DATEADD
                (
                    MINUTE,
                    DATEDIFF
                    (
                        MINUTE,
                        GETUTCDATE(),
                        SYSDATETIME()
                    ),
                    dr.event_date
                ),
            dr.victim_id,
            dr.resource_xml
        INTO
            #deadlock_resource
        FROM
        (
            SELECT
                event_date = dd.deadlock_xml.value('(event/@timestamp)[1]', 'datetime2'),
                victim_id = dd.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'nvarchar(256)'),
                resource_xml = ISNULL(ca.dp.query(N'.'), N'')
            FROM #deadlock_data AS dd
            CROSS APPLY dd.deadlock_xml.nodes('//deadlock/resource-list') AS ca(dp)
        ) AS dr
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse object locks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse object locks %s', 0, 1, @d) WITH NOWAIT;

        SELECT DISTINCT
            ca.event_date,
            ca.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(ca.database_id),
                    N'UNKNOWN'
                ),
            object_name =
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    ca.object_name COLLATE Latin1_General_BIN2,
                NCHAR(31), N'?'), NCHAR(30), N'?'), NCHAR(29), N'?'), NCHAR(28), N'?'), NCHAR(27), N'?'),
                NCHAR(26), N'?'), NCHAR(25), N'?'), NCHAR(24), N'?'), NCHAR(23), N'?'), NCHAR(22), N'?'),
                NCHAR(21), N'?'), NCHAR(20), N'?'), NCHAR(19), N'?'), NCHAR(18), N'?'), NCHAR(17), N'?'),
                NCHAR(16), N'?'), NCHAR(15), N'?'), NCHAR(14), N'?'), NCHAR(12), N'?'), NCHAR(11), N'?'),
                NCHAR(8),  N'?'), NCHAR(7), N'?'),  NCHAR(6), N'?'),  NCHAR(5), N'?'),  NCHAR(4), N'?'),
                NCHAR(3),  N'?'), NCHAR(2), N'?'),  NCHAR(1), N'?'),  NCHAR(0), N'?'),
            ca.lock_mode,
            ca.index_name,
            ca.associatedObjectId,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            waiter_mode = w.l.value('@mode', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)'),
            owner_mode = o.l.value('@mode', 'nvarchar(256)'),
            lock_type = CAST(N'OBJECT' AS nvarchar(100))
        INTO #deadlock_owner_waiter
        FROM
        (
            SELECT
                dr.event_date,
                database_id = ca.dr.value('@dbid', 'bigint'),
                object_name = ca.dr.value('@objectname', 'nvarchar(1024)'),
                lock_mode = ca.dr.value('@mode', 'nvarchar(256)'),
                index_name = ca.dr.value('@indexname', 'nvarchar(256)'),
                associatedObjectId = ca.dr.value('@associatedObjectId', 'bigint'),
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/objectlock') AS ca(dr)
            WHERE (ca.dr.exist('@objectname[. = sql:variable("@ObjectName")]') = 1 OR @ObjectName IS NULL)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse page locks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse page locks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT DISTINCT
            ca.event_date,
            ca.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(ca.database_id),
                    N'UNKNOWN'
                ),
            ca.object_name,
            ca.lock_mode,
            ca.index_name,
            ca.associatedObjectId,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            waiter_mode = w.l.value('@mode', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)'),
            owner_mode = o.l.value('@mode', 'nvarchar(256)'),
            lock_type = N'PAGE'
        FROM
        (
            SELECT
                dr.event_date,
                database_id = ca.dr.value('@dbid', 'bigint'),
                object_name = ca.dr.value('@objectname', 'nvarchar(1024)'),
                lock_mode = ca.dr.value('@mode', 'nvarchar(256)'),
                index_name = ca.dr.value('@indexname', 'nvarchar(256)'),
                associatedObjectId = ca.dr.value('@associatedObjectId', 'bigint'),
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/pagelock') AS ca(dr)
            WHERE (ca.dr.exist('@objectname[. = sql:variable("@ObjectName")]') = 1 OR @ObjectName IS NULL)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse key locks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse key locks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT DISTINCT
            ca.event_date,
            ca.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(ca.database_id),
                    N'UNKNOWN'
                ),
            ca.object_name,
            ca.lock_mode,
            ca.index_name,
            ca.associatedObjectId,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            waiter_mode = w.l.value('@mode', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)'),
            owner_mode = o.l.value('@mode', 'nvarchar(256)'),
            lock_type = N'KEY'
        FROM
        (
            SELECT
                dr.event_date,
                database_id = ca.dr.value('@dbid', 'bigint'),
                object_name = ca.dr.value('@objectname', 'nvarchar(1024)'),
                lock_mode = ca.dr.value('@mode', 'nvarchar(256)'),
                index_name = ca.dr.value('@indexname', 'nvarchar(256)'),
                associatedObjectId = ca.dr.value('@associatedObjectId', 'bigint'),
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/keylock') AS ca(dr)
            WHERE (ca.dr.exist('@objectname[. = sql:variable("@ObjectName")]') = 1 OR @ObjectName IS NULL)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse RID locks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse RID locks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT DISTINCT
            ca.event_date,
            ca.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(ca.database_id),
                    N'UNKNOWN'
                ),
            ca.object_name,
            ca.lock_mode,
            ca.index_name,
            ca.associatedObjectId,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            waiter_mode = w.l.value('@mode', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)'),
            owner_mode = o.l.value('@mode', 'nvarchar(256)'),
            lock_type = N'RID'
        FROM
        (
            SELECT
                dr.event_date,
                database_id = ca.dr.value('@dbid', 'bigint'),
                object_name = ca.dr.value('@objectname', 'nvarchar(1024)'),
                lock_mode = ca.dr.value('@mode', 'nvarchar(256)'),
                index_name = ca.dr.value('@indexname', 'nvarchar(256)'),
                associatedObjectId = ca.dr.value('@associatedObjectId', 'bigint'),
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/ridlock') AS ca(dr)
            WHERE (ca.dr.exist('@objectname[. = sql:variable("@ObjectName")]') = 1 OR @ObjectName IS NULL)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse row group locks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse row group locks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT DISTINCT
            ca.event_date,
            ca.database_id,
            database_name =
                ISNULL
                (
                    DB_NAME(ca.database_id),
                    N'UNKNOWN'
                ),
            ca.object_name,
            ca.lock_mode,
            ca.index_name,
            ca.associatedObjectId,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            waiter_mode = w.l.value('@mode', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)'),
            owner_mode = o.l.value('@mode', 'nvarchar(256)'),
            lock_type = N'ROWGROUP'
        FROM
        (
            SELECT
                dr.event_date,
                database_id = ca.dr.value('@dbid', 'bigint'),
                object_name = ca.dr.value('@objectname', 'nvarchar(1024)'),
                lock_mode = ca.dr.value('@mode', 'nvarchar(256)'),
                index_name = ca.dr.value('@indexname', 'nvarchar(256)'),
                associatedObjectId = ca.dr.value('@associatedObjectId', 'bigint'),
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/rowgrouplock') AS ca(dr)
            WHERE (ca.dr.exist('@objectname[. = sql:variable("@ObjectName")]') = 1 OR @ObjectName IS NULL)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Fixing heaps in #deadlock_owner_waiter %s', 0, 1, @d) WITH NOWAIT;

        UPDATE
            d
        SET
            d.index_name =
                d.object_name + N'.HEAP'
        FROM #deadlock_owner_waiter AS d
        WHERE d.lock_type IN
              (
                  N'HEAP',
                  N'RID'
              )
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Parse parallel deadlocks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Parse parallel deadlocks %s', 0, 1, @d) WITH NOWAIT;

        SELECT DISTINCT
            ca.id,
            ca.event_date,
            ca.wait_type,
            ca.node_id,
            ca.waiter_type,
            ca.owner_activity,
            ca.waiter_activity,
            ca.merging,
            ca.spilling,
            ca.waiting_to_close,
            waiter_id = w.l.value('@id', 'nvarchar(256)'),
            owner_id = o.l.value('@id', 'nvarchar(256)')
        INTO #deadlock_resource_parallel
        FROM
        (
            SELECT
                dr.event_date,
                id = ca.dr.value('@id', 'nvarchar(256)'),
                wait_type = ca.dr.value('@WaitType', 'nvarchar(256)'),
                node_id = ca.dr.value('@nodeId', 'bigint'),
                /* These columns are in 2017 CU5+ ONLY */
                waiter_type = ca.dr.value('@waiterType', 'nvarchar(256)'),
                owner_activity = ca.dr.value('@ownerActivity', 'nvarchar(256)'),
                waiter_activity = ca.dr.value('@waiterActivity', 'nvarchar(256)'),
                merging = ca.dr.value('@merging', 'nvarchar(256)'),
                spilling = ca.dr.value('@spilling', 'nvarchar(256)'),
                waiting_to_close = ca.dr.value('@waitingToClose', 'nvarchar(256)'),
                /* These columns are in 2017 CU5+ ONLY */
                dr = ca.dr.query('.')
            FROM #deadlock_resource AS dr
            CROSS APPLY dr.resource_xml.nodes('//resource-list/exchangeEvent') AS ca(dr)
        ) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Get rid of parallel noise*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Get rid of parallel noise %s', 0, 1, @d) WITH NOWAIT;

        WITH
            c AS
        (
            SELECT
                *,
                rn =
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY
                            drp.owner_id,
                            drp.waiter_id
                        ORDER BY
                            drp.event_date
                    )
            FROM #deadlock_resource_parallel AS drp
        )
        DELETE
        FROM c
        WHERE c.rn > 1
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Get rid of nonsense*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Get rid of nonsense %s', 0, 1, @d) WITH NOWAIT;

        DELETE dow
        FROM #deadlock_owner_waiter AS dow
        WHERE dow.owner_id = dow.waiter_id
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Add some nonsense*/
        ALTER TABLE
            #deadlock_process
        ADD
            waiter_mode nvarchar(256),
            owner_mode nvarchar(256),
            is_victim AS
                CONVERT
                (
                    bit,
                    CASE
                        WHEN id = victim_id
                        THEN 1
                        ELSE 0
                    END
                ) PERSISTED;

        /*Update some nonsense*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Update some nonsense part 1 %s', 0, 1, @d) WITH NOWAIT;

        UPDATE
            dp
        SET
            dp.owner_mode = dow.owner_mode
        FROM #deadlock_process AS dp
        JOIN #deadlock_owner_waiter AS dow
          ON  dp.id = dow.owner_id
          AND dp.event_date = dow.event_date
        WHERE dp.is_victim = 0
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Update some nonsense part 2 %s', 0, 1, @d) WITH NOWAIT;

        UPDATE
            dp
        SET
            dp.waiter_mode = dow.waiter_mode
        FROM #deadlock_process AS dp
        JOIN #deadlock_owner_waiter AS dow
          ON  dp.victim_id = dow.waiter_id
          AND dp.event_date = dow.event_date
        WHERE dp.is_victim = 1
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Get Agent Job and Step names*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Get Agent Job and Step names %s', 0, 1, @d) WITH NOWAIT;

        SELECT
            x.event_date,
            x.victim_id,
            x.id,
            x.database_id,
            x.client_app,
            x.job_id,
            x.step_id,
            job_id_guid =
                CONVERT
                (
                    uniqueidentifier,
                    TRY_CAST
                    (
                        N''
                        AS xml
                    ).value('xs:hexBinary(substring(sql:column("x.job_id"), 0))', 'binary(16)')
                )
        INTO #agent_job
        FROM
        (
            SELECT
                dp.event_date,
                dp.victim_id,
                dp.id,
                dp.database_id,
                dp.client_app,
                job_id =
                    SUBSTRING
                    (
                        dp.client_app,
                        CHARINDEX(N'0x', dp.client_app) + LEN(N'0x'),
                        32
                    ),
                step_id =
                    CASE
                        WHEN CHARINDEX(N': Step ', dp.client_app) > 0
                        AND  CHARINDEX(N')', dp.client_app, CHARINDEX(N': Step ', dp.client_app)) > 0
                        THEN
                            SUBSTRING
                            (
                                dp.client_app,
                                CHARINDEX(N': Step ', dp.client_app) + LEN(N': Step '),
                                CHARINDEX(N')', dp.client_app, CHARINDEX(N': Step ', dp.client_app)) -
                                  (CHARINDEX(N': Step ', dp.client_app) + LEN(N': Step '))
                            )
                        ELSE dp.client_app
                    END
            FROM #deadlock_process AS dp
            WHERE dp.client_app LIKE N'SQLAgent - %'
            AND   dp.client_app <> N'SQLAgent - Initial Boot Probe'
        ) AS x
        OPTION(RECOMPILE);

        ALTER TABLE
            #agent_job
        ADD
            job_name nvarchar(256),
            step_name nvarchar(256);

        IF
        (
                @Azure = 0
            AND @RDS = 0
        )
        BEGIN
            SET @StringToExecute =
                N'
    UPDATE
        aj
    SET
        aj.job_name = j.name,
        aj.step_name = s.step_name
    FROM msdb.dbo.sysjobs AS j
    JOIN msdb.dbo.sysjobsteps AS s
      ON j.job_id = s.job_id
    JOIN #agent_job AS aj
      ON  aj.job_id_guid = j.job_id
      AND aj.step_id = s.step_id
    OPTION(RECOMPILE);
                 ';

            IF @Debug = 1 BEGIN PRINT @StringToExecute; END;
            EXECUTE sys.sp_executesql
                @StringToExecute;

        END;

        UPDATE
            dp
        SET
            dp.client_app =
                CASE
                    WHEN dp.client_app LIKE N'SQLAgent - %'
                    THEN N'SQLAgent - Job: ' +
                         aj.job_name +
                         N' Step: ' +
                         aj.step_name
                    ELSE dp.client_app
                END
        FROM #deadlock_process AS dp
        JOIN #agent_job AS aj
          ON  dp.event_date = aj.event_date
          AND dp.victim_id = aj.victim_id
          AND dp.id = aj.id
        OPTION(RECOMPILE);

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Get each and every table of all databases*/
        IF @Azure = 0
        BEGIN
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Inserting to @sysAssObjId %s', 0, 1, @d) WITH NOWAIT;

        INSERT INTO
            @sysAssObjId
        (
            database_id,
            partition_id,
            schema_name,
            table_name
        )
        EXECUTE sys.sp_MSforeachdb
            N'
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

            USE [?];

            IF EXISTS
            (
                SELECT
                    1/0
                FROM #deadlock_process AS dp
                WHERE dp.database_id = DB_ID()
            )
            BEGIN
                SELECT
                    database_id =
                        DB_ID(),
                    p.partition_id,
                    schema_name =
                        s.name,
                    table_name =
                        t.name
                FROM sys.partitions p
                JOIN sys.tables t
                  ON t.object_id = p.object_id
                JOIN sys.schemas s
                  ON s.schema_id = t.schema_id
                WHERE s.name IS NOT NULL
                AND   t.name IS NOT NULL
                OPTION(RECOMPILE);
            END;
            ';

        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
        END;

        IF @Azure = 1
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Inserting to @sysAssObjId at %s', 0, 1, @d) WITH NOWAIT;

            INSERT INTO
                @sysAssObjId
            (
                database_id,
                partition_id,
                schema_name,
                table_name
            )
            SELECT
                database_id =
                    DB_ID(),
                p.partition_id,
                schema_name =
                    s.name,
                table_name =
                    t.name
            FROM sys.partitions p
            JOIN sys.tables t
              ON t.object_id = p.object_id
            JOIN sys.schemas s
              ON s.schema_id = t.schema_id
            WHERE s.name IS NOT NULL
            AND   t.name IS NOT NULL
            OPTION(RECOMPILE);

            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
        END;

        /*Begin checks based on parsed values*/

        /*
        First, revert these back since we already converted the event data to local time,
        and searches will break if we use the times converted over to UTC for the event data
        */
        SELECT
            @StartDate = @StartDateOriginal,
            @EndDate = @EndDateOriginal;

        /*Check 1 is deadlocks by database*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 1  database deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 1,
            dp.database_name,
            object_name = N'-',
            finding_group = N'Total Database Deadlocks',
            finding =
                N'This database had ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' deadlocks.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dp.event_date) DESC)
        FROM #deadlock_process AS dp
        WHERE 1 = 1
        AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
        AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dp.event_date < @EndDate OR @EndDate IS NULL)
        AND (dp.client_app = @AppName OR @AppName IS NULL)
        AND (dp.host_name = @HostName OR @HostName IS NULL)
        AND (dp.login_name = @LoginName OR @LoginName IS NULL)
        GROUP BY dp.database_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 2 is deadlocks with selects*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 2 select deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 2,
            dow.database_name,
            object_name =
                CASE
                    WHEN EXISTS
                         (
                             SELECT
                                 1/0
                             FROM sys.databases AS d
                             WHERE d.name COLLATE DATABASE_DEFAULT = dow.database_name COLLATE DATABASE_DEFAULT
                             AND   d.is_read_committed_snapshot_on = 1
                         )
                    THEN N'You already enabled RCSI, but...'
                    ELSE N'You Might Need RCSI'
                END,
            finding_group = N'Total Deadlocks Involving Selects',
            finding =
                N'There have been ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dow.event_date)
                ) +
                N' deadlock(s) between read queries and modification queries.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dow.event_date) DESC)
        FROM #deadlock_owner_waiter AS dow
        WHERE 1 = 1
        AND dow.lock_mode IN
            (
                N'S',
                N'IS'
            )
        OR  dow.owner_mode IN
            (
                N'S',
                N'IS'
            )
        OR  dow.waiter_mode IN
            (
                N'S',
                N'IS'
            )
        AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
        AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dow.event_date < @EndDate OR @EndDate IS NULL)
        AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
        GROUP BY
            dow.database_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 3 is deadlocks by object*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 3 object deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 3,
            dow.database_name,
            object_name =
                ISNULL
                (
                    dow.object_name,
                    N'UNKNOWN'
                ),
            finding_group = N'Total Object Deadlocks',
            finding =
                N'This object was involved in ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dow.event_date)
                ) +
                N' deadlock(s).',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dow.event_date) DESC)
        FROM #deadlock_owner_waiter AS dow
        WHERE 1 = 1
        AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
        AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dow.event_date < @EndDate OR @EndDate IS NULL)
        AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
        GROUP BY
            dow.database_name,
            dow.object_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 3 continuation, number of deadlocks per index*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 3 (continued) index deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 3,
            dow.database_name,
            index_name = dow.index_name,
            finding_group = N'Total Index Deadlocks',
            finding =
                N'This index was involved in ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dow.event_date)
                ) +
                N' deadlock(s).',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dow.event_date) DESC)
        FROM #deadlock_owner_waiter AS dow
        WHERE 1 = 1
        AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
        AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dow.event_date < @EndDate OR @EndDate IS NULL)
        AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
        AND dow.lock_type NOT IN
            (
                N'HEAP',
                N'RID'
            )
        AND dow.index_name IS NOT NULL
        GROUP BY
            dow.database_name,
            dow.index_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 3 continuation, number of deadlocks per heap*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 3 (continued) heap deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 3,
            dow.database_name,
            index_name = dow.index_name,
            finding_group = N'Total Heap Deadlocks',
            finding =
                N'This heap was involved in ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dow.event_date)
                ) +
                N' deadlock(s).',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dow.event_date) DESC)
        FROM #deadlock_owner_waiter AS dow
        WHERE 1 = 1
        AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
        AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dow.event_date < @EndDate OR @EndDate IS NULL)
        AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
        AND dow.lock_type IN
            (
                N'HEAP',
                N'RID'
            )
        GROUP BY
            dow.database_name,
            dow.index_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 4 looks for Serializable deadlocks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 4 serializable deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 4,
            database_name =
                dp.database_name,
            object_name = N'-',
            finding_group = N'Serializable Deadlocking',
            finding =
                N'This database has had ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' instances of Serializable deadlocks.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dp.event_date) DESC)
        FROM #deadlock_process AS dp
        WHERE dp.isolation_level LIKE N'serializable%'
        AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
        AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dp.event_date < @EndDate OR @EndDate IS NULL)
        AND (dp.client_app = @AppName OR @AppName IS NULL)
        AND (dp.host_name = @HostName OR @HostName IS NULL)
        AND (dp.login_name = @LoginName OR @LoginName IS NULL)
        GROUP BY dp.database_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 5 looks for Repeatable Read deadlocks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 5 repeatable read deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 5,
            dp.database_name,
            object_name = N'-',
            finding_group = N'Repeatable Read Deadlocking',
            finding =
                N'This database has had ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' instances of Repeatable Read deadlocks.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dp.event_date) DESC)
        FROM #deadlock_process AS dp
        WHERE dp.isolation_level LIKE N'repeatable%'
        AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
        AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dp.event_date < @EndDate OR @EndDate IS NULL)
        AND (dp.client_app = @AppName OR @AppName IS NULL)
        AND (dp.host_name = @HostName OR @HostName IS NULL)
        AND (dp.login_name = @LoginName OR @LoginName IS NULL)
        GROUP BY dp.database_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 6 breaks down app, host, and login information*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 6 app/host/login deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 6,
            database_name =
                dp.database_name,
            object_name = N'-',
            finding_group = N'Login, App, and Host deadlocks',
            finding =
                N'This database has had ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' instances of deadlocks involving the login ' +
                ISNULL
                (
                    dp.login_name,
                    N'UNKNOWN'
                ) +
                N' from the application ' +
                ISNULL
                (
                    dp.client_app,
                    N'UNKNOWN'
                ) +
                N' on host ' +
                ISNULL
                (
                    dp.host_name,
                    N'UNKNOWN'
                ) +
                N'.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT dp.event_date) DESC)
        FROM #deadlock_process AS dp
        WHERE 1 = 1
        AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
        AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dp.event_date < @EndDate OR @EndDate IS NULL)
        AND (dp.client_app = @AppName OR @AppName IS NULL)
        AND (dp.host_name = @HostName OR @HostName IS NULL)
        AND (dp.login_name = @LoginName OR @LoginName IS NULL)
        GROUP BY
            dp.database_name,
            dp.login_name,
            dp.client_app,
            dp.host_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 7 breaks down the types of deadlocks (object, page, key, etc.)*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 7 types of deadlocks %s', 0, 1, @d) WITH NOWAIT;

        WITH
            lock_types AS
        (
            SELECT
                database_name =
                    dp.database_name,
                dow.object_name,
                lock =
                    CASE
                        WHEN CHARINDEX(N':', dp.wait_resource) > 0
                        THEN SUBSTRING
                             (
                                 dp.wait_resource,
                                 1,
                                 CHARINDEX(N':', dp.wait_resource) - 1
                             )
                        ELSE dp.wait_resource
                    END,
                lock_count =
                    CONVERT
                    (
                        nvarchar(20),
                        COUNT_BIG(DISTINCT dp.event_date)
                    )
            FROM #deadlock_process AS dp
            JOIN #deadlock_owner_waiter AS dow
              ON (dp.id = dow.owner_id
                  OR dp.victim_id = dow.waiter_id)
              AND dp.event_date = dow.event_date
            WHERE 1 = 1
            AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
            AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
            AND (dp.event_date < @EndDate OR @EndDate IS NULL)
            AND (dp.client_app = @AppName OR @AppName IS NULL)
            AND (dp.host_name = @HostName OR @HostName IS NULL)
            AND (dp.login_name = @LoginName OR @LoginName IS NULL)
            AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
            AND dow.object_name IS NOT NULL
            GROUP BY
                dp.database_name,
                CASE
                    WHEN CHARINDEX(N':', dp.wait_resource) > 0
                    THEN SUBSTRING
                         (
                             dp.wait_resource,
                             1,
                             CHARINDEX(N':', dp.wait_resource) - 1
                         )
                    ELSE dp.wait_resource
                END,
                dow.object_name
        )
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 7,
            lt.database_name,
            lt.object_name,
            finding_group = N'Types of locks by object',
            finding =
                N'This object has had ' +
                STUFF
                (
                    (
                        SELECT
                            N', ' +
                            lt2.lock_count +
                            N' ' +
                            lt2.lock
                        FROM lock_types AS lt2
                        WHERE lt2.database_name = lt.database_name
                        AND   lt2.object_name = lt.object_name
                        FOR XML
                            PATH(N''),
                            TYPE
                    ).value(N'.[1]', N'nvarchar(MAX)'),
                    1,
                    1,
                    N''
                ) + N' locks.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY CONVERT(bigint, lt.lock_count) DESC)
        FROM lock_types AS lt
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 8 gives you more info queries for sp_BlitzCache & BlitzQueryStore*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 8 more info part 1 BlitzCache %s', 0, 1, @d) WITH NOWAIT;

        WITH
            deadlock_stack AS
            (
                SELECT DISTINCT
                ds.id,
                ds.event_date,
                ds.proc_name,
                    database_name =
                        PARSENAME(ds.proc_name, 3),
                    schema_name =
                        PARSENAME(ds.proc_name, 2),
                    proc_only_name =
                        PARSENAME(ds.proc_name, 1),
                    sql_handle_csv =
                        N'''' +
                        STUFF
                        (
                            (
                                SELECT DISTINCT
                                    N',' +
                                    ds2.sql_handle
                                FROM #deadlock_stack AS ds2
                                WHERE ds2.id = ds.id
                                AND   ds2.event_date = ds.event_date
                                AND   ds2.sql_handle <> 0x
                                FOR XML
                                    PATH(N''),
                                    TYPE
                            ).value(N'.[1]', N'nvarchar(MAX)'),
                            1,
                            1,
                            N''
                        ) +
                        N''''
                FROM #deadlock_stack AS ds
                WHERE ds.sql_handle <> 0x
                GROUP BY
                    PARSENAME(ds.proc_name, 3),
                    PARSENAME(ds.proc_name, 2),
                    PARSENAME(ds.proc_name, 1),
                    ds.proc_name,
                    ds.id,
                    ds.event_date
            )
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT DISTINCT
            check_id = 8,
            dow.database_name,
            object_name = ds.proc_name,
            finding_group = N'More Info - Query',
            finding = N'EXECUTE sp_BlitzCache ' +
                CASE
                    WHEN ds.proc_name = N'adhoc'
                    THEN N'@OnlySqlHandles = ' + ds.sql_handle_csv
                    ELSE N'@StoredProcName = ' + QUOTENAME(ds.proc_only_name, N'''')
                END + N';'
        FROM deadlock_stack AS ds
        JOIN #deadlock_owner_waiter AS dow
          ON dow.owner_id = ds.id
          AND dow.event_date = ds.event_date
        WHERE 1 = 1
        AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
        AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dow.event_date < @EndDate OR @EndDate IS NULL)
        AND (dow.object_name = @StoredProcName OR @StoredProcName IS NULL)
        AND ds.proc_name NOT LIKE 'Unknown%'
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        IF (@ProductVersionMajor >= 13 OR @Azure = 1)
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Check 8 more info part 2 BlitzQueryStore %s', 0, 1, @d) WITH NOWAIT;

            WITH
                deadlock_stack AS
            (
                SELECT DISTINCT
                    ds.id,
                    ds.sql_handle,
                    ds.proc_name,
                    ds.event_date,
                    database_name =
                        PARSENAME(ds.proc_name, 3),
                    schema_name =
                        PARSENAME(ds.proc_name, 2),
                    proc_only_name =
                        PARSENAME(ds.proc_name, 1)
                FROM #deadlock_stack AS ds
            )
            INSERT
                #deadlock_findings WITH(TABLOCKX)
            (
                check_id,
                database_name,
                object_name,
                finding_group,
                finding
            )
            SELECT DISTINCT
                check_id = 8,
                dow.database_name,
                object_name = ds.proc_name,
                finding_group = N'More Info - Query',
                finding =
                    N'EXECUTE sp_BlitzQueryStore ' +
                    N'@DatabaseName = ' +
                    QUOTENAME(ds.database_name, N'''') +
                    N', ' +
                    N'@StoredProcName = ' +
                    QUOTENAME(ds.proc_only_name, N'''') +
                    N';'
            FROM deadlock_stack AS ds
            JOIN #deadlock_owner_waiter AS dow
              ON dow.owner_id = ds.id
              AND dow.event_date = ds.event_date
            WHERE ds.proc_name <> N'adhoc'
            AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
            AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
            AND (dow.event_date < @EndDate OR @EndDate IS NULL)
            AND (dow.object_name = @StoredProcName OR @StoredProcName IS NULL)
            OPTION(RECOMPILE);

            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
        END;

        /*Check 9 gives you stored procedure deadlock counts*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 9 stored procedure deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 9,
            database_name =
                dp.database_name,
            object_name = ds.proc_name,
            finding_group = N'Stored Procedure Deadlocks',
            finding =
                N'The stored procedure ' +
                PARSENAME(ds.proc_name, 2) +
                N'.' +
                PARSENAME(ds.proc_name, 1) +
                N' has been involved in ' +
                CONVERT
                (
                    nvarchar(10),
                    COUNT_BIG(DISTINCT ds.id)
                ) +
                N' deadlocks.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT ds.id) DESC)
        FROM #deadlock_stack AS ds
        JOIN #deadlock_process AS dp
          ON dp.id = ds.id
          AND ds.event_date = dp.event_date
        WHERE ds.proc_name <> N'adhoc'
        AND (ds.proc_name = @StoredProcName OR @StoredProcName IS NULL)
        AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
        AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
        AND (dp.event_date < @EndDate OR @EndDate IS NULL)
        AND (dp.client_app = @AppName OR @AppName IS NULL)
        AND (dp.host_name = @HostName OR @HostName IS NULL)
        AND (dp.login_name = @LoginName OR @LoginName IS NULL)
        GROUP BY
            dp.database_name,
            ds.proc_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 10 gives you more info queries for sp_BlitzIndex */
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 10 more info, BlitzIndex  %s', 0, 1, @d) WITH NOWAIT;

        WITH
            bi AS
            (
                SELECT DISTINCT
                    dow.object_name,
                    dow.database_name,
                    schema_name = s.schema_name,
                    table_name = s.table_name
                FROM #deadlock_owner_waiter AS dow
                JOIN @sysAssObjId AS s
                  ON  s.database_id = dow.database_id
                  AND s.partition_id = dow.associatedObjectId
                WHERE 1 = 1
                AND (dow.database_id = @DatabaseId OR @DatabaseName IS NULL)
                AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
                AND (dow.event_date < @EndDate OR @EndDate IS NULL)
                AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
                AND dow.object_name IS NOT NULL
            )
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT DISTINCT
            check_id = 10,
            bi.database_name,
            bi.object_name,
            finding_group = N'More Info - Table',
            finding =
                N'EXECUTE sp_BlitzIndex ' +
                N'@DatabaseName = ' +
                QUOTENAME(bi.database_name, N'''') +
                N', @SchemaName = ' +
                QUOTENAME(bi.schema_name, N'''') +
                N', @TableName = ' +
                QUOTENAME(bi.table_name, N'''') +
                N';'
        FROM bi
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 11 gets total deadlock wait time per object*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 11 deadlock wait time per object %s', 0, 1, @d) WITH NOWAIT;

        WITH
            chopsuey AS
        (

         SELECT
             database_name =
                 dp.database_name,
             dow.object_name,
             wait_days =
                 CONVERT
                 (
                     nvarchar(30),
                     (
                         SUM
                         (
                             CONVERT
                             (
                                 bigint,
                                 dp.wait_time
                             )
                          ) / 1000 / 86400
                     )
                 ),
             wait_time_hms =
             /*the more wait time you rack up the less accurate this gets,
             it's either that or erroring out*/
            CASE
                WHEN
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            dp.wait_time
                        )
                    )/1000 > 2147483647
                THEN
                   CONVERT
                   (
                       nvarchar(30),
                       DATEADD
                       (
                            MINUTE,
                            (
                                 (
                                    SUM
                                    (
                                       CONVERT
                                       (
                                           bigint,
                                           dp.wait_time
                                       )
                                    )
                                 )/
                                 60000
                            ),
                            0
                       ),
                       14
                   )
                WHEN
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            dp.wait_time
                        )
                    ) BETWEEN 2147483648 AND 2147483647000
                THEN
                   CONVERT
                   (
                       nvarchar(30),
                       DATEADD
                       (
                            SECOND,
                            (
                                 (
                                    SUM
                                    (
                                       CONVERT
                                       (
                                           bigint,
                                           dp.wait_time
                                       )
                                    )
                                 )/
                                 1000
                            ),
                            0
                       ),
                       14
                   )
                ELSE
                 CONVERT
                 (
                     nvarchar(30),
                     DATEADD
                     (
                         MILLISECOND,
                         (
                             SUM
                             (
                                 CONVERT
                                 (
                                     bigint,
                                     dp.wait_time
                                  )
                             )
                         ),
                         0
                    ),
                    14
                 )
                 END,
                 total_waits =
                     SUM(CONVERT(bigint, dp.wait_time))
            FROM #deadlock_owner_waiter AS dow
            JOIN #deadlock_process AS dp
              ON (dp.id = dow.owner_id
                  OR dp.victim_id = dow.waiter_id)
              AND dp.event_date = dow.event_date
            WHERE 1 = 1
            AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
            AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
            AND (dp.event_date < @EndDate OR @EndDate IS NULL)
            AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
            AND (dp.client_app = @AppName OR @AppName IS NULL)
            AND (dp.host_name = @HostName OR @HostName IS NULL)
            AND (dp.login_name = @LoginName OR @LoginName IS NULL)
            GROUP BY
                dp.database_name,
                dow.object_name
        )
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 11,
            cs.database_name,
            cs.object_name,
            finding_group = N'Total object deadlock wait time',
            finding =
                N'This object has had ' +
                CONVERT
                (
                    nvarchar(30),
                    cs.wait_days
                ) +
                N' ' +
                CONVERT
                (
                    nvarchar(30),
                    cs.wait_time_hms,
                    14
                ) +
                N' [dd hh:mm:ss:ms] of deadlock wait time.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY cs.total_waits DESC)
        FROM chopsuey AS cs
        WHERE cs.object_name IS NOT NULL
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 12 gets total deadlock wait time per database*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 12 deadlock wait time per database %s', 0, 1, @d) WITH NOWAIT;

        WITH
            wait_time AS
        (
            SELECT
                database_name =
                    dp.database_name,
                total_wait_time_ms =
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            dp.wait_time
                        )
                    )
            FROM #deadlock_owner_waiter AS dow
            JOIN #deadlock_process AS dp
              ON (dp.id = dow.owner_id
                  OR dp.victim_id = dow.waiter_id)
              AND dp.event_date = dow.event_date
            WHERE 1 = 1
            AND (dp.database_name = @DatabaseName OR @DatabaseName IS NULL)
            AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
            AND (dp.event_date < @EndDate OR @EndDate IS NULL)
            AND (dp.client_app = @AppName OR @AppName IS NULL)
            AND (dp.host_name = @HostName OR @HostName IS NULL)
            AND (dp.login_name = @LoginName OR @LoginName IS NULL)
            GROUP BY
                dp.database_name
        )
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 12,
            wt.database_name,
            object_name = N'-',
            finding_group = N'Total database deadlock wait time',
            N'This database has had ' +
            CONVERT
            (
                nvarchar(30),
                (
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            wt.total_wait_time_ms
                        )
                    ) / 1000 / 86400
                )
            ) +
            N' ' +
        /*the more wait time you rack up the less accurate this gets,
        it's either that or erroring out*/
            CASE
                WHEN
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            wt.total_wait_time_ms
                        )
                    )/1000 > 2147483647
                THEN
                   CONVERT
                   (
                       nvarchar(30),
                       DATEADD
                       (
                            MINUTE,
                            (
                                 (
                                    SUM
                                    (
                                       CONVERT
                                       (
                                           bigint,
                                           wt.total_wait_time_ms
                                       )
                                    )
                                 )/
                                 60000
                            ),
                            0
                       ),
                       14
                   )
                WHEN
                    SUM
                    (
                        CONVERT
                        (
                            bigint,
                            wt.total_wait_time_ms
                        )
                    ) BETWEEN 2147483648 AND 2147483647000
                THEN
                   CONVERT
                   (
                       nvarchar(30),
                       DATEADD
                       (
                            SECOND,
                            (
                                 (
                                    SUM
                                    (
                                       CONVERT
                                       (
                                           bigint,
                                           wt.total_wait_time_ms
                                       )
                                    )
                                 )/
                                 1000
                            ),
                            0
                       ),
                       14
                   )
                ELSE
            CONVERT
              (
                  nvarchar(30),
                  DATEADD
                  (
                      MILLISECOND,
                      (
                          SUM
                          (
                              CONVERT
                              (
                                  bigint,
                                  wt.total_wait_time_ms
                              )
                          )
                      ),
                      0
                  ),
                  14
              ) END +
            N' [dd hh:mm:ss:ms] of deadlock wait time.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY SUM(CONVERT(bigint, wt.total_wait_time_ms)) DESC)
        FROM wait_time AS wt
        GROUP BY
            wt.database_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 13 gets total deadlock wait time for SQL Agent*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 13 deadlock count for SQL Agent %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding,
            sort_order
        )
        SELECT
            check_id = 13,
            database_name =
                DB_NAME(aj.database_id),
            object_name =
                N'SQLAgent - Job: ' +
                aj.job_name +
                N' Step: ' +
                aj.step_name,
            finding_group = N'Agent Job Deadlocks',
            finding =
                N'There have been ' +
                RTRIM(COUNT_BIG(DISTINCT aj.event_date)) +
                N' deadlocks from this Agent Job and Step.',
            sort_order = 
                ROW_NUMBER()
                OVER (ORDER BY COUNT_BIG(DISTINCT aj.event_date) DESC)
        FROM #agent_job AS aj
        GROUP BY
            DB_NAME(aj.database_id),
            aj.job_name,
            aj.step_name
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 14 is total parallel deadlocks*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 14 parallel deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT
            check_id = 14,
            database_name = N'-',
            object_name = N'-',
            finding_group = N'Total parallel deadlocks',
            finding =
                N'There have been ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT drp.event_date)
                ) +
                N' parallel deadlocks.'
        FROM #deadlock_resource_parallel AS drp
        HAVING COUNT_BIG(DISTINCT drp.event_date) > 0
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 15 is total deadlocks involving sleeping sessions*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 15 sleeping and background deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT
            check_id = 15,
            database_name = N'-',
            object_name = N'-',
            finding_group = N'Total deadlocks involving sleeping sessions',
            finding =
                N'There have been ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' sleepy deadlocks.'
        FROM #deadlock_process AS dp
        WHERE dp.status = N'sleeping'
        HAVING COUNT_BIG(DISTINCT dp.event_date) > 0
        OPTION(RECOMPILE);

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT
            check_id = 15,
            database_name = N'-',
            object_name = N'-',
            finding_group = N'Total deadlocks involving background processes',
            finding =
                N'There have been ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' deadlocks with background task.'
        FROM #deadlock_process AS dp
        WHERE dp.status = N'background'
        HAVING COUNT_BIG(DISTINCT dp.event_date) > 0
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Check 16 is total deadlocks involving implicit transactions*/
        SET @d = CONVERT(varchar(40), GETDATE(), 109);
        RAISERROR('Check 16 implicit transaction deadlocks %s', 0, 1, @d) WITH NOWAIT;

        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        SELECT
            check_id = 14,
            database_name = N'-',
            object_name = N'-',
            finding_group = N'Total implicit transaction deadlocks',
            finding =
                N'There have been ' +
                CONVERT
                (
                    nvarchar(20),
                    COUNT_BIG(DISTINCT dp.event_date)
                ) +
                N' implicit transaction deadlocks.'
        FROM #deadlock_process AS dp
        WHERE dp.transaction_name = N'implicit_transaction'
        HAVING COUNT_BIG(DISTINCT dp.event_date) > 0
        OPTION(RECOMPILE);

        RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

        /*Thank you goodnight*/
        INSERT
            #deadlock_findings WITH(TABLOCKX)
        (
            check_id,
            database_name,
            object_name,
            finding_group,
            finding
        )
        VALUES
        (
            -1,
            N'sp_BlitzLock version ' + CONVERT(nvarchar(10), @Version),
            N'Results for ' + CONVERT(nvarchar(10), @StartDate, 23) + N' through ' + CONVERT(nvarchar(10), @EndDate, 23),
            N'http://FirstResponderKit.org/',
            N'To get help or add your own contributions to the SQL Server First Responder Kit, join us at http://FirstResponderKit.org.'
        );

        RAISERROR('Finished rollup at %s', 0, 1, @d) WITH NOWAIT;

        /*Results*/
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Results 1 %s', 0, 1, @d) WITH NOWAIT;

            CREATE CLUSTERED INDEX cx_whatever_dp  ON #deadlock_process (event_date, id);
            CREATE CLUSTERED INDEX cx_whatever_drp ON #deadlock_resource_parallel (event_date, owner_id);
            CREATE CLUSTERED INDEX cx_whatever_dow ON #deadlock_owner_waiter (event_date, owner_id, waiter_id);

            IF @Debug = 1 BEGIN SET STATISTICS XML ON; END;

            WITH
                deadlocks AS
            (
                SELECT
                    deadlock_type =
                        N'Regular Deadlock',
                    dp.event_date,
                    dp.id,
                    dp.victim_id,
                    dp.spid,
                    dp.database_id,
                    dp.database_name,
                    dp.current_database_name,
                    dp.priority,
                    dp.log_used,
                    wait_resource =
                        dp.wait_resource COLLATE DATABASE_DEFAULT,
                    object_names =
                        CONVERT
                        (
                            xml,
                            STUFF
                            (
                                (
                                    SELECT DISTINCT
                                        object_name =
                                            NCHAR(10) +
                                            N' <object>' +
                                            ISNULL(c.object_name, N'') +
                                            N'</object> ' COLLATE DATABASE_DEFAULT
                                    FROM #deadlock_owner_waiter AS c
                                    WHERE (dp.id = c.owner_id
                                           OR dp.victim_id = c.waiter_id)
                                    AND dp.event_date = c.event_date
                                    FOR XML
                                        PATH(N''),
                                        TYPE
                                ).value(N'.[1]', N'nvarchar(4000)'),
                                1,
                                1,
                                N''
                            )
                        ),
                    dp.wait_time,
                    dp.transaction_name,
                    dp.status,
                    dp.last_tran_started,
                    dp.last_batch_started,
                    dp.last_batch_completed,
                    dp.lock_mode,
                    dp.transaction_count,
                    dp.client_app,
                    dp.host_name,
                    dp.login_name,
                    dp.isolation_level,
                    dp.client_option_1,
                    dp.client_option_2,
                    inputbuf =
                        dp.process_xml.value('(//process/inputbuf/text())[1]', 'nvarchar(MAX)'),
                    en =
                        DENSE_RANK() OVER (ORDER BY dp.event_date),
                    qn =
                        ROW_NUMBER() OVER (PARTITION BY dp.event_date ORDER BY dp.event_date) - 1,
                    dn =
                        ROW_NUMBER() OVER (PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date),
                    dp.is_victim,
                    owner_mode =
                        ISNULL(dp.owner_mode, N'-'),
                    owner_waiter_type = NULL,
                    owner_activity = NULL,
                    owner_waiter_activity = NULL,
                    owner_merging = NULL,
                    owner_spilling = NULL,
                    owner_waiting_to_close = NULL,
                    waiter_mode =
                        ISNULL(dp.waiter_mode, N'-'),
                    waiter_waiter_type = NULL,
                    waiter_owner_activity = NULL,
                    waiter_waiter_activity = NULL,
                    waiter_merging = NULL,
                    waiter_spilling = NULL,
                    waiter_waiting_to_close = NULL,
                    dp.deadlock_graph
                FROM #deadlock_process AS dp
                WHERE dp.victim_id IS NOT NULL
                AND   dp.is_parallel = 0

                UNION ALL

                SELECT
                    deadlock_type =
                        N'Parallel Deadlock',
                    dp.event_date,
                    dp.id,
                    dp.victim_id,
                    dp.spid,
                    dp.database_id,
                    dp.database_name,
                    dp.current_database_name,
                    dp.priority,
                    dp.log_used,
                    dp.wait_resource COLLATE DATABASE_DEFAULT,
                    object_names =
                        CONVERT
                        (
                            xml,
                            STUFF
                            (
                                (
                                    SELECT DISTINCT
                                        object_name =
                                            NCHAR(10) +
                                            N' <object>' +
                                            ISNULL(c.object_name, N'') +
                                            N'</object> ' COLLATE DATABASE_DEFAULT
                                    FROM #deadlock_owner_waiter AS c
                                    WHERE (dp.id = c.owner_id
                                           OR dp.victim_id = c.waiter_id)
                                    AND dp.event_date = c.event_date
                                    FOR XML
                                        PATH(N''),
                                        TYPE
                                ).value(N'.[1]', N'nvarchar(4000)'),
                                1,
                                1,
                                N''
                            )
                        ),
                    dp.wait_time,
                    dp.transaction_name,
                    dp.status,
                    dp.last_tran_started,
                    dp.last_batch_started,
                    dp.last_batch_completed,
                    dp.lock_mode,
                    dp.transaction_count,
                    dp.client_app,
                    dp.host_name,
                    dp.login_name,
                    dp.isolation_level,
                    dp.client_option_1,
                    dp.client_option_2,
                    inputbuf =
                        dp.process_xml.value('(//process/inputbuf/text())[1]', 'nvarchar(MAX)'),
                    en =
                        DENSE_RANK() OVER (ORDER BY dp.event_date),
                    qn =
                        ROW_NUMBER() OVER (PARTITION BY dp.event_date ORDER BY dp.event_date) - 1,
                    dn =
                        ROW_NUMBER() OVER (PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date),
                    is_victim = 1,
                    owner_mode = cao.wait_type COLLATE DATABASE_DEFAULT,
                    owner_waiter_type = cao.waiter_type,
                    owner_activity = cao.owner_activity,
                    owner_waiter_activity = cao.waiter_activity,
                    owner_merging = cao.merging,
                    owner_spilling = cao.spilling,
                    owner_waiting_to_close = cao.waiting_to_close,
                    waiter_mode = caw.wait_type COLLATE DATABASE_DEFAULT,
                    waiter_waiter_type = caw.waiter_type,
                    waiter_owner_activity = caw.owner_activity,
                    waiter_waiter_activity = caw.waiter_activity,
                    waiter_merging = caw.merging,
                    waiter_spilling = caw.spilling,
                    waiter_waiting_to_close = caw.waiting_to_close,
                    dp.deadlock_graph
                FROM #deadlock_process AS dp
                OUTER APPLY
                (
                    SELECT TOP (1)
                        drp.*
                    FROM #deadlock_resource_parallel AS drp
                    WHERE drp.owner_id = dp.id
                    AND drp.wait_type IN
                        (
                            N'e_waitPortOpen',
                            N'e_waitPipeNewRow'
                        )
                    ORDER BY drp.event_date
                ) AS cao
                OUTER APPLY
                (
                    SELECT TOP (1)
                        drp.*
                    FROM #deadlock_resource_parallel AS drp
                    WHERE drp.owner_id = dp.id
                    AND drp.wait_type IN
                        (
                            N'e_waitPortOpen',
                            N'e_waitPipeGetRow'
                        )
                    ORDER BY drp.event_date
                ) AS caw
                WHERE dp.is_parallel = 1
            )
            SELECT
                d.deadlock_type,
                d.event_date,
                d.id,
                d.victim_id,
                d.spid,
                deadlock_group =
                    N'Deadlock #' +
                    CONVERT
                    (
                        nvarchar(10),
                        d.en
                    ) +
                    N', Query #'
                    + CASE
                          WHEN d.qn = 0
                          THEN N'1'
                          ELSE CONVERT(nvarchar(10), d.qn)
                      END + CASE
                                WHEN d.is_victim = 1
                                THEN N' - VICTIM'
                                ELSE N''
                            END,
                d.database_id,
                d.database_name,
                d.current_database_name,
                d.priority,
                d.log_used,
                d.wait_resource,
                d.object_names,
                d.wait_time,
                d.transaction_name,
                d.status,
                d.last_tran_started,
                d.last_batch_started,
                d.last_batch_completed,
                d.lock_mode,
                d.transaction_count,
                d.client_app,
                d.host_name,
                d.login_name,
                d.isolation_level,
                d.client_option_1,
                d.client_option_2,
                inputbuf =
                    CASE
                        WHEN d.inputbuf
                             LIKE @inputbuf_bom + N'Proc |[Database Id = %' ESCAPE N'|'
                        THEN
                        OBJECT_SCHEMA_NAME
                        (
                                SUBSTRING
                                (
                                    d.inputbuf,
                                    CHARINDEX(N'Object Id = ', d.inputbuf) + 12,
                                    LEN(d.inputbuf) - (CHARINDEX(N'Object Id = ', d.inputbuf) + 12)
                                )
                                ,
                                SUBSTRING
                                (
                                    d.inputbuf,
                                    CHARINDEX(N'Database Id = ', d.inputbuf) + 14,
                                    CHARINDEX(N'Object Id', d.inputbuf) - (CHARINDEX(N'Database Id = ', d.inputbuf) + 14)
                                )
                        ) +
                        N'.' +
                        OBJECT_NAME
                        (
                             SUBSTRING
                             (
                                 d.inputbuf,
                                 CHARINDEX(N'Object Id = ', d.inputbuf) + 12,
                                 LEN(d.inputbuf) - (CHARINDEX(N'Object Id = ', d.inputbuf) + 12)
                             )
                             ,
                             SUBSTRING
                             (
                                 d.inputbuf,
                                 CHARINDEX(N'Database Id = ', d.inputbuf) + 14,
                                 CHARINDEX(N'Object Id', d.inputbuf) - (CHARINDEX(N'Database Id = ', d.inputbuf) + 14)
                             )
                        )
                        ELSE d.inputbuf
                    END COLLATE Latin1_General_BIN2,
                d.owner_mode,
                d.owner_waiter_type,
                d.owner_activity,
                d.owner_waiter_activity,
                d.owner_merging,
                d.owner_spilling,
                d.owner_waiting_to_close,
                d.waiter_mode,
                d.waiter_waiter_type,
                d.waiter_owner_activity,
                d.waiter_waiter_activity,
                d.waiter_merging,
                d.waiter_spilling,
                d.waiter_waiting_to_close,
                d.deadlock_graph,
                d.is_victim
            INTO #deadlocks
            FROM deadlocks AS d
            WHERE d.dn = 1
            AND  (d.is_victim = @VictimsOnly
            OR    @VictimsOnly = 0)
            AND d.qn < CASE
                           WHEN d.deadlock_type = N'Parallel Deadlock'
                           THEN 2
                           ELSE 2147483647
                       END
            AND (DB_NAME(d.database_id) = @DatabaseName OR @DatabaseName IS NULL)
            AND (d.event_date >= @StartDate OR @StartDate IS NULL)
            AND (d.event_date < @EndDate OR @EndDate IS NULL)
            AND (CONVERT(nvarchar(MAX), d.object_names) COLLATE Latin1_General_BIN2 LIKE N'%' + @ObjectName + N'%' OR @ObjectName IS NULL)
            AND (d.client_app = @AppName OR @AppName IS NULL)
            AND (d.host_name = @HostName OR @HostName IS NULL)
            AND (d.login_name = @LoginName OR @LoginName IS NULL)
            AND (d.deadlock_type = @DeadlockType OR @DeadlockType IS NULL)
            OPTION (RECOMPILE, LOOP JOIN, HASH JOIN);

            UPDATE d
                SET d.inputbuf =
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            d.inputbuf,
                        NCHAR(31), N'?'), NCHAR(30), N'?'), NCHAR(29), N'?'), NCHAR(28), N'?'), NCHAR(27), N'?'),
                        NCHAR(26), N'?'), NCHAR(25), N'?'), NCHAR(24), N'?'), NCHAR(23), N'?'), NCHAR(22), N'?'),
                        NCHAR(21), N'?'), NCHAR(20), N'?'), NCHAR(19), N'?'), NCHAR(18), N'?'), NCHAR(17), N'?'),
                        NCHAR(16), N'?'), NCHAR(15), N'?'), NCHAR(14), N'?'), NCHAR(12), N'?'), NCHAR(11), N'?'),
                        NCHAR(8),  N'?'),  NCHAR(7), N'?'),  NCHAR(6), N'?'),  NCHAR(5), N'?'),  NCHAR(4), N'?'),
                        NCHAR(3),  N'?'),  NCHAR(2), N'?'),  NCHAR(1), N'?'),  NCHAR(0), N'?')
            FROM #deadlocks AS d
            OPTION(RECOMPILE);

            SELECT
                d.deadlock_type,
                d.event_date,
                database_name =
                    DB_NAME(d.database_id),
                database_name_x =
                    d.database_name,
                d.current_database_name,
                d.spid,
                d.deadlock_group,
                d.client_option_1,
                d.client_option_2,
                d.lock_mode,
                query_xml =
                    (
                        SELECT
                            [processing-instruction(query)] =
                                d.inputbuf
                        FOR XML
                            PATH(N''),
                            TYPE
                    ),
                query_string =
                    d.inputbuf,
                d.object_names,
                d.isolation_level,
                d.owner_mode,
                d.waiter_mode,
                d.transaction_count,
                d.login_name,
                d.host_name,
                d.client_app,
                d.wait_time,
                d.wait_resource,
                d.priority,
                d.log_used,
                d.last_tran_started,
                d.last_batch_started,
                d.last_batch_completed,
                d.transaction_name,
                d.status,
                /*These columns will be NULL for regular (non-parallel) deadlocks*/
                parallel_deadlock_details =
                    (
                        SELECT
                            d.owner_waiter_type,
                            d.owner_activity,
                            d.owner_waiter_activity,
                            d.owner_merging,
                            d.owner_spilling,
                            d.owner_waiting_to_close,
                            d.waiter_waiter_type,
                            d.waiter_owner_activity,
                            d.waiter_waiter_activity,
                            d.waiter_merging,
                            d.waiter_spilling,
                            d.waiter_waiting_to_close
                        FOR XML
                            PATH('parallel_deadlock_details'),
                            TYPE
                    ),
                d.owner_waiter_type,
                d.owner_activity,
                d.owner_waiter_activity,
                d.owner_merging,
                d.owner_spilling,
                d.owner_waiting_to_close,
                d.waiter_waiter_type,
                d.waiter_owner_activity,
                d.waiter_waiter_activity,
                d.waiter_merging,
                d.waiter_spilling,
                d.waiter_waiting_to_close,
                /*end parallel deadlock columns*/
                d.deadlock_graph,
                d.is_victim,
                d.id
            INTO #deadlock_results
            FROM #deadlocks AS d;

            IF @Debug = 1 BEGIN SET STATISTICS XML OFF; END;
            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

            /*There's too much risk of errors sending the*/
            IF @OutputDatabaseCheck = 0
            BEGIN
                SET @ExportToExcel = 0;
            END;

            SET @deadlock_result += N'
            SELECT
                server_name =
                    @@SERVERNAME,
                dr.deadlock_type,
                dr.event_date,
                database_name =
                    COALESCE
                    (
                        dr.database_name,
                        dr.database_name_x,
                        dr.current_database_name
                    ),
                dr.spid,
                dr.deadlock_group,
                ' + CASE @ExportToExcel
                         WHEN 1
                         THEN N'
                query = dr.query_string,
                object_names =
                    REPLACE(
                    REPLACE(
                        CONVERT
                        (
                            nvarchar(MAX),
                            dr.object_names
                        ) COLLATE Latin1_General_BIN2,
                    ''<object>'', ''''),
                    ''</object>'', ''''),'
                         ELSE N'query = dr.query_xml,
                dr.object_names,'
                    END + N'
                dr.isolation_level,
                dr.owner_mode,
                dr.waiter_mode,
                dr.lock_mode,
                dr.transaction_count,
                dr.client_option_1,
                dr.client_option_2,
                dr.login_name,
                dr.host_name,
                dr.client_app,
                dr.wait_time,
                dr.wait_resource,
                dr.priority,
                dr.log_used,
                dr.last_tran_started,
                dr.last_batch_started,
                dr.last_batch_completed,
                dr.transaction_name,
                dr.status,' +
                    CASE
                        WHEN (@ExportToExcel = 1
                              OR @OutputDatabaseCheck = 0)
                        THEN N'
                dr.owner_waiter_type,
                dr.owner_activity,
                dr.owner_waiter_activity,
                dr.owner_merging,
                dr.owner_spilling,
                dr.owner_waiting_to_close,
                dr.waiter_waiter_type,
                dr.waiter_owner_activity,
                dr.waiter_waiter_activity,
                dr.waiter_merging,
                dr.waiter_spilling,
                dr.waiter_waiting_to_close,'
                        ELSE N'
                dr.parallel_deadlock_details,'
                        END +
                    CASE
                        @ExportToExcel
                        WHEN 1
                        THEN N'
                deadlock_graph =
                    REPLACE(REPLACE(
                    REPLACE(REPLACE(
                        CONVERT
                        (
                            nvarchar(MAX),
                            dr.deadlock_graph
                        ) COLLATE Latin1_General_BIN2,
                    ''NCHAR(10)'', ''''), ''NCHAR(13)'', ''''),
                    ''CHAR(10)'', ''''), ''CHAR(13)'', '''')'
                        ELSE N'
                dr.deadlock_graph'
                   END + N'
            FROM #deadlock_results AS dr
            ORDER BY
                dr.event_date,
                dr.is_victim DESC
            OPTION(RECOMPILE, LOOP JOIN, HASH JOIN);
            ';

        IF (@OutputDatabaseCheck = 0)
        BEGIN
            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Results to table %s', 0, 1, @d) WITH NOWAIT;

            IF @Debug = 1
            BEGIN
                PRINT @deadlock_result;
                SET STATISTICS XML ON;
            END;

            SET @StringToExecute = N'

                INSERT INTO ' + QUOTENAME(DB_NAME()) + N'..DeadLockTbl
            (
                ServerName,
                deadlock_type,
                event_date,
                database_name,
                spid,
                deadlock_group,
                query,
                object_names,
                isolation_level,
                owner_mode,
                waiter_mode,
                lock_mode,
                transaction_count,
                client_option_1,
                client_option_2,
                login_name,
                host_name,
                client_app,
                wait_time,
                wait_resource,
                priority,
                log_used,
                last_tran_started,
                last_batch_started,
                last_batch_completed,
                transaction_name,
                status,
                owner_waiter_type,
                owner_activity,
                owner_waiter_activity,
                owner_merging,
                owner_spilling,
                owner_waiting_to_close,
                waiter_waiter_type,
                waiter_owner_activity,
                waiter_waiter_activity,
                waiter_merging,
                waiter_spilling,
                waiter_waiting_to_close,
                deadlock_graph
            )
            EXECUTE sys.sp_executesql
                @deadlock_result;';
            EXECUTE sys.sp_executesql @StringToExecute, N'@deadlock_result NVARCHAR(MAX)', @deadlock_result;

            IF @Debug = 1
            BEGIN
                SET STATISTICS XML OFF;
            END;

            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

            DROP SYNONYM dbo.DeadLockTbl;

            SET @d = CONVERT(varchar(40), GETDATE(), 109);
            RAISERROR('Findings to table %s', 0, 1, @d) WITH NOWAIT;

            SET @StringToExecute = N'

                INSERT INTO ' + QUOTENAME(DB_NAME()) + N'..DeadlockFindings
            (
                ServerName,
                check_id,
                database_name,
                object_name,
                finding_group,
                finding
            )
            SELECT
                @@SERVERNAME,
                df.check_id,
                df.database_name,
                df.object_name,
                df.finding_group,
                df.finding
            FROM #deadlock_findings AS df
            ORDER BY df.check_id
            OPTION(RECOMPILE);';
            EXECUTE sys.sp_executesql @StringToExecute;

            RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

            DROP SYNONYM dbo.DeadlockFindings; /*done with inserting.*/
        END;
        ELSE /*Output to database is not set output to client app*/
        BEGIN
                SET @d = CONVERT(varchar(40), GETDATE(), 109);
                RAISERROR('Results to client %s', 0, 1, @d) WITH NOWAIT;
           
                IF @Debug = 1 BEGIN SET STATISTICS XML ON; END;
           
                EXECUTE sys.sp_executesql
                    @deadlock_result;
           
                IF @Debug = 1
                BEGIN
                    SET STATISTICS XML OFF;
                    PRINT @deadlock_result;
                END;
           
                RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;

                SET @d = CONVERT(varchar(40), GETDATE(), 109);
                RAISERROR('Getting available execution plans for deadlocks %s', 0, 1, @d) WITH NOWAIT;
             
                SELECT DISTINCT
                    available_plans =
                        'available_plans',
                    ds.proc_name,
                    sql_handle =
                        CONVERT(varbinary(64), ds.sql_handle, 1),
                    dow.database_name,
                    dow.database_id,
                    dow.object_name,
                    query_xml =
                        TRY_CAST(dr.query_xml AS nvarchar(MAX))
                INTO #available_plans
                FROM #deadlock_stack AS ds
                JOIN #deadlock_owner_waiter AS dow
                  ON dow.owner_id = ds.id
                  AND dow.event_date = ds.event_date
                JOIN #deadlock_results AS dr
                  ON  dr.id = ds.id
                  AND dr.event_date = ds.event_date
                OPTION(RECOMPILE);

                SELECT
                    deqs.sql_handle,
                    deqs.plan_handle,
                    deqs.statement_start_offset,
                    deqs.statement_end_offset,
                    deqs.creation_time,
                    deqs.last_execution_time,
                    deqs.execution_count,
                    total_worker_time_ms =
                        deqs.total_worker_time / 1000.,
                    avg_worker_time_ms =
                        CONVERT(decimal(38, 6), deqs.total_worker_time / 1000. / deqs.execution_count),
                    total_elapsed_time_ms =
                        deqs.total_elapsed_time / 1000.,
                    avg_elapsed_time_ms =
                        CONVERT(decimal(38, 6), deqs.total_elapsed_time / 1000. / deqs.execution_count),
                    executions_per_second =
                        ISNULL
                        (
                            deqs.execution_count /
                                NULLIF
                                (
                                    DATEDIFF
                                    (
                                        SECOND,
                                        deqs.creation_time,
                                        NULLIF(deqs.last_execution_time, '1900-01-01 00:00:00.000')
                                    ),
                                    0
                                ),
                                0
                        ),
                    total_physical_reads_mb =
                        deqs.total_physical_reads * 8. / 1024.,
                    total_logical_writes_mb =
                        deqs.total_logical_writes * 8. / 1024.,
                    total_logical_reads_mb =
                        deqs.total_logical_reads * 8. / 1024.,
                    min_grant_mb =
                        deqs.min_grant_kb * 8. / 1024.,
                    max_grant_mb =
                        deqs.max_grant_kb * 8. / 1024.,
                    min_used_grant_mb =
                        deqs.min_used_grant_kb * 8. / 1024.,
                    max_used_grant_mb =
                        deqs.max_used_grant_kb * 8. / 1024.,  
                    deqs.min_reserved_threads,
                    deqs.max_reserved_threads,
                    deqs.min_used_threads,
                    deqs.max_used_threads,
                    deqs.total_rows,
                    max_worker_time_ms =
                        deqs.max_worker_time / 1000.,
                    max_elapsed_time_ms =
                        deqs.max_elapsed_time / 1000.
                INTO #dm_exec_query_stats
                FROM sys.dm_exec_query_stats AS deqs
                WHERE EXISTS
                (
                    SELECT
                        1/0
                    FROM #available_plans AS ap
                    WHERE ap.sql_handle = deqs.sql_handle
                )
                AND deqs.query_hash IS NOT NULL;
              
                CREATE CLUSTERED INDEX
                    deqs
                ON #dm_exec_query_stats
                (
                    sql_handle,
                    plan_handle
                );
              
                SELECT
                    ap.available_plans,
                    ap.database_name,
                    query_text =
                        TRY_CAST(ap.query_xml AS xml),
                    ap.query_plan,
                    ap.creation_time,
                    ap.last_execution_time,
                    ap.execution_count,
                    ap.executions_per_second,
                    ap.total_worker_time_ms,
                    ap.avg_worker_time_ms,
                    ap.max_worker_time_ms,
                    ap.total_elapsed_time_ms,
                    ap.avg_elapsed_time_ms,
                    ap.max_elapsed_time_ms,
                    ap.total_logical_reads_mb,
                    ap.total_physical_reads_mb,
                    ap.total_logical_writes_mb,
                    ap.min_grant_mb,
                    ap.max_grant_mb,
                    ap.min_used_grant_mb,
                    ap.max_used_grant_mb,
                    ap.min_reserved_threads,
                    ap.max_reserved_threads,
                    ap.min_used_threads,
                    ap.max_used_threads,
                    ap.total_rows,
                    ap.sql_handle,
                    ap.statement_start_offset,
                    ap.statement_end_offset
                FROM
                (
             
                    SELECT
                        ap.*,
                        c.statement_start_offset,
                        c.statement_end_offset,
                        c.creation_time,
                        c.last_execution_time,
                        c.execution_count,
                        c.total_worker_time_ms,
                        c.avg_worker_time_ms,
                        c.total_elapsed_time_ms,
                        c.avg_elapsed_time_ms,
                        c.executions_per_second,
                        c.total_physical_reads_mb,
                        c.total_logical_writes_mb,
                        c.total_logical_reads_mb,
                        c.min_grant_mb,
                        c.max_grant_mb,
                        c.min_used_grant_mb,
                        c.max_used_grant_mb,
                        c.min_reserved_threads,
                        c.max_reserved_threads,
                        c.min_used_threads,
                        c.max_used_threads,
                        c.total_rows,
                        c.query_plan,
                        c.max_worker_time_ms,
                        c.max_elapsed_time_ms
                    FROM #available_plans AS ap
                    OUTER APPLY
                    (
                        SELECT
                            deqs.*,
                            query_plan =
                                TRY_CAST(deps.query_plan AS xml)
                        FROM #dm_exec_query_stats deqs
                        OUTER APPLY sys.dm_exec_text_query_plan
                        (
                            deqs.plan_handle,
                            deqs.statement_start_offset,
                            deqs.statement_end_offset
                        ) AS deps
                        WHERE deqs.sql_handle = ap.sql_handle
                        AND   deps.dbid = ap.database_id
                    ) AS c
                ) AS ap
                WHERE ap.query_plan IS NOT NULL
                ORDER BY
                    ap.avg_worker_time_ms DESC
                OPTION(RECOMPILE, LOOP JOIN, HASH JOIN);

                RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
           
                SET @d = CONVERT(varchar(40), GETDATE(), 109);
                RAISERROR('Returning findings %s', 0, 1, @d) WITH NOWAIT;
           
                SELECT
                    df.check_id,
                    df.database_name,
                    df.object_name,
                    df.finding_group,
                    df.finding
                FROM #deadlock_findings AS df
                ORDER BY
                    df.check_id,
                    df.sort_order
                OPTION(RECOMPILE);
           
                SET @d = CONVERT(varchar(40), GETDATE(), 109);
                RAISERROR('Finished at %s', 0, 1, @d) WITH NOWAIT;
            END; /*done with output to client app.*/
        END;

        IF @Debug = 1
        BEGIN
            SELECT
                table_name = N'#deadlock_data',
                *
            FROM #deadlock_data AS dd
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#dd',
                *
            FROM #dd AS d
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_resource',
                *
            FROM #deadlock_resource AS dr
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_resource_parallel',
                *
            FROM #deadlock_resource_parallel AS drp
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_owner_waiter',
                *
            FROM #deadlock_owner_waiter AS dow
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_process',
                *
            FROM #deadlock_process AS dp
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_stack',
                *
            FROM #deadlock_stack AS ds
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlocks',
                *
            FROM #deadlocks AS d
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#deadlock_results',
                *
            FROM #deadlock_results AS dr
            OPTION(RECOMPILE);

            SELECT
                table_name = N'#x',
                *
            FROM #x AS x
            OPTION(RECOMPILE);

            SELECT
                table_name = N'@sysAssObjId',
                *
            FROM @sysAssObjId AS s
            OPTION(RECOMPILE);

            IF OBJECT_ID('tempdb..#available_plans') IS NOT NULL
            BEGIN
            SELECT
                table_name = N'#available_plans',
                *
            FROM #available_plans AS ap
            OPTION(RECOMPILE);
            END;

            IF OBJECT_ID('tempdb..#dm_exec_query_stats') IS NOT NULL
            BEGIN   
            SELECT
                table_name = N'#dm_exec_query_stats',
                *
            FROM #dm_exec_query_stats
            OPTION(RECOMPILE);
            END;

            SELECT
                procedure_parameters =
                    'procedure_parameters',
            DatabaseName =
                @DatabaseName,
            StartDate =
                @StartDate,
            EndDate =
                @EndDate,
            ObjectName =
                @ObjectName,
            StoredProcName =
                @StoredProcName,
            AppName =
                @AppName,
            HostName =
                @HostName,
            LoginName =
                @LoginName,
            EventSessionName =
                @EventSessionName,
            TargetSessionType =
                @TargetSessionType,
            VictimsOnly =
                @VictimsOnly,
            DeadlockType =
                @DeadlockType,
            TargetDatabaseName =
                @TargetDatabaseName,
            TargetSchemaName =
                @TargetSchemaName,
            TargetTableName =
                @TargetTableName,
            TargetColumnName =
                @TargetColumnName,
            TargetTimestampColumnName =
                @TargetTimestampColumnName,
            Debug =
                @Debug,
            Help =
                @Help,
            Version =
                @Version,
            VersionDate =
                @VersionDate,
            VersionCheckMode =
                @VersionCheckMode,
            OutputDatabaseName =
                @OutputDatabaseName,
            OutputSchemaName =
                @OutputSchemaName,
            OutputTableName =
                @OutputTableName,
            ExportToExcel =
                @ExportToExcel;

        SELECT
            declared_variables =
                'declared_variables',
            DatabaseId =
                @DatabaseId,
            StartDateUTC =
                @StartDateUTC,
            EndDateUTC =
                @EndDateUTC,
            ProductVersion =
                @ProductVersion,
            ProductVersionMajor =
                @ProductVersionMajor,
            ProductVersionMinor =
                @ProductVersionMinor,
            ObjectFullName =
                @ObjectFullName,
            Azure =
                @Azure,
            RDS =
                @RDS,
            d =
                @d,
            StringToExecute =
                @StringToExecute,
            StringToExecuteParams =
                @StringToExecuteParams,
            r =
                @r,
            OutputTableFindings =
                @OutputTableFindings,
            DeadlockCount =
                @DeadlockCount,
            ServerName =
                @ServerName,
            OutputDatabaseCheck =
                @OutputDatabaseCheck,
            SessionId =
                @SessionId,
            TargetSessionId =
                @TargetSessionId,
            FileName =
                @FileName,
            inputbuf_bom =
                @inputbuf_bom,
            deadlock_result =
                @deadlock_result;
        END; /*End debug*/
    END; /*Final End*/
GO
