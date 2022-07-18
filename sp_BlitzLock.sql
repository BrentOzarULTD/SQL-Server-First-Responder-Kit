IF OBJECT_ID('dbo.sp_BlitzLock') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzLock AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_BlitzLock
(
    @Top INT = 2147483647, 
	@DatabaseName NVARCHAR(256) = NULL,
	@StartDate DATETIME = '19000101', 
	@EndDate DATETIME = '99991231', 
	@ObjectName NVARCHAR(1000) = NULL,
	@StoredProcName NVARCHAR(1000) = NULL,
	@AppName NVARCHAR(256) = NULL,
	@HostName NVARCHAR(256) = NULL,
	@LoginName NVARCHAR(256) = NULL,
	@EventSessionPath VARCHAR(256) = 'system_health*.xel',
	@VictimsOnly BIT = 0,
	@Debug BIT = 0, 
	@Help BIT = 0,
	@Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0,
	@OutputDatabaseName NVARCHAR(256) = NULL ,
    @OutputSchemaName NVARCHAR(256) = 'dbo' ,  --ditto as below
    @OutputTableName NVARCHAR(256) = 'BlitzLock',  --put a standard here no need to check later in the script
    @ExportToExcel BIT = 0
)
WITH RECOMPILE
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
	/*
	sp_BlitzLock from http://FirstResponderKit.org
	
	This script checks for and analyzes deadlocks from the system health session or a custom extended event path

	Variables you can use:
		@Top: Use if you want to limit the number of deadlocks to return.
			  This is ordered by event date ascending

		@DatabaseName: If you want to filter to a specific database

		@StartDate: The date you want to start searching on.

		@EndDate: The date you want to stop searching on.

		@ObjectName: If you want to filter to a specific able. 
					 The object name has to be fully qualified ''Database.Schema.Table''

		@StoredProcName: If you want to search for a single stored proc
					 The proc name has to be fully qualified ''Database.Schema.Sproc''
		
		@AppName: If you want to filter to a specific application
		
		@HostName: If you want to filter to a specific host
		
		@LoginName: If you want to filter to a specific login

		@EventSessionPath: If you want to point this at an XE session rather than the system health session.
	
		@OutputDatabaseName: If you want to output information to a specific database
		@OutputSchemaName: Specify a schema name to output information to a specific Schema
		@OutputTableName: Specify table name to to output information to a specific table
	
	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - Only SQL Server 2012 and newer is supported
	 - If your tables have weird characters in them (https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references) you may get errors trying to parse the XML.
	   I took a long look at this one, and:
		1) Trying to account for all the weird places these could crop up is a losing effort. 
		2) Replace is slow af on lots of XML.




	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)


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

	*/';
	RETURN;
	END;    /* @Help = 1 */
	
        DECLARE @ProductVersion NVARCHAR(128), 
			@ProductVersionMajor FLOAT,
			@ProductVersionMinor INT,
			@ObjectFullName NVARCHAR(2000);


        SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));

        SELECT @ProductVersionMajor = SUBSTRING(@ProductVersion, 1, CHARINDEX('.', @ProductVersion) + 1),
               @ProductVersionMinor = PARSENAME(CONVERT(VARCHAR(32), @ProductVersion), 2);


        IF @ProductVersionMajor < 11.0
            BEGIN
                RAISERROR(
                    'sp_BlitzLock will throw a bunch of angry errors on versions of SQL Server earlier than 2012.',
                    0,
                    1) WITH NOWAIT;
                RETURN;
            END;
        
        IF ((SELECT SERVERPROPERTY ('EDITION')) = 'SQL Azure'
             AND 
             LOWER(@EventSessionPath) NOT LIKE 'http%')
            BEGIN
                    RAISERROR(
                    'The default storage path doesn''t work in Azure SQLDB/Managed instances.
You need to use an Azure storage account, and the path has to look like this: https://StorageAccount.blob.core.windows.net/Container/FileName.xel',
                    0,
                    1) WITH NOWAIT;
                RETURN;
            END;


		IF @Top IS NULL
			SET @Top = 2147483647;

		IF @StartDate IS NULL
			SET @StartDate = '19000101';

		IF @EndDate IS NULL
			SET @EndDate = '99991231';
		

        IF OBJECT_ID('tempdb..#deadlock_data') IS NOT NULL
            DROP TABLE #deadlock_data;

        IF OBJECT_ID('tempdb..#deadlock_process') IS NOT NULL
            DROP TABLE #deadlock_process;

        IF OBJECT_ID('tempdb..#deadlock_stack') IS NOT NULL
            DROP TABLE #deadlock_stack;

        IF OBJECT_ID('tempdb..#deadlock_resource') IS NOT NULL
            DROP TABLE #deadlock_resource;

        IF OBJECT_ID('tempdb..#deadlock_owner_waiter') IS NOT NULL
            DROP TABLE #deadlock_owner_waiter;

        IF OBJECT_ID('tempdb..#deadlock_findings') IS NOT NULL
            DROP TABLE #deadlock_findings;

		CREATE TABLE #deadlock_findings
		(
		    id INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
		    check_id INT NOT NULL,
			database_name NVARCHAR(256),
			object_name NVARCHAR(1000),
			finding_group NVARCHAR(100),
			finding NVARCHAR(4000)
		);

		DECLARE @d VARCHAR(40), @StringToExecute NVARCHAR(4000),@StringToExecuteParams NVARCHAR(500),@r NVARCHAR(200),@OutputTableFindings NVARCHAR(100),@DeadlockCount INT;
		DECLARE @ServerName NVARCHAR(256)
		DECLARE @OutputDatabaseCheck BIT;
		SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
		SET @OutputTableFindings = '[BlitzLockFindings]'
		SET @ServerName = (select @@ServerName)
		if(@OutputDatabaseName is not null)
		BEGIN --if databaseName is set do some sanity checks and put [] around def.
			if( (select name from sys.databases where name=@OutputDatabaseName) is null ) --if database is invalid raiserror and set bitcheck
				BEGIN
					RAISERROR('Database Name for output of table is invalid please correct, Output to Table will not be preformed', 0, 1, @d) WITH NOWAIT;	
					set @OutputDatabaseCheck = -1 -- -1 invalid/false, 0 = good/true
				END
			ELSE
				BEGIN
					set @OutputDatabaseCheck = 0
					select @StringToExecute = N'select @r = name from ' + '' + @OutputDatabaseName + 
					'' + '.sys.objects where type_desc=''USER_TABLE'' and name=' + '''' + @OutputTableName + '''',
					@StringToExecuteParams = N'@OutputDatabaseName NVARCHAR(200),@OutputTableName NVARCHAR(200),@r NVARCHAR(200) OUTPUT'
					exec sp_executesql @StringToExecute,@StringToExecuteParams,@OutputDatabaseName,@OutputTableName,@r OUTPUT
					--put covers around all before.
					SELECT @OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
					@OutputTableName = QUOTENAME(@OutputTableName), 
					@OutputSchemaName = QUOTENAME(@OutputSchemaName) 
					if(@r is not null) --if it is not null, there is a table, so check for newly added columns
					BEGIN
						/* If the table doesn't have the new spid column, add it. See Github #3101. */
						SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
						SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
							WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + ''')) AND name = ''spid'')
							ALTER TABLE ' + @ObjectFullName + N' ADD spid SMALLINT NULL;';
						EXEC(@StringToExecute);

						/* If the table doesn't have the new wait_resource column, add it. See Github #3101. */
						SET @ObjectFullName = @OutputDatabaseName + N'.' + @OutputSchemaName + N'.' +  @OutputTableName;
						SET @StringToExecute = N'IF NOT EXISTS (SELECT * FROM ' + @OutputDatabaseName + N'.sys.all_columns 
							WHERE object_id = (OBJECT_ID(''' + @ObjectFullName + ''')) AND name = ''wait_resource'')
							ALTER TABLE ' + @ObjectFullName + N' ADD wait_resource NVARCHAR(MAX) NULL;';
						EXEC(@StringToExecute);
					END
					ELSE --if(@r is not null) --if it is null there is no table, create it from above execution
					BEGIN
						select @StringToExecute = N'use ' + @OutputDatabaseName + ';create table ' + @OutputSchemaName + '.' + @OutputTableName + ' (
							ServerName NVARCHAR(256),
							deadlock_type NVARCHAR(256),
							event_date datetime,
							database_name NVARCHAR(256),
							spid SMALLINT,
							deadlock_group NVARCHAR(256),
							query XML,
							object_names XML,
							isolation_level NVARCHAR(256),
							owner_mode NVARCHAR(256),
							waiter_mode NVARCHAR(256),
							transaction_count bigint,
							login_name NVARCHAR(256),
							host_name NVARCHAR(256),
							client_app NVARCHAR(256),
							wait_time BIGINT,
							wait_resource NVARCHAR(max),
							priority smallint,
							log_used BIGINT,
							last_tran_started datetime,
							last_batch_started datetime,
							last_batch_completed datetime,
							transaction_name NVARCHAR(256),
							owner_waiter_type NVARCHAR(256),
							owner_activity NVARCHAR(256),
							owner_waiter_activity NVARCHAR(256),
							owner_merging NVARCHAR(256),
							owner_spilling NVARCHAR(256),
							owner_waiting_to_close NVARCHAR(256),
							waiter_waiter_type NVARCHAR(256),
							waiter_owner_activity NVARCHAR(256),
							waiter_waiter_activity NVARCHAR(256),
							waiter_merging NVARCHAR(256),
							waiter_spilling NVARCHAR(256),
							waiter_waiting_to_close NVARCHAR(256),
							deadlock_graph XML)',
							@StringToExecuteParams = N'@OutputDatabaseName NVARCHAR(200),@OutputSchemaName NVARCHAR(100),@OutputTableName NVARCHAR(200)'
							exec sp_executesql @StringToExecute, @StringToExecuteParams,@OutputDatabaseName,@OutputSchemaName,@OutputTableName
							--table created.
							select @StringToExecute = N'select @r = name from ' + '' + @OutputDatabaseName + 
								'' + '.sys.objects where type_desc=''USER_TABLE'' and name=''BlitzLockFindings''',
							@StringToExecuteParams = N'@OutputDatabaseName NVARCHAR(200),@r NVARCHAR(200) OUTPUT'
							exec sp_executesql @StringToExecute,@StringToExecuteParams,@OutputDatabaseName,@r OUTPUT
							if(@r is null) --if table does not excist
							BEGIN
								select @OutputTableFindings=N'[BlitzLockFindings]',
								@StringToExecute = N'use ' + @OutputDatabaseName + ';create table ' + @OutputSchemaName + '.' + @OutputTableFindings + ' (
								ServerName NVARCHAR(256),
								check_id INT, 
								database_name NVARCHAR(256), 
								object_name NVARCHAR(1000), 
								finding_group NVARCHAR(100), 
								finding NVARCHAR(4000))',
								@StringToExecuteParams = N'@OutputDatabaseName NVARCHAR(200),@OutputSchemaName NVARCHAR(100),@OutputTableFindings NVARCHAR(200)'
								exec sp_executesql @StringToExecute, @StringToExecuteParams, @OutputDatabaseName,@OutputSchemaName,@OutputTableFindings
					
							END

					END
							--create synonym for deadlockfindings.
							if((select name from sys.objects where name='DeadlockFindings' and type_desc='SYNONYM')IS NOT NULL)
							BEGIN
								RAISERROR('found synonym', 0, 1) WITH NOWAIT;
								drop synonym DeadlockFindings;
							END
							set @StringToExecute = 'CREATE SYNONYM DeadlockFindings FOR ' + @OutputDatabaseName + '.' + @OutputSchemaName + '.' + @OutputTableFindings; 
							exec sp_executesql @StringToExecute
							
							--create synonym for deadlock table.
							if((select name from sys.objects where name='DeadLockTbl' and type_desc='SYNONYM') IS NOT NULL)
							BEGIN	
								drop SYNONYM DeadLockTbl;
							END
							set @StringToExecute = 'CREATE SYNONYM DeadLockTbl FOR ' + @OutputDatabaseName + '.' + @OutputSchemaName + '.' + @OutputTableName; 
							exec sp_executesql @StringToExecute
					
				END
		END
        

        CREATE TABLE #t (id INT NOT NULL);

		/* WITH ROWCOUNT doesn't work on Amazon RDS - see: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2037 */
		IF LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR(8000)), 8) <> 'EC2AMAZ-'
		   AND LEFT(CAST(SERVERPROPERTY('MachineName') AS VARCHAR(8000)), 8) <> 'EC2AMAZ-'
		   AND LEFT(CAST(SERVERPROPERTY('ServerName') AS VARCHAR(8000)), 8) <> 'EC2AMAZ-'
		   AND db_id('rdsadmin') IS NULL
		   BEGIN;
				BEGIN TRY;
					UPDATE STATISTICS #t WITH ROWCOUNT = 100000000, PAGECOUNT = 100000000;
				END TRY
				BEGIN CATCH;
					/* Misleading error returned, if run without permissions to update statistics the error returned is "Cannot find object".
					   Catching specific error, and returning message with better info. If any other error is returned, then throw as normal */
					IF (ERROR_NUMBER() = 1088)
					BEGIN;
						SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
						RAISERROR('Cannot run UPDATE STATISTICS on temp table without db_owner or sysadmin permissions %s', 0, 1, @d) WITH NOWAIT;
					END;
					ELSE
					BEGIN;
						THROW;
					END;
				END CATCH;
			END;

		/*Grab the initial set of XML to parse*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Grab the initial set of XML to parse at %s', 0, 1, @d) WITH NOWAIT;
        WITH xml
        AS ( SELECT CONVERT(XML, event_data) AS deadlock_xml
             FROM   sys.fn_xe_file_target_read_file(@EventSessionPath, NULL, NULL, NULL) )
        SELECT ISNULL(xml.deadlock_xml, '') AS deadlock_xml
        INTO   #deadlock_data
        FROM   xml
        LEFT JOIN #t AS t
        ON 1 = 1
        CROSS APPLY xml.deadlock_xml.nodes('/event/@name') AS x(c)
        WHERE  1 = 1 
        AND    x.c.exist('/event/@name[ .= "xml_deadlock_report"]') = 1
        AND    CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') ), DATENAME(TzOffset, SYSDATETIMEOFFSET())))  > @StartDate
        AND    CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') ), DATENAME(TzOffset, SYSDATETIMEOFFSET())))  < @EndDate
		ORDER BY xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') DESC
		OPTION ( RECOMPILE );

		/*Optimization: if we got back more rows than @Top, remove them. This seems to be better than using @Top in the query above as that results in excessive memory grant*/
		SET @DeadlockCount = @@ROWCOUNT
		IF( @Top < @DeadlockCount ) BEGIN
			WITH T
			AS (
				SELECT TOP ( @DeadlockCount - @Top) *
				FROM   #deadlock_data
				ORDER  BY #deadlock_data.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') ASC)
			DELETE FROM T 
		END

		/*Parse process and input buffer XML*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse process and input buffer XML %s', 0, 1, @d) WITH NOWAIT;
        SELECT      q.event_date,
                    q.victim_id,
					CONVERT(BIT, q.is_parallel) AS is_parallel,
                    q.deadlock_graph,
                    q.id,
                    q.spid,
                    q.database_id,
                    q.priority,
                    q.log_used,
                    q.wait_resource,
                    q.wait_time,
                    q.transaction_name,
                    q.last_tran_started,
                    q.last_batch_started,
                    q.last_batch_completed,
                    q.lock_mode,
                    q.transaction_count,
                    q.client_app,
                    q.host_name,
                    q.login_name,
                    q.isolation_level,
                    q.process_xml,
                    ISNULL(ca2.ib.query('.'), '') AS input_buffer
        INTO        #deadlock_process
        FROM        (   SELECT      dd.deadlock_xml,
                                    CONVERT(DATETIME2(7), SWITCHOFFSET(CONVERT(datetimeoffset, dd.event_date ), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS event_date,
                                    dd.victim_id,
									CONVERT(tinyint, dd.is_parallel) + CONVERT(tinyint, dd.is_parallel_batch) AS is_parallel,
                                    dd.deadlock_graph,
                                    ca.dp.value('@id', 'NVARCHAR(256)') AS id,
									ca.dp.value('@spid', 'SMALLINT') AS spid,
                                    ca.dp.value('@currentdb', 'BIGINT') AS database_id,
                                    ca.dp.value('@priority', 'SMALLINT') AS priority,
                                    ca.dp.value('@logused', 'BIGINT') AS log_used,
                                    ca.dp.value('@waitresource', 'NVARCHAR(256)') AS wait_resource,
                                    ca.dp.value('@waittime', 'BIGINT') AS wait_time,
                                    ca.dp.value('@transactionname', 'NVARCHAR(256)') AS transaction_name,
                                    ca.dp.value('@lasttranstarted', 'DATETIME2(7)') AS last_tran_started,
                                    ca.dp.value('@lastbatchstarted', 'DATETIME2(7)') AS last_batch_started,
                                    ca.dp.value('@lastbatchcompleted', 'DATETIME2(7)') AS last_batch_completed,
                                    ca.dp.value('@lockMode', 'NVARCHAR(256)') AS lock_mode,
                                    ca.dp.value('@trancount', 'BIGINT') AS transaction_count,
                                    ca.dp.value('@clientapp', 'NVARCHAR(256)') AS client_app,
                                    ca.dp.value('@hostname', 'NVARCHAR(256)') AS host_name,
                                    ca.dp.value('@loginname', 'NVARCHAR(256)') AS login_name,
                                    ca.dp.value('@isolationlevel', 'NVARCHAR(256)') AS isolation_level,
                                    ISNULL(ca.dp.query('.'), '') AS process_xml
                        FROM        (   SELECT d1.deadlock_xml,
                                               d1.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2') AS event_date,
                                               d1.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'NVARCHAR(256)') AS victim_id,
											   d1.deadlock_xml.exist('//deadlock/resource-list/exchangeEvent') AS is_parallel,
											   d1.deadlock_xml.exist('//deadlock/resource-list/SyncPoint') AS is_parallel_batch,
                                               d1.deadlock_xml.query('/event/data/value/deadlock') AS deadlock_graph
                                        FROM   #deadlock_data AS d1 ) AS dd
                        CROSS APPLY dd.deadlock_xml.nodes('//deadlock/process-list/process') AS ca(dp)
        WHERE (ca.dp.value('@currentdb', 'BIGINT') = DB_ID(@DatabaseName) OR @DatabaseName IS NULL) 
        AND   (ca.dp.value('@clientapp', 'NVARCHAR(256)') = @AppName OR @AppName IS NULL) 
        AND   (ca.dp.value('@hostname', 'NVARCHAR(256)') = @HostName OR @HostName IS NULL) 
        AND   (ca.dp.value('@loginname', 'NVARCHAR(256)') = @LoginName OR @LoginName IS NULL) 
        ) AS q
        CROSS APPLY q.deadlock_xml.nodes('//deadlock/process-list/process/inputbuf') AS ca2(ib)
		OPTION ( RECOMPILE );


		/*Parse execution stack XML*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse execution stack XML %s', 0, 1, @d) WITH NOWAIT;
        SELECT      DISTINCT 
		            dp.id,
					dp.event_date,
                    ca.dp.value('@procname', 'NVARCHAR(1000)') AS proc_name,
                    ca.dp.value('@sqlhandle', 'NVARCHAR(128)') AS sql_handle
        INTO        #deadlock_stack
        FROM        #deadlock_process AS dp
        CROSS APPLY dp.process_xml.nodes('//executionStack/frame') AS ca(dp)
		WHERE (ca.dp.value('@procname', 'NVARCHAR(256)') = @StoredProcName OR @StoredProcName IS NULL)
		OPTION ( RECOMPILE );



		/*Grab the full resource list*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Grab the full resource list %s', 0, 1, @d) WITH NOWAIT;
        SELECT 
                    CONVERT(DATETIME2(7), SWITCHOFFSET(CONVERT(datetimeoffset, dr.event_date), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS event_date,
                    dr.victim_id, 
                    dr.resource_xml
        INTO        #deadlock_resource
        FROM 
        (
        SELECT      dd.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2') AS event_date,
					dd.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'NVARCHAR(256)') AS victim_id,
					ISNULL(ca.dp.query('.'), '') AS resource_xml
        FROM        #deadlock_data AS dd
        CROSS APPLY dd.deadlock_xml.nodes('//deadlock/resource-list') AS ca(dp)
        ) AS dr
		OPTION ( RECOMPILE );


		/*Parse object locks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse object locks %s', 0, 1, @d) WITH NOWAIT;
        SELECT      DISTINCT 
		            ca.event_date,
					ca.database_id,
					ca.object_name,
					ca.lock_mode,
					ca.index_name,
					ca.associatedObjectId,
				    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode,
					N'OBJECT' AS lock_type
        INTO        #deadlock_owner_waiter
		FROM (
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
					ca.dr.value('@associatedObjectId', 'BIGINT') AS associatedObjectId,
					ca.dr.query('.') AS dr
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/objectlock') AS ca(dr)
		) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		WHERE (ca.object_name = @ObjectName OR @ObjectName IS NULL)
		OPTION ( RECOMPILE );



		/*Parse page locks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse page locks %s', 0, 1, @d) WITH NOWAIT;
        INSERT #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT      DISTINCT 
		            ca.event_date,
					ca.database_id,
					ca.object_name,
					ca.lock_mode,
					ca.index_name,
					ca.associatedObjectId,
				    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode,
					N'PAGE' AS lock_type
		FROM (
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
					ca.dr.value('@associatedObjectId', 'BIGINT') AS associatedObjectId,
					ca.dr.query('.') AS dr
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/pagelock') AS ca(dr)
		) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*Parse key locks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse key locks %s', 0, 1, @d) WITH NOWAIT;
        INSERT #deadlock_owner_waiter WITH(TABLOCKX) 
        SELECT      DISTINCT 
		            ca.event_date,
					ca.database_id,
					ca.object_name,
					ca.lock_mode,
					ca.index_name,
					ca.associatedObjectId,
				    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode,
					N'KEY' AS lock_type
		FROM (
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
					ca.dr.value('@associatedObjectId', 'BIGINT') AS associatedObjectId,
					ca.dr.query('.') AS dr
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/keylock') AS ca(dr)
		) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*Parse RID locks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse RID locks %s', 0, 1, @d) WITH NOWAIT;
        INSERT #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT      DISTINCT 
		            ca.event_date,
					ca.database_id,
					ca.object_name,
					ca.lock_mode,
					ca.index_name,
					ca.associatedObjectId,
				    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode,
					N'RID' AS lock_type
		FROM (
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
					ca.dr.value('@associatedObjectId', 'BIGINT') AS associatedObjectId,
					ca.dr.query('.') AS dr
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/ridlock') AS ca(dr)
		) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*Parse row group locks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Parse row group locks %s', 0, 1, @d) WITH NOWAIT;
        INSERT #deadlock_owner_waiter WITH(TABLOCKX)
        SELECT      DISTINCT 
		            ca.event_date,
					ca.database_id,
					ca.object_name,
					ca.lock_mode,
					ca.index_name,
					ca.associatedObjectId,
				    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode,
					N'ROWGROUP' AS lock_type
		FROM (
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
					ca.dr.value('@associatedObjectId', 'BIGINT') AS associatedObjectId,
					ca.dr.query('.') AS dr
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/rowgrouplock') AS ca(dr)
		) AS ca
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );

		UPDATE d
		    SET	d.index_name = d.object_name
							   + '.HEAP'
		FROM #deadlock_owner_waiter AS d
		WHERE lock_type IN (N'HEAP', N'RID')
		OPTION(RECOMPILE);

		/*Parse parallel deadlocks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
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
 	   		  w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
              o.l.value('@id', 'NVARCHAR(256)') AS owner_id
 	   INTO #deadlock_resource_parallel
 	   FROM (
        SELECT		dr.event_date,
 					ca.dr.value('@id', 'NVARCHAR(256)') AS id,
                    ca.dr.value('@WaitType', 'NVARCHAR(256)') AS wait_type,
                    ca.dr.value('@nodeId', 'BIGINT') AS node_id,
 					/* These columns are in 2017 CU5 ONLY */
 					ca.dr.value('@waiterType', 'NVARCHAR(256)') AS waiter_type,
 					ca.dr.value('@ownerActivity', 'NVARCHAR(256)') AS owner_activity,
 					ca.dr.value('@waiterActivity', 'NVARCHAR(256)') AS waiter_activity,
 					ca.dr.value('@merging', 'NVARCHAR(256)') AS merging,
 					ca.dr.value('@spilling', 'NVARCHAR(256)') AS spilling,
 					ca.dr.value('@waitingToClose', 'NVARCHAR(256)') AS waiting_to_close,
                     /*                                    */      
 					ca.dr.query('.') AS dr
 		FROM        #deadlock_resource AS dr
         CROSS APPLY dr.resource_xml.nodes('//resource-list/exchangeEvent') AS ca(dr)
 		) AS ca
         CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
         CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
 		OPTION ( RECOMPILE );


		/*Get rid of parallel noise*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Get rid of parallel noise %s', 0, 1, @d) WITH NOWAIT;
		WITH c
		    AS
		     (
		         SELECT *, ROW_NUMBER() OVER ( PARTITION BY drp.owner_id, drp.waiter_id ORDER BY drp.event_date ) AS rn
		         FROM   #deadlock_resource_parallel AS drp
		     )
		DELETE FROM c
		WHERE c.rn > 1
		OPTION ( RECOMPILE );


		/*Get rid of nonsense*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Get rid of nonsense %s', 0, 1, @d) WITH NOWAIT;
		DELETE dow
		FROM #deadlock_owner_waiter AS dow
		WHERE dow.owner_id = dow.waiter_id
		OPTION ( RECOMPILE );

		/*Add some nonsense*/
		ALTER TABLE #deadlock_process
		ADD waiter_mode	NVARCHAR(256),
			owner_mode NVARCHAR(256),
			is_victim AS CONVERT(BIT, CASE WHEN id = victim_id THEN 1 ELSE 0 END);

		/*Update some nonsense*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Update some nonsense part 1 %s', 0, 1, @d) WITH NOWAIT;
		UPDATE dp
		SET dp.owner_mode = dow.owner_mode
		FROM #deadlock_process AS dp
		JOIN #deadlock_owner_waiter AS dow
		ON dp.id = dow.owner_id
		AND dp.event_date = dow.event_date
		WHERE dp.is_victim = 0
		OPTION ( RECOMPILE );

        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Update some nonsense part 2 %s', 0, 1, @d) WITH NOWAIT;
		UPDATE dp
		SET dp.waiter_mode = dow.waiter_mode
		FROM #deadlock_process AS dp
		JOIN #deadlock_owner_waiter AS dow
		ON dp.victim_id = dow.waiter_id
		AND dp.event_date = dow.event_date
		WHERE dp.is_victim = 1
		OPTION ( RECOMPILE );

		/*Get Agent Job and Step names*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Get Agent Job and Step names %s', 0, 1, @d) WITH NOWAIT;
        SELECT *,
               CONVERT(UNIQUEIDENTIFIER, 
                           CONVERT(XML, '').value('xs:hexBinary(substring(sql:column("x.job_id"), 0) )', 'BINARY(16)')
                      ) AS job_id_guid
        INTO #agent_job
        FROM (
            SELECT dp.event_date,
                   dp.victim_id,
                   dp.id,
                   dp.database_id,
                   dp.client_app,
                   SUBSTRING(dp.client_app, 
                             CHARINDEX('0x', dp.client_app) + LEN('0x'), 
                             32
                             ) AS job_id,
		    	   SUBSTRING(dp.client_app, 
                             CHARINDEX(': Step ', dp.client_app) + LEN(': Step '), 
                             CHARINDEX(')', dp.client_app, CHARINDEX(': Step ', dp.client_app)) 
                                 - (CHARINDEX(': Step ', dp.client_app) 
                                     + LEN(': Step '))
                             ) AS step_id
            FROM #deadlock_process AS dp
            WHERE dp.client_app LIKE 'SQLAgent - %'
			AND   dp.client_app <> 'SQLAgent - Initial Boot Probe'
        ) AS x
		OPTION ( RECOMPILE );


        ALTER TABLE #agent_job ADD job_name NVARCHAR(256),
                                   step_name NVARCHAR(256);

        IF SERVERPROPERTY('EngineEdition') NOT IN (5, 6) /* Azure SQL DB doesn't support querying jobs */
		  AND NOT (LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR(8000)), 8) = 'EC2AMAZ-'   /* Neither does Amazon RDS Express Edition */
					AND LEFT(CAST(SERVERPROPERTY('MachineName') AS VARCHAR(8000)), 8) = 'EC2AMAZ-'
					AND LEFT(CAST(SERVERPROPERTY('ServerName') AS VARCHAR(8000)), 8) = 'EC2AMAZ-'
					AND db_id('rdsadmin') IS NOT NULL
					AND EXISTS(SELECT * FROM master.sys.all_objects WHERE name IN ('rds_startup_tasks', 'rds_help_revlogin', 'rds_hexadecimal', 'rds_failover_tracking', 'rds_database_tracking', 'rds_track_change'))
		   		)
            BEGIN
            SET @StringToExecute = N'UPDATE aj
                    SET  aj.job_name = j.name, 
                         aj.step_name = s.step_name
		            FROM msdb.dbo.sysjobs AS j
		            JOIN msdb.dbo.sysjobsteps AS s 
                        ON j.job_id = s.job_id
                    JOIN #agent_job AS aj
                        ON  aj.job_id_guid = j.job_id
                        AND aj.step_id = s.step_id
						OPTION ( RECOMPILE );';
            EXEC(@StringToExecute);
            END

        UPDATE dp
           SET dp.client_app = 
               CASE WHEN dp.client_app LIKE N'SQLAgent - %'
                    THEN N'SQLAgent - Job: ' 
                         + aj.job_name
                         + N' Step: '
                         + aj.step_name
                    ELSE dp.client_app
               END  
        FROM #deadlock_process AS dp
        JOIN #agent_job AS aj
        ON dp.event_date = aj.event_date
        AND dp.victim_id = aj.victim_id
        AND dp.id = aj.id
		OPTION ( RECOMPILE );

		/*Get each and every table of all databases*/
		DECLARE @sysAssObjId AS TABLE (database_id bigint, partition_id bigint, schema_name varchar(255), table_name varchar(255));
		INSERT into @sysAssObjId EXECUTE sp_MSforeachdb
		  N'USE [?];
		    SELECT DB_ID() as database_id, p.partition_id, s.name as schema_name, t.name as table_name
			FROM sys.partitions p
			LEFT JOIN sys.tables t ON t.object_id = p.object_id 
			LEFT JOIN sys.schemas s ON s.schema_id = t.schema_id 
			WHERE s.name is not NULL AND t.name is not NULL';


		/*Begin checks based on parsed values*/

		/*Check 1 is deadlocks by database*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 1 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
        ( check_id, database_name, object_name, finding_group, finding ) 	
		SELECT 1 AS check_id, 
			   DB_NAME(dp.database_id) AS database_name, 
			   '-' AS object_name,
			   'Total database locks' AS finding_group,
			   'This database had ' 
				+ CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dp.event_date)) 
				+ ' deadlocks.'
        FROM   #deadlock_process AS dp
		WHERE 1 = 1
		AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dp.event_date < @EndDate OR @EndDate IS NULL)
		AND (dp.client_app = @AppName OR @AppName IS NULL)
		AND (dp.host_name = @HostName OR @HostName IS NULL)
		AND (dp.login_name = @LoginName OR @LoginName IS NULL)
		GROUP BY DB_NAME(dp.database_id)
		OPTION ( RECOMPILE );

		/*Check 2 is deadlocks by object*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 2 objects %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 	
		SELECT 2 AS check_id, 
			   ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') AS database_name, 
			   ISNULL(dow.object_name, 'UNKNOWN') AS object_name,
			   'Total object deadlocks' AS finding_group,
			   'This object was involved in ' 
				+ CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dow.event_date))
				+ ' deadlock(s).'
        FROM   #deadlock_owner_waiter AS dow
		WHERE 1 = 1
		AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dow.event_date < @EndDate OR @EndDate IS NULL)
		AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
		GROUP BY DB_NAME(dow.database_id), dow.object_name
		OPTION ( RECOMPILE );

		/*Check 2 continuation, number of locks per index*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 2 indexes %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 	
		SELECT 2 AS check_id, 
			   ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') AS database_name, 
			   dow.index_name AS index_name,
			   'Total index deadlocks' AS finding_group,
			   'This index was involved in ' 
				+ CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dow.event_date))
				+ ' deadlock(s).'
        FROM   #deadlock_owner_waiter AS dow
		WHERE 1 = 1
		AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dow.event_date < @EndDate OR @EndDate IS NULL)
		AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
		AND dow.lock_type NOT IN (N'HEAP', N'RID')
		AND dow.index_name is not null
		GROUP BY DB_NAME(dow.database_id), dow.index_name
		OPTION ( RECOMPILE );


		/*Check 2 continuation, number of locks per heap*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 2 heaps %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 	
		SELECT 2 AS check_id, 
			   ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') AS database_name, 
			   dow.index_name AS index_name,
			   'Total heap deadlocks' AS finding_group,
			   'This heap was involved in ' 
				+ CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dow.event_date))
				+ ' deadlock(s).'
        FROM   #deadlock_owner_waiter AS dow
		WHERE 1 = 1
		AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dow.event_date < @EndDate OR @EndDate IS NULL)
		AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
		AND dow.lock_type IN (N'HEAP', N'RID')
		GROUP BY DB_NAME(dow.database_id), dow.index_name
		OPTION ( RECOMPILE );
		

		/*Check 3 looks for Serializable locking*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 3 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 3 AS check_id,
			   DB_NAME(dp.database_id) AS database_name,
			   '-' AS object_name,
			   'Serializable locking' AS finding_group,
			   'This database has had ' + 
			   CONVERT(NVARCHAR(20), COUNT_BIG(*)) +
			   ' instances of serializable deadlocks.'
			   AS finding
		FROM #deadlock_process AS dp
		WHERE dp.isolation_level LIKE 'serializable%'
		AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dp.event_date < @EndDate OR @EndDate IS NULL)
		AND (dp.client_app = @AppName OR @AppName IS NULL)
		AND (dp.host_name = @HostName OR @HostName IS NULL)
		AND (dp.login_name = @LoginName OR @LoginName IS NULL)
		GROUP BY DB_NAME(dp.database_id)
		OPTION ( RECOMPILE );


		/*Check 4 looks for Repeatable Read locking*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 4 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 4 AS check_id,
			   DB_NAME(dp.database_id) AS database_name,
			   '-' AS object_name,
			   'Repeatable Read locking' AS finding_group,
			   'This database has had ' + 
			   CONVERT(NVARCHAR(20), COUNT_BIG(*)) +
			   ' instances of repeatable read deadlocks.'
			   AS finding
		FROM #deadlock_process AS dp
		WHERE dp.isolation_level LIKE 'repeatable read%'
		AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dp.event_date < @EndDate OR @EndDate IS NULL)
		AND (dp.client_app = @AppName OR @AppName IS NULL)
		AND (dp.host_name = @HostName OR @HostName IS NULL)
		AND (dp.login_name = @LoginName OR @LoginName IS NULL)
		GROUP BY DB_NAME(dp.database_id)
		OPTION ( RECOMPILE );


		/*Check 5 breaks down app, host, and login information*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 5 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 5 AS check_id,
			   DB_NAME(dp.database_id) AS database_name,
			   '-' AS object_name,
			   'Login, App, and Host locking' AS finding_group,
			   'This database has had ' + 
			   CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dp.event_date)) +
			   ' instances of deadlocks involving the login ' +
			   ISNULL(dp.login_name, 'UNKNOWN') + 
			   ' from the application ' + 
			   ISNULL(dp.client_app, 'UNKNOWN') + 
			   ' on host ' + 
			   ISNULL(dp.host_name, 'UNKNOWN')
			   AS finding
		FROM #deadlock_process AS dp
		WHERE 1 = 1
		AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dp.event_date < @EndDate OR @EndDate IS NULL)
		AND (dp.client_app = @AppName OR @AppName IS NULL)
		AND (dp.host_name = @HostName OR @HostName IS NULL)
		AND (dp.login_name = @LoginName OR @LoginName IS NULL)
		GROUP BY DB_NAME(dp.database_id), dp.login_name, dp.client_app, dp.host_name
		OPTION ( RECOMPILE );


		/*Check 6 breaks down the types of locks (object, page, key, etc.)*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 6 %s', 0, 1, @d) WITH NOWAIT;
		WITH lock_types AS (
				SELECT DB_NAME(dp.database_id) AS database_name,
					   dow.object_name, 
					   SUBSTRING(dp.wait_resource, 1, CHARINDEX(':', dp.wait_resource) -1) AS lock,
					   CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT dp.id)) AS lock_count
				FROM #deadlock_process AS dp 
				JOIN #deadlock_owner_waiter AS dow
				ON dp.id = dow.owner_id
				AND dp.event_date = dow.event_date
				WHERE 1 = 1
				AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
				AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
				AND (dp.event_date < @EndDate OR @EndDate IS NULL)
				AND (dp.client_app = @AppName OR @AppName IS NULL)
				AND (dp.host_name = @HostName OR @HostName IS NULL)
				AND (dp.login_name = @LoginName OR @LoginName IS NULL)
				AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
				AND dow.object_name IS NOT NULL
				GROUP BY DB_NAME(dp.database_id), SUBSTRING(dp.wait_resource, 1, CHARINDEX(':', dp.wait_resource) - 1), dow.object_name
							)	
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT DISTINCT 6 AS check_id,
			   lt.database_name,
			   lt.object_name,
			   'Types of locks by object' AS finding_group,
			   'This object has had ' +
			   STUFF((SELECT DISTINCT N', ' + lt2.lock_count + ' ' + lt2.lock
									FROM lock_types AS lt2
									WHERE lt2.database_name = lt.database_name
									AND lt2.object_name = lt.object_name
									FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
			   + ' locks'
		FROM lock_types AS lt
		OPTION ( RECOMPILE );


		/*Check 7 gives you more info queries for sp_BlitzCache & BlitzQueryStore*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 7 part 1 %s', 0, 1, @d) WITH NOWAIT;
		WITH deadlock_stack AS (
			SELECT  DISTINCT
					ds.id,
					ds.proc_name,
					ds.event_date,
					PARSENAME(ds.proc_name, 3) AS database_name,
					PARSENAME(ds.proc_name, 2) AS schema_name,
					PARSENAME(ds.proc_name, 1) AS proc_only_name,
					'''' + STUFF((SELECT DISTINCT N',' + ds2.sql_handle
									FROM #deadlock_stack AS ds2
									WHERE ds2.id = ds.id
									AND ds2.event_date = ds.event_date
					FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'') + '''' AS sql_handle_csv
			FROM #deadlock_stack AS ds
			GROUP BY PARSENAME(ds.proc_name, 3),
                     PARSENAME(ds.proc_name, 2),
                     PARSENAME(ds.proc_name, 1),
                     ds.id,
                     ds.proc_name,
                     ds.event_date
					)
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT DISTINCT 7 AS check_id,
			   ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') AS database_name,
			   ds.proc_name AS object_name,
			   'More Info - Query' AS finding_group,
			   'EXEC sp_BlitzCache ' +
					CASE WHEN ds.proc_name = 'adhoc'
						 THEN ' @OnlySqlHandles = ' + ds.sql_handle_csv
						 ELSE '@StoredProcName = ' + 
						       QUOTENAME(ds.proc_only_name, '''')
					END +
					';' AS finding
		FROM deadlock_stack AS ds
		JOIN #deadlock_owner_waiter AS dow
		ON dow.owner_id = ds.id
		AND dow.event_date = ds.event_date
		WHERE 1 = 1
		AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dow.event_date < @EndDate OR @EndDate IS NULL)
		AND (dow.object_name = @StoredProcName OR @StoredProcName IS NULL)
		OPTION ( RECOMPILE );

		IF @ProductVersionMajor >= 13
		BEGIN
		SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 7 part 2 %s', 0, 1, @d) WITH NOWAIT;
		WITH deadlock_stack AS (
			SELECT  DISTINCT
					ds.id,
					ds.sql_handle,
					ds.proc_name,
					ds.event_date,
					PARSENAME(ds.proc_name, 3) AS database_name,
					PARSENAME(ds.proc_name, 2) AS schema_name,
					PARSENAME(ds.proc_name, 1) AS proc_only_name
			FROM #deadlock_stack AS ds	
					)
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT DISTINCT 7 AS check_id,
			   DB_NAME(dow.database_id) AS database_name,
			   ds.proc_name AS object_name,
			   'More Info - Query' AS finding_group,
			   'EXEC sp_BlitzQueryStore ' 
			   + '@DatabaseName = ' 
			   + QUOTENAME(ds.database_name, '''')
			   + ', '
			   + '@StoredProcName = ' 
			   + QUOTENAME(ds.proc_only_name, '''')
			   + ';' AS finding
		FROM deadlock_stack AS ds
		JOIN #deadlock_owner_waiter AS dow
		ON dow.owner_id = ds.id
		AND dow.event_date = ds.event_date
		WHERE ds.proc_name <> 'adhoc'
		AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dow.event_date < @EndDate OR @EndDate IS NULL)
		AND (dow.object_name = @StoredProcName OR @StoredProcName IS NULL)
		OPTION ( RECOMPILE );
		END;
		

		/*Check 8 gives you stored proc deadlock counts*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 8 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding )
		SELECT 8 AS check_id,
			   DB_NAME(dp.database_id) AS database_name,
			   ds.proc_name, 
			   'Stored Procedure Deadlocks',
			   'The stored procedure ' 
			   + PARSENAME(ds.proc_name, 2)
			   + '.'
			   + PARSENAME(ds.proc_name, 1)
			   + ' has been involved in '
			   + CONVERT(NVARCHAR(10), COUNT_BIG(DISTINCT ds.id))
			   + ' deadlocks.'
		FROM #deadlock_stack AS ds
		JOIN #deadlock_process AS dp
		ON dp.id = ds.id
		AND ds.event_date = dp.event_date
		WHERE ds.proc_name <> 'adhoc'
		AND (ds.proc_name = @StoredProcName OR @StoredProcName IS NULL)
		AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
		AND (dp.event_date < @EndDate OR @EndDate IS NULL)
		AND (dp.client_app = @AppName OR @AppName IS NULL)
		AND (dp.host_name = @HostName OR @HostName IS NULL)
		AND (dp.login_name = @LoginName OR @LoginName IS NULL)
		GROUP BY DB_NAME(dp.database_id), ds.proc_name
		OPTION(RECOMPILE);


		/*Check 9 gives you more info queries for sp_BlitzIndex */
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 9 %s', 0, 1, @d) WITH NOWAIT;
		WITH bi AS (
				SELECT  DISTINCT
					dow.object_name,
					DB_NAME(dow.database_id) as database_name,
					a.schema_name AS schema_name,
					a.table_name AS table_name
				FROM #deadlock_owner_waiter AS dow
				LEFT JOIN @sysAssObjId a ON a.database_id=dow.database_id AND a.partition_id = dow.associatedObjectId
				WHERE 1 = 1
				AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
				AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
				AND (dow.event_date < @EndDate OR @EndDate IS NULL)
				AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
				AND dow.object_name IS NOT NULL
		)
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 9 AS check_id,	
				bi.database_name,
				bi.object_name,
				'More Info - Table' AS finding_group,
				'EXEC sp_BlitzIndex ' + 
				'@DatabaseName = ' + QUOTENAME(bi.database_name, '''') + 
				', @SchemaName = ' + QUOTENAME(bi.schema_name, '''') + 
				', @TableName = ' + QUOTENAME(bi.table_name, '''') +
				';'	 AS finding
		FROM bi
		OPTION ( RECOMPILE );

		/*Check 10 gets total deadlock wait time per object*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 10 %s', 0, 1, @d) WITH NOWAIT;
		WITH chopsuey AS (
				SELECT DISTINCT
				PARSENAME(dow.object_name, 3) AS database_name,
				dow.object_name,
				CONVERT(VARCHAR(10), (SUM(DISTINCT CONVERT(BIGINT, dp.wait_time)) / 1000) / 86400) AS wait_days,
				CONVERT(VARCHAR(20), DATEADD(SECOND, (SUM(DISTINCT CONVERT(BIGINT, dp.wait_time)) / 1000), 0), 108) AS wait_time_hms
				FROM #deadlock_owner_waiter AS dow
				JOIN #deadlock_process AS dp
				ON (dp.id = dow.owner_id OR dp.victim_id = dow.waiter_id)
					AND dp.event_date = dow.event_date
				WHERE 1 = 1
				AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
				AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
				AND (dow.event_date < @EndDate OR @EndDate IS NULL)
				AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
				AND (dp.client_app = @AppName OR @AppName IS NULL)
				AND (dp.host_name = @HostName OR @HostName IS NULL)
				AND (dp.login_name = @LoginName OR @LoginName IS NULL)
				GROUP BY PARSENAME(dow.object_name, 3), dow.object_name
						)
				INSERT #deadlock_findings WITH (TABLOCKX) 
                ( check_id, database_name, object_name, finding_group, finding ) 
				SELECT 10 AS check_id,
						cs.database_name,
						cs.object_name,
						'Total object deadlock wait time' AS finding_group,
						'This object has had ' 
						+ CONVERT(VARCHAR(10), cs.wait_days) 
						+ ':' + CONVERT(VARCHAR(20), cs.wait_time_hms, 108)
						+ ' [d/h/m/s] of deadlock wait time.' AS finding
				FROM chopsuey AS cs
				WHERE cs.object_name IS NOT NULL
				OPTION ( RECOMPILE );

		/*Check 11 gets total deadlock wait time per database*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 11 %s', 0, 1, @d) WITH NOWAIT;
		WITH wait_time AS (
						SELECT DB_NAME(dp.database_id) AS database_name,
							   SUM(CONVERT(BIGINT, dp.wait_time)) AS total_wait_time_ms
						FROM #deadlock_process AS dp
						WHERE 1 = 1
						AND (DB_NAME(dp.database_id) = @DatabaseName OR @DatabaseName IS NULL)
						AND (dp.event_date >= @StartDate OR @StartDate IS NULL)
						AND (dp.event_date < @EndDate OR @EndDate IS NULL)
						AND (dp.client_app = @AppName OR @AppName IS NULL)
						AND (dp.host_name = @HostName OR @HostName IS NULL)
						AND (dp.login_name = @LoginName OR @LoginName IS NULL)
						GROUP BY DB_NAME(dp.database_id)
						  )
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 11 AS check_id,
				wt.database_name,
				'-' AS object_name,
				'Total database deadlock wait time' AS finding_group,
				'This database has had ' 
				+ CONVERT(VARCHAR(10), (SUM(DISTINCT CONVERT(BIGINT, wt.total_wait_time_ms)) / 1000) / 86400) 
				+ ':' + CONVERT(VARCHAR(20), DATEADD(SECOND, (SUM(DISTINCT CONVERT(BIGINT, wt.total_wait_time_ms)) / 1000), 0), 108)
				+ ' [d/h/m/s] of deadlock wait time.'
		FROM wait_time AS wt
		GROUP BY wt.database_name
		OPTION ( RECOMPILE );

		/*Check 12 gets total deadlock wait time for SQL Agent*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 12 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding )         
        SELECT 12,
               DB_NAME(aj.database_id),
               'SQLAgent - Job: ' 
                + aj.job_name
                + ' Step: '
                + aj.step_name,
               'Agent Job Deadlocks',
               RTRIM(COUNT(*)) + ' deadlocks from this Agent Job and Step'
        FROM #agent_job AS aj
        GROUP BY DB_NAME(aj.database_id), aj.job_name, aj.step_name
		OPTION ( RECOMPILE );

		/*Check 13 is total parallel deadlocks*/
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Check 13 %s', 0, 1, @d) WITH NOWAIT;
		INSERT #deadlock_findings WITH (TABLOCKX) 
        ( check_id, database_name, object_name, finding_group, finding ) 	
		SELECT 13 AS check_id, 
			   N'-' AS database_name, 
			   '-' AS object_name,
			   'Total parallel deadlocks' AS finding_group,
			   'There have been ' 
				+ CONVERT(NVARCHAR(20), COUNT_BIG(DISTINCT drp.event_date)) 
				+ ' parallel deadlocks.'
        FROM   #deadlock_resource_parallel AS drp
		WHERE 1 = 1
		HAVING COUNT_BIG(DISTINCT drp.event_date) > 0
		OPTION ( RECOMPILE );

		/*Thank you goodnight*/
		INSERT #deadlock_findings WITH (TABLOCKX) 
         ( check_id, database_name, object_name, finding_group, finding ) 
		VALUES ( -1, 
				 N'sp_BlitzLock ' + CAST(CONVERT(DATETIME, @VersionDate, 102) AS VARCHAR(100)), 
				 N'SQL Server First Responder Kit', 
				 N'http://FirstResponderKit.org/', 
				 N'To get help or add your own contributions, join us at http://FirstResponderKit.org.');

		


		/*Results*/
        /*Break in case of emergency*/
        --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_process (event_date, id);
        --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_resource_parallel (event_date, owner_id);
        --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_owner_waiter (event_date, owner_id, waiter_id);
	IF(@OutputDatabaseCheck = 0)
	BEGIN
		
        SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Results 1 %s', 0, 1, @d) WITH NOWAIT;
		WITH deadlocks
		AS ( SELECT N'Regular Deadlock' AS deadlock_type,
					dp.event_date,
		            dp.id,
					dp.victim_id,
					dp.spid,
		            dp.database_id,
		            dp.priority,
		            dp.log_used,
		            dp.wait_resource COLLATE DATABASE_DEFAULT AS wait_resource,
				    CONVERT(
						XML,
						STUFF(( SELECT DISTINCT NCHAR(10) 
										+ N' <object>' 
										+ ISNULL(c.object_name, N'') 
										+ N'</object> ' COLLATE DATABASE_DEFAULT AS object_name
								FROM   #deadlock_owner_waiter AS c
								WHERE  ( dp.id = c.owner_id
										OR dp.victim_id = c.waiter_id )
								AND dp.event_date = c.event_date
								FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(4000)'),
					1, 1, N'')) AS object_names,
		            dp.wait_time,
		            dp.transaction_name,
		            dp.last_tran_started,
		            dp.last_batch_started,
		            dp.last_batch_completed,
		            dp.lock_mode,
		            dp.transaction_count,
		            dp.client_app,
		            dp.host_name,
		            dp.login_name,
		            dp.isolation_level,
		            dp.process_xml.value('(//process/inputbuf/text())[1]', 'NVARCHAR(MAX)') AS inputbuf,
					DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
					dp.is_victim,
					ISNULL(dp.owner_mode, '-') AS owner_mode,
					NULL AS owner_waiter_type,
					NULL AS owner_activity,
					NULL AS owner_waiter_activity,
					NULL AS owner_merging,
					NULL AS owner_spilling,
					NULL AS owner_waiting_to_close,
					ISNULL(dp.waiter_mode, '-') AS waiter_mode,
					NULL AS waiter_waiter_type,
					NULL AS waiter_owner_activity,
					NULL AS waiter_waiter_activity,
					NULL AS waiter_merging,
					NULL AS waiter_spilling,
					NULL AS waiter_waiting_to_close,
					dp.deadlock_graph
		     FROM   #deadlock_process AS dp 
			 WHERE dp.victim_id IS NOT NULL
			 AND   dp.is_parallel = 0
			 
			 UNION ALL
			 
			 SELECT N'Parallel Deadlock' AS deadlock_type,
					dp.event_date,
		            dp.id,
					dp.victim_id,
					dp.spid,
		            dp.database_id,
		            dp.priority,
		            dp.log_used,
		            dp.wait_resource COLLATE DATABASE_DEFAULT,
				    CONVERT(
						XML,
						STUFF(( SELECT DISTINCT NCHAR(10) 
										+ N' <object>' 
										+ ISNULL(c.object_name, N'') 
										+ N'</object> ' COLLATE DATABASE_DEFAULT AS object_name
								FROM   #deadlock_owner_waiter AS c
								WHERE  ( dp.id = c.owner_id
										OR dp.victim_id = c.waiter_id )
								AND dp.event_date = c.event_date
								FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(4000)'),
					1, 1, N'')) AS object_names,
		            dp.wait_time,
		            dp.transaction_name,
		            dp.last_tran_started,
		            dp.last_batch_started,
		            dp.last_batch_completed,
		            dp.lock_mode,
		            dp.transaction_count,
		            dp.client_app,
		            dp.host_name,
		            dp.login_name,
		            dp.isolation_level,
		            dp.process_xml.value('(//process/inputbuf/text())[1]', 'NVARCHAR(MAX)') AS inputbuf,
					DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
					1 AS is_victim,
					cao.wait_type COLLATE DATABASE_DEFAULT AS owner_mode,
					cao.waiter_type AS owner_waiter_type,
					cao.owner_activity AS owner_activity,
					cao.waiter_activity	AS owner_waiter_activity,
					cao.merging	AS owner_merging,
					cao.spilling AS owner_spilling,
					cao.waiting_to_close AS owner_waiting_to_close,
					caw.wait_type COLLATE DATABASE_DEFAULT AS waiter_mode,
					caw.waiter_type AS waiter_waiter_type,
					caw.owner_activity AS waiter_owner_activity,
					caw.waiter_activity	AS waiter_waiter_activity,
					caw.merging	AS waiter_merging,
					caw.spilling AS waiter_spilling,
					caw.waiting_to_close AS waiter_waiting_to_close,
					dp.deadlock_graph
		     FROM   #deadlock_process AS dp 
			 CROSS APPLY (SELECT TOP 1 * FROM  #deadlock_resource_parallel AS drp WHERE drp.owner_id = dp.id AND drp.wait_type = 'e_waitPipeNewRow' ORDER BY drp.event_date) AS cao
			 CROSS APPLY (SELECT TOP 1 * FROM  #deadlock_resource_parallel AS drp WHERE drp.owner_id = dp.id AND drp.wait_type = 'e_waitPipeGetRow' ORDER BY drp.event_date) AS caw
			 WHERE dp.is_parallel = 1 )
		insert into DeadLockTbl (
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
			transaction_count,
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
		SELECT @ServerName,
			   d.deadlock_type,
			   d.event_date,
			   DB_NAME(d.database_id) AS database_name,
			   d.spid,
		       'Deadlock #' 
			   + CONVERT(NVARCHAR(10), d.en)
			   + ', Query #' 
			   + CASE WHEN d.qn = 0 THEN N'1' ELSE CONVERT(NVARCHAR(10), d.qn) END 
			   + CASE WHEN d.is_victim = 1 THEN ' - VICTIM' ELSE '' END
			   AS deadlock_group, 
		       CONVERT(XML, N'<inputbuf><![CDATA[' + d.inputbuf + N']]></inputbuf>') AS query,
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
			   /*These columns will be NULL for regular (non-parallel) deadlocks*/
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
			   d.deadlock_graph
		FROM   deadlocks AS d
		WHERE  d.dn = 1
		AND (is_victim = @VictimsOnly OR @VictimsOnly = 0)
		AND d.qn < CASE WHEN d.deadlock_type = N'Parallel Deadlock' THEN 2 ELSE 2147483647 END  
		AND (DB_NAME(d.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (d.event_date >= @StartDate OR @StartDate IS NULL)
		AND (d.event_date < @EndDate OR @EndDate IS NULL)
		AND (CONVERT(NVARCHAR(MAX), d.object_names) LIKE '%' + @ObjectName + '%' OR @ObjectName IS NULL)
		AND (d.client_app = @AppName OR @AppName IS NULL)
		AND (d.host_name = @HostName OR @HostName IS NULL)
		AND (d.login_name = @LoginName OR @LoginName IS NULL)
		ORDER BY d.event_date, is_victim DESC
		OPTION ( RECOMPILE );
		
		drop SYNONYM DeadLockTbl; --done insert into blitzlock table going to insert into findings table first create synonym.

		--	RAISERROR('att deadlock findings', 0, 1) WITH NOWAIT;
	

		SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
        RAISERROR('Findings %s', 0, 1, @d) WITH NOWAIT;

		Insert into DeadlockFindings (ServerName,check_id,database_name,object_name,finding_group,finding)
		SELECT @ServerName,df.check_id, df.database_name, df.object_name, df.finding_group, df.finding
		FROM #deadlock_findings AS df
		ORDER BY df.check_id
		OPTION ( RECOMPILE );

		drop SYNONYM DeadlockFindings; --done with inserting.
END
ELSE  --Output to database is not set output to client app
	BEGIN
			SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
			RAISERROR('Results 1 %s', 0, 1, @d) WITH NOWAIT;
			WITH deadlocks
			AS ( SELECT N'Regular Deadlock' AS deadlock_type,
						dp.event_date,
						dp.id,
						dp.victim_id,
						dp.spid,
						dp.database_id,
						dp.priority,
						dp.log_used,
						dp.wait_resource COLLATE DATABASE_DEFAULT AS wait_resource,
					    CONVERT(
						XML,
						STUFF(( SELECT DISTINCT NCHAR(10) 
										+ N' <object>' 
										+ ISNULL(c.object_name, N'') 
										+ N'</object> ' COLLATE DATABASE_DEFAULT AS object_name
								FROM   #deadlock_owner_waiter AS c
								WHERE  ( dp.id = c.owner_id
										OR dp.victim_id = c.waiter_id )
								AND dp.event_date = c.event_date
								FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(4000)'),
					    1, 1, N'')) AS object_names,
						dp.wait_time,
						dp.transaction_name,
						dp.last_tran_started,
						dp.last_batch_started,
						dp.last_batch_completed,
						dp.lock_mode,
						dp.transaction_count,
						dp.client_app,
						dp.host_name,
						dp.login_name,
						dp.isolation_level,
						dp.process_xml.value('(//process/inputbuf/text())[1]', 'NVARCHAR(MAX)') AS inputbuf,
						DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
						ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
						ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
						dp.is_victim,
						ISNULL(dp.owner_mode, '-') AS owner_mode,
						NULL AS owner_waiter_type,
						NULL AS owner_activity,
						NULL AS owner_waiter_activity,
						NULL AS owner_merging,
						NULL AS owner_spilling,
						NULL AS owner_waiting_to_close,
						ISNULL(dp.waiter_mode, '-') AS waiter_mode,
						NULL AS waiter_waiter_type,
						NULL AS waiter_owner_activity,
						NULL AS waiter_waiter_activity,
						NULL AS waiter_merging,
						NULL AS waiter_spilling,
						NULL AS waiter_waiting_to_close,
						dp.deadlock_graph
				FROM   #deadlock_process AS dp 
				WHERE dp.victim_id IS NOT NULL
				AND   dp.is_parallel = 0			

				UNION ALL
				
				SELECT N'Parallel Deadlock' AS deadlock_type,
						dp.event_date,
						dp.id,
						dp.victim_id,
						dp.spid,
						dp.database_id,
						dp.priority,
						dp.log_used,
						dp.wait_resource COLLATE DATABASE_DEFAULT,
					    CONVERT(
						XML,
						STUFF(( SELECT DISTINCT NCHAR(10) 
										+ N' <object>' 
										+ ISNULL(c.object_name, N'') 
										+ N'</object> ' COLLATE DATABASE_DEFAULT AS object_name
								FROM   #deadlock_owner_waiter AS c
								WHERE  ( dp.id = c.owner_id
										OR dp.victim_id = c.waiter_id )
								AND dp.event_date = c.event_date
								FOR XML PATH(N''), TYPE ).value(N'.[1]', N'NVARCHAR(4000)'),
					    1, 1, N'')) AS object_names,
						dp.wait_time,
						dp.transaction_name,
						dp.last_tran_started,
						dp.last_batch_started,
						dp.last_batch_completed,
						dp.lock_mode,
						dp.transaction_count,
						dp.client_app,
						dp.host_name,
						dp.login_name,
						dp.isolation_level,
						dp.process_xml.value('(//process/inputbuf/text())[1]', 'NVARCHAR(MAX)') AS inputbuf,
						DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
						ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
						ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
						1 AS is_victim,
						cao.wait_type COLLATE DATABASE_DEFAULT AS owner_mode,
						cao.waiter_type AS owner_waiter_type,
						cao.owner_activity AS owner_activity,
						cao.waiter_activity	AS owner_waiter_activity,
						cao.merging	AS owner_merging,
						cao.spilling AS owner_spilling,
						cao.waiting_to_close AS owner_waiting_to_close,
						caw.wait_type COLLATE DATABASE_DEFAULT AS waiter_mode,
						caw.waiter_type AS waiter_waiter_type,
						caw.owner_activity AS waiter_owner_activity,
						caw.waiter_activity	AS waiter_waiter_activity,
						caw.merging	AS waiter_merging,
						caw.spilling AS waiter_spilling,
						caw.waiting_to_close AS waiter_waiting_to_close,
						dp.deadlock_graph
				FROM   #deadlock_process AS dp 
				OUTER APPLY (SELECT TOP 1 * FROM  #deadlock_resource_parallel AS drp WHERE drp.owner_id = dp.id AND drp.wait_type IN ('e_waitPortOpen', 'e_waitPipeNewRow') ORDER BY drp.event_date) AS cao
				OUTER APPLY (SELECT TOP 1 * FROM  #deadlock_resource_parallel AS drp WHERE drp.owner_id = dp.id AND drp.wait_type IN ('e_waitPortOpen', 'e_waitPipeGetRow') ORDER BY drp.event_date) AS caw
				WHERE dp.is_parallel = 1
				)
				SELECT d.deadlock_type,
				d.event_date,
				DB_NAME(d.database_id) AS database_name,
				d.spid,
				'Deadlock #' 
				+ CONVERT(NVARCHAR(10), d.en)
				+ ', Query #' 
				+ CASE WHEN d.qn = 0 THEN N'1' ELSE CONVERT(NVARCHAR(10), d.qn) END 
				+ CASE WHEN d.is_victim = 1 THEN ' - VICTIM' ELSE '' END
				AS deadlock_group, 
				CONVERT(XML, N'<inputbuf><![CDATA[' + d.inputbuf + N']]></inputbuf>') AS query_xml,
				d.inputbuf AS query_string,
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
				/*These columns will be NULL for regular (non-parallel) deadlocks*/
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
				d.deadlock_graph,
				d.is_victim
			INTO #deadlock_results
			FROM deadlocks AS d
			WHERE  d.dn = 1
			AND (d.is_victim = @VictimsOnly OR @VictimsOnly = 0)
			AND d.qn < CASE WHEN d.deadlock_type = N'Parallel Deadlock' THEN 2 ELSE 2147483647 END 
			AND (DB_NAME(d.database_id) = @DatabaseName OR @DatabaseName IS NULL)
			AND (d.event_date >= @StartDate OR @StartDate IS NULL)
			AND (d.event_date < @EndDate OR @EndDate IS NULL)
			AND (CONVERT(NVARCHAR(MAX), d.object_names) LIKE '%' + @ObjectName + '%' OR @ObjectName IS NULL)
			AND (d.client_app = @AppName OR @AppName IS NULL)
			AND (d.host_name = @HostName OR @HostName IS NULL)
			AND (d.login_name = @LoginName OR @LoginName IS NULL)
			OPTION ( RECOMPILE );
			

			DECLARE @deadlock_result NVARCHAR(MAX) = N''

			SET @deadlock_result += N'
			SELECT
			   dr.deadlock_type,
               dr.event_date,
               dr.database_name,
               dr.spid,
               dr.deadlock_group,
			   '
			   + CASE @ExportToExcel
			     WHEN 1
				 THEN N'dr.query_string AS query,
				        REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), dr.object_names), ''<object>'', ''''), ''</object>'', '''') AS object_names,'
				 ELSE N'dr.query_xml AS query,
				        dr.object_names,'
				 END +
			   N'
               dr.isolation_level,
               dr.owner_mode,
               dr.waiter_mode,
               dr.transaction_count,
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
               dr.waiter_waiting_to_close'
			   + CASE @ExportToExcel
			     WHEN 1
				 THEN N''
				 ELSE N',
				 dr.deadlock_graph'
				 END +
			   '
			FROM #deadlock_results AS dr
			ORDER BY dr.event_date, dr.is_victim DESC
			OPTION(RECOMPILE);
			'

			EXEC sys.sp_executesql
			    @deadlock_result;

			SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
			RAISERROR('Findings %s', 0, 1, @d) WITH NOWAIT;
			SELECT df.check_id, df.database_name, df.object_name, df.finding_group, df.finding
			FROM #deadlock_findings AS df
			ORDER BY df.check_id
			OPTION ( RECOMPILE );

			SET @d = CONVERT(VARCHAR(40), GETDATE(), 109);
			RAISERROR('Done %s', 0, 1, @d) WITH NOWAIT;
		END --done with output to client app.



        IF @Debug = 1
            BEGIN

                SELECT '#deadlock_data' AS table_name, *
                FROM   #deadlock_data AS dd
				OPTION ( RECOMPILE );

                SELECT '#deadlock_resource' AS table_name, *
                FROM   #deadlock_resource AS dr
				OPTION ( RECOMPILE );

                SELECT '#deadlock_resource_parallel' AS table_name, *
                FROM   #deadlock_resource_parallel AS drp
				OPTION ( RECOMPILE );

                SELECT '#deadlock_owner_waiter' AS table_name, *
                FROM   #deadlock_owner_waiter AS dow
				OPTION ( RECOMPILE );

                SELECT '#deadlock_process' AS table_name, *
                FROM   #deadlock_process AS dp
				OPTION ( RECOMPILE );

                SELECT '#deadlock_stack' AS table_name, *
                FROM   #deadlock_stack AS ds
				OPTION ( RECOMPILE );

                SELECT '#deadlock_results' AS table_name, *
                FROM   #deadlock_results AS dr
				OPTION ( RECOMPILE );

            END; -- End debug

    END; --Final End

GO
