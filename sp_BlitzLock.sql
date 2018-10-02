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
	@Debug BIT = 0, 
	@Help BIT = 0,
	@VersionDate DATETIME = NULL OUTPUT
)
WITH RECOMPILE
AS
BEGIN

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Version VARCHAR(30);
SET @Version = '1.10';
SET @VersionDate = '20181001';


	IF @Help = 1 PRINT '
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
	
	
	
	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - Only 2012+ is supported (2008 and 2008R2 are kaput in 2019, so I''m not putting time into them)
	 - If your tables have weird characters in them (https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references) you may get errors trying to parse the XML.
	   I took a long look at this one, and:
		1) Trying to account for all the weird places these could crop up is a losing effort. 
		2) Replace is slow af on lots of XML.
	- Your mom.




	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)

     Changes - for the full list of improvements and fixes in this version, see:
     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/


    MIT License
	   
	All other copyright for sp_BlitzLock are held by Brent Ozar Unlimited, 2018.

	Copyright (c) 2018 Brent Ozar Unlimited

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


        DECLARE @ProductVersion NVARCHAR(128);
        DECLARE @ProductVersionMajor FLOAT;
        DECLARE @ProductVersionMinor INT;

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


		/*Grab the initial set of XML to parse*/
        WITH xml
        AS ( SELECT CONVERT(XML, event_data) AS deadlock_xml
             FROM   sys.fn_xe_file_target_read_file(@EventSessionPath, NULL, NULL, NULL) )
        SELECT TOP ( @Top ) xml.deadlock_xml
        INTO   #deadlock_data
        FROM   xml
        WHERE  xml.deadlock_xml.value('(/event/@name)[1]', 'VARCHAR(256)') = 'xml_deadlock_report'
               AND xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') >= @StartDate
               AND xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') < @EndDate
			   ORDER BY xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime')
			   OPTION ( RECOMPILE );

		

		/*Parse process and input buffer XML*/
        SELECT      dd.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2') AS event_date,
					dd.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'NVARCHAR(256)') AS victim_id,
					ca.dp.value('@id', 'NVARCHAR(256)') AS id,
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
                    ca2.ib.query('.') AS input_buffer,
                    ca.dp.query('.') AS process_xml,
                    dd.deadlock_xml.query('/event/data/value/deadlock') AS deadlock_graph
        INTO        #deadlock_process
        FROM        #deadlock_data AS dd
        CROSS APPLY dd.deadlock_xml.nodes('//deadlock/process-list/process') AS ca(dp)
        CROSS APPLY dd.deadlock_xml.nodes('//deadlock/process-list/process/inputbuf') AS ca2(ib)
		WHERE (ca.dp.value('@currentdb', 'BIGINT') = DB_ID(@DatabaseName) OR @DatabaseName IS NULL)
		AND   (ca.dp.value('@clientapp', 'NVARCHAR(256)') = @AppName OR @AppName IS NULL)
		AND   (ca.dp.value('@hostname', 'NVARCHAR(256)') = @HostName OR @HostName IS NULL)
		AND   (ca.dp.value('@loginname', 'NVARCHAR(256)') = @LoginName OR @LoginName IS NULL)
		OPTION ( RECOMPILE );



		/*Parse execution stack XML*/
        SELECT      dp.id,
					dp.event_date,
                    ca.dp.value('@procname', 'NVARCHAR(1000)') AS proc_name,
                    ca.dp.value('@sqlhandle', 'NVARCHAR(128)') AS sql_handle
        INTO        #deadlock_stack
        FROM        #deadlock_process AS dp
        CROSS APPLY dp.process_xml.nodes('//executionStack/frame') AS ca(dp)
		WHERE (ca.dp.value('@procname', 'NVARCHAR(256)') = @StoredProcName OR @StoredProcName IS NULL)
		OPTION ( RECOMPILE );



		/*Grab the full resource list*/
        SELECT      dd.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2') AS event_date,
					dd.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'NVARCHAR(256)') AS victim_id,
					ca.dp.query('.') AS resource_xml
        INTO        #deadlock_resource
        FROM        #deadlock_data AS dd
        CROSS APPLY dd.deadlock_xml.nodes('//deadlock/resource-list') AS ca(dp)
		OPTION ( RECOMPILE );


		/*This parses object locks*/
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(1000)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
                    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode
        INTO        #deadlock_owner_waiter
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/objectlock') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		WHERE (ca.dr.value('@objectname', 'NVARCHAR(1000)') = @ObjectName OR @ObjectName IS NULL)
		OPTION ( RECOMPILE );



		/*This parses page locks*/
        INSERT #deadlock_owner_waiter
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
                    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/pagelock') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*This parses key locks*/
        INSERT #deadlock_owner_waiter
        SELECT      dr.event_date,     
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
                    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/keylock') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*This parses rid locks*/
        INSERT #deadlock_owner_waiter
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
                    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/ridlock') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*This parses row group locks*/
        INSERT #deadlock_owner_waiter
        SELECT      dr.event_date,
					ca.dr.value('@dbid', 'BIGINT') AS database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') AS object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)') AS lock_mode,
					ca.dr.value('@indexname', 'NVARCHAR(256)') AS index_name,
                    w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') AS waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') AS owner_mode
        FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/rowgrouplock') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );


		/*Parse parallel deadlocks*/
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
					w.l.value('@id', 'NVARCHAR(256)') AS waiter_id,
                    o.l.value('@id', 'NVARCHAR(256)') AS owner_id
        INTO #deadlock_resource_parallel
		FROM        #deadlock_resource AS dr
        CROSS APPLY dr.resource_xml.nodes('//resource-list/exchangeEvent') AS ca(dr)
        CROSS APPLY ca.dr.nodes('//waiter-list/waiter') AS w(l)
        CROSS APPLY ca.dr.nodes('//owner-list/owner') AS o(l)
		OPTION ( RECOMPILE );

		/*Get rid of parallel noise*/
		WITH c
		    AS
		     (
		         SELECT *, ROW_NUMBER() OVER ( PARTITION BY drp.owner_id, drp.waiter_id ORDER BY drp.event_date ) AS rn
		         FROM   #deadlock_resource_parallel AS drp
		     )
		DELETE FROM c
		WHERE c.rn > 1;



		/*Get rid of nonsense*/
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
		UPDATE dp
		SET dp.owner_mode = dow.owner_mode
		FROM #deadlock_process AS dp
		JOIN #deadlock_owner_waiter AS dow
		ON dp.id = dow.owner_id
		AND dp.event_date = dow.event_date
		WHERE dp.is_victim = 0
		OPTION ( RECOMPILE );

		UPDATE dp
		SET dp.waiter_mode = dow.waiter_mode
		FROM #deadlock_process AS dp
		JOIN #deadlock_owner_waiter AS dow
		ON dp.victim_id = dow.waiter_id
		AND dp.event_date = dow.event_date
		WHERE dp.is_victim = 1
		OPTION ( RECOMPILE );


		/*Begin checks based on parsed values*/

		/*Check 1 is deadlocks by database*/
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 	
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

		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 	
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

		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 	
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
		AND dow.index_name IS NOT NULL
		GROUP BY DB_NAME(dow.database_id), dow.index_name
		OPTION ( RECOMPILE );

		

		/*Check 3 looks for Serializable locking*/
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT DISTINCT 7 AS check_id,
			   ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') AS database_name,
			   ds.proc_name AS object_name,
			   'More Info - Query' AS finding_group,
			   'EXEC sp_BlitzCache ' +
					CASE WHEN ds.proc_name = 'adhoc'
						 THEN ' @OnlySqlHandles = ' + sql_handle_csv
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding )
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
		WITH bi AS (
				SELECT  DISTINCT
						dow.object_name,
						PARSENAME(dow.object_name, 3) AS database_name,
						PARSENAME(dow.object_name, 2) AS schema_name,
						PARSENAME(dow.object_name, 1) AS table_name
				FROM #deadlock_owner_waiter AS dow
				WHERE 1 = 1
				AND (DB_NAME(dow.database_id) = @DatabaseName OR @DatabaseName IS NULL)
				AND (dow.event_date >= @StartDate OR @StartDate IS NULL)
				AND (dow.event_date < @EndDate OR @EndDate IS NULL)
				AND (dow.object_name = @ObjectName OR @ObjectName IS NULL)
				AND dow.object_name IS NOT NULL
					)
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 9 AS check_id,	
				bi.database_name,
				bi.schema_name + '.' + bi.table_name,
				'More Info - Table' AS finding_group,
				'EXEC sp_BlitzIndex ' + 
				'@DatabaseName = ' + QUOTENAME(bi.database_name, '''') + 
				', @SchemaName = ' + QUOTENAME(bi.schema_name, '''') + 
				', @TableName = ' + QUOTENAME(bi.table_name, '''') +
				';'	 AS finding
		FROM bi
		OPTION ( RECOMPILE );

		/*Check 10 gets total deadlock wait time per object*/
		WITH chopsuey AS (
				SELECT DISTINCT
				PARSENAME(dow.object_name, 3) AS database_name,
				dow.object_name,
				CONVERT(VARCHAR(10), (SUM(DISTINCT dp.wait_time) / 1000) / 86400) AS wait_days,
				CONVERT(VARCHAR(20), DATEADD(SECOND, (SUM(DISTINCT dp.wait_time) / 1000), 0), 108) AS wait_time_hms
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
				INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
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
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
		SELECT 11 AS check_id,
				wt.database_name,
				'-' AS object_name,
				'Total database deadlock wait time' AS finding_group,
				'This database has had ' 
				+ CONVERT(VARCHAR(10), (SUM(DISTINCT wt.total_wait_time_ms) / 1000) / 86400) 
				+ ':' + CONVERT(VARCHAR(20), DATEADD(SECOND, (SUM(DISTINCT wt.total_wait_time_ms) / 1000), 0), 108)
				+ ' [d/h/m/s] of deadlock wait time.'
		FROM wait_time AS wt
		GROUP BY wt.database_name
		OPTION ( RECOMPILE );


		/*Thank you goodnight*/
		INSERT #deadlock_findings ( check_id, database_name, object_name, finding_group, finding ) 
		VALUES ( -1, 
				 N'sp_BlitzLock ' + CAST(CONVERT(DATETIME, @VersionDate, 102) AS VARCHAR(100)), 
				 N'SQL Server First Responder Kit', 
				 N'http://FirstResponderKit.org/', 
				 N'To get help or add your own contributions, join us at http://FirstResponderKit.org.');

		


		/*Results*/
		WITH deadlocks
		AS ( SELECT N'Regular Deadlock' AS deadlock_type,
					dp.event_date,
		            dp.id,
					dp.victim_id,
		            dp.database_id,
		            dp.priority,
		            dp.log_used,
		            dp.wait_resource,
		            CONVERT(
		                XML,
		                STUFF((   SELECT DISTINCT NCHAR(10) 
										+ N' <object>' 
										+ ISNULL(c.object_name, N'') 
										+ N'</object> ' AS object_name
		                        FROM   #deadlock_owner_waiter AS c
		                        WHERE  (dp.id = c.owner_id
								OR		dp.victim_id = c.waiter_id)
								AND	    dp.event_date = c.event_date
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
		            ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
					DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
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
			 
			 UNION ALL 
			 
			 SELECT N'Parallel Deadlock' AS deadlock_type,
					dp.event_date,
		            dp.id,
					dp.victim_id,
		            dp.database_id,
		            dp.priority,
		            dp.log_used,
		            dp.wait_resource,
		            CONVERT(XML, N'parallel_deadlock') AS object_names,
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
		            ROW_NUMBER() OVER ( PARTITION BY dp.event_date, dp.id ORDER BY dp.event_date ) AS dn,
					DENSE_RANK() OVER ( ORDER BY dp.event_date ) AS en,
					ROW_NUMBER() OVER ( PARTITION BY dp.event_date ORDER BY dp.event_date ) -1 AS qn,
					NULL AS is_victim,
					cao.wait_type AS owner_mode,
					cao.waiter_type AS owner_waiter_type,
					cao.owner_activity AS owner_activity,
					cao.waiter_activity	AS owner_waiter_activity,
					cao.merging	AS owner_merging,
					cao.spilling AS owner_spilling,
					cao.waiting_to_close AS owner_waiting_to_close,
					caw.wait_type AS waiter_mode,
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
			 WHERE dp.victim_id IS NULL
			 AND dp.login_name IS NOT NULL)
		SELECT d.deadlock_type,
			   d.event_date,
			   DB_NAME(d.database_id) AS database_name,
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
		       d.priority,
			   d.log_used,
		       d.last_tran_started,
		       d.last_batch_started,
		       d.last_batch_completed,
		       d.transaction_name,
			   /*These columns will be NULL for regular (non-parallel) deadlocks*/
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
			   d.deadlock_graph
		FROM   deadlocks AS d
		WHERE  d.dn = 1
		AND en < CASE WHEN d.deadlock_type = N'Parallel Deadlock' THEN 2 ELSE 2147483647 END 
		AND (DB_NAME(d.database_id) = @DatabaseName OR @DatabaseName IS NULL)
		AND (d.event_date >= @StartDate OR @StartDate IS NULL)
		AND (d.event_date < @EndDate OR @EndDate IS NULL)
		AND (CONVERT(NVARCHAR(MAX), d.object_names) LIKE '%' + @ObjectName + '%' OR @ObjectName IS NULL)
		AND (d.client_app = @AppName OR @AppName IS NULL)
		AND (d.host_name = @HostName OR @HostName IS NULL)
		AND (d.login_name = @LoginName OR @LoginName IS NULL)
		ORDER BY d.event_date, is_victim DESC
		OPTION ( RECOMPILE );



		SELECT df.check_id, df.database_name, df.object_name, df.finding_group, df.finding
		FROM #deadlock_findings AS df
		ORDER BY df.check_id
		OPTION ( RECOMPILE );


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
				
            END; -- End debug

    END; --Final End

GO

