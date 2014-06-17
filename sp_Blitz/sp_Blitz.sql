USE [master];
GO

IF OBJECT_ID('dbo.sp_Blitz') IS NOT NULL
    DROP PROC dbo.sp_Blitz;
GO

CREATE PROCEDURE [dbo].[sp_Blitz]
    @CheckUserDatabaseObjects TINYINT = 1 ,
    @CheckProcedureCache TINYINT = 0 ,
    @OutputType VARCHAR(20) = 'TABLE' ,
    @OutputProcedureCache TINYINT = 0 ,
    @CheckProcedureCacheFilter VARCHAR(10) = NULL ,
    @CheckServerInfo TINYINT = 0 ,
    @SkipChecksServer NVARCHAR(256) = NULL ,
    @SkipChecksDatabase NVARCHAR(256) = NULL ,
    @SkipChecksSchema NVARCHAR(256) = NULL ,
    @SkipChecksTable NVARCHAR(256) = NULL ,
    @IgnorePrioritiesBelow INT = NULL ,
    @IgnorePrioritiesAbove INT = NULL ,
    @OutputDatabaseName NVARCHAR(128) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
    @OutputXMLasNVARCHAR TINYINT = 0 ,
    @EmailRecipients VARCHAR(MAX) = NULL ,
    @EmailProfile sysname = NULL ,
	@SummaryMode TINYINT = 0 ,
    @Help TINYINT = 0 ,
    @Version INT = NULL OUTPUT,
    @VersionDate DATETIME = NULL OUTPUT
AS
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SELECT @Version = 35, @VersionDate = '20140618'

	IF @Help = 1 PRINT '
	/*
	sp_Blitz (TM) v35 - June 18, 2014

	(C) 2014, Brent Ozar Unlimited.
	See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

	To learn more, visit http://www.BrentOzar.com/blitz where you can download
	new versions for free, watch training videos on how it works, get more info on
	the findings, and more.  To contribute code and see your name in the change
	log, email your improvements & checks to Help@BrentOzar.com.

	Known limitations of this version:
	 - No support for SQL Server 2000 or compatibility mode 80.
	 - If a database name has a question mark in it, some tests will fail. Gotta
	   love that unsupported sp_MSforeachdb.
	 - If you have offline databases, sp_Blitz fails the first time you run it,
	   but does work the second time. (Hoo, boy, this will be fun to debug.)

	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)

	Changes in v35 - June 17, 2014
	 - John Hill fixed a bug in check 134 looking for deadlocks.
	 - Robert Virag improved check 19 looking for replication subscribers.
	 - Russell Hart improved check 34 to avoid blocking during restores.
	 - Added check 126 for priority boost enabled. It was always in the non-
	   default configurations check, but this one is so bad we called it out.
	 - Added checks 128 and 129 for unsupported builds of SQL Server.
	 - Added check 127 for unneccessary backups of ReportServerTempDB.
	 - Changed fill factor threshold to <80% to match sp_BlitzIndex.

	Changes in v34 - April 2, 2014
	 - Jason Pritchard fixed a bug in the plan cache analysis that did not return
	   results when analyzing for high logical reads.
	 - Kirby Richter @SqlKirby fixed a bug in check 75 (t-log sizes) that failed
	   on really big transaction log files. (Not even gonna say how big.)
	 - Oleg Ivashov improved check 94 (jobs without failure emails) to exclude
	   SSRS jobs.
	 - Added @SummaryMode parameter to return only one result set per finding.
	 - Added check 124 for Performance: Deadlocks Happening Daily. Looks for more
	   than 10 deadlocks per day.
	 - Moved check 121 for Performance: Serializable Locking to be lower
	   priority (down to 100 from 10) and only triggers when more than 10
	   minutes of the wait have happened since startup.
	 - Changed checks 107-109 for Poison Waits to have higher thresholds, now
	   looking at more than 5 seconds per hour of server uptime. Been up for 10
	   hours, we look for 50 seconds, that kind of thing.

	Changes in v33 - January 20, 2014
	 - Bob Klimes fixed a bug that Russell Hart introduced in v32, hahaha. Check
	   59 was false-alarming on Agent jobs that actually had notifications.

	Changes in v32 - January 19, 2014
	 - Russell Hart fixed a bug in check 59 (Agent jobs without notifications).
	 - Added @EmailRecipients and @EmailProfile parameters to send the results via
	   Database Mail. Assumes that database mail is already configured correctly.
	   Only sends the main results table, and it will not work well if you also
	   try to use @CheckProcedureCache. Execution plans will not render in email.
     - Fixed a bug in checks 108 and 109 that showed poison waits even if they had
	   0ms of wait time since restart.
	 - Removed check 120 which warned about backups not using WITH CHECKSUM. We
	   fell out of love with WITH CHECKSUM - turns out nobody uses it.
	 - Added check 121 - Poison Wait Detected: Serializable Locking - looking for
	   waits with %LCK%R%. Happens when a query uses a combination of lock hints
	   that make the query serializable.
	 - Added check 122 - User-Created Statistics In Place. There is nothing wrong
	   with creating your own statistics, but it can cause an IO explosion when
	   statistics are updated.
	 - Added check 123 - Multiple Agent Jobs Starting Simultaneously. Ran into an
	   issue where dozens of jobs started at the exact same time every hour.

	Changes in v31 - December 1, 2013
	 - Dick Baker, Ambrosetti Ltd (UK):
	    - Fixed typos in checks 107-109 that looked for the wrong CheckID when
	      skipping checks, plus improved performance while he was in there.
	    - Improved check 106 (default trace file) so that it will not error out
	      if the user does not have permissions on sys.traces.
	- Christoph Muller-Spengler @cms4j added check 118 looking at the top queries
	  in the plan cache for key lookups.
  	- Philip Dietrich added check 119 for TDE certificates that have not been
  	  backed up recently.
	- Ricky Lively added @Help to print inline help. I love his approach to it.
	- Added check 120 looking for databases that have not had a full backup using
	  the WITH CHECKSUM option in the last 30 days.

	For prior changes, see http://www.BrentOzar.com/blitz/changelog/


	Parameter explanations:

	@CheckUserDatabaseObjects	1=review user databases for triggers, heaps, etc. Takes more time for more databases and objects.
	@CheckServerInfo			1=show server info like CPUs, memory, virtualization
	@CheckProcedureCache		1=top 20-50 resource-intensive cache plans and analyze them for common performance issues.
	@OutputProcedureCache		1=output the top 20-50 resource-intensive plans even if they did not trigger an alarm
	@CheckProcedureCacheFilter	''CPU'' | ''Reads'' | ''Duration'' | ''ExecCount''
	@OutputType					''TABLE''=table | ''COUNT''=row with number found | ''SCHEMA''=version and field list
	@IgnorePrioritiesBelow		100=ignore priorities below 100
	@IgnorePrioritiesAbove		100=ignore priorities above 100
	For the rest of the parameters, see http://www.brentozar.com/blitz/documentation for details.


	*/'
	ELSE IF @OutputType = 'SCHEMA'
	BEGIN
		SELECT @Version AS Version,
		FieldList = '[Priority] TINYINT, [FindingsGroup] VARCHAR(50), [Finding] VARCHAR(200), [DatabaseName] NVARCHAR(128), [URL] VARCHAR(200), [Details] NVARCHAR(4000), [QueryPlan] NVARCHAR(MAX), [QueryPlanFiltered] NVARCHAR(MAX), [CheckID] INT'

	END
	ELSE /* IF @OutputType = 'SCHEMA' */
	BEGIN

		/*
		We start by creating #BlitzResults. It's a temp table that will store all of
		the results from our checks. Throughout the rest of this stored procedure,
		we're running a series of checks looking for dangerous things inside the SQL
		Server. When we find a problem, we insert rows into #BlitzResults. At the
		end, we return these results to the end user.

		#BlitzResults has a CheckID field, but there's no Check table. As we do
		checks, we insert data into this table, and we manually put in the CheckID.
		We (Brent Ozar Unlimited) maintain a list of the checks by ID#. You can
		download that from http://www.BrentOzar.com/blitz/documentation/ - you'll
		see why it can help shortly.
		*/
		DECLARE @StringToExecute NVARCHAR(4000)
			,@curr_tracefilename NVARCHAR(500)
			,@base_tracefilename NVARCHAR(500)
			,@indx int
			,@query_result_separator CHAR(1)
			,@EmailSubject NVARCHAR(255)
			,@EmailBody NVARCHAR(MAX)
			,@EmailAttachmentFilename NVARCHAR(255)
			,@ProductVersion NVARCHAR(128)
			,@ProductVersionMajor DECIMAL(10,2)
			,@ProductVersionMinor DECIMAL(10,2);

		IF OBJECT_ID('tempdb..#BlitzResults') IS NOT NULL
			DROP TABLE #BlitzResults;
		CREATE TABLE #BlitzResults
			(
			  ID INT IDENTITY(1, 1) ,
			  CheckID INT ,
			  DatabaseName NVARCHAR(128) ,
			  Priority TINYINT ,
			  FindingsGroup VARCHAR(50) ,
			  Finding VARCHAR(200) ,
			  URL VARCHAR(200) ,
			  Details NVARCHAR(4000) ,
			  QueryPlan [XML] NULL ,
			  QueryPlanFiltered [NVARCHAR](MAX) NULL
			);

		/*
		You can build your own table with a list of checks to skip. For example, you
		might have some databases that you don't care about, or some checks you don't
		want to run. Then, when you run sp_Blitz, you can specify these parameters:
		@SkipChecksDatabase = 'DBAtools',
		@SkipChecksSchema = 'dbo',
		@SkipChecksTable = 'BlitzChecksToSkip'
		Pass in the database, schema, and table that contains the list of checks you
		want to skip. This part of the code checks those parameters, gets the list,
		and then saves those in a temp table. As we run each check, we'll see if we
		need to skip it.

		Really anal-retentive users will note that the @SkipChecksServer parameter is
		not used. YET. We added that parameter in so that we could avoid changing the
		stored proc's surface area (interface) later.
		*/
		IF OBJECT_ID('tempdb..#SkipChecks') IS NOT NULL
			DROP TABLE #SkipChecks;
		CREATE TABLE #SkipChecks
			(
			  DatabaseName NVARCHAR(128) ,
			  CheckID INT ,
			  ServerName NVARCHAR(128)
			);
		CREATE CLUSTERED INDEX IX_CheckID_DatabaseName ON #SkipChecks(CheckID, DatabaseName);

		IF @SkipChecksTable IS NOT NULL
			AND @SkipChecksSchema IS NOT NULL
			AND @SkipChecksDatabase IS NOT NULL
			BEGIN
				SET @StringToExecute = 'INSERT INTO #SkipChecks(DatabaseName, CheckID, ServerName )
				SELECT DISTINCT DatabaseName, CheckID, ServerName
				FROM ' + QUOTENAME(@SkipChecksDatabase) + '.' + QUOTENAME(@SkipChecksSchema) + '.' + QUOTENAME(@SkipChecksTable)
					+ ' WHERE ServerName IS NULL OR ServerName = SERVERPROPERTY(''ServerName'');'
				EXEC(@StringToExecute)
			END

		IF NOT EXISTS ( SELECT  1
							FROM    #SkipChecks
							WHERE   DatabaseName IS NULL AND CheckID = 106 )
							AND (select convert(int,value_in_use) from sys.configurations where name = 'default trace enabled' ) = 1
			BEGIN
					select @curr_tracefilename = [path] from sys.traces where is_default = 1 ;
					set @curr_tracefilename = reverse(@curr_tracefilename);
					select @indx = patindex('%\%', @curr_tracefilename) ;
					set @curr_tracefilename = reverse(@curr_tracefilename) ;
					set @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ;
			END


		/*
		That's the end of the SkipChecks stuff.
		The next several tables are used by various checks later.
		*/
		IF OBJECT_ID('tempdb..#ConfigurationDefaults') IS NOT NULL
			DROP TABLE #ConfigurationDefaults;
		CREATE TABLE #ConfigurationDefaults
			(
			  name NVARCHAR(128) ,
			  DefaultValue BIGINT,
			  CheckID INT
			);

		IF OBJECT_ID('tempdb..#DBCCs') IS NOT NULL
			DROP TABLE #DBCCs;
		CREATE TABLE #DBCCs
			(
			  ID INT IDENTITY(1, 1)
					 PRIMARY KEY ,
			  ParentObject VARCHAR(255) ,
			  Object VARCHAR(255) ,
			  Field VARCHAR(255) ,
			  Value VARCHAR(255) ,
			  DbName NVARCHAR(128) NULL
			)


		IF OBJECT_ID('tempdb..#LogInfo2012') IS NOT NULL
			DROP TABLE #LogInfo2012;
		CREATE TABLE #LogInfo2012
			(
			  recoveryunitid INT ,
			  FileID SMALLINT ,
			  FileSize BIGINT ,
			  StartOffset BIGINT ,
			  FSeqNo BIGINT ,
			  [Status] TINYINT ,
			  Parity TINYINT ,
			  CreateLSN NUMERIC(38)
			);

		IF OBJECT_ID('tempdb..#LogInfo') IS NOT NULL
			DROP TABLE #LogInfo;
		CREATE TABLE #LogInfo
			(
			  FileID SMALLINT ,
			  FileSize BIGINT ,
			  StartOffset BIGINT ,
			  FSeqNo BIGINT ,
			  [Status] TINYINT ,
			  Parity TINYINT ,
			  CreateLSN NUMERIC(38)
			);

		IF OBJECT_ID('tempdb..#partdb') IS NOT NULL
			DROP TABLE #partdb;
		CREATE TABLE #partdb
			(
			  dbname NVARCHAR(128) ,
			  objectname NVARCHAR(200) ,
			  type_desc NVARCHAR(128)
			)

		IF OBJECT_ID('tempdb..#TraceStatus') IS NOT NULL
			DROP TABLE #TraceStatus;
		CREATE TABLE #TraceStatus
			(
			  TraceFlag VARCHAR(10) ,
			  status BIT ,
			  Global BIT ,
			  Session BIT
			);

		IF OBJECT_ID('tempdb..#driveInfo') IS NOT NULL
			DROP TABLE #driveInfo;
		CREATE TABLE #driveInfo
			(
			  drive NVARCHAR ,
			  SIZE DECIMAL(18, 2)
			)


		IF OBJECT_ID('tempdb..#dm_exec_query_stats') IS NOT NULL
			DROP TABLE #dm_exec_query_stats;
		CREATE TABLE #dm_exec_query_stats
			(
			  [id] [int] NOT NULL
						 IDENTITY(1, 1) ,
			  [sql_handle] [varbinary](64) NOT NULL ,
			  [statement_start_offset] [int] NOT NULL ,
			  [statement_end_offset] [int] NOT NULL ,
			  [plan_generation_num] [bigint] NOT NULL ,
			  [plan_handle] [varbinary](64) NOT NULL ,
			  [creation_time] [datetime] NOT NULL ,
			  [last_execution_time] [datetime] NOT NULL ,
			  [execution_count] [bigint] NOT NULL ,
			  [total_worker_time] [bigint] NOT NULL ,
			  [last_worker_time] [bigint] NOT NULL ,
			  [min_worker_time] [bigint] NOT NULL ,
			  [max_worker_time] [bigint] NOT NULL ,
			  [total_physical_reads] [bigint] NOT NULL ,
			  [last_physical_reads] [bigint] NOT NULL ,
			  [min_physical_reads] [bigint] NOT NULL ,
			  [max_physical_reads] [bigint] NOT NULL ,
			  [total_logical_writes] [bigint] NOT NULL ,
			  [last_logical_writes] [bigint] NOT NULL ,
			  [min_logical_writes] [bigint] NOT NULL ,
			  [max_logical_writes] [bigint] NOT NULL ,
			  [total_logical_reads] [bigint] NOT NULL ,
			  [last_logical_reads] [bigint] NOT NULL ,
			  [min_logical_reads] [bigint] NOT NULL ,
			  [max_logical_reads] [bigint] NOT NULL ,
			  [total_clr_time] [bigint] NOT NULL ,
			  [last_clr_time] [bigint] NOT NULL ,
			  [min_clr_time] [bigint] NOT NULL ,
			  [max_clr_time] [bigint] NOT NULL ,
			  [total_elapsed_time] [bigint] NOT NULL ,
			  [last_elapsed_time] [bigint] NOT NULL ,
			  [min_elapsed_time] [bigint] NOT NULL ,
			  [max_elapsed_time] [bigint] NOT NULL ,
			  [query_hash] [binary](8) NULL ,
			  [query_plan_hash] [binary](8) NULL ,
			  [query_plan] [xml] NULL ,
			  [query_plan_filtered] [nvarchar](MAX) NULL ,
			  [text] [nvarchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
									 NULL ,
			  [text_filtered] [nvarchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
											  NULL
			)


		/* If we're outputting CSV, don't bother checking the plan cache because we cannot export plans. */
		IF @OutputType = 'CSV'
			SET @CheckProcedureCache = 0;

		/* Sanitize our inputs */
		SELECT
			@OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
			@OutputSchemaName = QUOTENAME(@OutputSchemaName),
			@OutputTableName = QUOTENAME(@OutputTableName)

		/* Get the major and minor build numbers */
		SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
		SELECT @ProductVersionMajor = SUBSTRING(@ProductVersion, 1,CHARINDEX('.', @ProductVersion) + 1 ),
			@ProductVersionMinor = PARSENAME(CONVERT(varchar(32), @ProductVersion), 2)


		/*
		Whew! we're finally done with the setup, and we can start doing checks.
		First, let's make sure we're actually supposed to do checks on this server.
		The user could have passed in a SkipChecks table that specified to skip ALL
		checks on this server, so let's check for that:
		*/
		IF ( ( SERVERPROPERTY('ServerName') NOT IN ( SELECT ServerName
													 FROM   #SkipChecks
													 WHERE  DatabaseName IS NULL
															AND CheckID IS NULL ) )
			 OR ( @SkipChecksTable IS NULL )
		   )
			BEGIN

				/*
				Our very first check! We'll put more comments in this one just to
				explain exactly how it works. First, we check to see if we're
				supposed to skip CheckID 1 (that's the check we're working on.)
				*/
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 1 )
					BEGIN

						/*
						Below, we check master.sys.databases looking for databases
						that haven't had a backup in the last week. If we find any,
						we insert them into #BlitzResults, the temp table that
						tracks our server's problems. Note that if the check does
						NOT find any problems, we don't save that. We're only
						saving the problems, not the successful checks.
						*/
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  1 AS CheckID ,
										d.[name] AS DatabaseName ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Backups Not Performed Recently' AS Finding ,
										'http://BrentOzar.com/go/nobak' AS URL ,
										'Database ' + d.Name + ' last backed up: '
										+ CAST(COALESCE(MAX(b.backup_finish_date),
														' never ') AS VARCHAR(200)) AS Details
								FROM    master.sys.databases d
										LEFT OUTER JOIN msdb.dbo.backupset b ON d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																  AND b.type = 'D'
																  AND b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server */
								WHERE   d.database_id <> 2  /* Bonus points if you know what that means */
										AND d.state <> 1 /* Not currently restoring, like log shipping databases */
										AND d.is_in_standby = 0 /* Not a log shipping target database */
										AND d.source_database_id IS NULL /* Excludes database snapshots */
										AND d.name NOT IN ( SELECT DISTINCT
																  DatabaseName
															FROM  #SkipChecks
															WHERE CheckID IS NULL )
										/*
										The above NOT IN filters out the databases we're not supposed to check.
										*/
								GROUP BY d.name
								HAVING  MAX(b.backup_finish_date) <= DATEADD(dd,
																  -7, GETDATE());
						/*
						And there you have it. The rest of this stored procedure works the same
						way: it asks:
						- Should I skip this check?
						- If not, do I find problems?
						- Insert the results into #BlitzResults
						This particular check is just a little bit fancy - it also has a second
						query below that checks for databases that have NEVER been backed up.
						We use CheckID #1 for both of these just because they represent the same
						problem - a database that needs a backup.
						*/

						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  1 AS CheckID ,
										d.name AS DatabaseName ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Backups Not Performed Recently' AS Finding ,
										'http://BrentOzar.com/go/nobak' AS URL ,
										( 'Database ' + d.Name
										  + ' never backed up.' ) AS Details
								FROM    master.sys.databases d
								WHERE   d.database_id <> 2 /* Bonus points if you know what that means */
										AND d.state <> 1 /* Not currently restoring, like log shipping databases */
										AND d.is_in_standby = 0 /* Not a log shipping target database */
										AND d.source_database_id IS NULL /* Excludes database snapshots */
										AND d.name NOT IN ( SELECT DISTINCT
																  DatabaseName
															FROM  #SkipChecks
															WHERE CheckID IS NULL )
										AND NOT EXISTS ( SELECT *
														 FROM   msdb.dbo.backupset b
														 WHERE  d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																AND b.type = 'D'
																AND b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server */)

					END

				/*
				And that's the end of CheckID #1.

				CheckID #2 is a little simpler because it only involves one query, and it's
				more typical for queries that people contribute. But keep reading, because
				the next check gets more complex again.
				*/

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 2 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										2 AS CheckID ,
										d.name AS DatabaseName ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Full Recovery Mode w/o Log Backups' AS Finding ,
										'http://BrentOzar.com/go/biglogs' AS URL ,
										( 'Database ' + ( d.Name COLLATE database_default )
										  + ' is in ' + d.recovery_model_desc
										  + ' recovery mode but has not had a log backup in the last week.' ) AS Details
								FROM    master.sys.databases d
								WHERE   d.recovery_model IN ( 1, 2 )
										AND d.database_id NOT IN ( 2, 3 )
										AND d.source_database_id IS NULL
										AND d.state <> 1 /* Not currently restoring, like log shipping databases */
										AND d.is_in_standby = 0 /* Not a log shipping target database */
										AND d.source_database_id IS NULL /* Excludes database snapshots */
										AND d.name NOT IN ( SELECT DISTINCT
																  DatabaseName
															FROM  #SkipChecks
															WHERE CheckID IS NULL )
										AND NOT EXISTS ( SELECT *
														 FROM   msdb.dbo.backupset b
														 WHERE  d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																AND b.type = 'L'
																AND b.backup_finish_date >= DATEADD(dd,
																  -7, GETDATE()) );
					END


				/*
				Next up, we've got CheckID 8. (These don't have to go in order.) This one
				won't work on SQL Server 2005 because it relies on a new DMV that didn't
				exist prior to SQL Server 2008. This means we have to check the SQL Server
				version first, then build a dynamic string with the query we want to run:
				*/

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 8 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID, Priority,
							FindingsGroup,
							Finding, URL,
							Details)
					  SELECT 8 AS CheckID,
					  150 AS Priority,
					  ''Security'' AS FindingsGroup,
					  ''Server Audits Running'' AS Finding,
					  ''http://BrentOzar.com/go/audits'' AS URL,
					  (''SQL Server built-in audit functionality is being used by server audit: '' + [name]) AS Details FROM sys.dm_server_audit_status'
								EXECUTE(@StringToExecute)
							END;
					END

				/*
				But what if you need to run a query in every individual database?
				Check out CheckID 99 below. Yes, it uses sp_MSforeachdb, and no,
				we're not happy about that. sp_MSforeachdb is known to have a lot
				of issues, like skipping databases sometimes. However, this is the
				only built-in option that we have. If you're writing your own code
				for database maintenance, consider Aaron Bertrand's alternative:
				http://www.mssqltips.com/sqlservertip/2201/making-a-more-reliable-and-flexible-spmsforeachdb/
				We don't include that as part of sp_Blitz, of course, because
				copying and distributing copyrighted code from others without their
				written permission isn't a good idea.
				*/
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 99 )
					BEGIN
						EXEC dbo.sp_MSforeachdb 'USE [?];  IF EXISTS (SELECT * FROM  sys.tables WITH (NOLOCK) WHERE name = ''sysmergepublications'' ) IF EXISTS ( SELECT * FROM sysmergepublications WITH (NOLOCK) WHERE retention = 0)   INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 99, DB_NAME(), 110, ''Performance'', ''Infinite merge replication metadata retention period'', ''http://BrentOzar.com/go/merge'', (''The ['' + DB_NAME() + ''] database has merge replication metadata retention period set to infinite - this can be the case of significant performance issues.'')';
					END
				/*
				Note that by using sp_MSforeachdb, we're running the query in all
				databases. We're not checking #SkipChecks here for each database to
				see if we should run the check in this database. That means we may
				still run a skipped check if it involves sp_MSforeachdb. We just
				don't output those results in the last step.

				And that's the basic idea! You can read through the rest of the
				checks if you like - some more exciting stuff happens closer to the
				end of the stored proc, where we start doing things like checking
				the plan cache, but those aren't as cleanly commented.

				If you'd like to contribute your own check, use one of the check
				formats shown above and email it to Help@BrentOzar.com. You don't
				have to pick a CheckID or a link - we'll take care of that when we
				test and publish the code. Thanks!
				*/


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 93 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										93 AS CheckID ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Backing Up to Same Drive Where Databases Reside' AS Finding ,
										'http://BrentOzar.com/go/backup' AS URL ,
										'Drive '
										+ UPPER(LEFT(bmf.physical_device_name, 3))
										+ ' houses both database files AND backups taken in the last two weeks. This represents a serious risk if that array fails.' Details
								FROM    msdb.dbo.backupmediafamily AS bmf
										INNER JOIN msdb.dbo.backupset AS bs ON bmf.media_set_id = bs.media_set_id
																  AND bs.backup_start_date >= ( DATEADD(dd,
																  -14, GETDATE()) )
								WHERE   UPPER(LEFT(bmf.physical_device_name COLLATE SQL_Latin1_General_CP1_CI_AS, 3)) IN (
										SELECT DISTINCT
												UPPER(LEFT(mf.physical_name COLLATE SQL_Latin1_General_CP1_CI_AS, 3))
										FROM    sys.master_files AS mf )
					END


					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 119 )
						AND EXISTS ( SELECT *
									 FROM   sys.all_objects o
									 WHERE  o.name = 'dm_database_encryption_keys' )
						BEGIN
							SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, DatabaseName, URL, Details)
								SELECT 119 AS CheckID,
								1 AS Priority,
								''Backup'' AS FindingsGroup,
								''TDE Certificate Not Backed Up Recently'' AS Finding,
								db_name(dek.database_id) AS DatabaseName,
								''http://BrentOzar.com/go/tde'' AS URL,
								''The certificate '' + c.name + '' is used to encrypt database '' + db_name(dek.database_id) + ''. Last backup date: '' + COALESCE(CAST(c.pvt_key_last_backup_date AS VARCHAR(100)), ''Never'') AS Details
								FROM sys.certificates c INNER JOIN sys.dm_database_encryption_keys dek ON c.thumbprint = dek.encryptor_thumbprint
								WHERE pvt_key_last_backup_date IS NULL OR pvt_key_last_backup_date <= DATEADD(dd, -30, GETDATE())';
							EXECUTE(@StringToExecute);
						END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 3 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT TOP 1
										3 AS CheckID ,
										'msdb' ,
										200 AS Priority ,
										'Backup' AS FindingsGroup ,
										'MSDB Backup History Not Purged' AS Finding ,
										'http://BrentOzar.com/go/history' AS URL ,
										( 'Database backup history retained back to '
										  + CAST(bs.backup_start_date AS VARCHAR(20)) ) AS Details
								FROM    msdb.dbo.backupset bs
								WHERE   bs.backup_start_date <= DATEADD(dd, -60,
																  GETDATE())
								ORDER BY backup_set_id ASC;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 4 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  4 AS CheckID ,
										10 AS Priority ,
										'Security' AS FindingsGroup ,
										'Sysadmins' AS Finding ,
										'http://BrentOzar.com/go/sa' AS URL ,
										( 'Login [' + l.name
										  + '] is a sysadmin - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' ) AS Details
								FROM    master.sys.syslogins l
								WHERE   l.sysadmin = 1
										AND l.name <> SUSER_SNAME(0x01)
										AND l.denylogin = 0;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 5 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  5 AS CheckID ,
										10 AS Priority ,
										'Security' AS FindingsGroup ,
										'Security Admins' AS Finding ,
										'http://BrentOzar.com/go/sa' AS URL ,
										( 'Login [' + l.name
										  + '] is a security admin - meaning they can give themselves permission to do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' ) AS Details
								FROM    master.sys.syslogins l
								WHERE   l.securityadmin = 1
										AND l.name <> SUSER_SNAME(0x01)
										AND l.denylogin = 0;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 104 )
					BEGIN
						INSERT  INTO #BlitzResults
								( [CheckID] ,
								  [Priority] ,
								  [FindingsGroup] ,
								  [Finding] ,
								  [URL] ,
								  [Details]
								)
								SELECT  104 AS [CheckID] ,
										10 AS [Priority] ,
										'Security' AS [FindingsGroup] ,
										'Login Can Control Server' AS [Finding] ,
										'http://BrentOzar.com/go/sa' AS [URL] ,
										'Login [' + pri.[name]
										+ '] has the CONTROL SERVER permission - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' AS [Details]
								FROM    sys.server_principals AS pri
								WHERE   pri.[principal_id] IN (
										SELECT  p.[grantee_principal_id]
										FROM    sys.server_permissions AS p
										WHERE   p.[state] IN ( 'G', 'W' )
												AND p.[class] = 100
												AND p.[type] = 'CL' )
										AND pri.[name] NOT LIKE '##%##'
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 6 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  6 AS CheckID ,
										200 AS Priority ,
										'Security' AS FindingsGroup ,
										'Jobs Owned By Users' AS Finding ,
										'http://BrentOzar.com/go/owners' AS URL ,
										( 'Job [' + j.name + '] is owned by ['
										  + SUSER_SNAME(j.owner_sid)
										  + '] - meaning if their login is disabled or not available due to Active Directory problems, the job will stop working.' ) AS Details
								FROM    msdb.dbo.sysjobs j
								WHERE   j.enabled = 1
										AND SUSER_SNAME(j.owner_sid) <> SUSER_SNAME(0x01);
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 7 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  7 AS CheckID ,
										10 AS Priority ,
										'Security' AS FindingsGroup ,
										'Stored Procedure Runs at Startup' AS Finding ,
										'http://BrentOzar.com/go/startup' AS URL ,
										( 'Stored procedure [master].['
										  + r.SPECIFIC_SCHEMA + '].['
										  + r.SPECIFIC_NAME
										  + '] runs automatically when SQL Server starts up.  Make sure you know exactly what this stored procedure is doing, because it could pose a security risk.' ) AS Details
								FROM    master.INFORMATION_SCHEMA.ROUTINES r
								WHERE   OBJECTPROPERTY(OBJECT_ID(ROUTINE_NAME),
													   'ExecIsStartup') = 1;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 9 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 9 AS CheckID,
					  200 AS Priority,
					  ''Surface Area'' AS FindingsGroup,
					  ''Endpoints Configured'' AS Finding,
					  ''http://BrentOzar.com/go/endpoints/'' AS URL,
					  (''SQL Server endpoints are configured.  These can be used for database mirroring or Service Broker, but if you do not need them, avoid leaving them enabled.  Endpoint name: '' + [name]) AS Details FROM sys.endpoints WHERE type <> 2'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 10 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 10 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Resource Governor Enabled'' AS Finding,
					  ''http://BrentOzar.com/go/rg'' AS URL,
					  (''Resource Governor is enabled.  Queries may be throttled.  Make sure you understand how the Classifier Function is configured.'') AS Details FROM sys.resource_governor_configuration WHERE is_enabled = 1'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 11 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 11 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Server Triggers Enabled'' AS Finding,
					  ''http://BrentOzar.com/go/logontriggers/'' AS URL,
					  (''Server Trigger ['' + [name] ++ ''] is enabled, so it runs every time someone logs in.  Make sure you understand what that trigger is doing - the less work it does, the better.'') AS Details FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 12 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  12 AS CheckID ,
										[name] AS DatabaseName ,
										10 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Close Enabled' AS Finding ,
										'http://BrentOzar.com/go/autoclose' AS URL ,
										( 'Database [' + [name]
										  + '] has auto-close enabled.  This setting can dramatically decrease performance.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_close_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 13 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  13 AS CheckID ,
										[name] AS DatabaseName ,
										10 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Shrink Enabled' AS Finding ,
										'http://BrentOzar.com/go/autoshrink' AS URL ,
										( 'Database [' + [name]
										  + '] has auto-shrink enabled.  This setting can dramatically decrease performance.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_shrink_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks );
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 14 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 14 AS CheckID,
					  [name] as DatabaseName,
					  50 AS Priority,
					  ''Reliability'' AS FindingsGroup,
					  ''Page Verification Not Optimal'' AS Finding,
					  ''http://BrentOzar.com/go/torn'' AS URL,
					  (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') COLLATE database_default AS Details
					  FROM sys.databases
					  WHERE page_verify_option < 2
					  AND name <> ''tempdb''
					  and name not in (select distinct DatabaseName from #SkipChecks)'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 15 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  15 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Create Stats Disabled' AS Finding ,
										'http://BrentOzar.com/go/acs' AS URL ,
										( 'Database [' + [name]
										  + '] has auto-create-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_create_stats_on = 0
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 16 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  16 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Update Stats Disabled' AS Finding ,
										'http://BrentOzar.com/go/aus' AS URL ,
										( 'Database [' + [name]
										  + '] has auto-update-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_update_stats_on = 0
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 17 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  17 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Stats Updated Asynchronously' AS Finding ,
										'http://BrentOzar.com/go/asyncstats' AS URL ,
										( 'Database [' + [name]
										  + '] has auto-update-stats-async enabled.  When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_update_stats_async_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 18 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  18 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Forced Parameterization On' AS Finding ,
										'http://BrentOzar.com/go/forced' AS URL ,
										( 'Database [' + [name]
										  + '] has forced parameterization enabled.  SQL Server will aggressively reuse query execution plans even if the applications do not parameterize their queries.  This can be a performance booster with some programming languages, or it may use universally bad execution plans when better alternatives are available for certain parameters.' ) AS Details
								FROM    sys.databases
								WHERE   is_parameterization_forced = 1
										AND name NOT IN ( SELECT  DatabaseName
														  FROM    #SkipChecks )
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 19 )
					BEGIN
						/* Method 1: Check sys.databases parameters */
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)

								SELECT  19 AS CheckID ,
										[name] AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Replication In Use' AS Finding ,
										'http://BrentOzar.com/go/repl' AS URL ,
										( 'Database [' + [name]
										  + '] is a replication publisher, subscriber, or distributor.' ) AS Details
								FROM    sys.databases
								WHERE   name NOT IN ( SELECT DISTINCT
																DatabaseName
													  FROM      #SkipChecks )
										AND is_published = 1
										OR is_subscribed = 1
										OR is_merge_published = 1
										OR is_distributor = 1;

						/* Method B: check subscribers for MSreplication_objects tables */
						EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults
										(CheckID,
										DatabaseName,
										Priority,
										FindingsGroup,
										Finding,
										URL,
										Details)
							  SELECT DISTINCT 19,
							  db_name(),
							  200,
							  ''Informational'',
							  ''Replication In Use'',
							  ''http://BrentOzar.com/go/repl'',
							  (''['' + DB_NAME() + ''] has MSreplication_objects tables in it, indicating it is a replication subscriber.'')
							  FROM [?].sys.tables
							  WHERE name = ''dbo.MSreplication_objects'' AND ''?'' <> ''master''';

					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 20 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  20 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Date Correlation On' AS Finding ,
										'http://BrentOzar.com/go/corr' AS URL ,
										( 'Database [' + [name]
										  + '] has date correlation enabled.  This is not a default setting, and it has some performance overhead.  It tells SQL Server that date fields in two tables are related, and SQL Server maintains statistics showing that relation.' ) AS Details
								FROM    sys.databases
								WHERE   is_date_correlation_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 21 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 21 AS CheckID,
					  [name] as DatabaseName,
					  20 AS Priority,
					  ''Encryption'' AS FindingsGroup,
					  ''Database Encrypted'' AS Finding,
					  ''http://BrentOzar.com/go/tde'' AS URL,
					  (''Database ['' + [name] + ''] has Transparent Data Encryption enabled.  Make absolutely sure you have backed up the certificate and private key, or else you will not be able to restore this database.'') AS Details
					  FROM sys.databases
					  WHERE is_encrypted = 1
					  and name not in (select distinct DatabaseName from #SkipChecks)'
								EXECUTE(@StringToExecute)
							END;
					END

				/*
				Believe it or not, SQL Server doesn't track the default values
				for sp_configure options! We'll make our own list here.
				*/
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'access check cache bucket count', 0, 1001 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'access check cache quota', 0, 1002 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Ad Hoc Distributed Queries', 0, 1003 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity I/O mask', 0, 1004 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity mask', 0, 1005 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Agent XPs', 0, 1006 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'allow updates', 0, 1007 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'awe enabled', 0, 1008 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'blocked process threshold', 0, 1009 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'c2 audit mode', 0, 1010 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'clr enabled', 0, 1011 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cost threshold for parallelism', 5, 1012 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cross db ownership chaining', 0, 1013 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cursor threshold', -1, 1014 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Database Mail XPs', 0, 1015 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default full-text language', 1033, 1016 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default language', 0, 1017 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default trace enabled', 1, 1018 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'disallow results from triggers', 0, 1019 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'fill factor (%)', 0, 1020 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft crawl bandwidth (max)', 100, 1021 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft crawl bandwidth (min)', 0, 1022 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft notify bandwidth (max)', 100, 1023 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft notify bandwidth (min)', 0, 1024 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'index create memory (KB)', 0, 1025 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'in-doubt xact resolution', 0, 1026 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'lightweight pooling', 0, 1027 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'locks', 0, 1028 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max degree of parallelism', 0, 1029 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max full-text crawl range', 4, 1030 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max server memory (MB)', 2147483647, 1031 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max text repl size (B)', 65536, 1032 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max worker threads', 0, 1033 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'media retention', 0, 1034 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'min memory per query (KB)', 1024, 1035 );
				/* Accepting both 0 and 16 below because both have been seen in the wild as defaults. */
				IF EXISTS ( SELECT  *
							FROM    sys.configurations
							WHERE   name = 'min server memory (MB)'
									AND value_in_use IN ( 0, 16 ) )
					INSERT  INTO #ConfigurationDefaults
							SELECT  'min server memory (MB)' ,
									CAST(value_in_use AS BIGINT), 1036
							FROM    sys.configurations
							WHERE   name = 'min server memory (MB)'
				ELSE
					INSERT  INTO #ConfigurationDefaults
					VALUES  ( 'min server memory (MB)', 0, 1036 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'nested triggers', 1, 1037 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'network packet size (B)', 4096, 1038 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Ole Automation Procedures', 0, 1039 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'open objects', 0, 1040 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'optimize for ad hoc workloads', 0, 1041 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'PH timeout (s)', 60, 1042 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'precompute rank', 0, 1043 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'priority boost', 0, 1044 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'query governor cost limit', 0, 1045 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'query wait (s)', -1, 1046 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'recovery interval (min)', 0, 1047 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote access', 1, 1048 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote admin connections', 0, 1049 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote proc trans', 0, 1050 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote query timeout (s)', 600, 1051 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Replication XPs', 0, 1052 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'RPC parameter data validation', 0, 1053 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'scan for startup procs', 0, 1054 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'server trigger recursion', 1, 1055 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'set working set size', 0, 1056 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'show advanced options', 0, 1057 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'SMO and DMO XPs', 1, 1058 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'SQL Mail XPs', 0, 1059 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'transform noise words', 0, 1060 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'two digit year cutoff', 2049, 1061 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'user connections', 0, 1062 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'user options', 0, 1063 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Web Assistant Procedures', 0, 1064 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'xp_cmdshell', 0, 1065 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity64 mask', 0, 1066 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity64 I/O mask', 0, 1067 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'contained database authentication', 0, 1068 );
				/* SQL Server 2012 also changes a configuration default */
				IF @@VERSION LIKE '%Microsoft SQL Server 2005%'
					OR @@VERSION LIKE '%Microsoft SQL Server 2008%'
					BEGIN
						INSERT  INTO #ConfigurationDefaults
						VALUES  ( 'remote login timeout (s)', 20, 1069 );
					END
				ELSE
					BEGIN
						INSERT  INTO #ConfigurationDefaults
						VALUES  ( 'remote login timeout (s)', 10, 1070 );
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 22 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  cd.CheckID ,
										200 AS Priority ,
										'Non-Default Server Config' AS FindingsGroup ,
										cr.name AS Finding ,
										'http://BrentOzar.com/go/conf' AS URL ,
										( 'This sp_configure option has been changed.  Its default value is '
										  + COALESCE(CAST(cd.[DefaultValue] AS VARCHAR(100)),
													 '(unknown)')
										  + ' and it has been set to '
										  + CAST(cr.value_in_use AS VARCHAR(100))
										  + '.' ) AS Details
								FROM    sys.configurations cr
										INNER JOIN #ConfigurationDefaults cd ON cd.name = cr.name
										LEFT OUTER JOIN #ConfigurationDefaults cdUsed ON cdUsed.name = cr.name
																  AND cdUsed.DefaultValue = cr.value_in_use
								WHERE   cdUsed.name IS NULL;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 24 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										24 AS CheckID ,
										DB_NAME(database_id) AS DatabaseName ,
										20 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'System Database on C Drive' AS Finding ,
										'http://BrentOzar.com/go/cdrive' AS URL ,
										( 'The ' + DB_NAME(database_id)
										  + ' database has a file on the C drive.  Putting system databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) IN ( 'master',
																  'model', 'msdb' );
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 25 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT TOP 1
										25 AS CheckID ,
										'tempdb' ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'TempDB on C Drive' AS Finding ,
										'http://BrentOzar.com/go/cdrive' AS URL ,
										CASE WHEN growth > 0
											 THEN ( 'The tempdb database has files on the C drive.  TempDB frequently grows unpredictably, putting your server at risk of running out of C drive space and crashing hard.  C is also often much slower than other drives, so performance may be suffering.' )
											 ELSE ( 'The tempdb database has files on the C drive.  TempDB is not set to Autogrow, hopefully it is big enough.  C is also often much slower than other drives, so performance may be suffering.' )
										END AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) = 'tempdb';
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 26 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										26 AS CheckID ,
										DB_NAME(database_id) AS DatabaseName ,
										20 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'User Databases on C Drive' AS Finding ,
										'http://BrentOzar.com/go/cdrive' AS URL ,
										( 'The ' + DB_NAME(database_id)
										  + ' database has a file on the C drive.  Putting databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) NOT IN ( 'master',
																  'model', 'msdb',
																  'tempdb' )
										AND DB_NAME(database_id) NOT IN (
										SELECT DISTINCT
												DatabaseName
										FROM    #SkipChecks )
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 27 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  27 AS CheckID ,
										'master' AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the Master Database' AS Finding ,
										'http://BrentOzar.com/go/mastuser' AS URL ,
										( 'The ' + name
										  + ' table in the master database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the master database may not be restored in the event of a disaster.' ) AS Details
								FROM    master.sys.tables
								WHERE   is_ms_shipped = 0;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 28 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  28 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the MSDB Database' AS Finding ,
										'http://BrentOzar.com/go/msdbuser' AS URL ,
										( 'The ' + name
										  + ' table in the msdb database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the msdb database may not be restored in the event of a disaster.' ) AS Details
								FROM    msdb.sys.tables
								WHERE   is_ms_shipped = 0;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 29 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  29 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the Model Database' AS Finding ,
										'http://BrentOzar.com/go/model' AS URL ,
										( 'The ' + name
										  + ' table in the model database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the model database are automatically copied into all new databases.' ) AS Details
								FROM    model.sys.tables
								WHERE   is_ms_shipped = 0;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 30 )
					BEGIN
						IF ( SELECT COUNT(*)
							 FROM   msdb.dbo.sysalerts
							 WHERE  severity BETWEEN 19 AND 25
						   ) < 7
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  30 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'Not All Alerts Configured' AS Finding ,
											'http://BrentOzar.com/go/alert' AS URL ,
											( 'Not all SQL Server Agent alerts have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
					END



				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 59 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 1
											AND COALESCE(has_notification, 0) = 0
											AND (job_id IS NULL OR job_id = 0x))
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  59 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'Alerts Configured without Follow Up' AS Finding ,
											'http://BrentOzar.com/go/alert' AS URL ,
											( 'SQL Server Agent alerts have been configured but they either do not notify anyone or else they do not take any action.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 96 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysalerts
										WHERE   message_id IN ( 823, 824, 825 ) )
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  96 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'No Alerts for Corruption' AS Finding ,
											'http://BrentOzar.com/go/alert' AS URL ,
											( 'SQL Server Agent alerts do not exist for errors 823, 824, and 825.  These three errors can give you notification about early hardware failure. Enabling them can prevent you a lot of heartbreak.' ) AS Details;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 61 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysalerts
										WHERE   severity BETWEEN 19 AND 25 )
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  61 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'No Alerts for Sev 19-25' AS Finding ,
											'http://BrentOzar.com/go/alert' AS URL ,
											( 'SQL Server Agent alerts do not exist for severity levels 19 through 25.  These are some very severe SQL Server errors. Knowing that these are happening may let you recover from errors faster.' ) AS Details;
					END

		--check for disabled alerts
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 98 )
					BEGIN
						IF EXISTS ( SELECT  name
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 0 )
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  98 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'Alerts Disabled' AS Finding ,
											'http://www.BrentOzar.com/go/alerts/' AS URL ,
											( 'The following Alert is disabled, please review and enable if desired: '
											  + name ) AS Details
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 0
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 31 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysoperators
										WHERE   enabled = 1 )
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  31 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingsGroup ,
											'No Operators Configured/Enabled' AS Finding ,
											'http://BrentOzar.com/go/op' AS URL ,
											( 'No SQL Server Agent operators (emails) have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 33 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults
					(CheckID,
					DatabaseName,
					Priority,
					FindingsGroup,
					Finding,
					URL,
					Details)
		  SELECT DISTINCT 33,
		  db_name(),
		  200,
		  ''Licensing'',
		  ''Enterprise Edition Features In Use'',
		  ''http://BrentOzar.com/go/ee'',
		  (''The ['' + DB_NAME() + ''] database is using '' + feature_name + ''.  If this database is restored onto a Standard Edition server, the restore will fail.'')
		  FROM [?].sys.dm_db_persisted_sku_features';
							END;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 34 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    sys.all_objects
									WHERE   name = 'dm_db_mirroring_auto_page_repair' )
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  34 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''http://BrentOzar.com/go/repair'' AS URL ,
		  ( ''Database mirroring has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_db_mirroring_auto_page_repair.'' ) AS Details
		  FROM (SELECT rp2.database_id, rp2.modification_time 
			FROM sys.dm_db_mirroring_auto_page_repair rp2 
			WHERE rp2.[database_id] not in (
			SELECT db2.[database_id] 
			FROM sys.databases as db2 
			WHERE db2.[state] = 1
			) ) as rp 
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE()) ;'
								EXECUTE(@StringToExecute)
							END;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 89 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    sys.all_objects
									WHERE   name = 'dm_hadr_auto_page_repair' )
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  89 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''http://BrentOzar.com/go/repair'' AS URL ,
		  ( ''AlwaysOn has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_hadr_auto_page_repair.'' ) AS Details
		  FROM    sys.dm_hadr_auto_page_repair rp
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE()) ;'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 90 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    msdb.sys.all_objects
									WHERE   name = 'suspect_pages' )
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  90 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''http://BrentOzar.com/go/repair'' AS URL ,
		  ( ''SQL Server has detected at least one corrupt page in the last 30 days. For more information, query the system table msdb.dbo.suspect_pages.'' ) AS Details
		  FROM    msdb.dbo.suspect_pages sp
		  INNER JOIN master.sys.databases db ON sp.database_id = db.database_id
		  WHERE   sp.last_update_date >= DATEADD(dd, -30, GETDATE()) ;'
								EXECUTE(@StringToExecute)
							END;
					END


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 36 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										36 AS CheckID ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Slow Storage Reads on Drive '
										+ UPPER(LEFT(mf.physical_name, 1)) AS Finding ,
										'http://BrentOzar.com/go/slow' AS URL ,
										'Reads are averaging longer than 100ms for at least one database on this drive.  For specific database file speeds, run the query from the information link.' AS Details
								FROM    sys.dm_io_virtual_file_stats(NULL, NULL)
										AS fs
										INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
																  AND fs.[file_id] = mf.[file_id]
								WHERE   ( io_stall_read_ms / ( 1.0 + num_of_reads ) ) > 100;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 37 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										37 AS CheckID ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Slow Storage Writes on Drive '
										+ UPPER(LEFT(mf.physical_name, 1)) AS Finding ,
										'http://BrentOzar.com/go/slow' AS URL ,
										'Writes are averaging longer than 20ms for at least one database on this drive.  For specific database file speeds, run the query from the information link.' AS Details
								FROM    sys.dm_io_virtual_file_stats(NULL, NULL)
										AS fs
										INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
																  AND fs.[file_id] = mf.[file_id]
								WHERE   ( io_stall_write_ms / ( 1.0
																+ num_of_writes ) ) > 20;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 40 )
					BEGIN
						IF ( SELECT COUNT(*)
							 FROM   tempdb.sys.database_files
							 WHERE  type_desc = 'ROWS'
						   ) = 1
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  DatabaseName ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
								VALUES  ( 40 ,
										  'tempdb' ,
										  100 ,
										  'Performance' ,
										  'TempDB Only Has 1 Data File' ,
										  'http://BrentOzar.com/go/tempdb' ,
										  'TempDB is only configured with one data file.  More data files are usually required to alleviate SGAM contention.'
										);
							END;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 41 )
					BEGIN
						EXEC dbo.sp_MSforeachdb 'use [?];
		  INSERT INTO #BlitzResults
		  (CheckID,
		  DatabaseName,
		  Priority,
		  FindingsGroup,
		  Finding,
		  URL,
		  Details)
		  SELECT 41,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Multiple Log Files on One Drive'',
		  ''http://BrentOzar.com/go/manylogs'',
		  (''The ['' + DB_NAME() + ''] database has multiple log files on the '' + LEFT(physical_name, 1) + '' drive. This is not a performance booster because log file access is sequential, not parallel.'')
		  FROM [?].sys.database_files WHERE type_desc = ''LOG''
			AND ''?'' <> ''[tempdb]''
		  GROUP BY LEFT(physical_name, 1)
		  HAVING COUNT(*) > 1';
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 42 )
					BEGIN
						EXEC dbo.sp_MSforeachdb 'use [?];
			INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
			SELECT DISTINCT 42,
			''?'',
			100,
			''Performance'',
			''Uneven File Growth Settings in One Filegroup'',
			''http://BrentOzar.com/go/grow'',
			(''The ['' + DB_NAME() + ''] database has multiple data files in one filegroup, but they are not all set up to grow in identical amounts.  This can lead to uneven file activity inside the filegroup.'')
			FROM [?].sys.database_files
			WHERE type_desc = ''ROWS''
			GROUP BY data_space_id
			HAVING COUNT(DISTINCT growth) > 1 OR COUNT(DISTINCT is_percent_growth) > 1';
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 44 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  44 AS CheckID ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Queries Forcing Order Hints' AS Finding ,
										'http://BrentOzar.com/go/hints' AS URL ,
										CAST(occurrence AS VARCHAR(10))
										+ ' instances of order hinting have been recorded since restart.  This means queries are bossing the SQL Server optimizer around, and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' AS Details
								FROM    sys.dm_exec_query_optimizer_info
								WHERE   counter = 'order hint'
										AND occurrence > 1
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 45 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  45 AS CheckID ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Queries Forcing Join Hints' AS Finding ,
										'http://BrentOzar.com/go/hints' AS URL ,
										CAST(occurrence AS VARCHAR(10))
										+ ' instances of join hinting have been recorded since restart.  This means queries are bossing the SQL Server optimizer around, and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' AS Details
								FROM    sys.dm_exec_query_optimizer_info
								WHERE   counter = 'join hint'
										AND occurrence > 1
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 49 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT DISTINCT
										49 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Linked Server Configured' AS Finding ,
										'http://BrentOzar.com/go/link' AS URL ,
										+CASE WHEN l.remote_name = 'sa'
											  THEN s.data_source
												   + ' is configured as a linked server. Check its security configuration as it is connecting with sa, because any user who queries it will get admin-level permissions.'
											  ELSE s.data_source
												   + ' is configured as a linked server. Check its security configuration to make sure it isn''t connecting with SA or some other bone-headed administrative login, because any user who queries it might get admin-level permissions.'
										 END AS Details
								FROM    sys.servers s
										INNER JOIN sys.linked_logins l ON s.server_id = l.server_id
								WHERE   s.is_linked = 1
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 50 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		  SELECT  50 AS CheckID ,
		  100 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Max Memory Set Too High'' AS Finding ,
		  ''http://BrentOzar.com/go/max'' AS URL ,
		  ''SQL Server max memory is set to ''
			+ CAST(c.value_in_use AS VARCHAR(20))
			+ '' megabytes, but the server only has ''
			+ CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes.  SQL Server may drain the system dry of memory, and under certain conditions, this can cause Windows to swap to disk.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  INNER JOIN sys.configurations c ON c.name = ''max server memory (MB)''
		  WHERE   CAST(m.total_physical_memory_kb AS BIGINT) < ( CAST(c.value_in_use AS BIGINT) * 1024 )'
								EXECUTE(@StringToExecute)
							END;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 51 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		  SELECT  51 AS CheckID ,
		  1 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Memory Dangerously Low'' AS Finding ,
		  ''http://BrentOzar.com/go/max'' AS URL ,
		  ''The server has '' + CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20)) + '' megabytes of physical memory, but only '' + CAST(( CAST(m.available_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes are available.  As the server runs out of memory, there is danger of swapping to disk, which will kill performance.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  WHERE   CAST(m.available_physical_memory_kb AS BIGINT) < 262144'
								EXECUTE(@StringToExecute)
							END;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 53 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT TOP 1
										53 AS CheckID ,
										200 AS Priority ,
										'High Availability' AS FindingsGroup ,
										'Cluster Node' AS Finding ,
										'http://BrentOzar.com/go/node' AS URL ,
										'This is a node in a cluster.' AS Details
								FROM    sys.dm_os_cluster_nodes
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 55 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  55 AS CheckID ,
										[name] AS DatabaseName ,
										200 AS Priority ,
										'Security' AS FindingsGroup ,
										'Database Owner <> SA' AS Finding ,
										'http://BrentOzar.com/go/owndb' AS URL ,
										( 'Database name: ' + [name] + '   '
										  + 'Owner name: ' + SUSER_SNAME(owner_sid) ) AS Details
								FROM    sys.databases
								WHERE   SUSER_SNAME(owner_sid) <> SUSER_SNAME(0x01)
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks );
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 57 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  57 AS CheckID ,
										10 AS Priority ,
										'Security' AS FindingsGroup ,
										'SQL Agent Job Runs at Startup' AS Finding ,
										'http://BrentOzar.com/go/startup' AS URL ,
										( 'Job [' + j.name
										  + '] runs automatically when SQL Server Agent starts up.  Make sure you know exactly what this job is doing, because it could pose a security risk.' ) AS Details
								FROM    msdb.dbo.sysschedules sched
										JOIN msdb.dbo.sysjobschedules jsched ON sched.schedule_id = jsched.schedule_id
										JOIN msdb.dbo.sysjobs j ON jsched.job_id = j.job_id
								WHERE   sched.freq_type = 64;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 58 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  58 AS CheckID ,
										d.[name] AS DatabaseName ,
										200 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Database Collation Mismatch' AS Finding ,
										'http://BrentOzar.com/go/collate' AS URL ,
										( 'Database ' + d.NAME + ' has collation '
										  + d.collation_name
										  + '; Server collation is '
										  + CONVERT(VARCHAR(100), SERVERPROPERTY('collation')) ) AS Details
								FROM    master.sys.databases d
								WHERE   d.collation_name <> SERVERPROPERTY('collation')
										AND d.name NOT IN ( SELECT DISTINCT
																  DatabaseName
															FROM  #SkipChecks )
										AND d.name NOT LIKE 'ReportServer%'
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 82 )
					BEGIN
						EXEC sp_MSforeachdb 'use [?];
		INSERT INTO #BlitzResults
		(CheckID,
		DatabaseName,
		Priority,
		FindingsGroup,
		Finding,
		URL, Details)
		SELECT  DISTINCT 82 AS CheckID,
		''?'' as DatabaseName,
		100 AS Priority,
		''Performance'' AS FindingsGroup,
		''File growth set to percent'',
		''http://brentozar.com/go/percentgrowth'' AS URL,
		''The ['' + DB_NAME() + ''] database is using percent filegrowth settings. This can lead to out of control filegrowth.''
		FROM    [?].sys.database_files
		WHERE   is_percent_growth = 1 ';
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 97 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  97 AS CheckID ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Unusual SQL Server Edition' AS Finding ,
										'http://BrentOzar.com/go/workgroup' AS URL ,
										( 'This server is using '
										  + CAST(SERVERPROPERTY('edition') AS VARCHAR(100))
										  + ', which is capped at low amounts of CPU and memory.' ) AS Details
								WHERE   CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Standard%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Enterprise%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Developer%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Business Intelligence%'
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 62 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  62 AS CheckID ,
										[name] AS DatabaseName ,
										200 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Old Compatibility Level' AS Finding ,
										'http://BrentOzar.com/go/compatlevel' AS URL ,
										( 'Database ' + [name]
										  + ' is compatibility level '
										  + CAST(compatibility_level AS VARCHAR(20))
										  + ', which may cause unwanted results when trying to run queries that have newer T-SQL features.' ) AS Details
								FROM    sys.databases
								WHERE   name NOT IN ( SELECT DISTINCT
																DatabaseName
													  FROM      #SkipChecks )
										AND compatibility_level <> ( SELECT
																  compatibility_level
																  FROM
																  sys.databases
																  WHERE
																  [name] = 'model'
																  )
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 94 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  94 AS CheckID ,
										50 AS [Priority] ,
										'Reliability' AS FindingsGroup ,
										'Agent Jobs Without Failure Emails' AS Finding ,
										'http://BrentOzar.com/go/alerts' AS URL ,
										'The job ' + [name]
										+ ' has not been set up to notify an operator if it fails.' AS Details
								FROM    msdb.[dbo].[sysjobs] j
										INNER JOIN ( SELECT DISTINCT
															[job_id]
													 FROM   [msdb].[dbo].[sysjobschedules]
													 WHERE  next_run_date > 0
												   ) s ON j.job_id = s.job_id
								WHERE   j.enabled = 1
										AND j.notify_email_operator_id = 0
										AND j.notify_netsend_operator_id = 0
										AND j.notify_page_operator_id = 0
										AND j.category_id <> 100 /* Exclude SSRS category */
					END


				IF EXISTS ( SELECT  1
							FROM    sys.configurations
							WHERE   name = 'remote admin connections'
									AND value_in_use = 0 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 100 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  100 AS CheckID ,
										50 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Remote DAC Disabled' AS Finding ,
										'http://BrentOzar.com/go/dac' AS URL ,
										'Remote access to the Dedicated Admin Connection (DAC) is not enabled. The DAC can make remote troubleshooting much easier when SQL Server is unresponsive.'
					END


				IF EXISTS ( SELECT  *
							FROM    sys.dm_os_schedulers
							WHERE   is_online = 0 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 101 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  101 AS CheckID ,
										50 AS Priority ,
										'Performance' AS FindingGroup ,
										'CPU Schedulers Offline' AS Finding ,
										'http://BrentOzar.com/go/schedulers' AS URL ,
										'Some CPU cores are not accessible to SQL Server due to affinity masking or licensing problems.'
					END


					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 110 )
								AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'dm_os_memory_nodes')
						BEGIN
							SET @StringToExecute = 'IF EXISTS (SELECT  *
												FROM sys.dm_os_nodes n
												INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
												WHERE n.node_state_desc = ''OFFLINE'')
												INSERT  INTO #BlitzResults
														( CheckID ,
														  Priority ,
														  FindingsGroup ,
														  Finding ,
														  URL ,
														  Details
														)
														SELECT  110 AS CheckID ,
																50 AS Priority ,
																''Performance'' AS FindingGroup ,
																''Memory Nodes Offline'' AS Finding ,
																''http://BrentOzar.com/go/schedulers'' AS URL ,
																''Due to affinity masking or licensing problems, some of the memory may not be available.''';
									EXECUTE(@StringToExecute);
						END


				IF EXISTS ( SELECT  *
							FROM    sys.databases
							WHERE   state > 1 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 102 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  102 AS CheckID ,
										[name] ,
										20 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Unusual Database State: ' + [state_desc] AS Finding ,
										'http://BrentOzar.com/go/repair' AS URL ,
										'This database may not be online.'
								FROM    sys.databases
								WHERE   state > 1
					END

				IF EXISTS ( SELECT  *
							FROM    master.sys.extended_procedures )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 105 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  105 AS CheckID ,
										'master' ,
										50 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Extended Stored Procedures in Master' AS Finding ,
										'http://BrentOzar.com/go/clr' AS URL ,
										'The [' + name
										+ '] extended stored procedure is in the master database. CLR may be in use, and the master database now needs to be part of your backup/recovery planning.'
								FROM    master.sys.extended_procedures
					END



					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 107 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  107 AS CheckID ,
											100 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: THREADPOOL'  AS Finding ,
											'http://BrentOzar.com/go/poison' AS URL ,
											CAST(SUM([wait_time_ms]) AS VARCHAR(100)) + ' milliseconds of this wait have been recorded. This wait often indicates killer performance problems.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type = 'THREADPOOL'
									GROUP BY wait_type
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
						END

					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 108 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  108 AS CheckID ,
											100 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: RESOURCE_SEMAPHORE'  AS Finding ,
											'http://BrentOzar.com/go/poison' AS URL ,
											CAST(SUM([wait_time_ms]) AS VARCHAR(100)) + ' milliseconds of this wait have been recorded. This wait often indicates killer performance problems.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type = 'RESOURCE_SEMAPHORE'
									GROUP BY wait_type
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
						END


					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 109 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  109 AS CheckID ,
											100 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: RESOURCE_SEMAPHORE_QUERY_COMPILE'  AS Finding ,
											'http://BrentOzar.com/go/poison' AS URL ,
											CAST(SUM([wait_time_ms]) AS VARCHAR(100)) + ' milliseconds of this wait have been recorded. This wait often indicates killer performance problems.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type = 'RESOURCE_SEMAPHORE_QUERY_COMPILE'
									GROUP BY wait_type
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
						END


					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 121 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  121 AS CheckID ,
											100 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: Serializable Locking'  AS Finding ,
											'http://BrentOzar.com/go/serializable' AS URL ,
											CAST(SUM([wait_time_ms]) / 1000 AS VARCHAR(100)) + ' seconds of this wait have been recorded. Queries are forcing serial operation (one query at a time) with lock hints.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type LIKE '%LCK%R%'
									GROUP BY wait_type
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
						END



						IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 111 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  DatabaseName ,
									  URL ,
									  Details
									)
									SELECT  111 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingGroup ,
											'Possibly Broken Log Shipping'  AS Finding ,
											d.[name] ,
											'http://BrentOzar.com/go/shipping' AS URL ,
											d.[name] + ' is in a restoring state, but has not had a backup applied in the last two days. This is a possible indication of a broken transaction log shipping setup.'
											FROM [master].sys.databases d
											INNER JOIN [master].sys.database_mirroring dm ON d.database_id = dm.database_id
												AND dm.mirroring_role IS NULL
											WHERE ( d.[state] = 1
											OR (d.[state] = 0 AND d.[is_in_standby] = 1) )
											AND NOT EXISTS(SELECT * FROM msdb.dbo.restorehistory rh
											INNER JOIN msdb.dbo.backupset bs ON rh.backup_set_id = bs.backup_set_id
											WHERE d.[name] COLLATE SQL_Latin1_General_CP1_CI_AS = rh.destination_database_name COLLATE SQL_Latin1_General_CP1_CI_AS
											AND rh.restore_date >= DATEADD(dd, -2, GETDATE()))

						END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 112 )
									AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'change_tracking_databases')
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
							  SELECT 112 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Change Tracking Enabled'' AS Finding,
							  ''http://BrentOzar.com/go/tracking'' AS URL,
							  ( d.[name] + '' has change tracking enabled. This is not a default setting, and it has some performance overhead. It keeps track of changes to rows in tables that have change tracking turned on.'' ) AS Details FROM sys.change_tracking_databases AS ctd INNER JOIN sys.databases AS d ON ctd.database_id = d.database_id';
										EXECUTE(@StringToExecute);
							END

						IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 116 )
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  116 AS CheckID ,
											200 AS Priority ,
											'Informational' AS FindingGroup ,
											'Backup Compression Default Off'  AS Finding ,
											'http://BrentOzar.com/go/backup' AS URL ,
											'Backup compression is included with SQL Server 2008R2 & newer, even in Standard Edition. We recommend turning backup compression on by default so that ad-hoc backups will get compressed.'
											FROM sys.configurations
											WHERE configuration_id = 1579 AND CAST(value_in_use AS INT) = 0

						END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 117 )
									AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'dm_exec_query_resource_semaphores')
							BEGIN
								SET @StringToExecute = 'IF 0 < (SELECT SUM([forced_grant_count]) FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL)
								INSERT INTO #BlitzResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
							  SELECT 117 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Memory Pressure Affecting Queries'' AS Finding,
							  ''http://BrentOzar.com/go/grants'' AS URL,
							  CAST(SUM(forced_grant_count) AS NVARCHAR(100)) + '' forced grants reported in the DMV sys.dm_exec_query_resource_semaphores, indicating memory pressure has affected query runtimes.''
							  FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL;'
										EXECUTE(@StringToExecute);
							END



						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 124 )
							BEGIN
								INSERT INTO #BlitzResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
								SELECT 124, 100, 'Performance', 'Deadlocks Happening Daily', 'http://BrentOzar.com/go/deadlocks',
									CAST(p.cntr_value AS NVARCHAR(100)) + ' deadlocks have been recorded since startup.' AS Details
								FROM sys.dm_os_performance_counters p
									INNER JOIN sys.databases d ON d.name = 'tempdb'
								WHERE RTRIM(p.counter_name) = 'Number of Deadlocks/sec'
									AND RTRIM(p.instance_name) = '_Total'
									AND p.cntr_value > 0
									AND (1.0 * p.cntr_value / NULLIF(datediff(DD,create_date,CURRENT_TIMESTAMP),0)) > 10;
							END


						IF DATEADD(mi, -15, GETDATE()) < (SELECT TOP 1 creation_time FROM sys.dm_exec_query_stats ORDER BY creation_time)
						BEGIN
							INSERT INTO #BlitzResults
								(CheckID,
								Priority,
								FindingsGroup,
								Finding,
								URL,
								Details)
							SELECT TOP 1 125, 10, 'Performance', 'Plan Cache Erased Recently', 'http://BrentOzar.com/askbrent/plan-cache-erased-recently/',
								'The oldest query in the plan cache was created at ' + CAST(creation_time AS NVARCHAR(50)) + '. Someone ran DBCC FREEPROCCACHE, restarted SQL Server, or it is under horrific memory pressure.'
							FROM sys.dm_exec_query_stats WITH (NOLOCK)
							ORDER BY creation_time	
						END;

						IF EXISTS (SELECT * FROM sys.configurations WHERE name = 'priority boost' AND (value = 1 OR value_in_use = 1))
						BEGIN
							INSERT INTO #BlitzResults
								(CheckID,
								Priority,
								FindingsGroup,
								Finding,
								URL,
								Details)
							VALUES(126, 5, 'Reliability', 'Priority Boost Enabled', 'http://BrentOzar.com/go/priorityboost/',
								'Priority Boost sounds awesome, but it can actually cause your SQL Server to crash.')
						END;

						IF EXISTS (select * from msdb.dbo.backupset WHERE database_name = 'ReportServerTempDB')
						BEGIN
							INSERT INTO #BlitzResults
								(CheckID,
								Priority,
								DatabaseName,
								FindingsGroup,
								Finding,
								URL,
								Details)
							VALUES(127, 200, 'ReportServerTempDB', 'Backup', 'Backing Up Unneeded Database', 'http://BrentOzar.com/go/reportservertempdb/',
								'This database is being backed up, but you probably do not need to. See the URL for more details on how to reconstruct it.')
						END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 128 )
							BEGIN

							IF (@ProductVersionMajor = 12 AND @ProductVersionMinor < 2000) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor <= 2100) OR
							   (@ProductVersionMajor = 10.5 AND @ProductVersionMinor <= 2500) OR
							   (@ProductVersionMajor = 10 AND @ProductVersionMinor <= 4000) OR
							   (@ProductVersionMajor = 9 AND @ProductVersionMinor <= 5000)
								BEGIN
								INSERT INTO #BlitzResults(CheckID, Priority, FindingsGroup, Finding, URL, Details)
									VALUES(128, 20, 'Reliability', 'Unsupported Build of SQL Server', 'http://BrentOzar.com/go/unsupported',
										'Version ' + CAST(@ProductVersionMajor AS VARCHAR(100)) + '.' + CAST(@ProductVersionMinor AS VARCHAR(100)) + ' is no longer supported by Microsoft. You need to apply a service pack.');
								END;

							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 129 )
							BEGIN
							IF (@ProductVersionMajor = 11 AND @ProductVersionMinor >= 3000 AND @ProductVersionMinor <= 3436) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor = 5058) OR
							   (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 2000 AND @ProductVersionMinor <= 2342)
								BEGIN
								INSERT INTO #BlitzResults(CheckID, Priority, FindingsGroup, Finding, URL, Details)
									VALUES(129, 20, 'Reliability', 'Dangerous Build of SQL Server', 'http://sqlperformance.com/2014/06/sql-indexes/hotfix-sql-2012-rebuilds',
										'There are dangerous known bugs with version ' + CAST(@ProductVersionMajor AS VARCHAR(100)) + '.' + CAST(@ProductVersionMinor AS VARCHAR(100)) + '. Check the URL for details and apply the right service pack or hotfix.');
								END;

							END;
							
				IF @CheckUserDatabaseObjects = 1
					BEGIN

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 32 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
			SELECT DISTINCT 32,
			''?'',
			110,
			''Performance'',
			''Triggers on Tables'',
			''http://BrentOzar.com/go/trig'',
			(''The ['' + DB_NAME() + ''] database has triggers on the '' + s.name + ''.'' + o.name + '' table.'')
			FROM [?].sys.triggers t INNER JOIN [?].sys.objects o ON t.parent_id = o.object_id
			INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE t.is_ms_shipped = 0 AND DB_NAME() != ''ReportServer''';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 38 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
		  SELECT DISTINCT 38,
		  ''?'',
		  110,
		  ''Performance'',
		  ''Active Tables Without Clustered Indexes'',
		  ''http://BrentOzar.com/go/heaps'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that are being actively queried.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = ''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NOT NULL
		  AND sd.name <> ''tempdb'' AND o.is_ms_shipped = 0 AND o.type <> ''S''';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 39 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
		  SELECT DISTINCT 39,
		  ''?'',
		  110,
		  ''Performance'',
		  ''Inactive Tables Without Clustered Indexes'',
		  ''http://BrentOzar.com/go/heaps'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that have not been queried since the last restart.  These may be backup tables carelessly left behind.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = ''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NULL
		  AND sd.name <> ''tempdb'' AND o.is_ms_shipped = 0 AND o.type <> ''S''';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 46 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 46,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Leftover Fake Indexes From Wizards'',
		  ''http://BrentOzar.com/go/hypo'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is a leftover hypothetical index from the Index Tuning Wizard or Database Tuning Advisor.  This index is not actually helping performance and should be removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_hypothetical = 1';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 47 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 47,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Indexes Disabled'',
		  ''http://BrentOzar.com/go/ixoff'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is disabled.  This index is not actually helping performance and should either be enabled or removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_disabled = 1';
							END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 48 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT 48,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Foreign Keys Not Trusted'',
		  ''http://BrentOzar.com/go/trust'',
		  (''The ['' + DB_NAME() + ''] database has foreign keys that were probably disabled, data was changed, and then the key was enabled again.  Simply enabling the key is not enough for the optimizer to use this key - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.foreign_keys i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 56 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 56,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Check Constraint Not Trusted'',
		  ''http://BrentOzar.com/go/trust'',
		  (''The check constraint ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is not trusted - meaning, it was disabled, data was changed, and then the constraint was enabled again.  Simply enabling the constraint is not enough for the optimizer to use this constraint - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.check_constraints i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id
		  INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 95 )
							BEGIN
								IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
									AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
									BEGIN
										EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #BlitzResults
				  (CheckID,
				  DatabaseName,
				  Priority,
				  FindingsGroup,
				  Finding,
				  URL,
				  Details)
			SELECT TOP 1 95 AS CheckID,
			''?'' as DatabaseName,
			110 AS Priority,
			''Performance'' AS FindingsGroup,
			''Plan Guides Enabled'' AS Finding,
			''http://BrentOzar.com/go/guides'' AS URL,
			(''Database ['' + DB_NAME() + ''] has query plan guides so a query will always get a specific execution plan. If you are having trouble getting query performance to improve, it might be due to a frozen plan. Review the DMV sys.plan_guides to learn more about the plan guides in place on this server.'') AS Details
			FROM [?].sys.plan_guides WHERE is_disabled = 0'
									END;
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 60 )
							BEGIN
								EXEC sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT  DISTINCT 60 AS CheckID,
		  ''?'' as DatabaseName,
		  100 AS Priority,
		  ''Performance'' AS FindingsGroup,
		  ''Fill Factor Changed'',
		  ''http://brentozar.com/go/fillfactor'' AS URL,
		  ''The ['' + DB_NAME() + ''] database has objects with fill factor < 80%. This can cause memory and storage performance problems, but may also prevent page splits.''
		  FROM    [?].sys.indexes
		  WHERE   fill_factor <> 0 AND fill_factor < 80 AND is_disabled = 0 AND is_hypothetical = 0';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 78 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 78,
		  ''?'',
		  100,
		  ''Performance'',
		  ''Stored Procedure WITH RECOMPILE'',
		  ''http://BrentOzar.com/go/recompile'',
		  (''['' + DB_NAME() + ''].['' + SPECIFIC_SCHEMA + ''].['' + SPECIFIC_NAME + ''] has WITH RECOMPILE in the stored procedure code, which may cause increased CPU usage due to constant recompiles of the code.'')
		  from [?].INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_DEFINITION LIKE N''%WITH RECOMPILE%''';
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 86 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 86, DB_NAME(), 20, ''Security'', ''Elevated Permissions on a Database'', ''http://BrentOzar.com/go/elevated'', (''In ['' + DB_NAME() + ''], user ['' + u.name + '']  has the role ['' + g.name + ''].  This user can perform tasks beyond just reading and writing data.'') FROM [?].dbo.sysmembers m inner join [?].dbo.sysusers u on m.memberuid = u.uid inner join sysusers g on m.groupuid = g.uid where u.name <> ''dbo'' and g.name in (''db_owner'' , ''db_accessAdmin'' , ''db_securityadmin'' , ''db_ddladmin'')';
							END


							/*Check for non-aligned indexes in partioned databases*/

										IF NOT EXISTS ( SELECT  1
														FROM    #SkipChecks
														WHERE   DatabaseName IS NULL AND CheckID = 72 )
											BEGIN
												EXEC dbo.sp_MSforeachdb 'USE [?];
								insert into #partdb(dbname, objectname, type_desc)
								SELECT distinct db_name(DB_ID()) as DBName,o.name Object_Name,ds.type_desc
								FROM sys.objects AS o JOIN sys.indexes AS i ON o.object_id = i.object_id
								JOIN sys.data_spaces ds on ds.data_space_id = i.data_space_id
								LEFT OUTER JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
								WHERE  o.type = ''u''
								 -- Clustered and Non-Clustered indexes
								AND i.type IN (1, 2)
								AND o.object_id in
								  (
									SELECT a.object_id from
									  (SELECT ob.object_id, ds.type_desc from sys.objects ob JOIN sys.indexes ind on ind.object_id = ob.object_id join sys.data_spaces ds on ds.data_space_id = ind.data_space_id
									  GROUP BY ob.object_id, ds.type_desc ) a group by a.object_id having COUNT (*) > 1
								  )'
												INSERT  INTO #BlitzResults
														( CheckID ,
														  DatabaseName ,
														  Priority ,
														  FindingsGroup ,
														  Finding ,
														  URL ,
														  Details
														)
														SELECT DISTINCT
																72 AS CheckID ,
																dbname AS DatabaseName ,
																100 AS Priority ,
																'Performance' AS FindingsGroup ,
																'The partitioned database ' + dbname
																+ ' may have non-aligned indexes' AS Finding ,
																'http://BrentOzar.com/go/aligned' AS URL ,
																'Having non-aligned indexes on partitioned tables may cause inefficient query plans and CPU pressure' AS Details
														FROM    #partdb
														WHERE   dbname IS NOT NULL
																AND dbname NOT IN ( SELECT DISTINCT
																						  DatabaseName
																					FROM  #SkipChecks )
												DROP TABLE #partdb
											END


											IF NOT EXISTS ( SELECT  1
															FROM    #SkipChecks
															WHERE   DatabaseName IS NULL AND CheckID = 113 )
												BEGIN
													EXEC dbo.sp_MSforeachdb 'USE [?];
							  INSERT INTO #BlitzResults
									(CheckID,
									DatabaseName,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
							  SELECT DISTINCT 113,
							  ''?'',
							  50,
							  ''Reliability'',
							  ''Full Text Indexes Not Updating'',
							  ''http://BrentOzar.com/go/fulltext'',
							  (''At least one full text index in this database has not been crawled in the last week.'')
							  from [?].sys.fulltext_indexes i WHERE i.is_enabled = 1 AND i.crawl_end_date < DATEADD(dd, -7, GETDATE())';
												END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 115 )
							BEGIN
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 115,
		  ''?'',
		  110,
		  ''Performance'',
		  ''Parallelism Rocket Surgery'',
		  ''http://BrentOzar.com/go/makeparallel'',
		  (''['' + DB_NAME() + ''] has a make_parallel function, indicating that an advanced developer may be manhandling SQL Server into forcing queries to go parallel.'')
		  from [?].INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = ''make_parallel'' AND ROUTINE_TYPE = ''FUNCTION''';
							END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 122 )
							BEGIN
								/* SQL Server 2012 and newer uses temporary stats for AlwaysOn Availability Groups, and those show up as user-created */
								IF EXISTS (SELECT *
									  FROM sys.all_columns c
									  INNER JOIN sys.all_objects o ON c.object_id = o.object_id
									  WHERE c.name = 'is_temporary' AND o.name = 'stats')

										EXEC dbo.sp_MSforeachdb 'USE [?];
												INSERT INTO #BlitzResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													URL,
													Details)
												SELECT TOP 1 122,
												''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												''http://BrentOzar.com/go/userstats'',
												(''['' + DB_NAME() + ''] has user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1 AND is_temporary = 0';

									ELSE
										EXEC dbo.sp_MSforeachdb 'USE [?];
												INSERT INTO #BlitzResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													URL,
													Details)
												SELECT TOP 1 122,
												''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												''http://BrentOzar.com/go/userstats'',
												(''['' + DB_NAME() + ''] has user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1';


							END /* IF NOT EXISTS ( SELECT  1 */


					END /* IF @CheckUserDatabaseObjects = 1 */

				IF @CheckProcedureCache = 1
					BEGIN

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 35 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
										SELECT  35 AS CheckID ,
												100 AS Priority ,
												'Performance' AS FindingsGroup ,
												'Single-Use Plans in Procedure Cache' AS Finding ,
												'http://BrentOzar.com/go/single' AS URL ,
												( CAST(COUNT(*) AS VARCHAR(10))
												  + ' query plans are taking up memory in the procedure cache. This may be wasted memory if we cache plans for queries that never get called again. This may be a good use case for SQL Server 2008''s Optimize for Ad Hoc or for Forced Parameterization.' ) AS Details
										FROM    sys.dm_exec_cached_plans AS cp
										WHERE   cp.usecounts = 1
												AND cp.objtype = 'Adhoc'
												AND EXISTS ( SELECT
																  1
															 FROM sys.configurations
															 WHERE
																  name = 'optimize for ad hoc workloads'
																  AND value_in_use = 0 )
										HAVING  COUNT(*) > 1;
							END


		  /* Set up the cache tables. Different on 2005 since it doesn't support query_hash, query_plan_hash. */
						IF @@VERSION LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								IF @CheckProcedureCacheFilter = 'CPU'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM sys.dm_exec_query_stats qs
			  ORDER BY qs.total_worker_time DESC)
			  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM queries qs
			  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'Reads'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'ExecCount'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'Duration'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM sys.dm_exec_query_stats qs
			ORDER BY qs.total_elapsed_time DESC)
			INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM queries qs
			LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

							END;
						IF @@VERSION LIKE '%Microsoft SQL Server 2008%'
							OR @@VERSION LIKE '%Microsoft SQL Server 2012%'
							OR @@VERSION LIKE '%Microsoft SQL Server 2014%'
							BEGIN
								IF @CheckProcedureCacheFilter = 'CPU'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_worker_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'Reads'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'ExecCount'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

								IF @CheckProcedureCacheFilter = 'Duration'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_elapsed_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL;'
										EXECUTE(@StringToExecute)
									END

		/* Populate the query_plan_filtered field. Only works in 2005SP2+, but we're just doing it in 2008 to be safe. */
								UPDATE  #dm_exec_query_stats
								SET     query_plan_filtered = qp.query_plan
								FROM    #dm_exec_query_stats qs
										CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
																  qs.statement_start_offset,
																  qs.statement_end_offset)
										AS qp

							END;

		/* Populate the additional query_plan, text, and text_filtered fields */
						UPDATE  #dm_exec_query_stats
						SET     query_plan = qp.query_plan ,
								[text] = st.[text] ,
								text_filtered = SUBSTRING(st.text,
														  ( qs.statement_start_offset
															/ 2 ) + 1,
														  ( ( CASE qs.statement_end_offset
																WHEN -1
																THEN DATALENGTH(st.text)
																ELSE qs.statement_end_offset
															  END
															  - qs.statement_start_offset )
															/ 2 ) + 1)
						FROM    #dm_exec_query_stats qs
								CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
								CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle)
								AS qp

		/* Dump instances of our own script. We're not trying to tune ourselves. */
						DELETE  #dm_exec_query_stats
						WHERE   text LIKE '%sp_Blitz%'
								OR text LIKE '%#BlitzResults%'

		/* Look for implicit conversions */

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 63 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  63 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Implicit Conversion' AS Finding ,
												'http://BrentOzar.com/go/implicit' AS URL ,
												( 'One of the top resource-intensive queries is comparing two fields that are not the same datatype.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%CONVERT_IMPLICIT%'
												AND COALESCE(qs.query_plan_filtered,
															 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%PhysicalOp="Index Scan"%'
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 64 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  64 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Implicit Conversion Affecting Cardinality' AS Finding ,
												'http://BrentOzar.com/go/implicit' AS URL ,
												( 'One of the top resource-intensive queries has an implicit conversion that is affecting cardinality estimation.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT_IMPLICIT%'
							END

							/* @cms4j, 29.11.2013: Look for RID or Key Lookups */
							IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 118 )
								BEGIN
									INSERT  INTO #BlitzResults
											( CheckID ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  URL ,
											  Details ,
											  QueryPlan ,
											  QueryPlanFiltered
											)
											SELECT  118 AS CheckID ,
													120 AS Priority ,
													'Query Plans' AS FindingsGroup ,
													'RID or Key Lookups' AS Finding ,
													'http://BrentOzar.com/go/lookup' AS URL ,
													'One of the top resource-intensive queries contains RID or Key Lookups. Try to avoid them by creating covering indexes.' AS Details ,
													qs.query_plan ,
													qs.query_plan_filtered
											FROM    #dm_exec_query_stats qs
											WHERE   COALESCE(qs.query_plan_filtered,
															 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%Lookup="1"%'
								END /* @cms4j, 29.11.2013: Look for RID or Key Lookups */


						/* Look for missing indexes */
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 65 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  65 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Missing Index' AS Finding ,
												'http://BrentOzar.com/go/missingindex' AS URL ,
												( 'One of the top resource-intensive queries may be dramatically improved by adding an index.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%MissingIndexGroup%'
							END

						/* Look for cursors */
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 66 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  66 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Cursor' AS Finding ,
												'http://BrentOzar.com/go/cursor' AS URL ,
												( 'One of the top resource-intensive queries is using a cursor.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<StmtCursor%'
							END

		/* Look for scalar user-defined functions */

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 67 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  67 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Scalar UDFs' AS Finding ,
												'http://BrentOzar.com/go/functions' AS URL ,
												( 'One of the top resource-intensive queries is using a user-defined scalar function that may inhibit parallelism.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<UserDefinedFunction%'
							END

					END /* IF @CheckProcedureCache = 1 */

		/*Check for the last good DBCC CHECKDB date */
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 68 )
					BEGIN
						EXEC sp_MSforeachdb N'USE [?];
		INSERT #DBCCs
			(ParentObject,
			Object,
			Field,
			Value)
		EXEC (''DBCC DBInfo() With TableResults, NO_INFOMSGS'');
		UPDATE #DBCCs SET DbName = N''?'' WHERE DbName IS NULL;';

						WITH    DB2
								  AS ( SELECT DISTINCT
												Field ,
												Value ,
												DbName
									   FROM     #DBCCs
									   WHERE    Field = 'dbi_dbccLastKnownGood'
									 )
							INSERT  INTO #BlitzResults
									( CheckID ,
									  DatabaseName ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  68 AS CheckID ,
											DB2.DbName AS DatabaseName ,
											50 AS PRIORITY ,
											'Reliability' AS FindingsGroup ,
											'Last good DBCC CHECKDB over 2 weeks old' AS Finding ,
											'http://BrentOzar.com/go/checkdb' AS URL ,
											'Database [' + DB2.DbName + ']'
											+ CASE DB2.Value
												WHEN '1900-01-01 00:00:00.000'
												THEN ' never had a successful DBCC CHECKDB.'
												ELSE ' last had a successful DBCC CHECKDB run on '
													 + DB2.Value + '.'
											  END
											+ ' This check should be run regularly to catch any database corruption as soon as possible.'
											+ ' Note: you can restore a backup of a busy production database to a test server and run DBCC CHECKDB '
											+ ' against that to minimize impact. If you do that, you can ignore this warning.' AS Details
									FROM    DB2
									WHERE   DB2.DbName NOT IN ( SELECT DISTINCT
																  DatabaseName
																FROM
																  #SkipChecks )
											AND CONVERT(DATETIME, DB2.Value, 121) < DATEADD(DD,
																  -14,
																  CURRENT_TIMESTAMP)
					END

		/*Check for high VLF count: this will omit any database snapshots*/

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 69 )
					BEGIN
						IF @@VERSION LIKE 'Microsoft SQL Server 2012%' OR @@VERSION LIKE 'Microsoft SQL Server 2014%'
							BEGIN
								EXEC sp_MSforeachdb N'USE [?];
		  INSERT INTO #LogInfo2012
		  EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		  IF    @@ROWCOUNT > 999
		  BEGIN
			INSERT  INTO #BlitzResults
			( CheckID
			,DatabaseName
			,Priority
			,FindingsGroup
			,Finding
			,URL
			,Details)
			SELECT      69
			,DB_NAME()
			,100
			,''Performance''
			,''High VLF Count''
			,''http://BrentOzar.com/go/vlf''
			,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			FROM #LogInfo2012
			WHERE EXISTS (SELECT name FROM master.sys.databases
					WHERE source_database_id is null) ;
		  END
		TRUNCATE TABLE #LogInfo2012;'
								DROP TABLE #LogInfo2012;
							END
						ELSE
							BEGIN
								EXEC sp_MSforeachdb N'USE [?];
		  INSERT INTO #LogInfo
		  EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		  IF    @@ROWCOUNT > 999
		  BEGIN
			INSERT  INTO #BlitzResults
			( CheckID
			,DatabaseName
			,Priority
			,FindingsGroup
			,Finding
			,URL
			,Details)
			SELECT      69
			,DB_NAME()
			,100
			,''Performance''
			,''High VLF Count''
			,''http://BrentOzar.com/go/vlf''
			,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			FROM #LogInfo
			WHERE EXISTS (SELECT name FROM master.sys.databases
			WHERE source_database_id is null);
		  END
		  TRUNCATE TABLE #LogInfo;'
								DROP TABLE #LogInfo;
							END
					END
		/*Verify that the servername is set */
	/*Verify that the servername is set */
			IF NOT EXISTS ( SELECT  1
							FROM    #SkipChecks
							WHERE   DatabaseName IS NULL AND CheckID = 70 )
				BEGIN
					IF @@SERVERNAME IS NULL
						BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  70 AS CheckID ,
											200 AS Priority ,
											'Configuration' AS FindingsGroup ,
											'@@Servername Not Set' AS Finding ,
											'http://BrentOzar.com/go/servername' AS URL ,
											'@@Servername variable is null. You can fix it by executing: "sp_addserver ''<LocalServerName>'', local"' AS Details
						END;

					IF  /* @@SERVERNAME IS set */
						(@@SERVERNAME IS NOT NULL
						AND
						/* not a named instance */
						CHARINDEX('\',CAST(SERVERPROPERTY('ServerName') AS NVARCHAR)) = 0
						AND
						/* not clustered, when computername may be different than the servername */
						SERVERPROPERTY('IsClustered') = 0
						AND
						/* @@SERVERNAME is different than the computer name */
						@@SERVERNAME <> CAST(ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),@@SERVERNAME) AS NVARCHAR) )
						 BEGIN
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  70 AS CheckID ,
											200 AS Priority ,
											'Configuration' AS FindingsGroup ,
											'@@Servername Not Correct' AS Finding ,
											'http://BrentOzar.com/go/servername' AS URL ,
											'The @@Servername is different than the computer name, which may trigger certificate errors.' AS Details
						END;

				END
		/*Check to see if a failsafe operator has been configured*/
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 73 )
					BEGIN

						DECLARE @AlertInfo TABLE
							(
							  FailSafeOperator NVARCHAR(255) ,
							  NotificationMethod INT ,
							  ForwardingServer NVARCHAR(255) ,
							  ForwardingSeverity INT ,
							  PagerToTemplate NVARCHAR(255) ,
							  PagerCCTemplate NVARCHAR(255) ,
							  PagerSubjectTemplate NVARCHAR(255) ,
							  PagerSendSubjectOnly NVARCHAR(255) ,
							  ForwardAlways INT
							)
						INSERT  INTO @AlertInfo
								EXEC [master].[dbo].[sp_MSgetalertinfo] @includeaddresses = 0
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  73 AS CheckID ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'No failsafe operator configured' AS Finding ,
										'http://BrentOzar.com/go/failsafe' AS URL ,
										( 'No failsafe operator is configured on this server.  This is a good idea just in-case there are issues with the [msdb] database that prevents alerting.' ) AS Details
								FROM    @AlertInfo
								WHERE   FailSafeOperator IS NULL;
					END

		/*Identify globally enabled trace flags*/
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 74 )
					BEGIN
						INSERT  INTO #TraceStatus
								EXEC ( ' DBCC TRACESTATUS(-1) WITH NO_INFOMSGS'
									)
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  74 AS CheckID ,
										200 AS Priority ,
										'Global Trace Flag' AS FindingsGroup ,
										'TraceFlag On' AS Finding ,
										'http://www.BrentOzar.com/go/traceflags/' AS URL ,
										'Trace flag ' + T.TraceFlag
										+ ' is enabled globally.' AS Details
								FROM    #TraceStatus T
					END

		/*Check for transaction log file larger than data file */
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 75 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  75 AS CheckID ,
										DB_NAME(a.database_id) ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Transaction Log Larger than Data File' AS Finding ,
										'http://BrentOzar.com/go/biglog' AS URL ,
										'The database [' + DB_NAME(a.database_id)
										+ '] has a transaction log file larger than a data file. This may indicate that transaction log backups are not being performed or not performed often enough.' AS Details
								FROM    sys.master_files a
								WHERE   a.type = 1
										AND DB_NAME(a.database_id) NOT IN (
										SELECT DISTINCT
												DatabaseName
										FROM    #SkipChecks )
										AND a.size > 125000 /* Size is measured in pages here, so this gets us log files over 1GB. */
										AND a.size > ( SELECT   SUM(CAST(b.size AS BIGINT))
													   FROM     sys.master_files b
													   WHERE    a.database_id = b.database_id
																AND b.type = 0
													 )
										AND a.database_id IN (
										SELECT  database_id
										FROM    sys.databases
										WHERE   source_database_id IS NULL )
					END

		/*Check for collation conflicts between user databases and tempdb */
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 76 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  76 AS CheckID ,
										name AS DatabaseName ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Collation for ' + name
										+ ' different than tempdb collation' AS Finding ,
										'http://BrentOzar.com/go/collate' AS URL ,
										'Collation differences between user databases and tempdb can cause conflicts especially when comparing string values' AS Details
								FROM    sys.databases
							WHERE   name NOT IN ( 'master', 'model', 'msdb')
										AND name NOT LIKE 'ReportServer%'
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks )
										AND collation_name <> ( SELECT
																  collation_name
																FROM
																  sys.databases
																WHERE
																  name = 'tempdb'
															  )
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 77 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  77 AS CheckID ,
										dSnap.[name] AS DatabaseName ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Database Snapshot Online' AS Finding ,
										'http://BrentOzar.com/go/snapshot' AS URL ,
										'Database [' + dSnap.[name]
										+ '] is a snapshot of ['
										+ dOriginal.[name]
										+ ']. Make sure you have enough drive space to maintain the snapshot as the original database grows.' AS Details
								FROM    sys.databases dSnap
										INNER JOIN sys.databases dOriginal ON dSnap.source_database_id = dOriginal.database_id
																  AND dSnap.name NOT IN (
																  SELECT DISTINCT
																  DatabaseName
																  FROM
																  #SkipChecks )
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 79 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  79 AS CheckID ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Shrink Database Job' AS Finding ,
										'http://BrentOzar.com/go/autoshrink' AS URL ,
										'In the [' + j.[name] + '] job, step ['
										+ step.[step_name]
										+ '] has SHRINKDATABASE or SHRINKFILE, which may be causing database fragmentation.' AS Details
								FROM    msdb.dbo.sysjobs j
										INNER JOIN msdb.dbo.sysjobsteps step ON j.job_id = step.job_id
								WHERE   step.command LIKE N'%SHRINKDATABASE%'
										OR step.command LIKE N'%SHRINKFILE%'
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 80 )
					BEGIN
						EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 80, DB_NAME(), 50, ''Reliability'', ''Max File Size Set'', ''http://BrentOzar.com/go/maxsize'', (''The ['' + DB_NAME() + ''] database file '' + name + '' has a max file size set to '' + CAST(CAST(max_size AS BIGINT) * 8 / 1024 AS VARCHAR(100)) + ''MB. If it runs out of space, the database will stop working even though there may be drive space available.'') FROM sys.database_files WHERE max_size <> 268435456 AND max_size <> -1 AND type <> 2';
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 81 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT  81 AS CheckID ,
										200 AS Priority ,
										'Non-Active Server Config' AS FindingsGroup ,
										cr.name AS Finding ,
										'http://www.BrentOzar.com/blitz/sp_configure/' AS URL ,
										( 'This sp_configure option isn''t running under its set value.  Its set value is '
										  + CAST(cr.[Value] AS VARCHAR(100))
										  + ' and its running value is '
										  + CAST(cr.value_in_use AS VARCHAR(100))
										  + '. When someone does a RECONFIGURE or restarts the instance, this setting will start taking effect.' ) AS Details
								FROM    sys.configurations cr
								WHERE   cr.value <> cr.value_in_use;
					END

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 123 )
					BEGIN
						INSERT  INTO #BlitzResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  URL ,
								  Details
								)
								SELECT TOP 1 123 AS CheckID ,
										200 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Agent Jobs Starting Simultaneously' AS Finding ,
										'http://BrentOzar.com/go/busyagent/' AS URL ,
										( 'Multiple SQL Server Agent jobs are configured to start simultaneously. For detailed schedule listings, see the query in the URL.' ) AS Details
								FROM    msdb.dbo.sysjobactivity
								WHERE start_execution_date > DATEADD(dd, -14, GETDATE())
								GROUP BY start_execution_date HAVING COUNT(*) > 1;
					END


				IF @CheckServerInfo = 1
					BEGIN

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 83 )
							BEGIN
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
									BEGIN
										SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT  83 AS CheckID ,
				250 AS Priority ,
				''Server Info'' AS FindingsGroup ,
				''Services'' AS Finding ,
				'''' AS URL ,
				N''Service: '' + servicename + N'' runs under service account '' + service_account + N''. Last startup time: '' + COALESCE(CAST(CAST(last_startup_time AS DATETIME) AS VARCHAR(50)), ''not shown.'') + ''. Startup type: '' + startup_type_desc + N'', currently '' + status_desc + ''.''
				FROM sys.dm_server_services;'
										EXECUTE(@StringToExecute);
									END
							END

			/* Check 84 - SQL Server 2012 */
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 84 )
							BEGIN
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_os_sys_info'
													AND c.name = 'physical_memory_kb' )
									BEGIN
										SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			'''' AS URL ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_kb / 1024.0 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info';
										EXECUTE(@StringToExecute);
									END

			/* Check 84 - SQL Server 2008 */
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_os_sys_info'
													AND c.name = 'physical_memory_in_bytes' )
									BEGIN
										SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			'''' AS URL ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_in_bytes / 1024.0 / 1024 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info';
										EXECUTE(@StringToExecute);
									END
							END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 85 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
										SELECT  85 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'SQL Server Service' AS Finding ,
												'' AS URL ,
												N'Version: '
												+ CAST(SERVERPROPERTY('productversion') AS NVARCHAR(100))
												+ N'. Patch Level: '
												+ CAST(SERVERPROPERTY('productlevel') AS NVARCHAR(100))
												+ N'. Edition: '
												+ CAST(SERVERPROPERTY('edition') AS VARCHAR(100))
												+ N'. AlwaysOn Enabled: '
												+ CAST(COALESCE(SERVERPROPERTY('IsHadrEnabled'),
																0) AS VARCHAR(100))
												+ N'. AlwaysOn Mgr Status: '
												+ CAST(COALESCE(SERVERPROPERTY('HadrManagerStatus'),
																0) AS VARCHAR(100))
							END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 88 )
							BEGIN
								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
										SELECT  88 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'SQL Server Last Restart' AS Finding ,
												'' AS URL ,
												CAST(create_date AS VARCHAR(100))
										FROM    sys.databases
										WHERE   database_id = 2
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 92 )
							BEGIN
								INSERT  INTO #driveInfo
										( drive, SIZE )
										EXEC master..xp_fixeddrives

								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
										SELECT  92 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'Drive ' + i.drive + ' Space' AS Finding ,
												'' AS URL ,
												CAST(i.SIZE AS VARCHAR)
												+ 'MB free on ' + i.drive
												+ ' drive' AS Details
										FROM    #driveInfo AS i
								DROP TABLE #driveInfo
							END


						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 103 )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
												INNER JOIN sys.all_columns c ON o.object_id = c.object_id
										 WHERE  o.name = 'dm_os_sys_info'
												AND c.name = 'virtual_machine_type_desc' )
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
									SELECT 103 AS CheckID,
									250 AS Priority,
									''Server Info'' AS FindingsGroup,
									''Virtual Server'' AS Finding,
									''http://BrentOzar.com/go/virtual'' AS URL,
									''Type: ('' + virtual_machine_type_desc + '')'' AS Details
									FROM sys.dm_os_sys_info
									WHERE virtual_machine_type <> 0';
								EXECUTE(@StringToExecute);
							END

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 114 )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
										 WHERE  o.name = 'dm_os_memory_nodes' )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
										 INNER JOIN sys.all_columns c ON o.object_id = c.object_id
										 WHERE  o.name = 'dm_os_nodes'
                                	 		AND c.name = 'processor_group' )
							BEGIN
								SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
										SELECT  114 AS CheckID ,
												250 AS Priority ,
												''Server Info'' AS FindingsGroup ,
												''Hardware - NUMA Config'' AS Finding ,
												'''' AS URL ,
												''Node: '' + CAST(n.node_id AS NVARCHAR(10)) + '' State: '' + node_state_desc
												+ '' Online schedulers: '' + CAST(n.online_scheduler_count AS NVARCHAR(10)) + '' Processor Group: '' + CAST(n.processor_group AS NVARCHAR(10))
												+ '' Memory node: '' + CAST(n.memory_node_id AS NVARCHAR(10)) + '' Memory VAS Reserved GB: '' + CAST(CAST((m.virtual_address_space_reserved_kb / 1024.0 / 1024) AS INT) AS NVARCHAR(100))
										FROM sys.dm_os_nodes n
										INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
										WHERE n.node_state_desc NOT LIKE ''%DAC%''
										ORDER BY n.node_id'
								EXECUTE(@StringToExecute);
							END


							IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 106 )
											AND (select convert(int,value_in_use) from sys.configurations where name = 'default trace enabled' ) = 1
							BEGIN

								INSERT  INTO #BlitzResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  URL ,
										  Details
										)
										SELECT
												 106 AS CheckID
												,250 AS Priority
												,'Server Info' AS FindingsGroup
												,'Default Trace Contents' AS Finding
												,'http://BrentOzar.com/go/trace' AS URL
												,'The default trace holds '+cast(DATEDIFF(hour,MIN(StartTime),GETDATE())as varchar)+' hours of data'
												+' between '+cast(Min(StartTime) as varchar)+' and '+cast(GETDATE()as varchar)
												+('. The default trace files are located in: '+left( @curr_tracefilename,len(@curr_tracefilename) - @indx)
												) as Details
										FROM    ::fn_trace_gettable( @base_tracefilename, default )
										WHERE EventClass BETWEEN 65500 and 65600
							END /* CheckID 106 */



					END /* IF @CheckServerInfo = 1 */
			END /* IF ( ( SERVERPROPERTY('ServerName') NOT IN ( SELECT ServerName */


				/* Delete priorites they wanted to skip. */
				IF @IgnorePrioritiesAbove IS NOT NULL
					DELETE  #BlitzResults
					WHERE   [Priority] > @IgnorePrioritiesAbove AND CheckID <> -1;

				IF @IgnorePrioritiesBelow IS NOT NULL
					DELETE  #BlitzResults
					WHERE   [Priority] < @IgnorePrioritiesBelow AND CheckID <> -1;

				/* Delete checks they wanted to skip. */
				IF @SkipChecksTable IS NOT NULL
					BEGIN
						DELETE  FROM #BlitzResults
						WHERE   DatabaseName IN ( SELECT    DatabaseName
												  FROM      #SkipChecks
												  WHERE CheckID IS NULL
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName')));
						DELETE  FROM #BlitzResults
						WHERE   CheckID IN ( SELECT    CheckID
												  FROM      #SkipChecks
												  WHERE DatabaseName IS NULL
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName')));
						DELETE r FROM #BlitzResults r
							INNER JOIN #SkipChecks c ON r.DatabaseName = c.DatabaseName and r.CheckID = c.CheckID
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName'));
					END

				/* Add summary mode */
				IF @SummaryMode > 0
					BEGIN
					UPDATE #BlitzResults
					  SET Finding = br.Finding + ' (' + CAST(brTotals.recs AS NVARCHAR(20)) + ')'
					  FROM #BlitzResults br
						INNER JOIN (SELECT FindingsGroup, Finding, Priority, COUNT(*) AS recs FROM #BlitzResults GROUP BY FindingsGroup, Finding, Priority) brTotals ON br.FindingsGroup = brTotals.FindingsGroup AND br.Finding = brTotals.Finding AND br.Priority = brTotals.Priority
						WHERE brTotals.recs > 1;

					DELETE br
					  FROM #BlitzResults br
					  WHERE EXISTS (SELECT * FROM #BlitzResults brLower WHERE br.FindingsGroup = brLower.FindingsGroup AND br.Finding = brLower.Finding AND br.Priority = brLower.Priority AND br.ID > brLower.ID);

					END

				/* Add credits for the nice folks who put so much time into building and maintaining this for free: */
				INSERT  INTO #BlitzResults
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
						  'http://www.BrentOzar.com/blitz/' ,
						  'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
						);

				INSERT  INTO #BlitzResults
						( CheckID ,
						  Priority ,
						  FindingsGroup ,
						  Finding ,
						  URL ,
						  Details

						)
				VALUES  ( -1 ,
						  0 ,
						  'sp_Blitz (TM) v' + CAST(@Version AS VARCHAR(20)) + ' as of ' + CAST(CONVERT(DATETIME, @VersionDate, 102) AS VARCHAR(100)),
						  'From Brent Ozar Unlimited' ,
						  'http://www.BrentOzar.com/blitz/' ,
						  'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'

						);

				IF @EmailRecipients IS NOT NULL
					BEGIN
					/* Database mail won't work off a local temp table. I'm not happy about this hacky workaround either. */
					IF (OBJECT_ID('tempdb..##BlitzResults', 'U') IS NOT NULL) DROP TABLE ##BlitzResults;
					SELECT * INTO ##BlitzResults FROM #BlitzResults;
					SET @query_result_separator = char(9);
					SET @StringToExecute = 'SET NOCOUNT ON;SELECT [Priority] , [FindingsGroup] , [Finding] , [DatabaseName] , [URL] ,  [Details] , CheckID FROM ##BlitzResults ORDER BY Priority , FindingsGroup, Finding, Details; SET NOCOUNT OFF;';
					SET @EmailSubject = 'sp_Blitz (TM) Results for ' + @@SERVERNAME;
					SET @EmailBody = 'sp_Blitz (TM) v' + CAST(@Version AS VARCHAR(20)) + ' as of ' + CAST(CONVERT(DATETIME, @VersionDate, 102) AS VARCHAR(100)) + '. From Brent Ozar Unlimited: http://www.BrentOzar.com/blitz/';
					IF @EmailProfile IS NULL
						EXEC msdb.dbo.sp_send_dbmail
							@recipients = @EmailRecipients,
							@subject = @EmailSubject,
							@body = @EmailBody,
							@query_attachment_filename = 'sp_Blitz-Results.csv',
							@attach_query_result_as_file = 1,
							@query_result_header = 1,
							@query_result_width = 32767,
							@append_query_error = 1,
							@query_result_no_padding = 1,
							@query_result_separator = @query_result_separator,
							@query = @StringToExecute;
					ELSE
						EXEC msdb.dbo.sp_send_dbmail
							@profile_name = @EmailProfile,
							@recipients = @EmailRecipients,
							@subject = @EmailSubject,
							@body = @EmailBody,
							@query_attachment_filename = 'sp_Blitz-Results.csv',
							@attach_query_result_as_file = 1,
							@query_result_header = 1,
							@query_result_width = 32767,
							@append_query_error = 1,
							@query_result_no_padding = 1,
							@query_result_separator = @query_result_separator,
							@query = @StringToExecute;
					IF (OBJECT_ID('tempdb..##BlitzResults', 'U') IS NOT NULL) DROP TABLE ##BlitzResults;
				END


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
								BlitzVersion INT,
								Priority TINYINT ,
								FindingsGroup VARCHAR(50) ,
								Finding VARCHAR(200) ,
								DatabaseName NVARCHAR(128),
								URL VARCHAR(200) ,
								Details NVARCHAR(4000) ,
								QueryPlan [XML] NULL ,
								QueryPlanFiltered [NVARCHAR](MAX) NULL,
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
							+ ' (ServerName, CheckDate, BlitzVersion, CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered) SELECT '''
							+ CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
							+ ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
							+ ', CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';
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
								BlitzVersion INT,
								Priority TINYINT ,
								FindingsGroup VARCHAR(50) ,
								Finding VARCHAR(200) ,
								DatabaseName NVARCHAR(128),
								URL VARCHAR(200) ,
								Details NVARCHAR(4000) ,
								QueryPlan [XML] NULL ,
								QueryPlanFiltered [NVARCHAR](MAX) NULL,
								CheckID INT ,
								CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'
							+ ' INSERT '
							+ @OutputTableName
							+ ' (ServerName, CheckDate, BlitzVersion, CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered) SELECT '''
							+ CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
							+ ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
							+ ', CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';
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
						FROM    #BlitzResults
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
							FROM    #BlitzResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									Details;
						END
					ELSE IF @OutputXMLasNVARCHAR = 1
						BEGIN
							SELECT  [Priority] ,
									[FindingsGroup] ,
									[Finding] ,
									[DatabaseName] ,
									[URL] ,
									[Details] ,
									CAST([QueryPlan] AS NVARCHAR(MAX)) AS QueryPlan,
									[QueryPlanFiltered] ,
									CheckID
							FROM    #BlitzResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									Details;
						END
					ELSE
						BEGIN
							SELECT  [Priority] ,
									[FindingsGroup] ,
									[Finding] ,
									[DatabaseName] ,
									[URL] ,
									[Details] ,
									[QueryPlan] ,
									[QueryPlanFiltered] ,
									CheckID
							FROM    #BlitzResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									Details;
						END

				DROP TABLE #BlitzResults;

				IF @OutputProcedureCache = 1
					AND @CheckProcedureCache = 1
					SELECT TOP 20
							total_worker_time / execution_count AS AvgCPU ,
							total_worker_time AS TotalCPU ,
							CAST(ROUND(100.00 * total_worker_time
									   / ( SELECT   SUM(total_worker_time)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentCPU ,
							total_elapsed_time / execution_count AS AvgDuration ,
							total_elapsed_time AS TotalDuration ,
							CAST(ROUND(100.00 * total_elapsed_time
									   / ( SELECT   SUM(total_elapsed_time)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentDuration ,
							total_logical_reads / execution_count AS AvgReads ,
							total_logical_reads AS TotalReads ,
							CAST(ROUND(100.00 * total_logical_reads
									   / ( SELECT   SUM(total_logical_reads)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentReads ,
							execution_count ,
							CAST(ROUND(100.00 * execution_count
									   / ( SELECT   SUM(execution_count)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentExecutions ,
							CASE WHEN DATEDIFF(mi, creation_time,
											   qs.last_execution_time) = 0 THEN 0
								 ELSE CAST(( 1.00 * execution_count / DATEDIFF(mi,
																  creation_time,
																  qs.last_execution_time) ) AS MONEY)
							END AS executions_per_minute ,
							qs.creation_time AS plan_creation_time ,
							qs.last_execution_time ,
							text ,
							text_filtered ,
							query_plan ,
							query_plan_filtered ,
							sql_handle ,
							query_hash ,
							plan_handle ,
							query_plan_hash
					FROM    #dm_exec_query_stats qs
					ORDER BY CASE UPPER(@CheckProcedureCacheFilter)
							   WHEN 'CPU' THEN total_worker_time
							   WHEN 'READS' THEN total_logical_reads
							   WHEN 'EXECCOUNT' THEN execution_count
							   WHEN 'DURATION' THEN total_elapsed_time
							   ELSE total_worker_time
							 END DESC

	END /* ELSE -- IF @OutputType = 'SCHEMA' */
    SET NOCOUNT OFF;
GO

/*
--Sample execution call with the most common parameters:
EXEC [master].[dbo].[sp_Blitz]
    @CheckUserDatabaseObjects = 1 ,
    @CheckProcedureCache = 0 ,
    @OutputType = 'TABLE' ,
    @OutputProcedureCache = 0 ,
    @CheckProcedureCacheFilter = NULL,
    @CheckServerInfo = 1
*/
