USE [master];
GO

IF OBJECT_ID('master.dbo.sp_Blitz') IS NOT NULL 
    DROP PROC dbo.sp_Blitz;
GO

CREATE PROCEDURE [dbo].[sp_Blitz]
    @CheckUserDatabaseObjects TINYINT = 1 ,
    @CheckProcedureCache TINYINT = 0 ,
    @OutputType VARCHAR(20) = 'TABLE' ,
    @OutputProcedureCache TINYINT = 0 ,
    @CheckProcedureCacheFilter VARCHAR(10) = NULL ,
    @CheckServerInfo TINYINT = 0 ,
    @SkipChecksServer NVARCHAR(256) = NULL,
    @SkipChecksDatabase NVARCHAR(256) = NULL,
    @SkipChecksSchema NVARCHAR(256) = NULL,
    @SkipChecksTable NVARCHAR(256) = NULL,
    @IgnorePrioritiesBelow INT = NULL,
    @IgnorePrioritiesAbove INT = NULL,
    @Version INT = NULL OUTPUT
AS 
    SET NOCOUNT ON;
/*
    sp_Blitz (TM) v20 - April 12, 2013
    
    (C) 2013, Brent Ozar Unlimited. 
	See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

To learn more, visit http://www.BrentOzar.com/blitz where you can download
new versions for free, watch training videos on how it works, get more info on
the findings, and more.  To contribute code and see your name in the change
log, email your improvements & checks to Help@BrentOzar.com.

Explanation of priority levels:
  1 - Critical risk of data loss.  Fix this ASAP.
 10 - Security risk.
 20 - Security risk due to unusual configuration, but requires more research.
 50 - Reliability risk.
 60 - Reliability risk due to unusual configuration, but requires more research.
100 - Performance risk.
110 - Performance risk due to unusual configuration, but requires more research.
200 - Informational.
250 - Server info. Not warnings, just explaining data about the server.

Known limitations of this version:
 - No support for SQL Server 2000 or compatibility mode 80.
 - If a database name has a question mark in it, some tests will fail.  Gotta
   love that unsupported sp_MSforeachdb.
 - If you have offline databases, sp_Blitz fails the first time you run it,
   but does work the second time. (Hoo, boy, this will be fun to fix.)

Unknown limitations of this version:
 - None.  (If we knew them, they'd be known.  Duh.)

Changes in v20:
 - Randy Knight @Randy_Knight http://sqlsolutionsgroup.com identified a bunch
   of checks with duplicate IDs, so a few check IDs changed for uniqueness.

Changes in v19:
 - Frank van der Heide and Ken Wilson fixed a bug in @IgnorePrioritiesBelow.
   Pushed this out since it's a critical fix for people using sp_Blitz with
   monitoring tools.
	
Changes in v18:
 - Alin Selicean @AlinSelicean:
   - Added check 93 looking for backups stored on the same drive letter as user
	 database files. Will give false positives if both your user databases
	 and backup files are mount points on the same drive letter, but hey,
	 don't do that.
   - Fixed typo in check 88 that wouldn't run if check 91 was being skipped.
   - Improved check 72, non-aligned partitioned indexes, to include the 
	 database name even if the index hadn't been used since restart.
 - David Forck @Thirster42:
   - Added check 94 for Agent jobs that are not set up to notify an operator
	 upon failure.
   - Improved check 80 to ignore max file sizes for filestream data files.
 - Dino Maric added check 92 for free drive space. (Not an alert, just runs if
   you set @CheckServerInfo = 1.) Doesn't include mount points.
 - Eric Alter fixed bug with missing begin/end in check 83.
 - Nigel Maneffa fixed a broken link in check 91 for merge replication.
 - Added check 86 back in for elevated database permissions.
 - Replaced @@SERVERNAME usage with SERVERPROPERTY('ServerName') because in
   some cloud hosting environments, these don't match, and it's okay.
 - Changed database name variables to be NVARCHAR(128). Dang SharePoint.
	
Changes in v17: 
 - Alin Selician:
   - Fixed bug in check 72 for partitioned indexes that weren't aligned.
   - Suggested check 88 for SQL Server's last restart.
 - Nigel Maneffa:
   - Added check 90 for corruption alerts in msdb.dbo.suspect_pages.
   - Added check 91 for merge replication with infinite retention.
 - Nancy Hidy Wilson @NancyHidyWilson fixed a bug in the sys.configurations
   check 22: SQL 2012 changed the remote login timeout setting from 20 to 10.
 - Roland Rosso suggested to add parameters for @IgnorePrioritiesBelow and
   @IgnorePrioritiesAbove so you can just return results for some types of checks.
 - Russell Hart fixed a bug in the DBCC CHECKDB check that failed on systems
   using British date formats.
 - Stephanie Richter fixed description typo in check 57 for Agent startup jobs.
 - Steve Wales caught dupe checkID's 60, changed one to 87.
 - Improved check 78 for implicit conversion so that it checks sys.sql_modules
   for the is_recompiled bit flag rather than scanning the source code. 
 - Added check 89 looking for corruption reports in dm_hadr_auto_page_repair.
 - Improved checks 58 and 76 (collation mismatches) to exclude ReportServer and
   ReportServerTempDB. Microsoft builds those databases their own special way.
 - Improved check 22, the default sys.configurations check, to add new SQL 2012
   settings, deal with odd default min server memory settings (we're seeing
   both 0mb and 16mb in the wild as defaults).
 - Added @OutputType = 'CSV' option that strips commas and returns one field per
   row rather than separate fields of data. Doesn't return the query and query
   plan fields since those are monsters.
 - Created support for excluding databases and/or checks.  This will handled via a table that holds
   the servers, databases and checks that you would like skipped. Use the script below (naming the table)to 
   create the table
     CREATE TABLE dbo.Whatever
      (ID INT IDENTITY(1,1),
      ServerName NVARCHAR(128),
      DatabaseName NVARCHAR(128), 
      CheckID INT)
      
  ServerName = 'MyServer, DatabaseName = NULL, CheckId = NULL - will not check anything on servername MyServer
  ServerName = NULL, DatabaseName = 'MyDB', CheckId = NULL - will not check MyDB on any server
  ServerName = 'MyServer', DatabaseName = 'MyDB', CheckId = NULL - will not check MyDB on MyServer
  ServerName = NULL, DatabaseName = NULL, CheckId = 5 - will skip CheckId 5 on all databases
  ServerName = NULL, DatabaseName = 'MyDB', CheckId = 5 - will skip CheckId 5 on MyDB
  
  The new variables @SkipChecksServer, @SkipChecksDatabase, @SkipChecksSchema and @SkipChecksTable provide
  the path to where the new table lives. 


Changes in v16:
 - Vladimir Vissoultchev rewrote the DBCC CHECKDB check to work around a bug in
   SQL Server 2008 & R2 that report dbi_dbccLastKnownGood twice. For more info
   on the bug, check Connect ID 485869.
 - Chris Fradenburg @ChrisFradenburg http://www.fradensql.com:
   - Check 81 for non-active sp_configure options not yet taking effect.
   - Improved check 35 to not alert if Optimize for Ad Hoc is already enabled.
 - Rob Sullivan @DataChomp http://datachomp.com:
   - Suggested to add output variable @Version to manage server installations.
 - Vadim Mordkovich:
   - Added check 86 for database users with elevated database roles like
     db_owner, db_securityadmin, etc.
 - Vladimir Vissoultchev rewrote the DBCC CHECKDB check to work around a bug in
   SQL Server 2008 & R2 that report dbi_dbccLastKnownGood twice. For more info
   on the bug, check Connect ID 485869.
 - Added check 77 for database snapshots.
 - Added check 78 for stored procedures with WITH RECOMPILE in the source code.
 - Added check 79 for Agent jobs with SHRINKDATABASE or SHRINKFILE.
 - Added check 80 for databases with a max file size set.
 - Added @CheckServerInfo perameter default 0. Adds additional server inventory
   data in checks 83-85 for things like CPU, memory, service logins.  None of
   these are problems, but if you're using sp_Blitz to assess a server you've
   never seen, you may want to know more about what you're working with. I do.
 - Added checks 83-85 for server descriptions (CPU, memory, services.)
 - Tweaked check 75 for large log files so that it only alerts on files > 1GB.
 - Changed one of the two check 59's to be check 82. (Doh!)
 - Added WITH NO_INFOMSGS to the DBCC calls to ease life for automation folks.
 - Works with offline and restoring databases. (Just happened to test it in
   this version and it already worked - must have fixed this earlier.)

Changes in v15:
 - Mikael Wedham caught bugs in a few checks that reported the wrong database name.
 - Bob Klimes fixed bugs in several checks where v14 broke case sensitivity.
 - Seth Washeck fixed bugs in the VLF checks so they include the number of VLFs.

Changes in v14:
 - Lori Edwards @LoriEdwards http://sqlservertimes2.com
     - Did all the coding in this version! She did a killer job of integrating
       improvements and suggestions from all kinds of people, including:
 - Chris Fradenburg @ChrisFradenburg http://www.fradensql.com 
     - Check 74 to identify globally enabled traceflags
 - Jeremy Lowell @DataRealized http://datarealized.com added:
     - Check 72 for non-aligned indexes on partitioned tables
 - Paul Anderton @Panders69 added check 69 to check for high VLF count
 - Ron van Moorsel added several changes
   - Added a change to check 6 to use sys.server_principals instead of syslogins
   - Added a change to check 25 to check whether tempdb was set to autogrow.  
   - Added a change to check 49 to check for linked servers configured with the SA login
 - Shaun Stuart @shaunjstu http://shaunjstuart.com added several changes:
   - Added check 68 to check for the last successful DBCC CHECKDB
   - Updated check 1 to verify the backup came from the current 
   - Added check 70 to verify that @@servername is not null
 - Typo in check 51 changing free to present thanks to Sabu Varghese
 - Check 73 to determine if a failsafe operator has been configured
 - Check 75 for transaction log files larger than data files suggested by Chris Adkin
 - Fixed a bunch of bugs for oddball database names (like apostrophes).

Changes in v13:
 - Fixed typos in descriptions of checks 60 & 61 thanks to Mark Hions.
 - Improved check 14 to work with collations thanks to Greg Ackerland.
 - Improved several of the backup checks to exclude database snapshots and
   databases that are currently being restored thanks to Greg Ackerland.
 - Improved wording on check 51 thanks to Stephen Criddle.
 - Added top line introducing the reader to sp_Blitz and the version number.
 - Changed Brent Ozar PLF, LLC to Brent Ozar Unlimited. Great catch by
   Hondo Henriques, @SQLHondo.
 - If you've submitted code recently to sp_Blitz, hang in there! We're still
   building a big new version with lots of new checks. Just fixing bugs in
   this small release.

Changes in v12:
 - Added plan cache (aka procedure cache) analysis. Examines top resource-using
   queries for common problems like implicit conversions, missing indexes, etc.
 - Added @CheckProcedureCacheFilter to focus plan cache analysis on
   CPU, Reads, Duration, or ExecCount. If null, we analyze all of them.
 - Added @OutputProcedureCache to include the queries we analyzed. Results are
   sorted using the @CheckProcedureCacheFilter parameter, otherwise by CPU.
 - Fixed case sensitive calls of sp_MSforeachdb reported by several users.

Changes in v11:
 - Added check for optimize for ad hoc workloads in sys.configurations.
 - Added @OutputType parameter. Choices:
  - 'TABLE' - default of one result set table with all warnings.
  - 'COUNT' - Sesame Street's favorite character will tell you how many
        problems sp_Blitz found.  Useful if you want to use a
        monitoring tool to alert you when something changed.

Changes in v10:
 - Jeremiah Peschka added check 59 for file growths set to a percentage.
 - Ned Otter added check 62 for old compatibility levels.
 - Wayne Sheffield improved checks 38 & 39 by excluding more system tables.
 - Christopher Fradenburg improved check 30 (missing alerts) by making sure
   that alerts are set up for all of the severity levels involved, not just
   some of them.
 - James Siebengartner and others improved check 14 (page verification) by
   excluding TempDB, which can't be set to checksum in older versions.
 - Added check 60 for index fill factors <> 0, 100.
 - Added check 61 for unusual SQL Server editions (not Standard, Enterprise, or
   Developer)
 - Added limitations note to point out that compatibility mode 80 won't work.
 - Fixed a bug where changes in sp_configure weren't always reported.

Changes in v9:
 - Alex Pixley fixed a spelling typo.
 - Steinar Anderson http://www.sqlservice.se fixed a date bug in checkid 2.
   That bug was reported by several users, but Steinar coded the fix.
 - Stephen Schissler added a filter for checkid 2 (missing log backups) to look
   only for databases where source_database_id is null because these are
   database snapshots, and you can't run transaction log backups on snapshots.
 - Mark Fleming @markflemingnl added checkid 62 looking for disabled alerts.
 - Checkid 17 typo changed from "disabled" to "enabled" - the check
   functionality was right, but it was warning that auto update stats async
   was "disabled".  Disabled is actually the default, but the check was
   firing because it had been enabled.  (This one was reported by many.)

Changes in v8 May 10 2012:
 - Switched more-details URLs to be short.  This way they'll render better
   when viewed in our SQL Server Management Studio reports.
 - Removed ?VersionNumber querystring parameter to shorten links in SSMS.
 - Eliminated duplicate check for startup stored procedures.

Changes in v7 April 30 2012:
 - Thomas Rushton http://thelonedba.wordpress.com/ @ThomasRushton added check
   58 for database collations that don't match the server collation.
 - Rob Pellicaan caught a bug in check 13: it was only checking for plan guides
   in the master database rather than all user databases.
 - Michal Tinthofer http://www.woodler.eu improved check 2 to work across
   collations and fix a bug in the backup_finish_date check.  (Several people
   reported this, but Michal contributed the most improvements to this check.)
 - Chris Fradenburg improved checks 38 and 39 by excluding heaps if they are
   marked is_ms_shipped, thereby excluding more system stuff.
 - Jack Whittaker fixed a bug in checkid 1.  When checking for databases
   without a full backup, we were ignoring the model database, but some shops
   really do need to back up model because they put stuff in there to be
   copied into each new database, so let's alert on that too.  Larry Silverman
   also noticed this bug.
 - Michael Burgess caught a bug in the untrusted key/constraint checks that
   were not checking for is_disabled = 0.
 - Alex Friedman fixed a bug in check 44 which required a running trace.
 - New check for SQL Agent alerts configured without operator notifications.
 - Even if @CheckUserDatabaseObjects was set to 0, some user database object
   checks were being done.
 - Check 48 for untrusted foreign keys now just returns one line per database
   that has the issue rather than listing every foreign key individually. For
   the full list of untrusted keys, run the query in the finding's URL.

Changes in v6 Dec 26 2011:
 - Jonathan Allen @FatherJack suggested tweaking sp_BlitzUpdate's error message
    about Ad Hoc Queries not being enabled so that it also includes
    instructions on how to disable them again after temporarily enabling
    it to update sp_Blitz. 

Changes in v5 Dec 18 2011:
 - John Miner suggested tweaking checkid 48 and 56, the untrusted constraints
    and keys, to look for is_not_for_replication = 0 too.  This filters out
    constraints/keys that are only used for replication and don't need to
    be trusted.
 - Ned Otter caught a bug in the URL for check 7, startup stored procs.
 - Scott (Anon) recommended using SUSER_SNAME(0x01) instead of 'sa' when
    checking for job ownership, database ownership, etc.
 - Martin Schmidt http://www.geniiius.com/blog/ caught a bug in checkid 1 and
    contributed code to catch databases that had never been backed up.
 - Added parameter for @CheckProcedureCache.  When set to 0, we skip the checks
    that are typically the slowest on servers with lots of memory.  I'm
    defaulting this to 0 so more users can get results back faster.

Changes in v4 Nov 1 2011:
 - Andreas Schubert caught a typo in the explanations for checks 15-17.
 - K. Brian Kelley @kbriankelley added checkid 57 for SQL Agent jobs set to
      start automatically on startup.
 - Added parameter for @CheckUserDatabaseObjects.  When set to 0, we skip the
    checks that are typically the slowest on large servers, the user
    database schema checks for things like triggers, hypothetical
    indexes, untrusted constraints, etc.

Changes in v3 Oct 16 2011:
 - David Tolbert caught a bug in checkid 2.  If some backups had failed or
        been aborted, we raised a false alarm about no transaction log backups.
 - Fixed more bugs in checking for SQL Server 2005. (I need more 2005 VMs!)

Changes in v2 Oct 14 2011:
 - Ali Razeghi http://www.alirazeghi.com added checkid 55 looking for
   databases owned by <> SA.
 - Fixed bugs in checking for SQL Server 2005 (leading % signs)


Explanation of priority levels:
  1 - Critical risk of data loss.  Fix this ASAP.
 10 - Security risk.
 20 - Security risk due to unusual configuration, but requires more research.
 50 - Reliability risk.
 60 - Reliability risk due to unusual configuration, but requires more research.
100 - Performance risk.
110 - Performance risk due to unusual configuration, but requires more research.
200 - Informational.

*/


  IF OBJECT_ID('tempdb..#GetChecks') IS NOT NULL 
    DROP TABLE #GetChecks;
  CREATE TABLE #GetChecks(
  ID INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
  ServerName NVARCHAR(256) NULL,
  DatabaseName NVARCHAR(256) NULL,
  CheckId INT NULL
  )

  IF @SkipChecksTable IS NOT NULL AND @SkipChecksSchema IS NOT NULL AND @SkipChecksDatabase IS NOT NULL
  BEGIN
    DECLARE @exemptionpath NVARCHAR(2000)
    SET @exemptionpath = @SkipChecksDatabase +'.'+@SkipChecksSchema+'.'+@SkipChecksTable
    DECLARE @sqlcmd NVARCHAR(2500)
    SET @sqlcmd =  'insert into #GetChecks(ServerName, DatabaseName, CheckId )
            SELECT DISTINCT ServerName, DatabaseName, CheckId
            FROM '+@exemptionpath + ' WHERE ServerName IS NULL OR ServerName = SERVERPROPERTY(''ServerName'') '
    EXEC(@sqlcmd)
  END
  
  IF OBJECT_ID('tempdb..#tempchecks') IS NOT NULL 
    DROP TABLE #tempchecks;
  create table #tempchecks(DatabaseName NVARCHAR(128),
              CheckId int)

  insert into #tempchecks(DatabaseName)
  select ISNULL(DatabaseName,'') 
  from #GetChecks
  where CheckId is null
  and (ServerName is null 
  or ServerName = SERVERPROPERTY('ServerName'))

  insert into #tempchecks(DatabaseName,CheckId)
  select  '',CheckId
  from #GetChecks
  where DatabaseName is null
  and (ServerName is null 
  or ServerName = SERVERPROPERTY('ServerName'))


  IF OBJECT_ID('tempdb..#BlitzResults') IS NOT NULL 
    DROP TABLE #BlitzResults;
  CREATE TABLE #BlitzResults(
    ID INT IDENTITY(1, 1) ,
    CheckID INT ,
    DatabaseName NVARCHAR(128),
    Priority TINYINT ,
    FindingsGroup VARCHAR(50) ,
    Finding VARCHAR(200) ,
    URL VARCHAR(200) ,
    Details NVARCHAR(4000) ,
    QueryPlan [XML] NULL ,
    QueryPlanFiltered [NVARCHAR](MAX) NULL
    );

  IF OBJECT_ID('tempdb..#ConfigurationDefaults') IS NOT NULL 
    DROP TABLE #ConfigurationDefaults;
  CREATE TABLE #ConfigurationDefaults (
        name NVARCHAR(128) ,
        DefaultValue BIGINT
        );

  IF @CheckProcedureCache = 1 
  BEGIN
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
    [text_filtered] [nvarchar](MAX)
    COLLATE SQL_Latin1_General_CP1_CI_AS
    NULL
    )

  END

  DECLARE @StringToExecute NVARCHAR(4000);
  /* If we're outputting CSV, don't bother checking the plan cache because we cannot export plans. */
  IF @OutputType = 'CSV' SET @CheckProcedureCache = 0;

  if ((SERVERPROPERTY('ServerName') not in (select ServerName from #GetChecks where DatabaseName is null and CheckId is null)) or (@SkipChecksTable is null))
  begin
    if not exists (select 1 from #tempchecks where CheckId = 1)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  1 AS CheckID ,
    d.[name] as DatabaseName,
    1 AS Priority ,
    'Backup' AS FindingsGroup ,
    'Backups Not Performed Recently' AS Finding ,
    'http://BrentOzar.com/go/nobak' AS URL ,
    'Database ' + d.Name + ' last backed up: '
    + CAST(COALESCE(MAX(b.backup_finish_date), ' never ') AS VARCHAR(200)) AS Details
    FROM    master.sys.databases d
    LEFT OUTER JOIN msdb.dbo.backupset b ON d.name = b.database_name
      AND b.type = 'D'
      AND b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server */
    WHERE   d.database_id <> 2  /* Bonus points if you know what that means */
      AND d.state <> 1 /* Not currently restoring, like log shipping databases */
      AND d.is_in_standby = 0 /* Not a log shipping target database */
      AND d.source_database_id IS NULL /* Excludes database snapshots */
      and d.name not in (select distinct DatabaseName from #tempchecks)
      GROUP BY d.name
    HAVING  MAX(b.backup_finish_date) <= DATEADD(dd, -7, GETDATE());

    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  1 AS CheckID ,
    d.name as DatabaseName,
    1 AS Priority ,
    'Backup' AS FindingsGroup ,
    'Backups Not Performed Recently' AS Finding ,
    'http://BrentOzar.com/go/nobak' AS URL ,
    ( 'Database ' + d.Name + ' never backed up.' ) AS Details
    FROM    master.sys.databases d
    WHERE   d.database_id <> 2 /* Bonus points if you know what that means */
      AND d.state <> 1 /* Not currently restoring, like log shipping databases */
      AND d.is_in_standby = 0 /* Not a log shipping target database */
      AND d.source_database_id IS NULL /* Excludes database snapshots */
      and d.name not in (select distinct DatabaseName from #tempchecks)
      AND NOT EXISTS ( SELECT *
                FROM   msdb.dbo.backupset b
                WHERE  d.name = b.database_name
                AND b.type = 'D'
                AND b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server */)

    end
    
    if not exists (select 1 from #tempchecks where CheckId = 2)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT DISTINCT
    2 AS CheckID ,
    d.name as DatabaseName,
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
      and d.name not in (select distinct DatabaseName from #tempchecks)
      AND NOT EXISTS ( SELECT *
              FROM   msdb.dbo.backupset b
              WHERE  d.name = b.database_name
              AND b.type = 'L'
              AND b.backup_finish_date >= DATEADD(dd, -7, GETDATE()) );
    end


if not exists (select 1 from #tempchecks where CheckId = 93)
begin
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
'Drive ' + UPPER(LEFT(bmf.physical_device_name,3)) + ' houses both database files AND backups taken in the last two weeks. This represents a serious risk if that array fails.' Details 
FROM msdb.dbo.backupmediafamily AS bmf 
INNER JOIN msdb.dbo.backupset AS bs ON bmf.media_set_id = bs.media_set_id 
AND bs.backup_start_date >= (DATEADD(dd, -14, GETDATE()))
WHERE UPPER(LEFT(bmf.physical_device_name,3)) IN 
(SELECT DISTINCT UPPER(LEFT(mf.physical_name,3)) FROM sys.master_files AS mf)
end


    if not exists (select 1 from #tempchecks where CheckId = 3)
    begin
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
    WHERE   bs.backup_start_date <= DATEADD(dd, -60, GETDATE())
    ORDER BY backup_set_id ASC;
    end
    
    if not exists (select 1 from #tempchecks where CheckId = 4)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 5)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 6)
    begin
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
    ( 'Job [' + j.name + '] is owned by [' + sl.name
    + '] - meaning if their login is disabled or not available due to Active Directory problems, the job will stop working.' ) AS Details
    FROM    msdb.dbo.sysjobs j
    LEFT OUTER JOIN sys.server_principals sl ON j.owner_sid = sl.sid
    WHERE   j.enabled = 1
      AND sl.name <> SUSER_SNAME(0x01);
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 7)
    begin
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
    ( 'Stored procedure [master].[' + r.SPECIFIC_SCHEMA
    + '].[' + r.SPECIFIC_NAME
    + '] runs automatically when SQL Server starts up.  Make sure you know exactly what this stored procedure is doing, because it could pose a security risk.' ) AS Details
    FROM    master.INFORMATION_SCHEMA.ROUTINES r
    WHERE   OBJECTPROPERTY(OBJECT_ID(ROUTINE_NAME), 'ExecIsStartup') = 1;
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 8)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 9)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 10)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 11)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 12)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  12 AS CheckID ,
    [name] as DatabaseName,
    10 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Auto-Close Enabled' AS Finding ,
    'http://BrentOzar.com/go/autoclose' AS URL ,
    ( 'Database [' + [name]
    + '] has auto-close enabled.  This setting can dramatically decrease performance.' ) AS Details
    FROM    sys.databases
    WHERE   is_auto_close_on = 1
    and name not in (select distinct DatabaseName from #tempchecks)
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 13)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  13 AS CheckID ,
    [name] as DatabaseName,
    10 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Auto-Shrink Enabled' AS Finding ,
    'http://BrentOzar.com/go/autoshrink' AS URL ,
    ( 'Database [' + [name]
    + '] has auto-shrink enabled.  This setting can dramatically decrease performance.' ) AS Details
    FROM    sys.databases
    WHERE   is_auto_shrink_on = 1
    and name not in (select distinct DatabaseName from #tempchecks);
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 14)
    begin
    IF @@VERSION LIKE '%Microsoft SQL Server 2000%' 
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
                  WHERE page_verify_option < 1 
                  AND name <> ''tempdb''
                  and name not in (select distinct DatabaseName from #tempchecks)'
      EXECUTE(@StringToExecute)
    END;
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 14)
    begin
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
                  (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') AS Details 
                  FROM sys.databases 
                  WHERE page_verify_option < 2 
                  AND name <> ''tempdb''
                  and name not in (select distinct DatabaseName from #tempchecks)'
      EXECUTE(@StringToExecute)
    END;
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 15)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  15 AS CheckID ,
    [name] as DatabaseName,
    110 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Auto-Create Stats Disabled' AS Finding ,
    'http://BrentOzar.com/go/acs' AS URL ,
    ( 'Database [' + [name]
    + '] has auto-create-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.' ) AS Details
    FROM    sys.databases
    WHERE   is_auto_create_stats_on = 0
    and name not in (select distinct DatabaseName from #tempchecks)
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 16)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  16 AS CheckID ,
    [name] as DatabaseName,
    110 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Auto-Update Stats Disabled' AS Finding ,
    'http://BrentOzar.com/go/aus' AS URL ,
    ( 'Database [' + [name]
    + '] has auto-update-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.' ) AS Details
    FROM    sys.databases
    WHERE   is_auto_update_stats_on = 0
    and name not in (select distinct DatabaseName from #tempchecks)
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 17)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  17 AS CheckID ,
    [name] as DatabaseName,
    110 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Stats Updated Asynchronously' AS Finding ,
    'http://BrentOzar.com/go/asyncstats' AS URL ,
    ( 'Database [' + [name]
    + '] has auto-update-stats-async enabled.  When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.' ) AS Details
    FROM    sys.databases
    WHERE   is_auto_update_stats_async_on = 1
    and name not in (select distinct DatabaseName from #tempchecks)
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 18)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  18 AS CheckID ,
    [name] as DatabaseName,
    110 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Forced Parameterization On' AS Finding ,
    'http://BrentOzar.com/go/forced' AS URL ,
    ( 'Database [' + [name]
    + '] has forced parameterization enabled.  SQL Server will aggressively reuse query execution plans even if the applications do not parameterize their queries.  This can be a performance booster with some programming languages, or it may use universally bad execution plans when better alternatives are available for certain parameters.' ) AS Details
    FROM    sys.databases
    WHERE   is_parameterization_forced = 1
    and name not in (select DatabaseName from #tempchecks)
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 19)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  19 AS CheckID ,
    [name] as DatabaseName,
    200 AS Priority ,
    'Informational' AS FindingsGroup ,
    'Replication In Use' AS Finding ,
    'http://BrentOzar.com/go/repl' AS URL ,
    ( 'Database [' + [name]
    + '] is a replication publisher, subscriber, or distributor.' ) AS Details
    FROM    sys.databases
    WHERE  name not in (select distinct DatabaseName from #tempchecks)
      and is_published = 1
      OR is_subscribed = 1
      OR is_merge_published = 1
      OR is_distributor = 1
      ;
    end

    if not exists (select 1 from #tempchecks where CheckId = 99)
    begin
    EXEC dbo.sp_MSforeachdb 'USE [?];  IF EXISTS (SELECT * FROM  sys.tables WITH (NOLOCK) WHERE name = ''sysmergepublications'' ) IF EXISTS ( SELECT * FROM sysmergepublications WITH (NOLOCK) WHERE retention = 0)   INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 99, DB_NAME(), 110, ''Performance'', ''Infinite merge replication metadata retention period'', ''http://BrentOzar.com/go/merge'', (''The ['' + DB_NAME() + ''] database has merge replication metadata retention period set to infinite - this can be the case of significant performance issues.'')';
    end

            
    if not exists (select 1 from #tempchecks where CheckId = 20)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  20 AS CheckID ,
    [name] as DatabaseName,
    110 AS Priority ,
    'Informational' AS FindingsGroup ,
    'Date Correlation On' AS Finding ,
    'http://BrentOzar.com/go/corr' AS URL ,
    ( 'Database [' + [name]
    + '] has date correlation enabled.  This is not a default setting, and it has some performance overhead.  It tells SQL Server that date fields in two tables are related, and SQL Server maintains statistics showing that relation.' ) AS Details
    FROM    sys.databases
    WHERE   is_date_correlation_on = 1
    and name not in (select distinct DatabaseName from #tempchecks)
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 21)
    begin
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
                  and name not in (select distinct DatabaseName from #tempchecks)'
      EXECUTE(@StringToExecute)
    END;
    end
    
    /* Compare sp_configure defaults */
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'access check cache bucket count', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'access check cache quota', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Ad Hoc Distributed Queries', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'affinity I/O mask', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'affinity mask', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Agent XPs', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'allow updates', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'awe enabled', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'blocked process threshold', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'c2 audit mode', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'clr enabled', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'cost threshold for parallelism', 5 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'cross db ownership chaining', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'cursor threshold', -1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Database Mail XPs', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'default full-text language', 1033 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'default language', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'default trace enabled', 1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'disallow results from triggers', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'fill factor (%)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'ft crawl bandwidth (max)', 100 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'ft crawl bandwidth (min)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'ft notify bandwidth (max)', 100 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'ft notify bandwidth (min)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'index create memory (KB)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'in-doubt xact resolution', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'lightweight pooling', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'locks', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'max degree of parallelism', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'max full-text crawl range', 4 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'max server memory (MB)', 2147483647 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'max text repl size (B)', 65536 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'max worker threads', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'media retention', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'min memory per query (KB)', 1024 );
    /* Accepting both 0 and 16 below because both have been seen in the wild as defaults. */
    IF EXISTS(SELECT * FROM sys.configurations WHERE name = 'min server memory (MB)' AND value_in_use IN (0, 16))
        INSERT  INTO #ConfigurationDefaults
        SELECT 'min server memory (MB)', CAST(value_in_use AS BIGINT)
        FROM sys.configurations
        WHERE name = 'min server memory (MB)'
    ELSE
        INSERT  INTO #ConfigurationDefaults
        VALUES  ( 'min server memory (MB)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'nested triggers', 1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'network packet size (B)', 4096 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Ole Automation Procedures', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'open objects', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'optimize for ad hoc workloads', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'PH timeout (s)', 60 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'precompute rank', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'priority boost', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'query governor cost limit', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'query wait (s)', -1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'recovery interval (min)', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'remote access', 1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'remote admin connections', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'remote proc trans', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'remote query timeout (s)', 600 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Replication XPs', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'RPC parameter data validation', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'scan for startup procs', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'server trigger recursion', 1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'set working set size', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'show advanced options', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'SMO and DMO XPs', 1 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'SQL Mail XPs', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'transform noise words', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'two digit year cutoff', 2049 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'user connections', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'user options', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'Web Assistant Procedures', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'xp_cmdshell', 0 );
    
	/* New values for 2012 */    
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'affinity64 mask', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'affinity64 I/O mask', 0 );
    INSERT  INTO #ConfigurationDefaults
    VALUES  ( 'contained database authentication', 0 );

    /* SQL Server 2012 also changes a configuration default */
    IF @@VERSION LIKE '%Microsoft SQL Server 2005%'
    OR @@VERSION LIKE '%Microsoft SQL Server 2008%' 
    BEGIN 
        INSERT  INTO #ConfigurationDefaults
        VALUES  ( 'remote login timeout (s)', 20 );
    END
    ELSE
    BEGIN
        INSERT  INTO #ConfigurationDefaults
        VALUES  ( 'remote login timeout (s)', 10 );
    END

    
    if not exists (select 1 from #tempchecks where CheckId = 22)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  22 AS CheckID ,
    200 AS Priority ,
    'Non-Default Server Config' AS FindingsGroup ,
    cr.name AS Finding ,
    'http://BrentOzar.com/go/conf' AS URL ,
    ( 'This sp_configure option has been changed.  Its default value is '
      + COALESCE(CAST(cd.[DefaultValue] AS VARCHAR(100)), '(unknown)')
      + ' and it has been set to '
      + CAST(cr.value_in_use AS VARCHAR(100)) + '.' ) AS Details
    FROM sys.configurations cr
    INNER JOIN #ConfigurationDefaults cd ON cd.name = cr.name
    LEFT OUTER JOIN #ConfigurationDefaults cdUsed ON cdUsed.name = cr.name
    AND cdUsed.DefaultValue = cr.value_in_use
    WHERE cdUsed.name IS NULL;
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 24)
    begin
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
    'http://BrentOzar.com/go/drivec' AS URL ,
    ( 'The ' + DB_NAME(database_id)
    + ' database has a file on the C drive.  Putting system databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
    FROM    sys.master_files
    WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
    AND DB_NAME(database_id) IN ( 'master', 'model', 'msdb' );
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 25)
    begin
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
	'TempDB',
    100 AS Priority ,
    'Performance' AS FindingsGroup ,
    'TempDB on C Drive' AS Finding ,
    'http://BrentOzar.com/go/drivec' AS URL ,
    CASE WHEN growth > 0
      THEN ( 'The tempdb database has files on the C drive.  TempDB frequently grows unpredictably, putting your server at risk of running out of C drive space and crashing hard.  C is also often much slower than other drives, so performance may be suffering.' )
      ELSE ( 'The tempdb database has files on the C drive.  TempDB is not set to Autogrow, hopefully it is big enough.  C is also often much slower than other drives, so performance may be suffering.' )
      END AS Details
    FROM    sys.master_files
    WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
    AND DB_NAME(database_id) = 'tempdb';
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 26)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT DISTINCT
    26 AS CheckID ,
    DB_NAME(database_id) as DatabaseName,
    20 AS Priority ,
    'Reliability' AS FindingsGroup ,
    'User Databases on C Drive' AS Finding ,
    'http://BrentOzar.com/go/cdrive' AS URL ,
    ( 'The ' + DB_NAME(database_id)
    + ' database has a file on the C drive.  Putting databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
    FROM    sys.master_files
    WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
    AND DB_NAME(database_id) NOT IN ( 'master', 'model','msdb', 'tempdb' )
    and DB_NAME(database_id) not in (select distinct DatabaseName from #tempchecks)
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 27)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 28)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 29)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 30)
    begin
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
    end

    if not exists (select 1 from #tempchecks where CheckId = 59)
    begin   
    IF EXISTS ( SELECT  *
          FROM    msdb.dbo.sysalerts
          WHERE   enabled = 1
          AND COALESCE(has_notification, 0) = 0
          AND job_id IS NULL ) 
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
    end
    
    if not exists (select 1 from #tempchecks where CheckId = 96)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 61)
    begin
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
    end

    --check for disabled alerts
    if not exists (select 1 from #tempchecks where CheckId = 98)
    begin
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
    end

    
    if not exists (select 1 from #tempchecks where CheckId = 31)
    begin
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
    end
    
        
    if not exists (select 1 from #tempchecks where CheckId = 33)
    begin
    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
    AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
    BEGIN
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; INSERT INTO #BlitzResults 
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
    end
    
    if not exists (select 1 from #tempchecks where CheckId = 34)
    begin
    IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_db_mirroring_auto_page_repair')
    BEGIN
      SET @StringToExecute = 
      'INSERT INTO #BlitzResults 
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
      FROM    sys.dm_db_mirroring_auto_page_repair rp
      INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
      WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE()) ;'
      EXECUTE(@StringToExecute)
    END;
    end

    if not exists (select 1 from #tempchecks where CheckId = 89)
    begin
    IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_hadr_auto_page_repair')
    BEGIN
      SET @StringToExecute = 
      'INSERT INTO #BlitzResults 
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
    end

            
    if not exists (select 1 from #tempchecks where CheckId = 90)
    begin
    IF EXISTS (SELECT * FROM msdb.sys.all_objects WHERE name = 'suspect_pages')
    BEGIN
      SET @StringToExecute = 
      'INSERT INTO #BlitzResults 
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
    end

            
    if not exists (select 1 from #tempchecks where CheckId = 36)
    begin
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
    FROM    sys.dm_io_virtual_file_stats(NULL, NULL) AS fs
    INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id AND fs.[file_id] = mf.[file_id]
    WHERE   ( io_stall_read_ms / ( 1.0 + num_of_reads ) ) > 100;
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 37)
    begin
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
    FROM    sys.dm_io_virtual_file_stats(NULL, NULL) AS fs
    INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id AND fs.[file_id] = mf.[file_id]
    WHERE   ( io_stall_write_ms / ( 1.0 + num_of_writes ) ) > 20;
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 40)
    begin
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
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 41)
    begin   
    EXEC dbo.sp_MSforeachdb 
      'use [?]; 
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
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 42)
    begin
    EXEC dbo.sp_MSforeachdb 
        'use [?]; 
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
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 44)
    begin
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 45)
    begin
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 49)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT DISTINCT 49 AS CheckID ,
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 50)
    begin
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
    end
        
    if not exists (select 1 from #tempchecks where CheckId = 51)
    begin
    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
    AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
    BEGIN
      SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
      SELECT  51 AS CheckID ,
      1 AS Priority ,
      ''Performance'' AS FindingsGroup ,
      ''Memory Dangerously Low'' AS Finding ,
      ''http://BrentOzar.com/go/max'' AS URL ,
      ''Although available memory is ''
        + CAST(( CAST(m.available_physical_memory_kb AS BIGINT)
        / 1024 ) AS VARCHAR(20))
        + '' megabytes, only ''
        + CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
        + ''megabytes of memory are present.  As the server runs out of memory, there is danger of swapping to disk, which will kill performance.'' AS Details
      FROM    sys.dm_os_sys_memory m
      WHERE   CAST(m.available_physical_memory_kb AS BIGINT) < 262144'
      EXECUTE(@StringToExecute)
    END;
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 53)
    begin
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 55)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  55 AS CheckID ,
    [name] as DatabaseName,
    200 AS Priority ,
    'Security' AS FindingsGroup ,
    'Database Owner <> SA' AS Finding ,
    'http://BrentOzar.com/go/owndb' AS URL ,
    ( 'Database name: ' + [name] + '   ' + 'Owner name: '
    + SUSER_SNAME(owner_sid) ) AS Details
    FROM    sys.databases
    WHERE   SUSER_SNAME(owner_sid) <> SUSER_SNAME(0x01)
    and name not in (select distinct DatabaseName from #tempchecks);
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 57)
    begin
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 58)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  58 AS CheckID ,
    d.[name] as DatabaseName,
    200 AS Priority ,
    'Reliability' AS FindingsGroup ,
    'Database Collation Mismatch' AS Finding ,
    'http://BrentOzar.com/go/collate' AS URL ,
    ( 'Database ' + d.NAME + ' has collation '
    + d.collation_name + '; Server collation is '
    + CONVERT(VARCHAR(100), SERVERPROPERTY('collation')) ) AS Details
    FROM    master.sys.databases d
    WHERE   d.collation_name <> SERVERPROPERTY('collation')
    and d.name not in (select distinct DatabaseName from #tempchecks)
    AND d.name NOT IN ('ReportServer', 'ReportServerTempDB')
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 82)
    begin
    EXEC sp_MSforeachdb 
    'use [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 97)
    begin
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 62)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details)
    SELECT  62 AS CheckID ,
    [name] as DatabaseName,
    200 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Old Compatibility Level' AS Finding ,
    'http://BrentOzar.com/go/compatlevel' AS URL ,
    ( 'Database ' + [name] + ' is compatibility level '
    + CAST(compatibility_level AS VARCHAR(20))
    + ', which may cause unwanted results when trying to run queries that have newer T-SQL features.' ) AS Details
    FROM    sys.databases
    WHERE   name not in (select distinct DatabaseName from #tempchecks)
    and compatibility_level <> ( SELECT compatibility_level
    FROM   sys.databases
    WHERE  [name] = 'model'
    )
    end

if not exists (select 1 from #tempchecks where CheckId = 94)
begin
INSERT  INTO #BlitzResults
( CheckID ,
Priority ,
FindingsGroup ,
Finding ,
URL ,
Details)
SELECT  94 AS CheckID ,
50 as [Priority], 
'Reliability' as FindingsGroup, 
'Agent Jobs Without Failure Emails' as Finding, 
'http://BrentOzar.com/go/alerts' as URL, 
'The job ' + [name] + ' has not been set up to notify an operator if it fails.' as Details 
FROM msdb.[dbo].[sysjobs] j 
inner join 
( 
SELECT distinct 
[job_id] 
FROM [msdb].[dbo].[sysjobschedules] 
where next_run_date>0 
) s 
on j.job_id=s.job_id 
where j.enabled=1 
and j.notify_email_operator_id=0 
and j.notify_netsend_operator_id=0 
and j.notify_page_operator_id=0        
end
            
    IF @CheckUserDatabaseObjects = 1 
    BEGIN
              
    if not exists (select 1 from #tempchecks where CheckId = 32)
    begin
      EXEC dbo.sp_MSforeachdb 
        'USE [?]; 
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
        INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE t.is_ms_shipped = 0';
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 38)
    begin
      EXEC dbo.sp_MSforeachdb 
        'USE [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 39)
    begin
      EXEC dbo.sp_MSforeachdb 
        'USE [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 46)
    begin
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 47)
    begin
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; 
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
    end
    
            
    if not exists (select 1 from #tempchecks where CheckId = 48)
    begin
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 56)
    begin
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; 
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
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 95)
    begin
      IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
      AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
      BEGIN
        EXEC dbo.sp_MSforeachdb 
        'USE [?]; 
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
      end
              
    if not exists (select 1 from #tempchecks where CheckId = 60)
    begin
      EXEC sp_MSforeachdb 
      'USE [?]; 
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
      ''The ['' + DB_NAME() + ''] database has objects with fill factor <> 0. This can cause memory and storage performance problems, but may also prevent page splits.''
      FROM    [?].sys.indexes 
      WHERE   fill_factor <> 0 AND fill_factor <> 100 AND is_disabled = 0 AND is_hypothetical = 0';
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 78)
    begin
      EXEC dbo.sp_MSforeachdb 
      'USE [?]; 
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
    end

if not exists (select 1 from #tempchecks where CheckId = 86)
begin
            EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 86, DB_NAME(), 20, ''Security'', ''Elevated Permissions on a Database'', ''http://BrentOzar.com/go/elevated'', (''In ['' + DB_NAME() + ''], user ['' + u.name + '']  has the role ['' + g.name + ''].  This user can perform tasks beyond just reading and writing data.'') FROM [?].dbo.sysmembers m inner join [?].dbo.sysusers u on m.memberuid = u.uid inner join sysusers g on m.groupuid = g.uid where u.name <> ''dbo'' and g.name in (''db_owner'' , ''db_accessAdmin'' , ''db_securityadmin'' , ''db_ddladmin'')';
end
            
  END /* IF @CheckUserDatabaseObjects = 1 */

    IF @CheckProcedureCache = 1 
    BEGIN
        
    if not exists (select 1 from #tempchecks where CheckId = 35)
    begin
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
        AND EXISTS (SELECT 1 FROM sys.configurations WHERE name = 'optimize for ad hoc workloads' AND value_in_use = 0)
      HAVING  COUNT(*) > 1;
    end


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
    ( qs.statement_start_offset / 2 )
    + 1,
    ( ( CASE qs.statement_end_offset
    WHEN -1
    THEN DATALENGTH(st.text)
    ELSE qs.statement_end_offset
    END
    - qs.statement_start_offset )/ 2 ) + 1)
    FROM    #dm_exec_query_stats qs
      CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
      CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp

    /* Dump instances of our own script. We're not trying to tune ourselves. */
    DELETE  #dm_exec_query_stats
    WHERE   text LIKE '%sp_Blitz%'
    OR text LIKE '%#BlitzResults%'

    /* Look for implicit conversions */
            
    if not exists (select 1 from #tempchecks where CheckId = 63)
    begin
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
    qs.query_plan , qs.query_plan_filtered
    FROM    #dm_exec_query_stats qs
    WHERE   COALESCE(qs.query_plan_filtered,
      CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%CONVERT_IMPLICIT%'
      AND COALESCE(qs.query_plan_filtered,
      CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%PhysicalOp="Index Scan"%'
    end
            
    if not exists (select 1 from #tempchecks where CheckId = 64)
    begin
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
    qs.query_plan , qs.query_plan_filtered
    FROM    #dm_exec_query_stats qs
    WHERE   COALESCE(qs.query_plan_filtered,
    CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT_IMPLICIT%'
    end

    /* Look for missing indexes */
    if not exists (select 1 from #tempchecks where CheckId = 65)
    begin
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
    end

    /* Look for cursors */
                    
    if not exists (select 1 from #tempchecks where CheckId = 66)
    begin
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
    end
    
    /* Look for scalar user-defined functions */
                    
    if not exists (select 1 from #tempchecks where CheckId = 67)
    begin
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
    qs.query_plan , qs.query_plan_filtered
    FROM    #dm_exec_query_stats qs
    WHERE   COALESCE(qs.query_plan_filtered,
    CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<UserDefinedFunction%'
    end
    
    END /* IF @CheckProcedureCache = 1 */

    /*Check for the last good DBCC CHECKDB date */
    if not exists (select 1 from #tempchecks where CheckId = 68)
    begin
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
    EXEC sp_MSforeachdb 
    N'USE [?];
    INSERT #DBCCs
        (ParentObject, 
        Object, 
        Field, 
        Value)
    EXEC (''DBCC DBInfo() With TableResults, NO_INFOMSGS'');
    UPDATE #DBCCs SET DbName = N''?'' WHERE DbName IS NULL;';

    WITH    
    DB2
      AS ( SELECT DISTINCT Field ,
                Value ,
                DbName
        FROM     #DBCCs
        WHERE    Field = 'dbi_dbccLastKnownGood')
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  68 AS CheckID ,
    DB2.DbName as DatabaseName,
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
    FROM   DB2
    WHERE   DB2.DbName not in (select distinct DatabaseName from #tempchecks)
	and CONVERT(DATETIME, DB2.Value , 121 ) < DATEADD(DD, -14, CURRENT_TIMESTAMP)
  end

    /*Check for high VLF count: this will omit any database snapshots*/
                    
    if not exists (select 1 from #tempchecks where CheckId = 69)
    begin
    IF @@VERSION LIKE 'Microsoft SQL Server 2012%' 
    BEGIN
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
      EXEC sp_MSforeachdb N'USE [?];    
      INSERT INTO #LogInfo2012 
      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';      
      IF    @@ROWCOUNT > 50            
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
        ,''http://BrentOzar.com/go/vlf ''                              
        ,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''  
        FROM #LogInfo2012
        WHERE EXISTS (SELECT name FROM master.sys.databases 
                WHERE source_database_id is null) ;            
      END                       
    TRUNCATE TABLE #LogInfo2012;'
    DROP TABLE #LogInfo2012;
    END
    
    IF @@VERSION NOT LIKE 'Microsoft SQL Server 2012%' 
    BEGIN
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
      EXEC sp_MSforeachdb N'USE [?];    
      INSERT INTO #LogInfo 
      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';      
      IF    @@ROWCOUNT > 50            
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
    end
    /*Verify that the servername is set */          
    if not exists (select 1 from #tempchecks where CheckId = 70)
    begin
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
      '@@Servername not set' AS Finding ,
      'http://BrentOzar.com/go/servername' AS URL ,
      '@@Servername variable is null. Correct by executing "sp_addserver ''<LocalServerName>'', local"' AS Details
    END;
    end

/*Check for non-aligned indexes in partioned databases*/
                  
    if not exists (select 1 from #tempchecks where CheckId = 72)
    begin
    CREATE TABLE #partdb
        (
          dbname NVARCHAR(128) ,
          objectname NVARCHAR(200) ,
          type_desc NVARCHAR(128)
        )
  EXEC dbo.sp_MSforeachdb 
    'USE [?]; 
    insert into #partdb(dbname, objectname, type_desc)
    SELECT distinct db_name(DB_ID()) as DBName,o.name Object_Name,ds.type_desc
    FROM sys.objects AS o JOIN sys.indexes AS i ON o.object_id = i.object_id 
    JOIN sys.data_spaces ds on ds.data_space_id = i.data_space_id
    LEFT OUTER JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
    WHERE  o.type = ''u''
     -- Clustered and Non-Clustered indexes
    AND i.type IN (1, 2) 
    AND o.name in 
      (
        SELECT a.name from 
          (SELECT ob.name, ds.type_desc from sys.objects ob JOIN sys.indexes ind on ind.object_id = ob.object_id join sys.data_spaces ds on ds.data_space_id = ind.data_space_id
          GROUP BY ob.name, ds.type_desc ) a group by a.name having COUNT (*) > 1
      )'
      INSERT  INTO #BlitzResults
          ( CheckID ,
            DatabaseName,
            Priority ,
            FindingsGroup ,
            Finding ,
            URL ,
            Details
          )
          SELECT DISTINCT
              72 AS CheckId ,
              dbname as DatabaseName,
              100 AS Priority ,
              'Performance' AS FindingsGroup ,
              'The partitioned database ' + dbname
              + ' may have non-aligned indexes' AS Finding ,
              'http://BrentOzar.com/go/aligned' AS URL ,
              'Having non-aligned indexes on partitioned tables may cause inefficient query plans and CPU pressure' AS Details
          FROM    #partdb
          WHERE   dbname IS NOT NULL
          and dbname not in (select distinct DatabaseName from #tempchecks)
      DROP TABLE #partdb
    end
    
    /*Check to see if a failsafe operator has been configured*/   
    if not exists (select 1 from #tempchecks where CheckId = 73)
    begin

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
    end
    
    /*Identify globally enabled trace flags*/
    if not exists (select 1 from #tempchecks where CheckId = 74)
    begin
    IF OBJECT_ID('tempdb..#TraceStatus') IS NOT NULL 
    DROP TABLE #TraceStatus;
    CREATE TABLE #TraceStatus
    (
    TraceFlag VARCHAR(10) ,
    status BIT ,
    Global BIT ,
    Session BIT
    );
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
    'Trace flag ' + T.TraceFlag + ' is enabled globally.' AS Details
    FROM    #TraceStatus T
    end
    
    /*Check for transaction log file larger than data file */             
    if not exists (select 1 from #tempchecks where CheckId = 75)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  75 AS CheckId ,
    DB_NAME(a.database_id),
    50 AS Priority ,
    'Reliability' AS FindingsGroup ,
    'Transaction Log Larger than Data File' AS Finding ,
    'http://BrentOzar.com/go/biglog' AS URL ,
    'The database [' + DB_NAME(a.database_id)
    + '] has a transaction log file larger than a data file. This may indicate that transaction log backups are not being performed or not performed often enough.' AS Details
    FROM    sys.master_files a
    WHERE   a.type = 1
    and DB_NAME(a.database_id) not in (select distinct DatabaseName from #tempchecks)
    AND a.size > 125000 /* Size is measured in pages here, so this gets us log files over 1GB. */
    AND a.size > ( SELECT   SUM(b.size)
    FROM     sys.master_files b
    WHERE    a.database_id = b.database_id
    AND b.type = 0
    )
    AND a.database_id IN ( SELECT   database_id
    FROM     sys.databases
    WHERE    source_database_id IS NULL )
    end
    
    /*Check for collation conflicts between user databases and tempdb */          
    if not exists (select 1 from #tempchecks where CheckId = 76)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  76 AS CheckId ,
    name as DatabaseName,
    50 AS Priority ,
    'Reliability' AS FindingsGroup ,
    'Collation for ' + name
    + ' different than tempdb collation' AS Finding ,
    'http://BrentOzar.com/go/collate' AS URL ,
    'Collation differences between user databases and tempdb can cause conflicts especially when comparing string values' AS Details
    FROM    sys.databases
    WHERE   name NOT IN ( 'master', 'model', 'msdb', 'ReportServer', 'ReportServerTempDB' )
    and name not in (select distinct DatabaseName from #tempchecks)
    AND collation_name <> ( SELECT  collation_name
    FROM    sys.databases
    WHERE   name = 'tempdb'
    )
    end
                    
    if not exists (select 1 from #tempchecks where CheckId = 77)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    DatabaseName,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  77 AS CheckId ,
    dSnap.[name] as DatabaseName,
    50 AS Priority ,
    'Reliability' AS FindingsGroup ,
    'Database Snapshot Online' AS Finding ,
    'http://BrentOzar.com/go/snapshot' AS URL ,
    'Database [' + dSnap.[name] + '] is a snapshot of [' + dOriginal.[name] + ']. Make sure you have enough drive space to maintain the snapshot as the original database grows.' AS Details
    FROM sys.databases dSnap
    INNER JOIN sys.databases dOriginal ON dSnap.source_database_id = dOriginal.database_id
    and dSnap.name not in (select distinct DatabaseName from #tempchecks)
    end
                    
    if not exists (select 1 from #tempchecks where CheckId = 79)
    begin
    INSERT  INTO #BlitzResults
    ( CheckID ,
    Priority ,
    FindingsGroup ,
    Finding ,
    URL ,
    Details
    )
    SELECT  79 AS CheckId ,
    100 AS Priority ,
    'Performance' AS FindingsGroup ,
    'Shrink Database Job' AS Finding ,
    'http://BrentOzar.com/go/autoshrink' AS URL ,
    'In the [' + j.[name] + '] job, step [' + step.[step_name] + '] has SHRINKDATABASE or SHRINKFILE, which may be causing database fragmentation.' AS Details
    FROM msdb.dbo.sysjobs j 
    INNER JOIN msdb.dbo.sysjobsteps step ON j.job_id = step.job_id 
    WHERE step.command LIKE N'%SHRINKDATABASE%' OR step.command LIKE N'%SHRINKFILE%'
    end
                    
    if not exists (select 1 from #tempchecks where CheckId = 80)
    begin
    EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 80, DB_NAME(), 50, ''Reliability'', ''Max File Size Set'', ''http://BrentOzar.com/go/maxsize'', (''The ['' + DB_NAME() + ''] database file '' + name + '' has a max file size set to '' + CAST(CAST(max_size AS BIGINT) * 8 / 1024 AS VARCHAR(100)) + ''MB. If it runs out of space, the database will stop working even though there may be drive space available.'') FROM sys.database_files WHERE max_size <> 268435456 AND max_size <> -1 AND type <> 2';
	end

	if not exists (select 1 from #tempchecks where CheckId = 81)
	begin
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
    + CAST(cr.value_in_use AS VARCHAR(100)) + '. When someone does a RECONFIGURE or restarts the instance, this setting will start taking effect.' ) AS Details
    FROM    sys.configurations cr
    WHERE   cr.value <> cr.value_in_use ;
    end
                    

	IF @CheckServerInfo = 1
	BEGIN

		if not exists (select 1 from #tempchecks where CheckId = 83)
		begin
		IF EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_server_services')
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
		end

		/* Check 84 - SQL Server 2012 */              
		if not exists (select 1 from #tempchecks where CheckId = 84)
		begin
		IF EXISTS (SELECT * FROM sys.all_objects o INNER JOIN sys.all_columns c ON o.object_id = c.object_id WHERE o.name = 'dm_os_sys_info' AND c.name = 'physical_memory_kb')
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
		IF EXISTS (SELECT * FROM sys.all_objects o INNER JOIN sys.all_columns c ON o.object_id = c.object_id WHERE o.name = 'dm_os_sys_info' AND c.name = 'physical_memory_in_bytes')
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
		end

	                
		if not exists (select 1 from #tempchecks where CheckId = 85)
		begin
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
		N'Version: ' + CAST(SERVERPROPERTY('productversion') AS NVARCHAR(100)) 
		+ N'. Patch Level: ' + CAST(SERVERPROPERTY ('productlevel') AS NVARCHAR(100)) 
		+ N'. Edition: ' + CAST(SERVERPROPERTY ('edition') AS VARCHAR(100))
		+ N'. AlwaysOn Enabled: ' + CAST(COALESCE(SERVERPROPERTY ('IsHadrEnabled'),0) AS VARCHAR(100))
		+ N'. AlwaysOn Mgr Status: ' + CAST(COALESCE(SERVERPROPERTY ('HadrManagerStatus'),0) AS VARCHAR(100))
		end


		if not exists (select 1 from #tempchecks where CheckId = 88)
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
		FROM sys.databases
		WHERE database_id = 2
		END

	if not exists (select 1 from #tempchecks where CheckId = 92)
	BEGIN
	CREATE TABLE #driveInfo(drive NVARCHAR,SIZE DECIMAL(18,2))
	INSERT INTO #driveInfo(drive,SIZE)
	EXEC master..xp_fixeddrives

	INSERT  INTO #BlitzResults
	        ( CheckID ,
	          Priority ,
	          FindingsGroup ,
	          Finding ,
	          URL ,
	          Details
	        )
	SELECT 92 AS CheckID ,
	250 AS Priority ,
	'ServerInfo' AS FindingsGroup ,
	'Drive ' + i.drive + ' Space' as Finding,
	'' AS URL ,
	CAST(i.SIZE AS VARCHAR) + 'MB free on ' + i.drive + ' drive' AS Details
	FROM #driveInfo AS i
	DROP TABLE #driveInfo
    END
END /* IF @CheckServerInfo = 1 */


/* Delete priorites they wanted to skip. Needs to be above the Brent Ozar Unlimited credits. */
IF @IgnorePrioritiesAbove IS NOT NULL
	DELETE #BlitzResults
	WHERE [Priority] > @IgnorePrioritiesAbove;
	
IF @IgnorePrioritiesBelow IS NOT NULL
	DELETE #BlitzResults
	WHERE [Priority] < @IgnorePrioritiesBelow;


                    
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

    SET @Version = 19;
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
    'sp_Blitz (TM) v20 Apr 12 2013' ,
    'From Brent Ozar Unlimited' ,
    'http://www.BrentOzar.com/blitz/' ,
    'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'

    );

    if @SkipChecksTable is not null
      begin 
        delete from #BlitzResults
        where DatabaseName in (select DatabaseName from #tempchecks)

        create table #exempt(
        ID int identity(1,1),
        ServerName NVARCHAR(128),
        DatabaseName NVARCHAR(128),
        CheckId int)

        insert into #exempt(ServerName,DatabaseName, CheckId)
        select ServerName,DatabaseName, CheckId 
        from #GetChecks
        where DatabaseName is not null
        and CheckId is not null
        and (ServerName is null 
        or ServerName = SERVERPROPERTY('ServerName'))
        
        while (select COUNT(*) from #exempt) > 0
        begin 
          declare @ID int,
          @DatabaseName NVARCHAR(128),
          @checkid int
          set @ID = (select top 1 ID from #exempt)
          set @DatabaseName = (select DatabaseName from #exempt where ID = @ID)
          set @checkid = (select CheckId from #exempt where ID = @ID)
          delete from #BlitzResults
          where (DatabaseName = @DatabaseName)
          --and CheckID = @checkid)

          delete from #exempt
        where ID = @ID
        end
        drop table #exempt
      end

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
    ELSE IF @OutputType IN ('CSV', 'RSV')
		BEGIN
			
			SELECT  Result = CAST([Priority] AS NVARCHAR(100)) + @separator
				+ CAST(CheckID AS NVARCHAR(100)) + @separator
				+ COALESCE([FindingsGroup], '(N/A)') + @separator
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
    ELSE 
    BEGIN
    SELECT  [Priority] ,
    [FindingsGroup] ,
    [Finding] ,
    [DatabaseName],
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

    IF @OutputProcedureCache = 1 AND @CheckProcedureCache = 1
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
    CASE WHEN DATEDIFF(mi, creation_time, qs.last_execution_time) = 0
    THEN 0
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

  end
    SET NOCOUNT OFF;
GO

/*
Sample execution call with the most common parameters:
EXEC [master].[dbo].[sp_Blitz]
    @CheckUserDatabaseObjects = 1 ,
    @CheckProcedureCache = 0 ,
    @OutputType = 'TABLE' ,
    @OutputProcedureCache = 0 ,
    @CheckProcedureCacheFilter = NULL,
    @CheckServerInfo = 1

*/
