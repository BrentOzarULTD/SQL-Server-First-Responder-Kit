USE master;
GO

IF OBJECT_ID('master.dbo.sp_Blitz') IS NOT NULL 
    DROP PROC dbo.sp_Blitz;
GO

CREATE PROCEDURE dbo.sp_Blitz
    @CheckUserDatabaseObjects TINYINT = 1 ,
    @CheckProcedureCache TINYINT = 0
AS 
    SET NOCOUNT ON;
/*
    sp_Blitz v10 - Sept 26, 2012
    
    (C) 2012, Brent Ozar PLF, LLC

To learn more, visit http://www.BrentOzar.com/go/blitz where you can download
new versions for free, watch training videos on how it works, get more info on
the findings, and more.

Known limitations of this version:
 - No support for offline databases. If you can't query some of your databases,
   this script will fail.  That's fairly high on our list of things to improve
   since we like mirroring too.
 - No support for SQL Server 2000.
 - If the server has databases in compatibility mode 80, sp_Blitz will fail.

Unknown limitations of this version:
 - None.  (If we knew them, they'd be known.  Duh.)

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

    IF OBJECT_ID('tempdb..#BlitzResults') IS NOT NULL 
        DROP TABLE #BlitzResults;
    CREATE TABLE #BlitzResults
        (
          ID INT IDENTITY(1, 1) ,
          CheckID INT ,
          Priority TINYINT ,
          FindingsGroup VARCHAR(50) ,
          Finding VARCHAR(200) ,
          URL VARCHAR(200) ,
          Details NVARCHAR(4000)
        );

    IF OBJECT_ID('tempdb..#ConfigurationDefaults') IS NOT NULL 
        DROP TABLE #ConfigurationDefaults;
    CREATE TABLE #ConfigurationDefaults
        (
          name NVARCHAR(128) ,
          DefaultValue BIGINT
        );

    DECLARE @StringToExecute NVARCHAR(4000);

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  1 AS CheckID ,
                    1 AS Priority ,
                    'Backup' AS FindingsGroup ,
                    'Backups Not Performed Recently' AS Finding ,
                    'http://BrentOzar.com/go/nobak' AS URL ,
                    'Database ' + d.Name + ' last backed up: '
                    + CAST(COALESCE(MAX(b.backup_finish_date), ' never ') AS VARCHAR(200)) AS Details
            FROM    master.sys.databases d
                    LEFT OUTER JOIN msdb.dbo.backupset b ON d.name = b.database_name
                                                            AND b.type = 'D'
            WHERE   d.database_id <> 2  /* Bonus points if you know what that means */
            GROUP BY d.name
            HAVING  MAX(b.backup_finish_date) <= DATEADD(dd, -7, GETDATE());


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
              
            )
            SELECT  1 AS CheckID ,
                    1 AS Priority ,
                    'Backup' AS FindingsGroup ,
                    'Backups Not Performed Recently' AS Finding ,
                    'http://BrentOzar.com/go/nobak' AS URL ,
                    ( 'Database ' + d.Name + ' never backed up.' ) AS Details
            FROM    master.sys.databases d
            WHERE   d.database_id <> 2 /* Bonus points if you know what that means */
                    AND NOT EXISTS ( SELECT *
                                     FROM   msdb.dbo.backupset b
                                     WHERE  d.name = b.database_name
                                            AND b.type = 'D' )

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT DISTINCT
                    2 AS CheckID ,
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
                    AND NOT EXISTS ( SELECT *
                                     FROM   msdb.dbo.backupset b
                                     WHERE  d.name = b.database_name
                                            AND b.type = 'L'
                                            AND b.backup_finish_date >= DATEADD(dd,
                                                              -7, GETDATE()) );




    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT TOP 1
                    3 AS CheckID ,
                    200 AS Priority ,
                    'Backup' AS FindingsGroup ,
                    'MSDB Backup History Not Purged' AS Finding ,
                    'http://BrentOzar.com/go/history' AS URL ,
                    ( 'Database backup history retained back to '
                      + CAST(bs.backup_start_date AS VARCHAR(20)) ) AS Details
            FROM    msdb.dbo.backupset bs
            WHERE   bs.backup_start_date <= DATEADD(dd, -60, GETDATE())
            ORDER BY backup_set_id ASC;


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
                    LEFT OUTER JOIN sys.syslogins sl ON j.owner_sid = sl.sid
            WHERE   j.enabled = 1
                    AND sl.name <> SUSER_SNAME(0x01);

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

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 8 AS CheckID, 150 AS Priority, ''Security'' AS FindingsGroup, ''Server Audits Running'' AS Finding, 
    ''http://BrentOzar.com/go/audits'' AS URL,
    (''SQL Server built-in audit functionality is being used by server audit: '' + [name]) AS Details FROM sys.dm_server_audit_status'
            EXECUTE(@StringToExecute)
        END;

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 9 AS CheckID, 200 AS Priority, ''Surface Area'' AS FindingsGroup, ''Endpoints Configured'' AS Finding, 
    ''http://BrentOzar.com/go/endpoints/'' AS URL,
    (''SQL Server endpoints are configured.  These can be used for database mirroring or Service Broker, but if you do not need them, avoid leaving them enabled.  Endpoint name: '' + [name]) AS Details FROM sys.endpoints WHERE type <> 2'
            EXECUTE(@StringToExecute)
        END;

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 10 AS CheckID, 100 AS Priority, ''Performance'' AS FindingsGroup, ''Resource Governor Enabled'' AS Finding, 
    ''http://BrentOzar.com/go/rg'' AS URL,
    (''Resource Governor is enabled.  Queries may be throttled.  Make sure you understand how the Classifier Function is configured.'') AS Details FROM sys.resource_governor_configuration WHERE is_enabled = 1'
            EXECUTE(@StringToExecute)
        END;


    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 11 AS CheckID, 100 AS Priority, ''Performance'' AS FindingsGroup, ''Server Triggers Enabled'' AS Finding, 
    ''http://BrentOzar.com/go/logontriggers/'' AS URL,
    (''Server Trigger ['' + [name] ++ ''] is enabled, so it runs every time someone logs in.  Make sure you understand what that trigger is doing - the less work it does, the better.'') AS Details FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0'
            EXECUTE(@StringToExecute)
        END;


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  12 AS CheckID ,
                    10 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Auto-Close Enabled' AS Finding ,
                    'http://BrentOzar.com/go/autoclose' AS URL ,
                    ( 'Database [' + [name]
                      + '] has auto-close enabled.  This setting can dramatically decrease performance.' ) AS Details
            FROM    sys.databases
            WHERE   is_auto_close_on = 1;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  12 AS CheckID ,
                    10 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Auto-Shrink Enabled' AS Finding ,
                    'http://BrentOzar.com/go/autoshrink' AS URL ,
                    ( 'Database [' + [name]
                      + '] has auto-shrink enabled.  This setting can dramatically decrease performance.' ) AS Details
            FROM    sys.databases
            WHERE   is_auto_shrink_on = 1;


    IF @@VERSION LIKE '%Microsoft SQL Server 2000%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 14 AS CheckID, 50 AS Priority, ''Reliability'' AS FindingsGroup, ''Page Verification Not Optimal'' AS Finding, 
    ''http://BrentOzar.com/go/torn'' AS URL,
    (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') AS Details FROM sys.databases WHERE page_verify_option < 1 AND name <> ''tempdb'''
            EXECUTE(@StringToExecute)
        END;

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 14 AS CheckID, 50 AS Priority, ''Reliability'' AS FindingsGroup, ''Page Verification Not Optimal'' AS Finding, 
    ''http://BrentOzar.com/go/torn'' AS URL,
    (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') AS Details FROM sys.databases WHERE page_verify_option < 2 AND name <> ''tempdb'''
            EXECUTE(@StringToExecute)
        END;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  15 AS CheckID ,
                    110 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Auto-Create Stats Disabled' AS Finding ,
                    'http://BrentOzar.com/go/acs' AS URL ,
                    ( 'Database [' + [name]
                      + '] has auto-create-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.' ) AS Details
            FROM    sys.databases
            WHERE   is_auto_create_stats_on = 0;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  16 AS CheckID ,
                    110 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Auto-Update Stats Disabled' AS Finding ,
                    'http://BrentOzar.com/go/aus' AS URL ,
                    ( 'Database [' + [name]
                      + '] has auto-update-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.' ) AS Details
            FROM    sys.databases
            WHERE   is_auto_update_stats_on = 0;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  17 AS CheckID ,
                    110 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Stats Updated Asynchronously' AS Finding ,
                    'http://BrentOzar.com/go/asyncstats' AS URL ,
                    ( 'Database [' + [name]
                      + '] has auto-update-stats-async enabled.  When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.' ) AS Details
            FROM    sys.databases
            WHERE   is_auto_update_stats_async_on = 1;


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  18 AS CheckID ,
                    110 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'Forced Parameterization On' AS Finding ,
                    'http://BrentOzar.com/go/forced' AS URL ,
                    ( 'Database [' + [name]
                      + '] has forced parameterization enabled.  SQL Server will aggressively reuse query execution plans even if the applications do not parameterize their queries.  This can be a performance booster with some programming languages, or it may use universally bad execution plans when better alternatives are available for certain parameters.' ) AS Details
            FROM    sys.databases
            WHERE   is_parameterization_forced = 1;


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  19 AS CheckID ,
                    200 AS Priority ,
                    'Informational' AS FindingsGroup ,
                    'Replication In Use' AS Finding ,
                    'http://BrentOzar.com/go/repl' AS URL ,
                    ( 'Database [' + [name]
                      + '] is a replication publisher, subscriber, or distributor.' ) AS Details
            FROM    sys.databases
            WHERE   is_published = 1
                    OR is_subscribed = 1
                    OR is_merge_published = 1
                    OR is_distributor = 1;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  20 AS CheckID ,
                    110 AS Priority ,
                    'Informational' AS FindingsGroup ,
                    'Date Correlation On' AS Finding ,
                    'http://BrentOzar.com/go/corr' AS URL ,
                    ( 'Database [' + [name]
                      + '] has date correlation enabled.  This is not a default setting, and it has some performance overhead.  It tells SQL Server that date fields in two tables are related, and SQL Server maintains statistics showing that relation.' ) AS Details
            FROM    sys.databases
            WHERE   is_date_correlation_on = 1;

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT 21 AS CheckID, 20 AS Priority, ''Encryption'' AS FindingsGroup, ''Database Encrypted'' AS Finding, 
    ''http://BrentOzar.com/go/tde'' AS URL,
    (''Database ['' + [name] + ''] has Transparent Data Encryption enabled.  Make absolutely sure you have backed up the certificate and private key, or else you will not be able to restore this database.'') AS Details FROM sys.databases WHERE is_encrypted = 1'
            EXECUTE(@StringToExecute)
        END;

/* Compare sp_configure defaults */
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
    VALUES  ( 'remote login timeout (s)', 20 );
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
                    cd.name AS Finding ,
                    'http://BrentOzar.com/go/conf' AS URL ,
                    ( 'This sp_configure option has been changed.  Its default value is '
                      + CAST(cd.[DefaultValue] AS VARCHAR(100))
                      + ' and it has been set to '
                      + CAST(cr.value_in_use AS VARCHAR(100)) + '.' ) AS Details
            FROM    #ConfigurationDefaults cd
                    INNER JOIN sys.configurations cr ON cd.name = cr.name
            WHERE   cd.DefaultValue <> cr.value_in_use;

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT DISTINCT
                    24 AS CheckID ,
                    20 AS Priority ,
                    'Reliability' AS FindingsGroup ,
                    'System Database on C Drive' AS Finding ,
                    'http://BrentOzar.com/go/drivec' AS URL ,
                    ( 'The ' + DB_NAME(database_id)
                      + ' database has a file on the C drive.  Putting system databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
            FROM    sys.master_files
            WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
                    AND DB_NAME(database_id) IN ( 'master', 'model', 'msdb' );

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT TOP 1
                    25 AS CheckID ,
                    100 AS Priority ,
                    'Performance' AS FindingsGroup ,
                    'TempDB on C Drive' AS Finding ,
                    'http://BrentOzar.com/go/drivec' AS URL ,
                    ( 'The tempdb database has files on the C drive.  TempDB frequently grows unpredictably, putting your server at risk of running out of C drive space and crashing hard.  C is also often much slower than other drives, so performance may be suffering.' ) AS Details
            FROM    sys.master_files
            WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
                    AND DB_NAME(database_id) = 'tempdb';

    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT DISTINCT
                    26 AS CheckID ,
                    20 AS Priority ,
                    'Reliability' AS FindingsGroup ,
                    'User Databases on C Drive' AS Finding ,
                    'http://BrentOzar.com/go/cdrive' AS URL ,
                    ( 'The ' + DB_NAME(database_id)
                      + ' database has a file on the C drive.  Putting databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
            FROM    sys.master_files
            WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
                    AND DB_NAME(database_id) NOT IN ( 'master', 'model',
                                                      'msdb', 'tempdb' );


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  27 AS CheckID ,
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

    	IF ( SELECT count(*)
		                    FROM    msdb.dbo.sysalerts
		                    WHERE   severity BETWEEN 19 AND 25) < 7
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
                SELECT  60 AS CheckID ,
                        50 AS Priority ,
                        'Reliability' AS FindingsGroup ,
                        'No Alerts for Corruption' AS Finding ,
                        'http://BrentOzar.com/go/alert' AS URL ,
                        ( 'SQL Server Agent do not exist for errors 823, 824, and 825.  These three errors can give you notification about early hardware failure. Enabling them can prevent you a lot of heartbreak.' ) AS Details;

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
                        ( 'SQL Server Agent do not exist for severity levels 19 through 25.  These are some very severe SQL Server errors. Knowing that these are happening may let you recover from errors faster.' ) AS Details;

            --check for disabled alerts
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
                SELECT  62 AS CheckID ,
                        50 AS Priority ,
                        'Reliability' AS FindingsGroup ,
                        'Alerts Disabled' AS Finding ,
                        'http://www.BrentOzar.com/blitz/configure-sql-server-alerts/' AS URL ,
                        ( 'The following Alert is disabled, please review and enable if desired: '
                          + name ) AS Details
                FROM    msdb.dbo.sysalerts
                WHERE   enabled = 0


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

    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 33, 200, ''Licensing'', ''Enterprise Edition Features In Use'', ''http://BrentOzar.com/go/ee'', (''The ? database is using '' + feature_name + ''.  If this database is restored onto a Standard Edition server, the restore will fail.'') FROM [?].sys.dm_db_persisted_sku_features';
        END;


    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT TOP 1
                    34 AS CheckID ,
                    1 AS Priority ,
                    ''Corruption'' AS FindingsGroup ,
                    ''Database Corruption Detected'' AS Finding ,
                    ''http://BrentOzar.com/go/repair'' AS URL ,
                    ( ''Database mirroring has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_db_mirroring_auto_page_repair.'' ) AS Details
            FROM    sys.dm_db_mirroring_auto_page_repair
            WHERE   modification_time >= DATEADD(dd, -30, GETDATE()) ;'
            EXECUTE(@StringToExecute)
        END;

    IF @CheckProcedureCache = 1 
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
                              + ' query plans are taking up '
                              + CAST(( SUM(cp.size_in_bytes * 1.0) / 1048576 ) AS VARCHAR(20))
                              + ' megabytes in the procedure cache. This may be wasted memory if we cache plans for queries that never get called again. This may be a good use case for SQL Server 2008''s Optimize for Ad Hoc or for Forced Parameterization.' ) AS Details
                    FROM    sys.dm_exec_cached_plans AS cp
                            CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle)
                            AS qp
                    WHERE   cp.usecounts = 1
                            AND qp.query_plan IS NOT NULL
                            AND cp.objtype = 'Adhoc'
                    HAVING  COUNT(*) > 1;
        END

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
                    INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
                                                         AND fs.[file_id] = mf.[file_id]
            WHERE   ( io_stall_read_ms / ( 1.0 + num_of_reads ) ) > 100;

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
                    INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
                                                         AND fs.[file_id] = mf.[file_id]
            WHERE   ( io_stall_write_ms / ( 1.0 + num_of_writes ) ) > 20;


    IF ( SELECT COUNT(*)
         FROM   tempdb.sys.database_files
         WHERE  type_desc = 'ROWS'
       ) = 1 
        BEGIN
            INSERT  INTO #BlitzResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details
                    )
            VALUES  ( 40 ,
                      100 ,
                      'Performance' ,
                      'TempDB Only Has 1 Data File' ,
                      'http://BrentOzar.com/go/tempdb' ,
                      'TempDB is only configured with one data file.  More data files are usually required to alleviate SGAM contention.'
                    );
        END;

    EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT 41, 100, ''Performance'', ''Multiple Log Files on One Drive'', ''http://BrentOzar.com/go/manylogs'', (''The ? database has multiple log files on the '' + LEFT(physical_name, 1) + '' drive. This is not a performance booster because log file access is sequential, not parallel.'') FROM [?].sys.database_files WHERE type_desc = ''LOG'' AND ''?'' <> ''[tempdb]'' GROUP BY LEFT(physical_name, 1) HAVING COUNT(*) > 1';

    EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 42, 100, ''Performance'', ''Uneven File Growth Settings in One Filegroup'', ''http://BrentOzar.com/go/grow'', (''The ? database has multiple data files in one filegroup, but they are not all set up to grow in identical amounts.  This can lead to uneven file activity inside the filegroup.'') FROM [?].sys.database_files WHERE type_desc = ''ROWS'' GROUP BY data_space_id HAVING COUNT(DISTINCT growth) > 1 OR COUNT(DISTINCT is_percent_growth) > 1';

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



    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  49 AS CheckID ,
                    200 AS Priority ,
                    'Informational' AS FindingsGroup ,
                    'Linked Server Configured' AS Finding ,
                    'http://BrentOzar.com/go/link' AS URL ,
                    'The server [' + name
                    + '] is configured as a linked server. Check its security configuration to make sure it isn''t connecting with SA or some other bone-headed administrative login, because any user who queries it might get admin-level permissions.' AS Details
            FROM    sys.servers
            WHERE   is_linked = 1



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



    IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
        BEGIN
            SET @StringToExecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
            SELECT  51 AS CheckID ,
                    1 AS Priority ,
                    ''Performance'' AS FindingsGroup ,
                    ''Memory Dangerously Low'' AS Finding ,
                    ''http://BrentOzar.com/go/max'' AS URL ,
                    ''Only ''
                    + CAST(( CAST(m.available_physical_memory_kb AS BIGINT)
                             / 1024 ) AS VARCHAR(20))
                    + '' megabytes, but the server only has ''
                    + CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
                    + ''megabytes of memory are free.  As the server runs out of memory, there is danger of swapping to disk, which will kill performance.'' AS Details
            FROM    sys.dm_os_sys_memory m
            WHERE   CAST(m.available_physical_memory_kb AS BIGINT) < 262144'
            EXECUTE(@StringToExecute)
        END;


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

/*
    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  54 AS CheckID ,
                    200 AS Priority ,
                    'Service Accounts' AS FindingsGroup ,
                    servicename AS Finding ,
                    'http://www.BrentOzar.com/blitz/service-account' AS URL ,
                    'This service is running as [' + service_account
                    + '].  This affects the server''s ability to copy files to network locations, lock pages in memory, and perform Instant File Initialization.' AS Details
            FROM    sys.dm_server_services
*/

/* Thanks to Ali for this check! http://www.alirazeghi.com */
    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  55 AS CheckID ,
                    200 AS Priority ,
                    'Security' AS FindingsGroup ,
                    'Database Owner <> SA' AS Finding ,
                    'http://BrentOzar.com/go/owndb' AS URL ,
                    ( 'Database name: ' + name + '   ' + 'Owner name: '
                      + SUSER_SNAME(owner_sid) ) AS Details
            FROM    sys.databases
            WHERE   SUSER_SNAME(owner_sid) <> SUSER_SNAME(0x01);

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
                    ( 'Job ' + j.name
                      + '] runs automatically when SQL Server Agent starts up.  Make sure you know exactly what this job is doing, because it could pose a security risk.' ) AS Details
            FROM    msdb.dbo.sysschedules sched
                    JOIN msdb.dbo.sysjobschedules jsched ON sched.schedule_id = jsched.schedule_id
                    JOIN msdb.dbo.sysjobs j ON jsched.job_id = j.job_id
            WHERE   sched.freq_type = 64;


    INSERT  INTO #BlitzResults
            ( CheckID ,
              Priority ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  58 AS CheckID ,
                    200 AS Priority ,
                    'Reliability' AS FindingsGroup ,
                    'Database Collation Mismatch' AS Finding ,
                    'http://BrentOzar.com/go/collate' AS URL ,
                    ( 'Database ' + d.NAME + ' has collation '
                      + d.collation_name + '; Server collation is '
                      + CONVERT(VARCHAR(100), SERVERPROPERTY('collation')) ) AS Details
            FROM    master.sys.databases d
            WHERE   d.collation_name <> SERVERPROPERTY('collation')

    EXEC sp_msforeachdb 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
SELECT  DISTINCT 59 AS CheckID, 
        100 AS Priority, 
        ''Performance'' AS FindingsGroup, 
        ''File growth set to percent'', 
        ''http://brentozar.com/go/percentgrowth'' AS URL,
        ''The ? database is using percent filegrowth settings. This can lead to out of control filegrowth.''
FROM    [?].sys.database_files 
WHERE   is_percent_growth = 1 ';


INSERT  INTO #BlitzResults
        ( CheckID ,
          Priority ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  61 AS CheckID ,
              100 AS Priority ,
              'Performance' AS FindingsGroup ,
              'Unusual SQL Server Edition' AS Finding ,
              'http://BrentOzar.com/go/workgroup' AS URL ,
              ( 'This server is using ' + CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) + ', which is capped at low amounts of CPU and memory.' ) AS Details
      WHERE CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Standard%' AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Enterprise%' AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Developer%'

	  INSERT  INTO #BlitzResults
	          ( CheckID ,
	            Priority ,
	            FindingsGroup ,
	            Finding ,
	            URL ,
	            Details
	          )
	          SELECT  62 AS CheckID ,
	                200 AS Priority ,
	                'Performance' AS FindingsGroup ,
	                'Old Compatibility Level' AS Finding ,
	                'http://BrentOzar.com/go/compatlevel' AS URL ,
	                ( 'Database ' + name + ' is compatibility level ' + CAST(compatibility_level AS VARCHAR(20)) + ', which may cause unwanted results when trying to run queries that have newer T-SQL features.' ) AS Details
			from sys.databases
	  where compatibility_level <>
	     (select  compatibility_level from sys.databases
	     where name = 'model')
	  
	  
	  

    IF @CheckUserDatabaseObjects = 1 
        BEGIN

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 32, 110, ''Performance'', ''Triggers on Tables'', ''http://BrentOzar.com/go/trig'', (''The ? database has triggers on the '' + s.name + ''.'' + o.name + '' table.'') FROM [?].sys.triggers t INNER JOIN [?].sys.objects o ON t.parent_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE t.is_ms_shipped = 0';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 38, 110, ''Performance'', ''Active Tables Without Clustered Indexes'', ''http://BrentOzar.com/go/heaps'', (''The ? database has heaps - tables without a clustered index - that are being actively queried.'') FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id INNER JOIN sys.databases sd ON sd.name = ''?'' LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NOT NULL AND sd.name <> ''tempdb'' AND o.is_ms_shipped = 0 AND o.type <> ''S''';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 39, 110, ''Performance'', ''Inactive Tables Without Clustered Indexes'', ''http://BrentOzar.com/go/heaps'', (''The ? database has heaps - tables without a clustered index - that have not been queried since the last restart.  These may be backup tables carelessly left behind.'') FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id INNER JOIN sys.databases sd ON sd.name = ''?'' LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NULL AND sd.name <> ''tempdb'' AND o.is_ms_shipped = 0 AND o.type <> ''S''';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT 46, 100, ''Performance'', ''Leftover Fake Indexes From Wizards'', ''http://BrentOzar.com/go/hypo'', (''The index [?].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is a leftover hypothetical index from the Index Tuning Wizard or Database Tuning Advisor.  This index is not actually helping performance and should be removed.'') from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE i.is_hypothetical = 1';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT 47, 100, ''Performance'', ''Indexes Disabled'', ''http://BrentOzar.com/go/ixoff'', (''The index [?].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is disabled.  This index is not actually helping performance and should either be enabled or removed.'') from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE i.is_disabled = 1';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT DISTINCT 48, 100, ''Performance'', ''Foreign Keys Not Trusted'', ''http://BrentOzar.com/go/trust'', (''The [?] database has foreign keys that were probably disabled, data was changed, and then the key was enabled again.  Simply enabling the key is not enough for the optimizer to use this key - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'') from [?].sys.foreign_keys i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0';

            EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults SELECT 56, 100, ''Performance'', ''Check Constraint Not Trusted'', ''http://BrentOzar.com/go/trust'', (''The check constraint [?].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is not trusted - meaning, it was disabled, data was changed, and then the constraint was enabled again.  Simply enabling the constraint is not enough for the optimizer to use this constraint - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'') from [?].sys.check_constraints i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0';

            IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
                AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%' 
                BEGIN
                    EXEC dbo.sp_MSforeachdb 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details) SELECT TOP 1 13 AS CheckID, 110 AS Priority, ''Performance'' AS FindingsGroup, ''Plan Guides Enabled'' AS Finding, ''http://BrentOzar.com/go/guides'' AS URL, (''Database [?] has query plan guides so a query will always get a specific execution plan. If you are having trouble getting query performance to improve, it might be due to a frozen plan. Review the DMV sys.plan_guides to learn more about the plan guides in place on this server.'') AS Details FROM [?].sys.plan_guides WHERE is_disabled = 0'
                END;

		    EXEC sp_msforeachdb 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		SELECT  DISTINCT 60 AS CheckID, 
		        100 AS Priority, 
		        ''Performance'' AS FindingsGroup, 
		        ''Fill Factor Changed'', 
		        ''http://brentozar.com/go/fillfactor'' AS URL,
		        ''The ? database has objects with fill factor <> 0. This can cause memory and storage performance problems, but may also prevent page splits.''
		FROM    [?].sys.indexes 
		WHERE   fill_factor <> 0 AND fill_factor <> 100 AND is_disabled = 0 AND is_hypothetical = 0';



        END /* IF @CheckUserDatabaseObjects = 1 */




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
              'From Brent Ozar PLF, LLC' ,
              'http://www.BrentOzar.com/blitz/' ,
              'Thanks from the Brent Ozar PLF, LLC team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
            );

    SELECT  [Priority] ,
            [FindingsGroup] ,
            [Finding] ,
            [URL] ,
            [Details] ,
            CheckID
    FROM    #BlitzResults
    ORDER BY Priority ,
            FindingsGroup ,
            Finding ,
            Details;
  
  
    DROP TABLE #BlitzResults;
    SET NOCOUNT OFF;
GO
