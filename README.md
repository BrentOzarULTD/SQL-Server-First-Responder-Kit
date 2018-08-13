# SQL Server First Responder Kit
<a name="header1"></a>
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]

Navigation
 - [How to Get Support](#how-to-get-support)
 - Common Scripts:
   - [sp_Blitz: Overall Health Check](#sp_blitz-overall-health-check)
     - [Advanced sp_Blitz Parameters](#advanced-sp_blitz-parameters)
       - [Writing sp_Blitz Output to a Table](#writing-sp_blitz-output-to-a-table)
       - [Skipping Checks or Databases](#skipping-checks-or-databases)
   - [sp_BlitzCache: Find the Most Resource-Intensive Queries](#sp_blitzcache-find-the-most-resource-intensive-queries)
     - [Advanced sp_BlitzCache Parameters](#advanced-sp_blitzcache-parameters)
   - [sp_BlitzFirst: Real-Time Performance Advice](#sp_blitzfirst-real-time-performance-advice)
   - [sp_BlitzIndex: Tune Your Indexes](#sp_blitzindex-tune-your-indexes)
     - [Advanced sp_BlitzIndex Parameters](#advanced-sp_blitzindex-parameters)
 - Performance Tuning:   
   - [sp_BlitzInMemoryOLTP: Hekaton Analysis](#sp_blitzinmemoryoltp-hekaton-analysis) 
   - [sp_BlitzLock: Deadlock Analysis](#sp_blitzlock-deadlock-analysis) 
   - [sp_BlitzQueryStore: Like BlitzCache, for Query Store](#sp_blitzquerystore-query-store-sale)
   - [sp_BlitzWho: What Queries are Running Now](#sp_blitzwho-what-queries-are-running-now)
 - Backups and Restores:
   - [sp_BlitzBackups: How Much Data Could You Lose](#sp_blitzbackups-how-much-data-could-you-lose)  
   - [sp_AllNightLog: Back Up Faster to Lose Less Data](#sp_allnightlog-back-up-faster-to-lose-less-data)  
   - [sp_DatabaseRestore: Easier Multi-File Restores](#sp_databaserestore-easier-multi-file-restores)  
 - [Parameters Common to Many of the Stored Procedures](#parameters-common-to-many-of-the-stored-procedures)
 - [Power BI Dashboard for DBAs](#power-bi-dashboard-for-dbas)
 - [License MIT](#license)

You're a DBA, sysadmin, or developer who manages Microsoft SQL Servers. It's your fault if they're down or slow. These tools help you understand what's going on in your server.

* When you want an overall health check, run [sp_Blitz](#sp_blitz-overall-health-check).
* To learn which queries have been using the most resources, run [sp_BlitzCache](#sp_blitzcache-find-the-most-resource-intensive-queries).
* To analyze which indexes are missing or slowing you down, run [sp_BlitzIndex](#sp_blitzindex-tune-your-indexes).
* To find out why the server is slow right now, run [sp_BlitzFirst](#sp_blitzfirst-real-time-performance-advice).

To install, [download the latest release ZIP](https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/releases), then run the SQL files in the master database. (You can use other databases if you prefer.)

The First Responder Kit runs on:

* SQL Server 2008, 2008R2, 2012, 2014, 2016, 2017 on Windows - yes, fully supported
* SQL Server 2017 on Linux - yes, fully supported except sp_AllNightLog and sp_DatabaseRestore, which require xp_cmdshell, which Microsoft doesn't provide on Linux
* SQL Server 2000, 2005 - not supported by Microsoft anymore, so we don't either
* Amazon RDS SQL Server - fully supported
* Azure SQL DB - It's a dice roll. Microsoft changes DMV contents in here without warning, so no guarantees.


## How to Get Support
Everyone here is expected to abide by the [Contributor Covenant Code of Conduct](CONTRIBUTING.md#the-contributor-covenant-code-of-conduct).

When you have questions about how the tools work, talk with the community in the [#FirstResponderKit Slack channel](https://sqlcommunity.slack.com/messages/firstresponderkit/). If you need a free invite, hit [SQLslack.com](http://SQLslack.com/). Be patient - it's staffed with volunteers who have day jobs, heh.

When you find a bug or want something changed, [read the contributing.md file](CONTRIBUTING.md).

When you have a question about what the scripts found, first make sure you read the "More Details" URL for any warning you find. We put a lot of work into documentation, and we wouldn't want someone to yell at you to go read the fine manual. After that, when you've still got questions about how something works in SQL Server, post a question at [DBA.StackExchange.com](http://dba.stackexchange.com) and the community (that includes us!) will help. Include exact errors and any applicable screenshots, your SQL Server version number (including the build #), and the version of the tool you're working with.

[*Back to top*](#header1)


## sp_Blitz: Overall Health Check
Run sp_Blitz daily or weekly for an overall health check. Just run it from SQL Server Management Studio, and you'll get a prioritized list of issues on your server right now:

![sp_Blitz](http://u.brentozar.com/github-images/sp_Blitz.png)

Output columns include:

* Priority - 1 is the most urgent, stuff that could get you fired. The warnings get progressively less urgent.
* FindingsGroup, Findings - describe the problem sp_Blitz found on the server.
* DatabaseName - the database having the problem. If it's null, it's a server-wide problem.
* URL - copy/paste this into a browser for more information.
* Details - not just bland text, but dynamically generated stuff with more info.

Commonly used parameters:

* @CheckUserDatabaseObjects = 0 - by default, we check inside user databases for things like triggers or heaps. Turn this off (0) to make checks go faster, or ignore stuff you can't fix if you're managing third party databases. If a server has 50+ databases, @CheckUserDatabaseObjects is automatically turned off unless...
* @BringThePain = 1 - required if you want to run @CheckUserDatabaseObjects = 1 with over 50 databases. It's gonna be slow.
* @CheckServerInfo = 1 - includes additional rows at priority 250 with server configuration details like service accounts. 
* @IgnorePrioritiesAbove = 50 - if you want a daily bulletin of the most important warnings, set @IgnorePrioritiesAbove = 50 to only get the urgent stuff.

[*Back to top*](#header1)

### Advanced sp_Blitz Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_Blitz:

[*Back to top*](#header1)

#### Writing sp_Blitz Output to a Table

```SQL
sp_Blitz @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzResults';
```

Checks for the existence of a table DBAtools.dbo.BlitzResults, creates it if necessary, then adds the output of sp_Blitz into this table. This table is designed to support multiple outputs from multiple servers, so you can track your server's configuration history over time.

[*Back to top*](#header1)

#### Skipping Checks or Databases

```SQL
CREATE TABLE dbo.BlitzChecksToSkip (
ServerName NVARCHAR(128),
DatabaseName NVARCHAR(128),
CheckID INT
);
GO
INSERT INTO dbo.BlitzChecksToSkip (ServerName, DatabaseName, CheckID)
VALUES (NULL, 'SalesDB', 50)
sp_Blitz @SkipChecksDatabase = 'DBAtools', @SkipChecksSchema = 'dbo', @SkipChecksTable = 'BlitzChecksToSkip'
```

Checks for the existence of a table named Fred - just kidding, named DBAtools.dbo.BlitzChecksToSkip. The table needs at least the columns shown above (ServerName, DatabaseName, and CheckID). For each row:

* If the DatabaseName is populated but CheckID is null, then all checks will be skipped for that database
* If both DatabaseName and CheckID are populated, then that check will be skipped for that database
* If CheckID is populated but DatabaseName is null, then that check will be skipped for all databases

[*Back to top*](#header1)


## sp_BlitzCache: Find the Most Resource-Intensive Queries

sp_BlitzCache looks at your plan cache where SQL Server keeps track of which queries have run recently, and how much impact they've had on the server.

By default, it includes two result sets:

* The first result set shows your 10 most resource-intensive queries.
* The second result set explains the contents of the Warnings column - but it only shows the warnings that were produced in the first result set. (It's kinda like the most relevant glossary of execution plan terms.)

Output columns include:

* Database - the database context where the query ran. Keep in mind that if you fully qualify your object names, the same query might be run from multiple databases.
* Cost - the Estimated Subtree Cost of the query, what Kendra Little calls "Query Bucks."
* Query Text - don't copy/paste from here - it's only a quick reference. A better source for the query will show up later on.
* Warnings - problems we found.
* Created At - when the plan showed up in the cache.
* Last Execution - maybe the query only runs at night.
* Query Plan - click on this, and the graphical plan pops up.

### Common sp_BlitzCache Parameters

The @SortOrder parameter lets you pick which top 10 queries you want to examine:

* reads - logical reads
* CPU - from total_worker_time in sys.dm_exec_query_stats
* executions - how many times the query ran since the CreationDate
* xpm - executions per minute, derived from the CreationDate and LastExecution
* recent compilations - if you're looking for things that are recompiling a lot
* memory grant - if you're troubleshooting a RESOURCE_SEMAPHORE issue and want to find queries getting a lot of memory
* writes - if you wanna find those pesky ETL processes
* You can also use average or avg for a lot of the sorts, like @SortOrder = 'avg reads'
* all - sorts by all the different sort order options, and returns a single result set of hot messes. (Note that @SortOrder = 'all' is incompatible with the @OutputTableName parameter because it produces a different result set shape.)

Other common parameters include:

* @Top = 10 - by default, you get 10 plans, but you can ask for more. Just know that the more you get, the slower it goes.
* @ExportToExcel = 1 - turn this on, and it doesn't return XML fields that would hinder you from copy/pasting the data into Excel.
* @ExpertMode = 1 - turn this on, and you get more columns with more data. Doesn't take longer to run though.
* @IgnoreSystemDBs = 0 - if you want to show queries in master/model/msdb. By default we hide these.
* @MinimumExecutionCount = 0 - in servers like data warehouses where lots of queries only run a few times, you can set a floor number for examination.

[*Back to top*](#header1)

### Advanced sp_BlitzCache Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_BlitzCache:

* OnlyQueryHashes - if you want to examine specific query plans, you can pass in a comma-separated list of them in a string.
* IgnoreQueryHashes - if you know some queries suck and you don't want to see them, you can pass in a comma-separated list of them.
* OnlySqlHandles, @IgnoreSqlHandles - just like the above two params
* @DatabaseName - if you only want to analyze plans in a single database. However, keep in mind that this is only the database context. A single query that runs in Database1 can join across objects in Database2 and Database3, but we can only know that it ran in Database1.

[*Back to top*](#header1)

## sp_BlitzFirst: Real-Time Performance Advice

When performance emergencies strike, this should be the first stored proc in the kit you run.

It takes a sample from a bunch of DMVs (wait stats, Perfmon counters, plan cache), waits 5 seconds, and then takes another sample. It examines the differences between the samples, and then gives you a prioritized list of things that might be causing performance issues right now. Examples include:

* Data or log file growing (or heaven forbid, shrinking)
* Backup or restore running
* DBCC operation happening

If no problems are found, it'll tell you that too. That's one of our favorite features because you can have your help desk team run sp_BlitzFirst and read the output to you over the phone. If no problems are found, you can keep right on drinking at the bar. (Ha! Just kidding, you'll still have to close out your tab, but at least you'll feel better about finishing that drink rather than trying to sober up.)

Common sp_BlitzFirst parameters include:

* @Seconds = 5 by default. You can specify longer samples if you want to track stats during a load test or demo, for example.
* @ShowSleepingSPIDs = 0 by default. When set to 1, shows long-running sleeping queries that might be blocking others.
* @ExpertMode = 0 by default. When set to 1, it calls sp_BlitzWho when it starts (to show you what queries are running right now), plus outputs additional result sets for wait stats, Perfmon counters, and file stats during the sample, then finishes with one final execution of sp_BlitzWho to show you what was running at the end of the sample.

### Logging sp_BlitzFirst to Tables

You can log sp_BlitzFirst performance data to tables and then analyze the results with the Power BI dashboard. To do it, schedule an Agent job to run sp_BlitzFirst every 15 minutes with these parameters populated:

* @OutputDatabaseName = typically 'DBAtools'
* @OutputSchemaName = 'dbo'
* @OutputTableName = 'BlitzFirst' - the quick diagnosis result set goes here
* @OutputTableNameFileStats = 'BlitzFirst_FileStats'
* @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats'
* @OutputTableNameWaitStats = 'BlitzFirst_WaitStats'
* @OutputTableNameBlitzCache = 'BlitzCache' 

All of the above OutputTableName parameters are optional: if you don't want to collect all of the stats, you don't have to. Keep in mind that the sp_BlitzCache results will get large, fast, because each execution plan is megabytes in size.

Then fire up the [First Responder Kit Power BI dashboard.](https://www.brentozar.com/first-aid/first-responder-kit-power-bi-dashboard/)

### Logging Performance Tuning Activities

On the Power BI Dashboard, you can show lines for your own activities like tuning queries, adding indexes, or changing configuration settings. To do it, run sp_BlitzFirst with these parameters:

* @OutputDatabaseName = typically 'DBAtools'
* @OutputSchemaName = 'dbo'
* @OutputTableName = 'BlitzFirst' - the quick diagnosis result set goes here
* @LogMessage = 'Whatever you wanna show in the Power BI dashboard'

Optionally, you can also pass in:

* @LogMessagePriority = 1
* @LogMessageFindingsGroup = 'Logged Message'
* @LogMessageFinding = 'Logged from sp_BlitzFirst' - you could use other values here to track other data sources like DDL triggers, Agent jobs, ETL jobs
* @LogMessageURL = 'https://OurHelpDeskSystem/ticket/?12345' - or maybe a Github issue, or Pagerduty alert
* @LogMessageCheckDate = '2017/10/31 11:00' - in case you need to log a message for a prior date/time, like if you forgot to log the message earlier

[*Back to top*](#header1)

## sp_BlitzIndex: Tune Your Indexes

SQL Server tracks your indexes: how big they are, how often they change, whether they're used to make queries go faster, and which indexes you should consider adding. The results columns are fairly self-explanatory.

By default, sp_BlitzIndex analyzes the indexes of the database you're in (your current context.)

Common parameters include:

* @DatabaseName - if you want to analyze a specific database
* @SchemaName, @TableName - if you pass in these, sp_BlitzIndex does a deeper-dive analysis of just one table. You get several result sets back describing more information about the table's current indexes, foreign key relationships, missing indexes, and fields in the table.
* @GetAllDatabases = 1 - slower, but lets you analyze all the databases at once, up to 50. If you want more than 50 databases, you also have to pass in @BringThePain = 1.
* @ThresholdMB = 250 - by default, we only analyze objects over 250MB because you're busy.
* @Mode = 0 (default) - get different data with 0=Diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail, 4=Diagnose Details.


[*Back to top*](#header1)

### Advanced sp_BlitzIndex Parameters

In addition to the [parameters common to many of the stored procedures](#parameters-common-to-many-of-the-stored-procedures), here are the ones specific to sp_BlitzIndex:

* @SkipPartitions = 1 - add this if you want to analyze large partitioned tables. We skip these by default for performance reasons.
* @SkipStatistics = 0 - right now, by default, we skip statistics analysis because we've had some performance issues on this.
* @Filter = 0 (default) - 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB


[*Back to top*](#header1)


## sp_BlitzInMemoryOLTP: Hekaton Analysis

Examines your usage of In-Memory OLTP tables. Parameters you can use:

* @instanceLevelOnly BIT: This flag determines whether or not to simply report on the server-level environment (if applicable, i.e. there is no server-level environment for Azure SQL Database). With this parameter, memory-optimized databases are ignored. If you specify @instanceLevelOnly and a database name, the database name is ignored.
* @dbName NVARCHAR(4000) = N'ALL' - If you don't specify a database name, then sp_BlitzInMemoryOLTP reports on all memory-optimized databases within the instance that it executes in, or in the case of Azure SQL Database, the database that you provisioned. This is because the default for the @dbName parameter is N'ALL'.
* @tableName NVARCHAR(4000) = NULL
* @debug BIT

To interpret the output of this stored procedure, read [Ned Otter's sp_BlitzInMemoryOLTP documentation](http://nedotter.com/archive/2018/06/new-kid-on-the-block-sp_blitzinmemoryoltp/).


[*Back to top*](#header1)



## sp_BlitzLock: Deadlock Analysis

Checks either the System Health session or a specific Extended Event session that captures deadlocks and parses out all the XML for you.

Parameters you can use:
* @Top: Use if you want to limit the number of deadlocks to return. This is ordered by event date ascending.
* @DatabaseName: If you want to filter to a specific database
* @StartDate: The date you want to start searching on.
* @EndDate: The date you want to stop searching on.
* @ObjectName: If you want to filter to a specific table. The object name has to be fully qualified 'Database.Schema.Table'
* @StoredProcName: If you want to search for a single stored proc.
* @AppName: If you want to filter to a specific application.
* @HostName: If you want to filter to a specific host.
* @LoginName: If you want to filter to a specific login.
* @EventSessionPath: If you want to point this at an XE session rather than the system health session.


[*Back to top*](#header1)


## sp_BlitzQueryStore: Query Store Sale

Analyzes data in Query Store schema (2016+ only) in many similar ways to what sp_BlitzCache does for the plan cache.

* @Help: Right now this just prints the license if set to 1. I'm going to add better documentation here as the script matures.
* @DatabaseName: This one is required. Query Store is per database, so you have to point it at one to examine.
* @Top: How many plans from each "worst" you want to get. We look at your maxes for CPU, reads, duration, writes, memory, rows, executions, and additionally tempdb and log bytes for 2017. So it's the number of plans from each of those to gather.
* @StartDate: Fairly obvious, when you want to start looking at queries from. If NULL, we'll only go back seven days.
* @EndDate: When you want to stop looking at queries from. If you leave it NULL, we'll look ahead seven days.
* @MinimumExecutionCount: The minimum number of times a query has to have been executed (not just compiled) to be analyzed.
* @DurationFilter: The minimum number of seconds a query has to have been executed for to be analyzed.
* @StoredProcName: If you want to look at a single stored procedure.
* @Failed: If you want to look at failed queries, for some reason. I dunno, MS made such a big deal out of being able to look at these, I figured I'd add it.
* @PlanIdFilter: If you want to filter by a particular plan id. Remember that a query may have many different plans.
* @QueryIdFilter: If you want to filter by a particular query id. If you want to look at one specific plan for a query.
* @ExportToExcel: Leaves XML out of the input and tidies up query text so you can easily paste it into Excel.
* @HideSummary: Pulls the rolled up warnings and information out of the results.
* @SkipXML: Skips XML analysis.
* @Debug: Prints dynamic SQL and selects data from all temp tables if set to 1.

[*Back to top*](#header1)


## sp_BlitzWho: What Queries are Running Now

This is like sp_who, except it goes into way, way, way more details.

It's designed for query tuners, so it includes things like memory grants, degrees of parallelism, and execution plans.

[*Back to top*](#header1)


## sp_BlitzBackups: How Much Data Could You Lose

Checks your backups and reports estimated RPO and RTO based on historical data in msdb, or a centralized location for [msdb].dbo.backupset.

Parameters include:

* @HoursBack -- How many hours into backup history you want to go. Should be a negative number (we're going back in time, after all). But if you enter a positive number, we'll make it negative for you. You're welcome.
* @MSDBName -- if you need to prefix dbo.backupset with an alternate database name. 
* @AGName -- If you have more than 1 AG on the server, and you don't know the listener name, specify the name of the AG you want to use the listener for, to push backup data. This may get used during analysis in a future release for filtering.
* @RestoreSpeedFullMBps --[FIXFIX] Brent can word this better than I can
* @RestoreSpeedDiffMBps -- Nothing yet
* @RestoreSpeedLogMBps -- Nothing yet

* @PushBackupHistoryToListener -- Turn this to 1 to skip analysis and use sp_BlitzBackups to push backup data from msdb to a centralized location (more the mechanics of this to follow)
* @WriteBackupsToListenerName -- This is the name of the AG listener, and **MUST** have a linked server configured pointing to it. Yes, that means you need to create a linked server that points to the AG Listener, with the appropriate permissions to write data.  
* @WriteBackupsToDatabaseName -- This can't be 'msdb' if you're going to use the backup data pushing mechanism. We can't write to your actual msdb tables.
* @WriteBackupsLastHours -- How many hours in the past you want to move data for. Should be a negative number (we're going back in time, after all). But if you enter a positive number, we'll make it negative for you. You're welcome.

An example run of sp_BlitzBackups to push data looks like this:

```
EXEC sp_BlitzBackups    @PushBackupHistoryToListener = 1, -- Turn it on!
                        @WriteBackupsToListenerName = 'AG_LISTENER_NAME', -- Name of AG Listener and Linked Server 
                        @WriteBackupsToDatabaseName = 'FAKE_MSDB_NAME',  -- Fake MSDB name you want to push to. Remember, can't be real MSDB.
                        @WriteBackupsLastHours = -24 -- Hours back in time you want to go
```

In an effort to not clog your servers up, we've taken some care in batching things as we move data. Inspired by [Michael J. Swart's Take Care When Scripting Batches](http://michaeljswart.com/2014/09/take-care-when-scripting-batches/), we only move data in 10 minute intervals.

The reason behind that is, if you have 500 databases, and you're taking log backups every minute, you can have a lot of data to move. A 5000 row batch should move pretty quickly.

[*Back to top*](#header1)


## sp_AllNightLog: Back Up Faster to Lose Less Data

You manage a SQL Server instance with hundreds or thousands of mission-critical databases. You want to back them all up as quickly as possible, and one maintenance plan job isn't going to cut it.

Let's scale out our backup jobs by:

* Creating a table with a list of databases and their desired Recovery Point Objective (RPO, aka data loss) - done with sp_AllNightLog_Setup
* Set up several Agent jobs to back up databases as necessary - also done with sp_AllNightLog_Setup
* Inside each of those Agent jobs, they call sp_AllNightLog @Backup = 1, which loops through the table to find databases that need to be backed up, then call [Ola Hallengren's DatabaseBackup stored procedure](https://ola.hallengren.com/)
* Keeping that database list up to date as new databases are added - done by a job calling sp_AllNightLog @PollForNewDatabases = 1

For more information about how this works, see [sp_AllNightLog documentation.](https://www.BrentOzar.com/sp_AllNightLog)

Known issues:

* The msdbCentral database name is hard-coded.
* sp_AllNightLog depends on Ola Hallengren's DatabaseBackup, which must be installed separately. (We're not checking for it right now.)


[*Back to top*](#header1)


## sp_DatabaseRestore: Easier Multi-File Restores

If you use [Ola Hallengren's backup scripts](http://ola.hallengren.com), DatabaseRestore.sql helps you rapidly restore a database to the most recent point in time.

Parameters include:

* @Database - the database's name, like LogShipMe
* @RestoreDatabaseName
* @BackupPathFull - typically a UNC path like '\\FILESERVER\BACKUPS\SQL2016PROD1A\LogShipMe\FULL\' that points to where the full backups are stored. Note that if the path doesn't exist, we don't create it, and the query might take 30+ seconds if you specify an invalid server name.
* @BackupPathDiff, @BackupPathLog - as with the Full, this should be set to the exact path where the differentials and logs are stored. We don't append anything to these parameters.
* @MoveFiles, @MoveDataDrive, @MoveLogDrive - if you want to restore to somewhere other than your default database locations.
* @RunCheckDB - default 0. When set to 1, we run Ola Hallengren's DatabaseIntegrityCheck stored procedure on this database, and log the results to table. We use that stored proc's default parameters, nothing fancy.
* @TestRestore - default 0. When set to 1, we delete the database after the restore completes. Used for just testing your restores. Especially useful in combination with @RunCheckDB = 1 because we'll delete the database after running checkdb, but know that we delete the database even if it fails checkdb tests.
* @RestoreDiff - default 0. When set to 1, we restore the ncessary full, differential, and log backups (instead of just full and log) to get to the most recent point in time.
* @ContinueLogs - default 0. When set to 1, we don't restore a full or differential backup - we only restore the transaction log backups. Good for continuous log restores with tools like sp_AllNightLog.
* @RunRecovery - default 0. When set to 1, we run RESTORE WITH RECOVERY, putting the database into writable mode, and no additional log backups can be restored.
* @Debug - default 0. When 1, we print out messages of what we're doing in the messages tab of SSMS.
* @StopAt NVARCHAR(14) - pass in a date time to stop your restores at a time like '20170508201501'.


For information about how this works, see [Tara Kizer's white paper on Log Shipping 2.0 with Google Compute Engine.](https://BrentOzar.com/go/gce)

[*Back to top*](#header1)



## Parameters Common to Many of the Stored Procedures

* @Help = 1 - returns a result set or prints messages explaining the stored procedure's input and output. Make sure to check the Messages tab in SSMS to read it.
* @ExpertMode = 1 - turns on more details useful for digging deeper into results.
* @OutputDatabaseName, @OutputSchemaName, @OutputTableName - pass all three of these in, and the stored proc's output will be written to a table. We'll create the table if it doesn't already exist.
* @OutputServerName - not functional yet. To track (or help!) implementation status: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/293

[*Back to top*](#header1)

## Power BI Dashboard for DBAs

[Documentation for this part of the project is currently at BrentOzar.com.](https://www.brentozar.com/first-aid/first-responder-kit-power-bi-dashboard/)

To contribute changes, read the [contributing guide](CONTRIBUTING.md) - there's a special section for the Power BI Dashboard since it has to be changed differently than scripts.

[*Back to top*](#header1)



## License

[The SQL Server First Responder Kit uses the MIT License.](LICENSE.md)

[*Back to top*](#header1)

[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg
[forks badge]:https://img.shields.io/github/forks/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg
[issues badge]:https://img.shields.io/github/issues/BrentOzarULTD/SQL-Server-First-Responder-Kit.svg

[licence]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/blob/master/LICENSE.md
[stars]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/stargazers
[forks]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/network
[issues]:https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues
