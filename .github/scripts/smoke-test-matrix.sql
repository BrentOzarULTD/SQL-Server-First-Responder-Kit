/*
The commands CI runs against every non-deprecated script in the kit.

Seeded from Documentation/Development/Test in Azure.sql and extended to cover
the scripts that file does not reach. Kept separate from that file so CI can add
setup and teardown that a hand-run script should not carry (issue #4046,
decision 2B) -- if you add a parameter combination to one, consider the other.

FORMAT: every step starts with a line reading

    --#STEP: <label>

run-sql-server-smoke-tests.sh splits the file on those markers and runs each
step as its own sqlcmd batch, so a failure is attributed to one labelled step
and the remaining steps still run. That means each step must stand alone --
variables do not carry across steps.

Deliberately excluded:
  * sp_BlitzUpdate -- it replaces the kit's procedures mid-run, so every step
    after it would be exercising whatever it just downloaded rather than the
    code under test (issue #4046, decision 4).
*/

--#STEP: sp_Blitz default
EXEC dbo.sp_Blitz
     @SkipChecksDatabase = 'FRKSmokeTest',
     @SkipChecksSchema   = 'dbo',
     @SkipChecksTable    = 'BlitzChecksToSkip';

--#STEP: sp_Blitz full check
EXEC dbo.sp_Blitz
     @CheckUserDatabaseObjects = 1,
     @CheckServerInfo          = 1,
     @SkipChecksDatabase = 'FRKSmokeTest',
     @SkipChecksSchema   = 'dbo',
     @SkipChecksTable    = 'BlitzChecksToSkip';

--#STEP: sp_Blitz markdown output
EXEC dbo.sp_Blitz
     @OutputType = 'MARKDOWN',
     @SkipChecksDatabase = 'FRKSmokeTest',
     @SkipChecksSchema   = 'dbo',
     @SkipChecksTable    = 'BlitzChecksToSkip';

--#STEP: sp_Blitz count output
EXEC dbo.sp_Blitz
     @OutputType = 'COUNT',
     @SkipChecksDatabase = 'FRKSmokeTest',
     @SkipChecksSchema   = 'dbo',
     @SkipChecksTable    = 'BlitzChecksToSkip';

--#STEP: sp_Blitz to table
EXEC dbo.sp_Blitz
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @OutputTableName    = 'BlitzOutput',
     @SkipChecksDatabase = 'FRKSmokeTest',
     @SkipChecksSchema   = 'dbo',
     @SkipChecksTable    = 'BlitzChecksToSkip';

/*
Both @Check* flags together with table output and a skip list -- a combination
none of the other steps covers, and the one most likely to be run in anger.
*/
--#STEP: sp_Blitz full check to table with skip list
EXEC dbo.sp_Blitz
     @CheckUserDatabaseObjects = 1,
     @CheckServerInfo          = 1,
     @OutputDatabaseName       = 'FRKSmokeTest',
     @OutputSchemaName         = 'dbo',
     @OutputTableName          = 'BlitzFindings',
     @SkipChecksDatabase       = 'FRKSmokeTest',
     @SkipChecksSchema         = 'dbo',
     @SkipChecksTable          = 'BlitzChecksToSkip';

/* Reads the rows back out, so that renaming or dropping CheckID, DatabaseName
   or Finding fails this step. Those columns are what anything consuming a
   persisted sp_Blitz result set depends on, and writing the table proves only
   that it was created, not that it is still shaped the way callers expect. */
SELECT CONVERT(VARCHAR(10), CheckID)
       + '|' + ISNULL(DatabaseName, '(server)')
       + '|' + ISNULL(Finding, '')
FROM FRKSmokeTest.dbo.BlitzFindings
ORDER BY CheckID, DatabaseName, Finding;

--#STEP: sp_BlitzCache all sort orders
EXEC dbo.sp_BlitzCache @SortOrder = 'all';

--#STEP: sp_BlitzCache expert mode
EXEC dbo.sp_BlitzCache @ExpertMode = 1;

--#STEP: sp_BlitzCache to table
EXEC dbo.sp_BlitzCache
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @OutputTableName    = 'BlitzCache';

--#STEP: sp_BlitzCache filtered to database
EXEC dbo.sp_BlitzCache @DatabaseName = 'FRKSmokeTest';

--#STEP: sp_BlitzFirst 5 seconds expert mode
EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;

--#STEP: sp_BlitzFirst since startup
EXEC dbo.sp_BlitzFirst @SinceStartup = 1;

--#STEP: sp_BlitzFirst to tables
EXEC dbo.sp_BlitzFirst
     @OutputDatabaseName         = 'FRKSmokeTest',
     @OutputSchemaName           = 'dbo',
     @OutputTableName            = 'BlitzFirst',
     @OutputTableNameFileStats   = 'BlitzFirst_FileStats',
     @OutputTableNamePerfmonStats= 'BlitzFirst_PerfmonStats',
     @OutputTableNameWaitStats   = 'BlitzFirst_WaitStats',
     @OutputTableNameBlitzCache  = 'BlitzCache',
     @OutputTableNameBlitzWho    = 'BlitzWho';

--#STEP: sp_BlitzIndex mode 0
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @Mode = 0;

--#STEP: sp_BlitzIndex mode 1
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @Mode = 1;

--#STEP: sp_BlitzIndex mode 2
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @Mode = 2;

--#STEP: sp_BlitzIndex mode 3
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @Mode = 3;

--#STEP: sp_BlitzIndex mode 4
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @Mode = 4;

--#STEP: sp_BlitzIndex single table
EXEC dbo.sp_BlitzIndex @DatabaseName = 'FRKSmokeTest', @TableName = 'Users';

--#STEP: sp_BlitzIndex all databases
EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 0;

--#STEP: sp_BlitzIndex to table
EXEC dbo.sp_BlitzIndex
     @DatabaseName       = 'FRKSmokeTest',
     @Mode               = 0,
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @OutputTableName    = 'BlitzIndex';

--#STEP: sp_BlitzLock default
EXEC dbo.sp_BlitzLock;

--#STEP: sp_BlitzLock to table
EXEC dbo.sp_BlitzLock
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @OutputTableName    = 'BlitzLock';

--#STEP: sp_BlitzWho expert mode
EXEC dbo.sp_BlitzWho @ExpertMode = 1;

--#STEP: sp_BlitzWho normal mode
EXEC dbo.sp_BlitzWho @ExpertMode = 0;

--#STEP: sp_BlitzWho to table
EXEC dbo.sp_BlitzWho
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @OutputTableName    = 'BlitzWho_Results';

--#STEP: sp_BlitzBackups default
EXEC dbo.sp_BlitzBackups;

--#STEP: sp_BlitzBackups restore speeds
EXEC dbo.sp_BlitzBackups
     @HoursBack             = 168,
     @RestoreSpeedFullMBps  = 100,
     @RestoreSpeedDiffMBps  = 100,
     @RestoreSpeedLogMBps   = 100;

/* Depends on the sp_BlitzFirst logging step above having created its tables. */
--#STEP: sp_BlitzAnalysis default
EXEC dbo.sp_BlitzAnalysis @OutputDatabaseName = 'FRKSmokeTest', @OutputSchemaName = 'dbo';

--#STEP: sp_BlitzAnalysis filtered to database
EXEC dbo.sp_BlitzAnalysis
     @OutputDatabaseName = 'FRKSmokeTest',
     @OutputSchemaName   = 'dbo',
     @Databasename       = 'FRKSmokeTest';

--#STEP: sp_ineachdb simple command
EXEC dbo.sp_ineachdb @command = N'SELECT DB_NAME() AS CurrentDatabase;';

--#STEP: sp_ineachdb user databases only
EXEC dbo.sp_ineachdb @command = N'SELECT COUNT(*) AS TableCount FROM sys.tables;', @user_only = 1;

--#STEP: sp_ineachdb three part name rewrite
EXEC dbo.sp_ineachdb @command = N'SELECT TOP (1) name FROM [?].sys.tables;', @user_only = 1;

--#STEP: sp_kill report only
EXEC dbo.sp_kill @ExecuteKills = 'N';

--#STEP: sp_kill order by duration
EXEC dbo.sp_kill @ExecuteKills = 'N', @OrderBy = 'duration';

/*
NOT coverage of the kill loop, despite running with @ExecuteKills = 'Y'.

With a filter that matches no session, sp_kill sets @TotalKills to 0 and leaves
through its "nothing to kill" branch; the cursor and EXEC(@KillSQL) at
sp_kill.sql:793-840 are never reached. What this does cover is parameter
handling and the filter/report path under the executing flag, without CI killing
its own connection.

Real coverage needs a second session held open for sp_kill to kill, respawned
for every matrix run since the first one consumes it. Tracked in issue #4051.
Labelled for what it is rather than what it looks like -- a step whose name
implies coverage it does not provide is the same trap as the @VersionCheckMode
call this whole harness replaced.
*/
--#STEP: sp_kill executing flag with no matching session (not the kill loop)
EXEC dbo.sp_kill @ExecuteKills = 'Y', @AppName = 'NoSuchApp-FRKSmokeTest';

--#STEP: sp_DatabaseRestore help
EXEC dbo.sp_DatabaseRestore @Help = 1;

/*
sp_DatabaseRestore's execute path is NOT covered here, and deliberately so.

Its dependencies (Ola Hallengren's CommandLog + CommandExecute) are deliberately
NOT installed either -- see the re-enabling note below. They were, briefly, and
that is how the bug underneath was found: with both present the procedure runs
far enough to fail against a Linux fixture rather than stopping at the
missing-dependency check.

@MoveFiles defaults to 1, and that path handles paths with a hardcoded
backslash in two places: it splits the filename off PhysicalName with
CHARINDEX('\\', ...), which returns 0 on a forward-slash path and makes
LEFT(..., -1) raise Msg 537; and it joins the backup directory to the file name
with a backslash, producing '/var/opt/mssql/data/\\FRKSmokeTest_Full2.bak'.

Tracked in issue #4049. A permanently-failing step is worse than an absent one:
it trains everyone to expect red and it advertises coverage that does not exist.
So this stays at @Help until #4049 lands.

Re-enabling takes TWO changes, not one. Uncommenting the invocation below on its
own will fail immediately on sp_DatabaseRestore's CommandExecute prerequisite
check: the workflow no longer fetches Ola Hallengren's CommandLog and
CommandExecute, because with only @Help left nothing could reach them. Restore
the workflow's dependency step (its URL and both SHA-256 hashes are preserved in
a comment there) and this invocation together.

    EXEC dbo.sp_DatabaseRestore
         @Database            = 'FRKSmokeTest',
         @RestoreDatabaseName = 'FRKSmokeTestRestored',
         @BackupPathFull      = '/var/opt/mssql/data/',
         @RunRecovery         = 1,
         @ExistingDBAction    = 3;
*/

--#STEP: sp_BlitzPlanCompare help
EXEC dbo.sp_BlitzPlanCompare @Help = 1;

/*
Runs a uniquely marked query in FRKSmokeTest's own context, then finds that plan
by its marker AND by the plan's dbid. The dbid filter matters: this outer batch
runs in master and its text also contains the marker, so without it the lookup
can pick up the master-context plan and sp_BlitzPlanCompare's @DatabaseName
filter then fails to match it.

If the plan cannot be found the step fails loudly rather than skipping quietly --
a comparison that silently never runs is worse than no step at all.
*/
--#STEP: sp_BlitzPlanCompare against a cached plan
EXEC FRKSmokeTest.sys.sp_executesql
     N'SELECT /* FRKPlanCompareMarker */ COUNT_BIG(*) AS MarkedCount
       FROM dbo.Users AS u
       JOIN dbo.Posts AS p ON p.OwnerUserId = u.Id
       WHERE u.Reputation > 10;';

DECLARE @QueryPlanHash BINARY(8);

SELECT TOP (1) @QueryPlanHash = qs.query_plan_hash
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
WHERE st.text LIKE N'%FRKPlanCompareMarker%'
  AND pa.attribute = 'dbid'
  AND CONVERT(INT, pa.value) = DB_ID('FRKSmokeTest')
ORDER BY qs.creation_time DESC;

IF @QueryPlanHash IS NULL
    RAISERROR('Seeded marker query was not found in the plan cache; sp_BlitzPlanCompare was not exercised.', 16, 1);

EXEC dbo.sp_BlitzPlanCompare @QueryPlanHash = @QueryPlanHash, @DatabaseName = 'FRKSmokeTest';
