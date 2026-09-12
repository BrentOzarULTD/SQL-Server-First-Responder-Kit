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

--#STEP: sp_Blitz restricted login honors skip guards
/* Grants happen after the runner installs the procedures. */
GRANT EXECUTE ON dbo.sp_Blitz TO FRKSmokeLimited;
GRANT EXECUTE ON dbo.sp_ineachdb TO FRKSmokeLimited;
EXECUTE AS LOGIN = 'FRKSmokeLimited';
BEGIN TRY
    IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 1) <> 0
       OR ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE'), 0) <> 1
        THROW 51000, 'Restricted-login test has the wrong server permissions.', 1;

    /* Prove the probes fail without guards: empty metadata is not a denial. */
    DECLARE @Probes TABLE(CheckID int PRIMARY KEY, Query nvarchar(max));
    INSERT @Probes VALUES
      (202,N'SELECT TOP (0) * FROM msdb.INFORMATION_SCHEMA.COLUMNS;'),
      (178,N'SELECT TOP (0) * FROM msdb.dbo.backupset;'),
      (105,N'SELECT TOP (0) * FROM master.sys.extended_procedures;'),
      (116,N'SELECT TOP (0) * FROM msdb.sys.all_columns;'),
      (191,N'SELECT TOP (0) * FROM sys.master_files;');
    DECLARE @ProbeID int, @Probe nvarchar(max);
    WHILE EXISTS (SELECT 1 FROM @Probes)
    BEGIN
        SELECT TOP (1) @ProbeID = CheckID, @Probe = Query FROM @Probes ORDER BY CheckID;
        BEGIN TRY
            EXEC sys.sp_executesql @Probe;
            THROW 51000, 'A restricted metadata probe unexpectedly succeeded.', 1;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() NOT IN (229, 916) THROW;
        END CATCH;
        DELETE @Probes WHERE CheckID = @ProbeID;
    END;

    EXEC dbo.sp_Blitz
         @CheckUserDatabaseObjects = 0,
         @CheckProcedureCache = 0,
         @CheckServerInfo = 1,
         @SkipChecksDatabase = 'FRKSmokeTest',
         @SkipChecksSchema = 'dbo',
         @SkipChecksTable = 'LimitedLoginChecksToSkip';
    REVERT;
END TRY
BEGIN CATCH
    REVERT;
    THROW;
END CATCH;

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

--#STEP: sp_kill executing flag with no matching session
EXEC dbo.sp_kill @ExecuteKills = 'Y', @AppName = 'NoSuchApp-FRKSmokeTest';

--#STEP: sp_kill kills a dedicated session
DECLARE @Token uniqueidentifier = '$(KillVictimToken)', @VictimSpid int,
        @LoginTime datetime, @Attempts int = 0;
/* Wait for registration, checking session identity as well as the reusable SPID. */
WHILE @VictimSpid IS NULL AND @Attempts < 30
BEGIN
    SELECT @VictimSpid = v.SessionId, @LoginTime = v.LoginTime
    FROM FRKSmokeTest.dbo.KillVictim AS v
    JOIN sys.dm_exec_sessions AS s
      ON s.session_id = v.SessionId AND s.login_time = v.LoginTime
    WHERE v.Token = @Token
      AND s.is_user_process = 1 AND s.session_id <> @@SPID;
    IF @VictimSpid IS NULL WAITFOR DELAY '00:00:01';
    SET @Attempts += 1;
END;
IF @VictimSpid IS NULL
    THROW 51000, 'The dedicated sp_kill victim did not become ready.', 1;

EXEC dbo.sp_kill @ExecuteKills = 'Y', @SPID = @VictimSpid;

SET @Attempts = 0;
WHILE EXISTS (SELECT 1 FROM sys.dm_exec_sessions
              WHERE session_id = @VictimSpid AND login_time = @LoginTime)
      AND @Attempts < 10
BEGIN
    WAITFOR DELAY '00:00:01';
    SET @Attempts += 1;
END;
IF EXISTS (SELECT 1 FROM sys.dm_exec_sessions
           WHERE session_id = @VictimSpid AND login_time = @LoginTime)
    THROW 51000, 'sp_kill returned without terminating the dedicated victim.', 1;

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

--#STEP: sp_BlitzCache isolates analysis and all Excel export paths
EXEC FRKSmokeTest.sys.sp_executesql
     N'SELECT COUNT_BIG(*) FROM dbo.Posts WHERE Id > 10 /* FRK isolation workload */';
EXEC dbo.sp_BlitzCache @Top=100, @IgnoreSystemDBs=0, @SkipAnalysis=1, @HideSummary=1;
IF NOT EXISTS(SELECT 1 FROM ##BlitzCacheProcs WHERE SPID=@@SPID AND QueryHash IS NOT NULL)
    THROW 51000,'Isolation fixture has no cached statement.',1;
DELETE ##BlitzCacheProcs WHERE SPID=-9876;
INSERT ##BlitzCacheProcs(SPID,DatabaseName,QueryText,SqlHandle,QueryHash,QueryType,QueryPlanCost)
SELECT -9876,DatabaseName,N'  SELECT   123 /* FRK other session */  ',SqlHandle,QueryHash,N'Statement',-987
FROM ##BlitzCacheProcs WHERE SPID=@@SPID AND QueryHash IS NOT NULL;
DECLARE @OtherCount int=(SELECT COUNT(*) FROM ##BlitzCacheProcs WHERE SPID=-9876);
BEGIN TRY
    EXEC dbo.sp_BlitzCache @Top=100, @IgnoreSystemDBs=0, @HideSummary=1;
    IF NOT EXISTS(SELECT 1 FROM ##BlitzCacheProcs a JOIN ##BlitzCacheProcs b
                  ON a.SqlHandle=b.SqlHandle AND a.QueryHash=b.QueryHash
                  WHERE a.SPID=@@SPID AND b.SPID=-9876 AND a.QueryPlanCost>=0)
        THROW 51000,'Analysis did not exercise the shared plan handles.',1;
    IF EXISTS(SELECT 1 FROM ##BlitzCacheProcs WHERE SPID=-9876 AND QueryPlanCost<>-987)
        THROW 51000,'Analysis changed another session.',1;
    DECLARE @Modes TABLE(SortOrder varchar(20));
    INSERT @Modes VALUES('cpu'),('all'),('all avg');
    DECLARE @Sort varchar(20);
    WHILE EXISTS(SELECT 1 FROM @Modes)
    BEGIN
        SELECT TOP(1) @Sort=SortOrder FROM @Modes;
        EXEC dbo.sp_BlitzCache @Top=1, @DatabaseName=N'FRKSmokeTest',
             @HideSummary=1, @ExportToExcel=1, @SortOrder=@Sort;
        IF (SELECT COUNT(*) FROM ##BlitzCacheProcs WHERE SPID=-9876)<>@OtherCount
           OR EXISTS(SELECT 1 FROM ##BlitzCacheProcs WHERE SPID=-9876
                     AND (QueryText<>N'  SELECT   123 /* FRK other session */  ' OR QueryPlanCost<>-987))
            THROW 51000,'Excel export changed another session.',1;
        DELETE @Modes WHERE SortOrder=@Sort;
    END;
    DELETE ##BlitzCacheProcs WHERE SPID=-9876;
END TRY
BEGIN CATCH
    DELETE ##BlitzCacheProcs WHERE SPID=-9876;
    THROW;
END CATCH;
PRINT 'Analysis, direct export, all, and all avg preserved the other session.';
