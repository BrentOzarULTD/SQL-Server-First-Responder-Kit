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

--#STEP: sp_DatabaseRestore multiple log directories and empty-directory rejection
/* Real full/log backups; names and files are owned by this disposable CI test. */
IF DB_ID(N'FRKMultiLogSource') IS NOT NULL OR DB_ID(N'FRKMultiLogRestored') IS NOT NULL
    THROW 51000, 'Restore fixture already exists.', 1;
DECLARE @Root nvarchar(512) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath')),
        @Separator nchar(1), @Full nvarchar(512), @LogA nvarchar(512), @LogB nvarchar(512),
        @Empty nvarchar(512), @Missing nvarchar(512), @File nvarchar(512), @Paths nvarchar(max);
SET @Separator = CASE WHEN CHARINDEX(N'/', @Root) > 0 THEN N'/' ELSE N'\' END;
IF RIGHT(@Root, 1) <> @Separator SET @Root += @Separator;
SET @Root += N'FRKMultiLog_' + REPLACE(CONVERT(nvarchar(36), NEWID()), N'-', N'');
EXEC master.dbo.xp_create_subdir @Root;
SET @Full = @Root + @Separator + N'full' + @Separator;
SET @LogA = @Root + @Separator + N'logA' + @Separator;
SET @LogB = @Root + @Separator + N'logB' + @Separator;
SET @Empty = @Root + @Separator + N'logEmpty' + @Separator;
SET @Missing = @Root + @Separator + N'logMissing' + @Separator;
EXEC master.dbo.xp_create_subdir @Full;
EXEC master.dbo.xp_create_subdir @LogA;
EXEC master.dbo.xp_create_subdir @LogB;
EXEC master.dbo.xp_create_subdir @Empty;
BEGIN TRY
    EXEC(N'CREATE DATABASE FRKMultiLogSource;');
    ALTER DATABASE FRKMultiLogSource SET RECOVERY FULL;
    EXEC FRKMultiLogSource.sys.sp_executesql N'CREATE TABLE dbo.Proof(Id int PRIMARY KEY); INSERT dbo.Proof VALUES(1);';
    SET @File = @Full + N'FRKMultiLogSource_FULL_20260101_000000.bak';
    BACKUP DATABASE FRKMultiLogSource TO DISK = @File WITH INIT;
    INSERT FRKMultiLogSource.dbo.Proof VALUES(2);
    SET @File = @LogA + N'FRKMultiLogSource_LOG_20260101_000100.trn';
    BACKUP LOG FRKMultiLogSource TO DISK = @File WITH INIT;
    INSERT FRKMultiLogSource.dbo.Proof VALUES(3);
    SET @File = @LogB + N'FRKMultiLogSource_LOG_20260101_000200.trn';
    BACKUP LOG FRKMultiLogSource TO DISK = @File WITH INIT;

    EXEC dbo.sp_DatabaseRestore @Database=N'FRKMultiLogSource', @RestoreDatabaseName=N'FRKMultiLogRestored',
        @BackupPathFull=@Full, @BackupPathLog=@LogA, @SimpleFolderEnumeration=1, @RunRecovery=1;
    IF (SELECT COUNT(*) FROM FRKMultiLogRestored.dbo.Proof) <> 2
        THROW 51000, 'Single directory did not restore exactly the first log.', 1;
    DROP DATABASE FRKMultiLogRestored;
    SET @Paths = @LogA + N',' + @LogB;
    EXEC dbo.sp_DatabaseRestore @Database=N'FRKMultiLogSource', @RestoreDatabaseName=N'FRKMultiLogRestored',
        @BackupPathFull=@Full, @BackupPathLog=@Paths, @SimpleFolderEnumeration=1, @RunRecovery=1;
    IF (SELECT COUNT(*) FROM FRKMultiLogRestored.dbo.Proof) <> 3
        THROW 51000, 'Multiple directories did not restore both logs.', 1;
    DROP DATABASE FRKMultiLogRestored;

    DECLARE @BadPath nvarchar(512) = @Empty, @Rejected bit;
    WHILE @BadPath IS NOT NULL
    BEGIN
        SET @Rejected = 0;
        SET @Paths = @LogA + N',' + @BadPath;
        BEGIN TRY
            EXEC dbo.sp_DatabaseRestore @Database=N'FRKMultiLogSource', @RestoreDatabaseName=N'FRKMultiLogRestored',
                @BackupPathFull=@Full, @BackupPathLog=@Paths, @SimpleFolderEnumeration=1, @RunRecovery=1;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() <> 50000 OR ERROR_MESSAGE() NOT LIKE N'(LOG) No files were returned%'
                OR CHARINDEX(@BadPath, ERROR_MESSAGE()) = 0 THROW;
            SET @Rejected = 1;
        END CATCH;
        IF @Rejected = 0 THROW 51000, 'An empty or missing later log directory was silently accepted.', 1;
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name=N'FRKMultiLogRestored' AND state_desc<>N'RESTORING')
            THROW 51000, 'The incomplete restore was recovered.', 1;
        IF DB_ID(N'FRKMultiLogRestored') IS NOT NULL DROP DATABASE FRKMultiLogRestored;
        SET @BadPath = CASE WHEN @BadPath = @Empty THEN @Missing ELSE NULL END;
    END;
    INSERT FRKMultiLogSource.dbo.Proof VALUES(4);
    DECLARE @Stripe2 nvarchar(512) = @LogB + N'FRKMultiLogSource_LOG_20260101_000300_2.trn';
    SET @File = @LogA + N'FRKMultiLogSource_LOG_20260101_000300_1.trn';
    BACKUP LOG FRKMultiLogSource TO DISK = @File, DISK = @Stripe2 WITH INIT;
    SET @Paths = @LogA + N',' + @LogB;
    EXEC dbo.sp_DatabaseRestore @Database=N'FRKMultiLogSource', @RestoreDatabaseName=N'FRKMultiLogRestored',
        @BackupPathFull=@Full, @BackupPathLog=@Paths, @SimpleFolderEnumeration=1, @RunRecovery=1;
    IF (SELECT COUNT(*) FROM FRKMultiLogRestored.dbo.Proof) <> 4
        THROW 51000, 'The log striped across two directories was not restored.', 1;
    DROP DATABASE FRKMultiLogRestored;

    DROP DATABASE FRKMultiLogSource;
    /* xp_delete_file removes only backup files in these unique owned directories. */
    EXEC master.dbo.xp_delete_file 0, @Full, N'bak';
    EXEC master.dbo.xp_delete_file 0, @LogA, N'trn';
    EXEC master.dbo.xp_delete_file 0, @LogB, N'trn';
END TRY
BEGIN CATCH
    IF DB_ID(N'FRKMultiLogRestored') IS NOT NULL DROP DATABASE FRKMultiLogRestored;
    IF DB_ID(N'FRKMultiLogSource') IS NOT NULL DROP DATABASE FRKMultiLogSource;
    EXEC master.dbo.xp_delete_file 0, @Full, N'bak';
    EXEC master.dbo.xp_delete_file 0, @LogA, N'trn';
    EXEC master.dbo.xp_delete_file 0, @LogB, N'trn';
    THROW;
END CATCH;
PRINT 'PASS: single/multiple log directories and empty/missing later paths';

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
