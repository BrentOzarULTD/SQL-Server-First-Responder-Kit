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

--#STEP: sp_BlitzCache reserved global names in a case-sensitive database
/* This runner owns a disposable SQL Server. A separate database makes the
   procedure's comparisons case-sensitive even when master/tempdb are not. */
IF DB_ID(N'FRKReservedNameTest') IS NOT NULL
    THROW 51000, 'Reserved-name fixture database already exists.', 1;
EXEC(N'CREATE DATABASE FRKReservedNameTest COLLATE Latin1_General_100_CS_AS;');
BEGIN TRY
    DECLARE @Definition nvarchar(max) = OBJECT_DEFINITION(OBJECT_ID(N'dbo.sp_BlitzCache'));
    SET @Definition = REPLACE(@Definition, N'ALTER PROCEDURE dbo.sp_BlitzCache', N'CREATE PROCEDURE dbo.sp_BlitzCache');
    EXEC FRKReservedNameTest.sys.sp_executesql @Definition;

    DECLARE @Names TABLE (Name sysname COLLATE Latin1_General_100_BIN2, SortOrder varchar(50), Qualified bit);
    INSERT @Names VALUES
        (N'##BlitzCacheProcs', 'cpu', 0),
        (N'##BlitzCacheResults', 'cpu', 0),
        (N'##blitzcacheprocs', 'duplicate', 0),
        (N'##BLITZCACHERESULTS', 'query hash', 0),
        (N'##BlitzCachéProcs', 'cpu', 0),
        (N'##ＢlitzCacheResults', 'cpu', 0),
        (N'##BlitzCacheProcs', 'cpu', 1),
        (N'##BlitzCacheResults', 'cpu', 1);
    DECLARE @Name sysname, @Sort varchar(50), @Qualified bit,
            @OutputDB sysname, @OutputSchema sysname;
    WHILE EXISTS (SELECT 1 FROM @Names)
    BEGIN
        SELECT TOP (1) @Name = Name, @Sort = SortOrder, @Qualified = Qualified FROM @Names;
        SELECT @OutputDB = CASE WHEN @Qualified = 1 THEN N'FRKReservedNameTest' END,
               @OutputSchema = CASE WHEN @Qualified = 1 THEN N'dbo' END;
        BEGIN TRY
            EXEC FRKReservedNameTest.dbo.sp_BlitzCache
                 @Top = 1, @SortOrder = @Sort, @OutputTableName = @Name,
                 @OutputDatabaseName = @OutputDB, @OutputSchemaName = @OutputSchema;
            THROW 51000, 'Reserved global name was accepted.', 1;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() <> 50000 OR ERROR_MESSAGE() NOT LIKE 'OutputTableName is a reserved name%'
                THROW;
        END CATCH;
        DELETE @Names WHERE Name = @Name AND SortOrder = @Sort AND Qualified = @Qualified;
    END;
    DROP DATABASE FRKReservedNameTest;
END TRY
BEGIN CATCH
    DROP DATABASE FRKReservedNameTest;
    THROW;
END CATCH;

--#STEP: sp_BlitzCache ordinary global output still works
IF OBJECT_ID(N'tempdb..##FRKCacheOutput') IS NOT NULL
    THROW 51000, 'Global-output fixture already exists.', 1;
BEGIN TRY
    EXEC FRKSmokeTest.sys.sp_executesql N'SELECT SUM(CONVERT(bigint, a.Id)) FROM dbo.Users a CROSS JOIN dbo.Users b;';
    EXEC dbo.sp_BlitzCache @Top = 5, @DatabaseName = N'FRKSmokeTest',
         @MinimumExecutionCount = 0, @OutputTableName = N'##FRKCacheOutput';
    IF OBJECT_ID(N'tempdb..##FRKCacheOutput') IS NULL
       OR COL_LENGTH(N'tempdb..##FRKCacheOutput', N'QueryText') IS NULL
        THROW 51000, 'Global cache output is missing or has the wrong schema.', 1;
    IF (SELECT COUNT(*) FROM ##FRKCacheOutput) < 1
        THROW 51000, 'Global cache output is unexpectedly empty.', 1;
    DROP TABLE ##FRKCacheOutput;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS ##FRKCacheOutput;
    THROW;
END CATCH;
--#STEP: sp_BlitzCache rejects unsupported filters before reanalysis
/* Populate real results in this session so @Reanalyze cannot silently fall
   back to a fresh collection. */
EXEC dbo.sp_BlitzCache @Top = 1;
IF OBJECT_ID(N'tempdb..##BlitzCacheResults') IS NULL
    THROW 51000, 'Reanalysis fixture was not created.', 1;
DECLARE @Cases TABLE (SortOrder varchar(50), QueryFilter varchar(10));
INSERT @Cases
SELECT s.SortOrder, f.QueryFilter
FROM (VALUES ('memory grant'), ('avg memory grant'), ('unused grant'), ('duplicate')) s(SortOrder)
CROSS JOIN (VALUES ('procedures'), ('functions')) f(QueryFilter);
INSERT @Cases VALUES ('spills', 'functions'), ('avg spills', 'functions'),
    ('average memory grants', 'procedures'), ('duplicates', 'functions'),
    ('query hash, average memory grants', 'procedures');
DECLARE @Sort varchar(50), @Filter varchar(10), @Reanalyze bit;
BEGIN TRY
    WHILE EXISTS (SELECT 1 FROM @Cases)
    BEGIN
        SELECT TOP (1) @Sort = SortOrder, @Filter = QueryFilter FROM @Cases;
        SET @Reanalyze = 0;
        WHILE @Reanalyze IS NOT NULL
        BEGIN
            BEGIN TRY
                EXEC dbo.sp_BlitzCache @Top = 1, @SortOrder = @Sort,
                     @QueryFilter = @Filter, @Reanalyze = @Reanalyze;
                THROW 51000, 'Unsupported sort/filter combination was accepted.', 1;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() <> 50000 OR
                   (ERROR_MESSAGE() NOT LIKE 'This sort order requires statement statistics.%'
                    AND ERROR_MESSAGE() NOT LIKE 'Function statistics do not support sorting by spills.%')
                    THROW;
            END CATCH;
            SET @Reanalyze = CASE WHEN @Reanalyze = 0 THEN 1 END;
        END;
        DELETE @Cases WHERE SortOrder = @Sort AND QueryFilter = @Filter;
    END;
    DROP TABLE ##BlitzCacheResults;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS ##BlitzCacheResults;
    THROW;
END CATCH;

--#STEP: sp_BlitzCache supported filters and query-hash aliases
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'procedures', @SortOrder = 'cpu';
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'functions', @SortOrder = 'cpu';
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'procedures', @SortOrder = 'spills';
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'statements', @SortOrder = 'average memory grants';
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'statements', @SortOrder = 'query hash, average reads';
EXEC dbo.sp_BlitzCache @Top = 1, @QueryFilter = 'statements', @SortOrder = 'query hash';

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
        @Empty nvarchar(512), @Missing nvarchar(512), @File nvarchar(512), @Paths nvarchar(max), @DeleteBefore datetime = DATEADD(day,1,GETDATE());
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
    EXEC master.dbo.xp_delete_file 0, @Full, N'bak', @DeleteBefore;
    EXEC master.dbo.xp_delete_file 0, @LogA, N'trn', @DeleteBefore;
    EXEC master.dbo.xp_delete_file 0, @LogB, N'trn', @DeleteBefore;
END TRY
BEGIN CATCH
    IF DB_ID(N'FRKMultiLogRestored') IS NOT NULL DROP DATABASE FRKMultiLogRestored;
    IF DB_ID(N'FRKMultiLogSource') IS NOT NULL DROP DATABASE FRKMultiLogSource;
    EXEC master.dbo.xp_delete_file 0, @Full, N'bak', @DeleteBefore;
    EXEC master.dbo.xp_delete_file 0, @LogA, N'trn', @DeleteBefore;
    EXEC master.dbo.xp_delete_file 0, @LogB, N'trn', @DeleteBefore;
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

--#STEP: sp_BlitzFirst multi-server deltas and in-place upgrades
IF DB_ID(N'FRKDeltaSmoke') IS NOT NULL THROW 51000,'Delta fixture already exists.',1;
EXEC(N'CREATE DATABASE FRKDeltaSmoke;');
GO
EXEC master.dbo.sp_BlitzFirst @Seconds=1,@OutputDatabaseName=N'FRKDeltaSmoke',@OutputSchemaName=N'dbo',@OutputTableNameFileStats=N'Files',@OutputTableNamePerfmonStats=N'Perfmon',@OutputTableNameWaitStats=N'Waits';
GO
USE FRKDeltaSmoke;

DELETE dbo.Files; DELETE dbo.Perfmon; DELETE dbo.Waits;
CREATE TABLE dbo.Expected(ServerName nvarchar(128),CheckDate datetimeoffset,CounterValue bigint,ElapsedSeconds int);
DECLARE @Base datetimeoffset=DATEADD(hour,-1,SYSDATETIMEOFFSET()),
 @ServerA nvarchar(128)=N'FRKDeltaA_'+CONVERT(nvarchar(36),NEWID()),
 @ServerB nvarchar(128)=N'FRKDeltaB_'+CONVERT(nvarchar(36),NEWID());
INSERT dbo.Expected VALUES
(@ServerA,DATEADD(minute,0,@Base),1000,NULL),
(@ServerA,DATEADD(minute,5,@Base),2000,300),
(@ServerA,DATEADD(minute,10,@Base),3000,300),
(@ServerB,DATEADD(minute,0,@Base),4000,NULL),
(@ServerB,DATEADD(minute,5,@Base),5000,300),
(@ServerB,DATEADD(minute,12,@Base),6000,420);
INSERT dbo.Files(ServerName,CheckDate,DatabaseID,FileID,num_of_reads,num_of_writes,io_stall_read_ms,io_stall_write_ms,bytes_read,bytes_written)
SELECT ServerName,CheckDate,1,1,CounterValue,CounterValue,CounterValue,CounterValue,CounterValue,CounterValue FROM dbo.Expected;
INSERT dbo.Perfmon(ServerName,CheckDate,object_name,counter_name,instance_name,cntr_type,cntr_value)
SELECT ServerName,CheckDate,N'Object',N'Counter',N'Instance',272696576,CounterValue FROM dbo.Expected;
INSERT dbo.Waits(ServerName,CheckDate,wait_type,wait_time_ms,signal_wait_time_ms,waiting_tasks_count)
SELECT ServerName,CheckDate,N'LCK_M_X',CounterValue,0,CounterValue FROM dbo.Expected;
CREATE USER CodexDeltaReader WITHOUT LOGIN;
GRANT SELECT ON dbo.Files_Deltas TO CodexDeltaReader;
GRANT SELECT ON dbo.Perfmon_Deltas TO CodexDeltaReader;
GRANT SELECT ON dbo.Waits_Deltas TO CodexDeltaReader;
SELECT object_id,name INTO dbo.OriginalViewIds FROM sys.views WHERE name IN('Files_Deltas','Perfmon_Deltas','Waits_Deltas');

DELETE d FROM dbo.Files d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
DELETE d FROM dbo.Perfmon d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
DELETE d FROM dbo.Waits d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
IF (SELECT COUNT(*) FROM dbo.Files_Deltas)<>4 OR (SELECT COUNT(*) FROM dbo.Perfmon_Deltas)<>4 OR (SELECT COUNT(*) FROM dbo.Waits_Deltas)<>4 THROW 51000,'Incorrect view row counts',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Files_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Files delta key.',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Perfmon_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Perfmon delta key.',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Waits_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Waits delta key.',1;
IF EXISTS(SELECT 1 FROM dbo.Files_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.num_of_reads IS NULL OR d.num_of_reads<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect file delta',1;
IF EXISTS(SELECT 1 FROM dbo.Perfmon_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.cntr_delta IS NULL OR d.cntr_delta<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect perfmon delta',1;
IF EXISTS(SELECT 1 FROM dbo.Waits_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.wait_time_ms_delta IS NULL OR d.wait_time_ms_delta<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect wait delta',1;
IF EXISTS(SELECT 1 FROM dbo.OriginalViewIds i LEFT JOIN sys.views v ON i.object_id=v.object_id AND i.name=v.name WHERE v.object_id IS NULL) THROW 51000,'View object ID changed',1;
IF (SELECT COUNT(*) FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID('CodexDeltaReader') AND permission_name='SELECT')<>3 THROW 51000,'View permissions lost',1;
PRINT 'SERVER DELTAS AND UPGRADE PASS';

GO
ALTER VIEW dbo.Files_Deltas AS SELECT CAST(1 AS int) AS Legacy;
GO
ALTER VIEW dbo.Perfmon_Deltas AS SELECT CAST(1 AS int) AS Legacy;
GO
ALTER VIEW dbo.Waits_Deltas AS SELECT CAST(1 AS int) AS Legacy;
GO
EXEC master.dbo.sp_BlitzFirst @Seconds=1,@OutputDatabaseName=N'FRKDeltaSmoke',@OutputSchemaName=N'dbo',@OutputTableNameFileStats=N'Files',@OutputTableNamePerfmonStats=N'Perfmon',@OutputTableNameWaitStats=N'Waits';
GO

DELETE d FROM dbo.Files d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
DELETE d FROM dbo.Perfmon d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
DELETE d FROM dbo.Waits d WHERE NOT EXISTS(SELECT 1 FROM dbo.Expected e WHERE e.ServerName=d.ServerName);
IF (SELECT COUNT(*) FROM dbo.Files_Deltas)<>4 OR (SELECT COUNT(*) FROM dbo.Perfmon_Deltas)<>4 OR (SELECT COUNT(*) FROM dbo.Waits_Deltas)<>4 THROW 51000,'Incorrect view row counts',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Files_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Files delta key.',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Perfmon_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Perfmon delta key.',1;
IF EXISTS(
    SELECT 1 FROM (SELECT ServerName,CheckDate FROM dbo.Expected WHERE ElapsedSeconds IS NOT NULL) e
    FULL OUTER JOIN (SELECT ServerName,CheckDate,COUNT(*) AS Copies FROM dbo.Waits_Deltas GROUP BY ServerName,CheckDate) d
    ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate
    WHERE e.ServerName IS NULL OR d.ServerName IS NULL OR d.Copies<>1)
    THROW 51000,'Missing, extra, or duplicate Waits delta key.',1;
IF EXISTS(SELECT 1 FROM dbo.Files_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.num_of_reads IS NULL OR d.num_of_reads<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect file delta',1;
IF EXISTS(SELECT 1 FROM dbo.Perfmon_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.cntr_delta IS NULL OR d.cntr_delta<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect perfmon delta',1;
IF EXISTS(SELECT 1 FROM dbo.Waits_Deltas d JOIN dbo.Expected e ON e.ServerName=d.ServerName AND e.CheckDate=d.CheckDate WHERE d.wait_time_ms_delta IS NULL OR d.wait_time_ms_delta<>1000 OR d.ElapsedSeconds IS NULL OR d.ElapsedSeconds<>e.ElapsedSeconds OR e.ElapsedSeconds IS NULL) THROW 51000,'Incorrect wait delta',1;
IF EXISTS(SELECT 1 FROM dbo.OriginalViewIds i LEFT JOIN sys.views v ON i.object_id=v.object_id AND i.name=v.name WHERE v.object_id IS NULL) THROW 51000,'View object ID changed',1;
IF (SELECT COUNT(*) FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID('CodexDeltaReader') AND permission_name='SELECT')<>3 THROW 51000,'View permissions lost',1;
PRINT 'SERVER DELTAS AND UPGRADE PASS';

GRANT VIEW DEFINITION ON dbo.Files_Deltas TO CodexDeltaReader;
GRANT VIEW DEFINITION ON dbo.Perfmon_Deltas TO CodexDeltaReader;
GRANT VIEW DEFINITION ON dbo.Waits_Deltas TO CodexDeltaReader;
EXECUTE AS USER=N'CodexDeltaReader';
BEGIN TRY
    IF (SELECT COUNT(*) FROM sys.sql_modules WHERE definition LIKE '%FRK_ServerScopedDeltas_v1%') <> 3
        THROW 51000,'Collector cannot recognize the migrated views.',1;
    IF HAS_PERMS_BY_NAME(N'dbo.Files_Deltas', N'OBJECT', N'ALTER') <> 0
        THROW 51000,'Metadata visibility test unexpectedly has ALTER permission.',1;
    REVERT;
END TRY
BEGIN CATCH
    REVERT;
    THROW;
END CATCH;
SELECT object_id,modify_date INTO dbo.ViewModified FROM sys.views
WHERE name IN(N'Files_Deltas',N'Perfmon_Deltas',N'Waits_Deltas');
WAITFOR DELAY '00:00:01';
GO
EXEC master.dbo.sp_BlitzFirst @Seconds=1,@OutputDatabaseName=N'FRKDeltaSmoke',@OutputSchemaName=N'dbo',@OutputTableNameFileStats=N'Files',@OutputTableNamePerfmonStats=N'Perfmon',@OutputTableNameWaitStats=N'Waits';
GO
IF EXISTS(SELECT 1 FROM dbo.ViewModified m JOIN sys.views v ON v.object_id=m.object_id
          WHERE v.modify_date<>m.modify_date)
    THROW 51000,'Repeated collection unnecessarily altered a migrated view.',1;
PRINT 'Multi-server delta values, upgrades, permissions, and repeat behavior passed.';
USE master;
DROP DATABASE FRKDeltaSmoke;
--#STEP: sp_BlitzLock parses a real system_health ring-buffer deadlock
/* The runner creates two concurrent workers and asserts the parsed participants. */
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
--#STEP: sp_BlitzAnalysis defaults and isolates output schemas
/* The runner checks three result sets using analysis-schema-regression.sql. */
