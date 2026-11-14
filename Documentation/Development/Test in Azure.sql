/* Verify the installer supplied the same release of sp_kill as sp_BlitzFirst.
   A pre-existing procedure must not make a missing installer entry look green. */
IF OBJECT_ID(N'dbo.sp_kill', N'P') IS NULL
    THROW 51000, 'Install-Azure.sql did not install sp_kill.', 1;
DECLARE @KillVersion varchar(30), @KillDate datetime,
        @FirstVersion varchar(30), @FirstDate datetime;
EXEC dbo.sp_kill @VersionCheckMode = 1, @Version = @KillVersion OUTPUT, @VersionDate = @KillDate OUTPUT;
EXEC dbo.sp_BlitzFirst @VersionCheckMode = 1, @Version = @FirstVersion OUTPUT, @VersionDate = @FirstDate OUTPUT;
IF @KillVersion IS NULL OR @KillDate IS NULL OR @FirstVersion IS NULL OR @FirstDate IS NULL
    OR @KillVersion <> @FirstVersion OR @KillDate <> @FirstDate
    THROW 51000, 'Installed sp_kill version/date does not match this release.', 1;
GO
DROP TABLE IF EXISTS dbo.BlitzWho_Results;
DROP TABLE IF EXISTS dbo.BlitzFirst;
DROP TABLE IF EXISTS dbo.BlitzFirst_FileStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_PerfmonStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_WaitStats;
DROP TABLE IF EXISTS dbo.BlitzCache;
DROP TABLE IF EXISTS dbo.BlitzWho;
DROP TABLE IF EXISTS dbo.BlitzLock;
GO
EXEC dbo.sp_BlitzCache @SortOrder = 'all';
GO
DECLARE @OutputDatabase sysname = DB_NAME();
EXEC dbo.sp_BlitzCache @OutputDatabaseName = @OutputDatabase, @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';
IF OBJECT_ID(N'dbo.BlitzCache', N'U') IS NULL
    THROW 51000, 'sp_BlitzCache did not create its output table.', 1;
GO
EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzFirst @SinceStartup = 1;
GO
DECLARE @OutputDatabase sysname = DB_NAME();
EXEC dbo.sp_BlitzFirst @OutputDatabaseName = @OutputDatabase, @OutputSchemaName = 'dbo',
                       @OutputTableName = 'BlitzFirst',
                       @OutputTableNameFileStats = 'BlitzFirst_FileStats',
                       @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats',
                       @OutputTableNameWaitStats = 'BlitzFirst_WaitStats',
                       @OutputTableNameBlitzCache = 'BlitzCache',
                       @OutputTableNameBlitzWho = 'BlitzWho';
IF EXISTS
(
    SELECT 1 FROM (VALUES (N'BlitzFirst'), (N'BlitzFirst_FileStats'),
        (N'BlitzFirst_PerfmonStats'), (N'BlitzFirst_WaitStats'),
        (N'BlitzCache'), (N'BlitzWho')) AS expected(name)
    WHERE OBJECT_ID(N'dbo.' + QUOTENAME(expected.name), N'U') IS NULL
)
    THROW 51000, 'sp_BlitzFirst did not create all requested output tables.', 1;
/* Session/wait/cache outputs may be empty on an idle database. File statistics
   must contain at least one sample, so also prove that rows were persisted. */
EXEC(N'IF NOT EXISTS (SELECT 1 FROM dbo.BlitzFirst_FileStats)
    THROW 51000, ''sp_BlitzFirst did not persist file statistics.'', 1;');
GO
EXEC dbo.sp_BlitzIndex @Mode = 0;
GO
EXEC dbo.sp_BlitzIndex @Mode = 1;
GO
EXEC dbo.sp_BlitzIndex @Mode = 2;
GO
EXEC dbo.sp_BlitzIndex @Mode = 3;
GO
EXEC dbo.sp_BlitzIndex @Mode = 4;
GO
EXEC dbo.sp_BlitzIndex @TableName = 'Users'
GO
EXEC dbo.sp_BlitzLock;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 0;
GO
EXEC dbo.sp_BlitzWho @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzWho_Results';
GO
EXEC dbo.sp_kill @ExecuteKills = 'N';
GO
EXEC dbo.sp_kill @ExecuteKills = 'Y';
GO
