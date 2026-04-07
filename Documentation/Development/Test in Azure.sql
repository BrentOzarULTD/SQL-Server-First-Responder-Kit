DROP TABLE IF EXISTS dbo.Blitz;
DROP TABLE IF EXISTS dbo.BlitzWho_Results;
DROP TABLE IF EXISTS dbo.BlitzFirst;
DROP TABLE IF EXISTS dbo.BlitzFirst_FileStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_PerfmonStats;
DROP TABLE IF EXISTS dbo.BlitzFirst_WaitStats;
DROP TABLE IF EXISTS dbo.BlitzCache;
DROP TABLE IF EXISTS dbo.BlitzWho;
DROP TABLE IF EXISTS dbo.BlitzLock;
GO
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1;
GO
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz';
GO
EXEC dbo.sp_BlitzCache @SortOrder = 'all';
GO
EXEC dbo.sp_BlitzCache @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';
GO
EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzFirst @SinceStartup = 1;
GO
EXEC dbo.sp_BlitzFirst @OutputSchemaName = 'dbo',
                       @OutputTableName = 'BlitzFirst',
                       @OutputTableNameFileStats = 'BlitzFirst_FileStats',
                       @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats',
                       @OutputTableNameWaitStats = 'BlitzFirst_WaitStats',
                       @OutputTableNameBlitzCache = 'BlitzCache',
                       @OutputTableNameBlitzWho = 'BlitzWho';
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
