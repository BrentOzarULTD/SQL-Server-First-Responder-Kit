/* Remove configuration tables from last tests */
DROP TABLE IF EXISTS DBAtools.dbo.Blitz;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzWho_Results;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_FileStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_PerfmonStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_WaitStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzCache;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzWho;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzLock;
GO


EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1;
GO
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz';
GO


EXEC dbo.sp_BlitzBackups @HoursBack = 1000000;
GO


EXEC dbo.sp_BlitzCache;
GO
EXEC dbo.sp_BlitzCache @AI = 2;
GO
EXEC dbo.sp_BlitzCache @SortOrder = 'all';
GO
EXEC dbo.sp_BlitzCache @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';
GO


EXEC dbo.sp_BlitzFirst;
GO
EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzFirst @SinceStartup = 1;
GO
EXEC dbo.sp_BlitzFirst @OutputDatabaseName = 'DBAtools',
                       @OutputSchemaName = 'dbo',
                       @OutputTableName = 'BlitzFirst',
                       @OutputTableNameFileStats = 'BlitzFirst_FileStats',
                       @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats',
                       @OutputTableNameWaitStats = 'BlitzFirst_WaitStats',
                       @OutputTableNameBlitzCache = 'BlitzCache',
                       @OutputTableNameBlitzWho = 'BlitzWho';
GO


EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 0;
GO
EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 1;
GO
EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 2;
GO
EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 3;
GO
EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 4;
GO
EXEC dbo.sp_BlitzIndex @DatabaseName = 'StackOverflow', @TableName = 'Users'
GO
EXEC dbo.sp_BlitzIndex @DatabaseName = 'StackOverflow', @TableName = 'Users', @AI = 2;
GO



EXEC dbo.sp_BlitzLock;
GO
EXEC dbo.sp_BlitzLock @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzLock';
GO


EXEC dbo.sp_BlitzWho;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzWho @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzWho_Results';
GO

