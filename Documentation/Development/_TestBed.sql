/*Blitz*/
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1;
GO
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz';
GO

/*BlitzWho*/
EXEC dbo.sp_BlitzWho @ExpertMode = 1;
GO
EXEC dbo.sp_BlitzWho @ExpertMode = 0;
GO
EXEC dbo.sp_BlitzWho @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzWho_Results';
GO

/*BlitzFirst*/
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


/*BlitzIndex*/
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


/*BlitzCache*/
EXEC dbo.sp_BlitzCache @SortOrder = 'all';
GO
EXEC dbo.sp_BlitzCache @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';
GO

/*BlitzQueryStore - uncomment this when testing on 2016+ instances: 
EXEC dbo.sp_BlitzQueryStore @DatabaseName = 'StackOverflow';
GO
*/

/*BlitzBackups*/
EXEC dbo.sp_BlitzBackups @HoursBack = 1000000;
GO

/*sp_BlitzLock*/
EXEC dbo.sp_BlitzLock;
GO
