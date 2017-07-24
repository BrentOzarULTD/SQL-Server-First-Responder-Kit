/*Blitz*/
EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'ChangeMe', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz'

/*BlitzFirst*/
EXEC sp_BlitzFirst @Seconds = 5, @ExpertMode = 1

EXEC sp_BlitzFirst @SinceStartup = 1

EXEC sp_BlitzFirst @Seconds = 5, @ExpertMode = 1, @ShowSleepingSPIDs = 1

/*BlitzIndex*/
EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = 'StackOverflow', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = 'ChangeMe', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = 'StackOverflow', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @DatabaseName = 'ChangeMe', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 1

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 2

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 3


/*BlitzCache*/
EXEC sp_BlitzCache @SortOrder = 'all'

EXEC sp_BlitzCache @SortOrder = 'all avg'

EXEC sp_BlitzCache @MinimumExecutionCount = 10

EXEC sp_BlitzCache @OutputDatabaseName = 'ChangeMe', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache'
