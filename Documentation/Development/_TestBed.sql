/*Blitz*/
EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'ChangeMe', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz'

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @Debug = 1

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @Debug = 2

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

/*BlitzBackups*/
EXEC sp_BlitzBackups @HoursBack = 1000000

/*sp_AllNightLog_Setup*/
EXEC master.dbo.sp_AllNightLog_Setup @RPOSeconds = 30, @RTOSeconds = 30, @BackupPath = 'D:\Backup', @RestorePath = 'D:\Backup', @RunSetup = 1
