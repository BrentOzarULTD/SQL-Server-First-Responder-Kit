/*Blitz*/
EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'OutputTest', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz'

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @Debug = 1

EXEC sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @Debug = 2

/*BlitzWho*/

EXEC dbo.sp_BlitzWho @ExpertMode = 1, @Debug = 1

EXEC dbo.sp_BlitzWho @ExpertMode = 0, @Debug = 1

/*BlitzFirst*/
EXEC sp_BlitzFirst @Seconds = 5, @ExpertMode = 1

EXEC sp_BlitzFirst @SinceStartup = 1

EXEC sp_BlitzFirst @Seconds = 5, @ExpertMode = 1, @ShowSleepingSPIDs = 1

/*BlitzIndex*/
EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = 'StackOverflow', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = N'StackOverflow2010ಠ_ಠ', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = N'ಠ_ಠ', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = N'¯\_(ツ)_/¯', @Mode = 4

EXEC sp_BlitzIndex @DatabaseName = 'StackOverflow', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @DatabaseName = N'¯\_(ツ)_/¯', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @DatabaseName = N'StackOverflow2010ಠ_ಠ', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @DatabaseName = N'ಠ_ಠ', @Mode = 4, @SkipPartitions = 0, @SkipStatistics = 0

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 1

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 2

EXEC sp_BlitzIndex @GetAllDatabases = 1, @Mode = 3


/*BlitzCache*/
EXEC sp_BlitzCache @SortOrder = 'all', @Debug = 1

EXEC sp_BlitzCache @SortOrder = 'all avg'

EXEC sp_BlitzCache @MinimumExecutionCount = 10

EXEC sp_BlitzCache @DatabaseName = N'¯\_(ツ)_/¯'

EXEC sp_BlitzCache @DatabaseName = N'StackOverflow2010ಠ_ಠ'

EXEC sp_BlitzCache @OutputDatabaseName = 'OutputTest', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache'

EXEC sp_BlitzCache @ExpertMode = 1

EXEC sp_BlitzCache @ExpertMode = 2

/*BlitzQueryStore*/
EXEC sp_BlitzQueryStore @DatabaseName = '¯\_(ツ)_/¯'

/*BlitzBackups*/
EXEC sp_BlitzBackups @HoursBack = 1000000

/*sp_AllNightLog_Setup*/
EXEC sp_AllNightLog_Setup @RPOSeconds = 30, @RTOSeconds = 30, @BackupPath = 'D:\Backup', @RestorePath = 'D:\Backup', @RunSetup = 1

/*sp_BlitzLock*/
EXEC sp_BlitzLock @Debug = 1