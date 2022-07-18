SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON

IF NOT EXISTS (SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID(N'[dbo].[sp_BlitzAnalysis]') AND [type] in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[sp_BlitzAnalysis] AS' 
END
GO

ALTER PROCEDURE [dbo].[sp_BlitzAnalysis] (
@Help TINYINT = 0,
@StartDate DATETIMEOFFSET(7) = NULL,
@EndDate DATETIMEOFFSET(7) = NULL,
@OutputDatabaseName NVARCHAR(256) = 'DBAtools',
@OutputSchemaName NVARCHAR(256) = N'dbo',
@OutputTableNameBlitzFirst NVARCHAR(256) = N'BlitzFirst', 
@OutputTableNameFileStats NVARCHAR(256) = N'BlitzFirst_FileStats',
@OutputTableNamePerfmonStats NVARCHAR(256)  = N'BlitzFirst_PerfmonStats',
@OutputTableNameWaitStats NVARCHAR(256) = N'BlitzFirst_WaitStats',
@OutputTableNameBlitzCache NVARCHAR(256) = N'BlitzCache',
@OutputTableNameBlitzWho NVARCHAR(256) = N'BlitzWho',
@Servername NVARCHAR(128) = @@SERVERNAME,
@Databasename NVARCHAR(128) = NULL,
@BlitzCacheSortorder NVARCHAR(20) = N'cpu',
@MaxBlitzFirstPriority INT = 249,
@ReadLatencyThreshold INT = 100,
@WriteLatencyThreshold INT = 100,
@WaitStatsTop TINYINT = 10,
@Version VARCHAR(30) = NULL OUTPUT,
@VersionDate DATETIME = NULL OUTPUT,
@VersionCheckMode BIT = 0,
@BringThePain BIT = 0,
@Maxdop INT = 1,
@Debug BIT = 0
)
AS 
SET NOCOUNT ON;
SET STATISTICS XML OFF;

SELECT @Version = '8.10', @VersionDate = '20220718';

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

IF (@Help = 1) 
BEGIN 
	PRINT 'EXEC sp_BlitzAnalysis 
@StartDate = NULL,		/* Specify a datetime or NULL will get an hour ago */
@EndDate = NULL,		/* Specify a datetime or NULL will get an hour of data since @StartDate */
@OutputDatabaseName = N''DBA'',		/* Specify the database name where where we can find your logged blitz data */
@OutputSchemaName = N''dbo'',		/* Specify the schema */
@OutputTableNameBlitzFirst = N''BlitzFirst'',		/* Table name where you are storing sp_BlitzFirst output, Set to NULL to ignore */ 
@OutputTableNameFileStats = N''BlitzFirst_FileStats'',		/* Table name where you are storing sp_BlitzFirst filestats output, Set to NULL to ignore */ 
@OutputTableNamePerfmonStats  = N''BlitzFirst_PerfmonStats'',		/* Table name where you are storing sp_BlitzFirst Perfmon output, Set to NULL to ignore */ 
@OutputTableNameWaitStats = N''BlitzFirst_WaitStats'',		/* Table name where you are storing sp_BlitzFirst Wait stats output, Set to NULL to ignore */ 
@OutputTableNameBlitzCache = N''BlitzCache'',		/* Table name where you are storing sp_BlitzCache output, Set to NULL to ignore */ 
@OutputTableNameBlitzWho = N''BlitzWho'',		/* Table name where you are storing sp_BlitzWho output, Set to NULL to ignore */
@Databasename = NULL,		/* Filters results for BlitzCache, FileStats (will also include tempdb), BlitzWho. Leave as NULL for all databases */
@MaxBlitzFirstPriority = 249,		/* Max priority to include in the results */
@BlitzCacheSortorder = ''cpu'',		/* Accepted values ''all'' ''cpu'' ''reads'' ''writes'' ''duration'' ''executions'' ''memory grant'' ''spills'' */
@WaitStatsTop = 3,		/* Controls the top for wait stats only */
@Maxdop = 1,		/* Control the degree of parallelism that the queries within this proc can use if they want to*/
@Debug = 0;		/* Show sp_BlitzAnalysis SQL commands in the messages tab as they execute */

/*
Additional parameters:
@ReadLatencyThreshold INT		/* Default: 100 - Sets the threshold in ms to compare against io_stall_read_average_ms in your filestats table */
@WriteLatencyThreshold INT		/* Default: 100 - Sets the threshold in ms to compare against io_stall_write_average_ms in your filestats table */
@BringThePain BIT		/* Default: 0 - If you are getting more than 4 hours of data with blitzcachesortorder set to ''all'' you will need to set BringThePain to 1 */
*/';
	RETURN;
END

/* Declare all local variables required */
DECLARE @FullOutputTableNameBlitzFirst NVARCHAR(1000); 
DECLARE @FullOutputTableNameFileStats NVARCHAR(1000);
DECLARE @FullOutputTableNamePerfmonStats NVARCHAR(1000);
DECLARE @FullOutputTableNameWaitStats NVARCHAR(1000);
DECLARE @FullOutputTableNameBlitzCache NVARCHAR(1000);
DECLARE @FullOutputTableNameBlitzWho NVARCHAR(1000);
DECLARE @Sql NVARCHAR(MAX);
DECLARE @NewLine NVARCHAR(2) = CHAR(13);
DECLARE @IncludeMemoryGrants BIT;
DECLARE @IncludeSpills BIT;

/* Validate the database name */
IF (DB_ID(@OutputDatabaseName) IS NULL)
BEGIN
	RAISERROR('Invalid database name provided for parameter @OutputDatabaseName: %s',11,0,@OutputDatabaseName);
	RETURN;
END

/* Set fully qualified table names */
SET @FullOutputTableNameBlitzFirst = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNameBlitzFirst);
SET @FullOutputTableNameFileStats = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNameFileStats+N'_Deltas');
SET @FullOutputTableNamePerfmonStats = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNamePerfmonStats+N'_Actuals');
SET @FullOutputTableNameWaitStats = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNameWaitStats+N'_Deltas');
SET @FullOutputTableNameBlitzCache = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNameBlitzCache);
SET @FullOutputTableNameBlitzWho = QUOTENAME(@OutputDatabaseName)+N'.'+QUOTENAME(@OutputSchemaName)+N'.'+QUOTENAME(@OutputTableNameBlitzWho+N'_Deltas');

IF OBJECT_ID('tempdb.dbo.#BlitzFirstCounts') IS NOT NULL 
BEGIN
	DROP TABLE #BlitzFirstCounts;
END

CREATE TABLE #BlitzFirstCounts (
	[Priority] TINYINT NOT NULL,
	[FindingsGroup] VARCHAR(50) NOT NULL,
	[Finding] VARCHAR(200) NOT NULL,
	[TotalOccurrences] INT NULL,
	[FirstOccurrence] DATETIMEOFFSET(7) NULL,
	[LastOccurrence] DATETIMEOFFSET(7) NULL
);

/* Validate variables and set defaults as required */
IF (@BlitzCacheSortorder IS NULL) 
BEGIN 
	SET @BlitzCacheSortorder = N'cpu';
END 

SET @BlitzCacheSortorder = LOWER(@BlitzCacheSortorder);

IF (@OutputTableNameBlitzCache IS NOT NULL AND @BlitzCacheSortorder NOT IN (N'all',N'cpu',N'reads',N'writes',N'duration',N'executions',N'memory grant',N'spills'))
BEGIN
	RAISERROR('Invalid sort option specified for @BlitzCacheSortorder, supported values are ''all'', ''cpu'', ''reads'', ''writes'', ''duration'', ''executions'', ''memory grant'', ''spills''',11,0) WITH NOWAIT;
	RETURN;
END

/* Set @Maxdop to 1 if NULL was passed in */
IF (@Maxdop IS NULL)
BEGIN
	SET @Maxdop = 1;
END

/* iF @Maxdop is set higher than the core count just set it to 0 */
IF (@Maxdop > (SELECT CAST(cpu_count AS INT) FROM sys.dm_os_sys_info))
BEGIN
	SET @Maxdop = 0;
END

/* We need to check if your SQL version has memory grant and spills columns in sys.dm_exec_query_stats */
SELECT @IncludeMemoryGrants = 
	CASE 
		WHEN (EXISTS(SELECT * FROM sys.all_columns WHERE [object_id] = OBJECT_ID('sys.dm_exec_query_stats') AND name = 'max_grant_kb')) THEN 1
		ELSE 0
	END;

SELECT @IncludeSpills = 
	CASE
		WHEN (EXISTS(SELECT * FROM sys.all_columns WHERE [object_id] = OBJECT_ID('sys.dm_exec_query_stats') AND name = 'max_spills')) THEN 1 
		ELSE 0
	END;


IF (@StartDate IS NULL)
BEGIN 
	RAISERROR('Setting @StartDate to: 1 hour ago',0,0) WITH NOWAIT;
	/* Set StartDate to be an hour ago */
	SET @StartDate = DATEADD(HOUR,-1,SYSDATETIMEOFFSET());

	IF (@EndDate IS NULL)
	BEGIN 
		RAISERROR('Setting @EndDate to: Now',0,0) WITH NOWAIT;
		/* Get data right up to now */
		SET @EndDate = SYSDATETIMEOFFSET();
	END
END 

IF (@EndDate IS NULL)
BEGIN 
	/* Default to an hour of data or SYSDATETIMEOFFSET() if now is earlier than the hour added to @StartDate */
	IF(DATEADD(HOUR,1,@StartDate) < SYSDATETIMEOFFSET())
	BEGIN 
		RAISERROR('@EndDate was NULL - Setting to return 1 hour of information, if you want more then set @EndDate aswell',0,0) WITH NOWAIT;
		SET @EndDate = DATEADD(HOUR,1,@StartDate);
	END
	ELSE 
	BEGIN 
		RAISERROR('@EndDate was NULL - Setting to SYSDATETIMEOFFSET()',0,0) WITH NOWAIT;
		SET @EndDate = SYSDATETIMEOFFSET();
	END
END 

/* Default to dbo schema if NULL is passed in */
IF (@OutputSchemaName IS NULL) 
BEGIN 
	SET @OutputSchemaName = 'dbo';
END

/* Prompt the user for @BringThePain = 1 if they are searching a timeframe greater than 4 hours and they are using BlitzCacheSortorder = 'all' */
IF(@BlitzCacheSortorder = 'all' AND DATEDIFF(HOUR,@StartDate,@EndDate) > 4 AND @BringThePain = 0)
BEGIN
	RAISERROR('Wow! hold up now, are you sure you wanna do this? Are sure you want to query over 4 hours of data with @BlitzCacheSortorder set to ''all''? IF you do then set @BringThePain = 1 but I gotta warn you this might hurt a bit!',11,1) WITH NOWAIT;
	RETURN;
END

/* Output report window information */
SELECT 
	@Servername AS [ServerToReportOn],
	CAST(1 AS NVARCHAR(20)) + N' - '+ CAST(@MaxBlitzFirstPriority AS NVARCHAR(20)) AS [PrioritesToInclude],
	@StartDate AS [StartDatetime],
	@EndDate AS [EndDatetime];;


/* BlitzFirst data */
SET @Sql = N'
INSERT INTO #BlitzFirstCounts ([Priority],[FindingsGroup],[Finding],[TotalOccurrences],[FirstOccurrence],[LastOccurrence])
SELECT 
[Priority],
[FindingsGroup],
[Finding],
COUNT(*) AS [TotalOccurrences],
MIN(CheckDate) AS [FirstOccurrence],
MAX(CheckDate) AS [LastOccurrence]
FROM '+@FullOutputTableNameBlitzFirst+N'
WHERE [ServerName] = @Servername
AND [Priority] BETWEEN 1 AND @MaxBlitzFirstPriority
AND CheckDate BETWEEN @StartDate AND @EndDate
AND [CheckID] > -1
GROUP BY [Priority],[FindingsGroup],[Finding];

IF EXISTS(SELECT 1 FROM #BlitzFirstCounts) 
BEGIN 
	SELECT 
	[Priority],
	[FindingsGroup],
	[Finding],
	[TotalOccurrences],
	[FirstOccurrence],
	[LastOccurrence]
	FROM #BlitzFirstCounts
	ORDER BY [Priority] ASC,[TotalOccurrences] DESC;
END
ELSE 
BEGIN 
	SELECT N''No findings with a priority between 1 and ''+CAST(@MaxBlitzFirstPriority AS NVARCHAR(10))+N'' found for this period'';
END
SELECT 
 [ServerName]
,[CheckDate]
,[CheckID]
,[Priority]
,[Finding]
,[URL]
,[Details]
,[HowToStopIt]
,[QueryPlan]
,[QueryText] 
FROM '+@FullOutputTableNameBlitzFirst+N' Findings
WHERE [ServerName] = @Servername
AND [Priority] BETWEEN 1 AND @MaxBlitzFirstPriority
AND [CheckDate] BETWEEN @StartDate AND @EndDate
AND [CheckID] > -1
ORDER BY CheckDate ASC,[Priority] ASC
OPTION (RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');';


RAISERROR('Getting BlitzFirst info from %s',0,0,@FullOutputTableNameBlitzFirst) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT @Sql;
END

IF (OBJECT_ID(@FullOutputTableNameBlitzFirst) IS NULL) 
BEGIN
	IF (@OutputTableNameBlitzFirst IS NULL)
	BEGIN
		RAISERROR('BlitzFirst data skipped',10,0);
		SELECT N'Skipped logged BlitzFirst data as NULL was passed to parameter @OutputTableNameBlitzFirst';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for BlitzFirst data: %s does not exist',10,0,@FullOutputTableNameBlitzFirst);
		SELECT N'No BlitzFirst data available as the table cannot be found';
	END

END
ELSE /* Table exists then run the query */
BEGIN 
	EXEC sp_executesql @Sql,
	N'@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7),
	@Servername NVARCHAR(128),
	@MaxBlitzFirstPriority INT',
	@StartDate=@StartDate, 
	@EndDate=@EndDate,
	@Servername=@Servername,
	@MaxBlitzFirstPriority = @MaxBlitzFirstPriority;
END

/* Blitz WaitStats data */
SET @Sql = N'SELECT 
[ServerName], 
[CheckDate], 
[wait_type], 
[WaitsRank],
[WaitCategory], 
[Ignorable], 
[ElapsedSeconds], 
[wait_time_ms_delta], 
[wait_time_minutes_delta], 
[wait_time_minutes_per_minute], 
[signal_wait_time_ms_delta], 
[waiting_tasks_count_delta],
ISNULL((CAST([wait_time_ms_delta] AS DECIMAL(38,2))/NULLIF(CAST([waiting_tasks_count_delta] AS DECIMAL(38,2)),0)),0) AS [wait_time_ms_per_wait]
FROM 
(
	SELECT
	[ServerName], 
	[CheckDate], 
	[wait_type], 
	[WaitCategory], 
	[Ignorable], 
	[ElapsedSeconds], 
	[wait_time_ms_delta], 
	[wait_time_minutes_delta], 
	[wait_time_minutes_per_minute], 
	[signal_wait_time_ms_delta], 
	[waiting_tasks_count_delta],
	ROW_NUMBER() OVER(PARTITION BY [CheckDate] ORDER BY [CheckDate] ASC,[wait_time_ms_delta] DESC) AS [WaitsRank]
	FROM '+@FullOutputTableNameWaitStats+N' AS [Waits]
	WHERE [ServerName] = @Servername
	AND [CheckDate] BETWEEN @StartDate AND @EndDate
) TopWaits
WHERE [WaitsRank] <= @WaitStatsTop
ORDER BY 
[CheckDate] ASC, 
[wait_time_ms_delta] DESC
OPTION(RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');'

RAISERROR('Getting wait stats info from %s',0,0,@FullOutputTableNameWaitStats) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT @Sql;
END

IF (OBJECT_ID(@FullOutputTableNameWaitStats) IS NULL) 
BEGIN
	IF (@OutputTableNameWaitStats IS NULL)
	BEGIN
		RAISERROR('Wait stats data skipped',10,0);
		SELECT N'Skipped logged wait stats data as NULL was passed to parameter @OutputTableNameWaitStats';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for wait stats data: %s does not exist',10,0,@FullOutputTableNameWaitStats);
		SELECT N'No wait stats data available as the table cannot be found';
	END
END
ELSE /* Table exists then run the query */
BEGIN
	EXEC sp_executesql @Sql,
	N'@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7),
	@Servername NVARCHAR(128),
	@WaitStatsTop TINYINT',
	@StartDate=@StartDate, 
	@EndDate=@EndDate,
	@Servername=@Servername, 
	@WaitStatsTop=@WaitStatsTop;
END

/* BlitzFileStats info */ 
SET @Sql = N'
SELECT 
[ServerName], 
[CheckDate],
CASE 
	WHEN MAX([io_stall_read_ms_average]) > @ReadLatencyThreshold THEN ''Yes''
	WHEN MAX([io_stall_write_ms_average]) > @WriteLatencyThreshold THEN ''Yes''
	ELSE ''No'' 
END AS [io_stall_ms_breached],
LEFT([PhysicalName],LEN([PhysicalName])-CHARINDEX(''\'',REVERSE([PhysicalName]))+1) AS [PhysicalPath],
SUM([SizeOnDiskMB]) AS [SizeOnDiskMB], 
SUM([SizeOnDiskMBgrowth]) AS [SizeOnDiskMBgrowth], 
MAX([io_stall_read_ms]) AS [max_io_stall_read_ms], 
MAX([io_stall_read_ms_average]) AS [max_io_stall_read_ms_average], 
@ReadLatencyThreshold AS [is_stall_read_ms_threshold],
SUM([num_of_reads]) AS [num_of_reads], 
SUM([megabytes_read]) AS [megabytes_read], 
MAX([io_stall_write_ms]) AS [max_io_stall_write_ms], 
MAX([io_stall_write_ms_average]) AS [max_io_stall_write_ms_average], 
@WriteLatencyThreshold AS [io_stall_write_ms_average],
SUM([num_of_writes]) AS [num_of_writes], 
SUM([megabytes_written]) AS [megabytes_written]
FROM '+@FullOutputTableNameFileStats+N'
WHERE [ServerName] = @Servername
AND [CheckDate] BETWEEN @StartDate AND @EndDate
'
+CASE
	WHEN @Databasename IS NOT NULL THEN N'AND [DatabaseName] IN (N''tempdb'',@Databasename)
'
	ELSE N''
END
+N'GROUP BY 
[ServerName], 
[CheckDate],
LEFT([PhysicalName],LEN([PhysicalName])-CHARINDEX(''\'',REVERSE([PhysicalName]))+1)
ORDER BY 
[CheckDate] ASC
OPTION (RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');'

RAISERROR('Getting FileStats info from %s',0,0,@FullOutputTableNameFileStats) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT @Sql;
END

IF (OBJECT_ID(@FullOutputTableNameFileStats) IS NULL) 
BEGIN
	IF (@OutputTableNameFileStats IS NULL)
	BEGIN
		RAISERROR('File stats data skipped',10,0);
		SELECT N'Skipped logged File stats data as NULL was passed to parameter @OutputTableNameFileStats';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for FileStats data: %s does not exist',10,0,@FullOutputTableNameFileStats);
		SELECT N'No File stats data available as the table cannot be found';
	END
END
ELSE /* Table exists then run the query */
BEGIN 
	EXEC sp_executesql @Sql,
	N'@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7),
	@Servername NVARCHAR(128),
	@Databasename NVARCHAR(128),
	@ReadLatencyThreshold INT,
	@WriteLatencyThreshold INT',
	@StartDate=@StartDate, 
	@EndDate=@EndDate,
	@Servername=@Servername,
	@Databasename = @Databasename,
	@ReadLatencyThreshold = @ReadLatencyThreshold,
	@WriteLatencyThreshold = @WriteLatencyThreshold;
END

/* Blitz Perfmon stats*/
SET @Sql = N'
SELECT 
    [ServerName]
	,[CheckDate]
	,[counter_name]
    ,[object_name]
    ,[instance_name]
    ,[cntr_value]
FROM '+@FullOutputTableNamePerfmonStats+N'
WHERE [ServerName] = @Servername
AND CheckDate BETWEEN @StartDate AND @EndDate
ORDER BY 
	[CheckDate] ASC,
	[counter_name] ASC
OPTION (RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');'

RAISERROR('Getting Perfmon info from %s',0,0,@FullOutputTableNamePerfmonStats) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT @Sql;
END

IF (OBJECT_ID(@FullOutputTableNamePerfmonStats) IS NULL) 
BEGIN
	IF (@OutputTableNamePerfmonStats IS NULL)
	BEGIN
		RAISERROR('Perfmon stats data skipped',10,0);
		SELECT N'Skipped logged Perfmon stats data as NULL was passed to parameter @OutputTableNamePerfmonStats';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for Perfmon stats data: %s does not exist',10,0,@FullOutputTableNamePerfmonStats);
		SELECT N'No Perfmon data available as the table cannot be found';
	END
END
ELSE /* Table exists then run the query */
BEGIN 
	EXEC sp_executesql @Sql,
	N'@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7),
	@Servername NVARCHAR(128)',
	@StartDate=@StartDate, 
	@EndDate=@EndDate,
	@Servername=@Servername;
END

/* Blitz cache data */
RAISERROR('Sortorder for BlitzCache data: %s',0,0,@BlitzCacheSortorder) WITH NOWAIT;

/* Set intial CTE */
SET @Sql = N'WITH CheckDates AS (
SELECT DISTINCT CheckDate 
FROM '
+@FullOutputTableNameBlitzCache
+N'
WHERE [ServerName] = @Servername
AND [CheckDate] BETWEEN @StartDate AND @EndDate'
+@NewLine
+CASE
	WHEN @Databasename IS NOT NULL THEN N'AND [DatabaseName] = @Databasename'+@NewLine
	ELSE N''
END
+N')'
;

SET @Sql += @NewLine;

/* Append additional CTEs based on sortorder */
SET @Sql += (
SELECT CAST(N',' AS NVARCHAR(MAX))
+[SortOptions].[Aliasname]+N' AS (
SELECT
	[ServerName]
    ,'+[SortOptions].[Aliasname]+N'.[CheckDate]
	,[Sortorder]
	,[TimeFrameRank]
	,ROW_NUMBER() OVER(ORDER BY ['+[SortOptions].[Columnname]+N'] DESC) AS [OverallRank]
	,'+[SortOptions].[Aliasname]+N'.[QueryType]
	,'+[SortOptions].[Aliasname]+N'.[QueryText]
	,'+[SortOptions].[Aliasname]+N'.[DatabaseName]
	,'+[SortOptions].[Aliasname]+N'.[AverageCPU]
	,'+[SortOptions].[Aliasname]+N'.[TotalCPU]
	,'+[SortOptions].[Aliasname]+N'.[PercentCPUByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageDuration]
	,'+[SortOptions].[Aliasname]+N'.[TotalDuration]
	,'+[SortOptions].[Aliasname]+N'.[PercentDurationByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageReads]
	,'+[SortOptions].[Aliasname]+N'.[TotalReads]
	,'+[SortOptions].[Aliasname]+N'.[PercentReadsByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageWrites]
	,'+[SortOptions].[Aliasname]+N'.[TotalWrites]
	,'+[SortOptions].[Aliasname]+N'.[PercentWritesByType]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionCount]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionWeight]
	,'+[SortOptions].[Aliasname]+N'.[PercentExecutionsByType]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionsPerMinute]
	,'+[SortOptions].[Aliasname]+N'.[PlanCreationTime]
	,'+[SortOptions].[Aliasname]+N'.[PlanCreationTimeHours]
	,'+[SortOptions].[Aliasname]+N'.[LastExecutionTime]
	,'+[SortOptions].[Aliasname]+N'.[PlanHandle]
	,'+[SortOptions].[Aliasname]+N'.[SqlHandle]
	,'+[SortOptions].[Aliasname]+N'.[SQL Handle More Info]
	,'+[SortOptions].[Aliasname]+N'.[QueryHash]
	,'+[SortOptions].[Aliasname]+N'.[Query Hash More Info]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlanHash]
	,'+[SortOptions].[Aliasname]+N'.[StatementStartOffset]
	,'+[SortOptions].[Aliasname]+N'.[StatementEndOffset]
	,'+[SortOptions].[Aliasname]+N'.[MinReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[MaxReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[AverageReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[TotalReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlan]
	,'+[SortOptions].[Aliasname]+N'.[NumberOfPlans]
	,'+[SortOptions].[Aliasname]+N'.[NumberOfDistinctPlans]
	,'+[SortOptions].[Aliasname]+N'.[MinGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MaxGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MinUsedGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MaxUsedGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[PercentMemoryGrantUsed]
	,'+[SortOptions].[Aliasname]+N'.[AvgMaxMemoryGrant]
	,'+[SortOptions].[Aliasname]+N'.[MinSpills]
	,'+[SortOptions].[Aliasname]+N'.[MaxSpills]
	,'+[SortOptions].[Aliasname]+N'.[TotalSpills]
	,'+[SortOptions].[Aliasname]+N'.[AvgSpills]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlanCost]
FROM CheckDates
CROSS APPLY (
	SELECT TOP (5) 	
	[ServerName]
    ,'+[SortOptions].[Aliasname]+N'.[CheckDate]
	,'+QUOTENAME(UPPER([SortOptions].[Sortorder]),N'''')+N' AS [Sortorder]
	,ROW_NUMBER() OVER(ORDER BY ['+[SortOptions].[Columnname]+N'] DESC) AS [TimeFrameRank]
	,'+[SortOptions].[Aliasname]+N'.[QueryType]
	,'+[SortOptions].[Aliasname]+N'.[QueryText]
	,'+[SortOptions].[Aliasname]+N'.[DatabaseName]
	,'+[SortOptions].[Aliasname]+N'.[AverageCPU]
	,'+[SortOptions].[Aliasname]+N'.[TotalCPU]
	,'+[SortOptions].[Aliasname]+N'.[PercentCPUByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageDuration]
	,'+[SortOptions].[Aliasname]+N'.[TotalDuration]
	,'+[SortOptions].[Aliasname]+N'.[PercentDurationByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageReads]
	,'+[SortOptions].[Aliasname]+N'.[TotalReads]
	,'+[SortOptions].[Aliasname]+N'.[PercentReadsByType]
	,'+[SortOptions].[Aliasname]+N'.[AverageWrites]
	,'+[SortOptions].[Aliasname]+N'.[TotalWrites]
	,'+[SortOptions].[Aliasname]+N'.[PercentWritesByType]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionCount]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionWeight]
	,'+[SortOptions].[Aliasname]+N'.[PercentExecutionsByType]
	,'+[SortOptions].[Aliasname]+N'.[ExecutionsPerMinute]
	,'+[SortOptions].[Aliasname]+N'.[PlanCreationTime]
	,'+[SortOptions].[Aliasname]+N'.[PlanCreationTimeHours]
	,'+[SortOptions].[Aliasname]+N'.[LastExecutionTime]
	,'+[SortOptions].[Aliasname]+N'.[PlanHandle]
	,'+[SortOptions].[Aliasname]+N'.[SqlHandle]
	,'+[SortOptions].[Aliasname]+N'.[SQL Handle More Info]
	,'+[SortOptions].[Aliasname]+N'.[QueryHash]
	,'+[SortOptions].[Aliasname]+N'.[Query Hash More Info]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlanHash]
	,'+[SortOptions].[Aliasname]+N'.[StatementStartOffset]
	,'+[SortOptions].[Aliasname]+N'.[StatementEndOffset]
	,'+[SortOptions].[Aliasname]+N'.[MinReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[MaxReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[AverageReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[TotalReturnedRows]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlan]
	,'+[SortOptions].[Aliasname]+N'.[NumberOfPlans]
	,'+[SortOptions].[Aliasname]+N'.[NumberOfDistinctPlans]
	,'+[SortOptions].[Aliasname]+N'.[MinGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MaxGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MinUsedGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[MaxUsedGrantKB]
	,'+[SortOptions].[Aliasname]+N'.[PercentMemoryGrantUsed]
	,'+[SortOptions].[Aliasname]+N'.[AvgMaxMemoryGrant]
	,'+[SortOptions].[Aliasname]+N'.[MinSpills]
	,'+[SortOptions].[Aliasname]+N'.[MaxSpills]
	,'+[SortOptions].[Aliasname]+N'.[TotalSpills]
	,'+[SortOptions].[Aliasname]+N'.[AvgSpills]
	,'+[SortOptions].[Aliasname]+N'.[QueryPlanCost]
	FROM '+@FullOutputTableNameBlitzCache+N' AS '+[SortOptions].[Aliasname]+N'
	WHERE [ServerName] = @Servername
	AND [CheckDate] BETWEEN @StartDate AND @EndDate
	AND ['+[SortOptions].[Aliasname]+N'].[CheckDate] = [CheckDates].[CheckDate]'
	+@NewLine
	+CASE
		WHEN @Databasename IS NOT NULL THEN N'AND ['+[SortOptions].[Aliasname]+N'].[DatabaseName] = @Databasename'+@NewLine
		ELSE N''
	END
	+CASE 
		WHEN [Sortorder] = N'cpu' THEN N'AND [TotalCPU] > 0'
		WHEN [Sortorder] = N'reads' THEN N'AND [TotalReads] > 0'
		WHEN [Sortorder] = N'writes' THEN N'AND [TotalWrites] > 0'
		WHEN [Sortorder] = N'duration' THEN N'AND [TotalDuration] > 0'
		WHEN [Sortorder] = N'executions' THEN N'AND [ExecutionCount] > 0'
		WHEN [Sortorder] = N'memory grant' THEN N'AND [MaxGrantKB] > 0'
		WHEN [Sortorder] = N'spills' THEN N'AND [MaxSpills] > 0'
		ELSE N''
	END
	+N'
	ORDER BY ['+[SortOptions].[Columnname]+N'] DESC) '+[SortOptions].[Aliasname]+N'
)'
FROM (VALUES
		(N'cpu',N'TopCPU',N'TotalCPU'),
		(N'reads',N'TopReads',N'TotalReads'),
		(N'writes',N'TopWrites',N'TotalWrites'),
		(N'duration',N'TopDuration',N'TotalDuration'),
		(N'executions',N'TopExecutions',N'ExecutionCount'),
		(N'memory grant',N'TopMemoryGrants',N'MaxGrantKB'),
		(N'spills',N'TopSpills',N'MaxSpills')
	) SortOptions(Sortorder,Aliasname,Columnname)
WHERE
	CASE /* for spills and memory grant sorts make sure the underlying columns exist in the DMV otherwise do not include them */
		WHEN (@IncludeMemoryGrants = 0 OR @IncludeMemoryGrants IS NULL) AND ([SortOptions].[Sortorder] = N'memory grant' OR [SortOptions].[Sortorder] = N'all') THEN NULL
		WHEN (@IncludeSpills = 0 OR @IncludeSpills IS NULL) AND ([SortOptions].[Sortorder] = N'spills' OR [SortOptions].[Sortorder] = N'all') THEN NULL
		ELSE [SortOptions].[Sortorder]
	END = ISNULL(NULLIF(@BlitzCacheSortorder,N'all'),[SortOptions].[Sortorder])
FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)');

SET @Sql += @NewLine;

/* Build the select statements to return the data after CTE declarations */
SET @Sql += (
SELECT STUFF((
SELECT @NewLine
+N'UNION ALL'
+@NewLine
+N'SELECT *
FROM '+[SortOptions].[Aliasname]
FROM (VALUES
		(N'cpu',N'TopCPU',N'TotalCPU'),
		(N'reads',N'TopReads',N'TotalReads'),
		(N'writes',N'TopWrites',N'TotalWrites'),
		(N'duration',N'TopDuration',N'TotalDuration'),
		(N'executions',N'TopExecutions',N'ExecutionCount'),
		(N'memory grant',N'TopMemoryGrants',N'MaxGrantKB'),
		(N'spills',N'TopSpills',N'MaxSpills')
	) SortOptions(Sortorder,Aliasname,Columnname)
WHERE
	CASE /* for spills and memory grant sorts make sure the underlying columns exist in the DMV otherwise do not include them */
		WHEN (@IncludeMemoryGrants = 0 OR @IncludeMemoryGrants IS NULL) AND ([SortOptions].[Sortorder] = N'memory grant' OR [SortOptions].[Sortorder] = N'all') THEN NULL
		WHEN (@IncludeSpills = 0 OR @IncludeSpills IS NULL) AND ([SortOptions].[Sortorder] = N'spills' OR [SortOptions].[Sortorder] = N'all') THEN NULL
		ELSE [SortOptions].[Sortorder]
	END = ISNULL(NULLIF(@BlitzCacheSortorder,N'all'),[SortOptions].[Sortorder])
FOR XML PATH(N''), TYPE).value(N'.[1]', N'NVARCHAR(MAX)'),1,11,N'')
);

/* Append Order By */
SET @Sql += @NewLine
+N'ORDER BY 
	[Sortorder] ASC,
	[CheckDate] ASC,
	[TimeFrameRank] ASC';

/* Append OPTION(RECOMPILE, MAXDOP) to complete the statement */
SET @Sql += @NewLine
+N'OPTION(RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');';

RAISERROR('Getting BlitzCache info from %s',0,0,@FullOutputTableNameBlitzCache) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT SUBSTRING(@Sql, 0, 4000);
	PRINT SUBSTRING(@Sql, 4000, 8000);
	PRINT SUBSTRING(@Sql, 8000, 12000);
	PRINT SUBSTRING(@Sql, 12000, 16000);
	PRINT SUBSTRING(@Sql, 16000, 20000);
	PRINT SUBSTRING(@Sql, 20000, 24000);
	PRINT SUBSTRING(@Sql, 24000, 28000);
END

IF (OBJECT_ID(@FullOutputTableNameBlitzCache) IS NULL) 
BEGIN
	IF (@OutputTableNameBlitzCache IS NULL)
	BEGIN
		RAISERROR('BlitzCache data skipped',10,0);
		SELECT N'Skipped logged BlitzCache data as NULL was passed to parameter @OutputTableNameBlitzCache';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for BlitzCache data: %s does not exist',10,0,@FullOutputTableNameBlitzCache);
		SELECT N'No BlitzCache data available as the table cannot be found';
	END
END
ELSE /* Table exists then run the query */
BEGIN
	EXEC sp_executesql @Sql,
	N'@Servername NVARCHAR(128),
	@Databasename NVARCHAR(128),
	@BlitzCacheSortorder NVARCHAR(20),
	@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7)',
	@Servername = @Servername,
	@Databasename = @Databasename,
	@BlitzCacheSortorder = @BlitzCacheSortorder,
	@StartDate = @StartDate,
	@EndDate = @EndDate;
END



/* BlitzWho data */
SET @Sql = N'
SELECT [ServerName]
      ,[CheckDate]
      ,[elapsed_time]
      ,[session_id]
      ,[database_name]
      ,[query_text_snippet]
      ,[query_plan]
      ,[live_query_plan]
      ,[query_cost]
      ,[status]
      ,[wait_info]
      ,[top_session_waits]
      ,[blocking_session_id]
      ,[open_transaction_count]
      ,[is_implicit_transaction]
      ,[nt_domain]
      ,[host_name]
      ,[login_name]
      ,[nt_user_name]
      ,[program_name]
      ,[fix_parameter_sniffing]
      ,[client_interface_name]
      ,[login_time]
      ,[start_time]
      ,[request_time]
      ,[request_cpu_time]
      ,[degree_of_parallelism]
      ,[request_logical_reads]
      ,[Logical_Reads_MB]
      ,[request_writes]
      ,[Logical_Writes_MB]
      ,[request_physical_reads]
      ,[Physical_reads_MB]
      ,[session_cpu]
      ,[session_logical_reads]
      ,[session_logical_reads_MB]
      ,[session_physical_reads]
      ,[session_physical_reads_MB]
      ,[session_writes]
      ,[session_writes_MB]
      ,[tempdb_allocations_mb]
      ,[memory_usage]
      ,[estimated_completion_time]
      ,[percent_complete]
      ,[deadlock_priority]
      ,[transaction_isolation_level]
      ,[last_dop]
      ,[min_dop]
      ,[max_dop]
      ,[last_grant_kb]
      ,[min_grant_kb]
      ,[max_grant_kb]
      ,[last_used_grant_kb]
      ,[min_used_grant_kb]
      ,[max_used_grant_kb]
      ,[last_ideal_grant_kb]
      ,[min_ideal_grant_kb]
      ,[max_ideal_grant_kb]
      ,[last_reserved_threads]
      ,[min_reserved_threads]
      ,[max_reserved_threads]
      ,[last_used_threads]
      ,[min_used_threads]
      ,[max_used_threads]
      ,[grant_time]
      ,[requested_memory_kb]
      ,[grant_memory_kb]
      ,[is_request_granted]
      ,[required_memory_kb]
      ,[query_memory_grant_used_memory_kb]
      ,[ideal_memory_kb]
      ,[is_small]
      ,[timeout_sec]
      ,[resource_semaphore_id]
      ,[wait_order]
      ,[wait_time_ms]
      ,[next_candidate_for_memory_grant]
      ,[target_memory_kb]
      ,[max_target_memory_kb]
      ,[total_memory_kb]
      ,[available_memory_kb]
      ,[granted_memory_kb]
      ,[query_resource_semaphore_used_memory_kb]
      ,[grantee_count]
      ,[waiter_count]
      ,[timeout_error_count]
      ,[forced_grant_count]
      ,[workload_group_name]
      ,[resource_pool_name]
      ,[context_info]
      ,[query_hash]
      ,[query_plan_hash]
      ,[sql_handle]
      ,[plan_handle]
      ,[statement_start_offset]
      ,[statement_end_offset]
  FROM '+@FullOutputTableNameBlitzWho+N'
  WHERE [ServerName] = @Servername
  AND ([CheckDate] BETWEEN @StartDate AND @EndDate OR [start_time] BETWEEN CAST(@StartDate AS DATETIME) AND CAST(@EndDate AS DATETIME))
  '
  +CASE
	WHEN @Databasename IS NOT NULL THEN N'AND [database_name] = @Databasename
  '
  	ELSE N''
  END
+N'ORDER BY [CheckDate] ASC
  OPTION (RECOMPILE, MAXDOP '+CAST(@Maxdop AS NVARCHAR(2))+N');';

RAISERROR('Getting BlitzWho info from %s',0,0,@FullOutputTableNameBlitzWho) WITH NOWAIT;

IF (@Debug = 1)
BEGIN 
	PRINT @Sql;
END

IF (OBJECT_ID(@FullOutputTableNameBlitzWho) IS NULL) 
BEGIN
	IF (@OutputTableNameBlitzWho IS NULL)
	BEGIN
		RAISERROR('BlitzWho data skipped',10,0);
		SELECT N'Skipped logged BlitzWho data as NULL was passed to parameter @OutputTableNameBlitzWho';
	END
	ELSE
	BEGIN
		RAISERROR('Table provided for BlitzWho data: %s does not exist',10,0,@FullOutputTableNameBlitzWho);
		SELECT N'No BlitzWho data available as the table cannot be found';
	END
END
ELSE
BEGIN 
	EXEC sp_executesql @Sql,
	N'@StartDate DATETIMEOFFSET(7),
	@EndDate DATETIMEOFFSET(7),
	@Servername NVARCHAR(128),
	@Databasename NVARCHAR(128)',
	@StartDate=@StartDate, 
	@EndDate=@EndDate,
	@Servername=@Servername,
	@Databasename = @Databasename;
END


GO
