IF OBJECT_ID('dbo.sp_BlitzBackups') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzBackups AS RETURN 0;');
GO
ALTER PROCEDURE [dbo].[sp_BlitzBackups]
    @Help TINYINT = 0 ,
	@HoursBack INT = 168,
	@MSDBName NVARCHAR(256) = 'msdb',
	@AGName NVARCHAR(256) = NULL,
	@RestoreSpeedFullMBps INT = NULL,
	@RestoreSpeedDiffMBps INT = NULL,
	@RestoreSpeedLogMBps INT = NULL,
	@Debug TINYINT = 0,
	@PushBackupHistoryToListener BIT = 0,
	@WriteBackupsToListenerName NVARCHAR(256) = NULL,
    @WriteBackupsToDatabaseName NVARCHAR(256) = NULL,
    @WriteBackupsLastHours INT = 168,
    @Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
WITH RECOMPILE
AS
	BEGIN
    SET NOCOUNT ON;
	SET STATISTICS XML OFF;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	SELECT @Version = '8.10', @VersionDate = '20220718';
	
	IF(@VersionCheckMode = 1)
	BEGIN
		RETURN;
	END;

	IF @Help = 1 PRINT '
	/*
	sp_BlitzBackups from http://FirstResponderKit.org
	
	This script checks your backups to see how much data you might lose when
	this server fails, and how long it might take to recover.

	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.

	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)

     Changes - for the full list of improvements and fixes in this version, see:
     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/


	Parameter explanations:

	@HoursBack INT = 168		How many hours of history to examine, back from now.
								You can check just the last 24 hours of backups, for example.
	@MSDBName NVARCHAR(255) 	You can restore MSDB from different servers and check them
								centrally. Also useful if you create a DBA utility database
								and merge data from several servers in an AG into one DB.
	@RestoreSpeedFullMBps INT	By default, we use the backup speed from MSDB to guesstimate
								how fast your restores will go. If you have done performance
								tuning and testing of your backups (or if they horribly go even
								slower in your DR environment, and you want to account for
								that), then you can pass in different numbers here.
	@RestoreSpeedDiffMBps INT	See above.
	@RestoreSpeedLogMBps INT	See above.

	For more documentation: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/

    MIT License
	
	Copyright (c) 2021 Brent Ozar Unlimited

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.




	*/';
ELSE
BEGIN
DECLARE @StringToExecute NVARCHAR(MAX) = N'', 
		@InnerStringToExecute NVARCHAR(MAX) = N'',
		@ProductVersion NVARCHAR(128), 
		@ProductVersionMajor DECIMAL(10, 2),
        @ProductVersionMinor DECIMAL(10, 2), 
		@StartTime DATETIME2, @ResultText NVARCHAR(MAX), 
		@crlf NVARCHAR(2),
        @MoreInfoHeader NVARCHAR(100), 
		@MoreInfoFooter NVARCHAR(100);

IF @HoursBack > 0
    SET @HoursBack = @HoursBack * -1;

IF	@WriteBackupsLastHours > 0
    SET @WriteBackupsLastHours = @WriteBackupsLastHours * -1;

SELECT  @crlf = NCHAR(13) + NCHAR(10), 
		@StartTime = DATEADD(hh, @HoursBack, GETDATE()),
        @MoreInfoHeader = N'<?ClickToSeeDetails -- ' + @crlf, @MoreInfoFooter = @crlf + N' -- ?>';

SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @ProductVersionMajor = SUBSTRING(@ProductVersion, 1, CHARINDEX('.', @ProductVersion) + 1),
       @ProductVersionMinor = PARSENAME(CONVERT(VARCHAR(32), @ProductVersion), 2);

CREATE TABLE #Backups
(
    id INT IDENTITY(1, 1),
    database_name NVARCHAR(128),
    database_guid UNIQUEIDENTIFIER,
    RPOWorstCaseMinutes DECIMAL(18, 1),
    RTOWorstCaseMinutes DECIMAL(18, 1),
    RPOWorstCaseBackupSetID INT,
    RPOWorstCaseBackupSetFinishTime DATETIME,
    RPOWorstCaseBackupSetIDPrior INT,
    RPOWorstCaseBackupSetPriorFinishTime DATETIME,
    RPOWorstCaseMoreInfoQuery XML,
    RTOWorstCaseBackupFileSizeMB DECIMAL(18, 2),
    RTOWorstCaseMoreInfoQuery XML,
    FullMBpsAvg DECIMAL(18, 2),
    FullMBpsMin DECIMAL(18, 2),
    FullMBpsMax DECIMAL(18, 2),
    FullSizeMBAvg DECIMAL(18, 2),
    FullSizeMBMin DECIMAL(18, 2),
    FullSizeMBMax DECIMAL(18, 2),
    FullCompressedSizeMBAvg DECIMAL(18, 2),
    FullCompressedSizeMBMin DECIMAL(18, 2),
    FullCompressedSizeMBMax DECIMAL(18, 2),
    DiffMBpsAvg DECIMAL(18, 2),
    DiffMBpsMin DECIMAL(18, 2),
    DiffMBpsMax DECIMAL(18, 2),
    DiffSizeMBAvg DECIMAL(18, 2),
    DiffSizeMBMin DECIMAL(18, 2),
    DiffSizeMBMax DECIMAL(18, 2),
    DiffCompressedSizeMBAvg DECIMAL(18, 2),
    DiffCompressedSizeMBMin DECIMAL(18, 2),
    DiffCompressedSizeMBMax DECIMAL(18, 2),
    LogMBpsAvg DECIMAL(18, 2),
    LogMBpsMin DECIMAL(18, 2),
    LogMBpsMax DECIMAL(18, 2),
    LogSizeMBAvg DECIMAL(18, 2),
    LogSizeMBMin DECIMAL(18, 2),
    LogSizeMBMax DECIMAL(18, 2),
    LogCompressedSizeMBAvg DECIMAL(18, 2),
    LogCompressedSizeMBMin DECIMAL(18, 2),
    LogCompressedSizeMBMax DECIMAL(18, 2)
);

CREATE TABLE #RTORecoveryPoints
(
    id INT IDENTITY(1, 1),
    database_name NVARCHAR(128),
    database_guid UNIQUEIDENTIFIER,
    rto_worst_case_size_mb AS
        ( COALESCE(log_file_size_mb, 0) + COALESCE(diff_file_size_mb, 0) + COALESCE(full_file_size_mb, 0)),
    rto_worst_case_time_seconds AS
        ( COALESCE(log_time_seconds, 0) + COALESCE(diff_time_seconds, 0) + COALESCE(full_time_seconds, 0)),
    full_backup_set_id INT,
    full_last_lsn NUMERIC(25, 0),
    full_backup_set_uuid UNIQUEIDENTIFIER,
    full_time_seconds BIGINT,
    full_file_size_mb DECIMAL(18, 2),
    diff_backup_set_id INT,
    diff_last_lsn NUMERIC(25, 0),
    diff_time_seconds BIGINT,
    diff_file_size_mb DECIMAL(18, 2),
    log_backup_set_id INT,
    log_last_lsn NUMERIC(25, 0),
    log_time_seconds BIGINT,
    log_file_size_mb DECIMAL(18, 2),
    log_backups INT
);

CREATE TABLE #Recoverability
	(
		Id INT IDENTITY ,
		DatabaseName NVARCHAR(128),
		DatabaseGUID UNIQUEIDENTIFIER,
		LastBackupRecoveryModel NVARCHAR(60),
		FirstFullBackupSizeMB DECIMAL (18,2),
		FirstFullBackupDate DATETIME,
		LastFullBackupSizeMB DECIMAL (18,2),
		LastFullBackupDate DATETIME,
		AvgFullBackupThroughputMB DECIMAL (18,2),
		AvgFullBackupDurationSeconds INT,
		AvgDiffBackupThroughputMB DECIMAL (18,2),
		AvgDiffBackupDurationSeconds INT,
		AvgLogBackupThroughputMB DECIMAL (18,2),
		AvgLogBackupDurationSeconds INT,
		AvgFullSizeMB DECIMAL (18,2),
		AvgDiffSizeMB DECIMAL (18,2),
		AvgLogSizeMB DECIMAL (18,2),
		IsBigDiff AS CASE WHEN (AvgFullSizeMB > 10240. AND ((AvgDiffSizeMB * 100.) / AvgFullSizeMB >= 40.))  THEN 1 ELSE 0 END,
		IsBigLog AS CASE WHEN (AvgFullSizeMB > 10240. AND ((AvgLogSizeMB * 100.) / AvgFullSizeMB >= 20.)) THEN 1 ELSE 0 END
	);

CREATE TABLE #Trending
(
    DatabaseName NVARCHAR(128),
	DatabaseGUID UNIQUEIDENTIFIER,
    [0] DECIMAL(18, 2),
    [-1] DECIMAL(18, 2),
    [-2] DECIMAL(18, 2),
    [-3] DECIMAL(18, 2),
    [-4] DECIMAL(18, 2),
    [-5] DECIMAL(18, 2),
    [-6] DECIMAL(18, 2),
    [-7] DECIMAL(18, 2),
    [-8] DECIMAL(18, 2),
    [-9] DECIMAL(18, 2),
    [-10] DECIMAL(18, 2),
    [-11] DECIMAL(18, 2),
    [-12] DECIMAL(18, 2)
);


CREATE TABLE #Warnings
(
    Id INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
    CheckId INT,
    Priority INT,
    DatabaseName VARCHAR(128),
    Finding VARCHAR(256),
    Warning VARCHAR(8000)
);

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @MSDBName)
	BEGIN
	RAISERROR('@MSDBName was specified, but the database does not exist.', 16, 1) WITH NOWAIT;
	RETURN;
	END

IF @PushBackupHistoryToListener = 1
GOTO PushBackupHistoryToListener


	RAISERROR('Inserting to #Backups', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'WITH Backups AS (SELECT bs.database_name, bs.database_guid, bs.type AS backup_type ' + @crlf
		+ ' , MBpsAvg = CAST(AVG(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMin = CAST(MIN(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMax = CAST(MAX(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , SizeMBAvg = AVG(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMin = MIN(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMax = MAX(backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBAvg = AVG(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMin = MIN(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMax = MAX(compressed_backup_size / 1048576.0) ' + @crlf;


	SET @StringToExecute += N' FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bs ' + @crlf
		+ N' WHERE bs.backup_finish_date >= @StartTime AND bs.is_damaged = 0 ' + @crlf
		+ N' GROUP BY bs.database_name, bs.database_guid, bs.type)' + @crlf;

	SET @StringToExecute += + N'INSERT INTO #Backups(database_name, database_guid, ' + @crlf
		+ N' FullMBpsAvg, FullMBpsMin, FullMBpsMax, FullSizeMBAvg, FullSizeMBMin, FullSizeMBMax, FullCompressedSizeMBAvg, FullCompressedSizeMBMin, FullCompressedSizeMBMax, ' + @crlf
		+ N' DiffMBpsAvg, DiffMBpsMin, DiffMBpsMax, DiffSizeMBAvg, DiffSizeMBMin, DiffSizeMBMax, DiffCompressedSizeMBAvg, DiffCompressedSizeMBMin, DiffCompressedSizeMBMax, ' + @crlf
		+ N' LogMBpsAvg, LogMBpsMin, LogMBpsMax, LogSizeMBAvg, LogSizeMBMin, LogSizeMBMax, LogCompressedSizeMBAvg, LogCompressedSizeMBMin, LogCompressedSizeMBMax ) ' + @crlf
		+ N'SELECT bF.database_name, bF.database_guid ' + @crlf
		+ N' , bF.MBpsAvg AS FullMBpsAvg ' + @crlf
		+ N' , bF.MBpsMin AS FullMBpsMin ' + @crlf
		+ N' , bF.MBpsMax AS FullMBpsMax ' + @crlf
		+ N' , bF.SizeMBAvg AS FullSizeMBAvg ' + @crlf
		+ N' , bF.SizeMBMin AS FullSizeMBMin ' + @crlf
		+ N' , bF.SizeMBMax AS FullSizeMBMax ' + @crlf
		+ N' , bF.CompressedSizeMBAvg AS FullCompressedSizeMBAvg ' + @crlf
		+ N' , bF.CompressedSizeMBMin AS FullCompressedSizeMBMin ' + @crlf
		+ N' , bF.CompressedSizeMBMax AS FullCompressedSizeMBMax ' + @crlf
		+ N' , bD.MBpsAvg AS DiffMBpsAvg ' + @crlf
		+ N' , bD.MBpsMin AS DiffMBpsMin ' + @crlf
		+ N' , bD.MBpsMax AS DiffMBpsMax ' + @crlf
		+ N' , bD.SizeMBAvg AS DiffSizeMBAvg ' + @crlf
		+ N' , bD.SizeMBMin AS DiffSizeMBMin ' + @crlf
		+ N' , bD.SizeMBMax AS DiffSizeMBMax ' + @crlf
		+ N' , bD.CompressedSizeMBAvg AS DiffCompressedSizeMBAvg ' + @crlf
		+ N' , bD.CompressedSizeMBMin AS DiffCompressedSizeMBMin ' + @crlf
		+ N' , bD.CompressedSizeMBMax AS DiffCompressedSizeMBMax ' + @crlf
		+ N' , bL.MBpsAvg AS LogMBpsAvg ' + @crlf
		+ N' , bL.MBpsMin AS LogMBpsMin ' + @crlf
		+ N' , bL.MBpsMax AS LogMBpsMax ' + @crlf
		+ N' , bL.SizeMBAvg AS LogSizeMBAvg ' + @crlf
		+ N' , bL.SizeMBMin AS LogSizeMBMin ' + @crlf
		+ N' , bL.SizeMBMax AS LogSizeMBMax ' + @crlf
		+ N' , bL.CompressedSizeMBAvg AS LogCompressedSizeMBAvg ' + @crlf
		+ N' , bL.CompressedSizeMBMin AS LogCompressedSizeMBMin ' + @crlf
		+ N' , bL.CompressedSizeMBMax AS LogCompressedSizeMBMax ' + @crlf
		+ N' FROM Backups bF ' + @crlf
		+ N' LEFT OUTER JOIN Backups bD ON bF.database_name = bD.database_name AND bF.database_guid = bD.database_guid AND bD.backup_type = ''I''' + @crlf
		+ N' LEFT OUTER JOIN Backups bL ON bF.database_name = bL.database_name AND bF.database_guid = bL.database_guid AND bL.backup_type = ''L''' + @crlf
		+ N' WHERE bF.backup_type = ''D''; ' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;


	RAISERROR('Updating #Backups with worst RPO case', 0, 1) WITH NOWAIT;

	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							 SELECT  bs.database_name, bs.database_guid, bs.backup_set_id, bsPrior.backup_set_id AS backup_set_id_prior,
							         bs.backup_finish_date, bsPrior.backup_finish_date AS backup_finish_date_prior,
							         DATEDIFF(ss, bsPrior.backup_finish_date, bs.backup_finish_date) AS backup_gap_seconds
							 INTO #backup_gaps
							 FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS bs
							 CROSS APPLY ( 
							 	SELECT TOP 1 bs1.backup_set_id, bs1.backup_finish_date
							 	FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS bs1
							 	WHERE bs.database_name = bs1.database_name
							 	        AND bs.database_guid = bs1.database_guid
							 	        AND bs.backup_finish_date > bs1.backup_finish_date
							 			AND bs.backup_set_id > bs1.backup_set_id
							 	ORDER BY bs1.backup_finish_date DESC, bs1.backup_set_id DESC 
							 ) bsPrior
							 WHERE bs.backup_finish_date > @StartTime
							 
							 CREATE CLUSTERED INDEX cx_backup_gaps ON #backup_gaps (database_name, database_guid, backup_set_id, backup_finish_date, backup_gap_seconds);
							 
							 WITH max_gaps AS (
							 SELECT g.database_name, g.database_guid, g.backup_set_id, g.backup_set_id_prior, g.backup_finish_date_prior, 
							        g.backup_finish_date, MAX(g.backup_gap_seconds) AS max_backup_gap_seconds 
							 FROM #backup_gaps AS g
							 GROUP BY g.database_name, g.database_guid, g.backup_set_id, g.backup_set_id_prior, g.backup_finish_date_prior,  g.backup_finish_date
												)
							UPDATE #Backups
								SET   RPOWorstCaseMinutes = bg.max_backup_gap_seconds / 60.0
							        , RPOWorstCaseBackupSetID = bg.backup_set_id
									, RPOWorstCaseBackupSetFinishTime = bg.backup_finish_date
									, RPOWorstCaseBackupSetIDPrior = bg.backup_set_id_prior
									, RPOWorstCaseBackupSetPriorFinishTime = bg.backup_finish_date_prior
								FROM #Backups b
								INNER HASH JOIN max_gaps bg ON b.database_name = bg.database_name AND b.database_guid = bg.database_guid
								LEFT OUTER HASH JOIN max_gaps bgBigger ON bg.database_name = bgBigger.database_name AND bg.database_guid = bgBigger.database_guid AND bg.max_backup_gap_seconds < bgBigger.max_backup_gap_seconds
								WHERE bgBigger.backup_set_id IS NULL;
								';
	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

	RAISERROR('Updating #Backups with worst RPO case queries', 0, 1) WITH NOWAIT;

    UPDATE #Backups
      SET RPOWorstCaseMoreInfoQuery = @MoreInfoHeader + N'SELECT * ' + @crlf
            + N' FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset ' + @crlf
            + N' WHERE database_name = ''' + database_name + ''' ' + @crlf
            + N' AND database_guid = ''' + CAST(database_guid AS NVARCHAR(50)) + ''' ' + @crlf
            + N' AND backup_finish_date >= DATEADD(hh, -2, ''' + CAST(CONVERT(DATETIME, RPOWorstCaseBackupSetPriorFinishTime, 102) AS NVARCHAR(100)) + ''') ' + @crlf
            + N' AND backup_finish_date <= DATEADD(hh, 2, ''' + CAST(CONVERT(DATETIME, RPOWorstCaseBackupSetPriorFinishTime, 102) AS NVARCHAR(100)) + ''') ' + @crlf
            + N' ORDER BY backup_finish_date;'
            + @MoreInfoFooter;


/* RTO */

RAISERROR('Gathering RTO information', 0, 1) WITH NOWAIT;


	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							 INSERT INTO #RTORecoveryPoints(database_name, database_guid, log_last_lsn)
							 SELECT database_name, database_guid, MAX(last_lsn) AS log_last_lsn
							 FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLastLog
							 WHERE type = ''L''
							 AND bLastLog.backup_finish_date >= @StartTime
							 GROUP BY database_name, database_guid;
							';

	IF @Debug = 1
		PRINT @StringToExecute;

		EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

/* Find the most recent full backups for those logs */

RAISERROR('Updating #RTORecoveryPoints', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							 UPDATE #RTORecoveryPoints
							 SET log_backup_set_id = bLasted.backup_set_id
							     ,full_backup_set_id = bLasted.backup_set_id
							     ,full_last_lsn = bLasted.last_lsn
							     ,full_backup_set_uuid = bLasted.backup_set_uuid
							 FROM #RTORecoveryPoints rp
							 		CROSS APPLY (
							 				SELECT TOP 1 bLog.backup_set_id AS backup_set_id_log, bLastFull.backup_set_id, bLastFull.last_lsn, bLastFull.backup_set_uuid, bLastFull.database_guid, bLastFull.database_name
							 				FROM  ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bLog 
							 				INNER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bLastFull 
							 					ON bLog.database_guid = bLastFull.database_guid 
							 					AND bLog.database_name = bLastFull.database_name
							 					AND bLog.first_lsn > bLastFull.last_lsn
							 					AND bLastFull.type = ''D''
							 				WHERE rp.database_guid = bLog.database_guid 
							 					AND rp.database_name = bLog.database_name
							 			) bLasted
							 LEFT OUTER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bLaterFulls ON bLasted.database_guid = bLaterFulls.database_guid AND bLasted.database_name = bLaterFulls.database_name
							     AND bLasted.last_lsn < bLaterFulls.last_lsn
							     AND bLaterFulls.first_lsn < bLasted.last_lsn
							     AND bLaterFulls.type = ''D''
							 WHERE bLaterFulls.backup_set_id IS NULL;
							 ';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

/* Add any full backups in the StartDate range that weren't part of the above log backup chain */

RAISERROR('Add any full backups in the StartDate range that weren''t part of the above log backup chain', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							 INSERT INTO #RTORecoveryPoints(database_name, database_guid, full_backup_set_id, full_last_lsn, full_backup_set_uuid)
							 SELECT bFull.database_name, bFull.database_guid, bFull.backup_set_id, bFull.last_lsn, bFull.backup_set_uuid
							 FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bFull
							 LEFT OUTER JOIN #RTORecoveryPoints rp ON bFull.backup_set_uuid = rp.full_backup_set_uuid
							 WHERE bFull.type = ''D''
							     AND bFull.backup_finish_date IS NOT NULL
							     AND rp.full_backup_set_uuid IS NULL
							     AND bFull.backup_finish_date >= @StartTime;
							';

	IF @Debug = 1
		PRINT @StringToExecute;

		EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

/* Fill out the most recent log for that full, but before the next full */

RAISERROR('Fill out the most recent log for that full, but before the next full', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE rp
						    SET log_last_lsn = (SELECT MAX(last_lsn) FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bLog WHERE bLog.first_lsn >= rp.full_last_lsn AND bLog.first_lsn <= rpNextFull.full_last_lsn AND bLog.type = ''L'')
							FROM #RTORecoveryPoints rp
						    INNER JOIN #RTORecoveryPoints rpNextFull ON rp.database_guid = rpNextFull.database_guid AND rp.database_name = rpNextFull.database_name
						        AND rp.full_last_lsn < rpNextFull.full_last_lsn
						    LEFT OUTER JOIN #RTORecoveryPoints rpEarlierFull ON rp.database_guid = rpEarlierFull.database_guid AND rp.database_name = rpEarlierFull.database_name
						        AND rp.full_last_lsn < rpEarlierFull.full_last_lsn
						        AND rpNextFull.full_last_lsn > rpEarlierFull.full_last_lsn
						    WHERE rpEarlierFull.full_backup_set_id IS NULL;
							';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

/* Fill out a diff in that range */

RAISERROR('Fill out a diff in that range', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE #RTORecoveryPoints
							SET diff_last_lsn = (SELECT TOP 1 bDiff.last_lsn FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bDiff
							                        WHERE rp.database_guid = bDiff.database_guid AND rp.database_name = bDiff.database_name
							                            AND bDiff.type = ''I''
							                            AND bDiff.last_lsn < rp.log_last_lsn
							                            AND rp.full_backup_set_uuid = bDiff.differential_base_guid
							                            ORDER BY bDiff.last_lsn DESC)
							FROM #RTORecoveryPoints rp
							WHERE diff_last_lsn IS NULL;
							';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

/* Get time & size totals for full & diff */

RAISERROR('Get time & size totals for full & diff', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE #RTORecoveryPoints
							    SET full_time_seconds = DATEDIFF(ss,bFull.backup_start_date, bFull.backup_finish_date)
							    , full_file_size_mb = bFull.backup_size / 1048576.0
							    , diff_backup_set_id = bDiff.backup_set_id
							    , diff_time_seconds = DATEDIFF(ss,bDiff.backup_start_date, bDiff.backup_finish_date)
							    , diff_file_size_mb = bDiff.backup_size / 1048576.0
							FROM #RTORecoveryPoints rp
							INNER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bFull ON rp.database_guid = bFull.database_guid AND rp.database_name = bFull.database_name AND rp.full_last_lsn = bFull.last_lsn
							LEFT OUTER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bDiff ON rp.database_guid = bDiff.database_guid AND rp.database_name = bDiff.database_name AND rp.diff_last_lsn = bDiff.last_lsn AND bDiff.last_lsn IS NOT NULL;
							';

	IF @Debug = 1
		PRINT @StringToExecute;
	
	EXEC sys.sp_executesql @StringToExecute;


/* Get time & size totals for logs */

RAISERROR('Get time & size totals for logs', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							WITH LogTotals AS (
								 SELECT rp.id, log_time_seconds = SUM(DATEDIFF(ss,bLog.backup_start_date, bLog.backup_finish_date))
								    , log_file_size = SUM(bLog.backup_size)
								    , SUM(1) AS log_backups
								        FROM #RTORecoveryPoints rp
								            INNER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupset bLog ON rp.database_guid = bLog.database_guid AND rp.database_name = bLog.database_name AND bLog.type = ''L''
								            AND bLog.first_lsn > COALESCE(rp.diff_last_lsn, rp.full_last_lsn)
								            AND bLog.first_lsn <= rp.log_last_lsn
								        GROUP BY rp.id
								)
								UPDATE #RTORecoveryPoints
								    SET log_time_seconds = lt.log_time_seconds
								    , log_file_size_mb = lt.log_file_size / 1048576.0
								    , log_backups = lt.log_backups
								FROM #RTORecoveryPoints rp
								    INNER JOIN LogTotals lt ON rp.id = lt.id;
									';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

RAISERROR('Gathering RTO worst cases', 0, 1) WITH NOWAIT;

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							WITH WorstCases AS (
								SELECT rp.*
								  FROM #RTORecoveryPoints rp
								    LEFT OUTER JOIN #RTORecoveryPoints rpNewer 
										ON rp.database_guid = rpNewer.database_guid 
										AND rp.database_name = rpNewer.database_name 
										AND rp.full_last_lsn < rpNewer.full_last_lsn 
										AND rpNewer.rto_worst_case_size_mb = (SELECT TOP 1 rto_worst_case_size_mb FROM #RTORecoveryPoints s	WHERE rp.database_guid = s.database_guid AND rp.database_name = s.database_name ORDER BY rto_worst_case_size_mb DESC)
								  WHERE rp.rto_worst_case_size_mb = (SELECT TOP 1 rto_worst_case_size_mb FROM #RTORecoveryPoints s WHERE rp.database_guid = s.database_guid AND rp.database_name = s.database_name ORDER BY rto_worst_case_size_mb DESC)
								    /* OR  rp.rto_worst_case_time_seconds = (SELECT TOP 1 rto_worst_case_time_seconds FROM #RTORecoveryPoints s WHERE rp.database_guid = s.database_guid AND rp.database_name = s.database_name ORDER BY rto_worst_case_time_seconds DESC) */
								  AND rpNewer.database_guid IS NULL
								)
								UPDATE #Backups
										SET RTOWorstCaseMinutes = 
                                                                    /* Fulls */
                                                                    (CASE WHEN @RestoreSpeedFullMBps IS NULL 
																	   THEN wc.full_time_seconds / 60.0
																	   ELSE @RestoreSpeedFullMBps / wc.full_file_size_mb
																	   END)

                                                                    /* Diffs, which might not have been taken */
                                                                    + (CASE WHEN @RestoreSpeedDiffMBps IS NOT NULL AND wc.diff_file_size_mb IS NOT NULL
                                                                        THEN @RestoreSpeedDiffMBps / wc.diff_file_size_mb
                                                                        ELSE COALESCE(wc.diff_time_seconds,0) / 60.0
                                                                        END)

                                                                    /* Logs, which might not have been taken */
                                                                    + (CASE WHEN @RestoreSpeedLogMBps IS NOT NULL AND wc.log_file_size_mb IS NOT NULL
                                                                        THEN @RestoreSpeedLogMBps / wc.log_file_size_mb
                                                                        ELSE COALESCE(wc.log_time_seconds,0) / 60.0
                                                                        END)
								        , RTOWorstCaseBackupFileSizeMB = wc.rto_worst_case_size_mb
								FROM #Backups b
								INNER JOIN WorstCases wc 
								ON b.database_guid = wc.database_guid 
									AND b.database_name = wc.database_name;
								';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@RestoreSpeedFullMBps INT, @RestoreSpeedDiffMBps INT, @RestoreSpeedLogMBps INT', @RestoreSpeedFullMBps, @RestoreSpeedDiffMBps, @RestoreSpeedLogMBps;



/*Populating Recoverability*/


	/*Get distinct list of databases*/
	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							 SELECT DISTINCT b.database_name, database_guid
							 FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b;'

	IF @Debug = 1
		PRINT @StringToExecute;

	INSERT #Recoverability ( DatabaseName, DatabaseGUID )
	EXEC sys.sp_executesql @StringToExecute;


	/*Find most recent recovery model, backup size, and backup date*/
	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 	N'
								UPDATE r
								SET r.LastBackupRecoveryModel = ca.recovery_model,
									r.LastFullBackupSizeMB = ca.compressed_backup_size,
									r.LastFullBackupDate = ca.backup_finish_date
								FROM #Recoverability r
									CROSS APPLY (
										SELECT TOP 1 b.recovery_model, (b.compressed_backup_size / 1048576.0) AS compressed_backup_size, b.backup_finish_date
										FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
										WHERE r.DatabaseName = b.database_name
										AND r.DatabaseGUID = b.database_guid
										AND b.type = ''D''
										AND b.backup_finish_date > @StartTime
										ORDER BY b.backup_finish_date DESC
												) ca;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

	/*Find first backup size and date*/
	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE r
							SET r.FirstFullBackupSizeMB = ca.compressed_backup_size,
								r.FirstFullBackupDate = ca.backup_finish_date
							FROM #Recoverability r
								CROSS APPLY (
									SELECT TOP 1 (b.compressed_backup_size / 1048576.0) AS compressed_backup_size, b.backup_finish_date
									FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
									WHERE r.DatabaseName = b.database_name 
									AND r.DatabaseGUID = b.database_guid
									AND b.type = ''D''
									AND b.backup_finish_date > @StartTime
									ORDER BY b.backup_finish_date ASC
											) ca;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;


	/*Find average backup throughputs for full, diff, and log*/
	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE r
							SET r.AvgFullBackupThroughputMB = ca_full.AvgFullSpeed,
								r.AvgDiffBackupThroughputMB = ca_diff.AvgDiffSpeed,
								r.AvgLogBackupThroughputMB = ca_log.AvgLogSpeed,
								r.AvgFullBackupDurationSeconds = AvgFullDuration,
								r.AvgDiffBackupDurationSeconds = AvgDiffDuration,
								r.AvgLogBackupDurationSeconds = AvgLogDuration
							FROM #Recoverability AS r
							OUTER APPLY (
								SELECT b.database_name, 
									   AVG( b.compressed_backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576.0 ) AS AvgFullSpeed,
									   AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS AvgFullDuration
								FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
								AND b.type = ''D'' 
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								AND b.backup_finish_date > @StartTime
								GROUP BY b.database_name
										) ca_full
							OUTER APPLY (
								SELECT b.database_name, 
									   AVG( b.compressed_backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576.0 ) AS AvgDiffSpeed,
									   AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS AvgDiffDuration
								FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
								AND b.type = ''I'' 
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								AND b.backup_finish_date > @StartTime
								GROUP BY b.database_name
										) ca_diff
							OUTER APPLY (
								SELECT b.database_name, 
									   AVG( b.compressed_backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576.0 ) AS AvgLogSpeed,
									   AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS AvgLogDuration
								FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
								AND b.type = ''L''
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								AND b.backup_finish_date > @StartTime
								GROUP BY b.database_name
										) ca_log;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;


	/*Find max and avg diff and log sizes*/
	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							UPDATE r
							 SET r.AvgFullSizeMB = fulls.avg_full_size,
							 	 r.AvgDiffSizeMB = diffs.avg_diff_size,
							 	 r.AvgLogSizeMB = logs.avg_log_size
							 FROM #Recoverability AS r
							 OUTER APPLY (
							 	SELECT b.database_name, AVG(b.compressed_backup_size / 1048576.0) AS avg_full_size
							 	FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							 	WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
							 	AND b.type = ''D''
								AND b.backup_finish_date > @StartTime
							 	GROUP BY b.database_name
							 			) AS fulls
							 OUTER APPLY (
							 	SELECT b.database_name, AVG(b.compressed_backup_size / 1048576.0) AS avg_diff_size
							 	FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							 	WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
							 	AND b.type = ''I''
								AND b.backup_finish_date > @StartTime
							 	GROUP BY b.database_name
							 			) AS diffs
							 OUTER APPLY (
							 	SELECT b.database_name, AVG(b.compressed_backup_size / 1048576.0) AS avg_log_size
							 	FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							 	WHERE r.DatabaseName = b.database_name
								AND r.DatabaseGUID = b.database_guid
							 	AND b.type = ''L''
								AND b.backup_finish_date > @StartTime
							 	GROUP BY b.database_name
							 			) AS logs;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;
	
/*Trending - only works if backupfile is populated, which means in msdb */
IF @MSDBName = N'msdb'
BEGIN
	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' --+ @crlf;

	SET @StringToExecute += N'
							SELECT  p.DatabaseName,
									p.DatabaseGUID,
									p.[0],
									p.[-1],
									p.[-2],
									p.[-3],
									p.[-4],
									p.[-5],
									p.[-6],
									p.[-7],
									p.[-8],
									p.[-9],
									p.[-10],
									p.[-11],
									p.[-12]
								FROM ( SELECT b.database_name AS DatabaseName,
											  b.database_guid AS DatabaseGUID,
											  DATEDIFF(MONTH, @StartTime, b.backup_start_date) AS MonthsAgo ,
											  CONVERT(DECIMAL(18, 2), AVG(bf.file_size / 1048576.0)) AS AvgSizeMB
										FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b 
										INNER JOIN ' + QUOTENAME(@MSDBName) + N'.dbo.backupfile AS bf
										ON b.backup_set_id = bf.backup_set_id
										WHERE b.database_name NOT IN ( ''master'', ''msdb'', ''model'', ''tempdb'' )
										AND bf.file_type = ''D''
										AND b.backup_start_date >= DATEADD(YEAR, -1, @StartTime)
										AND b.backup_start_date <= SYSDATETIME()
										GROUP BY b.database_name,
												 b.database_guid,	
												 DATEDIFF(mm, @StartTime, b.backup_start_date)
									 ) AS bckstat PIVOT ( SUM(bckstat.AvgSizeMB) FOR bckstat.MonthsAgo IN ( [0], [-1], [-2], [-3], [-4], [-5], [-6], [-7], [-8], [-9], [-10], [-11], [-12] ) ) AS p
								ORDER BY p.DatabaseName;
								'

		IF @Debug = 1
			PRINT @StringToExecute;

		INSERT #Trending ( DatabaseName, DatabaseGUID, [0], [-1], [-2], [-3], [-4], [-5], [-6], [-7], [-8], [-9], [-10], [-11], [-12] )
		EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

END

/*End Trending*/

/*End populating Recoverability*/

RAISERROR('Returning data', 0, 1) WITH NOWAIT;

	SELECT   b.*
		FROM     #Backups AS b
		ORDER BY b.database_name;

	SELECT   r.*,
             t.[0], t.[-1], t.[-2], t.[-3], t.[-4], t.[-5], t.[-6], t.[-7], t.[-8], t.[-9], t.[-10], t.[-11], t.[-12]
		FROM #Recoverability AS r
		LEFT JOIN #Trending t
		ON r.DatabaseName = t.DatabaseName
		AND r.DatabaseGUID = t.DatabaseGUID	
		WHERE r.LastBackupRecoveryModel IS NOT NULL
		ORDER BY r.DatabaseName


RAISERROR('Rules analysis starting', 0, 1) WITH NOWAIT;

/*Looking for out of band backups by finding most common backup operator user_name and noting backups taken by other user_names*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'
							WITH common_people AS (
									SELECT TOP 1 b.user_name, COUNT_BIG(*) AS Records
									FROM ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
									GROUP BY b.user_name
									ORDER BY Records DESC
													)								
								SELECT 
								1 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Non-Agent backups taken'' AS [Finding], 
								''The database '' + QUOTENAME(b.database_name) + '' has been backed up by '' + QUOTENAME(b.user_name) + '' '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE  b.user_name NOT LIKE ''%Agent%'' AND b.user_name NOT LIKE ''%AGENT%'' 
							AND NOT EXISTS (
											SELECT 1
											FROM common_people AS cp
											WHERE cp.user_name = b.user_name
											)
							GROUP BY b.database_name, b.user_name
							HAVING COUNT(*) > 1;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings (CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for compatibility level changing. Only looking for databases that have changed more than twice (It''s possible someone may have changed up, had CE problems, and then changed back)*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								2 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Compatibility level changing'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has changed compatibility levels '' + CONVERT(VARCHAR(10), COUNT(DISTINCT b.compatibility_level)) + '' times.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							GROUP BY b.database_name
							HAVING COUNT(DISTINCT b.compatibility_level) > 2;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for password protected backups. This hasn''t been a popular option ever, and was largely replaced by encrypted backups, but it''s simple to check for.*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								3 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Password backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has been backed up with a password '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times. Who has the password?'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.is_password_protected = 1
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for snapshot backups. There are legit reasons for these, but we should flag them so the questions get asked. What questions? Good question.*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								4 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Snapshot backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' snapshot backups. This message is purely informational.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.is_snapshot = 1
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*It''s fine to take backups of read only databases, but it''s not always necessary (there''s no new data, after all).*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								5 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Read only state backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times while in a read-only state. This can be normal if it''''s a secondary, but a bit odd otherwise.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.is_readonly = 1
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*So, I''ve come across people who think they need to change their database to single user mode to take a backup. Or that doing that will help something. I just need to know, here.*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								6 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Single user mode backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times while in single-user mode. This is really weird! Make sure your backup process doesn''''t include a mode change anywhere.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.is_single_user = 1
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*C''mon, it''s 2017. Take your backups with CHECKSUMS, people.*/

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								7 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''No CHECKSUMS'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times without CHECKSUMS in the past 30 days. CHECKSUMS can help alert you to corruption errors.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.has_backup_checksums = 0
							AND b.backup_finish_date >= DATEADD(DAY, -30, SYSDATETIME())
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Damaged is a Black Flag album. You don''t want your backups to be like a Black Flag album. */

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								8 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Damaged backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' damaged backups taken without stopping to throw an error. This is done by specifying CONTINUE_AFTER_ERROR in your BACKUP commands.'' AS [Warning]
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.is_damaged = 1
							GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Checking for encrypted backups and the last backup of the encryption key.*/

	/*2014 ONLY*/

IF @ProductVersionMajor >= 12
	BEGIN

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
								9 AS CheckId,
								100 AS [Priority],
								b.database_name AS [Database Name],
								''Encrypted backups'' AS [Finding],
								''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' '' + b.encryptor_type + '' backups, and the last time a certificate was backed up is '
								+ CASE WHEN LOWER(@MSDBName) <> N'msdb'
									THEN + N'...well, that information is on another server, anyway.'' AS [Warning]'
									ELSE + CONVERT(VARCHAR(30), (SELECT MAX(c.pvt_key_last_backup_date) FROM sys.certificates AS c WHERE c.name NOT LIKE '##%')) + N'.'' AS [Warning]'
									END + 
							N'
							FROM   ' + QUOTENAME(@MSDBName) + N'.dbo.backupset AS b
							WHERE b.encryptor_type IS NOT NULL
							GROUP BY b.database_name, b.encryptor_type;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	END
	
	/*Looking for backups that have BULK LOGGED data in them -- this can screw up point in time LOG recovery.*/

	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
		10 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Bulk logged backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' backups with bulk logged data. This can make point in time recovery awkward. '' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.has_bulk_logged_data = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for recovery model being switched between FULL and SIMPLE, because it''s a bad practice.*/

	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
		11 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Recovery model switched'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has changed recovery models from between FULL and SIMPLE '' + CONVERT(VARCHAR(10), COUNT(DISTINCT b.recovery_model)) + '' times. This breaks the log chain and is generally a bad idea.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.recovery_model <> ''BULK-LOGGED''
	GROUP BY b.database_name
	HAVING COUNT(DISTINCT b.recovery_model) > 4;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;

	/*Looking for uncompressed backups.*/

	SET @StringToExecute =N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT 
		12 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Uncompressed backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' uncompressed backups in the last 30 days. This is a free way to save time and space. And SPACETIME. If your version of SQL supports it.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE backup_size = compressed_backup_size AND type = ''D''
	AND b.backup_finish_date >= DATEADD(DAY, -30, SYSDATETIME())
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;

RAISERROR('Rules analysis starting on temp tables', 0, 1) WITH NOWAIT;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		SELECT
			13 AS CheckId,
			100 AS Priority,
			r.DatabaseName as [DatabaseName],
			'Big Diffs' AS [Finding],
			'On average, Differential backups for this database are >=40% of the size of the average Full backup.' AS [Warning]
			FROM #Recoverability AS r
			WHERE r.IsBigDiff = 1

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		SELECT
			13 AS CheckId,
			100 AS Priority,
			r.DatabaseName as [DatabaseName],
			'Big Logs' AS [Finding],
			'On average, Log backups for this database are >=20% of the size of the average Full backup.' AS [Warning]
			FROM #Recoverability AS r
			WHERE r.IsBigLog = 1



/*Insert thank you stuff last*/
		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )

		SELECT
		2147483647 AS [CheckId],
		2147483647 AS [Priority],
		'From Your Community Volunteers' AS [DatabaseName],
		'sp_BlitzBackups Version: ' + @Version + ', Version Date: ' + CONVERT(VARCHAR(30), @VersionDate) + '.' AS [Finding],
		'Thanks for using our stored procedure. We hope you find it useful! Check out our other free SQL Server scripts at firstresponderkit.org!' AS [Warning];

RAISERROR('Rules analysis finished', 0, 1) WITH NOWAIT;

SELECT w.CheckId, w.Priority, w.DatabaseName, w.Finding, w.Warning
FROM #Warnings AS w
ORDER BY w.Priority, w.CheckId;

DROP TABLE #Backups, #Warnings, #Recoverability, #RTORecoveryPoints


RETURN;

PushBackupHistoryToListener:

RAISERROR('Pushing backup history to listener', 0, 1) WITH NOWAIT;

DECLARE @msg NVARCHAR(4000) = N'';
DECLARE @RemoteCheck TABLE (c INT NULL);


IF @WriteBackupsToDatabaseName IS NULL
	BEGIN
	RAISERROR('@WriteBackupsToDatabaseName can''t be NULL.', 16, 1) WITH NOWAIT
	RETURN;
	END

IF LOWER(@WriteBackupsToDatabaseName) = N'msdb'
	BEGIN
	RAISERROR('We can''t write to the real msdb, we have to write to a fake msdb.', 16, 1) WITH NOWAIT
	RETURN;
	END

IF @WriteBackupsToListenerName IS NULL
BEGIN
	IF @AGName IS NULL
		BEGIN
			RAISERROR('@WriteBackupsToListenerName and @AGName can''t both be NULL.', 16, 1) WITH NOWAIT;
			RETURN;
		END
	ELSE
		BEGIN	
			SELECT @WriteBackupsToListenerName =  dns_name
			FROM sys.availability_groups AS ag
			JOIN sys.availability_group_listeners AS agl
			ON ag.group_id = agl.group_id
			WHERE name = @AGName;
		END

END

IF @WriteBackupsToListenerName IS NOT NULL
BEGIN
	IF NOT EXISTS 
		(
			SELECT *
			FROM sys.servers s
			WHERE name = @WriteBackupsToListenerName
		)
			BEGIN
				SET @msg = N'We need a linked server to write data across. Please set one up for ' + @WriteBackupsToListenerName + N'.';
				RAISERROR(@msg, 16, 1) WITH NOWAIT;
				RETURN;
			END
END

	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT TOP 1 1 FROM ' 
							+ QUOTENAME(@WriteBackupsToListenerName) + N'.master.sys.databases d WHERE d.name = @i_WriteBackupsToDatabaseName;'

	IF @Debug = 1
		PRINT @StringToExecute;

	INSERT @RemoteCheck (c)
	EXEC sp_executesql @StringToExecute, N'@i_WriteBackupsToDatabaseName NVARCHAR(256)', @i_WriteBackupsToDatabaseName = @WriteBackupsToDatabaseName;

	IF @@ROWCOUNT = 0
		BEGIN
		SET @msg = N'The database ' + @WriteBackupsToDatabaseName + N' doesn''t appear to exist on that server.'
		RAISERROR(@msg, 16, 1) WITH NOWAIT
		RETURN;
		END


	SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += N'SELECT TOP 1 1 FROM ' 
							+ QUOTENAME(@WriteBackupsToListenerName) + '.' + QUOTENAME(@WriteBackupsToDatabaseName) + '.sys.tables WHERE name = ''backupset'' AND SCHEMA_NAME(schema_id) = ''dbo'';
							' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

	INSERT @RemoteCheck (c)
	EXEC sp_executesql @StringToExecute;

	IF @@ROWCOUNT = 0
		BEGIN

		SET @msg = N'The database ' + @WriteBackupsToDatabaseName + N' doesn''t appear to have a table called dbo.backupset in it.'
		RAISERROR(@msg, 0, 1) WITH NOWAIT
		RAISERROR('Don''t worry, we''ll create it for you!', 0, 1) WITH NOWAIT
		
		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

		SET @StringToExecute += N'CREATE TABLE ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.dbo.backupset
											( backup_set_id INT IDENTITY(1, 1), backup_set_uuid UNIQUEIDENTIFIER, media_set_id INT, first_family_number TINYINT, first_media_number SMALLINT, 
											  last_family_number TINYINT, last_media_number SMALLINT, catalog_family_number TINYINT, catalog_media_number SMALLINT, position INT, expiration_date DATETIME, 
											  software_vendor_id INT, name NVARCHAR(128), description NVARCHAR(255), user_name NVARCHAR(128), software_major_version TINYINT, software_minor_version TINYINT, 
											  software_build_version SMALLINT, time_zone SMALLINT, mtf_minor_version TINYINT, first_lsn NUMERIC(25, 0), last_lsn NUMERIC(25, 0), checkpoint_lsn NUMERIC(25, 0), 
											  database_backup_lsn NUMERIC(25, 0), database_creation_date DATETIME, backup_start_date DATETIME, backup_finish_date DATETIME, type CHAR(1), sort_order SMALLINT, 
											  code_page SMALLINT, compatibility_level TINYINT, database_version INT, backup_size NUMERIC(20, 0), database_name NVARCHAR(128), server_name NVARCHAR(128), 
											  machine_name NVARCHAR(128), flags INT, unicode_locale INT, unicode_compare_style INT, collation_name NVARCHAR(128), is_password_protected BIT, recovery_model NVARCHAR(60), 
											  has_bulk_logged_data BIT, is_snapshot BIT, is_readonly BIT, is_single_user BIT, has_backup_checksums BIT, is_damaged BIT, begins_log_chain BIT, has_incomplete_metadata BIT, 
											  is_force_offline BIT, is_copy_only BIT, first_recovery_fork_guid UNIQUEIDENTIFIER, last_recovery_fork_guid UNIQUEIDENTIFIER, fork_point_lsn NUMERIC(25, 0), database_guid UNIQUEIDENTIFIER, 
											  family_guid UNIQUEIDENTIFIER, differential_base_lsn NUMERIC(25, 0), differential_base_guid UNIQUEIDENTIFIER, compressed_backup_size NUMERIC(20, 0), key_algorithm NVARCHAR(32), 
											  encryptor_thumbprint VARBINARY(20) , encryptor_type NVARCHAR(32) 
											);
								 ' + @crlf;
		
		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute


		RAISERROR('We''ll even make the indexes!', 0, 1) WITH NOWAIT

		/*Checking for and creating the PK/CX*/

		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;
		
		SET @StringToExecute += N'
		
		IF NOT EXISTS (
		SELECT t.name, i.name
		FROM ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.tables AS t
		JOIN ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.indexes AS i  
		ON t.object_id = i.object_id
		WHERE t.name = ?
		AND i.name LIKE ?
		)

		BEGIN
		ALTER TABLE ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.[dbo].[backupset] ADD PRIMARY KEY CLUSTERED ([backup_set_id] ASC)
		END
		'

		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''', ''backupset'', ''PK[_][_]%'' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute



		/*Checking for and creating index on backup_set_uuid*/

		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;
		
		SET @StringToExecute += N'IF NOT EXISTS (
		SELECT t.name, i.name
		FROM ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.tables AS t
		JOIN ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.indexes AS i  
		ON t.object_id = i.object_id
		WHERE t.name = ?
		AND i.name = ?
		)

		BEGIN
		CREATE NONCLUSTERED INDEX [backupsetuuid] ON ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.[dbo].[backupset] ([backup_set_uuid] ASC)
		END
		'

		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''', ''backupset'', ''backupsetuuid'' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute		
		
		
		
		/*Checking for and creating index on media_set_id*/

		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;
		
		SET @StringToExecute += 'IF NOT EXISTS (
		SELECT t.name, i.name
		FROM ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.tables AS t
		JOIN ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.indexes AS i  
		ON t.object_id = i.object_id
		WHERE t.name = ?
		AND i.name = ?
		)

		BEGIN
		CREATE NONCLUSTERED INDEX [backupsetMediaSetId] ON ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.[dbo].[backupset] ([media_set_id] ASC)
		END
		'

		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''', ''backupset'', ''backupsetMediaSetId'' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute
		
				
				
		/*Checking for and creating index on backup_finish_date*/

		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;
		
		SET @StringToExecute += N'IF NOT EXISTS (
		SELECT t.name, i.name
		FROM ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.tables AS t
		JOIN ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.indexes AS i  
		ON t.object_id = i.object_id
		WHERE t.name = ?
		AND i.name = ?
		)

		BEGIN
		CREATE NONCLUSTERED INDEX [backupsetDate] ON ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.[dbo].[backupset] ([backup_finish_date] ASC)
		END
		'

		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''', ''backupset'', ''backupsetDate'' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute


			
		/*Checking for and creating index on database_name*/

		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;
		
		SET @StringToExecute += N'IF NOT EXISTS (
		SELECT t.name, i.name
		FROM ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.tables AS t
		JOIN ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.sys.indexes AS i  
		ON t.object_id = i.object_id
		WHERE t.name = ?
		AND i.name = ?
		)

		BEGIN				
		CREATE NONCLUSTERED INDEX [backupsetDatabaseName] ON ' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.[dbo].[backupset] ([database_name] ASC ) INCLUDE ([backup_set_id], [media_set_id]) 
		END

		'

		SET @InnerStringToExecute = N'EXEC( ''' + @StringToExecute +  ''', ''backupset'', ''backupsetDatabaseName'' ) AT ' + QUOTENAME(@WriteBackupsToListenerName) + N';'
	
	IF @Debug = 1
		PRINT @InnerStringToExecute;		
		
		EXEC sp_executesql @InnerStringToExecute

		RAISERROR('Table and indexes created! You''re welcome!', 0, 1) WITH NOWAIT
		END


		RAISERROR('Beginning inserts', 0, 1) WITH NOWAIT;
		RAISERROR(@crlf, 0, 1) WITH NOWAIT;

		/*
		Batching code comes from the lovely and talented Michael J. Swart
		http://michaeljswart.com/2014/09/take-care-when-scripting-batches/
		If you're ever in Canada, he says you can stay at his house, too.
		*/


		SET @StringToExecute = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

		SET @StringToExecute += N'	
									DECLARE
										@StartDate DATETIME = DATEADD(HOUR, @i_WriteBackupsLastHours, SYSDATETIME()),
										@StartDateNext DATETIME,
										@RC INT = 1,
										@msg NVARCHAR(4000) = N'''';
									
									SELECT @StartDate = MIN(b.backup_start_date)
									FROM msdb.dbo.backupset b
									WHERE b.backup_start_date >= @StartDate
									AND  NOT EXISTS (
													SELECT 1 
													FROM ' + QUOTENAME(@WriteBackupsToListenerName) + N'.' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.dbo.backupset b2
													WHERE b.backup_set_uuid = b2.backup_set_uuid
													AND b2.backup_start_date >= @StartDate
													)

									SET @StartDateNext = DATEADD(MINUTE, 10, @StartDate);

								 IF
									( @StartDate IS NULL )
										BEGIN
											SET @msg = N''No data to move, exiting.''
											RAISERROR(@msg, 0, 1) WITH NOWAIT	

											RETURN;
										END

									RAISERROR(''Starting insert loop'', 0, 1) WITH NOWAIT;

									WHILE EXISTS (
												SELECT 1
												FROM msdb.dbo.backupset b
												WHERE NOT EXISTS (
													SELECT 1 
													FROM ' + QUOTENAME(@WriteBackupsToListenerName) + N'.' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.dbo.backupset b2
													WHERE b.backup_set_uuid = b2.backup_set_uuid
													AND b2.backup_start_date >= @StartDate
																)
												)
									BEGIN
										
									SET @msg = N''Inserting data for '' + CONVERT(NVARCHAR(30), @StartDate) + '' through '' +  + CONVERT(NVARCHAR(30), @StartDateNext) + ''.''
									RAISERROR(@msg, 0, 1) WITH NOWAIT																			
										
										'

		SET @StringToExecute += N'INSERT ' + QUOTENAME(@WriteBackupsToListenerName) + N'.' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.dbo.backupset
									' 
		SET @StringToExecute += N' (database_name, database_guid, backup_set_uuid, type, backup_size, backup_start_date, backup_finish_date, media_set_id, time_zone, 
									compressed_backup_size, recovery_model, server_name, machine_name, first_lsn, last_lsn, user_name, compatibility_level, 
									is_password_protected, is_snapshot, is_readonly, is_single_user, has_backup_checksums, is_damaged, ' + CASE WHEN @ProductVersionMajor >= 12 
																																				THEN + N'encryptor_type, has_bulk_logged_data)' + @crlf
																																				ELSE + N'has_bulk_logged_data)' + @crlf
																																				END
		
		SET @StringToExecute +=N'
									SELECT database_name, database_guid, backup_set_uuid, type, backup_size, backup_start_date, backup_finish_date, media_set_id, time_zone, 
									compressed_backup_size, recovery_model, server_name, machine_name, first_lsn, last_lsn, user_name, compatibility_level, 
									is_password_protected, is_snapshot, is_readonly, is_single_user, has_backup_checksums, is_damaged, ' + CASE WHEN @ProductVersionMajor >= 12 
																																				THEN + N'encryptor_type, has_bulk_logged_data' + @crlf
																																				ELSE + N'has_bulk_logged_data' + @crlf
																																				END
		SET @StringToExecute +=N'
								 FROM msdb.dbo.backupset b
								 WHERE 1=1
								 AND b.backup_start_date >= @StartDate
								 AND b.backup_start_date < @StartDateNext
								 AND NOT EXISTS (
										SELECT 1 
										FROM ' + QUOTENAME(@WriteBackupsToListenerName) + N'.' + QUOTENAME(@WriteBackupsToDatabaseName) + N'.dbo.backupset b2
										WHERE b.backup_set_uuid = b2.backup_set_uuid
										AND b2.backup_start_date >= @StartDate
													)'  + @crlf;


		SET @StringToExecute +=N'
								 SET @RC = @@ROWCOUNT;
								 								
								SET @msg = N''Inserted '' + CONVERT(NVARCHAR(30), @RC) + '' rows for ''+ CONVERT(NVARCHAR(30), @StartDate) + '' through '' + CONVERT(NVARCHAR(30), @StartDateNext) + ''.''
								RAISERROR(@msg, 0, 1) WITH NOWAIT
								 
								 SET @StartDate = @StartDateNext;
								 SET @StartDateNext = DATEADD(MINUTE, 10, @StartDate);

								 IF
									( @StartDate > SYSDATETIME() )
										BEGIN
											
											SET @msg = N''No more data to move, exiting.''
											RAISERROR(@msg, 0, 1) WITH NOWAIT	
											
											BREAK;

										END
								 END'  + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sp_executesql @StringToExecute, N'@i_WriteBackupsLastHours INT', @i_WriteBackupsLastHours = @WriteBackupsLastHours;

END;

END;

GO
