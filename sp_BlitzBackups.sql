IF OBJECT_ID('dbo.sp_BlitzBackups') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzBackups AS RETURN 0;');
GO
ALTER PROCEDURE [dbo].[sp_BlitzBackups]
    @Help TINYINT = 0 ,
	@HoursBack INT = 168,
	@MSDBName NVARCHAR(255) = 'msdb',
	@RestoreSpeedFullMBps INT = NULL,
	@RestoreSpeedDiffMBps INT = NULL,
	@RestoreSpeedLogMBps INT = NULL,
	@Debug TINYINT = 0,
    @VersionDate DATE = NULL OUTPUT
WITH RECOMPILE
AS
	BEGIN
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	DECLARE @Version VARCHAR(30);
	SET @Version = '5.3';
	SET @VersionDate = '20170501';

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
	
	Copyright (c) 2017 Brent Ozar Unlimited

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
DECLARE @StringToExecute NVARCHAR(MAX), 
		@ProductVersion NVARCHAR(128), 
		@ProductVersionMajor DECIMAL(10, 2),
        @ProductVersionMinor DECIMAL(10, 2), 
		@StartTime DATETIME2, @ResultText NVARCHAR(MAX), 
		@crlf NVARCHAR(2),
        @MoreInfoHeader NVARCHAR(100), 
		@MoreInfoFooter NVARCHAR(100);

IF @HoursBack > 0
    SET @HoursBack = @HoursBack * -1;

SELECT  @crlf = NCHAR(13) + NCHAR(10), 
		@StartTime = DATEADD(hh, @HoursBack, GETDATE()),
        @MoreInfoHeader = '<?ClickToSeeDetails -- ' + @crlf, @MoreInfoFooter = @crlf + ' -- ?>';

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
		LastBackupRecoveryModel NVARCHAR(60),
		FirstFullBackupSize BIGINT,
		FirstFullBackupDate DATETIME,
		LastFullBackupSize BIGINT,
		LastFullBackupDate DATETIME,
		AvgFullBackupThroughut DECIMAL (18,2),
		AvgFullBackupDurationSeconds INT,
		AvgDiffBackupThroughput DECIMAL (18,2),
		AvgDiffBackupDurationSeconds INT,
		AvgLogBackupThroughput DECIMAL (18,2),
		AvgLogBackupDurationSeconds INT,
		MaxDiffSize BIGINT,
		AvgDiffSize BIGINT,
		MaxLogSize BIGINT,
		AvgLogSize BIGINT
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


	RAISERROR('Inserting to #Backups', 0, 1) WITH NOWAIT;

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute = @StringToExecute + 'WITH Backups AS (SELECT bs.database_name, bs.database_guid, bs.type AS backup_type ' + @crlf
		+ ' , MBpsAvg = CAST(AVG(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMin = CAST(MIN(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMax = CAST(MAX(( bs.backup_size / ( CASE WHEN DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) = 0 THEN 1 ELSE DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) END ) / 1048576 )) AS INT) ' + @crlf
		+ ' , SizeMBAvg = AVG(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMin = MIN(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMax = MAX(backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBAvg = AVG(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMin = MIN(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMax = MAX(compressed_backup_size / 1048576.0) ' + @crlf;


	SET @StringToExecute = @StringToExecute + ' FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bs ' + @crlf
		+ ' WHERE bs.backup_finish_date >= @StartTime AND bs.is_damaged = 0 ' + @crlf
		+ ' GROUP BY bs.database_name, bs.database_guid, bs.type)' + @crlf;

	SET @StringToExecute = @StringToExecute + 'INSERT INTO #Backups(database_name, database_guid, ' + @crlf
		+ ' FullMBpsAvg, FullMBpsMin, FullMBpsMax, FullSizeMBAvg, FullSizeMBMin, FullSizeMBMax, FullCompressedSizeMBAvg, FullCompressedSizeMBMin, FullCompressedSizeMBMax, ' + @crlf
		+ ' DiffMBpsAvg, DiffMBpsMin, DiffMBpsMax, DiffSizeMBAvg, DiffSizeMBMin, DiffSizeMBMax, DiffCompressedSizeMBAvg, DiffCompressedSizeMBMin, DiffCompressedSizeMBMax, ' + @crlf
		+ ' LogMBpsAvg, LogMBpsMin, LogMBpsMax, LogSizeMBAvg, LogSizeMBMin, LogSizeMBMax, LogCompressedSizeMBAvg, LogCompressedSizeMBMin, LogCompressedSizeMBMax ) ' + @crlf
		+ 'SELECT bF.database_name, bF.database_guid ' + @crlf
		+ ' , bF.MBpsAvg AS FullMBpsAvg ' + @crlf
		+ ' , bF.MBpsMin AS FullMBpsMin ' + @crlf
		+ ' , bF.MBpsMax AS FullMBpsMax ' + @crlf
		+ ' , bF.SizeMBAvg AS FullSizeMBAvg ' + @crlf
		+ ' , bF.SizeMBMin AS FullSizeMBMin ' + @crlf
		+ ' , bF.SizeMBMax AS FullSizeMBMax ' + @crlf
		+ ' , bF.CompressedSizeMBAvg AS FullCompressedSizeMBAvg ' + @crlf
		+ ' , bF.CompressedSizeMBMin AS FullCompressedSizeMBMin ' + @crlf
		+ ' , bF.CompressedSizeMBMax AS FullCompressedSizeMBMax ' + @crlf
		+ ' , bD.MBpsAvg AS DiffMBpsAvg ' + @crlf
		+ ' , bD.MBpsMin AS DiffMBpsMin ' + @crlf
		+ ' , bD.MBpsMax AS DiffMBpsMax ' + @crlf
		+ ' , bD.SizeMBAvg AS DiffSizeMBAvg ' + @crlf
		+ ' , bD.SizeMBMin AS DiffSizeMBMin ' + @crlf
		+ ' , bD.SizeMBMax AS DiffSizeMBMax ' + @crlf
		+ ' , bD.CompressedSizeMBAvg AS DiffCompressedSizeMBAvg ' + @crlf
		+ ' , bD.CompressedSizeMBMin AS DiffCompressedSizeMBMin ' + @crlf
		+ ' , bD.CompressedSizeMBMax AS DiffCompressedSizeMBMax ' + @crlf
		+ ' , bL.MBpsAvg AS LogMBpsAvg ' + @crlf
		+ ' , bL.MBpsMin AS LogMBpsMin ' + @crlf
		+ ' , bL.MBpsMax AS LogMBpsMax ' + @crlf
		+ ' , bL.SizeMBAvg AS LogSizeMBAvg ' + @crlf
		+ ' , bL.SizeMBMin AS LogSizeMBMin ' + @crlf
		+ ' , bL.SizeMBMax AS LogSizeMBMax ' + @crlf
		+ ' , bL.CompressedSizeMBAvg AS LogCompressedSizeMBAvg ' + @crlf
		+ ' , bL.CompressedSizeMBMin AS LogCompressedSizeMBMin ' + @crlf
		+ ' , bL.CompressedSizeMBMax AS LogCompressedSizeMBMax ' + @crlf
		+ ' FROM Backups bF ' + @crlf
		+ ' LEFT OUTER JOIN Backups bD ON bF.database_name = bD.database_name AND bF.database_guid = bD.database_guid AND bD.backup_type = ''I''' + @crlf
		+ ' LEFT OUTER JOIN Backups bL ON bF.database_name = bL.database_name AND bF.database_guid = bD.database_guid AND bL.backup_type = ''L''' + @crlf
		+ ' WHERE bF.backup_type = ''D''; ' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;


	RAISERROR('Updating #Backups with worst RPO case', 0, 1) WITH NOWAIT;

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							 SELECT  bs.database_name, bs.database_guid, bs.backup_set_id, bsPrior.backup_set_id AS backup_set_id_prior,
							         bs.backup_finish_date, bsPrior.backup_finish_date AS backup_finish_date_prior,
							         DATEDIFF(ss, bsPrior.backup_finish_date, bs.backup_finish_date) AS backup_gap_seconds
							 INTO #backup_gaps
							 FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS bs
							 CROSS APPLY ( 
							 	SELECT TOP 1 bs1.backup_set_id, bs1.backup_finish_date
							 	FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS bs1
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
      SET RPOWorstCaseMoreInfoQuery = @MoreInfoHeader + 'SELECT * ' + @crlf
            + ' FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset ' + @crlf
            + ' WHERE database_name = ''' + database_name + ''' ' + @crlf
            + ' AND database_guid = ''' + CAST(database_guid AS NVARCHAR(50)) + ''' ' + @crlf
            + ' AND backup_finish_date >= DATEADD(hh, -2, ''' + CAST(CONVERT(DATETIME, RPOWorstCaseBackupSetPriorFinishTime, 102) AS NVARCHAR(100)) + ''') ' + @crlf
            + ' AND backup_finish_date <= DATEADD(hh, 2, ''' + CAST(CONVERT(DATETIME, RPOWorstCaseBackupSetPriorFinishTime, 102) AS NVARCHAR(100)) + ''') ' + @crlf
            + ' ORDER BY backup_finish_date;'
            + @MoreInfoFooter;


/* RTO */

RAISERROR('Gathering RTO information', 0, 1) WITH NOWAIT;


	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							 UPDATE #RTORecoveryPoints
							 SET log_backup_set_id = bLasted.backup_set_id
							     ,full_backup_set_id = bLasted.backup_set_id
							     ,full_last_lsn = bLasted.last_lsn
							     ,full_backup_set_uuid = bLasted.backup_set_uuid
							 FROM #RTORecoveryPoints rp
							 		CROSS APPLY (
							 				SELECT TOP 1 bLog.backup_set_id AS backup_set_id_log, bLastFull.backup_set_id, bLastFull.last_lsn, bLastFull.backup_set_uuid, bLastFull.database_guid, bLastFull.database_name
							 				FROM  ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLog 
							 				INNER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLastFull 
							 					ON bLog.database_guid = bLastFull.database_guid 
							 					AND bLog.database_name = bLastFull.database_name
							 					AND bLog.first_lsn > bLastFull.last_lsn
							 					AND bLastFull.type = ''D''
							 				WHERE rp.database_guid = bLog.database_guid 
							 					AND rp.database_name = bLog.database_name
							 			) bLasted
							 LEFT OUTER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLaterFulls ON bLasted.database_guid = bLaterFulls.database_guid AND bLasted.database_name = bLaterFulls.database_name
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							 INSERT INTO #RTORecoveryPoints(database_name, database_guid, full_backup_set_id, full_last_lsn, full_backup_set_uuid)
							 SELECT bFull.database_name, bFull.database_guid, bFull.backup_set_id, bFull.last_lsn, bFull.backup_set_uuid
							 FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bFull
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE rp
						    SET log_last_lsn = (SELECT MAX(last_lsn) FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLog WHERE bLog.first_lsn >= rp.full_last_lsn AND bLog.first_lsn <= rpNextFull.full_last_lsn AND bLog.type = ''L'')
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE #RTORecoveryPoints
							SET diff_last_lsn = (SELECT TOP 1 bDiff.last_lsn FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bDiff
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE #RTORecoveryPoints
							    SET full_time_seconds = DATEDIFF(ss,bFull.backup_start_date, bFull.backup_finish_date)
							    , full_file_size_mb = bFull.backup_size / 1048576.0
							    , diff_backup_set_id = bDiff.backup_set_id
							    , diff_time_seconds = DATEDIFF(ss,bDiff.backup_start_date, bDiff.backup_finish_date)
							    , diff_file_size_mb = bDiff.backup_size / 1048576.0
							FROM #RTORecoveryPoints rp
							INNER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bFull ON rp.database_guid = bFull.database_guid AND rp.database_name = bFull.database_name AND rp.full_last_lsn = bFull.last_lsn
							LEFT OUTER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bDiff ON rp.database_guid = bDiff.database_guid AND rp.database_name = bDiff.database_name AND rp.diff_last_lsn = bDiff.last_lsn AND bDiff.last_lsn IS NOT NULL;
							';

	IF @Debug = 1
		PRINT @StringToExecute;
	
	EXEC sys.sp_executesql @StringToExecute;


/* Get time & size totals for logs */

RAISERROR('Get time & size totals for logs', 0, 1) WITH NOWAIT;

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							WITH LogTotals AS (
								 SELECT rp.id, log_time_seconds = SUM(DATEDIFF(ss,bLog.backup_start_date, bLog.backup_finish_date))
								    , log_file_size = SUM(bLog.backup_size)
								    , SUM(1) AS log_backups
								        FROM #RTORecoveryPoints rp
								            INNER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bLog ON rp.database_guid = bLog.database_guid AND rp.database_name = bLog.database_name AND bLog.type = ''L''
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
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
										SET RTOWorstCaseMinutes = wc.rto_worst_case_time_seconds / 60.0
								        , RTOWorstCaseBackupFileSizeMB = wc.rto_worst_case_size_mb
								FROM #Backups b
								INNER JOIN WorstCases wc 
								ON b.database_guid = wc.database_guid 
									AND b.database_name = wc.database_name;
								';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

/*Populating Recoverability*/


	/*Get distinct list of databases*/
	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							 SELECT DISTINCT b.database_name AS [DatabaseName]
							 FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b;'

	IF @Debug = 1
		PRINT @StringToExecute;

	INSERT #Recoverability ( DatabaseName )
	EXEC sys.sp_executesql @StringToExecute;


	/*Find most recent recovery model, backup size, and backup date*/
	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 	'
								UPDATE r
								SET r.LastBackupRecoveryModel = ca.recovery_model,
									r.LastFullBackupSize = ca.backup_size,
									r.LastFullBackupDate = ca.backup_finish_date
								FROM #Recoverability r
									CROSS APPLY (
										SELECT TOP 1 b.recovery_model, b.backup_size, b.backup_finish_date
										FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
										WHERE r.DatabaseName = b.database_name
										AND b.type = ''D''
										ORDER BY b.backup_finish_date DESC
												) ca;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;

	/*Find first backup size and date*/
	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE r
							SET r.FirstFullBackupSize = ca.backup_size,
								r.FirstFullBackupDate = ca.backup_finish_date
							FROM #Recoverability r
								CROSS APPLY (
									SELECT TOP 1 b.backup_size, b.backup_finish_date
									FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
									WHERE r.DatabaseName = b.database_name 
									AND b.type = ''D''
									ORDER BY b.backup_finish_date ASC
											) ca;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;


	/*Find average backup throughputs for full, diff, and log*/
	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE r
							SET r.AvgFullBackupThroughut = ca_full.AvgFullSpeed,
								r.AvgDiffBackupThroughput = ca_diff.AvgDiffSpeed,
								r.AvgLogBackupThroughput = ca_log.AvgLogSpeed,
								r.AvgFullBackupDurationSeconds = AvgFullDuration,
								r.AvgDiffBackupDurationSeconds = AvgDiffDuration,
								r.AvgLogBackupDurationSeconds = AvgLogDuration
							FROM #Recoverability AS r
							OUTER APPLY (
								SELECT b.database_name, 
									   CAST(AVG(( backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576 )) AS INT) AS AvgFullSpeed,
									   CAST(AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS INT) AS AvgFullDuration
								FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND b.type = ''D'' 
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								GROUP BY b.database_name
										) ca_full
							OUTER APPLY (
								SELECT b.database_name, 
									   CAST(AVG(( backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576 )) AS INT) AS AvgDiffSpeed,
									   CAST(AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS INT) AS AvgDiffDuration
								FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND b.type = ''I'' 
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								GROUP BY b.database_name
										) ca_diff
							OUTER APPLY (
								SELECT b.database_name, 
									   CAST(AVG(( backup_size / ( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) / 1048576 )) AS INT) AS AvgLogSpeed,
									   CAST(AVG( DATEDIFF(ss, b.backup_start_date, b.backup_finish_date) ) AS INT) AS AvgLogDuration
								FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset b
								WHERE r.DatabaseName = b.database_name
								AND b.type = ''L''
								AND DATEDIFF(SECOND, b.backup_start_date, b.backup_finish_date) > 0
								GROUP BY b.database_name
										) ca_log;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;


	/*Find max and avg diff and log sizes*/
	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += '
							UPDATE r
							 SET r.MaxDiffSize = diffs.max_diff_size,
							 	 r.AvgDiffSize = diffs.avg_diff_size,
							 	 r.MaxLogSize = logs.max_log_size,
							 	 r.AvgLogSize = logs.avg_log_size
							 FROM #Recoverability AS r
							 OUTER APPLY (
							 	SELECT b.database_name, MAX(b.backup_size) AS max_diff_size, AVG(b.backup_size) AS avg_diff_size
							 	FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
							 	WHERE r.DatabaseName = b.database_name
							 	AND b.type = ''I''
							 	GROUP BY b.database_name
							 			) AS diffs
							 OUTER APPLY (
							 	SELECT b.database_name, MAX(b.backup_size) AS max_log_size, AVG(b.backup_size) AS avg_log_size
							 	FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
							 	WHERE r.DatabaseName = b.database_name
							 	AND b.type = ''L''
							 	GROUP BY b.database_name
							 			) AS logs;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sys.sp_executesql @StringToExecute;


/*End populating Recoverability*/

RAISERROR('Returning data', 0, 1) WITH NOWAIT;

	SELECT   b.*
		FROM     #Backups AS b
		ORDER BY b.database_name;

	SELECT   *
		FROM     #Recoverability AS r
		ORDER BY r.DatabaseName

/*Looking for non-Agent backups. Agent handles most backups, can expand or change depending on what we find out there*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		1 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Non-Agent backups taken'' AS [Finding], 
		''The database '' + QUOTENAME(b.database_name) + '' has been backed up by '' + QUOTENAME(b.user_name) + '' '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE  b.user_name NOT LIKE ''%Agent%'' 
	GROUP BY b.database_name, b.user_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings (CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for compatibility level changing. Only looking for databases that have changed more than twice (It''s possible someone may have changed up, had CE problems, and then changed back)*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		2 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Compatibility level changing'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has changed compatibility levels '' + CONVERT(VARCHAR(10), COUNT(DISTINCT b.compatibility_level)) + '' times.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	GROUP BY b.database_name
	HAVING COUNT(DISTINCT b.compatibility_level) > 2;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for password protected backups. This hasn''t been a popular option ever, and was largely replaced by encrypted backups, but it''s simple to check for.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		3 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Password backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has been backed up with a password '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times. Who has the password?'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.is_password_protected = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for snapshot backups. There are legit reasons for these, but we should flag them so the questions get asked. What questions? Good question.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		4 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Snapshot backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' snapshot backups. This message is purely informational.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.is_snapshot = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*It''s fine to take backups of read only databases, but it''s not always necessary (there''s no new data, after all).*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		5 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Read only state backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times while in a read-only state. This can be normal if it''''s a secondary, but a bit odd otherwise.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.is_readonly = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*So, I''ve come across people who think they need to change their database to single user mode to take a backup. Or that doing that will help something. I just need to know, here.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		6 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Single user mode backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times while in single-user mode. This is really weird! Make sure your backup process doesn''''t include a mode change anywhere.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.is_single_user = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*C''mon, it''s 2017. Take your backups with CHECKSUMS, people.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		7 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''No CHECKSUMS'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has been backed up '' + CONVERT(VARCHAR(10), COUNT(*)) + '' times without CHECKSUMS in the past 30 days. CHECKSUMS can help alert you to corruption errors.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.has_backup_checksums = 0
	AND b.backup_finish_date >= DATEADD(DAY, -30, SYSDATETIME())
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Damaged is a Black Flag album. You don''t want your backups to be like a Black Flag album. */

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		8 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Damaged backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' damaged backups taken without stopping to throw an error. This is done by specifying CONTINUE_AFTER_ERROR in your BACKUP commands.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.is_damaged = 1
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Checking for encrypted backups and the last backup of the encryption key.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		9 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Encrypted backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' '' + b.encryptor_type + '' backups, and the last time a certificate was backed up is '' 
		+ CONVERT(VARCHAR(30), (SELECT MAX(c.pvt_key_last_backup_date) FROM sys.certificates AS c WHERE c.name NOT LIKE ''##%'')) + ''.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE b.encryptor_type IS NOT NULL
	GROUP BY b.database_name, b.encryptor_type;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;
	
	/*Looking for backups that have BULK LOGGED data in them -- this can screw up point in time LOG recovery.*/

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
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

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf;

	SET @StringToExecute += 'SELECT 
		12 AS CheckId,
		100 AS [Priority],
		b.database_name AS [Database Name],
		''Uncompressed backups'' AS [Finding],
		''The database '' + QUOTENAME(b.database_name) + '' has had '' + CONVERT(VARCHAR(10), COUNT(*)) + '' uncompressed backups in the last 30 days. This is a free way to save time and space. And SPACETIME.'' AS [Warning]
	FROM   ' + QUOTENAME(@MSDBName) + '.dbo.backupset AS b
	WHERE backup_size = compressed_backup_size AND type = ''D''
	AND b.backup_finish_date >= DATEADD(DAY, -30, SYSDATETIME())
	GROUP BY b.database_name;' + @crlf;

	IF @Debug = 1
		PRINT @StringToExecute;

		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )
		EXEC sys.sp_executesql @StringToExecute;


/*Insert thank you stuff last*/
		INSERT #Warnings ( CheckId, Priority, DatabaseName, Finding, Warning )

		SELECT
		2147483647 AS [CheckId],
		2147483647 AS [Priority],
		'From Your Community Volunteers' AS [DatabaseName],
		'sp_BlitzBackups Version: ' + @Version + ', Version Date: ' + CONVERT(VARCHAR(30), @VersionDate) + '.' AS [Finding],
		'Thanks for using our stored procedure. We hope you find it useful! Check out our other free SQL Server scripts at firstresponderkit.org!' AS [Warning];


SELECT w.CheckId, w.Priority, w.DatabaseName, w.Finding, w.Warning
FROM #Warnings AS w
ORDER BY w.Priority, w.CheckId;

DROP TABLE #Backups, #Warnings, #Recoverability, #RTORecoveryPoints


END;
END;
GO