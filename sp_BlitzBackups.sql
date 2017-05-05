IF OBJECT_ID('dbo.sp_BlitzBackups') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzBackups AS RETURN 0;')
GO
ALTER PROCEDURE [dbo].[sp_BlitzBackups]
    @Help TINYINT = 0 ,
	@HoursBack INT = 168,
	@MSDBName NVARCHAR(255) = 'msdb',
	@RestoreSpeedFullMBps INT = NULL,
	@RestoreSpeedDiffMBps INT = NULL,
	@RestoreSpeedLogMBps INT = NULL,
	@Debug TINYINT = 0,
    @VersionDate DATETIME = NULL OUTPUT
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
	DECLARE @StringToExecute NVARCHAR(4000)
		,@ProductVersion NVARCHAR(128)
		,@ProductVersionMajor DECIMAL(10,2)
		,@ProductVersionMinor DECIMAL(10,2)
		,@StartTime DATETIME2
		,@ResultText NVARCHAR(MAX)
		,@crlf NVARCHAR(2);

	IF @HoursBack > 0 SET @HoursBack = @HoursBack * -1;
	SELECT @crlf = NCHAR(13) + NCHAR(10),
		@StartTime = DATEADD(hh, @HoursBack, GETDATE());

	CREATE TABLE #Backups (id INT IDENTITY(1,1)
		,database_name NVARCHAR(128)
		,database_guid UNIQUEIDENTIFIER
		,FullMBpsAvg DECIMAL(18,2)
		,FullMBpsMin DECIMAL(18,2)
		,FullMBpsMax DECIMAL(18,2)
		,FullSizeMBAvg DECIMAL(18,2)
		,FullSizeMBMin DECIMAL(18,2)
		,FullSizeMBMax DECIMAL(18,2)
		,FullCompressedSizeMBAvg DECIMAL(18,2)
		,FullCompressedSizeMBMin DECIMAL(18,2)
		,FullCompressedSizeMBMax DECIMAL(18,2)
		,DiffMBpsAvg DECIMAL(18,2)
		,DiffMBpsMin DECIMAL(18,2)
		,DiffMBpsMax DECIMAL(18,2)
		,DiffSizeMBAvg DECIMAL(18,2)
		,DiffSizeMBMin DECIMAL(18,2)
		,DiffSizeMBMax DECIMAL(18,2)
		,DiffCompressedSizeMBAvg DECIMAL(18,2)
		,DiffCompressedSizeMBMin DECIMAL(18,2)
		,DiffCompressedSizeMBMax DECIMAL(18,2)
		,LogMBpsAvg DECIMAL(18,2)
		,LogMBpsMin DECIMAL(18,2)
		,LogMBpsMax DECIMAL(18,2)
		,LogSizeMBAvg DECIMAL(18,2)
		,LogSizeMBMin DECIMAL(18,2)
		,LogSizeMBMax DECIMAL(18,2)
		,LogCompressedSizeMBAvg DECIMAL(18,2)
		,LogCompressedSizeMBMin DECIMAL(18,2)
		,LogCompressedSizeMBMax DECIMAL(18,2)
		,RPOWorstCaseBackupSetID INT
		,RPOWorstCaseBackupSetFinishTime DATETIME
		,RPOWorstCaseBackupSetIDPrior INT
		,RPOWorstCaseBackupSetPriorFinishTime DATETIME
		);

	SET @StringToExecute = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;' + @crlf

	SET @StringToExecute = @StringToExecute + 'WITH Backups AS (SELECT bs.database_name, bs.database_guid, bs.type AS backup_type ' + @crlf
		+ ' , MBpsAvg = CAST(AVG(( bs.backup_size / ( DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMin = CAST(AVG(( bs.backup_size / ( DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) ) / 1048576 )) AS INT) ' + @crlf
		+ ' , MBpsMax = CAST(AVG(( bs.backup_size / ( DATEDIFF(ss, bs.backup_start_date, bs.backup_finish_date) ) / 1048576 )) AS INT) ' + @crlf
		+ ' , SizeMBAvg = AVG(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMin = MIN(backup_size / 1048576.0) ' + @crlf
		+ ' , SizeMBMax = MAX(backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBAvg = AVG(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMin = MIN(compressed_backup_size / 1048576.0) ' + @crlf
		+ ' , CompressedSizeMBMax = MAX(compressed_backup_size / 1048576.0) ' + @crlf


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
		+ ' INNER JOIN Backups bD ON bF.database_name = bD.database_name AND bF.database_guid = bD.database_guid AND bD.backup_type = ''I''' + @crlf
		+ ' INNER JOIN Backups bL ON bF.database_name = bL.database_name AND bF.database_guid = bD.database_guid AND bL.backup_type = ''L''' + @crlf
		+ ' WHERE bF.backup_type = ''D''; ';

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

	SET @StringToExecute = 'WITH BackupGaps AS (SELECT bs.database_name, bs.database_guid, bs.backup_set_id, bsPrior.backup_set_id AS backup_set_id_prior,
			bs.backup_finish_date, bsPrior.backup_finish_date AS backup_finish_date_prior,
			DATEDIFF(ss, bs.backup_finish_date, bsPrior.backup_finish_date) AS backup_gap_seconds
		FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset bs 
		INNER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bsPrior 
			ON bs.database_name = bsPrior.database_name 
			AND bs.database_guid = bsPrior.database_guid 
			AND bs.backup_finish_date > bsPrior.backup_finish_date
		LEFT OUTER JOIN ' + QUOTENAME(@MSDBName) + '.dbo.backupset bsBetween
			ON bs.database_name = bsBetween.database_name 
			AND bs.database_guid = bsBetween.database_guid 
			AND bs.backup_finish_date > bsBetween.backup_finish_date
			AND bsPrior.backup_finish_date < bsBetween.backup_finish_date
		WHERE bs.backup_finish_date > @StartTime
		AND bsBetween.backup_set_id IS NULL
	)
	UPDATE #Backups
		SET RPOWorstCaseBackupSetID = bg.backup_set_id
			, RPOWorstCaseBackupSetFinishTime = bg.backup_finish_date
			, RPOWorstCaseBackupSetIDPrior = bg.backup_set_id_prior
			, RPOWorstCaseBackupSetPriorFinishTime = bg.backup_set_id_prior
		FROM #Backups b
		INNER JOIN BackupGaps bg ON b.database_name = bg.database_name AND b.database_guid = bg.database_guid
		LEFT OUTER JOIN BackupGaps bgBigger ON bg.database_name = bgBigger.database_name AND bg.database_guid = bgBigger.database_guid AND bg.backup_gap_seconds < bgBigger.backup_gap_seconds
		WHERE bgBigger.backup_set_id IS NULL;'

	IF @Debug = 1
		PRINT @StringToExecute;

	EXEC sp_executesql @StringToExecute, N'@StartTime DATETIME2', @StartTime;

	SELECT b.*, 
		RPOWorstCaseMoreInfoQuery = 'SELECT * FROM ' + QUOTENAME(@MSDBName) + '.dbo.backupset WHERE database_name = ''' + b.database_name + ''' AND database_guid = ''' + CAST(b.database_guid AS NVARCHAR(50)) + ''' AND backup_finish_date >= DATEADD(hh, -2, ''' + CAST(b.RPOWorstCaseBackupSetPriorFinishTime AS NVARCHAR(50)) + ''') AND backup_finish_date <= DATEADD(hh, 2, ''' + CAST(b.RPOWorstCaseBackupSetFinishTime AS NVARCHAR(50)) + ''');'
	  FROM #Backups b;

	DROP TABLE #Backups;
END
END
GO
