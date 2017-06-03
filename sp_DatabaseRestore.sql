
/*
EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 0, 
	@RunRecovery = 0;

EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 1, 
	@RunRecovery = 0;

EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 1, 
	@RunRecovery = 1;

EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 0, 
	@RunRecovery = 1;

EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathDiff = 'D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\',
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@RestoreDiff = 1,
	@ContinueLogs = 0, 
	@RunRecovery = 1;
 
EXEC dbo.sp_DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = '\\StorageServer\LogShipMe\FULL\', 
	@BackupPathDiff = '\\StorageServer\LogShipMe\DIFF\',
	@BackupPathLog = '\\StorageServer\LogShipMe\LOG\', 
	@RestoreDiff = 1,
	@ContinueLogs = 0, 
	@RunRecovery = 1,
	@TestRestore = 1,
	@RunCheckDB = 1,
	@Debug = 0;

--This example will restore the latest differential backup, and stop transaction logs at the specified date time.  It will also only print the commands.
EXEC dbo.sp_DatabaseRestore 
	@Database = 'DBA', 
	@BackupPathFull = '\\StorageServer\LogShipMe\FULL\', 
	@BackupPathDiff = '\\StorageServer\LogShipMe\DIFF\',
	@BackupPathLog = '\\StorageServer\LogShipMe\LOG\', 
	@RestoreDiff = 1,
	@ContinueLogs = 0, 
	@RunRecovery = 1,
	@StopAt = '20170508201501',
	@Debug = 1;

Variables:

@RestoreDiff - This variable is a flag for whether or not the script is expecting to restore differentials
@StopAt - This variable is used to restore transaction logs to a specific date and time.  The format must be in YYYYMMDDHHMMSS.  The time is in military format.

About Debug Modes:

There are 3 Debug Modes.  Mode 0 is the default and will execute the script.  Debug 1 will print just the commands.  Debug 2 will print other useful information that
has mostly been useful for troubleshooting.  Debug 2 needs to be expanded to make it more useful.
*/

IF OBJECT_ID('dbo.sp_DatabaseRestore') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_DatabaseRestore AS RETURN 0;')
GO

ALTER PROCEDURE [dbo].[sp_DatabaseRestore]
	  @Database NVARCHAR(128), @RestoreDatabaseName NVARCHAR(128) = NULL, @BackupPathFull NVARCHAR(MAX), @BackupPathDiff NVARCHAR(MAX), @BackupPathLog NVARCHAR(MAX),
	  @MoveFiles bit = 0, @MoveDataDrive NVARCHAR(260) = NULL, @MoveLogDrive NVARCHAR(260) = NULL, @TestRestore bit = 0, @RunCheckDB bit = 0, @RestoreDiff bit = 0,
	  @ContinueLogs bit = 0, @RunRecovery bit = 0, @Debug INT = 0, @VersionDate DATETIME = NULL OUTPUT, @StopAt VARCHAR(14) = NULL
AS
SET NOCOUNT ON;

	DECLARE @Version VARCHAR(30);
	SET @Version = '5.4';
	SET @VersionDate = '20170603';

DECLARE @cmd NVARCHAR(4000), @sql NVARCHAR(MAX), @LastFullBackup NVARCHAR(500), @LastDiffBackup NVARCHAR(500), @LastDiffBackupDateTime NVARCHAR(500), @BackupFile NVARCHAR(500), @BackupDateTime AS CHAR(15), @FullLastLSN NUMERIC(25, 0), @DiffLastLSN NUMERIC(25, 0);
DECLARE @FileList TABLE (BackupFile NVARCHAR(255));

IF @RestoreDatabaseName IS NULL
	SET @RestoreDatabaseName = @Database;

-- get list of files 
SET @cmd = 'DIR /b "'+ @BackupPathFull + '"';
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

-- select * from @fileList
-- Find latest full backup 
SELECT @LastFullBackup = MAX(BackupFile)
FROM @FileList
WHERE BackupFile LIKE '%.bak'
    AND
    BackupFile LIKE '%'+@Database+'%';

-- Get the SQL Server version number because the columns returned by RESTORE commands vary by version
-- Based on: https://www.brentozar.com/archive/2015/05/sql-server-version-detection/
-- Need to capture BuildVersion because RESTORE HEADERONLY changed with 2014 CU1, not RTM
DECLARE @ProductVersion AS varchar(20) = CAST(SERVERPROPERTY ('productversion') AS varchar(20));
DECLARE @MajorVersion AS smallint = CAST(PARSENAME(@ProductVersion, 4) AS smallint);
DECLARE @MinorVersion AS smallint = CAST(PARSENAME(@ProductVersion, 3) AS smallint);
DECLARE @BuildVersion AS smallint = CAST(PARSENAME(@ProductVersion, 2) AS smallint);

IF @MajorVersion < 10
BEGIN
  RAISERROR('Sorry, DatabaseRestore doesn''t work on versions of SQL prior to 2008.', 15, 1);
  RETURN;
END

-- Build SQL for RESTORE FILELIST ONLY
IF OBJECT_ID(N'tempdb..#FileListParameters') IS NOT NULL DROP TABLE #FileListParameters;
CREATE TABLE #FileListParameters
(
	 LogicalName NVARCHAR(128) NOT NULL
	,PhysicalName NVARCHAR(260) NOT NULL
	,Type CHAR(1) NOT NULL
	,FileGroupName NVARCHAR(120) NULL
	,Size NUMERIC(20, 0) NOT NULL
	,MaxSize NUMERIC(20, 0) NOT NULL
	,FileID BIGINT NULL
	,CreateLSN NUMERIC(25, 0) NULL
	,DropLSN NUMERIC(25, 0) NULL
	,UniqueID UNIQUEIDENTIFIER NULL
	,ReadOnlyLSN NUMERIC(25, 0) NULL
	,ReadWriteLSN NUMERIC(25, 0) NULL
	,BackupSizeInBytes BIGINT NULL
	,SourceBlockSize INT NULL
	,FileGroupID INT NULL
	,LogGroupGUID UNIQUEIDENTIFIER NULL
	,DifferentialBaseLSN NUMERIC(25, 0) NULL
	,DifferentialBaseGUID UNIQUEIDENTIFIER NULL
	,IsReadOnly BIT NULL
	,IsPresent BIT NULL
	,TDEThumbprint VARBINARY(32) NULL
	,SnapshotUrl NVARCHAR(360) NULL
);
DECLARE @FileListParamSQL AS nvarchar(4000);
SET @FileListParamSQL = 
  'INSERT INTO #FileListParameters
   (LogicalName, PhysicalName, Type, FileGroupName, Size, MaxSize, FileID, CreateLSN, DropLSN
   ,UniqueID, ReadOnlyLSN, ReadWriteLSN, BackupSizeInBytes, SourceBlockSize, FileGroupID, LogGroupGUID
   ,DifferentialBaseLSN, DifferentialBaseGUID, IsReadOnly, IsPresent, TDEThumbprint'

IF @MajorVersion >= 13
  SET @FileListParamSQL += ', SnapshotUrl'

SET @FileListParamSQL += ')' + CHAR(13) + CHAR(10);
SET @FileListParamSQL += 'EXEC (''RESTORE FILELISTONLY FROM DISK=''''{Path}'''''')';

SET @sql = REPLACE(@FileListParamSQL, '{Path}', @BackupPathFull + @LastFullBackup);
IF @Debug = 2
	PRINT @sql;

EXEC (@sql);

-- Build SQL for RESTORE HEADERONLY - this will be used a bit further below
IF OBJECT_ID(N'tempdb..#Headers') IS NOT NULL DROP TABLE #Headers;
CREATE TABLE #Headers (
	BackupName VARCHAR(256), BackupDescription VARCHAR(256), BackupType VARCHAR(256), ExpirationDate VARCHAR(256),
	Compressed VARCHAR(256), Position VARCHAR(256), DeviceType VARCHAR(256), UserName VARCHAR(256), ServerName VARCHAR(
	256), DatabaseName VARCHAR(256), DatabaseVersion VARCHAR(256), DatabaseCreationDate VARCHAR(256), BackupSize VARCHAR(
	256), FirstLSN VARCHAR(256), LastLSN VARCHAR(256), CheckpointLSN VARCHAR(256), DatabaseBackupLSN VARCHAR(256),
	BackupStartDate VARCHAR(256), BackupFinishDate VARCHAR(256), SortOrder VARCHAR(256), CodePage VARCHAR(256),
	UnicodeLocaleId VARCHAR(256), UnicodeComparisonStyle VARCHAR(256), CompatibilityLevel VARCHAR(256), SoftwareVendorId
	VARCHAR(256), SoftwareVersionMajor VARCHAR(256), SoftwareVersionMinor VARCHAR(256), SoftwareVersionBuild VARCHAR(256),
	MachineName VARCHAR(256), Flags VARCHAR(256), BindingID VARCHAR(256), RecoveryForkID VARCHAR(256), Collation VARCHAR(
	256), FamilyGUID VARCHAR(256), HasBulkLoggedData VARCHAR(256), IsSnapshot VARCHAR(256), IsReadOnly VARCHAR(256),
	IsSingleUser VARCHAR(256), HasBackupChecksums VARCHAR(256), IsDamaged VARCHAR(256), BeginsLogChain VARCHAR(256),
	HasIncompleteMetaData VARCHAR(256), IsForceOffline VARCHAR(256), IsCopyOnly VARCHAR(256), FirstRecoveryForkID VARCHAR
	(256), ForkPointLSN VARCHAR(256), RecoveryModel VARCHAR(256), DifferentialBaseLSN VARCHAR(256), DifferentialBaseGUID
	VARCHAR(256), BackupTypeDescription VARCHAR(256), BackupSetGUID VARCHAR(256), CompressedBackupSize VARCHAR(256),
	Containment VARCHAR(256), KeyAlgorithm NVARCHAR(32), EncryptorThumbprint VARBINARY(20), EncryptorType NVARCHAR(32),
	--
	-- Seq added to retain order by
	--
	Seq INT NOT NULL IDENTITY(1, 1)
);

DECLARE @HeadersSQL AS nvarchar(4000);
SET @HeadersSQL = 
'INSERT INTO #Headers
  (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName
  ,DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
  ,BackupStartDate, BackupFinishDate, SortOrder, CodePage, UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel
  ,SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingID
  ,RecoveryForkID, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums
  ,IsDamaged, BeginsLogChain, HasIncompleteMetaData, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN
  ,RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize'
  
IF @MajorVersion >= 11
  SET @HeadersSQL += CHAR(13) + CHAR(10) + '  ,Containment';

IF @MajorVersion >= 13 OR (@MajorVersion = 12 AND @BuildVersion >= 2342)
  SET @HeadersSQL += ', KeyAlgorithm, EncryptorThumbprint, EncryptorType'

SET @HeadersSQL += ')' + CHAR(13) + CHAR(10);
SET @HeadersSQL += 'EXEC (''RESTORE HEADERONLY FROM DISK=''''{Path}'''''')';

DECLARE @MoveOption AS NVARCHAR(MAX)= '';

IF @MoveFiles = 1
BEGIN
	WITH Files
    AS (
		SELECT ',MOVE '''+LogicalName+''' TO '''+
			CASE
				WHEN Type = 'D' THEN @MoveDataDrive
				WHEN Type = 'L' THEN @MoveLogDrive
			END+REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1)-1)) +'''' AS logicalcmds
		FROM #FileListParameters)
	SELECT @MoveOption = @MoveOption + logicalcmds
	FROM Files;
END;
IF @ContinueLogs = 0
BEGIN
	SET @sql = 'RESTORE DATABASE '+@RestoreDatabaseName+' FROM DISK = '''+@BackupPathFull + @LastFullBackup+ ''' WITH NORECOVERY, REPLACE' + @MoveOption+CHAR(13);
	PRINT @sql;
	IF @Debug = 0
		EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';

  --get the backup completed data so we can apply tlogs from that point forwards                                                   
  SET @sql = REPLACE(@HeadersSQL, '{Path}', @BackupPathFull + @LastFullBackup);
  IF @Debug = 2
	PRINT @sql;
  
  EXECUTE (@sql);

    --setting the @BackupDateTime to a numeric string so that it can be used in comparisons
	SET @BackupDateTime = REPLACE(LEFT(RIGHT(@LastFullBackup, 19),15), '_', '')

	SELECT @FullLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) FROM #Headers WHERE BackupType = 1;  

  IF @Debug = 2
	PRINT @BackupDateTime                                                
END;
ELSE
BEGIN
	DECLARE @DatabaseLastLSN NUMERIC(25, 0);
	SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS NUMERIC(25, 0))
	FROM master.sys.databases d
	JOIN master.sys.master_files f ON d.database_id = f.database_id
	WHERE d.name = @RestoreDatabaseName AND f.file_id = 1
END;

--Clear out table variables for differential
DELETE FROM @FileList;

-- get list of files 
SET @cmd = 'DIR /b "'+ @BackupPathDiff + '"';
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

-- select * from @fileList
-- Find latest diff backup 
SELECT @LastDiffBackup = MAX(BackupFile)
FROM @FileList
WHERE BackupFile LIKE '%.bak'
    AND
    BackupFile LIKE '%'+@Database+'%';
	--set the @BackupDateTime so that it can be used for comparisons
	SET @BackupDateTime = REPLACE(@BackupDateTime, '_', '')
	SET @LastDiffBackupDateTime = REPLACE(LEFT(RIGHT(@LastDiffBackup, 19),15), '_', '')

IF @RestoreDiff = 1 AND @BackupDateTime < @LastDiffBackupDateTime
BEGIN
	SET @sql = 'RESTORE DATABASE '+@RestoreDatabaseName+' FROM DISK = '''+@BackupPathDiff + @LastDiffBackup+ ''' WITH NORECOVERY'+CHAR(13);
	PRINT @sql;
	IF @Debug = 0
		EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
	
  --get the backup completed data so we can apply tlogs from that point forwards                                                   
  SET @sql = REPLACE(@HeadersSQL, '{Path}', @BackupPathDiff + @LastDiffBackup);
  IF @Debug = 2
	PRINT @sql;
  
  EXECUTE (@sql);
  --set the @BackupDateTime to the date time on the most recent differential	
  SET @BackupDateTime = @LastDiffBackupDateTime
  SELECT @DiffLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) FROM #Headers WHERE BackupType = 5;                                                  
END;

--Clear out table variables for translogs
DELETE FROM @FileList;
        
SET @cmd = 'DIR /b "'+ @BackupPathLog + '"';
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd;
--check for log backups
IF(@StopAt IS NULL)
BEGIN 
DECLARE BackupFiles CURSOR FOR
	SELECT BackupFile
	FROM @FileList
	WHERE BackupFile LIKE '%.trn'
	  AND BackupFile LIKE '%'+@Database+'%'
	  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @BackupDateTime))
	  ORDER BY BackupFile;
OPEN BackupFiles;
END
ELSE
BEGIN
DECLARE BackupFiles CURSOR FOR
	SELECT BackupFile
	FROM @FileList
	WHERE BackupFile LIKE '%.trn'
	  AND BackupFile LIKE '%'+@Database+'%'
	  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @BackupDateTime) AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt)
	  ORDER BY BackupFile;
OPEN BackupFiles;
END
DECLARE @i tinyint = 1, @LogFirstLSN NUMERIC(25, 0), @LogLastLSN NUMERIC(25, 0);
-- Loop through all the files for the database  
FETCH NEXT FROM BackupFiles INTO @BackupFile;
WHILE @@FETCH_STATUS = 0
BEGIN
	IF @i = 1
	BEGIN
    SET @sql = REPLACE(@HeadersSQL, '{Path}', @BackupPathLog + @BackupFile);
	IF @Debug = 2
		PRINT @sql;
	EXECUTE (@sql);
		
		SELECT @LogFirstLSN = CAST(FirstLSN AS NUMERIC(25, 0)), @LogLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) FROM #Headers WHERE BackupType = 2;

		IF (@ContinueLogs = 0 AND @LogFirstLSN <= @FullLastLSN AND @FullLastLSN <= @LogLastLSN AND @RestoreDiff = 0) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 0)
			SET @i = 2;
		IF (@ContinueLogs = 0 AND @LogFirstLSN <= @DiffLastLSN AND @DiffLastLSN <= @LogLastLSN AND @RestoreDiff = 1) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 1)
			SET @i = 2;
		DELETE FROM #Headers WHERE BackupType = 2;
	END;
	IF @i = 2
	BEGIN
		SET @sql = 'RESTORE LOG '+@RestoreDatabaseName+' FROM DISK = '''+@BackupPathLog + @BackupFile+''' WITH NORECOVERY'+CHAR(13);
		PRINT @sql
		IF @Debug = 0
			EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE LOG', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
	END;
	FETCH NEXT FROM BackupFiles INTO @BackupFile;
END;
CLOSE BackupFiles;
DEALLOCATE BackupFiles;  
-- put database in a useable state 
IF @RunRecovery = 1
BEGIN
	SET @sql = 'RESTORE DATABASE '+@RestoreDatabaseName+' WITH RECOVERY'+CHAR(13);
	PRINT @sql
	IF @Debug = 0
		EXECUTE sp_executesql @sql;
END;
	    
 --Run checkdb against this database
IF @RunCheckDB = 1
BEGIN
	SET @sql = 'EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = ' + @RestoreDatabaseName + ', @LogToTable = ''Y'''+CHAR(13);
	PRINT @sql
	IF @Debug = 0
		EXECUTE sys.sp_executesql @sql
END;
 --If test restore then blow the database away (be careful)
IF @TestRestore = 1
BEGIN
	SET @sql = 'DROP DATABASE '+@RestoreDatabaseName+CHAR(13);
	PRINT @sql
	IF @Debug = 0
		EXECUTE sp_executesql @sql;
END;
