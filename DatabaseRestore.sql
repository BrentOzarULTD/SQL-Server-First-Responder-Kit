/*
EXEC dbo.DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 0, 
	@RunRecovery = 0;

EXEC dbo.DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 1, 
	@RunRecovery = 0;

EXEC dbo.DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 1, 
	@RunRecovery = 1;

EXEC dbo.DatabaseRestore 
	@Database = 'LogShipMe', 
	@BackupPathFull = 'D:\Backup\SQL2016PROD1A\LogShipMe\FULL\', 
	@BackupPathLog = 'D:\Backup\SQL2016PROD1A\LogShipMe\LOG\', 
	@ContinueLogs = 0, 
	@RunRecovery = 1;
*/

USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[DatabaseRestore]
	  @Database NVARCHAR(128), @RestoreDatabaseName NVARCHAR(128) = NULL, @BackupPathFull NVARCHAR(MAX), @BackupPathLog NVARCHAR(MAX),
	  @MoveFiles bit = 0, @MoveDataDrive NVARCHAR(260) = NULL, @MoveLogDrive NVARCHAR(260) = NULL, @TestRestore bit = 0, @RunCheckDB bit = 0, 
	  @ContinueLogs bit = 0, @RunRecovery bit = 0
AS

SET NOCOUNT ON;

DECLARE @cmd NVARCHAR(4000), @sql NVARCHAR(MAX), @LastFullBackup NVARCHAR(500), @BackupFile NVARCHAR(500);
DECLARE @FileList TABLE (BackupFile NVARCHAR(255));

DECLARE @MoveDataLocation AS NVARCHAR(500), @MoveDataLocationName AS NVARCHAR(500), @MoveLogLocation AS NVARCHAR(500), @MoveLogLocationName AS NVARCHAR(500);

IF @RestoreDatabaseName IS NULL
	SET @RestoreDatabaseName = @Database;

-- get list of files 
SET @cmd = 'DIR /b '+ @BackupPathFull;
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

-- select * from @fileList
-- Find latest full backup 
SELECT @LastFullBackup = MAX(BackupFile)
FROM @FileList
WHERE BackupFile LIKE '%.bak'
    AND
    BackupFile LIKE '%'+@Database+'%';

DECLARE @FileListParameters TABLE
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

INSERT INTO @FileListParameters
EXEC ('RESTORE FILELISTONLY FROM DISK='''+@BackupPathFull + @LastFullBackup+'''');

DECLARE @MoveOption AS NVARCHAR(1000)= '';

IF @MoveFiles = 1
BEGIN
	WITH Files
    AS (
		SELECT ',MOVE '''+LogicalName+''' TO '''+
			CASE
				WHEN Type = 'D' THEN @MoveDataDrive
				WHEN Type = 'L' THEN @MoveLogDrive
			END+REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1)-1)) +'''' AS logicalcmds
		FROM @FileListParameters)
	SELECT @MoveOption = @MoveOption + logicalcmds
	FROM Files;
END;

IF @ContinueLogs = 0
BEGIN
	SET @sql = 'RESTORE DATABASE '+@RestoreDatabaseName+' FROM DISK = '''+@BackupPathFull + @LastFullBackup+ ''' WITH NORECOVERY, REPLACE' + @MoveOption+CHAR(13);
	PRINT @sql;
	EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';

	--get the backup completed data so we can apply tlogs from that point forwards
	DECLARE @Headers TABLE (
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

	INSERT INTO @Headers
	EXEC ('RESTORE HEADERONLY FROM DISK = '''+@BackupPathFull + @LastFullBackup+'''');

	DECLARE @BackupDateTime AS CHAR(15), @FullLastLSN BIGINT;

	SELECT @BackupDateTime = RIGHT(@LastFullBackup, 19)

	SELECT @FullLastLSN = CAST(LastLSN AS BIGINT) FROM @Headers WHERE BackupType = 1;
END;
ELSE
BEGIN
	DECLARE @DatabaseLastLSN BIGINT;

	SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS BIGINT)
	FROM master.sys.databases d
	JOIN master.sys.master_files f ON d.database_id = f.database_id
	WHERE d.name = @RestoreDatabaseName AND f.file_id = 1
END;

--Clear out table variables for translogs
DELETE FROM @FileList;
        
SET @cmd = 'DIR /b '+ @BackupPathLog;
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd;

--check for log backups 
DECLARE BackupFiles CURSOR FOR
	SELECT BackupFile
	FROM @FileList
	WHERE BackupFile LIKE '%.trn'
	  AND BackupFile LIKE '%'+@Database+'%'
	  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND LEFT(RIGHT(BackupFile, 19), 15) >= @BackupDateTime));

OPEN BackupFiles;

DECLARE @i tinyint = 1, @LogFirstLSN BIGINT, @LogLastLSN BIGINT;;

-- Loop through all the files for the database  
FETCH NEXT FROM BackupFiles INTO @BackupFile;
WHILE @@FETCH_STATUS = 0
BEGIN
	IF @i = 1
	BEGIN
		INSERT INTO @Headers
		EXEC ('RESTORE HEADERONLY FROM DISK = '''+@BackupPathLog + @BackupFile+'''');
		
		SELECT @LogFirstLSN = CAST(FirstLSN AS BIGINT), @LogLastLSN = CAST(LastLSN AS BIGINT) FROM @Headers WHERE BackupType = 2;

		IF (@ContinueLogs = 0 AND @LogFirstLSN <= @FullLastLSN AND @FullLastLSN <= @LogLastLSN) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN)
			SET @i = 2;

		DELETE FROM @Headers WHERE BackupType = 2;
	END;

	IF @i = 2
	BEGIN
		SET @sql = 'RESTORE LOG '+@RestoreDatabaseName+' FROM DISK = '''+@BackupPathLog + @BackupFile+''' WITH NORECOVERY'+CHAR(13);
		PRINT @sql
		EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE LOG', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
	END;

	FETCH NEXT FROM BackupFiles INTO @BackupFile;
END;

CLOSE BackupFiles;
DEALLOCATE BackupFiles;  

-- put database in a useable state 
IF @RunRecovery = 1
BEGIN
	SET @sql = 'RESTORE DATABASE '+@RestoreDatabaseName+' WITH RECOVERY';
	EXECUTE sp_executesql @sql;
END;
	    
 --Run checkdb against this database
IF @RunCheckDB = 1
	EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = @RestoreDatabaseName, @LogToTable = 'Y';

 --If test restore then blow the database away (be careful)
IF @TestRestore = 1
BEGIN
	SET @sql = 'DROP DATABASE '+@RestoreDatabaseName;
	EXECUTE sp_executesql @sql;
END;

