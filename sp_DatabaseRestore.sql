IF OBJECT_ID('dbo.sp_DatabaseRestore') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_DatabaseRestore AS RETURN 0;');
GO

ALTER PROCEDURE [dbo].[sp_DatabaseRestore]
	  @Database NVARCHAR(128) = NULL, 
	  @RestoreDatabaseName NVARCHAR(128) = NULL, 
	  @BackupPathFull NVARCHAR(MAX) = NULL, 
	  @BackupPathDiff NVARCHAR(MAX) = NULL, 
	  @BackupPathLog NVARCHAR(MAX) = NULL,
	  @MoveFiles BIT = 0, 
	  @MoveDataDrive NVARCHAR(260) = NULL, 
	  @MoveLogDrive NVARCHAR(260) = NULL, 
	  @TestRestore BIT = 0, 
	  @RunCheckDB BIT = 0, 
	  @RestoreDiff BIT = 0,
	  @ContinueLogs BIT = 0, 
	  @RunRecovery BIT = 0, 
	  @StopAt NVARCHAR(14) = NULL,
	  @OnlyLogsAfter NVARCHAR(14) = NULL,
	  @Debug INT = 0, 
	  @Help BIT = 0,
	  @VersionDate DATETIME = NULL OUTPUT
AS
SET NOCOUNT ON;

/*Versioning details*/
	DECLARE @Version NVARCHAR(30);
	SET @Version = '5.9';
	SET @VersionDate = '20171101';


IF @Help = 1

	BEGIN
	
		PRINT '
		/*
			sp_DatabaseRestore from http://FirstResponderKit.org
			
			This script will restore a database from a given file path.
		
			To learn more, visit http://FirstResponderKit.org where you can download new
			versions for free, watch training videos on how it works, get more info on
			the findings, contribute your own code, and more.
		
			Known limitations of this version:
			 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
			 - Tastes awful with marmite.
		
			Unknown limitations of this version:
			 - None.  (If we knew them, they would be known. Duh.)
		
		     Changes - for the full list of improvements and fixes in this version, see:
		     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
		
		    MIT License
			
			Copyright for portions of sp_Blitz are held by Microsoft as part of project 
			tigertoolbox and are provided under the MIT license:
			https://github.com/Microsoft/tigertoolbox
			   
			All other copyright for sp_Blitz are held by Brent Ozar Unlimited, 2017.
		
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
		
		*/
		';
		
		PRINT '
		/*
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
			@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
			@ContinueLogs = 0, 
			@RunRecovery = 0;
		
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
			@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
			@ContinueLogs = 1, 
			@RunRecovery = 0;
		
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
			@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
			@ContinueLogs = 1, 
			@RunRecovery = 1;
		
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
			@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
			@ContinueLogs = 0, 
			@RunRecovery = 1;
		
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
			@BackupPathDiff = ''D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\'',
			@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
			@RestoreDiff = 1,
			@ContinueLogs = 0, 
			@RunRecovery = 1;
		 
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''LogShipMe'', 
			@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
			@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
			@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
			@RestoreDiff = 1,
			@ContinueLogs = 0, 
			@RunRecovery = 1,
			@TestRestore = 1,
			@RunCheckDB = 1,
			@Debug = 0;
		
		--This example will restore the latest differential backup, and stop transaction logs at the specified date time.  It will also only print the commands.
		EXEC dbo.sp_DatabaseRestore 
			@Database = ''DBA'', 
			@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
			@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
			@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
			@RestoreDiff = 1,
			@ContinueLogs = 0, 
			@RunRecovery = 1,
			@StopAt = ''20170508201501'',
			@Debug = 1;
		
		Variables:
		
		@RestoreDiff - This variable is a flag for whether or not the script is expecting to restore differentials
		@StopAt - This variable is used to restore transaction logs to a specific date and time.  The format must be in YYYYMMDDHHMMSS.  The time is in military format.
		
		About Debug Modes:
		
		There are 3 Debug Modes.  Mode 0 is the default and will execute the script.  Debug 1 will print just the commands.  Debug 2 will print other useful information that
		has mostly been useful for troubleshooting.  Debug 2 needs to be expanded to make it more useful.
		*/
		';
	
	RETURN;
	
	END;



-- Get the SQL Server version number because the columns returned by RESTORE commands vary by version
-- Based on: https://www.brentozar.com/archive/2015/05/sql-server-version-detection/
-- Need to capture BuildVersion because RESTORE HEADERONLY changed with 2014 CU1, not RTM
DECLARE @ProductVersion AS NVARCHAR(20) = CAST(SERVERPROPERTY ('productversion') AS NVARCHAR(20));
DECLARE @MajorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 4) AS SMALLINT);
DECLARE @MinorVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 3) AS SMALLINT);
DECLARE @BuildVersion AS SMALLINT = CAST(PARSENAME(@ProductVersion, 2) AS SMALLINT);

IF @MajorVersion < 10
BEGIN
  RAISERROR('Sorry, DatabaseRestore doesn''t work on versions of SQL prior to 2008.', 15, 1);
  RETURN;
END;


DECLARE @cmd NVARCHAR(4000) = N'', --Holds xp_cmdshell command
        @sql NVARCHAR(MAX) = N'', --Holds executable SQL commands
        @LastFullBackup NVARCHAR(500) = N'', --Last full backup name
        @LastDiffBackup NVARCHAR(500) = N'', --Last diff backup name
        @LastDiffBackupDateTime NVARCHAR(500) = N'', --Last diff backup date
        @BackupFile NVARCHAR(500) = N'', --Name of backup file
        @BackupDateTime AS CHAR(15) = N'', --Used for comparisons to generate ordered backup files/create a stopat point
        @FullLastLSN NUMERIC(25, 0), --LSN for full
        @DiffLastLSN NUMERIC(25, 0), --LSN for diff
		@HeadersSQL AS NVARCHAR(4000) = N'', --Dynamic insert into #Headers table (deals with varying results from RESTORE FILELISTONLY across different versions)
		@MoveOption AS NVARCHAR(MAX)= N'', --If you need to move restored files to a different directory
		@DatabaseLastLSN NUMERIC(25, 0), --redo_start_lsn of the current database
		@i TINYINT = 1,  --Maintains loop to continue logs
		@LogFirstLSN NUMERIC(25, 0), --Holds first LSN in log backup headers
		@LogLastLSN NUMERIC(25, 0), --Holds last LSN in log backup headers
		@FileListParamSQL NVARCHAR(4000) = N''; --Holds INSERT list for #FileListParameters

DECLARE @FileList TABLE
(
    BackupFile NVARCHAR(255)
);


IF OBJECT_ID(N'tempdb..#FileListParameters') IS NOT NULL DROP TABLE #FileListParameters;
CREATE TABLE #FileListParameters
(
    LogicalName NVARCHAR(128) NOT NULL,
    PhysicalName NVARCHAR(260) NOT NULL,
    Type CHAR(1) NOT NULL,
    FileGroupName NVARCHAR(120) NULL,
    Size NUMERIC(20, 0) NOT NULL,
    MaxSize NUMERIC(20, 0) NOT NULL,
    FileID BIGINT NULL,
    CreateLSN NUMERIC(25, 0) NULL,
    DropLSN NUMERIC(25, 0) NULL,
    UniqueID UNIQUEIDENTIFIER NULL,
    ReadOnlyLSN NUMERIC(25, 0) NULL,
    ReadWriteLSN NUMERIC(25, 0) NULL,
    BackupSizeInBytes BIGINT NULL,
    SourceBlockSize INT NULL,
    FileGroupID INT NULL,
    LogGroupGUID UNIQUEIDENTIFIER NULL,
    DifferentialBaseLSN NUMERIC(25, 0) NULL,
    DifferentialBaseGUID UNIQUEIDENTIFIER NULL,
    IsReadOnly BIT NULL,
    IsPresent BIT NULL,
    TDEThumbprint VARBINARY(32) NULL,
    SnapshotUrl NVARCHAR(360) NULL
);


IF OBJECT_ID(N'tempdb..#Headers') IS NOT NULL DROP TABLE #Headers;
CREATE TABLE #Headers
(
    BackupName NVARCHAR(256),
    BackupDescription NVARCHAR(256),
    BackupType NVARCHAR(256),
    ExpirationDate NVARCHAR(256),
    Compressed NVARCHAR(256),
    Position NVARCHAR(256),
    DeviceType NVARCHAR(256),
    UserName NVARCHAR(256),
    ServerName NVARCHAR(256),
    DatabaseName NVARCHAR(256),
    DatabaseVersion NVARCHAR(256),
    DatabaseCreationDate NVARCHAR(256),
    BackupSize NVARCHAR(256),
    FirstLSN NVARCHAR(256),
    LastLSN NVARCHAR(256),
    CheckpointLSN NVARCHAR(256),
    DatabaseBackupLSN NVARCHAR(256),
    BackupStartDate NVARCHAR(256),
    BackupFinishDate NVARCHAR(256),
    SortOrder NVARCHAR(256),
    CodePage NVARCHAR(256),
    UnicodeLocaleId NVARCHAR(256),
    UnicodeComparisonStyle NVARCHAR(256),
    CompatibilityLevel NVARCHAR(256),
    SoftwareVendorId NVARCHAR(256),
    SoftwareVersionMajor NVARCHAR(256),
    SoftwareVersionMinor NVARCHAR(256),
    SoftwareVersionBuild NVARCHAR(256),
    MachineName NVARCHAR(256),
    Flags NVARCHAR(256),
    BindingID NVARCHAR(256),
    RecoveryForkID NVARCHAR(256),
    Collation NVARCHAR(256),
    FamilyGUID NVARCHAR(256),
    HasBulkLoggedData NVARCHAR(256),
    IsSnapshot NVARCHAR(256),
    IsReadOnly NVARCHAR(256),
    IsSingleUser NVARCHAR(256),
    HasBackupChecksums NVARCHAR(256),
    IsDamaged NVARCHAR(256),
    BeginsLogChain NVARCHAR(256),
    HasIncompleteMetaData NVARCHAR(256),
    IsForceOffline NVARCHAR(256),
    IsCopyOnly NVARCHAR(256),
    FirstRecoveryForkID NVARCHAR(256),
    ForkPointLSN NVARCHAR(256),
    RecoveryModel NVARCHAR(256),
    DifferentialBaseLSN NVARCHAR(256),
    DifferentialBaseGUID NVARCHAR(256),
    BackupTypeDescription NVARCHAR(256),
    BackupSetGUID NVARCHAR(256),
    CompressedBackupSize NVARCHAR(256),
    Containment NVARCHAR(256),
    KeyAlgorithm NVARCHAR(32),
    EncryptorThumbprint VARBINARY(20),
    EncryptorType NVARCHAR(32),
    --
    -- Seq added to retain order by
    --
    Seq INT NOT NULL IDENTITY(1, 1)
);

/*

Correct paths in case people forget a final "\" 

*/

/*Full*/

IF (SELECT RIGHT(@BackupPathFull, 1)) <> '\' --Has to end in a '\'
	BEGIN
		RAISERROR('Fixing @BackupPathFull to add a "\"', 0, 1) WITH NOWAIT;
		SET @BackupPathFull += N'\';
	END;

/*Diff*/
IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '\' --Has to end in a '\'
	BEGIN
		RAISERROR('Fixing @BackupPathDiff to add a "\"', 0, 1) WITH NOWAIT;
		SET @BackupPathDiff += N'\';
	END;

/*Log*/
IF (SELECT RIGHT(@BackupPathLog, 1)) <> '\' --Has to end in a '\'
	BEGIN
		RAISERROR('Fixing @BackupPathLog to add a "\"', 0, 1) WITH NOWAIT;
		SET @BackupPathLog += N'\';
	END;

/*Move Data File*/
IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '\' --Has to end in a '\'
	BEGIN
		RAISERROR('Fixing @MoveDataDrive to add a "\"', 0, 1) WITH NOWAIT;
		SET @MoveDataDrive += N'\';
	END;

/*Move Log File*/
IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '\' --Has to end in a '\'
	BEGIN
		RAISERROR('Fixing @MoveDataDrive to add a "\"', 0, 1) WITH NOWAIT;
		SET @MoveLogDrive += N'\';
	END;



IF @RestoreDatabaseName IS NULL
	BEGIN
		SET @RestoreDatabaseName = QUOTENAME(@Database);
	END
ELSE
	BEGIN
		SET @RestoreDatabaseName = QUOTENAME(@RestoreDatabaseName)
	END


IF @BackupPathFull IS NOT NULL

BEGIN

-- get list of files 
SET @cmd = N'DIR /b "' + @BackupPathFull + N'"';

			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathFull';
				PRINT @cmd;
			END;  


INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

/*Sanity check folders*/

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'The system cannot find the path specified.'
		OR fl.BackupFile = 'File Not Found'
		) = 1

		BEGIN
	
			RAISERROR('No rows were returned for that database\path', 0, 1) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'Access is denied.'
		) = 1

		BEGIN
	
			RAISERROR('Access is denied to %s', 16, 1, @BackupPathFull) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		) = 1
	AND (
		SELECT COUNT(*) 
		FROM @FileList AS fl 							
		WHERE fl.BackupFile IS NULL
		) = 1

		BEGIN
	
			RAISERROR('That directory appears to be empty', 0, 1) WITH NOWAIT;
	
			RETURN;
	
		END

/*End folder sanity check*/

-- Find latest full backup 
SELECT @LastFullBackup = MAX(BackupFile)
FROM @FileList
WHERE BackupFile LIKE N'%.bak'
    AND
    BackupFile LIKE N'%' + @Database + N'%';

	IF @Debug = 1
	BEGIN
		SELECT *
		FROM   @FileList;
	END;



SET @FileListParamSQL = 
  N'INSERT INTO #FileListParameters WITH (TABLOCK)
   (LogicalName, PhysicalName, Type, FileGroupName, Size, MaxSize, FileID, CreateLSN, DropLSN
   ,UniqueID, ReadOnlyLSN, ReadWriteLSN, BackupSizeInBytes, SourceBlockSize, FileGroupID, LogGroupGUID
   ,DifferentialBaseLSN, DifferentialBaseGUID, IsReadOnly, IsPresent, TDEThumbprint';

IF @MajorVersion >= 13
	BEGIN
		SET @FileListParamSQL += N', SnapshotUrl';
	END;

SET @FileListParamSQL += N')' + NCHAR(13) + NCHAR(10);
SET @FileListParamSQL += N'EXEC (''RESTORE FILELISTONLY FROM DISK=''''{Path}'''''')';

SET @sql = REPLACE(@FileListParamSQL, N'{Path}', @BackupPathFull + @LastFullBackup);
		
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for INSERT to #FileListParameters: @BackupPathFull + @LastFullBackup';
			PRINT @sql;
		END;

EXEC (@sql);

	IF @Debug = 1
		BEGIN
			SELECT '#FileListParameters' AS table_name, * FROM #FileListParameters
		END


SET @HeadersSQL = 
N'INSERT INTO #Headers WITH (TABLOCK)
  (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName
  ,DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
  ,BackupStartDate, BackupFinishDate, SortOrder, CodePage, UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel
  ,SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingID
  ,RecoveryForkID, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums
  ,IsDamaged, BeginsLogChain, HasIncompleteMetaData, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN
  ,RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize';
  
IF @MajorVersion >= 11
  SET @HeadersSQL += NCHAR(13) + NCHAR(10) + N', Containment';


IF @MajorVersion >= 13 OR (@MajorVersion = 12 AND @BuildVersion >= 2342)
  SET @HeadersSQL += N', KeyAlgorithm, EncryptorThumbprint, EncryptorType';

SET @HeadersSQL += N')' + NCHAR(13) + NCHAR(10);
SET @HeadersSQL += N'EXEC (''RESTORE HEADERONLY FROM DISK=''''{Path}'''''')';


IF @MoveFiles = 1
	BEGIN
		
		RAISERROR('@MoveFiles = 1, adjusting paths', 0, 1) WITH NOWAIT;
	
		WITH Files
	    AS (
			SELECT N', MOVE ''' + LogicalName + N''' TO ''' +
				CASE
					WHEN Type = 'D' THEN @MoveDataDrive
					WHEN Type = 'L' THEN @MoveLogDrive
				END + CASE WHEN @Database = @RestoreDatabaseName THEN REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1)) + '''' 
					  ELSE REPLACE(REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1)), @Database, SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) -2)) + '''' 
					  END AS logicalcmds
			FROM #FileListParameters)
		
		SELECT @MoveOption = @MoveOption + Files.logicalcmds
		FROM Files;
		
		IF @Debug = 1 PRINT @MoveOption

	END;


IF @ContinueLogs = 0
	BEGIN

		RAISERROR('@ContinueLogs set to 0', 0, 1) WITH NOWAIT;
	
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathFull + @LastFullBackup + N''' WITH NORECOVERY, REPLACE' + @MoveOption + NCHAR(13);
		
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathFull, @LastFullBackup, @MoveOption';
			PRINT @sql;
		END;
		
		IF @Debug IN (0, 1)
			EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
	
	  --get the backup completed data so we can apply tlogs from that point forwards                                                   
	    SET @sql = REPLACE(@HeadersSQL, N'{Path}', @BackupPathFull + @LastFullBackup);
			
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for get backup completed data: @BackupPathFull, @LastFullBackup';
			PRINT @sql;
		END;
	    
	    EXECUTE (@sql);
	    
	      --setting the @BackupDateTime to a numeric string so that it can be used in comparisons
		  SET @BackupDateTime = REPLACE(LEFT(RIGHT(@LastFullBackup, 19),15), '_', '');
	    
		  SELECT @FullLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) FROM #Headers WHERE BackupType = 1;  

			IF @Debug = 1
			BEGIN
				IF @BackupDateTime IS NULL PRINT '@BackupDateTime is NULL for REPLACE: @LastFullBackup';
				PRINT @BackupDateTime;
			END;                                            
	    
	END;

ELSE

	BEGIN
		
		SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS NUMERIC(25, 0))
		FROM master.sys.databases d
		JOIN master.sys.master_files f ON d.database_id = f.database_id
		WHERE d.name = SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) - 2) AND f.file_id = 1;
	
	END;

--Clear out table variables for differential
DELETE FROM @FileList;

END


IF @BackupPathDiff IS NOT NULL

BEGIN 

-- get list of files 
SET @cmd = N'DIR /b "'+ @BackupPathDiff + N'"';

	IF @Debug = 1
	BEGIN
		IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathDiff check';
		PRINT @cmd;
	END;  

	IF @Debug = 1
	BEGIN
		SELECT *
		FROM   @FileList;
	END;


INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

/*Sanity check folders*/

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'The system cannot find the path specified.'
		OR fl.BackupFile = 'File Not Found'
		) = 1

		BEGIN
	
			RAISERROR('No rows were returned for that database\path', 0, 1) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'Access is denied.'
		) = 1

		BEGIN
	
			RAISERROR('Access is denied to %s', 16, 1, @BackupPathDiff) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		) = 1
	AND (
		SELECT COUNT(*) 
		FROM @FileList AS fl 							
		WHERE fl.BackupFile IS NULL
		) = 1

		BEGIN
	
			RAISERROR('That directory appears to be empty', 0, 1) WITH NOWAIT;
	
			RETURN;
	
		END

/*End folder sanity check*/


-- Find latest diff backup 
SELECT @LastDiffBackup = MAX(BackupFile)
FROM @FileList
WHERE BackupFile LIKE N'%.bak'
    AND
    BackupFile LIKE N'%' + @Database + '%';
	
	--set the @BackupDateTime so that it can be used for comparisons
	SET @BackupDateTime = REPLACE(@BackupDateTime, '_', '');
	SET @LastDiffBackupDateTime = REPLACE(LEFT(RIGHT(@LastDiffBackup, 19),15), '_', '');


IF @RestoreDiff = 1 AND @BackupDateTime < @LastDiffBackupDateTime
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathDiff + @LastDiffBackup + N''' WITH NORECOVERY' + NCHAR(13);
		
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;  

		
		IF @Debug IN (0, 1)
			EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
		
		--get the backup completed data so we can apply tlogs from that point forwards                                                   
		SET @sql = REPLACE(@HeadersSQL, N'{Path}', @BackupPathDiff + @LastDiffBackup);
		
		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @BackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;  
		
		EXECUTE (@sql);

			IF @Debug = 1
				BEGIN
					SELECT '#Headers' AS table_name, * FROM #Headers AS h
				END
		
		--set the @BackupDateTime to the date time on the most recent differential	
		SET @BackupDateTime = @LastDiffBackupDateTime;
		
		SELECT @DiffLastLSN = CAST(LastLSN AS NUMERIC(25, 0))
		FROM #Headers 
		WHERE BackupType = 5;                                                  
	END;

--Clear out table variables for translogs
DELETE FROM @FileList;
   
 END      

 IF @BackupPathLog IS NOT NULL

BEGIN

SET @cmd = N'DIR /b "' + @BackupPathLog + N'"';

		IF @Debug = 1
		BEGIN
			IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathLog check';
			PRINT @cmd;
		END; 

		IF @Debug = 1
		BEGIN
			SELECT *
			FROM   @FileList;
		END;


INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd;

/*Sanity check folders*/

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'The system cannot find the path specified.'
		OR fl.BackupFile = 'File Not Found'
		) = 1

		BEGIN
	
			RAISERROR('No rows were returned for that database\path', 0, 1) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		WHERE fl.BackupFile = 'Access is denied.'
		) = 1

		BEGIN
	
			RAISERROR('Access is denied to %s', 16, 1, @BackupPathLog) WITH NOWAIT;

		END;

	IF (
		SELECT COUNT(*) 
		FROM @FileList AS fl 
		) = 1
	AND (
		SELECT COUNT(*) 
		FROM @FileList AS fl 							
		WHERE fl.BackupFile IS NULL
		) = 1

		BEGIN
	
			RAISERROR('That directory appears to be empty', 0, 1) WITH NOWAIT;
	
			RETURN;
	
		END

/*End folder sanity check*/

IF (@OnlyLogsAfter IS NOT NULL)
	BEGIN
	
	RAISERROR('@OnlyLogsAfter is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;
	
		DELETE fl
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%' 
		AND REPLACE(LEFT(RIGHT(fl.BackupFile, 19), 15),'_','') < @OnlyLogsAfter
	
END



--check for log backups
IF(@StopAt IS NULL AND @OnlyLogsAfter IS NULL)
	BEGIN 
		DECLARE BackupFiles CURSOR FOR
			SELECT BackupFile
			FROM @FileList
			WHERE BackupFile LIKE N'%.trn'
			  AND BackupFile LIKE N'%' + @Database + N'%'
			  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @BackupDateTime))
			  ORDER BY BackupFile;
		
		OPEN BackupFiles;
	END;


IF (@StopAt IS NULL AND @OnlyLogsAfter IS NOT NULL)	
	BEGIN
		DECLARE BackupFiles CURSOR FOR
			SELECT BackupFile
			FROM @FileList
			WHERE BackupFile LIKE N'%.trn'
			  AND BackupFile LIKE N'%' + @Database + N'%'
			  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @OnlyLogsAfter))
			  ORDER BY BackupFile;
		
		OPEN BackupFiles;
	END


IF (@StopAt IS NOT NULL AND @OnlyLogsAfter IS NULL)	
	BEGIN
		DECLARE BackupFiles CURSOR FOR
			SELECT BackupFile
			FROM @FileList
			WHERE BackupFile LIKE N'%.trn'
			  AND BackupFile LIKE N'%' + @Database + N'%'
			  AND (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @BackupDateTime) AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt)
			  ORDER BY BackupFile;
		
		OPEN BackupFiles;
	END;


-- Loop through all the files for the database  
FETCH NEXT FROM BackupFiles INTO @BackupFile;
	WHILE @@FETCH_STATUS = 0
		BEGIN
			IF @i = 1
			
			BEGIN
		    SET @sql = REPLACE(@HeadersSQL, N'{Path}', @BackupPathLog + @BackupFile);
			
				IF @Debug = 1
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @HeadersSQL, @BackupPathLog, @BackupFile';
					PRINT @sql;
				END; 
			
			EXECUTE (@sql);
				
				SELECT @LogFirstLSN = CAST(FirstLSN AS NUMERIC(25, 0)), 
					   @LogLastLSN = CAST(LastLSN AS NUMERIC(25, 0)) 
					   FROM #Headers 
					   WHERE BackupType = 2;
		
				IF (@ContinueLogs = 0 AND @LogFirstLSN <= @FullLastLSN AND @FullLastLSN <= @LogLastLSN AND @RestoreDiff = 0) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 0)
					SET @i = 2;
				
				IF (@ContinueLogs = 0 AND @LogFirstLSN <= @DiffLastLSN AND @DiffLastLSN <= @LogLastLSN AND @RestoreDiff = 1) OR (@ContinueLogs = 1 AND @LogFirstLSN <= @DatabaseLastLSN AND @DatabaseLastLSN < @LogLastLSN AND @RestoreDiff = 1)
					SET @i = 2;
				
				DELETE FROM #Headers WHERE BackupType = 2;


			END;

			IF @i = 1
				BEGIN

					IF @Debug = 1 RAISERROR('No Log to Restore', 0, 1) WITH NOWAIT;

				END

			IF @i = 2
			BEGIN

				RAISERROR('@i set to 2, restoring logs', 0, 1) WITH NOWAIT;
				
				SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathLog + @BackupFile + N''' WITH NORECOVERY' + NCHAR(13);
				
					IF @Debug = 1
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for RESTORE LOG: @RestoreDatabaseName, @BackupPathLog, @BackupFile';
						PRINT @sql;
					END; 
				
					IF @Debug IN (0, 1)
						EXECUTE @sql = [dbo].[CommandExecute] @Command = @sql, @CommandType = 'RESTORE LOG', @Mode = 1, @DatabaseName = @Database, @LogToTable = 'Y', @Execute = 'Y';
			END;
			
			FETCH NEXT FROM BackupFiles INTO @BackupFile;
		END;
	
	CLOSE BackupFiles;

DEALLOCATE BackupFiles;  

				IF @Debug = 1
				BEGIN
					SELECT '#Headers' AS table_name, * FROM #Headers AS h
				END


END

-- put database in a useable state 
IF @RunRecovery = 1
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' WITH RECOVERY' + NCHAR(13);

			IF @Debug = 1
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @RestoreDatabaseName';
				PRINT @sql;
			END; 

		IF @Debug IN (0, 1)
			EXECUTE sp_executesql @sql;
	END;
	    

 --Run checkdb against this database
IF @RunCheckDB = 1
	BEGIN
		SET @sql = N'EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = ' + @RestoreDatabaseName + N', @LogToTable = ''Y''' + NCHAR(13);
			
			IF @Debug = 1
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for Run Integrity Check: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1)
			EXECUTE sys.sp_executesql @sql;
	END;

 --If test restore then blow the database away (be careful)
IF @TestRestore = 1
	BEGIN
		SET @sql = N'DROP DATABASE ' + @RestoreDatabaseName + NCHAR(13);
			
			IF @Debug = 1
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for DROP DATABASE: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1)
			EXECUTE sp_executesql @sql;

	END;

GO 
