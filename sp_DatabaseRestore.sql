IF OBJECT_ID('dbo.sp_DatabaseRestore') IS NULL
	EXEC ('CREATE PROCEDURE dbo.sp_DatabaseRestore AS RETURN 0;');
GO
ALTER PROCEDURE [dbo].[sp_DatabaseRestore]
    @Database NVARCHAR(128) = NULL, 
    @RestoreDatabaseName NVARCHAR(128) = NULL, 
    @BackupPathFull NVARCHAR(260) = NULL, 
    @BackupPathDiff NVARCHAR(260) = NULL, 
    @BackupPathLog NVARCHAR(260) = NULL,
    @MoveFiles BIT = 1, 
    @MoveDataDrive NVARCHAR(260) = NULL, 
    @MoveLogDrive NVARCHAR(260) = NULL, 
    @MoveFilestreamDrive NVARCHAR(260) = NULL,
	@BufferCount INT = NULL,
	@MaxTransferSize INT = NULL,
	@BlockSize INT = NULL,
    @TestRestore BIT = 0, 
    @RunCheckDB BIT = 0, 
    @RestoreDiff BIT = 0,
    @ContinueLogs BIT = 0, 
    @StandbyMode BIT = 0,
    @StandbyUndoPath NVARCHAR(MAX) = NULL,
    @RunRecovery BIT = 0, 
    @ForceSimpleRecovery BIT = 0,
    @ExistingDBAction tinyint = 0,
    @StopAt NVARCHAR(14) = NULL,
    @OnlyLogsAfter NVARCHAR(14) = NULL,
    @SimpleFolderEnumeration BIT = 0,
	@DatabaseOwner sysname = NULL,
    @Execute CHAR(1) = Y,
    @Debug INT = 0, 
    @Help BIT = 0,
    @Version     VARCHAR(30) = NULL OUTPUT,
	@VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0
AS
SET NOCOUNT ON;

/*Versioning details*/

SELECT @Version = '7.94', @VersionDate = '20200324';

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

 
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
			
		Copyright (c) 2020 Brent Ozar Unlimited
		
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

	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'',
		@StandbyMode = 1,
		@StandbyUndoPath = ''D:\Data\'',
		@ContinueLogs = 1, 
		@RunRecovery = 0,
		@Debug = 0;
		
	--This example will restore the latest differential backup, and stop transaction logs at the specified date time.  It will execute and print debug information.
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

	--This example NOT execute the restore.  Commands will be printed in a copy/paste ready format only
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@TestRestore = 1,
		@RunCheckDB = 1,
		@Debug = 0,
		@Execute = ''N'';
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
		@MoveOption AS NVARCHAR(MAX) = N'', --If you need to move restored files to a different directory
		@LogRecoveryOption AS NVARCHAR(MAX) = N'', --Holds the option to cause logs to be restored in standby mode or with no recovery
		@DatabaseLastLSN NUMERIC(25, 0), --redo_start_lsn of the current database
		@i TINYINT = 1,  --Maintains loop to continue logs
		@LogFirstLSN NUMERIC(25, 0), --Holds first LSN in log backup headers
		@LogLastLSN NUMERIC(25, 0), --Holds last LSN in log backup headers
		@FileListParamSQL NVARCHAR(4000) = N'', --Holds INSERT list for #FileListParameters
		@BackupParameters nvarchar(500) = N'', --Used to save BlockSize, MaxTransferSize and BufferCount
        @RestoreDatabaseID smallint;    --Holds DB_ID of @RestoreDatabaseName

DECLARE @FileListSimple TABLE (
    BackupFile NVARCHAR(255) NOT NULL, 
    depth int NOT NULL, 
    [file] int NOT NULL
);

DECLARE @FileList TABLE (
    BackupFile NVARCHAR(255) NULL
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
IF (SELECT RIGHT(@BackupPathFull, 1)) <> '\' AND CHARINDEX('\', @BackupPathFull) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'\';
END;
ELSE IF (SELECT RIGHT(@BackupPathFull, 1)) <> '/' AND CHARINDEX('/', @BackupPathFull) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'/';
END;
/*Diff*/
IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '\' AND CHARINDEX('\', @BackupPathDiff) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'\';
END;
ELSE IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '/' AND CHARINDEX('/', @BackupPathDiff) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'/';
END;
/*Log*/
IF (SELECT RIGHT(@BackupPathLog, 1)) <> '\' AND CHARINDEX('\', @BackupPathLog) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'\';
END;
ELSE IF (SELECT RIGHT(@BackupPathLog, 1)) <> '/' AND CHARINDEX('/', @BackupPathLog) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'/';
END;
/*Move Data File*/
IF NULLIF(@MoveDataDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default data drive for @MoveDataDrive', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '\' AND CHARINDEX('\', @MoveDataDrive) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'\';
END;
ELSE IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '/' AND CHARINDEX('/', @MoveDataDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'/';
END;
/*Move Log File*/
IF NULLIF(@MoveLogDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default log drive for @MoveLogDrive', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive  = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '\' AND CHARINDEX('\', @MoveLogDrive) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveLogDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'\';
END;
ELSE IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '/' AND CHARINDEX('/', @MoveLogDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing@MoveLogDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'/';
END;
/*Move Filestream File*/
IF NULLIF(@MoveFilestreamDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Setting default data drive for @MoveFilestreamDrive', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive  = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) <> '\' AND CHARINDEX('\', @MoveFilestreamDrive) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'\';
END;
ELSE IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) <> '/' AND CHARINDEX('/', @MoveFilestreamDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'/';
END;
/*Standby Undo File*/
IF (SELECT RIGHT(@StandbyUndoPath, 1)) <> '\' AND CHARINDEX('\', @StandbyUndoPath) > 0 --Has to end in a '\'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "\"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'\';
END;
ELSE IF (SELECT RIGHT(@StandbyUndoPath, 1)) <> '/' AND CHARINDEX('/', @StandbyUndoPath) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "/"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'/';
END;
IF @RestoreDatabaseName IS NULL
BEGIN
	SET @RestoreDatabaseName = @Database;
END;

/*check input parameters*/
IF NOT @MaxTransferSize IS NULL
BEGIN
	IF @MaxTransferSize > 4194304
	BEGIN
		RAISERROR('@MaxTransferSize can not be greater then 4194304', 0, 1) WITH NOWAIT;
	END

	IF @MaxTransferSize % 64 <> 0
	BEGIN
		RAISERROR('@MaxTransferSize has to be a multiple of 65536', 0, 1) WITH NOWAIT;
	END
END;

IF NOT @BlockSize IS NULL
BEGIN
	IF @BlockSize NOT IN (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
	BEGIN
		RAISERROR('Supported values for @BlockSize are 512, 1024, 2048, 4096, 8192, 16384, 32768, and 65536', 0, 1) WITH NOWAIT;
	END
END

SET @RestoreDatabaseID = DB_ID(@RestoreDatabaseName);
SET @RestoreDatabaseName = QUOTENAME(@RestoreDatabaseName);
--If xp_cmdshell is disabled, force use of xp_dirtree
IF NOT EXISTS (SELECT * FROM sys.configurations WHERE name = 'xp_cmdshell' AND value_in_use = 1)
    SET @SimpleFolderEnumeration = 1;

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

IF @BackupPathFull IS NOT NULL
BEGIN
    IF @SimpleFolderEnumeration = 1
    BEGIN    -- Get list of files 
        INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @BackupPathFull, 1, 1;
        INSERT @FileList (BackupFile) SELECT BackupFile FROM @FileListSimple;
    END
    ELSE
    BEGIN
        SET @cmd = N'DIR /b "' + @BackupPathFull + N'"';
		IF @Debug = 1
		BEGIN
			IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathFull';
			PRINT @cmd;
		END;  
        INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd; 
    END;
    
    IF @Debug = 1
    BEGIN
	    SELECT BackupFile FROM @FileList;
    END;
    IF @SimpleFolderEnumeration = 1
    BEGIN
        /*Check what we can*/
        IF NOT EXISTS (SELECT * FROM @FileList)
	    BEGIN
		    RAISERROR('(FULL) No rows were returned for that database in path %s', 16, 1, @BackupPathFull) WITH NOWAIT;
            RETURN;
	    END;
    END
    ELSE
    BEGIN
        /*Full Sanity check folders*/
        IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The system cannot find the path specified.'
		    OR fl.BackupFile = 'File Not Found'
		    ) = 1
	    BEGIN
		    RAISERROR('(FULL) No rows or bad value for path %s', 16, 1, @BackupPathFull) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'Access is denied.'
		    ) = 1
	    BEGIN
		    RAISERROR('(FULL) Access is denied to %s', 16, 1, @BackupPathFull) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    ) = 1
	        AND 
            (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 							
		    WHERE fl.BackupFile IS NULL
		    ) = 1
		    BEGIN
			    RAISERROR('(FULL) Empty directory %s', 16, 1, @BackupPathFull) WITH NOWAIT;
			    RETURN;
		    END
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The user name or password is incorrect.'
		    ) = 1
		    BEGIN
			    RAISERROR('(FULL) Incorrect user name or password for %s', 16, 1, @BackupPathFull) WITH NOWAIT;
                RETURN;
		    END;
    END;
    /*End folder sanity check*/

    -- Find latest full backup 
    SELECT @LastFullBackup = MAX(BackupFile)
    FROM @FileList
    WHERE BackupFile LIKE N'%.bak'
        AND
        BackupFile LIKE N'%' + @Database + N'%'
	    AND
	    (@StopAt IS NULL OR REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt);

    /*	To get all backups that belong to the same set we can do two things:
		    1.	RESTORE HEADERONLY of ALL backup files in the folder and look for BackupSetGUID.
			    Backups that belong to the same split will have the same BackupSetGUID.
		    2.	Olla Hallengren's solution appends file index at the end of the name:
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_1.bak
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_2.bak
			    SQLSERVER1_TEST_DB_FULL_20180703_213211_N.bak
			    We can and find all related files with the same timestamp but different index.
			    This option is simpler and requires less changes to this procedure */

    IF @LastFullBackup IS NULL
    BEGIN
        RAISERROR('No backups for "%s" found in "%s"', 16, 1, @Database, @BackupPathFull) WITH NOWAIT;
        RETURN;
    END;

    SELECT BackupFile
    INTO #SplitBackups
    FROM @FileList
    WHERE LEFT(BackupFile,LEN(BackupFile)-PATINDEX('%[_]%',REVERSE(BackupFile))) = LEFT(@LastFullBackup,LEN(@LastFullBackup)-PATINDEX('%[_]%',REVERSE(@LastFullBackup)))
    AND PATINDEX('%[_]%',REVERSE(@LastFullBackup)) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Olla only supports up to 64 file split.

    -- File list can be obtained by running RESTORE FILELISTONLY of any file from the given BackupSet therefore we do not have to cater for split backups when building @FileListParamSQL

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
	    SELECT '#SplitBackups' AS table_name, * FROM #SplitBackups
    END

    --get the backup completed data so we can apply tlogs from that point forwards                                                   
    SET @sql = REPLACE(@HeadersSQL, N'{Path}', @BackupPathFull + @LastFullBackup);
    IF @Debug = 1
    BEGIN
	    IF @sql IS NULL PRINT '@sql is NULL for get backup completed data: @BackupPathFull, @LastFullBackup';
	    PRINT @sql;
    END;
    EXECUTE (@sql);
    IF @Debug = 1
    BEGIN
	    SELECT '#Headers' AS table_name, @LastFullBackup AS FullBackupFile, * FROM #Headers
    END;

    --Ensure we are looking at the expected backup, but only if we expect to restore a FULL backups
    IF NOT EXISTS (SELECT * FROM #Headers h WHERE h.DatabaseName = @Database)
    BEGIN
        RAISERROR('Backupfile "%s" does not match @Database parameter "%s"', 16, 1, @LastFullBackup, @Database) WITH NOWAIT;
        RETURN;
    END;

	IF NOT @BufferCount IS NULL
	BEGIN
		SET @BackupParameters += N', BufferCount=' + cast(@BufferCount as NVARCHAR(10))
	END

	IF NOT @MaxTransferSize IS NULL
	BEGIN
		SET @BackupParameters += N', MaxTransferSize=' + cast(@MaxTransferSize as NVARCHAR(7))
	END

	IF NOT @BlockSize IS NULL
	BEGIN
		SET @BackupParameters += N', BlockSize=' + cast(@BlockSize as NVARCHAR(5))
	END

    IF @MoveFiles = 1
    BEGIN
	    IF @Execute = 'Y' RAISERROR('@MoveFiles = 1, adjusting paths', 0, 1) WITH NOWAIT;

	    WITH Files
	    AS (
		    SELECT
			    CASE
				    WHEN Type = 'D' THEN @MoveDataDrive
				    WHEN Type = 'L' THEN @MoveLogDrive
				    WHEN Type = 'S' THEN @MoveFilestreamDrive
			    END + CASE 
                        WHEN @Database = @RestoreDatabaseName THEN REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1))
					    ELSE REPLACE(REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX('\', REVERSE(PhysicalName), 1) -1)), @Database, SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) -2))
					    END AS TargetPhysicalName,
                    PhysicalName,
                    LogicalName
		    FROM #FileListParameters)
	    SELECT @MoveOption = @MoveOption + N', MOVE ''' + Files.LogicalName + N''' TO ''' + Files.TargetPhysicalName + ''''
	    FROM Files
        WHERE Files.TargetPhysicalName <> Files.PhysicalName;
		
	    IF @Debug = 1 PRINT @MoveOption
    END;

    /*Process @ExistingDBAction flag */
    IF @ExistingDBAction BETWEEN 1 AND 3
    BEGIN
        IF @RestoreDatabaseID IS NOT NULL
        BEGIN
            IF @ExistingDBAction = 1
            BEGIN
                RAISERROR('Setting single user', 0, 1) WITH NOWAIT;
                SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ' + NCHAR(13);
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for SINGLE_USER';
			        PRINT @sql;
		        END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
			        EXECUTE master.sys.sp_executesql @stmt = @sql;
            END
            IF @ExistingDBAction IN (2, 3)
            BEGIN
                RAISERROR('Killing connections', 0, 1) WITH NOWAIT;
                SET @sql = N'/* Kill connections */' + NCHAR(13);
                SELECT 
                    @sql = @sql + N'KILL ' + CAST(spid as nvarchar(5)) + N';' + NCHAR(13)
                FROM
                    --database_ID was only added to sys.dm_exec_sessions in SQL Server 2012 but we need to support older
                    sys.sysprocesses
                WHERE
                    dbid = @RestoreDatabaseID;
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for Kill connections';
			        PRINT @sql;
		        END;
                IF @Debug IN (0, 1) AND @Execute = 'Y'
			        EXECUTE master.sys.sp_executesql @stmt = @sql;
            END
            IF @ExistingDBAction = 3
            BEGIN
                RAISERROR('Dropping database', 0, 1) WITH NOWAIT;
            
                SET @sql = N'DROP DATABASE ' + @RestoreDatabaseName + NCHAR(13);
                IF @Debug = 1 OR @Execute = 'N'
		        BEGIN
			        IF @sql IS NULL PRINT '@sql is NULL for DROP DATABASE';
			        PRINT @sql;
		        END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
			        EXECUTE master.sys.sp_executesql @stmt = @sql;
            END
        END
        ELSE
            RAISERROR('@ExistingDBAction > 0, but no existing @RestoreDatabaseName', 0, 1) WITH NOWAIT;
    END
    ELSE
        IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@ExistingDBAction %u so do nothing', 0, 1, @ExistingDBAction) WITH NOWAIT;

    IF @ContinueLogs = 0
    BEGIN
	    IF @Execute = 'Y' RAISERROR('@ContinueLogs set to 0', 0, 1) WITH NOWAIT;
	
	    /* now take split backups into account */
	    IF (SELECT COUNT(*) FROM #SplitBackups) > 0
        BEGIN
            RAISERROR('Split backups found', 0, 1) WITH NOWAIT;

            SELECT
                @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM '
                       + STUFF(
                             (SELECT
                                  CHAR(10) + ',DISK=''' + @BackupPathFull + BackupFile + ''''
                              FROM
                                  #SplitBackups
                              ORDER BY
                                  BackupFile
                             FOR XML PATH('')),
                             1,
                             2,
                             '') + N' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption + NCHAR(13);
        END;
	    ELSE
		BEGIN
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathFull + @LastFullBackup + N''' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption + NCHAR(13);
		END
	    IF (@StandbyMode = 1)
	    BEGIN
		    IF (@StandbyUndoPath IS NULL)
			BEGIN
			    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END
	        ELSE
	        BEGIN
		        SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathFull + @LastFullBackup + N''' WITH  REPLACE' + @BackupParameters + @MoveOption + N' , STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''' + NCHAR(13);
	        END
        END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathFull, @LastFullBackup, @MoveOption';
			PRINT @sql;
		END;
			
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;

    -- We already loaded #Headers above

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
END;

IF @BackupPathFull IS NULL AND @ContinueLogs = 1
BEGIN
		
	SELECT @DatabaseLastLSN = CAST(f.redo_start_lsn AS NUMERIC(25, 0))
	FROM master.sys.databases d
	JOIN master.sys.master_files f ON d.database_id = f.database_id
	WHERE d.name = SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) - 2) AND f.file_id = 1;
	
END;

IF @BackupPathDiff IS NOT NULL
BEGIN 
    DELETE FROM @FileList;
    DELETE FROM @FileListSimple;
    IF @SimpleFolderEnumeration = 1
    BEGIN    -- Get list of files 
        INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @BackupPathDiff, 1, 1;
        INSERT @FileList (BackupFile) SELECT BackupFile FROM @FileListSimple;
    END
    ELSE
    BEGIN
        SET @cmd = N'DIR /b "' + @BackupPathDiff + N'"';
		IF @Debug = 1
		BEGIN
			IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathDiff';
			PRINT @cmd;
		END;  
        INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd; 
    END;
    
    IF @Debug = 1
    BEGIN
	    SELECT BackupFile FROM @FileList;
    END;
    IF @SimpleFolderEnumeration = 0
    BEGIN
        /*Full Sanity check folders*/
        IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The system cannot find the path specified.'
		    ) = 1
	    BEGIN
		    RAISERROR('(DIFF) Bad value for path %s', 16, 1, @BackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'Access is denied.'
		    ) = 1
	    BEGIN
		    RAISERROR('(DIFF) Access is denied to %s', 16, 1, @BackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The user name or password is incorrect.'
		    ) = 1
		    BEGIN
			    RAISERROR('(DIFF) Incorrect user name or password for %s', 16, 1, @BackupPathDiff) WITH NOWAIT;
                RETURN;
		    END;
    END;
    /*End folder sanity check*/
    -- Find latest diff backup 
    SELECT @LastDiffBackup = MAX(BackupFile)
    FROM @FileList
    WHERE BackupFile LIKE N'%.bak'
        AND
        BackupFile LIKE N'%' + @Database + '%'
	    AND
	    (@StopAt IS NULL OR REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt);
    --No file = no backup to restore
	SET @LastDiffBackupDateTime = REPLACE(LEFT(RIGHT(@LastDiffBackup, 19),15), '_', '');
    IF @RestoreDiff = 1 AND @BackupDateTime < @LastDiffBackupDateTime
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathDiff + @LastDiffBackup + N''' WITH NORECOVERY' + @BackupParameters + NCHAR(13);
	    IF (@StandbyMode = 1)
		BEGIN
		    IF (@StandbyUndoPath IS NULL)
			    BEGIN
				    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			    END
		    ELSE
			    SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathDiff + @LastDiffBackup + N''' WITH STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''' + @BackupParameters + NCHAR(13);
	    END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;  
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;
		
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
			SELECT '#Headers' AS table_name, @LastDiffBackup AS DiffbackupFile, * FROM #Headers AS h
		END
		
		--set the @BackupDateTime to the date time on the most recent differential	
		SET @BackupDateTime = @LastDiffBackupDateTime;
		
		SELECT @DiffLastLSN = CAST(LastLSN AS NUMERIC(25, 0))
		FROM #Headers 
		WHERE BackupType = 5;                                                  
	END;

	IF @DiffLastLSN IS NULL
	BEGIN
		SET @DiffLastLSN=@FullLastLSN
	END
END      


IF @BackupPathLog IS NOT NULL
BEGIN
    DELETE FROM @FileList;
    DELETE FROM @FileListSimple;

    IF @SimpleFolderEnumeration = 1
    BEGIN    -- Get list of files 
        INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @BackupPathLog, 1, 1;
        INSERT @FileList (BackupFile) SELECT BackupFile FROM @FileListSimple;
    END
    ELSE
    BEGIN
        SET @cmd = N'DIR /b "' + @BackupPathLog + N'"';
		IF @Debug = 1
		BEGIN
			IF @cmd IS NULL PRINT '@cmd is NULL for @BackupPathLog';
			PRINT @cmd;
		END;  
        INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd; 
    END;
    
    IF @SimpleFolderEnumeration = 1
    BEGIN
        /*Check what we can*/
        IF NOT EXISTS (SELECT * FROM @FileList)
	    BEGIN
		    RAISERROR('(LOG) No rows were returned for that database %s in path %s', 16, 1, @Database, @BackupPathLog) WITH NOWAIT;
            RETURN;
	    END;
    END
    ELSE
    BEGIN
        /*Full Sanity check folders*/
        IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The system cannot find the path specified.'
		    OR fl.BackupFile = 'File Not Found'
		    ) = 1
	    BEGIN
		    RAISERROR('(LOG) No rows or bad value for path %s', 16, 1, @BackupPathLog) WITH NOWAIT;
            RETURN;
	    END;

	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'Access is denied.'
		    ) = 1
	    BEGIN
		    RAISERROR('(LOG) Access is denied to %s', 16, 1, @BackupPathLog) WITH NOWAIT;
            RETURN;
	    END;

	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    ) = 1
	        AND 
            (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 							
		    WHERE fl.BackupFile IS NULL
		    ) = 1
		    BEGIN
			    RAISERROR('(LOG) Empty directory %s', 16, 1, @BackupPathLog) WITH NOWAIT;
			    RETURN;
		    END

	    IF (
		    SELECT COUNT(*) 
		    FROM @FileList AS fl 
		    WHERE fl.BackupFile = 'The user name or password is incorrect.'
		    ) = 1
		    BEGIN
			    RAISERROR('(LOG) Incorrect user name or password for %s', 16, 1, @BackupPathLog) WITH NOWAIT;
                RETURN;
		    END;
    END;
    /*End folder sanity check*/


IF (@OnlyLogsAfter IS NOT NULL)
BEGIN
	
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@OnlyLogsAfter is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;
	
    DELETE fl
	FROM @FileList AS fl
	WHERE BackupFile LIKE N'%.trn'
	AND BackupFile LIKE N'%' + @Database + N'%' 
	AND REPLACE(LEFT(RIGHT(fl.BackupFile, 19), 15),'_','') < @OnlyLogsAfter
	
END



-- Check for log backups
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
			  AND ((@ContinueLogs = 1 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt) OR (@ContinueLogs = 0 AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') >= @BackupDateTime) AND REPLACE(LEFT(RIGHT(BackupFile, 19), 15),'_','') <= @StopAt)
			  ORDER BY BackupFile;
		
		OPEN BackupFiles;
	END;

IF (@StandbyMode = 1)
	BEGIN
		IF (@StandbyUndoPath IS NULL)
			BEGIN
				IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. Logs will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END;
		ELSE
			SET @LogRecoveryOption = N'STANDBY = ''' + @StandbyUndoPath + @Database + 'Undo.ldf''';
	END;

IF (@LogRecoveryOption = N'')
	BEGIN
		SET @LogRecoveryOption = N'NORECOVERY';
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

				IF @Execute = 'Y' RAISERROR('@i set to 2, restoring logs', 0, 1) WITH NOWAIT;
				
				SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM DISK = ''' + @BackupPathLog + @BackupFile + N''' WITH ' + @LogRecoveryOption + NCHAR(13);
				
					IF @Debug = 1 OR @Execute = 'N'
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for RESTORE LOG: @RestoreDatabaseName, @BackupPathLog, @BackupFile';
						PRINT @sql;
					END; 
				
					IF @Debug IN (0, 1) AND @Execute = 'Y'
						EXECUTE master.sys.sp_executesql @stmt = @sql;
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

-- Put database in a useable state 
IF @RunRecovery = 1
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' WITH RECOVERY' + NCHAR(13);

			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @RestoreDatabaseName';
				PRINT @sql;
			END; 

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;
	END;

-- Ensure simple recovery model
IF @ForceSimpleRecovery = 1
	BEGIN
		SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + N' SET RECOVERY SIMPLE' + NCHAR(13);

			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for SET RECOVERY SIMPLE: @RestoreDatabaseName';
				PRINT @sql;
			END; 

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;
	END;	    

 -- Run checkdb against this database
IF @RunCheckDB = 1
	BEGIN
		SET @sql = N'DBCC CHECKDB (' + @RestoreDatabaseName + N') WITH NO_INFOMSGS, ALL_ERRORMSGS, DATA_PURITY;';
			
			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for Run Integrity Check: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;
	END;


IF @DatabaseOwner IS NOT NULL
	BEGIN
		IF EXISTS (SELECT * FROM master.dbo.syslogins WHERE syslogins.loginname = @DatabaseOwner)
		BEGIN
			SET @sql = N'ALTER AUTHORIZATION ON DATABASE::' + @RestoreDatabaseName + ' TO [' + @DatabaseOwner + ']';

				IF @Debug = 1 OR @Execute = 'N'
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for Set Database Owner';
					PRINT @sql;
				END;

			IF @Debug IN (0, 1) AND @Execute = 'Y'
				EXECUTE (@sql);
		END
		ELSE
		BEGIN
			PRINT @DatabaseOwner + ' is not a valid Login. Database Owner not set.'
		END
	END;

 -- If test restore then blow the database away (be careful)
IF @TestRestore = 1
	BEGIN
		SET @sql = N'DROP DATABASE ' + @RestoreDatabaseName + NCHAR(13);
			
			IF @Debug = 1 OR @Execute = 'N'
			BEGIN
				IF @sql IS NULL PRINT '@sql is NULL for DROP DATABASE: @RestoreDatabaseName';
				PRINT @sql;
			END; 
		
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE master.sys.sp_executesql @stmt = @sql;

	END;
GO
