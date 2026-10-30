IF OBJECT_ID('dbo.CommandExecute') IS NULL
BEGIN
	PRINT 'sp_DatabaseRestore is about to install, but you have not yet installed the Ola Hallengren maintenance scripts.'
	PRINT 'sp_DatabaseRestore will still install, but to use that stored proc, you will need to install this:'
	PRINT 'https://ola.hallengren.com'
	PRINT 'You will see a bunch of warnings below because the Ola scripts are not installed yet, and that is okay:'
END
GO


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
    @MoveFullTextCatalogDrive NVARCHAR(260) = NULL,
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
    @ExistingDBAction TINYINT = 0,
    @StopAt NVARCHAR(14) = NULL,
    @OnlyLogsAfter NVARCHAR(14) = NULL,
    @SimpleFolderEnumeration BIT = 0,
    @SkipBackupsAlreadyInMsdb BIT = 0,
    @DatabaseOwner sysname = NULL,
    @SetTrustworthyON BIT = 0,
    @FixOrphanUsers BIT = 0,
    @KeepCdc BIT = 0,
    @Execute CHAR(1) = 'Y',
    @FileExtensionBak NVARCHAR(128) = NULL,
    @FileExtensionDiff NVARCHAR(128) = NULL,
    @Debug INT = 0,
    @Help BIT = 0,
    @Version     VARCHAR(30) = NULL OUTPUT,
    @VersionDate DATETIME = NULL OUTPUT,
    @VersionCheckMode BIT = 0,
    @FileNamePrefix NVARCHAR(260) = NULL,
    @RunStoredProcAfterRestore NVARCHAR(260) = NULL,
    @EnableBroker BIT = 0,
    @RestoreFromS3 BIT = NULL -- S3: NULL = S3 when a path starts with s3://, 1 = S3 (paths derived from msdb when none are given), 0 = never use S3
AS
BEGIN
SET NOCOUNT ON;
SET STATISTICS XML OFF;

/*Versioning details*/

SELECT @Version = '8.34', @VersionDate = '20260702';

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
			- S3 restores need SQL Server 2022 or newer, an S3 credential, and the backups in the local msdb backup history.
			- Tastes awful with marmite.

		Unknown limitations of this version:
			- None.  (If we knew them, they would be known. Duh.)

		    Changes - for the full list of improvements and fixes in this version, see:
		    https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/

		MIT License

		Copyright (c) Brent Ozar Unlimited

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

	--Restore just through the latest DIFF, ignoring logs, and using a custom ".dif" file extension
	EXEC dbo.sp_DatabaseRestore
		@Database = ''LogShipMe'',
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'',
		@BackupPathDiff = ''D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\'',
		@RestoreDiff = 1,
		@FileExtensionDiff = ''dif'',
		@ContinueLogs = 0,
		@RunRecovery = 1;

	-- Restore from stripped backup set when multiple paths are used. This example will restore stripped full backup set along with stripped transactional logs set from multiple backup paths
	EXEC dbo.sp_DatabaseRestore
		@Database = ''DBA'',
		@BackupPathFull = ''D:\Backup1\DBA\FULL,D:\Backup2\DBA\FULL'',
		@BackupPathLog = ''D:\Backup1\DBA\LOG,D:\Backup2\DBA\LOG'',
		@StandbyMode = 0,
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

	--This example will NOT execute the restore.  Commands will be printed in a copy/paste ready format only
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

	--Parameter @FileExtensionBak indicates the file extension, in case backups have a different extension than default (.bak)
	EXEC dbo.sp_DatabaseRestore
		@Database = ''DBA'', 
		@BackupPathFull = ''D:\Backup1\DBA\FULL\'',
		@FileExtensionBak = ''DUMP'',
		@Debug = 1,
		@Execute = ''N'';

	*/
	';
	PRINT '
	/*
	-- S3 (SQL Server 2022+): with @RestoreFromS3 = 1 no paths are needed. The procedure looks up the S3 backups of @Database
	-- in the backup history in msdb, derives the FULL/DIFF/LOG locations from it, and restores them with RESTORE ... FROM URL.
	-- Prerequisite: a credential named after the bucket URL, e.g.
	--   CREATE CREDENTIAL [s3://s3.example.com:443/my-bucket] WITH IDENTITY = ''S3 Access Key'', SECRET = ''<AccessKeyID>:<SecretKeyID>'';
	EXEC dbo.sp_DatabaseRestore
		@Database = ''LogShipMe'',
		@RestoreDatabaseName = ''LogShipMe_Restored'',
		@RestoreFromS3 = 1,
		@RestoreDiff = 1,
		@ContinueLogs = 0,
		@RunRecovery = 1;

	-- S3, point in time, print the commands only
	EXEC dbo.sp_DatabaseRestore
		@Database = ''LogShipMe'',
		@RestoreFromS3 = 1,
		@RestoreDiff = 1,
		@StopAt = ''20170508201501'',
		@RunRecovery = 1,
		@Execute = ''N'';

	-- S3, limited to specific locations (acts as a filter on the backup history in msdb). s3:// paths switch to S3 on their own.
	EXEC dbo.sp_DatabaseRestore
		@Database = ''LogShipMe'',
		@BackupPathFull = ''s3://s3.example.com/my-bucket/LogShipMe/FULL/'',
		@BackupPathLog = ''s3://s3.example.com/my-bucket/LogShipMe/LOG/'',
		@RunRecovery = 1;
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

BEGIN TRY
DECLARE @CurrentDatabaseContext AS VARCHAR(128) = (SELECT DB_NAME());
DECLARE @CommandExecuteCheck VARCHAR(400);

SET @CommandExecuteCheck = 'IF NOT EXISTS (SELECT name FROM ' +@CurrentDatabaseContext+'.sys.objects WHERE type = ''P'' AND name = ''CommandExecute'')
BEGIN
	RAISERROR (''DatabaseRestore requires the CommandExecute stored procedure from the OLA Hallengren Maintenance solution, are you using the correct database?'', 15, 1);
	RETURN;
END;'
EXEC (@CommandExecuteCheck)
END TRY
BEGIN CATCH
THROW;
END CATCH

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
		@LogRestoreRanking INT = 1, --Holds Log iteration # when multiple paths & backup files are being stripped
		@LogFirstLSN NUMERIC(25, 0), --Holds first LSN in log backup headers
		@LogLastLSN NUMERIC(25, 0), --Holds last LSN in log backup headers
		@LogLastNameInMsdbAS NVARCHAR(MAX) = N'', -- Holds last TRN file name already restored
		@FileListParamSQL NVARCHAR(4000) = N'', --Holds INSERT list for #FileListParameters
		@BackupParameters NVARCHAR(500) = N'', --Used to save BlockSize, MaxTransferSize and BufferCount
        @RestoreDatabaseID SMALLINT, --Holds DB_ID of @RestoreDatabaseName
		@UnquotedRestoreDatabaseName NVARCHAR(128);   --Holds the unquoted @RestoreDatabaseName

DECLARE @FileListSimple TABLE (
    BackupFile NVARCHAR(255) NOT NULL,
    depth INT NOT NULL,
    [file] INT NOT NULL
);

DECLARE @FileList TABLE (
	BackupPath NVARCHAR(255) NULL,
    BackupFile NVARCHAR(255) NULL
);

DECLARE @PathItem TABLE (
	PathItem NVARCHAR(512)
);

-- S3: variables for S3 (RESTORE ... FROM URL) support
DECLARE @IsS3 BIT = 0, --1 when the backups live in S3 and RESTORE ... FROM URL has to be used
		@DeviceType NVARCHAR(10) = N'DISK', --DISK for file/UNC backups, URL for S3 backups
		@S3AllFolders BIT = 0, --1 when the paths were derived from msdb: then every folder in the history is a candidate, not just the newest one
		@S3SkipCopyOnlyFull BIT = 0; --1 when a differential will be restored: a copy-only full can not be its base

-- S3: S3 has no directory listing we can call from T-SQL, so the msdb backup history is our "directory listing"
DECLARE @S3Backups TABLE (
	BackupType CHAR(1) NOT NULL, --D = full, I = diff, L = log
	FinishDate DATETIME NULL,
	IsCopyOnly BIT NOT NULL,
	LastLSN NUMERIC(25, 0) NULL, --last_lsn of the backup, used to skip logs that are older than the restored database
	BackupSetUuid UNIQUEIDENTIFIER NULL, --identifies a full backup
	DiffBaseGuid UNIQUEIDENTIFIER NULL, --for a differential: the BackupSetUuid of the full it is based on
	FolderUrl NVARCHAR(260) NOT NULL, --s3://bucket/.../FULL/
	BackupFile NVARCHAR(260) NOT NULL --SERVER_DB_FULL_yyyymmdd_hhmmss.bak
);


IF OBJECT_ID(N'tempdb..#FileListParameters') IS NOT NULL DROP TABLE #FileListParameters;
CREATE TABLE #FileListParameters
(
    LogicalName NVARCHAR(128) NOT NULL,
    PhysicalName NVARCHAR(260) NOT NULL,
    [Type] CHAR(1) NOT NULL,
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
    [CodePage] NVARCHAR(256),
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
	LastValidRestoreTime DATETIME,
	TimeZone NVARCHAR(32),
	CompressionAlgorithm NVARCHAR(32),
    --
    -- Seq added to retain order by
    --
    Seq INT NOT NULL IDENTITY(1, 1)
);

/*
S3 support: decide whether the backups live in S3.
  @RestoreFromS3 = 1    -> S3. With no paths given, the FULL/DIFF/LOG locations are derived from the backup history in msdb.
  @RestoreFromS3 = 0    -> never S3 (original behaviour)
  @RestoreFromS3 = NULL -> S3 only when one of the paths starts with s3://. The msdb history is never consulted on its own,
                           so a call without paths behaves exactly as before unless @RestoreFromS3 = 1 is passed explicitly.
*/
IF @RestoreFromS3 IS NULL
	SET @RestoreFromS3 = CASE WHEN @BackupPathFull LIKE N's3://%' OR @BackupPathDiff LIKE N's3://%' OR @BackupPathLog LIKE N's3://%' THEN 1 ELSE 0 END;

IF @RestoreFromS3 = 1
BEGIN
	-- S3 can't be listed from T-SQL, so the backup history in msdb is our "directory listing"
	INSERT INTO @S3Backups (BackupType, FinishDate, IsCopyOnly, LastLSN, BackupSetUuid, DiffBaseGuid, FolderUrl, BackupFile)
	SELECT bs.type, bs.backup_finish_date, bs.is_copy_only, bs.last_lsn, bs.backup_set_uuid, bs.differential_base_guid,
		LEFT(bmf.physical_device_name, LEN(bmf.physical_device_name) - CHARINDEX('/', REVERSE(bmf.physical_device_name)) + 1),
		RIGHT(bmf.physical_device_name, CHARINDEX('/', REVERSE(bmf.physical_device_name)) - 1)
	FROM msdb.dbo.backupset AS bs
	INNER JOIN msdb.dbo.backupmediafamily AS bmf ON bmf.media_set_id = bs.media_set_id
	WHERE bs.database_name = @Database
	AND bmf.physical_device_name LIKE N's3://%'
	AND bs.type IN ('D', 'I', 'L');

	-- A differential can only be applied on top of its own base full. If that full was written somewhere else (for example a regular
	-- full to another device), the differential can't be used with the S3 fulls, so leave it out.
	IF @RestoreDiff = 1
		DELETE d
		FROM @S3Backups AS d
		WHERE d.BackupType = 'I'
		AND NOT EXISTS (SELECT 1 FROM @S3Backups AS f WHERE f.BackupType = 'D' AND f.BackupSetUuid = d.DiffBaseGuid);
END;

IF @RestoreFromS3 = 1
BEGIN
	IF @MajorVersion < 16
	BEGIN
		RAISERROR('Restoring from S3 requires SQL Server 2022 or newer.', 16, 1) WITH NOWAIT;
		RETURN;
	END;

	IF @MaxTransferSize > 20971520 -- checked here, before anything is changed on the target
	BEGIN
		RAISERROR('@MaxTransferSize can not be greater than 20971520 for S3', 16, 1) WITH NOWAIT;
		RETURN;
	END;

	SET @IsS3 = 1;
	SET @DeviceType = N'URL';
	SET @SimpleFolderEnumeration = 1; -- no xp_cmdshell/DIR for S3, the file list comes from @S3Backups
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Backups are in S3, restoring with RESTORE ... FROM URL', 0, 1) WITH NOWAIT;

	-- No paths given: derive them from the backup history in msdb
	IF @BackupPathFull IS NULL AND @BackupPathDiff IS NULL AND @BackupPathLog IS NULL
	BEGIN
		-- Backups can live in several folders (for example date-partitioned ones), so every folder in the history is a candidate.
		-- The paths below only mark which backup types exist, the folders themselves are enumerated from msdb later on.
		SET @S3AllFolders = 1;

		IF @ContinueLogs = 0 -- continuing logs on a RESTORING database needs the log backups only, the full may be gone from the history
		BEGIN
			IF @RestoreDiff = 1 AND NOT EXISTS (SELECT 1 FROM @S3Backups WHERE BackupType = 'I')
			BEGIN
				IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@RestoreDiff = 1, but no S3 DIFF backup found in msdb. Setting @RestoreDiff to 0', 0, 1) WITH NOWAIT;
				SET @RestoreDiff = 0;
			END;

			-- A copy-only full can not be the base of a differential, but it can be restored on its own (followed by logs)
			SELECT TOP 1 @BackupPathFull = FolderUrl FROM @S3Backups
			WHERE BackupType = 'D' AND (@RestoreDiff = 0 OR IsCopyOnly = 0)
			ORDER BY FinishDate DESC;

			IF @BackupPathFull IS NULL
			BEGIN
				RAISERROR('No usable S3 FULL backup for "%s" found in the backup history in msdb (a copy-only full can not be combined with @RestoreDiff = 1)', 16, 1, @Database) WITH NOWAIT;
				RETURN;
			END;

			IF @RestoreDiff = 1
				SELECT TOP 1 @BackupPathDiff = FolderUrl FROM @S3Backups WHERE BackupType = 'I' ORDER BY FinishDate DESC;
		END;

		SELECT TOP 1 @BackupPathLog = FolderUrl FROM @S3Backups WHERE BackupType = 'L' ORDER BY FinishDate DESC;

		IF @ContinueLogs = 1 AND @BackupPathLog IS NULL
		BEGIN
			RAISERROR('@ContinueLogs = 1, but no S3 LOG backup for "%s" found in the backup history in msdb', 16, 1, @Database) WITH NOWAIT;
			RETURN;
		END;

		IF @Execute = 'Y' OR @Debug = 1
		BEGIN
			IF @BackupPathFull IS NOT NULL RAISERROR('S3 FULL path derived from msdb: %s', 0, 1, @BackupPathFull) WITH NOWAIT;
			IF @BackupPathDiff IS NOT NULL RAISERROR('S3 DIFF path derived from msdb: %s', 0, 1, @BackupPathDiff) WITH NOWAIT;
			IF @BackupPathLog IS NOT NULL RAISERROR('S3 LOG path derived from msdb: %s', 0, 1, @BackupPathLog) WITH NOWAIT;
		END;
	END;

	IF @RestoreDiff = 1 AND @BackupPathDiff IS NOT NULL
		SET @S3SkipCopyOnlyFull = 1;
END;

/*
Correct paths in case people forget a final "\" or "/"
*/
/*Full*/
IF (SELECT RIGHT(@BackupPathFull, 1)) <> '/' AND CHARINDEX('/', @BackupPathFull) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'/';
END;
ELSE IF (SELECT RIGHT(@BackupPathFull, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathFull to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathFull += N'\';
END;
/*Diff*/
IF (SELECT RIGHT(@BackupPathDiff, 1)) <> '/' AND CHARINDEX('/', @BackupPathDiff) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'/';
END;
ELSE IF (SELECT RIGHT(@BackupPathDiff, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathDiff to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathDiff += N'\';
END;
/*Log*/
IF (SELECT RIGHT(@BackupPathLog, 1)) <> '/' AND CHARINDEX('/', @BackupPathLog) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "/"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'/';
END;
ELSE IF (SELECT RIGHT(@BackupPathLog, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @BackupPathLog to add a "\"', 0, 1) WITH NOWAIT;
	SET @BackupPathLog += N'\';
END;
/*Move Data File*/
IF NULLIF(@MoveDataDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default data drive for @MoveDataDrive', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveDataDrive, 1)) <> '/' AND CHARINDEX('/', @MoveDataDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveDataDrive, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveDataDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveDataDrive += N'\';
END;
/*Move Log File*/
IF NULLIF(@MoveLogDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Getting default log drive for @MoveLogDrive', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive  = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveLogDrive, 1)) <> '/' AND CHARINDEX('/', @MoveLogDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveLogDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveLogDrive, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveLogDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveLogDrive += N'\';
END;
/*Move Filestream File*/
IF NULLIF(@MoveFilestreamDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Setting default data drive for @MoveFilestreamDrive', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive  = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) <> '/' AND CHARINDEX('/', @MoveFilestreamDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveFilestreamDrive, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFilestreamDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveFilestreamDrive += N'\';
END;
/*Move FullText Catalog File*/
IF NULLIF(@MoveFullTextCatalogDrive, '') IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Setting default data drive for @MoveFullTextCatalogDrive', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive  = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(260));
END;
IF (SELECT RIGHT(@MoveFullTextCatalogDrive, 1)) <> '/' AND CHARINDEX('/', @MoveFullTextCatalogDrive) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFullTextCatalogDrive to add a "/"', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive += N'/';
END;
ELSE IF (SELECT RIGHT(@MoveFullTextCatalogDrive, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @MoveFullTextCatalogDrive to add a "\"', 0, 1) WITH NOWAIT;
	SET @MoveFullTextCatalogDrive += N'\';
END;
/*Standby Undo File*/
IF (SELECT RIGHT(@StandbyUndoPath, 1)) <> '/' AND CHARINDEX('/', @StandbyUndoPath) > 0 --Has to end in a '/'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "/"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'/';
END;
ELSE IF (SELECT RIGHT(@StandbyUndoPath, 1)) NOT IN ('\', '/') --Has to end in a path separator
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Fixing @StandbyUndoPath to add a "\"', 0, 1) WITH NOWAIT;
	SET @StandbyUndoPath += N'\';
END;

IF @RestoreDatabaseName IS NULL OR @RestoreDatabaseName LIKE N'' /*use LIKE instead of =, otherwise N'' = N' '. See: https://www.brentozar.com/archive/2017/04/surprising-behavior-trailing-spaces/ */
BEGIN
	SET @RestoreDatabaseName = @Database;
END;

/*check input parameters*/
IF NOT @MaxTransferSize IS NULL
BEGIN
	IF @MaxTransferSize > 4194304 AND @IsS3 = 0 -- S3: S3 has its own ceiling, checked in the S3 block above
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

/* Reject path-shaped parameters that contain shell metacharacters or control chars.
   Single quotes are deliberately allowed (legal in Windows paths) and are escaped at concat sites instead.
   COLLATE Latin1_General_BIN2 is required so the LIKE '[...]' class catches NCHAR(0) — under default
   collations the NUL byte is silently dropped from the pattern and compared values.
   @Database is intentionally NOT in this list: it is primarily an identifier (database names can
   legally contain '&', ';', etc.) and downstream usage either parameter-binds it (LIKE filters
   against @FileList) or single-quote-escapes it before concatenating into a quoted SQL literal. */
DECLARE @ForbiddenPathPattern NVARCHAR(40) = N'%["&|;^<>' + NCHAR(0) + NCHAR(10) + NCHAR(13) + N']%';
DECLARE @InvalidPathParam sysname = NULL;
IF @BackupPathFull            LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@BackupPathFull';
IF @InvalidPathParam IS NULL AND @BackupPathDiff           LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@BackupPathDiff';
IF @InvalidPathParam IS NULL AND @BackupPathLog            LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@BackupPathLog';
IF @InvalidPathParam IS NULL AND @MoveDataDrive            LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@MoveDataDrive';
IF @InvalidPathParam IS NULL AND @MoveLogDrive             LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@MoveLogDrive';
IF @InvalidPathParam IS NULL AND @MoveFilestreamDrive      LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@MoveFilestreamDrive';
IF @InvalidPathParam IS NULL AND @MoveFullTextCatalogDrive LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@MoveFullTextCatalogDrive';
IF @InvalidPathParam IS NULL AND @StandbyUndoPath          LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@StandbyUndoPath';
IF @InvalidPathParam IS NULL AND @FileNamePrefix           LIKE @ForbiddenPathPattern COLLATE Latin1_General_BIN2 SET @InvalidPathParam = N'@FileNamePrefix';
IF @InvalidPathParam IS NOT NULL
BEGIN
	RAISERROR('Parameter %s contains a forbidden character. Not allowed: " & | ; ^ < > or control characters.', 16, 1, @InvalidPathParam) WITH NOWAIT;
	RETURN;
END;

/* Validate @RunStoredProcAfterRestore shape up-front so an invalid input fails fast instead
   of after a successful restore. The actual EXEC build happens later (after the restore);
   here we only check that the value is a 1- or 2-part name and stash the whitespace-normalized
   form for later reuse. See the EXEC build site for the QUOTENAME logic. */
DECLARE @RunStoredProcNormalized nvarchar(260) = NULL;
IF @RunStoredProcAfterRestore IS NOT NULL AND LEN(LTRIM(@RunStoredProcAfterRestore)) > 0
BEGIN
	SET @RunStoredProcNormalized = LTRIM(RTRIM(@RunStoredProcAfterRestore));
	WHILE CHARINDEX(N' .', @RunStoredProcNormalized) > 0 SET @RunStoredProcNormalized = REPLACE(@RunStoredProcNormalized, N' .', N'.');
	WHILE CHARINDEX(N'. ', @RunStoredProcNormalized) > 0 SET @RunStoredProcNormalized = REPLACE(@RunStoredProcNormalized, N'. ', N'.');
	IF PARSENAME(@RunStoredProcNormalized, 1) IS NULL
	   OR PARSENAME(@RunStoredProcNormalized, 3) IS NOT NULL
	   OR PARSENAME(@RunStoredProcNormalized, 4) IS NOT NULL
	BEGIN
		RAISERROR('@RunStoredProcAfterRestore must be a procedure name or schema.procedure (1- or 2-part name).', 16, 1) WITH NOWAIT;
		RETURN;
	END;
END;

--File Extension cleanup
IF @FileExtensionDiff LIKE '%.%'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Removing "." from @FileExtensionDiff', 0, 1) WITH NOWAIT;
	SET @FileExtensionDiff = REPLACE(@FileExtensionDiff,'.','');
END

SET @FileExtensionBak = NULLIF(LTRIM(RTRIM(@FileExtensionBak)), '');
IF @FileExtensionBak IS NULL
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('No @FileExtensionBak given, assuming "bak".', 0, 1) WITH NOWAIT;
	SET @FileExtensionBak = 'bak';
END
IF @FileExtensionBak LIKE '%.%'
BEGIN
	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('Removing "." from @FileExtensionBak', 0, 1) WITH NOWAIT;
	SET @FileExtensionBak = REPLACE(@FileExtensionBak,'.','');
END

SET @RestoreDatabaseID = DB_ID(@RestoreDatabaseName);
SET @RestoreDatabaseName = QUOTENAME(@RestoreDatabaseName);
SET @UnquotedRestoreDatabaseName = PARSENAME(@RestoreDatabaseName,1);

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

IF @MajorVersion >= 16
  SET @HeadersSQL += N', LastValidRestoreTime, TimeZone, CompressionAlgorithm';

SET @HeadersSQL += N')' + NCHAR(13) + NCHAR(10);
SET @HeadersSQL += N'EXEC (''RESTORE HEADERONLY FROM ' + @DeviceType + N'=''''{Path}'''''')'; -- S3: DISK or URL

IF @BackupPathFull IS NOT NULL
BEGIN
	DECLARE @CurrentBackupPathFull NVARCHAR(255);

	 -- Split CSV string logic has taken from Ola Hallengren's :)
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															   AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, 1 ), 0 ), LEN( @BackupPathFull ) + 1 )									   AS EndPosition,
				  SUBSTRING( @BackupPathFull, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, 1 ), 0 ), LEN( @BackupPathFull ) + 1 ) - 1 ) AS PathItem
		   WHERE @BackupPathFull IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, EndPosition + 1 ), 0 ), LEN( @BackupPathFull ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathFull, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathFull, EndPosition + 1 ), 0 ), LEN( @BackupPathFull ) + 1 ) - EndPosition - 1 ) AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathFull ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE WHEN RIGHT(PathItem, 1) IN ('\', '/') THEN PathItem
             WHEN CHARINDEX('/', PathItem) > 0 THEN PathItem + '/'
             ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN

		SELECT TOP 1 @CurrentBackupPathFull = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathFull, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @IsS3 = 1
		BEGIN    -- S3: "list" the folder(s) via the backup history (type D = full)
			INSERT @FileList (BackupPath, BackupFile)
			SELECT DISTINCT FolderUrl, BackupFile FROM @S3Backups
			WHERE BackupType = 'D'
			AND (@S3SkipCopyOnlyFull = 0 OR IsCopyOnly = 0) -- a copy-only full can not be the base of a differential
			AND (@S3AllFolders = 1 OR LEFT( FolderUrl, LEN( @CurrentBackupPathFull ) ) = @CurrentBackupPathFull COLLATE Latin1_General_BIN2); -- S3 object keys are case-sensitive
			/* Validate every path on its own: rows from an earlier path must not hide a path without history (typo, missing history) */
			IF @@ROWCOUNT = 0
			BEGIN
				RAISERROR('(FULL) No S3 backups for database %s found in the msdb backup history for path %s', 16, 1, @Database, @CurrentBackupPathFull) WITH NOWAIT;
				RETURN;
			END;
		END
		ELSE IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @CurrentBackupPathFull, 1, 1;
			INSERT @FileList (BackupPath,BackupFile) SELECT @CurrentBackupPathFull, BackupFile FROM @FileListSimple;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathFull + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathFull';
				PRINT @cmd;
			END;
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathFull
			WHERE BackupPath IS NULL;
		END;

		IF @Debug = 1
		BEGIN
			SELECT BackupPath, BackupFile FROM @FileList;
		END;
		IF @SimpleFolderEnumeration = 1
		BEGIN
			/*Check what we can*/
			IF NOT EXISTS (SELECT * FROM @FileList)
			BEGIN
				RAISERROR('(FULL) No rows were returned for that database in path %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
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
				RAISERROR('(FULL) No rows or bad value for path %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
				RETURN;
			END;
			IF (
				SELECT COUNT(*)
				FROM @FileList AS fl
				WHERE fl.BackupFile = 'Access is denied.'
				) = 1
			BEGIN
				RAISERROR('(FULL) Access is denied to %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
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
					RAISERROR('(FULL) Empty directory %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
					RETURN;
				END
			IF (
				SELECT COUNT(*)
				FROM @FileList AS fl
				WHERE fl.BackupFile = 'The user name or password is incorrect.'
				) = 1
				BEGIN
					RAISERROR('(FULL) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathFull) WITH NOWAIT;
					RETURN;
				END;
		END;
	END
	/*End folder sanity check*/

	IF @StopAt IS NOT NULL
		BEGIN
			DELETE
			FROM @FileList
			WHERE BackupFile LIKE N'%[_][0-9].' + @FileExtensionBak
			AND	BackupFile LIKE N'%' + @Database + N'%'
			AND	(REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) > @StopAt);

			DELETE
			FROM @FileList
			WHERE BackupFile LIKE N'%[_][0-9][0-9].' + @FileExtensionBak
			AND	BackupFile LIKE N'%' + @Database + N'%'
			AND	(REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 18 ), '_', '' ) > @StopAt);

			DELETE
			FROM @FileList
			WHERE BackupFile LIKE N'%.bak' /*delete backups that aren't striped and new "enough"*/
			AND BackupFile NOT LIKE N'%[_][0-9].bak' /*skip remaining striped backups since those were deleted above*/
			AND BackupFile NOT LIKE N'%[_][0-9][0-9].bak'
			AND	BackupFile LIKE N'%' + @Database + N'%'
			AND (REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, PATINDEX( '%_[0-9][0-9]%', REVERSE( BackupFile ) ) ), '' ), 16 ), '_', '' ) > @StopAt);
		END;

    -- Find latest full backup
    -- Get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement as well as Non-Split Backups Restore Command
    SELECT TOP 1 @LastFullBackup = BackupFile, @CurrentBackupPathFull = BackupPath
    FROM @FileList
    WHERE BackupFile LIKE N'%.' + @FileExtensionBak
        AND
        BackupFile LIKE N'%' + @Database + N'%'
	    AND
	    (@StopAt IS NULL OR REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) <= @StopAt)
    ORDER BY BackupFile DESC;

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

	SELECT BackupPath, BackupFile INTO #SplitFullBackups
	FROM @FileList
	WHERE LEFT( BackupFile, LEN( BackupFile ) - PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ) = LEFT( @LastFullBackup, LEN( @LastFullBackup ) - PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) )
	AND PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Ola only supports up to 64 file split.
	ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) DESC;

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
    SET @FileListParamSQL += N'EXEC (''RESTORE FILELISTONLY FROM ' + @DeviceType + N'=''''{Path}'''''')'; -- S3: DISK or URL

    SET @sql = REPLACE(@FileListParamSQL, N'{Path}', REPLACE(@CurrentBackupPathFull + @LastFullBackup, N'''', REPLICATE(N'''', 4)));

    IF @Debug = 1
    BEGIN
	    IF @sql IS NULL PRINT '@sql is NULL for INSERT to #FileListParameters: @BackupPathFull + @LastFullBackup';
	    PRINT @sql;
    END;

    EXEC (@sql);
    IF @Debug = 1
    BEGIN
	    SELECT '#FileListParameters' AS table_name, * FROM #FileListParameters;
	    SELECT '@FileList' AS table_name, BackupPath, BackupFile FROM @FileList WHERE BackupFile IS NOT NULL;
    END

    --get the backup completed data so we can apply tlogs from that point forwards
    SET @sql = REPLACE(@HeadersSQL, N'{Path}', REPLACE(@CurrentBackupPathFull + @LastFullBackup, N'''', REPLICATE(N'''', 4)));

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
		SET @BackupParameters += N', MaxTransferSize=' + cast(@MaxTransferSize as NVARCHAR(10))
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
					WHEN Type = 'F' THEN @MoveFullTextCatalogDrive
			    END + COALESCE(@FileNamePrefix, '') + CASE
                        WHEN @Database = @RestoreDatabaseName THEN REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX(CASE WHEN CHARINDEX('\', PhysicalName) > 0 THEN '\' ELSE '/' END, REVERSE(PhysicalName), 1) -1))
					    ELSE REPLACE(REVERSE(LEFT(REVERSE(PhysicalName), CHARINDEX(CASE WHEN CHARINDEX('\', PhysicalName) > 0 THEN '\' ELSE '/' END, REVERSE(PhysicalName), 1) -1)), @Database, SUBSTRING(@RestoreDatabaseName, 2, LEN(@RestoreDatabaseName) -2))
					    END AS TargetPhysicalName,
                    PhysicalName,
                    LogicalName
		    FROM #FileListParameters)
	    /* Both LogicalName and TargetPhysicalName get embedded inside SQL string literals
	       below. TargetPhysicalName is built from caller parameters (@MoveDataDrive,
	       @MoveLogDrive, @MoveFilestreamDrive, @MoveFullTextCatalogDrive, @FileNamePrefix)
	       which the path-validation gate allows to contain apostrophes — so an apostrophe
	       in any of those params would otherwise close the literal. Double the quotes
	       inline. (LogicalName is filesystem-sourced and out of scope per threat model;
	       escaping it too is defense-in-depth at zero cost.) */
	    SELECT @MoveOption = @MoveOption + N', MOVE ''' + REPLACE(Files.LogicalName, N'''', N'''''') + N''' TO ''' + REPLACE(Files.TargetPhysicalName, N'''', N'''''') + ''''
	    FROM Files
        WHERE Files.TargetPhysicalName <> Files.PhysicalName;

	    IF @Debug = 1 PRINT @MoveOption
    END;

    /*Process @ExistingDBAction flag */
    IF @ExistingDBAction BETWEEN 1 AND 4
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
				BEGIN
					IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') != 'RESTORING'
					BEGIN
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'ALTER DATABASE SINGLE_USER', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
					END
					ELSE IF @Debug = 1
					BEGIN
						IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') IS NULL PRINT 'Unable to retrieve STATUS from "' + @UnquotedRestoreDatabaseName + '" database. Skipping setting database to SINGLE_USER';
						ELSE IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') = 'RESTORING' PRINT @UnquotedRestoreDatabaseName + ' database STATUS is RESTORING. Skipping setting database to SINGLE_USER';
			        END
		        END
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
			       EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'KILL CONNECTIONS', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
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
			        EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'DROP DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
            END
			IF @ExistingDBAction = 4
			BEGIN
				RAISERROR ('Offlining database', 0, 1) WITH NOWAIT;

				SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + SPACE( 1 ) + 'SET OFFLINE WITH ROLLBACK IMMEDIATE';
				IF @Debug = 1 OR @Execute = 'N'
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for Offline database';
					PRINT @sql;
				END;
		        IF @Debug IN (0, 1) AND @Execute = 'Y'
				BEGIN
					IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') != 'RESTORING'
					BEGIN
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'OFFLINE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
					END
					ELSE IF @Debug = 1
					BEGIN
						IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') IS NULL PRINT 'Unable to retrieve STATUS from "' + @UnquotedRestoreDatabaseName + '" database. Skipping setting database OFFLINE';
						ELSE IF DATABASEPROPERTYEX(@UnquotedRestoreDatabaseName,'STATUS') = 'RESTORING' PRINT @UnquotedRestoreDatabaseName + ' database STATUS is RESTORING. Skipping setting database OFFLINE';
			        END
		        END
			END;
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
	    IF (SELECT COUNT(*) FROM #SplitFullBackups) > 0
        BEGIN
            IF @Debug = 1 RAISERROR('Split backups found', 0, 1) WITH NOWAIT;

			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM '
                       + STUFF(
                             (SELECT CHAR( 10 ) + ',' + @DeviceType + '=''' + REPLACE(BackupPath, '''', '''''') + REPLACE(BackupFile, '''', '''''') + ''''
							  FROM #SplitFullBackups
							  ORDER BY BackupFile
							  FOR XML PATH (''), TYPE).value('.', 'nvarchar(max)'),
                             1,
                             2,
                             '') + N' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption + NCHAR(13) + NCHAR(10);
        END;
	    ELSE
		BEGIN
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM ' + @DeviceType + N' = ''' + REPLACE(@CurrentBackupPathFull, N'''', N'''''') + REPLACE(@LastFullBackup, N'''', N'''''') + N''' WITH NORECOVERY, REPLACE' + @BackupParameters + @MoveOption + NCHAR(13) + NCHAR(10);
		END
	    IF (@StandbyMode = 1)
	    BEGIN
		    IF (@StandbyUndoPath IS NULL)
			BEGIN
			    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END
	        ELSE IF (SELECT COUNT(*) FROM #SplitFullBackups) > 0
			BEGIN
				SET @sql = @sql + ', STANDBY = ''' + REPLACE(@StandbyUndoPath, N'''', N'''''') + REPLACE(@Database, N'''', N'''''') + 'Undo.ldf''' + NCHAR(13) + NCHAR(10);
			END
			ELSE
	        BEGIN
		        SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM ' + @DeviceType + N' = ''' + REPLACE(@CurrentBackupPathFull, N'''', N'''''') + REPLACE(@LastFullBackup, N'''', N'''''') + N''' WITH  REPLACE' + @BackupParameters + @MoveOption + N' , STANDBY = ''' + REPLACE(@StandbyUndoPath, N'''', N'''''') + REPLACE(@Database, N'''', N'''''') + 'Undo.ldf''' + NCHAR(13) + NCHAR(10);
	        END
        END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathFull, @LastFullBackup, @MoveOption';
			PRINT @sql;
		END;

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';

		-- We already loaded #Headers above

	    --setting the @BackupDateTime to a numeric string so that it can be used in comparisons
		SET @BackupDateTime = REPLACE( RIGHT( REPLACE( @LastFullBackup, RIGHT( @LastFullBackup, CASE WHEN PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( @LastFullBackup ) ) ELSE CHARINDEX( '.', REVERSE( @LastFullBackup ) ) END ), '' ), 16 ), '_', '' );

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
	DELETE FROM @PathItem;

	DECLARE @CurrentBackupPathDiff NVARCHAR(512);
	-- Split CSV string logic has taken from Ola Hallengren's :)
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															   AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, 1 ), 0 ), LEN( @BackupPathDiff ) + 1 )									   AS EndPosition,
				  SUBSTRING( @BackupPathDiff, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, 1 ), 0 ), LEN( @BackupPathDiff ) + 1 ) - 1 ) AS PathItem
		   WHERE @BackupPathDiff IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, EndPosition + 1 ), 0 ), LEN( @BackupPathDiff ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathDiff, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathDiff, EndPosition + 1 ), 0 ), LEN( @BackupPathDiff ) + 1 ) - EndPosition - 1 ) AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathDiff ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE WHEN RIGHT(PathItem, 1) IN ('\', '/') THEN PathItem
             WHEN CHARINDEX('/', PathItem) > 0 THEN PathItem + '/'
             ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN

		SELECT TOP 1 @CurrentBackupPathDiff = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathDiff, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @IsS3 = 1
		BEGIN    -- S3: "list" the folder(s) via the backup history (type I = differential)
			INSERT @FileList (BackupPath, BackupFile)
			SELECT DISTINCT FolderUrl, BackupFile FROM @S3Backups
			WHERE BackupType = 'I'
			AND (@S3AllFolders = 1 OR LEFT( FolderUrl, LEN( @CurrentBackupPathDiff ) ) = @CurrentBackupPathDiff COLLATE Latin1_General_BIN2); -- S3 object keys are case-sensitive
			/* Validate every path on its own: rows from an earlier path must not hide a path without history (typo, missing history) */
			IF @@ROWCOUNT = 0 AND @RestoreDiff = 1
			BEGIN
				RAISERROR('(DIFF) No S3 backups for database %s found in the msdb backup history for path %s', 16, 1, @Database, @CurrentBackupPathDiff) WITH NOWAIT;
				RETURN;
			END;
		END
		ELSE IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @CurrentBackupPathDiff, 1, 1;
			INSERT @FileList (BackupPath,BackupFile) SELECT @CurrentBackupPathDiff, BackupFile FROM @FileListSimple;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathDiff + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathDiff';
				PRINT @cmd;
			END;
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathDiff WHERE BackupPath IS NULL;
		END;

		IF @Debug = 1
		BEGIN
			SELECT BackupPath,BackupFile FROM @FileList WHERE BackupFile IS NOT NULL;
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
		    RAISERROR('(DIFF) Bad value for path %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*)
		    FROM @FileList AS fl
		    WHERE fl.BackupFile = 'Access is denied.'
		    ) = 1
	    BEGIN
		    RAISERROR('(DIFF) Access is denied to %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
            RETURN;
	    END;
	    IF (
		    SELECT COUNT(*)
		    FROM @FileList AS fl
		    WHERE fl.BackupFile = 'The user name or password is incorrect.'
		    ) = 1
		    BEGIN
			    RAISERROR('(DIFF) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathDiff) WITH NOWAIT;
                RETURN;
		    END;
    END;
	END
    /*End folder sanity check*/
    -- Find latest diff backup
	IF @FileExtensionDiff IS NULL
	BEGIN
		IF @Execute = 'Y' OR @Debug = 1 RAISERROR('No @FileExtensionDiff given, assuming "bak".', 0, 1) WITH NOWAIT;
		SET @FileExtensionDiff = 'bak';
	END

	SELECT TOP 1 @LastDiffBackup = BackupFile, @CurrentBackupPathDiff = BackupPath
    FROM @FileList
    WHERE BackupFile LIKE N'%.' + @FileExtensionDiff
        AND
        BackupFile LIKE N'%' + @Database + '%'
	    AND
	    (@StopAt IS NULL OR REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) <= @StopAt)
	ORDER BY BackupFile DESC;

	 -- Load FileList data into Temp Table sorted by DateTime Stamp desc
	 SELECT BackupPath, BackupFile INTO #SplitDiffBackups
	 FROM @FileList
	 WHERE LEFT( BackupFile, LEN( BackupFile ) - PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ) = LEFT( @LastDiffBackup, LEN( @LastDiffBackup ) - PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) )
	 AND PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Olla only supports up to 64 file split.
	 ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) DESC;

    --No file = no backup to restore
	SET @LastDiffBackupDateTime = REPLACE( RIGHT( REPLACE( @LastDiffBackup, RIGHT( @LastDiffBackup, CASE WHEN PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( @LastDiffBackup ) ) ELSE CHARINDEX( '.', REVERSE( @LastDiffBackup ) ) END ), '' ), 16 ), '_', '' );

    IF @RestoreDiff = 1 AND @BackupDateTime < @LastDiffBackupDateTime
	BEGIN

	    IF (SELECT COUNT(*) FROM #SplitDiffBackups) > 0
		BEGIN
			IF @Debug = 1 RAISERROR ('Split backups found', 0, 1) WITH NOWAIT;
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM '
									   + STUFF(
											 (SELECT CHAR( 10 ) + ',' + @DeviceType + '=''' + REPLACE(BackupPath, '''', '''''') + REPLACE(BackupFile, '''', '''''') + ''''
											 FROM #SplitDiffBackups
											 ORDER BY BackupFile
											 FOR XML PATH (''), TYPE).value('.', 'nvarchar(max)'),
									   1,
									   2,
									   '' ) + N' WITH NORECOVERY, REPLACE' +  @BackupParameters + @MoveOption + NCHAR(13) + NCHAR(10);
		END;
		ELSE
			SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM ' + @DeviceType + N' = ''' + REPLACE(@CurrentBackupPathDiff, N'''', N'''''') + REPLACE(@LastDiffBackup, N'''', N'''''') + N''' WITH NORECOVERY' + @BackupParameters + @MoveOption + NCHAR(13) + NCHAR(10);

	    IF (@StandbyMode = 1)
		BEGIN
		    IF (@StandbyUndoPath IS NULL)
			    BEGIN
				    IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			    END
		    ELSE IF (SELECT COUNT(*) FROM #SplitDiffBackups) > 0
				SET @sql = @sql + ', STANDBY = ''' + REPLACE(@StandbyUndoPath, N'''', N'''''') + REPLACE(@Database, N'''', N'''''') + 'Undo.ldf''' + NCHAR(13) + NCHAR(10);
			ELSE
			    SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' FROM ' + @DeviceType + N' = ''' + REPLACE(@CurrentBackupPathDiff, N'''', N'''''') + REPLACE(@LastDiffBackup, N'''', N'''''') + N''' WITH STANDBY = ''' + REPLACE(@StandbyUndoPath, N'''', N'''''') + REPLACE(@Database, N'''', N'''''') + 'Undo.ldf''' + @BackupParameters + @MoveOption + NCHAR(13) + NCHAR(10);
	    END;
		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @BackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';

		--get the backup completed data so we can apply tlogs from that point forwards
		SET @sql = REPLACE(@HeadersSQL, N'{Path}', REPLACE(@CurrentBackupPathDiff + @LastDiffBackup, N'''', REPLICATE(N'''', 4)));

		IF @Debug = 1
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @CurrentBackupPathDiff, @LastDiffBackup';
			PRINT @sql;
		END;

		EXECUTE (@sql);
		IF @Debug = 1
		BEGIN
			SELECT '#Headers' AS table_name, @LastDiffBackup AS DiffbackupFile, * FROM #Headers AS h WHERE h.BackupType = 5;
		END

		--set the @BackupDateTime to the date time on the most recent differential
		SET @BackupDateTime = ISNULL( @LastDiffBackupDateTime, @BackupDateTime );
		IF @Debug = 1
		BEGIN
			IF @BackupDateTime IS NULL PRINT '@BackupDateTime is NULL for REPLACE: @LastDiffBackupDateTime';
			PRINT @BackupDateTime;
		END;
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
	DELETE FROM @PathItem;

	DECLARE @CurrentBackupPathLog NVARCHAR(512);
	-- Split CSV string logic has taken from Ola Hallengren's :)
	WITH BackupPaths (
	StartPosition, EndPosition, PathItem
	)
	AS (
		   SELECT 1																															 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, 1 ), 0 ), LEN( @BackupPathLog ) + 1 )									     AS EndPosition,
				  SUBSTRING( @BackupPathLog, 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, 1 ), 0 ), LEN( @BackupPathLog ) + 1 ) - 1 )  AS PathItem
		   WHERE @BackupPathLog IS NOT NULL
		   UNION ALL
		   SELECT CAST( EndPosition AS INT ) + 1																																		 AS StartPosition,
				  ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, EndPosition + 1 ), 0 ), LEN( @BackupPathLog ) + 1 )																	 AS EndPosition,
				  SUBSTRING( @BackupPathLog, EndPosition + 1, ISNULL( NULLIF( CHARINDEX( ',', @BackupPathLog, EndPosition + 1 ), 0 ), LEN( @BackupPathLog ) + 1 ) - EndPosition - 1 )    AS PathItem
		   FROM BackupPaths
		   WHERE EndPosition < LEN( @BackupPathLog ) + 1
	 )
	INSERT INTO @PathItem
	SELECT CASE WHEN RIGHT(PathItem, 1) IN ('\', '/') THEN PathItem
             WHEN CHARINDEX('/', PathItem) > 0 THEN PathItem + '/'
             ELSE PathItem + '\' END FROM BackupPaths;

	WHILE 1 = 1
	BEGIN
		SELECT TOP 1 @CurrentBackupPathLog = PathItem FROM @PathItem
		WHERE PathItem > COALESCE( @CurrentBackupPathLog, '' ) ORDER BY PathItem;
		IF @@rowcount = 0 BREAK;

		IF @IsS3 = 1
		BEGIN    -- S3: "list" the folder(s) via the backup history (type L = log)
			INSERT @FileList (BackupPath, BackupFile)
			SELECT DISTINCT FolderUrl, BackupFile FROM @S3Backups
			WHERE BackupType = 'L'
			AND (@S3AllFolders = 1 OR LEFT( FolderUrl, LEN( @CurrentBackupPathLog ) ) = @CurrentBackupPathLog COLLATE Latin1_General_BIN2); -- S3 object keys are case-sensitive
			/* Validate every path on its own: rows from an earlier path must not hide a path without history (typo, missing history) */
			IF @@ROWCOUNT = 0
			BEGIN
				RAISERROR('(LOG) No S3 backups for database %s found in the msdb backup history for path %s', 16, 1, @Database, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;
		END
		ELSE IF @SimpleFolderEnumeration = 1
		BEGIN    -- Get list of files
			INSERT INTO @FileListSimple (BackupFile, depth, [file]) EXEC master.sys.xp_dirtree @CurrentBackupPathLog, 1, 1;
			/* Validate this directory before combining it with earlier directories. */
			IF NOT EXISTS (SELECT 1 FROM @FileListSimple WHERE [file] = 1)
			BEGIN
				RAISERROR('(LOG) No files were returned for database %s in path %s', 16, 1, @Database, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;
			INSERT @FileList (BackupPath, BackupFile) SELECT @CurrentBackupPathLog, BackupFile FROM @FileListSimple WHERE [file] = 1;
			DELETE FROM @FileListSimple;
		END
		ELSE
		BEGIN
			SET @cmd = N'DIR /b "' + @CurrentBackupPathLog + N'"';
			IF @Debug = 1
			BEGIN
				IF @cmd IS NULL PRINT '@cmd is NULL for @CurrentBackupPathLog';
				PRINT @cmd;
			END;
			INSERT INTO @FileList (BackupFile) EXEC master.sys.xp_cmdshell @cmd;
			UPDATE @FileList SET BackupPath = @CurrentBackupPathLog
			WHERE BackupPath IS NULL;
		END;

		IF @SimpleFolderEnumeration = 1
		BEGIN
			/*Check what we can*/
			IF NOT EXISTS (SELECT * FROM @FileList)
			BEGIN
				RAISERROR('(LOG) No rows were returned for that database %s in path %s', 16, 1, @Database, @CurrentBackupPathLog) WITH NOWAIT;
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
				RAISERROR('(LOG) No rows or bad value for path %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
				RETURN;
			END;

			IF (
				SELECT COUNT(*)
				FROM @FileList AS fl
				WHERE fl.BackupFile = 'Access is denied.'
				) = 1
			BEGIN
				RAISERROR('(LOG) Access is denied to %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
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
					RAISERROR('(LOG) Empty directory %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
					RETURN;
				END

			IF (
				SELECT COUNT(*)
				FROM @FileList AS fl
				WHERE fl.BackupFile = 'The user name or password is incorrect.'
				) = 1
				BEGIN
					RAISERROR('(LOG) Incorrect user name or password for %s', 16, 1, @CurrentBackupPathLog) WITH NOWAIT;
					RETURN;
				END;
		END;
	END
    /*End folder sanity check*/

IF @Debug = 1
BEGIN
	SELECT * FROM @FileList WHERE BackupFile IS NOT NULL;
END

-- S3: when continuing logs, skip log backups that end before the restored database's LSN. They can't be applied, and their files
-- may already be cleaned up on the storage while msdb still lists them.
IF @IsS3 = 1 AND @ContinueLogs = 1 AND @DatabaseLastLSN IS NOT NULL
	DELETE fl
	FROM @FileList AS fl
	INNER JOIN @S3Backups AS s3 ON s3.FolderUrl = fl.BackupPath COLLATE Latin1_General_BIN2
		AND s3.BackupFile = fl.BackupFile COLLATE Latin1_General_BIN2
		AND s3.BackupType = 'L'
	WHERE s3.LastLSN <= @DatabaseLastLSN;

IF @SkipBackupsAlreadyInMsdb = 1
BEGIN

	SELECT TOP 1 @LogLastNameInMsdbAS = bf.physical_device_name
		FROM msdb.dbo.backupmediafamily bf
		INNER JOIN msdb.dbo.backupset bs ON bs.media_set_id = bf.media_set_id
		INNER JOIN msdb.dbo.restorehistory rh ON rh.backup_set_id = bs.backup_set_id
		WHERE physical_device_name like @BackupPathLog + '%'
		AND rh.destination_database_name = @UnquotedRestoreDatabaseName
		ORDER BY physical_device_name DESC

	IF @Debug = 1
	BEGIN
		SELECT 'Keeping LOG backups with name > : ' + @LogLastNameInMsdbAS
	END

	DELETE fl
	FROM @FileList AS fl
	WHERE fl.BackupPath + fl.BackupFile <= @LogLastNameInMsdbAS
END



IF (@OnlyLogsAfter IS NOT NULL)
BEGIN

	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@OnlyLogsAfter is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;

    DELETE fl
	FROM @FileList AS fl
	WHERE BackupFile LIKE N'%.trn'
	AND BackupFile LIKE N'%' + @Database + N'%'
	AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( fl.BackupFile ) ) END ), '' ), 16 ), '_', '' ) < @OnlyLogsAfter;

END

-- Check for log backups
IF(@BackupDateTime IS NOT NULL AND @BackupDateTime <> '')
	BEGIN
		DELETE FROM @FileList
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND NOT (@ContinueLogs = 1 OR (@ContinueLogs = 0 AND REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' ) >= @BackupDateTime));
	END;


IF (@StandbyMode = 1)
	BEGIN
		IF (@StandbyUndoPath IS NULL)
			BEGIN
				IF @Execute = 'Y' OR @Debug = 1 RAISERROR('The file path of the undo file for standby mode was not specified. Logs will not be restored in standby mode.', 0, 1) WITH NOWAIT;
			END;
		ELSE
			SET @LogRecoveryOption = N'STANDBY = ''' + REPLACE(@StandbyUndoPath, N'''', N'''''') + REPLACE(@Database, N'''', N'''''') + 'Undo.ldf''';
	END;

IF (@LogRecoveryOption = N'')
	BEGIN
		SET @LogRecoveryOption = N'NORECOVERY';
	END;

IF (@StopAt IS NOT NULL)
BEGIN

	IF @Execute = 'Y' OR @Debug = 1 RAISERROR('@StopAt is NOT NULL, deleting from @FileList', 0, 1) WITH NOWAIT;

	IF LEN(@StopAt) <> 14 OR PATINDEX('%[^0-9]%', @StopAt) > 0
	BEGIN
		RAISERROR('@StopAt parameter is incorrect. It should contain exactly 14 digits in the format yyyyMMddhhmmss.', 16, 1) WITH NOWAIT;
		RETURN
	END

	IF ISDATE(STUFF(STUFF(STUFF(@StopAt, 13, 0, ':'), 11, 0, ':'), 9, 0, ' ')) = 0
	BEGIN
		RAISERROR('@StopAt is not a valid datetime.', 16, 1) WITH NOWAIT;
		RETURN
	END

	-- Add the STOPAT parameter to the log recovery options but change the value to a valid DATETIME, e.g. '20211118040230' -> '20211118 04:02:30'
	SET @LogRecoveryOption += ', STOPAT = ''' + STUFF(STUFF(STUFF(@StopAt, 13, 0, ':'), 11, 0, ':'), 9, 0, ' ') + ''''

	IF @BackupDateTime = @StopAt
	BEGIN
		IF @Debug = 1
		BEGIN
			RAISERROR('@StopAt is the end time of a FULL backup, no log files will be restored.', 0, 1) WITH NOWAIT;
		END
	END
	ELSE
	BEGIN
		DECLARE @ExtraLogFile NVARCHAR(255)
		SELECT TOP 1 @ExtraLogFile = fl.BackupFile
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( fl.BackupFile ) ) END ), '' ), 16 ), '_', '' ) > @StopAt
		ORDER BY BackupFile;
	END

	IF @ExtraLogFile IS NULL
	BEGIN
		DELETE fl
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND REPLACE( RIGHT( REPLACE( fl.BackupFile, RIGHT( fl.BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( fl.BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( fl.BackupFile ) ) END ), '' ), 16 ), '_', '' ) > @StopAt;
	END
	ELSE
	BEGIN
		-- If this is a split backup, @ExtraLogFile contains only the first split backup file, either _1.trn or _01.trn
		-- Change @ExtraLogFile to the max split backup file, then delete all log files greater than this
		SET @ExtraLogFile = REPLACE(REPLACE(@ExtraLogFile, '_1.trn', '_9.trn'), '_01.trn', '_64.trn')

		DELETE fl
		FROM @FileList AS fl
		WHERE BackupFile LIKE N'%.trn'
		AND BackupFile LIKE N'%' + @Database + N'%'
		AND fl.BackupFile >	@ExtraLogFile
	END
END


 -- Group Ordering based on Backup File Name excluding Index {#} to construct coma separated string in "Restore Log" Command
SELECT BackupPath,BackupFile,DENSE_RANK() OVER (ORDER BY REPLACE( RIGHT( REPLACE( BackupFile, RIGHT( BackupFile, CASE WHEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) <= 7 THEN PATINDEX( '%[_]%', REVERSE( BackupFile ) ) ELSE CHARINDEX( '.', REVERSE( BackupFile ) ) END ), '' ), 16 ), '_', '' )) AS DenseRank INTO #SplitLogBackups
FROM @FileList
WHERE BackupFile IS NOT NULL;

-- Loop through all the files for the database
	WHILE 1 = 1
		BEGIN

			-- Get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement
			SELECT TOP 1 @CurrentBackupPathLog = BackupPath, @BackupFile = BackupFile FROM #SplitLogBackups WHERE DenseRank = @LogRestoreRanking;
			IF @@rowcount = 0 BREAK;

			IF @i = 1

			BEGIN
		    SET @sql = REPLACE(@HeadersSQL, N'{Path}', REPLACE(@CurrentBackupPathLog + @BackupFile, N'''', REPLICATE(N'''', 4)));

				IF @Debug = 1
				BEGIN
					IF @sql IS NULL PRINT '@sql is NULL for REPLACE: @HeadersSQL, @CurrentBackupPathLog, @BackupFile';
					PRINT @sql;
				END;

			EXECUTE (@sql);

			SELECT TOP 1 @LogFirstLSN = CAST(FirstLSN AS NUMERIC(25, 0)),
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

				IF (SELECT COUNT( * ) FROM #SplitLogBackups WHERE DenseRank = @LogRestoreRanking) > 1
				BEGIN
					IF @Debug = 1 RAISERROR ('Split backups found', 0, 1) WITH NOWAIT;
					SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM '
							   + STUFF(
									(SELECT CHAR( 10 ) + ',' + @DeviceType + '=''' + REPLACE(BackupPath, '''', '''''') + REPLACE(BackupFile, '''', '''''') + ''''
									 FROM #SplitLogBackups
									 WHERE DenseRank = @LogRestoreRanking
									 ORDER BY BackupFile
									 FOR XML PATH (''), TYPE).value('.', 'nvarchar(max)'),
								1,
								2,
								'' ) + N' WITH ' + @LogRecoveryOption + NCHAR(13) + NCHAR(10);
				END;
				ELSE
				SET @sql = N'RESTORE LOG ' + @RestoreDatabaseName + N' FROM ' + @DeviceType + N' = ''' + REPLACE(@CurrentBackupPathLog, N'''', N'''''') + REPLACE(@BackupFile, N'''', N'''''') + N''' WITH ' + @LogRecoveryOption + NCHAR(13) + NCHAR(10);

					IF @Debug = 1 OR @Execute = 'N'
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for RESTORE LOG: @RestoreDatabaseName, @CurrentBackupPathLog, @BackupFile';
						PRINT @sql;
					END;

					IF @Debug IN (0, 1) AND @Execute = 'Y'
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RESTORE LOG', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
			END;

			SET @LogRestoreRanking += 1;
		END;


	IF @Debug = 1
	BEGIN
	    SELECT '#SplitLogBackups' AS table_name, BackupPath, BackupFile FROM #SplitLogBackups;
	END
END

-- Put database in a useable state
IF @RunRecovery = 1
	BEGIN
		SET @sql = N'RESTORE DATABASE ' + @RestoreDatabaseName + N' WITH RECOVERY';

		IF @KeepCdc = 1
			SET @sql = @sql + N', KEEP_CDC';

		IF @EnableBroker = 1
			SET @sql = @sql + N', ENABLE_BROKER';

		SET @sql = @sql + NCHAR(13);

		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for RESTORE DATABASE: @RestoreDatabaseName';
			PRINT @sql;
		END;

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'RECOVER DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
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
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'SIMPLE LOGGING', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
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
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'DBCC CHECKDB', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END;


IF @DatabaseOwner IS NOT NULL
	BEGIN
		IF @RunRecovery = 1
		BEGIN
			IF EXISTS (SELECT * FROM master.dbo.syslogins WHERE syslogins.loginname = @DatabaseOwner)
			BEGIN
				SET @sql = N'ALTER AUTHORIZATION ON DATABASE::' + @RestoreDatabaseName + N' TO ' + QUOTENAME(@DatabaseOwner);

					IF @Debug = 1 OR @Execute = 'N'
					BEGIN
						IF @sql IS NULL PRINT '@sql is NULL for Set Database Owner';
						PRINT @sql;
					END;

				IF @Debug IN (0, 1) AND @Execute = 'Y'
					EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master',@Command = @sql, @CommandType = 'ALTER AUTHORIZATION', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
			END
			ELSE
			BEGIN
				PRINT @DatabaseOwner + ' is not a valid Login. Database Owner not set.';
			END
		END
		ELSE
		BEGIN
			PRINT @RestoreDatabaseName + ' is still in Recovery, so we are unable to change the database owner to [' + @DatabaseOwner + '].';
		END
	END;

	IF @SetTrustworthyON = 1
		BEGIN
			IF @RunRecovery = 1
			BEGIN
				IF IS_SRVROLEMEMBER('sysadmin') = 1
				BEGIN
					SET @sql = N'ALTER DATABASE ' + @RestoreDatabaseName + N' SET TRUSTWORTHY ON;';

						IF @Debug = 1 OR @Execute = 'N'
						BEGIN
							IF @sql IS NULL PRINT '@sql is NULL for SET TRUSTWORTHY ON';
							PRINT @sql;
						END;

					IF @Debug IN (0, 1) AND @Execute = 'Y'
						EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master', @Command = @sql, @CommandType = 'ALTER DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
				END
				ELSE
				BEGIN
					PRINT 'Current user''s login is NOT a member of the sysadmin role. Database TRUSTWORTHY bit has not been enabled.';
				END
			END
			ELSE
			BEGIN
				PRINT @RestoreDatabaseName + ' is still in Recovery, so we are unable to enable the TRUSTWORTHY bit.';
			END
		END;

-- Link a user entry in the sys.database_principals system catalog view in the restored database to a SQL Server login of the same name
IF @FixOrphanUsers = 1
	BEGIN
		SET @sql = N'
-- Fixup Orphan Users by setting database user sid to match login sid
DECLARE @FixOrphansSql NVARCHAR(MAX);
DECLARE @OrphanUsers TABLE (SqlToExecute NVARCHAR(MAX));
USE ' + @RestoreDatabaseName + ';

INSERT @OrphanUsers
SELECT ''ALTER USER ['' + d.name + ''] WITH LOGIN = ['' + d.name + '']; ''
	FROM  sys.database_principals d
	INNER JOIN master.sys.server_principals s ON d.name COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT
	WHERE d.type_desc = ''SQL_USER''
		AND d.name NOT IN (''guest'',''dbo'')
		AND d.sid <> s.sid
	ORDER BY d.name;

SELECT @FixOrphansSql = (SELECT SqlToExecute AS [text()] FROM @OrphanUsers FOR XML PATH (''''), TYPE).value(''text()[1]'',''NVARCHAR(MAX)'');

IF @FixOrphansSql IS NULL
	PRINT ''No orphan users require a sid fixup.'';
ELSE
BEGIN
	PRINT ''Fix Orphan Users: '' + @FixOrphansSql;
	EXECUTE(@FixOrphansSql);
END;'

		IF @Debug = 1 OR @Execute = 'N'
		BEGIN
			IF @sql IS NULL PRINT '@sql is NULL for Fix Orphan Users';
			PRINT @sql;
		END;

		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXECUTE [dbo].[CommandExecute] @DatabaseContext = 'master', @Command = @sql, @CommandType = 'UPDATE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';
	END;

IF @RunStoredProcAfterRestore IS NOT NULL AND LEN(LTRIM(@RunStoredProcAfterRestore)) > 0
BEGIN
	/* Shape and whitespace already validated near the top of the proc; @RunStoredProcNormalized
	   holds the trimmed/dot-normalized form. Re-PARSENAME here just to extract the parts. */
	DECLARE @RunStoredProcSchema sysname = NULLIF(PARSENAME(@RunStoredProcNormalized, 2), N'');
	DECLARE @RunStoredProcName   sysname = PARSENAME(@RunStoredProcNormalized, 1);
	PRINT 'Attempting to run ' + @RunStoredProcAfterRestore
	/* Always emit a 3-part name (db.schema.proc). For 1-part input the schema slot is left empty
	   ([db]..[proc]) so SQL Server applies its own schema-resolution rules in the target DB.
	   Without the second dot, [db].[proc] is parsed as schema.object in the *current* DB. */
	SET @sql = N'EXEC ' + @RestoreDatabaseName + N'.'
	         + ISNULL(QUOTENAME(@RunStoredProcSchema), N'')
	         + N'.'
	         + QUOTENAME(@RunStoredProcName);

	IF @Debug = 1 OR @Execute = 'N'
	BEGIN
		IF @sql IS NULL PRINT '@sql is NULL when building for @RunStoredProcAfterRestore'
		PRINT @sql
	END

	IF @RunRecovery = 0
	BEGIN
		PRINT 'Unable to run Run Stored Procedure After Restore as database is not recovered. Run command again with @RunRecovery = 1'
	END
	ELSE
    BEGIN
		IF @Debug IN (0, 1) AND @Execute = 'Y'
			EXEC sp_executesql  @sql
	END
END

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
			EXECUTE @sql = [dbo].[CommandExecute] @DatabaseContext=N'master',@Command = @sql, @CommandType = 'DROP DATABASE', @Mode = 1, @DatabaseName = @UnquotedRestoreDatabaseName, @LogToTable = 'Y', @Execute = 'Y';

	END;

-- Clean-Up Tempdb Objects
IF OBJECT_ID( 'tempdb..#SplitFullBackups' ) IS NOT NULL DROP TABLE #SplitFullBackups;
IF OBJECT_ID( 'tempdb..#SplitDiffBackups' ) IS NOT NULL DROP TABLE #SplitDiffBackups;
IF OBJECT_ID( 'tempdb..#SplitLogBackups' ) IS NOT NULL DROP TABLE #SplitLogBackups;
END
GO
