DECLARE @database_name NVARCHAR(MAX), @backup_path NVARCHAR(MAX), @i INT

SET @database_name = 'ENTER YOUR DATABASE NAME'
SET @backup_path = 'Path to your Ola Backups ROOT FOLDER (see the db name will be attached later)'
SET @i = 0

EXECUTE dbo.DatabaseBackup
@Databases = @database_name,
@Directory = @backup_path,
@BackupType = 'FULL',
@Verify = 'Y',
@Compress = 'Y',
@CheckSum = 'Y',
@CleanupTime = 24

/*Why do I add delays, IDK just wanted to make sure streams do not get crossed */
WAITFOR DELAY '00:00:30'

EXECUTE dbo.DatabaseBackup
@Databases = @database_name,
@Directory = @backup_path,
@BackupType = 'DIFF',
@Verify = 'Y',
@Compress = 'Y',
@CheckSum = 'Y',
@CleanupTime = 24

WAITFOR DELAY '00:00:30'

WHILE (@i <= 10)
BEGIN
SET @i += 1
EXECUTE dbo.DatabaseBackup
@Databases = @database_name,
@Directory = @backup_path,
@BackupType = 'LOG',
@Verify = 'Y',
@Compress = 'Y',
@CheckSum = 'Y',
@CleanupTime = 24
WAITFOR DELAY '00:00:03'
END

DECLARE @cmd NVARCHAR(4000), @BackupPathDiff NVARCHAR(MAX), @PointInTime NVARCHAR(4000), @full_path NVARCHAR(MAX), @diff_path VARCHAR(MAX), @log_path VARCHAR(MAX)
DECLARE @FileList TABLE (BackupFile NVARCHAR(255))
SET @full_path = @backup_path + '\' + @database_name + '\FULL\'
SET @diff_path = @backup_path + '\' + @database_name + '\DIFF\'
SET @log_path =  @backup_path + '\' + @database_name + '\LOG\'

-- get list of files 
SET @cmd = 'DIR /b "'+ @log_path + '"';
INSERT INTO @FileList (BackupFile)
EXEC master.sys.xp_cmdshell @cmd; 

SET @PointInTime = (SELECT TOP 1 * FROM @FileList ORDER BY NEWID())
SET @PointInTime = REPLACE(LEFT(RIGHT(@PointInTime, 19),15), '_', '')
PRINT @full_path
PRINT @diff_path
PRINT @log_path

EXEC dbo.sp_DatabaseRestore 
	@Database = @database_name, 
	@BackupPathFull = @full_path, 
	@BackupPathDiff = @diff_path,
	@BackupPathLog = @log_path, 
	@RestoreDiff = 1,
	@ContinueLogs = 0, 
	@RunRecovery = 1,
	@StopAt = @PointInTime,
	@Debug = 1;
