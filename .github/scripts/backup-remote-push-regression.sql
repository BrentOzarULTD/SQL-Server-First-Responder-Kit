/* Manual integration test on two disposable SQL Server instances.
   Install this branch's sp_BlitzBackups in source master. Provision an exclusive
   FRKPushRemote linked server to the destination, with data access and RPC OUT,
   and a login that can create/drop databases and alter dbo.backupset there.
   Run with sqlcmd -b. The test toggles only this dedicated link's RPC OUT option.
   No source msdb metadata is modified. */
IF DB_ID(N'FRKPushSource') IS NOT NULL
    THROW 51000,'Source fixture already exists.',1;
IF EXISTS(SELECT 1 FROM FRKPushRemote.master.sys.databases WHERE name=N'FRKPushHistory')
    THROW 51000,'Remote fixture already exists.',1;
CREATE DATABASE FRKPushSource;
GO
DECLARE @File nvarchar(512)=CONVERT(nvarchar(400),SERVERPROPERTY('InstanceDefaultDataPath'))+N'FRKPushSource.bak',
        @Definition nvarchar(max),@UUID uniqueidentifier,@RemoteCreated bit=0;
BEGIN TRY
    BACKUP DATABASE FRKPushSource TO DISK=@File WITH INIT;
    SELECT @UUID=backup_set_uuid FROM msdb.dbo.backupset WHERE database_name=N'FRKPushSource'
      AND database_guid=(SELECT database_guid FROM sys.database_recovery_status WHERE database_id=DB_ID(N'FRKPushSource'));
    SELECT @Definition=definition FROM sys.sql_modules WHERE object_id=OBJECT_ID(N'dbo.sp_BlitzBackups');
    SET @Definition=REPLACE(@Definition,N'ALTER PROCEDURE',N'CREATE PROCEDURE');
    EXEC FRKPushSource.sys.sp_executesql @Definition;
    EXEC(N'CREATE DATABASE FRKPushHistory;') AT FRKPushRemote;
    SET @RemoteCreated=1;
    EXEC(N'SELECT TOP(0) * INTO FRKPushHistory.dbo.backupset FROM msdb.dbo.backupset;') AT FRKPushRemote;
    /* Existing legacy table: remote DDL adds media columns, then four-part DML fills them. */
    EXEC FRKPushSource.dbo.sp_BlitzBackups @PushBackupHistoryToListener=1,
      @WriteBackupsToListenerName=N'FRKPushRemote',@WriteBackupsToDatabaseName=N'FRKPushHistory',@WriteBackupsLastHours=1;
    EXEC sys.sp_executesql N'IF NOT EXISTS(SELECT 1 FROM FRKPushRemote.FRKPushHistory.dbo.backupset WHERE backup_set_uuid=@UUID
      AND frk_media_is_usable=1 AND frk_media_has_discard=0)
        THROW 51000,''Remote schema upgrade or media mapping failed.'',1;',N'@UUID uniqueidentifier',@UUID;
    EXEC sys.sp_executesql N'UPDATE FRKPushRemote.FRKPushHistory.dbo.backupset SET frk_media_is_usable=0,frk_media_has_discard=1 WHERE backup_set_uuid=@UUID;',N'@UUID uniqueidentifier',@UUID;
    EXEC master.dbo.sp_serveroption N'FRKPushRemote',N'rpc out',N'false';
    /* A retained full outside a zero-hour window must refresh already populated facts without RPC. */
    EXEC FRKPushSource.dbo.sp_BlitzBackups @PushBackupHistoryToListener=1,
      @WriteBackupsToListenerName=N'FRKPushRemote',@WriteBackupsToDatabaseName=N'FRKPushHistory',@WriteBackupsLastHours=0;
    EXEC sys.sp_executesql N'IF NOT EXISTS(SELECT 1 FROM FRKPushRemote.FRKPushHistory.dbo.backupset WHERE backup_set_uuid=@UUID
      AND frk_media_is_usable=1 AND frk_media_has_discard=0)
        THROW 51000,''Remote existing media facts were not refreshed without RPC.'',1;',N'@UUID uniqueidentifier',@UUID;
    EXEC master.dbo.sp_serveroption N'FRKPushRemote',N'rpc out',N'true';
    EXEC(N'USE FRKPushHistory; CREATE TABLE dbo.UpdateAudit(RowsUpdated int);') AT FRKPushRemote;
    EXEC(N'USE FRKPushHistory; EXEC(N''CREATE TRIGGER dbo.TrackUpdates ON dbo.backupset AFTER UPDATE AS INSERT dbo.UpdateAudit SELECT COUNT(*) FROM inserted;'');') AT FRKPushRemote;
    EXEC FRKPushSource.dbo.sp_BlitzBackups @PushBackupHistoryToListener=1,
      @WriteBackupsToListenerName=N'FRKPushRemote',@WriteBackupsToDatabaseName=N'FRKPushHistory',@WriteBackupsLastHours=0;
    EXEC(N'IF EXISTS(SELECT 1 FROM FRKPushHistory.dbo.UpdateAudit WHERE RowsUpdated>0) THROW 51000,''Unchanged history rows were rewritten.'',1;') AT FRKPushRemote;
    EXEC(N'USE FRKPushHistory; DROP TRIGGER dbo.TrackUpdates;') AT FRKPushRemote;
    EXEC(N'ALTER TABLE FRKPushHistory.dbo.backupset DROP COLUMN frk_media_is_usable,frk_media_has_discard; DELETE FRKPushHistory.dbo.backupset;') AT FRKPushRemote;
    EXEC master.dbo.sp_serveroption N'FRKPushRemote',N'rpc out',N'false';
    /* Legacy destinations without RPC retain the supported history-only push. */
    EXEC FRKPushSource.dbo.sp_BlitzBackups @PushBackupHistoryToListener=1,
      @WriteBackupsToListenerName=N'FRKPushRemote',@WriteBackupsToDatabaseName=N'FRKPushHistory',@WriteBackupsLastHours=1;
    EXEC sys.sp_executesql N'IF NOT EXISTS(SELECT 1 FROM FRKPushRemote.FRKPushHistory.dbo.backupset WHERE backup_set_uuid=@UUID)
        THROW 51000,''Legacy history push requires RPC.'',1;',N'@UUID uniqueidentifier',@UUID;
END TRY
BEGIN CATCH
    EXEC master.dbo.sp_serveroption N'FRKPushRemote',N'rpc out',N'true';
    IF @RemoteCreated=1 EXEC(N'DROP DATABASE FRKPushHistory;') AT FRKPushRemote;
    IF DB_ID(N'FRKPushSource') IS NOT NULL DROP DATABASE FRKPushSource;
    THROW;
END CATCH;
EXEC master.dbo.sp_serveroption N'FRKPushRemote',N'rpc out',N'true';
EXEC(N'DROP DATABASE FRKPushHistory;') AT FRKPushRemote;
DROP DATABASE FRKPushSource;
PRINT 'PASS: remote push schema upgrade, media refresh, and RPC-disabled compatibility';
