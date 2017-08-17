--USE [master]
--GO

--/****** Object:  StoredProcedure [dbo].[AGDatabaseRestoreOP]    Script Date: 8/15/2017 7:15:08 PM ******/
--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO

----example
----exec [dbo].[AGDatabaseRestoreOP] 'Db Name', '//sqlbackup/dev'

--CREATE procedure [dbo].[AGDatabaseRestoreOP](
--    @DbName NVARCHAR(4000),
--	@OlaBackupPath nvarchar(200)
--)
--AS
--BEGIN
--declare @AgName varchar(100)
--declare @ClusterName varchar(50)
--declare @FullPathForRestore nvarchar(max)

--select @ClusterName=cluster_name from sys.dm_hadr_cluster

--SELECT
--@AgName=AG.name 
--FROM master.sys.availability_groups AS AG
--LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates   ON AG.group_id = agstates.group_id
--INNER JOIN master.sys.availability_replicas AS AR    ON AG.group_id = AR.group_id
--INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates   ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
--INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs   ON arstates.replica_id = dbcs.replica_id
--where dbcs.database_name=@DbName


--IF(RIGHT(@OlaBackupPath,1)<>'\')
--BEGIN
--	set @OlaBackupPath=@OlaBackupPath+'\';
--END

--set @FullPathForRestore = ''''+@OlaBackupPath+@ClusterName+'$'+@AgName+'\'+@DbName;

--Declare @str nvarchar(max)='
--EXEC dbo.DatabaseRestore 
--  @Database = '''+@DbName+''', 
--  @BackupPathFull = '+@FullPathForRestore+'\FULL\'', 
--  @BackupPathDiff = '+@FullPathForRestore+'\DIFF\'', 
--  @BackupPathLog = '+@FullPathForRestore+'\LOG\'', 
--  @RestoreDiff = 1,   @ContinueLogs = 0,   @RunRecovery = 0;'
 
--exec(@str)

--DECLARE @sql nvarchar(max)='ALTER DATABASE ['+@DbName+'] SET HADR AVAILABILITY GROUP = ['+@AgName+']'+CHAR(13);

--EXECUTE  [dbo].[CommandExecute] @Command = @sql, @CommandType = 'SET HADR AG', @Mode = 1, @DatabaseName = @DbName, @LogToTable = 'Y', @Execute = 'Y';


--END

--GO








select @ClusterName=cluster_name from sys.dm_hadr_cluster

SELECT
@AgName=AG.name 
FROM master.sys.availability_groups AS AG
LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates   ON AG.group_id = agstates.group_id
INNER JOIN master.sys.availability_replicas AS AR    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates   ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs   ON arstates.replica_id = dbcs.replica_id
where dbcs.database_name=@DbName
