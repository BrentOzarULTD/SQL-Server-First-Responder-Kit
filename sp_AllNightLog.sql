SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('dbo.sp_AllNightLog') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_AllNightLog AS RETURN 0;')
GO


ALTER PROCEDURE dbo.sp_AllNightLog
								@PollForNewDatabases BIT = 0, /* Formerly Pollster */
								@Backup BIT = 0, /* Formerly LogShaming */
								@PollDiskForNewDatabases BIT = 0,
								@Restore BIT = 0,
								@Debug BIT = 0,
								@Help BIT = 0,
								@Version                 VARCHAR(30) = NULL OUTPUT,
								@VersionDate             DATETIME = NULL OUTPUT,
								@VersionCheckMode        BIT = 0
WITH RECOMPILE
AS
SET NOCOUNT ON;
SET STATISTICS XML OFF;

BEGIN;


SELECT @Version = '8.10', @VersionDate = '20220718';

IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;

IF @Help = 1

BEGIN

	PRINT '		
		/*


		sp_AllNightLog from http://FirstResponderKit.org
		
		* @PollForNewDatabases = 1 polls sys.databases for new entries
			* Unfortunately no other way currently to automate new database additions when restored from backups
				* No triggers or extended events that easily do this
	
		* @Backup = 1 polls msdbCentral.dbo.backup_worker for databases not backed up in [RPO], takes LOG backups
			* Will switch to a full backup if none exists
	
	
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
	
		Known limitations of this version:
		 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000! And really, maybe not even anything less than 2016. Heh.
         - When restoring encrypted backups, the encryption certificate must already be installed.
	
		Unknown limitations of this version:
		 - None.  (If we knew them, they would be known. Duh.)
	
	     Changes - for the full list of improvements and fixes in this version, see:
	     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	
		Parameter explanations:
	
		  @PollForNewDatabases BIT, defaults to 0. When this is set to 1, runs in a perma-loop to find new entries in sys.databases 
		  @Backup BIT, defaults to 0. When this is set to 1, runs in a perma-loop checking the backup_worker table for databases that need to be backed up
		  @Debug BIT, defaults to 0. Whent this is set to 1, it prints out dynamic SQL commands
		  @RPOSeconds BIGINT, defaults to 30. Value in seconds you want to use to determine if a new log backup needs to be taken.
		  @BackupPath NVARCHAR(MAX), defaults to = ''D:\Backup''. You 99.99999% will need to change this path to something else. This tells Ola''s job where to put backups.
	
		For more documentation: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	    MIT License
		
		Copyright (c) 2021 Brent Ozar Unlimited
	
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

RETURN
END

DECLARE	@database NVARCHAR(128) = NULL; --Holds the database that's currently being processed
DECLARE @error_number INT = NULL; --Used for TRY/CATCH
DECLARE @error_severity INT; --Used for TRY/CATCH
DECLARE @error_state INT; --Used for TRY/CATCH
DECLARE @msg NVARCHAR(4000) = N''; --Used for RAISERROR
DECLARE @rpo INT; --Used to hold the RPO value in our configuration table
DECLARE @rto INT; --Used to hold the RPO value in our configuration table
DECLARE @backup_path NVARCHAR(MAX); --Used to hold the backup path in our configuration table
DECLARE @changebackuptype NVARCHAR(MAX); --Config table: Y = escalate to full backup, MSDB = escalate if MSDB history doesn't show a recent full.
DECLARE @encrypt NVARCHAR(MAX); --Config table: Y = encrypt the backup. N (default) = do not encrypt.
DECLARE @encryptionalgorithm NVARCHAR(MAX); --Config table: native 2014 choices include TRIPLE_DES_3KEY, AES_128, AES_192, AES_256
DECLARE @servercertificate NVARCHAR(MAX); --Config table: server certificate that is used to encrypt the backup
DECLARE @restore_path_base NVARCHAR(MAX); --Used to hold the base backup path in our configuration table
DECLARE @restore_path_full NVARCHAR(MAX); --Used to hold the full backup path in our configuration table
DECLARE @restore_path_log NVARCHAR(MAX); --Used to hold the log backup path in our configuration table
DECLARE @restore_move_files INT; -- used to hold the move files bit in our configuration table
DECLARE @db_sql NVARCHAR(MAX) = N''; --Used to hold the dynamic SQL to create msdbCentral
DECLARE @tbl_sql NVARCHAR(MAX) = N''; --Used to hold the dynamic SQL that creates tables in msdbCentral
DECLARE @database_name NVARCHAR(256) = N'msdbCentral'; --Used to hold the name of the database we create to centralize data
													   --Right now it's hardcoded to msdbCentral, but I made it dynamic in case that changes down the line
DECLARE @cmd NVARCHAR(4000) = N'' --Holds dir cmd 
DECLARE @FileList TABLE ( BackupFile NVARCHAR(255) ); --Where we dump @cmd
DECLARE @restore_full BIT = 0 --We use this one
DECLARE @only_logs_after NVARCHAR(30) = N''



/*

Make sure we're doing something

*/

IF (
		  @PollForNewDatabases = 0
	  AND @PollDiskForNewDatabases = 0
	  AND @Backup = 0
	  AND @Restore = 0
	  AND @Help = 0
)
		BEGIN 		
			RAISERROR('You don''t seem to have picked an action for this stored procedure to take.', 0, 1) WITH NOWAIT
			
			RETURN;
		END 

/*
Make sure xp_cmdshell is enabled
*/
IF NOT EXISTS (SELECT * FROM sys.configurations WHERE name = 'xp_cmdshell' AND value_in_use = 1)
		BEGIN 		
			RAISERROR('xp_cmdshell must be enabled so we can get directory contents to check for new databases to restore.', 0, 1) WITH NOWAIT
			
			RETURN;
		END 

/*
Make sure Ola Hallengren's scripts are installed in same database
*/
DECLARE @CurrentDatabaseContext nvarchar(128) = DB_NAME();
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'CommandExecute')
		BEGIN 		
			RAISERROR('Ola Hallengren''s CommandExecute must be installed in the same database (%s) as SQL Server First Responder Kit. More info: http://ola.hallengren.com', 0, 1, @CurrentDatabaseContext) WITH NOWAIT;
			
			RETURN;
		END 
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'DatabaseBackup')
		BEGIN 		
			RAISERROR('Ola Hallengren''s DatabaseBackup must be installed in the same database (%s) as SQL Server First Responder Kit. More info: http://ola.hallengren.com', 0, 1, @CurrentDatabaseContext) WITH NOWAIT;
			
			RETURN;
		END 

/*
Make sure sp_DatabaseRestore is installed in same database
*/
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_DatabaseRestore')
		BEGIN 		
			RAISERROR('sp_DatabaseRestore must be installed in the same database (%s) as SQL Server First Responder Kit. To get it: http://FirstResponderKit.org', 0, 1, @CurrentDatabaseContext) WITH NOWAIT;
			
			RETURN;
		END 


IF (@PollDiskForNewDatabases = 1 OR @Restore = 1) AND OBJECT_ID('msdb.dbo.restore_configuration') IS NOT NULL
    BEGIN

		IF @Debug = 1 RAISERROR('Checking restore path', 0, 1) WITH NOWAIT;

		SELECT @restore_path_base = CONVERT(NVARCHAR(512), configuration_setting)
		FROM msdb.dbo.restore_configuration c
		WHERE configuration_name = N'log restore path';


		IF @restore_path_base IS NULL
			BEGIN
				RAISERROR('@restore_path cannot be NULL. Please check the msdb.dbo.restore_configuration table', 0, 1) WITH NOWAIT;
				RETURN;
			END;

        IF CHARINDEX('**', @restore_path_base) <> 0
            BEGIN

                /* If they passed in a dynamic **DATABASENAME**, stop at that folder looking for databases. More info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/993 */
                IF CHARINDEX('**DATABASENAME**', @restore_path_base) <> 0
                    BEGIN
                    SET @restore_path_base = SUBSTRING(@restore_path_base, 1, CHARINDEX('**DATABASENAME**',@restore_path_base) - 2);
                    END;

                SET @restore_path_base = REPLACE(@restore_path_base, '**AVAILABILITYGROUP**', '');
                SET @restore_path_base = REPLACE(@restore_path_base, '**BACKUPTYPE**', 'FULL');
                SET @restore_path_base = REPLACE(@restore_path_base, '**SERVERNAME**', REPLACE(CAST(SERVERPROPERTY('servername') AS nvarchar(max)),'\','$'));

                IF CHARINDEX('\',CAST(SERVERPROPERTY('servername') AS nvarchar(max))) > 0
                    BEGIN
                        SET @restore_path_base = REPLACE(@restore_path_base, '**SERVERNAMEWITHOUTINSTANCE**', SUBSTRING(CAST(SERVERPROPERTY('servername') AS nvarchar(max)), 1, (CHARINDEX('\',CAST(SERVERPROPERTY('servername') AS nvarchar(max))) - 1)));
                        SET @restore_path_base = REPLACE(@restore_path_base, '**INSTANCENAME**', SUBSTRING(CAST(SERVERPROPERTY('servername') AS nvarchar(max)), CHARINDEX('\',CAST(SERVERPROPERTY('servername') AS nvarchar(max))), (LEN(CAST(SERVERPROPERTY('servername') AS nvarchar(max))) - CHARINDEX('\',CAST(SERVERPROPERTY('servername') AS nvarchar(max)))) + 1));
                    END
                    ELSE /* No instance installed */
                    BEGIN
                        SET @restore_path_base = REPLACE(@restore_path_base, '**SERVERNAMEWITHOUTINSTANCE**', CAST(SERVERPROPERTY('servername') AS nvarchar(max)));
                        SET @restore_path_base = REPLACE(@restore_path_base, '**INSTANCENAME**', 'DEFAULT');
                    END

                IF CHARINDEX('**CLUSTER**', @restore_path_base) <> 0
                    BEGIN
                    DECLARE @ClusterName NVARCHAR(128);
                    IF EXISTS(SELECT * FROM sys.all_objects WHERE name = 'dm_hadr_cluster')
                        BEGIN
                            SELECT @ClusterName = cluster_name FROM sys.dm_hadr_cluster;
                        END
                    SET @restore_path_base = REPLACE(@restore_path_base, '**CLUSTER**', COALESCE(@ClusterName,''));
                    END;

            END /* IF CHARINDEX('**', @restore_path_base) <> 0 */
		
		SELECT @restore_move_files = CONVERT(BIT, configuration_setting)
		FROM msdb.dbo.restore_configuration c
		WHERE configuration_name = N'move files';
		
		IF @restore_move_files is NULL
		BEGIN
			-- Set to default value of 1
			SET @restore_move_files = 1
		END

    END /* IF @PollDiskForNewDatabases = 1 OR @Restore = 1 */


/*

Certain variables necessarily skip to parts of this script that are irrelevant
in both directions to each other. They are used for other stuff.

*/


/*

Pollster use happens strictly to check for new databases in sys.databases to place them in a worker queue

*/

IF @PollForNewDatabases = 1
	GOTO Pollster;

/*

LogShamer happens when we need to find and assign work to a worker job for backups

*/

IF @Backup = 1
	GOTO LogShamer;

/*

Pollster use happens strictly to check for new databases in sys.databases to place them in a worker queue

*/

IF @PollDiskForNewDatabases = 1
	GOTO DiskPollster;


/*

Restoregasm Addict happens when we need to find and assign work to a worker job for restores

*/

IF @Restore = 1
	GOTO Restoregasm_Addict;



/*

Begin Polling section

*/



/*

This section runs in a loop checking for new databases added to the server, or broken backups

*/


Pollster:

	IF @Debug = 1 RAISERROR('Beginning Pollster', 0, 1) WITH NOWAIT;
	
	IF OBJECT_ID('msdbCentral.dbo.backup_worker') IS NOT NULL
	
		BEGIN
		
			WHILE @PollForNewDatabases = 1
			
			BEGIN
				
				BEGIN TRY
			
					IF @Debug = 1 RAISERROR('Checking for new databases...', 0, 1) WITH NOWAIT;

					/*
					
					Look for new non-system databases -- there should probably be additional filters here for accessibility, etc.

					*/
	
						INSERT msdbCentral.dbo.backup_worker (database_name) 
						SELECT d.name
						FROM sys.databases d
						WHERE NOT EXISTS (
							SELECT 1 
							FROM msdbCentral.dbo.backup_worker bw
							WHERE bw.database_name = d.name
										)
						AND d.database_id > 4;

						IF @Debug = 1 RAISERROR('Checking for wayward databases', 0, 1) WITH NOWAIT;

						/*
							
						This section aims to find databases that have
							* Had a log backup ever (the default for finish time is 9999-12-31, so anything with a more recent finish time has had a log backup)
							* Not had a log backup start in the last 5 minutes (this could be trouble! or a really big log backup)
							* Also checks msdb.dbo.backupset to make sure the database has a full backup associated with it (otherwise it's the first full, and we don't need to start taking log backups yet)

						*/
	
						IF EXISTS (
								
							SELECT 1
							FROM msdbCentral.dbo.backup_worker bw WITH (READPAST)
							WHERE bw.last_log_backup_finish_time < '99991231'
							AND bw.last_log_backup_start_time < DATEADD(SECOND, (@rpo * -1), GETDATE())				
							AND EXISTS (
									SELECT 1
									FROM msdb.dbo.backupset b
									WHERE b.database_name = bw.database_name
									AND b.type = 'D'
										)								
								)
	
							BEGIN
									
								IF @Debug = 1 RAISERROR('Resetting databases with a log backup and no log backup in the last 5 minutes', 0, 1) WITH NOWAIT;

	
									UPDATE bw
											SET bw.is_started = 0,
												bw.is_completed = 1,
												bw.last_log_backup_start_time = '19000101'
									FROM msdbCentral.dbo.backup_worker bw
									WHERE bw.last_log_backup_finish_time < '99991231'
									AND bw.last_log_backup_start_time < DATEADD(SECOND, (@rpo * -1), GETDATE())
									AND EXISTS (
											SELECT 1
											FROM msdb.dbo.backupset b
											WHERE b.database_name = bw.database_name
											AND b.type = 'D'
												);

								
								END; --End check for wayward databases

						/*
						
						Wait 1 minute between runs, we don't need to be checking this constantly
						
						*/

	
					IF @Debug = 1 RAISERROR('Waiting for 1 minute', 0, 1) WITH NOWAIT;
					
					WAITFOR DELAY '00:01:00.000';

				END TRY

				BEGIN CATCH


						SELECT @msg = N'Error inserting databases to msdbCentral.dbo.backup_worker, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;

	
						WHILE @@TRANCOUNT > 0
							ROLLBACK;


				END CATCH;
	
			
			END; 

        /* Check to make sure job is still enabled */
		IF NOT EXISTS (
						SELECT *
						FROM msdb.dbo.sysjobs 
						WHERE name = 'sp_AllNightLog_PollForNewDatabases'
                        AND enabled = 1
						)
            BEGIN
				RAISERROR('sp_AllNightLog_PollForNewDatabases job is disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) WITH NOWAIT;
				RETURN;
            END        
		
		END;-- End Pollster loop
	
		ELSE
	
			BEGIN
	
				RAISERROR('msdbCentral.dbo.backup_worker does not exist, please create it.', 0, 1) WITH NOWAIT;
				RETURN;
			
			END; 
	RETURN;


/*

End of Pollster

*/


/*

Begin DiskPollster

*/


/*

This section runs in a loop checking restore path for new databases added to the server, or broken restores

*/

DiskPollster:

	IF @Debug = 1 RAISERROR('Beginning DiskPollster', 0, 1) WITH NOWAIT;
	
	IF OBJECT_ID('msdb.dbo.restore_configuration') IS NOT NULL
	
		BEGIN
		
			WHILE @PollDiskForNewDatabases = 1
			
			BEGIN
				
				BEGIN TRY
			
					IF @Debug = 1 RAISERROR('Checking for new databases in: ', 0, 1) WITH NOWAIT;
					IF @Debug = 1 RAISERROR(@restore_path_base, 0, 1) WITH NOWAIT;

					/*
					
					Look for new non-system databases -- there should probably be additional filters here for accessibility, etc.

					*/
						
						/*
						
						This setups up the @cmd variable to check the restore path for new folders
						
						In our case, a new folder means a new database, because we assume a pristine path

						*/

						SET @cmd = N'DIR /b "' + @restore_path_base + N'"';
						
									IF @Debug = 1
									BEGIN
										PRINT @cmd;
									END  
						
						
                        DELETE @FileList;
						INSERT INTO @FileList (BackupFile)
						EXEC master.sys.xp_cmdshell @cmd; 
						
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
						
								RAISERROR('Access is denied to %s', 16, 1, @restore_path_base) WITH NOWAIT;

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

						IF (
							SELECT COUNT(*) 
							FROM @FileList AS fl 
							WHERE fl.BackupFile = 'The user name or password is incorrect.'
							) = 1

							BEGIN
						
								RAISERROR('Incorrect user name or password for %s', 16, 1, @restore_path_base) WITH NOWAIT;

							END;

						INSERT msdb.dbo.restore_worker (database_name) 
						SELECT fl.BackupFile
						FROM @FileList AS fl
						WHERE fl.BackupFile IS NOT NULL
						AND fl.BackupFile NOT IN (SELECT name from sys.databases where database_id < 5)
						AND NOT EXISTS
							(
							SELECT 1
							FROM msdb.dbo.restore_worker rw
							WHERE rw.database_name = fl.BackupFile
							)

						IF @Debug = 1 RAISERROR('Checking for wayward databases', 0, 1) WITH NOWAIT;

						/*
							
						This section aims to find databases that have
							* Had a log restore ever (the default for finish time is 9999-12-31, so anything with a more recent finish time has had a log restore)
							* Not had a log restore start in the last 5 minutes (this could be trouble! or a really big log restore)
							* Also checks msdb.dbo.backupset to make sure the database has a full backup associated with it (otherwise it's the first full, and we don't need to start adding log restores yet)

						*/
	
						IF EXISTS (
								
							SELECT 1
							FROM msdb.dbo.restore_worker rw WITH (READPAST)
							WHERE rw.last_log_restore_finish_time < '99991231'
							AND rw.last_log_restore_start_time < DATEADD(SECOND, (@rto * -1), GETDATE())			
							AND EXISTS (
									SELECT 1
									FROM msdb.dbo.restorehistory r
									WHERE r.destination_database_name = rw.database_name
									AND r.restore_type = 'D'
										)								
								)
	
							BEGIN
									
								IF @Debug = 1 RAISERROR('Resetting databases with a log restore and no log restore in the last 5 minutes', 0, 1) WITH NOWAIT;

	
									UPDATE rw
											SET rw.is_started = 0,
												rw.is_completed = 1,
												rw.last_log_restore_start_time = '19000101'
									FROM msdb.dbo.restore_worker rw
									WHERE rw.last_log_restore_finish_time < '99991231'
									AND rw.last_log_restore_start_time < DATEADD(SECOND, (@rto * -1), GETDATE())
									AND EXISTS (
											SELECT 1
											FROM msdb.dbo.restorehistory r
											WHERE r.destination_database_name = rw.database_name
											AND r.restore_type = 'D'
												);

								
								END; --End check for wayward databases

						/*
						
						Wait 1 minute between runs, we don't need to be checking this constantly
						
						*/

                    /* Check to make sure job is still enabled */
		            IF NOT EXISTS (
						            SELECT *
						            FROM msdb.dbo.sysjobs 
						            WHERE name = 'sp_AllNightLog_PollDiskForNewDatabases'
                                    AND enabled = 1
						            )
                        BEGIN
				            RAISERROR('sp_AllNightLog_PollDiskForNewDatabases job is disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) WITH NOWAIT;
				            RETURN;
                        END        
	
					IF @Debug = 1 RAISERROR('Waiting for 1 minute', 0, 1) WITH NOWAIT;
					
					WAITFOR DELAY '00:01:00.000';

				END TRY

				BEGIN CATCH


						SELECT @msg = N'Error inserting databases to msdb.dbo.restore_worker, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;

	
						WHILE @@TRANCOUNT > 0
							ROLLBACK;


				END CATCH;
	
			
			END; 
		
		END;-- End Pollster loop
	
		ELSE
	
			BEGIN
	
				RAISERROR('msdb.dbo.restore_worker does not exist, please create it.', 0, 1) WITH NOWAIT;
				RETURN;
			
			END; 
	RETURN;



/*

Begin LogShamer

*/

LogShamer:

	IF @Debug = 1 RAISERROR('Beginning Backups', 0, 1) WITH NOWAIT;
	
	IF OBJECT_ID('msdbCentral.dbo.backup_worker') IS NOT NULL
	
		BEGIN
		
			/*
			
			Make sure configuration table exists...
			
			*/
	
			IF OBJECT_ID('msdbCentral.dbo.backup_configuration') IS NOT NULL
	
				BEGIN
	
					IF @Debug = 1 RAISERROR('Checking variables', 0, 1) WITH NOWAIT;
		
			/*
			
			These settings are configurable
	
			I haven't found a good way to find the default backup path that doesn't involve xp_regread
			
			*/
	
						SELECT @rpo  = CONVERT(INT, configuration_setting)
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'log backup frequency'
                          AND database_name = N'all';
	
							
							IF @rpo IS NULL
								BEGIN
									RAISERROR('@rpo cannot be NULL. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) WITH NOWAIT;
									RETURN;
								END;	
	
	
						SELECT @backup_path = CONVERT(NVARCHAR(512), configuration_setting)
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'log backup path'
                          AND database_name = N'all';
	
							
							IF @backup_path IS NULL
								BEGIN
									RAISERROR('@backup_path cannot be NULL. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) WITH NOWAIT;
									RETURN;
								END;	

						SELECT @changebackuptype = configuration_setting
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'change backup type'
                          AND database_name = N'all';

						SELECT @encrypt = configuration_setting
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'encrypt'
                          AND database_name = N'all';

						SELECT @encryptionalgorithm = configuration_setting
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'encryptionalgorithm'
                          AND database_name = N'all';

						SELECT @servercertificate = configuration_setting
						FROM msdbCentral.dbo.backup_configuration c
						WHERE configuration_name = N'servercertificate'
                          AND database_name = N'all';

							IF @encrypt = N'Y' AND (@encryptionalgorithm IS NULL OR @servercertificate IS NULL)
								BEGIN
									RAISERROR('If encryption is Y, then both the encryptionalgorithm and servercertificate must be set. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) WITH NOWAIT;
									RETURN;
								END;	
	
				END;
	
			ELSE
	
				BEGIN
	
					RAISERROR('msdbCentral.dbo.backup_configuration does not exist, please run setup script', 0, 1) WITH NOWAIT;
					RETURN;
				
				END;
	
	
			WHILE @Backup = 1

			/*
			
			Start loop to take log backups

			*/

			
				BEGIN
	
					BEGIN TRY
							
							BEGIN TRAN;
	
								IF @Debug = 1 RAISERROR('Begin tran to grab a database to back up', 0, 1) WITH NOWAIT;


								/*
								
								This grabs a database for a worker to work on

								The locking hints hope to provide some isolation when 10+ workers are in action
								
								*/
	
							
										SELECT TOP (1) 
												@database = bw.database_name
										FROM msdbCentral.dbo.backup_worker bw WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
										WHERE 
											  (		/*This section works on databases already part of the backup cycle*/
												    bw.is_started = 0
												AND bw.is_completed = 1
												AND bw.last_log_backup_start_time < DATEADD(SECOND, (@rpo * -1), GETDATE()) 
                                                AND (bw.error_number IS NULL OR bw.error_number > 0) /* negative numbers indicate human attention required */
												AND bw.ignore_database = 0
											  )
										OR    
											  (		/*This section picks up newly added databases by Pollster*/
											  	    bw.is_started = 0
											  	AND bw.is_completed = 0
											  	AND bw.last_log_backup_start_time = '1900-01-01 00:00:00.000'
											  	AND bw.last_log_backup_finish_time = '9999-12-31 00:00:00.000'
                                                AND (bw.error_number IS NULL OR bw.error_number > 0) /* negative numbers indicate human attention required */
												AND bw.ignore_database = 0
											  )
										ORDER BY bw.last_log_backup_start_time ASC, bw.last_log_backup_finish_time ASC, bw.database_name ASC;
	
								
									IF @database IS NOT NULL
										BEGIN
										SET @msg = N'Updating backup_worker for database ' + ISNULL(@database, 'UH OH NULL @database');
										IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;
								
										/*
								
										Update the worker table so other workers know a database is being backed up
								
										*/

								
										UPDATE bw
												SET bw.is_started = 1,
													bw.is_completed = 0,
													bw.last_log_backup_start_time = GETDATE()
										FROM msdbCentral.dbo.backup_worker bw 
										WHERE bw.database_name = @database;
										END
	
							COMMIT;
	
					END TRY
	
					BEGIN CATCH
						
						/*
						
						Do I need to build retry logic in here? Try to catch deadlocks? I don't know yet!
						
						*/

						SELECT @msg = N'Error securing a database to backup, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;

						SET @database = NULL;
	
						WHILE @@TRANCOUNT > 0
							ROLLBACK;
	
					END CATCH;


					/* If we don't find a database to work on, wait for a few seconds */
					IF @database IS NULL

						BEGIN
							IF @Debug = 1 RAISERROR('No databases to back up right now, starting 3 second throttle', 0, 1) WITH NOWAIT;
							WAITFOR DELAY '00:00:03.000';

                            /* Check to make sure job is still enabled */
		                    IF NOT EXISTS (
						                    SELECT *
						                    FROM msdb.dbo.sysjobs 
						                    WHERE name LIKE 'sp_AllNightLog_Backup%'
                                            AND enabled = 1
						                    )
                                BEGIN
				                    RAISERROR('sp_AllNightLog_Backup jobs are disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) WITH NOWAIT;
				                    RETURN;
                                END        


						END
	
	
					BEGIN TRY
						
						BEGIN
	
							IF @database IS NOT NULL

							/*
							
							Make sure we have a database to work on -- I should make this more robust so we do something if it is NULL, maybe
							
							*/

								
								BEGIN
	
									SET @msg = N'Taking backup of ' + ISNULL(@database, 'UH OH NULL @database');
									IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;

										/*
										
										Call Ola's proc to backup the database
										
										*/

	                                    IF @encrypt = 'Y'
										    EXEC dbo.DatabaseBackup @Databases = @database, --Database we're working on
																	       @BackupType = 'LOG', --Going for the LOGs
																	       @Directory = @backup_path, --The path we need to back up to
																	       @Verify = 'N', --We don't want to verify these, it eats into job time
																	       @ChangeBackupType = @changebackuptype, --If we need to switch to a FULL because one hasn't been taken
																	       @CheckSum = 'Y', --These are a good idea
																	       @Compress = 'Y', --This is usually a good idea
																	       @LogToTable = 'Y', --We should do this for posterity
                                                                           @Encrypt = @encrypt,
                                                                           @EncryptionAlgorithm = @encryptionalgorithm,
                                                                           @ServerCertificate = @servercertificate;

                                        ELSE
									        EXEC dbo.DatabaseBackup @Databases = @database, --Database we're working on
																	        @BackupType = 'LOG', --Going for the LOGs
																	        @Directory = @backup_path, --The path we need to back up to
																	        @Verify = 'N', --We don't want to verify these, it eats into job time
																	        @ChangeBackupType = @changebackuptype, --If we need to switch to a FULL because one hasn't been taken
																	        @CheckSum = 'Y', --These are a good idea
																	        @Compress = 'Y', --This is usually a good idea
																	        @LogToTable = 'Y'; --We should do this for posterity
	
										
										/*
										
										Catch any erroneous zones
										
										*/
										
										SELECT @error_number = ERROR_NUMBER(), 
											   @error_severity = ERROR_SEVERITY(), 
											   @error_state = ERROR_STATE();
	
								END; --End call to dbo.DatabaseBackup
	
						END; --End successful check of @database (not NULL)
					
					END TRY
	
					BEGIN CATCH
	
						IF  @error_number IS NOT NULL

						/*
						
						If the ERROR() function returns a number, update the table with it and the last error date.

						Also update the last start time to 1900-01-01 so it gets picked back up immediately -- the query to find a log backup to take sorts by start time

						*/
	
							BEGIN
	
								SET @msg = N'Error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()); 
								RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;
								
								SET @msg = N'Updating backup_worker for database ' + ISNULL(@database, 'UH OH NULL @database') + ' for unsuccessful backup';
								RAISERROR(@msg, 0, 1) WITH NOWAIT;
	
								
									UPDATE bw
											SET bw.is_started = 0,
												bw.is_completed = 1,
												bw.last_log_backup_start_time = '19000101',
												bw.error_number = @error_number,
												bw.last_error_date = GETDATE()
									FROM msdbCentral.dbo.backup_worker bw 
									WHERE bw.database_name = @database;


								/*
								
								Set @database back to NULL to avoid variable assignment weirdness
								
								*/

								SET @database = NULL;

										
										/*
										
										Wait around for a second so we're not just spinning wheels -- this only runs if the BEGIN CATCH is triggered by an error

										*/
										
										IF @Debug = 1 RAISERROR('Starting 1 second throttle', 0, 1) WITH NOWAIT;
										
										WAITFOR DELAY '00:00:01.000';

							END; -- End update of unsuccessful backup
	
					END CATCH;
	
					IF  @database IS NOT NULL AND @error_number IS NULL

					/*
						
					If no error, update everything normally
						
					*/

							
						BEGIN
	
							IF @Debug = 1 RAISERROR('Error number IS NULL', 0, 1) WITH NOWAIT;
								
							SET @msg = N'Updating backup_worker for database ' + ISNULL(@database, 'UH OH NULL @database') + ' for successful backup';
							IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;
	
								
								UPDATE bw
										SET bw.is_started = 0,
											bw.is_completed = 1,
											bw.last_log_backup_finish_time = GETDATE()
								FROM msdbCentral.dbo.backup_worker bw 
								WHERE bw.database_name = @database;

								
							/*
								
							Set @database back to NULL to avoid variable assignment weirdness
								
							*/

							SET @database = NULL;


						END; -- End update for successful backup	

										
				END; -- End @Backup WHILE loop

				
		END; -- End successful check for backup_worker and subsequent code

	
	ELSE
	
		BEGIN
	
			RAISERROR('msdbCentral.dbo.backup_worker does not exist, please run setup script', 0, 1) WITH NOWAIT;
			
			RETURN;
		
		END;
RETURN;


/*

Begin Restoregasm_Addict section

*/

Restoregasm_Addict:

IF @Restore = 1
	IF @Debug = 1 RAISERROR('Beginning Restores', 0, 1) WITH NOWAIT;
	
    /* Check to make sure backup jobs aren't enabled */
	IF EXISTS (
					SELECT *
					FROM msdb.dbo.sysjobs 
					WHERE name LIKE 'sp_AllNightLog_Backup%'
                    AND enabled = 1
					)
        BEGIN
			RAISERROR('sp_AllNightLog_Backup jobs are enabled, so gracefully exiting. You do not want to accidentally do restores over top of the databases you are backing up.', 0, 1) WITH NOWAIT;
			RETURN;
        END        

	IF OBJECT_ID('msdb.dbo.restore_worker') IS NOT NULL
	
		BEGIN
		
			/*
			
			Make sure configuration table exists...
			
			*/
	
			IF OBJECT_ID('msdb.dbo.restore_configuration') IS NOT NULL
	
				BEGIN
	
					IF @Debug = 1 RAISERROR('Checking variables', 0, 1) WITH NOWAIT;
		
			/*
			
			These settings are configurable
			
			*/
	
						SELECT @rto  = CONVERT(INT, configuration_setting)
						FROM msdb.dbo.restore_configuration c
						WHERE configuration_name = N'log restore frequency';
	
							
							IF @rto IS NULL
								BEGIN
									RAISERROR('@rto cannot be NULL. Please check the msdb.dbo.restore_configuration table', 0, 1) WITH NOWAIT;
									RETURN;
								END;	
	
		
				END;
	
			ELSE
	
				BEGIN
	
					RAISERROR('msdb.dbo.restore_configuration does not exist, please run setup script', 0, 1) WITH NOWAIT;
					
					RETURN;
				
				END;
	
	
			WHILE @Restore = 1

			/*
			
			Start loop to restore log backups

			*/

			
				BEGIN
	
					BEGIN TRY
							
							BEGIN TRAN;
	
								IF @Debug = 1 RAISERROR('Begin tran to grab a database to restore', 0, 1) WITH NOWAIT;


								/*
								
								This grabs a database for a worker to work on

								The locking hints hope to provide some isolation when 10+ workers are in action
								
								*/
	
							
										SELECT TOP (1) 
												@database = rw.database_name,
												@only_logs_after = REPLACE(REPLACE(REPLACE(CONVERT(NVARCHAR(30), rw.last_log_restore_start_time, 120), ' ', ''), '-', ''), ':', ''),
												@restore_full = CASE WHEN	  rw.is_started = 0
																		  AND rw.is_completed = 0
																		  AND rw.last_log_restore_start_time = '1900-01-01 00:00:00.000'
																		  AND rw.last_log_restore_finish_time = '9999-12-31 00:00:00.000'
																	THEN 1
																	ELSE 0
																END
										FROM msdb.dbo.restore_worker rw WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
										WHERE 
											  (		/*This section works on databases already part of the backup cycle*/
												    rw.is_started = 0
												AND rw.is_completed = 1
												AND rw.last_log_restore_start_time < DATEADD(SECOND, (@rto * -1), GETDATE()) 
                                                AND (rw.error_number IS NULL OR rw.error_number > 0) /* negative numbers indicate human attention required */
											  )
										OR    
											  (		/*This section picks up newly added databases by DiskPollster*/
											  	    rw.is_started = 0
											  	AND rw.is_completed = 0
											  	AND rw.last_log_restore_start_time = '1900-01-01 00:00:00.000'
											  	AND rw.last_log_restore_finish_time = '9999-12-31 00:00:00.000'
                                                AND (rw.error_number IS NULL OR rw.error_number > 0) /* negative numbers indicate human attention required */
											  )
										AND rw.ignore_database = 0
										ORDER BY rw.last_log_restore_start_time ASC, rw.last_log_restore_finish_time ASC, rw.database_name ASC;
	
								
									IF @database IS NOT NULL
										BEGIN
										SET @msg = N'Updating restore_worker for database ' + ISNULL(@database, 'UH OH NULL @database');
										IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;
								
										/*
								
										Update the worker table so other workers know a database is being restored
								
										*/

								
										UPDATE rw
												SET rw.is_started = 1,
													rw.is_completed = 0,
													rw.last_log_restore_start_time = GETDATE()
										FROM msdb.dbo.restore_worker rw 
										WHERE rw.database_name = @database;
										END
	
							COMMIT;
	
					END TRY
	
					BEGIN CATCH
						
						/*
						
						Do I need to build retry logic in here? Try to catch deadlocks? I don't know yet!
						
						*/

						SELECT @msg = N'Error securing a database to restore, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;

						SET @database = NULL;
	
						WHILE @@TRANCOUNT > 0
							ROLLBACK;
	
					END CATCH;


					/* If we don't find a database to work on, wait for a few seconds */
					IF @database IS NULL

						BEGIN
							IF @Debug = 1 RAISERROR('No databases to restore up right now, starting 3 second throttle', 0, 1) WITH NOWAIT;
							WAITFOR DELAY '00:00:03.000';

                            /* Check to make sure backup jobs aren't enabled */
	                        IF EXISTS (
					                        SELECT *
					                        FROM msdb.dbo.sysjobs 
					                        WHERE name LIKE 'sp_AllNightLog_Backup%'
                                            AND enabled = 1
					                        )
                                BEGIN
			                        RAISERROR('sp_AllNightLog_Backup jobs are enabled, so gracefully exiting. You do not want to accidentally do restores over top of the databases you are backing up.', 0, 1) WITH NOWAIT;
			                        RETURN;
                                END        

                            /* Check to make sure job is still enabled */
		                    IF NOT EXISTS (
						                    SELECT *
						                    FROM msdb.dbo.sysjobs 
						                    WHERE name LIKE 'sp_AllNightLog_Restore%'
                                            AND enabled = 1
						                    )
                                BEGIN
				                    RAISERROR('sp_AllNightLog_Restore jobs are disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) WITH NOWAIT;
				                    RETURN;
                                END        

						END
	
	
					BEGIN TRY
						
						BEGIN
	
							IF @database IS NOT NULL

							/*
							
							Make sure we have a database to work on -- I should make this more robust so we do something if it is NULL, maybe
							
							*/

								
								BEGIN
	
									SET @msg = CASE WHEN @restore_full = 0 
														 THEN N'Restoring logs for ' 
														 ELSE N'Restoring full backup for ' 
													END 
													+ ISNULL(@database, 'UH OH NULL @database');

									IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;

										/*
										
										Call sp_DatabaseRestore to backup the database
										
										*/

										SET @restore_path_full = @restore_path_base + N'\' + @database + N'\' + N'FULL\'
										
											SET @msg = N'Path for FULL backups for ' + @database + N' is ' + @restore_path_full
											IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;

										SET @restore_path_log = @restore_path_base + N'\' + @database + N'\' + N'LOG\'

											SET @msg = N'Path for LOG backups for ' + @database + N' is ' + @restore_path_log
											IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;

										IF @restore_full = 0

											BEGIN

												IF @Debug = 1 RAISERROR('Starting Log only restores', 0, 1) WITH NOWAIT;

												EXEC dbo.sp_DatabaseRestore @Database = @database, 
																				   @BackupPathFull = @restore_path_full,
																				   @BackupPathLog = @restore_path_log,
																				   @ContinueLogs = 1,
																				   @RunRecovery = 0,
																				   @OnlyLogsAfter = @only_logs_after,
																				   @MoveFiles = @restore_move_files,
																				   @Debug = @Debug
	
											END

										IF @restore_full = 1

											BEGIN

												IF @Debug = 1 RAISERROR('Starting first Full restore from: ', 0, 1) WITH NOWAIT;
												IF @Debug = 1 RAISERROR(@restore_path_full, 0, 1) WITH NOWAIT;

												EXEC dbo.sp_DatabaseRestore @Database = @database, 
																				   @BackupPathFull = @restore_path_full,
																				   @BackupPathLog = @restore_path_log,
																				   @ContinueLogs = 0,
																				   @RunRecovery = 0,
																				   @MoveFiles = @restore_move_files,
																				   @Debug = @Debug
	
											END


										
										/*
										
										Catch any erroneous zones
										
										*/
										
										SELECT @error_number = ERROR_NUMBER(), 
											   @error_severity = ERROR_SEVERITY(), 
											   @error_state = ERROR_STATE();
	
								END; --End call to dbo.sp_DatabaseRestore
	
						END; --End successful check of @database (not NULL)
					
					END TRY
	
					BEGIN CATCH
	
						IF  @error_number IS NOT NULL

						/*
						
						If the ERROR() function returns a number, update the table with it and the last error date.

						Also update the last start time to 1900-01-01 so it gets picked back up immediately -- the query to find a log restore to take sorts by start time

						*/
	
							BEGIN
	
								SET @msg = N'Error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()); 
								RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;
								
								SET @msg = N'Updating restore_worker for database ' + ISNULL(@database, 'UH OH NULL @database') + ' for unsuccessful backup';
								RAISERROR(@msg, 0, 1) WITH NOWAIT;
	
								
									UPDATE rw
											SET rw.is_started = 0,
												rw.is_completed = 1,
												rw.last_log_restore_start_time = '19000101',
												rw.error_number = @error_number,
												rw.last_error_date = GETDATE()
									FROM msdb.dbo.restore_worker rw 
									WHERE rw.database_name = @database;


								/*
								
								Set @database back to NULL to avoid variable assignment weirdness
								
								*/

								SET @database = NULL;

										
										/*
										
										Wait around for a second so we're not just spinning wheels -- this only runs if the BEGIN CATCH is triggered by an error

										*/
										
										IF @Debug = 1 RAISERROR('Starting 1 second throttle', 0, 1) WITH NOWAIT;
										
										WAITFOR DELAY '00:00:01.000';

							END; -- End update of unsuccessful restore
	
					END CATCH;


					IF  @database IS NOT NULL AND @error_number IS NULL

					/*
						
					If no error, update everything normally
						
					*/

							
						BEGIN
	
							IF @Debug = 1 RAISERROR('Error number IS NULL', 0, 1) WITH NOWAIT;

                            /* Make sure database actually exists and is in the restoring state */
                            IF EXISTS (SELECT * FROM sys.databases WHERE name = @database AND state = 1) /* Restoring */
								BEGIN
							        SET @msg = N'Updating backup_worker for database ' + ISNULL(@database, 'UH OH NULL @database') + ' for successful backup';
							        IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;
								
								    UPDATE rw
										    SET rw.is_started = 0,
											    rw.is_completed = 1,
											    rw.last_log_restore_finish_time = GETDATE()
								    FROM msdb.dbo.restore_worker rw 
								    WHERE rw.database_name = @database;

                                END
                            ELSE /* The database doesn't exist, or it's not in the restoring state */
                                BEGIN
							        SET @msg = N'Updating backup_worker for database ' + ISNULL(@database, 'UH OH NULL @database') + ' for UNsuccessful backup';
							        IF @Debug = 1 RAISERROR(@msg, 0, 1) WITH NOWAIT;
								
								    UPDATE rw
										    SET rw.is_started = 0,
											    rw.is_completed = 1,
                                                rw.error_number = -1, /* unknown, human attention required */
                                                rw.last_error_date = GETDATE()
											    /* rw.last_log_restore_finish_time = GETDATE()    don't change this - the last log may still be successful */
								    FROM msdb.dbo.restore_worker rw 
								    WHERE rw.database_name = @database;
                                END


								
							/*
								
							Set @database back to NULL to avoid variable assignment weirdness
								
							*/

							SET @database = NULL;


						END; -- End update for successful backup	
										
				END; -- End @Restore WHILE loop

				
		END; -- End successful check for restore_worker and subsequent code

	
	ELSE
	
		BEGIN
	
			RAISERROR('msdb.dbo.restore_worker does not exist, please run setup script', 0, 1) WITH NOWAIT;
			
			RETURN;
		
		END;
RETURN;



END; -- Final END for stored proc

GO 
