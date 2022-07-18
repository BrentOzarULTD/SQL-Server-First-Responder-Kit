SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('dbo.sp_AllNightLog_Setup') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_AllNightLog_Setup AS RETURN 0;');
GO


ALTER PROCEDURE dbo.sp_AllNightLog_Setup
				@RPOSeconds BIGINT = 30,
				@RTOSeconds BIGINT = 30,
				@BackupPath NVARCHAR(MAX) = NULL,
				@RestorePath NVARCHAR(MAX) = NULL,
				@Jobs TINYINT = 10,
				@RunSetup BIT = 0,
				@UpdateSetup BIT = 0,
                @EnableBackupJobs INT = NULL,
                @EnableRestoreJobs INT = NULL,
				@Debug BIT = 0,
				@FirstFullBackup BIT = 0,
				@FirstDiffBackup BIT = 0,
				@MoveFiles BIT = 1,
				@Help BIT = 0,
				@Version     VARCHAR(30) = NULL OUTPUT,
				@VersionDate DATETIME = NULL OUTPUT,
				@VersionCheckMode BIT = 0
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


		sp_AllNightLog_Setup from http://FirstResponderKit.org
		
		This script sets up a database, tables, rows, and jobs for sp_AllNightLog, including:

		* Creates a database
			* Right now it''s hard-coded to use msdbCentral, that might change later
	
		* Creates tables in that database!
			* dbo.backup_configuration
				* Hold variables used by stored proc to make runtime decisions
					* RPO: Seconds, how often we look for databases that need log backups
					* Backup Path: The path we feed to Ola H''s backup proc
			* dbo.backup_worker
				* Holds list of databases and some information that helps our Agent jobs figure out if they need to take another log backup
		
		* Creates tables in msdb
			* dbo.restore_configuration
				* Holds variables used by stored proc to make runtime decisions
					* RTO: Seconds, how often to look for log backups to restore
					* Restore Path: The path we feed to sp_DatabaseRestore 
					* Move Files: Whether to move files to default data/log directories.
			* dbo.restore_worker
				* Holds list of databases and some information that helps our Agent jobs figure out if they need to look for files to restore
	
		 * Creates agent jobs
			* 1 job that polls sys.databases for new entries
			* 10 jobs that run to take log backups
			 * Based on a queue table
			 * Requires Ola Hallengren''s Database Backup stored proc
	
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
	
		Known limitations of this version:
		 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000! And really, maybe not even anything less than 2016. Heh.
		 - The repository database name is hard-coded to msdbCentral.
	
		Unknown limitations of this version:
		 - None.  (If we knew them, they would be known. Duh.)
	
	     Changes - for the full list of improvements and fixes in this version, see:
	     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	
		Parameter explanations:
	
		  @RunSetup	BIT, defaults to 0. When this is set to 1, it will run the setup portion to create database, tables, and worker jobs.
		  @UpdateSetup BIT, defaults to 0. When set to 1, will update existing configs for RPO/RTO and database backup/restore paths.
		  @RPOSeconds BIGINT, defaults to 30. Value in seconds you want to use to determine if a new log backup needs to be taken.
		  @BackupPath NVARCHAR(MAX), This is REQUIRED if @Runsetup=1. This tells Ola''s job where to put backups.
		  @MoveFiles BIT, defaults to 1. When this is set to 1, it will move files to default data/log directories
		  @Debug BIT, defaults to 0. Whent this is set to 1, it prints out dynamic SQL commands
	
	    Sample call:
		EXEC dbo.sp_AllNightLog_Setup
			@RunSetup = 1,
			@RPOSeconds = 30,
			@BackupPath = N''M:\MSSQL\Backup'',
			@Debug = 1


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

RETURN;
END; /* IF @Help = 1 */

DECLARE	@database NVARCHAR(128) = NULL; --Holds the database that's currently being processed
DECLARE @error_number INT = NULL; --Used for TRY/CATCH
DECLARE @error_severity INT; --Used for TRY/CATCH
DECLARE @error_state INT; --Used for TRY/CATCH
DECLARE @msg NVARCHAR(4000) = N''; --Used for RAISERROR
DECLARE @rpo INT; --Used to hold the RPO value in our configuration table
DECLARE @backup_path NVARCHAR(MAX); --Used to hold the backup path in our configuration table
DECLARE @db_sql NVARCHAR(MAX) = N''; --Used to hold the dynamic SQL to create msdbCentral
DECLARE @tbl_sql NVARCHAR(MAX) = N''; --Used to hold the dynamic SQL that creates tables in msdbCentral
DECLARE @database_name NVARCHAR(256) = N'msdbCentral'; --Used to hold the name of the database we create to centralize data
													   --Right now it's hardcoded to msdbCentral, but I made it dynamic in case that changes down the line


/*These variables control the loop to create/modify jobs*/
DECLARE @job_sql NVARCHAR(MAX) = N''; --Used to hold the dynamic SQL that creates Agent jobs
DECLARE @counter INT = 0; --For looping to create 10 Agent jobs
DECLARE @job_category NVARCHAR(MAX) = N'''Database Maintenance'''; --Job category
DECLARE @job_owner NVARCHAR(128) = QUOTENAME(SUSER_SNAME(0x01), ''''); -- Admin user/owner
DECLARE @jobs_to_change TABLE(name SYSNAME); -- list of jobs we need to enable or disable
DECLARE @current_job_name SYSNAME; -- While looping through Agent jobs to enable or disable
DECLARE @active_start_date INT = (CONVERT(INT, CONVERT(VARCHAR(10), GETDATE(), 112)));
DECLARE @started_waiting_for_jobs DATETIME; --We need to wait for a while when disabling jobs

/*Specifically for Backups*/
DECLARE @job_name_backups NVARCHAR(MAX) = N'''sp_AllNightLog_Backup_Job_'''; --Name of log backup job
DECLARE @job_description_backups NVARCHAR(MAX) = N'''This is a worker for the purposes of taking log backups from msdbCentral.dbo.backup_worker queue table.'''; --Job description
DECLARE @job_command_backups NVARCHAR(MAX) = N'''EXEC sp_AllNightLog @Backup = 1'''; --Command the Agent job will run

/*Specifically for Restores*/
DECLARE @job_name_restores NVARCHAR(MAX) = N'''sp_AllNightLog_Restore_Job_'''; --Name of log backup job
DECLARE @job_description_restores NVARCHAR(MAX) = N'''This is a worker for the purposes of restoring log backups from msdb.dbo.restore_worker queue table.'''; --Job description
DECLARE @job_command_restores NVARCHAR(MAX) = N'''EXEC sp_AllNightLog @Restore = 1'''; --Command the Agent job will run


/*

Sanity check some variables

*/



IF ((@RunSetup = 0 OR @RunSetup IS NULL) AND (@UpdateSetup = 0 OR @UpdateSetup IS NULL))

	BEGIN

		RAISERROR('You have to either run setup or update setup. You can''t not do neither nor, if you follow. Or not.', 0, 1) WITH NOWAIT;

		RETURN;

	END;


/*

Should be a positive number

*/

IF (@RPOSeconds < 0)

		BEGIN
			RAISERROR('Please choose a positive number for @RPOSeconds', 0, 1) WITH NOWAIT;

			RETURN;
		END;


/*

Probably shouldn't be more than 20

*/

IF (@Jobs > 20) OR (@Jobs < 1)

		BEGIN
			RAISERROR('We advise sticking with 1-20 jobs.', 0, 1) WITH NOWAIT;

			RETURN;
		END;

/*

Probably shouldn't be more than 4 hours

*/

IF (@RPOSeconds >= 14400)
		BEGIN

			RAISERROR('If your RPO is really 4 hours, perhaps you''d be interested in a more modest recovery model, like SIMPLE?', 0, 1) WITH NOWAIT;

			RETURN;
		END;


/*

Can't enable both the backup and restore jobs at the same time

*/

IF @EnableBackupJobs = 1 AND @EnableRestoreJobs = 1
		BEGIN

			RAISERROR('You are not allowed to enable both the backup and restore jobs at the same time. Pick one, bucko.', 0, 1) WITH NOWAIT;

			RETURN;
		END;

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

/*

Basic path sanity checks

*/

IF @RunSetup = 1 and @BackupPath is NULL
		BEGIN
	
				RAISERROR('@BackupPath is required during setup', 0, 1) WITH NOWAIT;
				
				RETURN;
		END

IF  (@BackupPath NOT LIKE '[c-zC-Z]:\%') --Local path, don't think anyone has A or B drives
AND (@BackupPath NOT LIKE '\\[a-zA-Z0-9]%\%') --UNC path
	
		BEGIN 		
				RAISERROR('Are you sure that''s a real path?', 0, 1) WITH NOWAIT;
				
				RETURN;
		END; 

/*

If you want to update the table, one of these has to not be NULL

*/

IF @UpdateSetup = 1
	AND (	 @RPOSeconds IS NULL 
		 AND @BackupPath IS NULL 
		 AND @RPOSeconds IS NULL 
		 AND @RestorePath IS NULL
         AND @EnableBackupJobs IS NULL
         AND @EnableRestoreJobs IS NULL
		)

		BEGIN

			RAISERROR('If you want to update configuration settings, they can''t be NULL. Please Make sure @RPOSeconds / @RTOSeconds or @BackupPath / @RestorePath has a value', 0, 1) WITH NOWAIT;

			RETURN;

		END;


IF @UpdateSetup = 1
	GOTO UpdateConfigs;

IF @RunSetup = 1
BEGIN
		BEGIN TRY

			BEGIN 
			

				/*
				
				First check to see if Agent is running -- we'll get errors if it's not
				
				*/
				
				
				IF ( SELECT 1
						FROM sys.all_objects
						WHERE name = 'dm_server_services' ) IS NOT NULL 

				BEGIN

				IF EXISTS (
							SELECT 1
							FROM sys.dm_server_services
							WHERE servicename LIKE 'SQL Server Agent%'
							AND status_desc = 'Stopped'		
						  )
					
					BEGIN
		
						RAISERROR('SQL Server Agent is not currently running -- it needs to be enabled to add backup worker jobs and the new database polling job', 0, 1) WITH NOWAIT;
						
						RETURN;
		
					END;
				
				END 
				

				BEGIN


						/*
						
						Check to see if the database exists

						*/
 
						RAISERROR('Checking for msdbCentral', 0, 1) WITH NOWAIT;

						SET @db_sql += N'

							IF DATABASEPROPERTYEX(' + QUOTENAME(@database_name, '''') + ', ''Status'') IS NULL

								BEGIN

									RAISERROR(''Creating msdbCentral'', 0, 1) WITH NOWAIT;

									CREATE DATABASE ' + QUOTENAME(@database_name) + ';
									
									ALTER DATABASE ' + QUOTENAME(@database_name) + ' SET RECOVERY FULL;
								
								END

							';


							IF @Debug = 1
								BEGIN 
									RAISERROR(@db_sql, 0, 1) WITH NOWAIT;
								END; 


							IF @db_sql IS NULL
								BEGIN
									RAISERROR('@db_sql is NULL for some reason', 0, 1) WITH NOWAIT;
								END; 


							EXEC sp_executesql @db_sql; 


						/*
						
						Check for tables and stuff

						*/

						
						RAISERROR('Checking for tables in msdbCentral', 0, 1) WITH NOWAIT;

							SET @tbl_sql += N'
							
									USE ' + QUOTENAME(@database_name) + '
									
									
									IF OBJECT_ID(''' + QUOTENAME(@database_name) + '.dbo.backup_configuration'') IS NULL
									
										BEGIN
										
										RAISERROR(''Creating table dbo.backup_configuration'', 0, 1) WITH NOWAIT;
											
											CREATE TABLE dbo.backup_configuration (
																			database_name NVARCHAR(256), 
																			configuration_name NVARCHAR(512), 
																			configuration_description NVARCHAR(512), 
																			configuration_setting NVARCHAR(MAX)
																			);
											
										END
										
									ELSE 
										
										BEGIN
											
											
											RAISERROR(''Backup configuration table exists, truncating'', 0, 1) WITH NOWAIT;
										
											
											TRUNCATE TABLE dbo.backup_configuration

										
										END


											RAISERROR(''Inserting configuration values'', 0, 1) WITH NOWAIT;

											
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''log backup frequency'', ''The length of time in second between Log Backups.'', ''' + CONVERT(NVARCHAR(10), @RPOSeconds) + ''');
											
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''log backup path'', ''The path to which Log Backups should go.'', ''' + @BackupPath + ''');									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''change backup type'', ''For Ola Hallengren DatabaseBackup @ChangeBackupType param: Y = escalate to fulls, MSDB = escalate by checking msdb backup history.'', ''MSDB'');									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''encrypt'', ''For Ola Hallengren DatabaseBackup: Y = encrypt the backup. N (default) = do not encrypt.'', NULL);									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''encryptionalgorithm'', ''For Ola Hallengren DatabaseBackup: native 2014 choices include TRIPLE_DES_3KEY, AES_128, AES_192, AES_256.'', NULL);									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''servercertificate'', ''For Ola Hallengren DatabaseBackup: server certificate that is used to encrypt the backup.'', NULL);									
									
																		
									IF OBJECT_ID(''' + QUOTENAME(@database_name) + '.dbo.backup_worker'') IS NULL
										
										BEGIN
										
										
											RAISERROR(''Creating table dbo.backup_worker'', 0, 1) WITH NOWAIT;
											
												CREATE TABLE dbo.backup_worker (
																				id INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED, 
																				database_name NVARCHAR(256), 
																				last_log_backup_start_time DATETIME DEFAULT ''19000101'', 
																				last_log_backup_finish_time DATETIME DEFAULT ''99991231'', 
																				is_started BIT DEFAULT 0, 
																				is_completed BIT DEFAULT 0, 
																				error_number INT DEFAULT NULL, 
																				last_error_date DATETIME DEFAULT NULL,
																				ignore_database BIT DEFAULT 0,
																				full_backup_required BIT DEFAULT ' + CASE WHEN @FirstFullBackup = 0 THEN N'0,' ELSE N'1,' END + CHAR(10) +
																			  N'diff_backup_required BIT DEFAULT ' + CASE WHEN @FirstDiffBackup = 0 THEN N'0' ELSE N'1' END + CHAR(10) +
																			  N');
											
										END;
									
									ELSE

										BEGIN


											RAISERROR(''Backup worker table exists, truncating'', 0, 1) WITH NOWAIT;
										
											
											TRUNCATE TABLE dbo.backup_worker


										END

											
											RAISERROR(''Inserting databases for backups'', 0, 1) WITH NOWAIT;
									
											INSERT ' + QUOTENAME(@database_name) + '.dbo.backup_worker (database_name) 
											SELECT d.name
											FROM sys.databases d
											WHERE NOT EXISTS (
												SELECT * 
												FROM msdbCentral.dbo.backup_worker bw
												WHERE bw.database_name = d.name
															)
											AND d.database_id > 4;
									
									';

							
							IF @Debug = 1
								BEGIN 
									SET @msg = SUBSTRING(@tbl_sql, 0, 2044)
									RAISERROR(@msg, 0, 1) WITH NOWAIT;
									SET @msg = SUBSTRING(@tbl_sql, 2044, 4088)
									RAISERROR(@msg, 0, 1) WITH NOWAIT;
									SET @msg = SUBSTRING(@tbl_sql, 4088, 6132)
									RAISERROR(@msg, 0, 1) WITH NOWAIT;
									SET @msg = SUBSTRING(@tbl_sql, 6132, 8176)
									RAISERROR(@msg, 0, 1) WITH NOWAIT;
								END; 

							
							IF @tbl_sql IS NULL
								BEGIN
									RAISERROR('@tbl_sql is NULL for some reason', 0, 1) WITH NOWAIT;
								END; 


							EXEC sp_executesql @tbl_sql;

						
						/*
						
						This section creates tables for restore workers to work off of
						
						*/

						
						/* 
						
						In search of msdb 
						
						*/
						
						RAISERROR('Checking for msdb. Yeah, I know...', 0, 1) WITH NOWAIT;
						
						IF DATABASEPROPERTYEX('msdb', 'Status') IS NULL

							BEGIN

									RAISERROR('YOU HAVE NO MSDB WHY?!', 0, 1) WITH NOWAIT;

							RETURN;

							END;

						
						/* In search of restore_configuration */

						RAISERROR('Checking for Restore Worker tables in msdb', 0, 1) WITH NOWAIT;

						IF OBJECT_ID('msdb.dbo.restore_configuration') IS NULL

							BEGIN

								RAISERROR('Creating restore_configuration table in msdb', 0, 1) WITH NOWAIT;

								CREATE TABLE msdb.dbo.restore_configuration (
																			database_name NVARCHAR(256), 
																			configuration_name NVARCHAR(512), 
																			configuration_description NVARCHAR(512), 
																			configuration_setting NVARCHAR(MAX)
																			);

							END;


						ELSE


							BEGIN

								RAISERROR('Restore configuration table exists, truncating', 0, 1) WITH NOWAIT;

								TRUNCATE TABLE msdb.dbo.restore_configuration;
						
							END;


								RAISERROR('Inserting configuration values to msdb.dbo.restore_configuration', 0, 1) WITH NOWAIT;
								
								INSERT msdb.dbo.restore_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
												  VALUES ('all', 'log restore frequency', 'The length of time in second between Log Restores.', @RTOSeconds);
								
								INSERT msdb.dbo.restore_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
												  VALUES ('all', 'log restore path', 'The path to which Log Restores come from.', @RestorePath);	

								INSERT msdb.dbo.restore_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
												  VALUES ('all', 'move files', 'Determines if we move database files to default data/log directories.', @MoveFiles);	

						IF OBJECT_ID('msdb.dbo.restore_worker') IS NULL
							
							BEGIN
							
							
								RAISERROR('Creating table msdb.dbo.restore_worker', 0, 1) WITH NOWAIT;
								
								CREATE TABLE msdb.dbo.restore_worker (
																		 id INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED, 
																		 database_name NVARCHAR(256), 
																		 last_log_restore_start_time DATETIME DEFAULT '19000101', 
																		 last_log_restore_finish_time DATETIME DEFAULT '99991231', 
																		 is_started BIT DEFAULT 0, 
																		 is_completed BIT DEFAULT 0, 
																		 error_number INT DEFAULT NULL, 
																		 last_error_date DATETIME DEFAULT NULL,
																		 ignore_database BIT DEFAULT 0,
																		 full_backup_required BIT DEFAULT 0,
																	     diff_backup_required BIT DEFAULT 0
																	     );

								
								RAISERROR('Inserting databases for restores', 0, 1) WITH NOWAIT;
						
								INSERT msdb.dbo.restore_worker (database_name) 
								SELECT d.name
								FROM sys.databases d
								WHERE NOT EXISTS (
									SELECT * 
									FROM msdb.dbo.restore_worker bw
									WHERE bw.database_name = d.name
												)
								AND d.database_id > 4;
							
							
							END;


		
		/*
		
		Add Jobs
		
		*/
		

		
		/*
		
		Look for our ten second schedule -- all jobs use this to restart themselves if they fail

		Fun fact: you can add the same schedule name multiple times, so we don't want to just stick it in there
		
		*/


		RAISERROR('Checking for ten second schedule', 0, 1) WITH NOWAIT;

			IF NOT EXISTS (
							SELECT 1 
							FROM msdb.dbo.sysschedules 
							WHERE name = 'ten_seconds'
						  )
			
				BEGIN
					
					
					RAISERROR('Creating ten second schedule', 0, 1) WITH NOWAIT;

					
					EXEC msdb.dbo.sp_add_schedule    @schedule_name= ten_seconds, 
													 @enabled = 1, 
													 @freq_type = 4, 
													 @freq_interval = 1, 
													 @freq_subday_type = 2,  
													 @freq_subday_interval = 10, 
													 @freq_relative_interval = 0, 
													 @freq_recurrence_factor = 0, 
													 @active_start_date = @active_start_date, 
													 @active_end_date = 99991231, 
													 @active_start_time = 0, 
													 @active_end_time = 235959;
				
				END;
		
			
			/*
			
			Look for Backup Pollster job -- this job sets up our watcher for new databases to back up
			
			*/

			
			RAISERROR('Checking for pollster job', 0, 1) WITH NOWAIT;

			
			IF NOT EXISTS (
							SELECT 1 
							FROM msdb.dbo.sysjobs 
							WHERE name = 'sp_AllNightLog_PollForNewDatabases'
						  )
		
				
				BEGIN
					
					
					RAISERROR('Creating pollster job', 0, 1) WITH NOWAIT;

						IF @EnableBackupJobs = 1
                            BEGIN
						    EXEC msdb.dbo.sp_add_job @job_name = sp_AllNightLog_PollForNewDatabases, 
												     @description = 'This is a worker for the purposes of polling sys.databases for new entries to insert to the worker queue table.', 
												     @category_name = 'Database Maintenance', 
												     @owner_login_name = 'sa',
												     @enabled = 1;
                            END		
                        ELSE			
                            BEGIN
						    EXEC msdb.dbo.sp_add_job @job_name = sp_AllNightLog_PollForNewDatabases, 
												     @description = 'This is a worker for the purposes of polling sys.databases for new entries to insert to the worker queue table.', 
												     @category_name = 'Database Maintenance', 
												     @owner_login_name = 'sa',
												     @enabled = 0;
                            END		
					
					
					RAISERROR('Adding job step', 0, 1) WITH NOWAIT;

						
						EXEC msdb.dbo.sp_add_jobstep @job_name = sp_AllNightLog_PollForNewDatabases, 
													 @step_name = sp_AllNightLog_PollForNewDatabases, 
													 @subsystem = 'TSQL', 
													 @command = 'EXEC sp_AllNightLog @PollForNewDatabases = 1';
					
					
					
					RAISERROR('Adding job server', 0, 1) WITH NOWAIT;

						
						EXEC msdb.dbo.sp_add_jobserver @job_name = sp_AllNightLog_PollForNewDatabases;

					
									
					RAISERROR('Attaching schedule', 0, 1) WITH NOWAIT;
		
						
						EXEC msdb.dbo.sp_attach_schedule @job_name = sp_AllNightLog_PollForNewDatabases, 
														 @schedule_name = ten_seconds;
		
				
				END;	
				


			/*
			
			Look for Restore Pollster job -- this job sets up our watcher for new databases to back up
			
			*/

			
			RAISERROR('Checking for restore pollster job', 0, 1) WITH NOWAIT;

			
			IF NOT EXISTS (
							SELECT 1 
							FROM msdb.dbo.sysjobs 
							WHERE name = 'sp_AllNightLog_PollDiskForNewDatabases'
						  )
		
				
				BEGIN
					
					
					RAISERROR('Creating restore pollster job', 0, 1) WITH NOWAIT;

						
						IF @EnableRestoreJobs = 1
                            BEGIN
						    EXEC msdb.dbo.sp_add_job @job_name = sp_AllNightLog_PollDiskForNewDatabases, 
												     @description = 'This is a worker for the purposes of polling your restore path for new entries to insert to the worker queue table.', 
												     @category_name = 'Database Maintenance', 
												     @owner_login_name = 'sa',
												     @enabled = 1;
                            END
                        ELSE
                            BEGIN
						    EXEC msdb.dbo.sp_add_job @job_name = sp_AllNightLog_PollDiskForNewDatabases, 
												     @description = 'This is a worker for the purposes of polling your restore path for new entries to insert to the worker queue table.', 
												     @category_name = 'Database Maintenance', 
												     @owner_login_name = 'sa',
												     @enabled = 0;
                            END
					
					
					
					RAISERROR('Adding restore job step', 0, 1) WITH NOWAIT;

						
						EXEC msdb.dbo.sp_add_jobstep @job_name = sp_AllNightLog_PollDiskForNewDatabases, 
													 @step_name = sp_AllNightLog_PollDiskForNewDatabases, 
													 @subsystem = 'TSQL', 
													 @command = 'EXEC sp_AllNightLog @PollDiskForNewDatabases = 1';
					
					
					
					RAISERROR('Adding restore job server', 0, 1) WITH NOWAIT;

						
						EXEC msdb.dbo.sp_add_jobserver @job_name = sp_AllNightLog_PollDiskForNewDatabases;

					
									
					RAISERROR('Attaching schedule', 0, 1) WITH NOWAIT;
		
						
						EXEC msdb.dbo.sp_attach_schedule @job_name = sp_AllNightLog_PollDiskForNewDatabases, 
														 @schedule_name = ten_seconds;
		
				
				END;	



				/*
				
				This section creates @Jobs (quantity) of worker jobs to take log backups with

				They work in a queue

				It's queuete
				
				*/


				RAISERROR('Checking for sp_AllNightLog backup jobs', 0, 1) WITH NOWAIT;
				
					
					SELECT @counter = COUNT(*) + 1 
					FROM msdb.dbo.sysjobs 
					WHERE name LIKE 'sp[_]AllNightLog[_]Backup[_]%';

					SET @msg = 'Found ' + CONVERT(NVARCHAR(10), (@counter - 1)) + ' backup jobs -- ' +  CASE WHEN @counter < @Jobs THEN + 'starting loop!'
																											 WHEN @counter >= @Jobs THEN 'skipping loop!'
																											 ELSE 'Oh woah something weird happened!'
																										END;	

					RAISERROR(@msg, 0, 1) WITH NOWAIT;

					
							WHILE @counter <= @Jobs

							
								BEGIN

									
										RAISERROR('Setting job name', 0, 1) WITH NOWAIT;

											SET @job_name_backups = N'sp_AllNightLog_Backup_' + CASE WHEN @counter < 10 THEN N'0' + CONVERT(NVARCHAR(10), @counter)
																									 WHEN @counter >= 10 THEN CONVERT(NVARCHAR(10), @counter)
																								END; 
							
										
										RAISERROR('Setting @job_sql', 0, 1) WITH NOWAIT;

										
											SET @job_sql = N'
							
											EXEC msdb.dbo.sp_add_job @job_name = ' + @job_name_backups + ', 
																	 @description = ' + @job_description_backups + ', 
																	 @category_name = ' + @job_category + ', 
																	 @owner_login_name = ' + @job_owner + ',';
                                            IF @EnableBackupJobs = 1
                                                BEGIN
                                                SET @job_sql = @job_sql + ' @enabled = 1; ';
                                                END
                                            ELSE
                                                BEGIN
                                                SET @job_sql = @job_sql + ' @enabled = 0; ';
                                                END
								  
											
                                            SET @job_sql = @job_sql + '
											EXEC msdb.dbo.sp_add_jobstep @job_name = ' + @job_name_backups + ', 
																		 @step_name = ' + @job_name_backups + ', 
																		 @subsystem = ''TSQL'', 
																		 @command = ' + @job_command_backups + ';
								  
											
											EXEC msdb.dbo.sp_add_jobserver @job_name = ' + @job_name_backups + ';
											
											
											EXEC msdb.dbo.sp_attach_schedule  @job_name = ' + @job_name_backups + ', 
																			  @schedule_name = ten_seconds;
											
											';
							
										
										SET @counter += 1;

										
											IF @Debug = 1
												BEGIN 
													RAISERROR(@job_sql, 0, 1) WITH NOWAIT;
												END; 		

		
											IF @job_sql IS NULL
											BEGIN
												RAISERROR('@job_sql is NULL for some reason', 0, 1) WITH NOWAIT;
											END; 


										EXEC sp_executesql @job_sql;

							
								END;		



				/*
				
				This section creates @Jobs (quantity) of worker jobs to restore logs with

				They too work in a queue

				Like a queue-t 3.14
				
				*/


				RAISERROR('Checking for sp_AllNightLog Restore jobs', 0, 1) WITH NOWAIT;
				
					
					SELECT @counter = COUNT(*) + 1 
					FROM msdb.dbo.sysjobs 
					WHERE name LIKE 'sp[_]AllNightLog[_]Restore[_]%';

					SET @msg = 'Found ' + CONVERT(NVARCHAR(10), (@counter - 1)) + ' restore jobs -- ' +  CASE WHEN @counter < @Jobs THEN + 'starting loop!'
																											  WHEN @counter >= @Jobs THEN 'skipping loop!'
																											  ELSE 'Oh woah something weird happened!'
																										 END;	

					RAISERROR(@msg, 0, 1) WITH NOWAIT;

					
							WHILE @counter <= @Jobs

							
								BEGIN

									
										RAISERROR('Setting job name', 0, 1) WITH NOWAIT;

											SET @job_name_restores = N'sp_AllNightLog_Restore_' + CASE WHEN @counter < 10 THEN N'0' + CONVERT(NVARCHAR(10), @counter)
																									   WHEN @counter >= 10 THEN CONVERT(NVARCHAR(10), @counter)
																								  END; 
							
										
										RAISERROR('Setting @job_sql', 0, 1) WITH NOWAIT;

										
											SET @job_sql = N'
							
											EXEC msdb.dbo.sp_add_job @job_name = ' + @job_name_restores + ', 
																	 @description = ' + @job_description_restores + ', 
																	 @category_name = ' + @job_category + ', 
																	 @owner_login_name = ' + @job_owner + ',';
                                            IF @EnableRestoreJobs = 1
                                                BEGIN
                                                SET @job_sql = @job_sql + ' @enabled = 1; ';
                                                END
                                            ELSE
                                                BEGIN
                                                SET @job_sql = @job_sql + ' @enabled = 0; ';
                                                END
								  
											
                                            SET @job_sql = @job_sql + '
											
											EXEC msdb.dbo.sp_add_jobstep @job_name = ' + @job_name_restores + ', 
																		 @step_name = ' + @job_name_restores + ', 
																		 @subsystem = ''TSQL'', 
																		 @command = ' + @job_command_restores + ';
								  
											
											EXEC msdb.dbo.sp_add_jobserver @job_name = ' + @job_name_restores + ';
											
											
											EXEC msdb.dbo.sp_attach_schedule  @job_name = ' + @job_name_restores + ', 
																			  @schedule_name = ten_seconds;
											
											';
							
										
										SET @counter += 1;

										
											IF @Debug = 1
												BEGIN 
													RAISERROR(@job_sql, 0, 1) WITH NOWAIT;
												END; 		

		
											IF @job_sql IS NULL
											BEGIN
												RAISERROR('@job_sql is NULL for some reason', 0, 1) WITH NOWAIT;
											END; 


										EXEC sp_executesql @job_sql;

							
								END;		


		RAISERROR('Setup complete!', 0, 1) WITH NOWAIT;
		
			END; --End for the Agent job creation

		END;--End for Database and Table creation

	END TRY

	BEGIN CATCH


		SELECT @msg = N'Error occurred during setup: ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
			   @error_severity = ERROR_SEVERITY(), 
			   @error_state = ERROR_STATE();
		
		RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;


		WHILE @@TRANCOUNT > 0
			ROLLBACK;

	END CATCH;

END;  /* IF @RunSetup = 1 */

RETURN;


UpdateConfigs:

IF @UpdateSetup = 1
	
	BEGIN

        /* If we're enabling backup jobs, we may need to run restore with recovery on msdbCentral to bring it online: */
        IF @EnableBackupJobs = 1 AND EXISTS (SELECT * FROM sys.databases WHERE name = 'msdbCentral' AND state = 1)
            BEGIN 
				RAISERROR('msdbCentral exists, but is in restoring state. Running restore with recovery...', 0, 1) WITH NOWAIT;

                BEGIN TRY
                    RESTORE DATABASE [msdbCentral] WITH RECOVERY;
                END TRY

				BEGIN CATCH

					SELECT @error_number = ERROR_NUMBER(), 
							@error_severity = ERROR_SEVERITY(), 
							@error_state = ERROR_STATE();

					SELECT @msg = N'Error running restore with recovery on msdbCentral, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							@error_severity = ERROR_SEVERITY(), 
							@error_state = ERROR_STATE();
						
					RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;

				END CATCH;

            END

            /* Only check for this after trying to restore msdbCentral: */
            IF @EnableBackupJobs = 1 AND NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'msdbCentral' AND state = 0)
                    BEGIN
        				RAISERROR('msdbCentral is not online. Repair that first, then try to enable backup jobs.', 0, 1) WITH NOWAIT;
                        RETURN
                    END


			IF OBJECT_ID('msdbCentral.dbo.backup_configuration') IS NOT NULL

				RAISERROR('Found backup config, checking variables...', 0, 1) WITH NOWAIT;
	
				BEGIN

					BEGIN TRY

						
						IF @RPOSeconds IS NOT NULL


							BEGIN

								RAISERROR('Attempting to update RPO setting', 0, 1) WITH NOWAIT;

								UPDATE c
										SET c.configuration_setting = CONVERT(NVARCHAR(10), @RPOSeconds)
								FROM msdbCentral.dbo.backup_configuration AS c
								WHERE c.configuration_name = N'log backup frequency';

							END;

						
						IF @BackupPath IS NOT NULL

							BEGIN
								
								RAISERROR('Attempting to update Backup Path setting', 0, 1) WITH NOWAIT;

								UPDATE c
										SET c.configuration_setting = @BackupPath
								FROM msdbCentral.dbo.backup_configuration AS c
								WHERE c.configuration_name = N'log backup path';


							END;

					END TRY


					BEGIN CATCH


						SELECT @error_number = ERROR_NUMBER(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();

						SELECT @msg = N'Error updating backup configuration setting, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;


					END CATCH;

			END;


			IF OBJECT_ID('msdb.dbo.restore_configuration') IS NOT NULL

				RAISERROR('Found restore config, checking variables...', 0, 1) WITH NOWAIT;

				BEGIN

					BEGIN TRY

                        EXEC msdb.dbo.sp_update_schedule @name = ten_seconds, @active_start_date = @active_start_date, @active_start_time = 000000;

                        IF @EnableRestoreJobs IS NOT NULL
                            BEGIN
            				RAISERROR('Changing restore job status based on @EnableBackupJobs parameter...', 0, 1) WITH NOWAIT;
                            INSERT INTO @jobs_to_change(name)
                                SELECT name 
                                FROM msdb.dbo.sysjobs 
                                WHERE name LIKE 'sp_AllNightLog_Restore%' OR name = 'sp_AllNightLog_PollDiskForNewDatabases';
                            DECLARE jobs_cursor CURSOR FOR  
                                SELECT name 
                                FROM @jobs_to_change

                            OPEN jobs_cursor   
                            FETCH NEXT FROM jobs_cursor INTO @current_job_name   

                            WHILE @@FETCH_STATUS = 0   
                            BEGIN   
            				       RAISERROR(@current_job_name, 0, 1) WITH NOWAIT;
                                   EXEC msdb.dbo.sp_update_job @job_name=@current_job_name,@enabled = @EnableRestoreJobs;
                                   FETCH NEXT FROM jobs_cursor INTO @current_job_name   
                            END   

                            CLOSE jobs_cursor   
                            DEALLOCATE jobs_cursor
                            DELETE @jobs_to_change;
                            END;

                        /* If they wanted to turn off restore jobs, wait to make sure that finishes before we start enabling the backup jobs */
                        IF @EnableRestoreJobs = 0
                            BEGIN
                            SET @started_waiting_for_jobs = GETDATE();
                            SELECT  @counter = COUNT(*)
		                            FROM    [msdb].[dbo].[sysjobactivity] [ja]
		                            INNER JOIN [msdb].[dbo].[sysjobs] [j]
			                            ON [ja].[job_id] = [j].[job_id]
		                            WHERE    [ja].[session_id] = (
										                            SELECT    TOP 1 [session_id]
										                            FROM    [msdb].[dbo].[syssessions]
										                            ORDER BY [agent_start_date] DESC
									                            )
				                            AND [start_execution_date] IS NOT NULL
				                            AND [stop_execution_date] IS NULL
				                            AND [j].[name] LIKE 'sp_AllNightLog_Restore%';

                            WHILE @counter > 0
                                BEGIN
                                    IF DATEADD(SS, 120, @started_waiting_for_jobs) < GETDATE()
                                        BEGIN
                                        RAISERROR('OH NOES! We waited 2 minutes and restore jobs are still running. We are stopping here - get a meatbag involved to figure out if restore jobs need to be killed, and the backup jobs will need to be enabled manually.', 16, 1) WITH NOWAIT;
                                        RETURN
                                        END
                                    SET @msg = N'Waiting for ' + CAST(@counter AS NVARCHAR(100)) + N' sp_AllNightLog_Restore job(s) to finish.'
                                    RAISERROR(@msg, 0, 1) WITH NOWAIT;
                                    WAITFOR DELAY '0:00:01'; -- Wait until the restore jobs are fully stopped
	
	                                SELECT  @counter = COUNT(*)
		                                FROM    [msdb].[dbo].[sysjobactivity] [ja]
		                                INNER JOIN [msdb].[dbo].[sysjobs] [j]
			                                ON [ja].[job_id] = [j].[job_id]
		                                WHERE    [ja].[session_id] = (
										                                SELECT    TOP 1 [session_id]
										                                FROM    [msdb].[dbo].[syssessions]
										                                ORDER BY [agent_start_date] DESC
									                                )
				                                AND [start_execution_date] IS NOT NULL
				                                AND [stop_execution_date] IS NULL
				                                AND [j].[name] LIKE 'sp_AllNightLog_Restore%';
                                END
                            END /* IF @EnableRestoreJobs = 0 */


                        IF @EnableBackupJobs IS NOT NULL
                            BEGIN
            				RAISERROR('Changing backup job status based on @EnableBackupJobs parameter...', 0, 1) WITH NOWAIT;
                            INSERT INTO @jobs_to_change(name)
                                SELECT name 
                                FROM msdb.dbo.sysjobs 
                                WHERE name LIKE 'sp_AllNightLog_Backup%' OR name = 'sp_AllNightLog_PollForNewDatabases';
                            DECLARE jobs_cursor CURSOR FOR  
                                SELECT name 
                                FROM @jobs_to_change

                            OPEN jobs_cursor   
                            FETCH NEXT FROM jobs_cursor INTO @current_job_name   

                            WHILE @@FETCH_STATUS = 0   
                            BEGIN   
            				       RAISERROR(@current_job_name, 0, 1) WITH NOWAIT;
                                   EXEC msdb.dbo.sp_update_job @job_name=@current_job_name,@enabled = @EnableBackupJobs;
                                   FETCH NEXT FROM jobs_cursor INTO @current_job_name   
                            END   

                            CLOSE jobs_cursor   
                            DEALLOCATE jobs_cursor
                            DELETE @jobs_to_change;
                            END;


						
						IF @RTOSeconds IS NOT NULL

							BEGIN

								RAISERROR('Attempting to update RTO setting', 0, 1) WITH NOWAIT;

								UPDATE c
										SET c.configuration_setting = CONVERT(NVARCHAR(10), @RTOSeconds)
								FROM msdb.dbo.restore_configuration AS c
								WHERE c.configuration_name = N'log restore frequency';

							END;

						
						IF @RestorePath IS NOT NULL

							BEGIN
								
								RAISERROR('Attempting to update Restore Path setting', 0, 1) WITH NOWAIT;

								UPDATE c
										SET c.configuration_setting = @RestorePath
								FROM msdb.dbo.restore_configuration AS c
								WHERE c.configuration_name = N'log restore path';


							END;

					END TRY


					BEGIN CATCH


						SELECT @error_number = ERROR_NUMBER(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();

						SELECT @msg = N'Error updating restore configuration setting, error number is ' + CONVERT(NVARCHAR(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(), 
							   @error_severity = ERROR_SEVERITY(), 
							   @error_state = ERROR_STATE();
						
						RAISERROR(@msg, @error_severity, @error_state) WITH NOWAIT;


					END CATCH;

				END;

				RAISERROR('Update complete!', 0, 1) WITH NOWAIT;

			RETURN;

	END; --End updates to configuration table


END; -- Final END for stored proc
GO

