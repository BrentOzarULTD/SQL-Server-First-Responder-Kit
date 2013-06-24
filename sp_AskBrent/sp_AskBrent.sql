USE [master];
GO

IF OBJECT_ID('master.dbo.sp_AskBrent') IS NOT NULL 
    DROP PROC dbo.sp_AskBrent;
GO

CREATE PROCEDURE [dbo].[sp_AskBrent]
    @Question NVARCHAR(MAX) = NULL ,
    @OutputType VARCHAR(20) = 'TABLE' ,
	@ExpertMode TINYINT = 0,
    @OutputDatabaseName NVARCHAR(128) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
    @Version INT = NULL OUTPUT
AS 
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	/*
	sp_AskBrent (TM) v1 - June 23, 2013
    
	(C) 2013, Brent Ozar Unlimited. 
	See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

	Sure, the server needs tuning - but why is it slow RIGHT NOW?
	sp_AskBrent performs quick checks for things like:

	* Blocking queries that have been running a long time
	* Backups and restores
	* Transactions that are rolling back
	* Long-running end user queries waiting on slow workstations

	To learn more, visit http://www.BrentOzar.com/askbrent/ where you can download
	new versions for free, watch training videos on how it works, get more info on
	the findings, and more.  To contribute code and see your name in the change
	log, email your improvements & checks to Help@BrentOzar.com.

	Known limitations of this version:
	 - No support for SQL Server 2000 or compatibility mode 80.

	Unknown limitations of this version:
	 - None. Like Zombo.com, the only limit is yourself.

    Changes in v1 - June 23, 2013
	 - Initial bug-filled release. We purposely left extra errors on here so
	   you could email bug reports to Help@BrentOzar.com, thereby increasing
	   your self-esteem.

	For prior changes, see http://www.BrentOzar.com/blitz/changelog/
	*/


	/*
	We start by creating #AskBrentResults. It's a temp table that will storef
	the results from our checks. Throughout the rest of this stored procedure,
	we're running a series of checks looking for dangerous things inside the SQL
	Server. When we find a problem, we insert rows into #BlitzResults. At the
	end, we return these results to the end user.

	#AskBrentResults has a CheckID field, but there's no Check table. As we do
	checks, we insert data into this table, and we manually put in the CheckID.
	We (Brent Ozar Unlimited) maintain a list of the checks by ID#. You can
	download that from http://www.BrentOzar.com/askbrent/documentation/ if you
	want to build a tool that relies on the output of sp_AskBrent.
	*/
	DECLARE @StringToExecute NVARCHAR(4000),
		@OurSessionID INT = @@SPID,
		@LineFeed NVARCHAR(10) = CHAR(13)+CHAR(10),
		@StockWarningHeader NVARCHAR(500),
		@StockWarningFooter NVARCHAR(100),
		@StockDetailsHeader NVARCHAR(100),
		@StockDetailsFooter NVARCHAR(100);

    IF OBJECT_ID('tempdb..#AskBrentResults') IS NOT NULL 
        DROP TABLE #AskBrentResults;
    CREATE TABLE #AskBrentResults
        (
          ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
          CheckID INT NOT NULL,
          Priority TINYINT NOT NULL,
          FindingsGroup VARCHAR(50) NOT NULL,
          Finding VARCHAR(200) NOT NULL,
          URL VARCHAR(200) NOT NULL,
          Details NVARCHAR(4000) NOT NULL,
		  HowToStopIt [XML] NULL,
          QueryPlan [XML] NULL,
		  StartTime DATETIME NULL,
		  LoginName NVARCHAR(128) NULL,
		  NTUserName NVARCHAR(128) NULL,
		  OriginalLoginName NVARCHAR(128) NULL,
		  ProgramName NVARCHAR(128) NULL,
		  HostName NVARCHAR(128) NULL,
		  DatabaseID INT NULL,
		  DatabaseName NVARCHAR(128) NULL,
		  OpenTransactionCount INT NULL
        );

	SET @StockWarningHeader = '<?ClickToSeeCommmand -- ' + @LineFeed + @LineFeed 
	    + 'WARNING: Running this command may result in data loss or an outage.' + @LineFeed
		+ 'This tool is meant as a shortcut to help generate scripts for DBAs.' + @LineFeed
		+ 'It is not a substitute for database training and experience.' + @LineFeed
		+ 'Now, having said that, here''s the script:' + @LineFeed + @LineFeed;

	SELECT @StockWarningFooter = @LineFeed + @LineFeed + '-- ?>',
		@StockDetailsHeader = '<?ClickToSeeDetails -- ' + @LineFeed,
		@StockDetailsFooter = @LineFeed + ' -- ?>';


/* Maintenance Tasks Running - Backup Running - CheckID 1 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 1 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'Backup Running' AS Finding,
	'http://BrentOzar.com/go/backups' AS URL,
	@StockDetailsHeader + 'Backup ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	p.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	s.database_id AS DatabaseID,
	d.[name] AS DatabaseName,
	s.open_transaction_count AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
INNER JOIN sys.databases d ON s.database_id = d.database_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) p
WHERE r.command = 'BACKUP DATABASE';

/* If there's a backup running, add details explaining how long full backup has been taking in the last month. */
UPDATE #AskBrentResults
SET Details = Details + ' Over the last 60 days, the full backup usually takes ' + CAST((SELECT AVG(DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date)) FROM msdb.dbo.backupset bs WHERE abr.DatabaseName = bs.database_name AND bs.type = 'D' AND bs.backup_start_date > DATEADD(dd, -60, GETDATE()) AND bs.backup_finish_date IS NOT NULL) AS NVARCHAR(100)) + ' minutes.'
FROM #AskBrentResults abr
WHERE abr.CheckID = 1 AND EXISTS (SELECT * FROM msdb.dbo.backupset bs WHERE bs.type = 'D' AND bs.backup_start_date > DATEADD(dd, -60, GETDATE()) AND bs.backup_finish_date IS NOT NULL AND abr.DatabaseName = bs.database_name AND DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date) > 1)



/* Maintenance Tasks Running - DBCC Running - CheckID 2 */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 2 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'DBCC Running' AS Finding,
	'http://BrentOzar.com/go/dbcc' AS URL,
	@StockDetailsHeader + 'Corruption check has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	p.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	s.database_id AS DatabaseID,
	d.[name] AS DatabaseName,
	s.open_transaction_count AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
LEFT OUTER JOIN sys.databases d ON s.database_id = d.database_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) p
WHERE r.command LIKE 'DBCC%';




/* Close out the XML field Details by adding a footer */
UPDATE #AskBrentResults
  SET Details = Details + @StockDetailsFooter;

			/* Add credits for the nice folks who put so much time into building and maintaining this for free: */                    
            INSERT  INTO #AskBrentResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details
                    )
            VALUES  ( -1 ,
                      255 ,
                      'Thanks!' ,
                      'From Brent Ozar Unlimited' ,
                      'http://www.BrentOzar.com/askbrent/' ,
                      '<?Thanks --' + @LineFeed + 'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com. ' + @LineFeed + '-- ?>'
                    );

            SET @Version = 1;
            INSERT  INTO #AskBrentResults
                    ( CheckID ,
                      Priority ,
                      FindingsGroup ,
                      Finding ,
                      URL ,
                      Details

                    )
            VALUES  ( -1 ,
                      0 ,
                      'sp_AskBrent (TM) v1 June 23 2013' ,
                      'From Brent Ozar Unlimited' ,
                      'http://www.BrentOzar.com/askbrent/' ,
                      '<?Thanks --' + @LineFeed + 'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.' + @LineFeed + ' -- ?>'
                    );


			/* @OutputTableName lets us export the results to a permanent table */
            IF @OutputDatabaseName IS NOT NULL
                AND @OutputSchemaName IS NOT NULL
                AND @OutputTableName IS NOT NULL
                AND EXISTS ( SELECT *
                             FROM   sys.databases
                             WHERE  QUOTENAME([name]) = @OutputDatabaseName) 
                BEGIN
                    SET @StringToExecute = 'USE '
                        + @OutputDatabaseName
                        + '; IF EXISTS(SELECT * FROM '
                        + @OutputDatabaseName
                        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                        + @OutputSchemaName
                        + ''') AND NOT EXISTS (SELECT * FROM '
                        + @OutputDatabaseName
                        + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
                        + @OutputSchemaName + ''' AND QUOTENAME(TABLE_NAME) = '''
                        + @OutputTableName + ''') CREATE TABLE '
                        + @OutputSchemaName + '.'
                        + @OutputTableName
                        + ' (ID INT IDENTITY(1,1) NOT NULL, 
							ServerName NVARCHAR(128), 
							CheckDate DATETIME, 
							AskBrentVersion INT,
							Priority TINYINT ,
							FindingsGroup VARCHAR(50) ,
							Finding VARCHAR(200) ,
							URL VARCHAR(200) ,
							Details [XML] ,
							HowToStopIt [XML] ,
							QueryPlan [XML] NULL ,
							CheckID INT ,
							CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'

                    EXEC(@StringToExecute);
                    SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
                        + @OutputDatabaseName
                        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                        + @OutputSchemaName + ''') INSERT '
                        + @OutputDatabaseName + '.'
                        + @OutputSchemaName + '.'
                        + @OutputTableName
                        + ' (ServerName, CheckDate, AskBrentVersion, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, CheckID) SELECT '''
                        + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
                        + ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
                        + ', Priority, FindingsGroup, Finding, URL, Details, QueryPlan, CheckID FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';
                    EXEC(@StringToExecute);
                END
			ELSE IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
				BEGIN
					SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
                        + @OutputTableName
                        + ''') IS NOT NULL) DROP TABLE ' + @OutputTableName + ';'
						+ 'CREATE TABLE '
                        + @OutputTableName
                        + ' (ID INT IDENTITY(1,1) NOT NULL, 
							ServerName NVARCHAR(128), 
							CheckDate DATETIME, 
							AskBrentVersion INT,
							Priority TINYINT ,
							FindingsGroup VARCHAR(50) ,
							Finding VARCHAR(200) ,
							URL VARCHAR(200) ,
							Details [XML] ,
							HowToStopIt [XML] ,
							QueryPlan [XML] NULL ,
							CheckID INT ,
							CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'
                        + ' INSERT '
                        + @OutputTableName
                        + ' (ServerName, CheckDate, AskBrentVersion, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, CheckID) SELECT '''
                        + CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
                        + ''', GETDATE(), ' + CAST(@Version AS NVARCHAR(128))
                        + ', CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered FROM #AskBrentResults ORDER BY Priority , FindingsGroup , Finding , Details';
                    EXEC(@StringToExecute);
				END
			ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
				BEGIN
					RAISERROR('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0)
				END


            DECLARE @separator AS VARCHAR(1);
            IF @OutputType = 'RSV' 
                SET @separator = CHAR(31);
            ELSE 
                SET @separator = ',';

            IF @OutputType = 'COUNT' 
                BEGIN
                    SELECT  COUNT(*) AS Warnings
                    FROM    #AskBrentResults
                END
            ELSE 
                IF @OutputType IN ( 'CSV', 'RSV' ) 
                    BEGIN
			
                        SELECT  Result = CAST([Priority] AS NVARCHAR(100))
                                + @separator + CAST(CheckID AS NVARCHAR(100))
                                + @separator + COALESCE([FindingsGroup],
                                                        '(N/A)') + @separator
                                + COALESCE([Finding], '(N/A)') + @separator
                                + COALESCE(DatabaseName, '(N/A)') + @separator
                                + COALESCE([URL], '(N/A)') + @separator
                                + COALESCE([Details], '(N/A)')
                        FROM    #AskBrentResults
                        ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                Details;
                    END
                ELSE IF @ExpertMode = 0
                    BEGIN
                        SELECT  [Priority] ,
                                [FindingsGroup] ,
                                [Finding] ,
                                [URL] ,
                                CAST([Details] AS [XML]) AS Details,
								[HowToStopIt]
                        FROM    #AskBrentResults
						ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                ID;
                    END
                ELSE IF @ExpertMode = 1
                    BEGIN
                        SELECT  *
                        FROM    #AskBrentResults
						ORDER BY Priority ,
                                FindingsGroup ,
                                Finding ,
                                ID;
                    END

            DROP TABLE #AskBrentResults;

    SET NOCOUNT OFF;
GO


EXEC dbo.sp_AskBrent;
