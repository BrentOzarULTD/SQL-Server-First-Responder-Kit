/* 
How sp_Blitz works:
1. Start by creating a temp table.
*/
CREATE TABLE #BlitzResults (ID INT IDENTITY(1, 1),
	Priority TINYINT ,
	Finding VARCHAR(200));

/* 2. Check for a problem. */

IF EXISTS (SELECT * FROM sys.databases WHERE is_auto_shrink_on = 1)
	/* Add a record to the temp table if we found problems. */
	INSERT INTO #BlitzResults (Priority, Finding)
	VALUES (1, 'Databases have auto-shrink turned on. Fix that.');


/* 3. Return the list of problems. */

SELECT * FROM #BlitzResults ORDER BY Priority;
DROP TABLE #BlitzResults;


















/* 
How sp_AskBrent works:
1. Start by creating a temp table.
*/
CREATE TABLE #AskBrentResults (ID INT IDENTITY(1, 1),
	Priority TINYINT ,
	Finding VARCHAR(200));

/* 2. Check for a problem. */

IF EXISTS (SELECT * FROM sys.dm_exec_requests WHERE command LIKE 'BACKUP%')
	/* Add a record to the temp table if we found problems. */
	INSERT INTO #AskBrentResults (Priority, Finding)
	VALUES (1, 'A backup is running right now.');


/* 3. Return the list of problems. */

SELECT * FROM #AskBrentResults ORDER BY Priority;
DROP TABLE #AskBrentResults;



















/* In the real sp_AskBrent, each check is just a little bit more complex. */
INSERT INTO #AskBrentResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, 
	StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount)
SELECT 1 AS CheckID,
	1 AS Priority,
	'Maintenance Tasks Running' AS FindingGroup,
	'Backup Running' AS Finding,
	'http://BrentOzar.com/go/backups' AS URL,
	@StockDetailsHeader + 'Backup of ' + DB_NAME(db.resource_database_id) + ' database (' + (SELECT CAST(CAST(SUM(size * 8.0 / 1024 / 1024) AS BIGINT) AS NVARCHAR) FROM sys.master_files WHERE database_id = db.resource_database_id) + 'GB) is ' + CAST(r.percent_complete AS NVARCHAR(100)) + '% complete, has been running since ' + CAST(r.start_time AS NVARCHAR(100)) + '. ' AS Details,
	CAST(@StockWarningHeader + 'KILL ' + CAST(r.session_id AS NVARCHAR(100)) + ';' + @StockWarningFooter AS XML) AS HowToStopIt,
	pl.query_plan AS QueryPlan,
	r.start_time AS StartTime,
	s.login_name AS LoginName,
	s.nt_user_name AS NTUserName,
	s.original_login_name AS OriginalLoginName,
	s.[program_name] AS ProgramName,
	s.[host_name] AS HostName,
	db.[resource_database_id] AS DatabaseID,
	DB_NAME(db.resource_database_id) AS DatabaseName,
	0 AS OpenTransactionCount
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
INNER JOIN (
SELECT DISTINCT request_session_id, resource_database_id
FROM    sys.dm_tran_locks
WHERE resource_type = N'DATABASE'
AND     request_mode = N'S'
AND     request_status = N'GRANT'
AND     request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') AS db ON s.session_id = db.request_session_id
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) pl
WHERE r.command LIKE 'BACKUP%';








/* To get started understanding what kind of data it's gathering,
   check out @ExpertMode = 1, which returns lots of the work tables. */

EXEC dbo.sp_AskBrent @ExpertMode = 1




/* You get 5-second samples of these to write your own checks:
	* Wait stats from sys.dm_os_wait_stats
	* Data and log file activity from sys.dm_io_virtual_file_stats
	* Perfmon counters from sysm.dm_os_performance_counters
	* Query plan cache from sys.dm_exec_query_stats - work in progress
*/











































/* Want to log to a table? */
EXEC [dbo].[sp_AskBrent] 
  @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'AskBrentResults'



/* Want to travel back in time? Check the table. */
EXEC dbo.sp_AskBrent @AsOf = '2013-10-17 17:30', 
  @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'AskBrentResults'













































/* To let end users run it, create a certificate and sign the stored proc: */
USE master;
GO
CREATE CERTIFICATE sp_AskBrent_cert
    ENCRYPTION BY PASSWORD = 'Get lucky'
    WITH SUBJECT = 'Certificate for sp_AskBrent',
    START_DATE = '20130711', EXPIRY_DATE = '21000101';
GO
CREATE LOGIN sp_AskBrent_login FROM CERTIFICATE sp_AskBrent_cert;
GO
CREATE USER [sp_AskBrent_login] FOR LOGIN [sp_AskBrent_login]
GO
GRANT EXECUTE ON dbo.sp_AskBrent TO sp_AskBrent_login;
GO
GRANT CONTROL SERVER TO sp_AskBrent_login;
GO
 
/* Sign the stored proc. Whenever you update sp_AskBrent, you'll need to redo this. */ 
ADD SIGNATURE TO sp_AskBrent BY CERTIFICATE sp_AskBrent_cert
    WITH PASSWORD = 'Get lucky';
GO
GRANT EXECUTE ON dbo.sp_AskBrent TO [public];
GO


/* Undoing the demo:
DROP USER sp_AskBrent_login;
DROP LOGIN sp_AskBrent_login;
DROP SIGNATURE FROM sp_AskBrent BY CERTIFICATE sp_AskBrent_cert;
DROP CERTIFICATE sp_AskBrent_cert;
GO
*/












































EXEC dbo.sp_AskBrent 'Should we put a clustered index on the table?'
