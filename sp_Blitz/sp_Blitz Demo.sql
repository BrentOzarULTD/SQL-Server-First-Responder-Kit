/* Calling it: */
EXEC dbo.sp_Blitz;







/* Got a messy vendor database that you don't care about? */
EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 0;








/* Want to know about your hardware? */
EXEC dbo.sp_Blitz @CheckServerInfo = 1;








/* Find problems in the most resource-intensive queries: */
EXEC dbo.sp_Blitz @CheckProcedureCache = 1








/* Want to log to a table? */
EXEC [dbo].[sp_Blitz] 
  @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzResults'







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










/* To let end users run it, create a certificate and sign the stored proc: */
USE master;
GO
CREATE CERTIFICATE sp_Blitz_cert
    ENCRYPTION BY PASSWORD = 'Get lucky'
    WITH SUBJECT = 'Certificate for sp_Blitz',
    START_DATE = '20130711', EXPIRY_DATE = '21000101';
GO
CREATE LOGIN sp_Blitz_login FROM CERTIFICATE sp_Blitz_cert;
GO
CREATE USER [sp_Blitz_login] FOR LOGIN [sp_Blitz_login]
GO
GRANT EXECUTE ON dbo.sp_Blitz TO sp_Blitz_login;
GO
GRANT CONTROL SERVER TO sp_Blitz_login;
GO
 
/* Sign the stored proc. Whenever you update sp_Blitz, you'll need to redo this. */ 
ADD SIGNATURE TO sp_Blitz BY CERTIFICATE sp_Blitz_cert
    WITH PASSWORD = 'Get lucky';
GO
GRANT EXECUTE ON dbo.sp_Blitz TO [public];
GO


/* Undoing the demo:
DROP USER sp_Blitz_login;
DROP LOGIN sp_Blitz_login;
DROP SIGNATURE FROM sp_Blitz BY CERTIFICATE sp_Blitz_cert;
DROP CERTIFICATE sp_Blitz_cert;
GO
*/