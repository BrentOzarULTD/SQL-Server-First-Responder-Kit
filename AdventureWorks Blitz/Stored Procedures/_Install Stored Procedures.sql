--******************
-- (C) 2013, Brent Ozar Unlimited. 
-- See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

--This script suitable only for test purposes.
--Do not run on production servers.
--WARNING: This clears procedure cache, reconfigures max server memory, and more!

--******************

----------------------
---You must be in SQLCMD mode for this script to work!
----------------------
:on error exit


use master;

--Restore AdventureWorks (with prejudice)
IF DB_ID('AdventureWorks2012') is not null
BEGIN
	ALTER DATABASE AdventureWorks2012 set single_user with rollback immediate
END
RESTORE DATABASE AdventureWorks2012 from 
	disk='C:\MSSQL11.SQL2012CS\MSSQL\Backup\AdventureWorks2012-Full Database Backup.bak' with replace,
	move 'AdventureWorks2012_Data' to 'C:\MSSQL11.SQL2012CS\MSSQL\DATA\AdventureWorksBlitz_Data.mdf',
	move 'AdventureWorks2012_Log' to 'C:\MSSQL11.SQL2012CS\MSSQL\DATA\AdventureWorksBlitz_log.ldf';
GO

:setvar ScriptDir "Y:\Git\CriticalCare\AdventureWorks Blitz\Stored Procedures"


--Create the stored procedures
:r $(ScriptDir)\bou.CustomerReport.sql
:r $(ScriptDir)\bou.InsertTransactionHistory.sql
:r $(ScriptDir)\bou.SelectEmployeeDeptHistoryByShift.sql
:r $(ScriptDir)\bou.SelectPersonByCity.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProduct.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProductAndDate.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByDateRange.sql

USE AdventureWorks2012;
GO
ALTER INDEX ALL ON [Production].[TransactionHistory] REBUILD  WITH (FILLFACTOR=10);
ALTER INDEX ALL ON [Person].[Address] REBUILD WITH (FILLFACTOR=10);
ALTER INDEX ALL ON [Person].[Person] REBUILD WITH (FILLFACTOR=10);
ALTER INDEX ALL ON [HumanResources].[EmployeeDepartmentHistory] REBUILD WITH (FILLFACTOR=10);

GO

--Do one read against small NC indexes
--This keeps them from distracting in sp_BlitzIndex demos
USE AdventureWorks2012;
GO
EXEC sp_msforeachtable 'select count(*) from ?'

--Clear the procedure cache
dbcc freeproccache;
GO

--set max server memory 
exec sp_configure 'max server memory', 2000;
GO
RECONFIGURE
GO

exec sp_configure 'max degree of parallelism', 2;
GO 
RECONFIGURE;
GO
