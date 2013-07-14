--******************
-- (C) 2013, Brent Ozar Unlimited. 
-- See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

--This script suitable only for test purposes.
--Do not run on production servers.
--******************

----------------------
---You must be in SQLCMD mode for this script to work!
----------------------
:on error exit

:setvar ScriptDir "Y:\Git\CriticalCare\AdventureWorks Blitz\Stored Procedures"

use master;

--Restore AdventureWorks (with prejudice)
IF DB_ID('AdventureWorks2012') is not null
BEGIN
	ALTER DATABASE AdventureWorks2012 set single_user with rollback immediate
END
RESTORE DATABASE AdventureWorks2012 from 
	disk='C:\Program Files\Microsoft SQL Server\MSSQL11.SQLEXPRESS\MSSQL\Backup\AdventureWorks2012-Full Database Backup.bak' with replace

--Create the stored procedures
:r $(ScriptDir)\bou.CustomerReport.sql
:r $(ScriptDir)\bou.InsertTransactionHistory.sql
:r $(ScriptDir)\bou.SelectEmployeeDeptHistoryByShift.sql
:r $(ScriptDir)\bou.SelectPersonByCity.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProduct.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProductAndDate.sql