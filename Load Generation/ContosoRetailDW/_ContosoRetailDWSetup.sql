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
IF DB_ID('ContosoRetailDW') is not null
BEGIN
	ALTER DATABASE ContosoRetailDW set single_user with rollback immediate
END
RESTORE DATABASE ContosoRetailDW from 
	disk='S:\_INSTALL\ContosoRetailDW.bak' with replace,
	move 'ContosoRetailDW2.0' to 'S:\MSSQL\Data\ContosoRetailDW_Data.mdf',
	move 'ContosoRetailDW2.0_log' to 'S:\MSSQL\Data\ContosoRetailDW_log.ldf';
GO



:setvar ScriptDir "Y:\Git\CriticalCare\Load Generation\AdventureWorks\"




--Do one read against small NC indexes
--This keeps them from distracting in sp_BlitzIndex demos
USE ContosoRetailDW;
GO
EXEC sp_msforeachtable 'select count(*) from ?'

--Clear the procedure cache
dbcc freeproccache;
GO

--set max server memory 
exec sp_configure 'max server memory', 700;
GO
RECONFIGURE
GO

exec sp_configure 'max degree of parallelism', 2;
GO 
RECONFIGURE;
GO
