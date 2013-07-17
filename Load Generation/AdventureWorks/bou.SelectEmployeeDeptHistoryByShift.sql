--******************
-- (C) 2013, Brent Ozar Unlimited. 
-- See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

--This script suitable only for test purposes.
--Do not run on production servers.
--******************

USE AdventureWorks2012;
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (SELECT COUNT(*) FROM sys.schemas WHERE name='bou') = 0
	EXEC ('CREATE SCHEMA bou AUTHORIZATION dbo');
GO

IF OBJECT_ID('bou.SelectEmployeeDeptHistoryByShift')IS NULL
	EXEC ('CREATE PROCEDURE bou.SelectEmployeeDeptHistoryByShift AS RETURN 0');
GO
--EXEC bou.SelectEmployeeDeptHistoryByShift
ALTER PROCEDURE bou.SelectEmployeeDeptHistoryByShift
	@ShiftID TINYINT --1-3
AS
SET NOCOUNT ON;


SELECT 
	BusinessEntityID, 
	DepartmentID
FROM HumanResources.EmployeeDepartmentHistory
WHERE ShiftID=@ShiftID;

GO 
