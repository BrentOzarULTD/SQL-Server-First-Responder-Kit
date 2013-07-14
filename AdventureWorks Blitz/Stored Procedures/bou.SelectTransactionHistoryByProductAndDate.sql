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

IF OBJECT_ID('bou.SelectTransactionHistoryByProductAndDate')IS NULL
	EXEC ('CREATE PROCEDURE bou.SelectTransactionHistoryByProductAndDate AS RETURN 0');
GO
--EXEC bou.SelectTransactionHistoryByProductAndDate
ALTER PROCEDURE bou.SelectTransactionHistoryByProductAndDate
	@ProductID INT /*316-999*/,
	@TransactionDate DATETIME /*2007-09-01 00:00:00.000	2013-07-14 08:36:55.543*/
AS
SET NOCOUNT ON;

SELECT 
	ReferenceOrderID, 
	ReferenceOrderLineID, 
	TransactionType, 
	Quantity, 
	ActualCost
FROM Production.TransactionHistory
WHERE 
	CAST(TransactionDate AS DATE)=CAST(@TransactionDate AS DATE) 
	AND ProductId=@ProductId;
GO
