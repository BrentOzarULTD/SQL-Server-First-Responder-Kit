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

IF OBJECT_ID('bou.SelectTransactionHistoryByProduct')IS NULL
	EXEC ('CREATE PROCEDURE bou.SelectTransactionHistoryByProduct AS RETURN 0');
GO
ALTER PROCEDURE [bou].[SelectTransactionHistoryByProduct]
	@ProductID INT --316-999
AS
SET NOCOUNT ON;


SELECT TOP 5 
	ReferenceOrderID, 
	ReferenceOrderLineID
FROM Production.TransactionHistory
WHERE ProductId=@ProductID
ORDER BY TransactionDate DESC;

GO

