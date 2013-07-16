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

IF OBJECT_ID('bou.SelectTransactionHistoryByDateRange')IS NULL
	EXEC ('CREATE PROCEDURE bou.SelectTransactionHistoryByDateRange AS RETURN 0');
GO
ALTER PROCEDURE [bou].[SelectTransactionHistoryByDateRange]
	@TransactionDate DATETIME 
AS
SET NOCOUNT ON;

SELECT 
	TransactionType, 
	SUM(Quantity) AS TotalQuantity, 
	SUM(ActualCost) AS TotalCost
FROM Production.TransactionHistory
WHERE TransactionDate >= @TransactionDate
GROUP BY TransactionType;
GO

