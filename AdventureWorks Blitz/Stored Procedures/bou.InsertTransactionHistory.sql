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

IF OBJECT_ID('bou.InsertTransactionHistory')IS NULL
	EXEC ('CREATE PROCEDURE bou.InsertTransactionHistory AS RETURN 0');
GO

ALTER PROCEDURE bou.InsertTransactionHistory
	@ProductID int, --318-999
	@ReferenceOrderID int,
	@Quantity int,
	@ActualCost money
AS
SET NOCOUNT ON;

declare @rowcount INT,
	@productid2 INT;

SELECT @productid2=ISNULL(min(ProductID),999) from Production.Product where ProductID > @ProductID;


INSERT INTO [Production].[TransactionHistory]
           ([ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,[TransactionDate]
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost]
           ,[ModifiedDate])
     SELECT
			@productid2,
			@ReferenceOrderID,
			0,
			GETDATE(),
			N'W',
			@Quantity,
			@ActualCost,
			GETDATE();

	SET  @rowcount= @@ROWCOUNT;
	SELECT  @rowcount;
GO 

