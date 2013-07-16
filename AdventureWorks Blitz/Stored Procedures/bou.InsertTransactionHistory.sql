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
	@Quantity int,
	@ActualCost money
AS
SET NOCOUNT ON;

DECLARE @rowcount INT,
	@productid2 INT,
	@AddressID int,
	@CustomerID int,
	@SalesOrderID int;


BEGIN TRAN

	SELECT @productid2=ISNULL(min(ProductID),999) from Production.Product where ProductID > @ProductID;

	SELECT @CustomerID=MIN(CustomerID) from Sales.Customer 
	WHERE 
	 StoreID is null; /*Online Sale */

	print @CustomerID;

	SELECT @AddressID=ISNULL(MAX(a.AddressID),1) /*Cheating!*/
	FROM Sales.Customer sc
	join Person.Person p on sc.PersonID=p.BusinessEntityID
	join Person.BusinessEntityAddress a on p.BusinessEntityID=a.BusinessEntityID
	where sc.CustomerID=@CustomerID;

	INSERT  [Sales].[SalesOrderHeader]
			   ([RevisionNumber]
			   ,[OrderDate]
			   ,[DueDate]
			   ,[OnlineOrderFlag]
			   ,[CustomerID]
			   ,[BillToAddressID]
			   ,[ShipToAddressID]
			   ,[ShipMethodID])
		SELECT
			   1,
			   getdate(),
				dateadd(dd,1,getdate()),
			   1, 
			   @CustomerID,
			   @AddressID,
			   @AddressID,
			   2

	SELECT @SalesOrderID=SCOPE_IDENTITY()


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
				@SalesOrderID,
				0,
				GETDATE(),
				N'W',
				@Quantity,
				@ActualCost,
				GETDATE();

		SET  @rowcount= @@ROWCOUNT;
		SELECT  @rowcount;



COMMIT
GO 

