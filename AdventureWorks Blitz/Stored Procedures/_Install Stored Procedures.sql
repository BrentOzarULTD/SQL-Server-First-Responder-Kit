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


--Create some custom stored procedures
:r $(ScriptDir)\bou.CustomerReport.sql
:r $(ScriptDir)\bou.InsertTransactionHistory.sql
:r $(ScriptDir)\bou.SelectEmployeeDeptHistoryByShift.sql
:r $(ScriptDir)\bou.SelectPersonByCity.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProduct.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByProductAndDate.sql
:r $(ScriptDir)\bou.SelectTransactionHistoryByDateRange.sql
:r $(ScriptDir)\bou.UpdateSalesOrderHeader.sql

--Inflate!
--This makes more queries go parallel 
--(The optimizer considers page count)
USE AdventureWorks2012;
GO
INSERT INTO [Production].[TransactionHistory]
           ([ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,[TransactionDate]
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost])
select
           [ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,DATEADD(yy,2,[TransactionDate])
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost]
FROM [Production].[TransactionHistory]
GO
INSERT INTO [Production].[TransactionHistory]
           ([ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,[TransactionDate]
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost])
select
           [ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,DATEADD(yy,3,[TransactionDate])
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost]
FROM [Production].[TransactionHistory]
GO
INSERT INTO [Production].[TransactionHistory]
           ([ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,[TransactionDate]
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost])
select
           [ProductID]
           ,[ReferenceOrderID]
           ,[ReferenceOrderLineID]
           ,DATEADD(hh,26,[TransactionDate])
           ,[TransactionType]
           ,[Quantity]
           ,[ActualCost]
FROM [Production].[TransactionHistory]
GO
ALTER INDEX ALL ON [Production].[TransactionHistory] REBUILD WITH (FILLFACTOR=90);
ALTER INDEX ALL ON [Person].[Address] REBUILD WITH (FILLFACTOR=70);
ALTER INDEX ALL ON [Person].[Person] REBUILD WITH (FILLFACTOR=70);
ALTER INDEX ALL ON [HumanResources].[EmployeeDepartmentHistory] REBUILD WITH (FILLFACTOR=80);

GO

--Modify a trigger 
--Make it have an implicit conversion
--This will get hit every time bou.UpdateSalesOrderHeader.sql is run

ALTER TRIGGER [Sales].[uSalesOrderHeader] ON [Sales].[SalesOrderHeader] 
AFTER UPDATE NOT FOR REPLICATION AS 
BEGIN
    DECLARE @Count int;

    SET @Count = @@ROWCOUNT;
    IF @Count = 0 
        RETURN;

    SET NOCOUNT ON;

    BEGIN TRY
        -- Update RevisionNumber for modification of any field EXCEPT the Status.
        IF NOT UPDATE([Status])
        BEGIN
            UPDATE [Sales].[SalesOrderHeader]
            SET [Sales].[SalesOrderHeader].[RevisionNumber] = 
                [Sales].[SalesOrderHeader].[RevisionNumber] + 1
            WHERE [Sales].[SalesOrderHeader].[SalesOrderID] IN 
                (SELECT inserted.[SalesOrderID] FROM inserted);
        END;

        -- Update the SalesPerson SalesYTD when SubTotal is updated
        IF UPDATE([SubTotal])
        BEGIN
            DECLARE @StartDate varchar(256),
                    @EndDate varchar(256)

            SET @StartDate = [dbo].[ufnGetAccountingStartDate]();
            SET @EndDate = [dbo].[ufnGetAccountingEndDate]();

            UPDATE [Sales].[SalesPerson]
            SET [Sales].[SalesPerson].[SalesYTD] = 
                (SELECT SUM([Sales].[SalesOrderHeader].[SubTotal])
                FROM [Sales].[SalesOrderHeader] 
                WHERE [Sales].[SalesPerson].[BusinessEntityID] = [Sales].[SalesOrderHeader].[SalesPersonID]
                    AND ([Sales].[SalesOrderHeader].[Status] = 5) -- Shipped
                    AND [Sales].[SalesOrderHeader].[OrderDate] BETWEEN @StartDate AND @EndDate)
            WHERE [Sales].[SalesPerson].[BusinessEntityID] 
                IN (SELECT DISTINCT inserted.[SalesPersonID] FROM inserted 
                    WHERE inserted.[OrderDate] BETWEEN @StartDate AND @EndDate);

            -- Update the SalesTerritory SalesYTD when SubTotal is updated
            UPDATE [Sales].[SalesTerritory]
            SET [Sales].[SalesTerritory].[SalesYTD] = 
                (SELECT SUM([Sales].[SalesOrderHeader].[SubTotal])
                FROM [Sales].[SalesOrderHeader] 
                WHERE [Sales].[SalesTerritory].[TerritoryID] = [Sales].[SalesOrderHeader].[TerritoryID]
                    AND ([Sales].[SalesOrderHeader].[Status] = 5) -- Shipped
                    AND [Sales].[SalesOrderHeader].[OrderDate] BETWEEN @StartDate AND @EndDate)
            WHERE [Sales].[SalesTerritory].[TerritoryID] 
                IN (SELECT DISTINCT inserted.[TerritoryID] FROM inserted 
                    WHERE inserted.[OrderDate] BETWEEN @StartDate AND @EndDate);
        END;
    END TRY
    BEGIN CATCH
        EXECUTE [dbo].[uspPrintError];

        -- Rollback any active or uncommittable transactions before
        -- inserting information in the ErrorLog
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END

        EXECUTE [dbo].[uspLogError];
    END CATCH;
END;

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
