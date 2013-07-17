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

IF OBJECT_ID('bou.UpdateSalesOrderHeader')IS NULL
	EXEC ('CREATE PROCEDURE bou.UpdateSalesOrderHeader AS RETURN 0');
GO

ALTER PROCEDURE bou.UpdateSalesOrderHeader
	@CustomerID int
AS
SET NOCOUNT ON;

SET XACT_ABORT ON;

declare @rowcount int;
BEGIN TRY

	BEGIN TRAN
	
		UPDATE Sales.SalesOrderHeader set ShipDate = getdate(), SubTotal=SubTotal+1  WHERE CustomerID=@CustomerID;

		SET  @rowcount= @@ROWCOUNT;
		SELECT  @rowcount;

		--Record a message
        INSERT [dbo].[ErrorLog] 
            (
            [UserName], 
            [ErrorNumber], 
            [ErrorSeverity], 
            [ErrorState], 
            [ErrorProcedure], 
            [ErrorMessage]
            ) 
        VALUES 
            (
            CONVERT(sysname, CURRENT_USER), 
            'We did a thing!',
            0,
            0,
            'bou.UpdateSalesOrderHeader',
            'We did a thing!'
            );
		COMMIT TRANSACTION;


END TRY
BEGIN CATCH
		--Record an error message
        INSERT [dbo].[ErrorLog] 
            (
            [UserName], 
            [ErrorNumber], 
            [ErrorSeverity], 
            [ErrorState], 
            [ErrorProcedure], 
            [ErrorMessage]
            ) 
		SELECT
        ERROR_NUMBER() AS ErrorNumber
        ,ERROR_SEVERITY() AS ErrorSeverity
        ,ERROR_STATE() AS ErrorState
        ,ERROR_PROCEDURE() AS ErrorProcedure
        ,ERROR_LINE() AS ErrorLine
        ,ERROR_MESSAGE() AS ErrorMessage;

    IF (XACT_STATE()) = -1
    BEGIN
        ROLLBACK TRANSACTION;
    END;

    IF (XACT_STATE()) = 1
    BEGIN
        COMMIT TRANSACTION;   
    END;

SELECT @@ROWCOUNT;

END CATCH;





	
GO 

