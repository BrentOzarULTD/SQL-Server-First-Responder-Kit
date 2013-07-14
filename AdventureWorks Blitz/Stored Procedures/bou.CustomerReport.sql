USE AdventureWorks2012;
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (SELECT COUNT(*) FROM sys.schemas WHERE name='bou') = 0
	EXEC ('CREATE SCHEMA bou AUTHORIZATION dbo');
GO

IF OBJECT_ID('bou.CustomerReport')IS NULL
	EXEC ('CREATE PROCEDURE bou.CustomerReport AS RETURN 0');
GO
--EXEC bou.CustomerReport
ALTER PROCEDURE bou.CustomerReport
	@City NVARCHAR(256)
AS
SET NOCOUNT ON;

SELECT 
    p.[BusinessEntityID]
    ,p.[Title]
    ,p.[FirstName]
    ,p.[MiddleName]
    ,p.[LastName]
    ,p.[Suffix]
    ,pp.[PhoneNumber]
	,pnt.[Name] AS [PhoneNumberType]
    ,ea.[EmailAddress]
    ,p.[EmailPromotion]
    ,at.[Name] AS [AddressType]
    ,a.[AddressLine1]
    ,a.[AddressLine2]
    ,a.[City]
    ,[StateProvinceName] = sp.[Name]
    ,a.[PostalCode]
    ,[CountryRegionName] = cr.[Name]
    ,p.[Demographics],
	soh.OrderDate,
	soh.ShipDate
FROM [Person].[Person] p
    JOIN [Person].[BusinessEntityAddress] bea 
    ON bea.[BusinessEntityID] = LTRIM(p.[BusinessEntityID])
    JOIN [Person].[Address] a 
    ON a.[AddressID] = LTRIM(bea.[AddressID])
    JOIN [Person].[StateProvince] sp 
    ON sp.[StateProvinceID] = LTRIM(a.[StateProvinceID])
    JOIN [Person].[CountryRegion] cr 
    ON cr.[CountryRegionCode] = LTRIM(sp.[CountryRegionCode])
    JOIN [Person].[AddressType] at 
    ON at.[AddressTypeID] = LTRIM(bea.[AddressTypeID])
	JOIN [Sales].[Customer] c
	ON c.[PersonID] = LTRIM(p.[BusinessEntityID])
	JOIN [Sales].[SalesOrderHeader] soh 
	ON c.CustomerID=LTRIM(soh.CustomerID)
	LEFT OUTER JOIN [Person].[EmailAddress] ea
	ON ea.[BusinessEntityID] = LTRIM(p.[BusinessEntityID])
	LEFT OUTER JOIN [Person].[PersonPhone] pp
	ON pp.[BusinessEntityID] = LTRIM(p.[BusinessEntityID])
	LEFT OUTER JOIN [Person].[PhoneNumberType] pnt
	ON pnt.[PhoneNumberTypeID] = LTRIM(pp.[PhoneNumberTypeID])
WHERE City=@City
GO 
