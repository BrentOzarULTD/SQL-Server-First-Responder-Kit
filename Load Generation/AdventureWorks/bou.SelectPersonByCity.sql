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

IF OBJECT_ID('bou.SelectPersonByCity')IS NULL
	EXEC ('CREATE PROCEDURE bou.SelectPersonByCity AS RETURN 0');
GO

ALTER PROCEDURE bou.SelectPersonByCity
	@City nvarchar(512),
	@PageStart int = 0,
	@PageEnd int = 10
AS
SET NOCOUNT ON;


with peeps AS (
	SELECT
		ROW_NUMBER() OVER (ORDER BY p.LastName, p.FirstName) as row_num,
		p.[Title],
		p.[FirstName],
		p.[MiddleName],
		p.[LastName],
		p.[Suffix],
		ea.[EmailAddress],
		p.[EmailPromotion],
		a.[AddressLine1],
		a.[AddressLine2],
		a.[City],
		sp.[Name] AS [StateProvinceName],
		a.[PostalCode],
		cr.[Name] AS [CountryRegionName],
		p.[AdditionalContactInfo]
	FROM [Person].[Person] p
	INNER JOIN [Person].[BusinessEntityAddress] bea ON bea.[BusinessEntityID] = p.[BusinessEntityID]
	INNER JOIN [Person].[Address] a ON a.[AddressID] = bea.[AddressID]
	INNER JOIN [Person].[StateProvince] sp ON sp.[StateProvinceID] = a.[StateProvinceID]
	INNER JOIN [Person].[CountryRegion] cr ON cr.[CountryRegionCode] = sp.[CountryRegionCode]
	LEFT JOIN [Person].[PersonPhone] pp ON pp.BusinessEntityID = p.[BusinessEntityID]
	LEFT JOIN [Person].[PhoneNumberType] pnt ON pp.[PhoneNumberTypeID] = pnt.[PhoneNumberTypeID]
	LEFT JOIN [Person].[EmailAddress] ea ON p.[BusinessEntityID] = ea.[BusinessEntityID]
	WHERE a.City = @City
)
SELECT *
FROM Peeps
where row_num between @PageStart and @PageEnd
GO 

