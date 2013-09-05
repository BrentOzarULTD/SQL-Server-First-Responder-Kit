USE [ContosoRetailDW]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (SELECT COUNT(*) FROM sys.schemas WHERE name='bou') = 0
	EXEC ('CREATE SCHEMA bou AUTHORIZATION dbo');
GO

IF OBJECT_ID('bou.SalesByRegion')IS NULL
	EXEC ('CREATE PROCEDURE bou.SalesByRegion AS RETURN 0');
GO



ALTER PROCEDURE bou.SalesByRegion
	@GeographyKey int /*1-3, 269-952*/
AS

 SELECT pc.[ProductCategoryName],
	psc.ProductSubcategoryName AS ProductSubcategory,
    p.ProductName AS Product,
	c.[CustomerKey],
    f.[SalesOrderNumber] AS [OrderNumber],
    f.SalesOrderLineNumber AS LineNumber,
    f.SalesQuantity AS Quantity,
    f.SalesAmount AS Amount,
	g.RegionCountryName AS [Region],
	CASE
		WHEN Month(GetDate()) < Month(c.[BirthDate])
			THEN DateDiff(yy,c.[BirthDate],GetDate()) - 1
        WHEN Month(GetDate()) = Month(c.[BirthDate])
            AND Day(GetDate()) < Day(c.[BirthDate])
            THEN DateDiff(yy,c.[BirthDate],GetDate()) - 1
        ELSE DateDiff(yy,c.[BirthDate],GetDate())
       END AS [Age],
	CASE 
		WHEN c.[YearlyIncome] < 40000 THEN 'Low'
        WHEN c.[YearlyIncome] > 100000 THEN 'High'
        ELSE 'Moderate'
    END AS [IncomeGroup],
    d.[CalendarYear],
	d.[FiscalYear],
	d.[CalendarMonth] AS [Month]
    FROM
        [dbo].[FactOnlineSales] f
    INNER JOIN [dbo].[DimDate] d
        ON f.[DateKey] = d.[DateKey]
    INNER JOIN [dbo].[DimProduct] p
        ON f.[ProductKey] = p.[ProductKey]
    INNER JOIN [dbo].[DimProductSubcategory] psc
        ON p.[ProductSubcategoryKey] = psc.[ProductSubcategoryKey]
    INNER JOIN [dbo].[DimProductCategory] pc
        ON psc.[ProductCategoryKey] = pc.[ProductCategoryKey]
    INNER JOIN [dbo].[DimCustomer] c
        ON f.[CustomerKey] = c.[CustomerKey]
    INNER JOIN [dbo].[DimGeography] g
        ON c.[GeographyKey] = g.[GeographyKey]
	WHERE g.GeographyKey=@GeographyKey;
GO
