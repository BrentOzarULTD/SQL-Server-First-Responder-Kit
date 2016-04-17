USE [ReportServer]
GO
/****** Object:  StoredProcedure [dbo].[sp_BlitzRS]    Script Date: 9/9/2014 10:02:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('dbo.sp_BlitzRS') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_BlitzRS AS RETURN 0;')
GO


ALTER PROCEDURE [dbo].[sp_BlitzRS]
(@WhoGetsWhat TINYINT = 0)
WITH RECOMPILE
/******************************************
sp_BlitzRS (TM) 2014, Brent Ozar Unlimited.
(C) 2014, Brent Ozar Unlimited.
See http://BrentOzar.com/go/eula for the End User Licensing Agreement.



Description: Displays information about a single SQL Server Reporting
Services instance based on the ReportServer database contents. Run this 
against the server with the ReportServer database (usually, but not 
necessarily the server running the SSRS service).

Output: One result set is presented that contains data from the ReportServer
database tables. Other result sets can be included via parameter.

To learn more, visit http://brentozar.com/blitzRS/
where you can download new versions for free, watch training videos on
how it works, get more info on the findings, and more. To contribute
code, file bugs, and see your name in the change log, visit:
http://support.brentozar.com/


KNOWN ISSUES:
- This query will not run on SQL Server 2005.
- Looks for the ReportServer database only.
- May run for several minutes if the catalog is large (1,000+ items).
- Will not identify data-driven subscription source query information
  such as tables, parameters, or recipients.

v0.92 - 2014-09-22
 - Fixed issue where query would try to parse images and other objects as XML.
 - Lengthened dataset CommandText and subscription Parameter variables.

v0.91 - 2014-09-17
 - Corrected inconsistent case-sensitivity

v0.9 - 2014-09-16
 - Changed support links, added credit.

v0.8 - 2014-08-29
 - Added "who gets what" parameter and query.



*******************************************/
AS
BEGIN
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#BlitzRSResults') IS NOT NULL
    DROP TABLE #BlitzRSResults;

CREATE TABLE #BlitzRSResults
    (
      ID INT IDENTITY(1, 1) ,
      CheckID INT ,
      [Priority] TINYINT ,
      FindingsGroup VARCHAR(50) ,
      Finding VARCHAR(200) ,
      URL VARCHAR(200) ,
      Details NVARCHAR(4000)
    )

/* Parameter variables */

DECLARE @DeadReportThreshold SMALLINT = 0

/* 
-------------------------------------------------------------------
CHECK #1
Find reports that don't have any stored procs or shared datasets.
-------------------------------------------------------------------
*/

DECLARE @DataSetContent TABLE
    (
      ItemID UNIQUEIDENTIFIER ,
      ReportName NVARCHAR(200) ,
      DataSetName NVARCHAR(200) ,
      DataSourceName NVARCHAR(200) ,
      CommandText NVARCHAR(MAX) ,
      SharedDataSetRef NVARCHAR(80)
    );
DECLARE @DataSets TABLE
    (
      ItemID UNIQUEIDENTIFIER ,
      ReportName NVARCHAR(200) ,
      Content XML
    );

INSERT  @DataSets
        ( ItemID ,
          ReportName ,
          Content 
        )
        SELECT  ItemID ,
                [Name] AS ReportName ,
                CAST(CAST(Content AS VARBINARY(MAX)) AS XML)
        FROM    dbo.Catalog
		WHERE [Type] = 2;
		;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/reporting/2008/01/reportdefinition' AS p
				 , 'http://schemas.microsoft.com/sqlserver/reporting/2010/01/reportdefinition' AS p1)
INSERT @DataSetContent
        ( ItemID, ReportName, DataSetName, DataSourceName, CommandText, SharedDataSetRef )
SELECT    ItemID , ReportName,
					AOP.Params.value('@Name', 'Varchar(800)') AS 'PDStName',
					AOP.Params.value('p:Query[1]/p:DataSourceName[1]', 'Varchar(800)') AS 'PName',
                    AOP.Params.value('p:Query[1]/p:CommandText[1]', 'Varchar(800)') AS 'PCmd',
					AOP.Params.value('p:SharedDataSet[1]/p:SharedDataSetReference[1]', 'Varchar(800)') AS 'PSDSR'
          FROM      @DataSets
          CROSS APPLY Content.nodes('/p:Report/p:DataSets/p:DataSet') AS AOP ( Params )
UNION
SELECT    ItemID , ReportName, AOP.Params.value('@Name', 'Varchar(800)') AS 'PDStName',
					AOP.Params.value('p1:Query[1]/p1:DataSourceName[1]', 'Varchar(800)') AS 'PName',
                    AOP.Params.value('p1:Query[1]/p1:CommandText[1]', 'Varchar(800)') AS 'PCmd',
					AOP.Params.value('p1:SharedDataSet[1]/p1:SharedDataSetReference[1]', 'Varchar(800)') AS 'PSDSR'
          FROM      @DataSets
          CROSS APPLY Content.nodes('/p1:Report/p1:DataSets/p1:DataSet') AS AOP ( Params )


INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  1 ,
                100 ,
                'Maintainability' ,
                'Reports with 100% inline T-SQL' ,
                'http://brentozar.com/BlitzRS/inlinetsql' ,
                'The datasets in the "' + ReportName
                + '" report all use inline T-SQL, rather that stored procs or shared datasets. This can make datasets harder to tune and schema changes harder to adapt to.'
        FROM    @DataSetContent
        GROUP BY ReportName
        HAVING  SUM(CASE WHEN CHARINDEX('SELECT', CommandText, 0) > 0 THEN 1
                         ELSE 0
                    END) > 1
                AND SUM(CASE WHEN CHARINDEX('SELECT', CommandText, 0) = 0
                                  AND SharedDataSetRef IS NULL THEN 1
                             ELSE 0
                        END)
                + SUM(CASE WHEN SharedDataSetRef IS NOT NULL THEN 1
                           ELSE 0
                      END) = 0


/* 
-------------------------------------------------------------------
CHECK #2
Finds data dumps (large, featureless reports) in disguise.
-------------------------------------------------------------------
*/


DECLARE @ContentFeatures TABLE
    (
      ItemID UNIQUEIDENTIFIER ,
      ReportName NVARCHAR(200) ,
      HasCharts BIT ,
      HasGauges BIT ,
      HasMaps BIT ,
      HasSubreports BIT ,
      HasDrillthroughs BIT ,
      HasToggles BIT
    );
DECLARE @ContentXML TABLE
    (
      ItemID UNIQUEIDENTIFIER ,
      ReportName NVARCHAR(200) ,
      Content XML
    );

INSERT  @ContentXML
        ( ItemID ,
          ReportName ,
          Content 
        )
        SELECT  ItemID ,
                [Name] AS ReportName ,
                CAST(CAST(Content AS VARBINARY(MAX)) AS XML)
        FROM    dbo.Catalog
        WHERE   [Type] = 2;
		;
WITH XMLNAMESPACES('http://schemas.microsoft.com/sqlserver/reporting/2008/01/reportdefinition' AS p
				 , 'http://schemas.microsoft.com/sqlserver/reporting/2010/01/reportdefinition' AS p1)
INSERT @ContentFeatures
        ( ItemID, ReportName, HasCharts, HasGauges, HasMaps, HasSubreports, HasDrillthroughs, HasToggles )
SELECT    ItemID , ReportName,
		CASE WHEN Content.exist('//p:Chart') = 1 THEN 1 ELSE 0 END AS HasCharts,
		CASE WHEN Content.exist('//p:GaugePanel') = 1 THEN 1 ELSE 0 END AS HasGauges,
		CASE WHEN Content.exist('//p:Map') = 1 THEN 1 ELSE 0 END AS HasMaps,
		CASE WHEN Content.exist('//p:Subreport') = 1 THEN 1 ELSE 0 END AS HasSubreports,
		CASE WHEN Content.exist('//p:Drillthrough') = 1 THEN 1 ELSE 0 END AS HasDrillthroughs,
		CASE WHEN Content.exist('//p:ToggleItem') = 1 THEN 1 ELSE 0 END AS HasToggles
          FROM      @ContentXML
		 WHERE CHARINDEX('http://schemas.microsoft.com/sqlserver/reporting/2008/01/reportdefinition', CAST(Content AS NVARCHAR(MAX))) > 0 
UNION
SELECT    ItemID , ReportName,
		CASE WHEN Content.exist('//p1:Chart') = 1 THEN 1 ELSE 0 END  ,
		CASE WHEN Content.exist('//p1:GaugePanel') = 1 THEN 1 ELSE 0 END ,
		CASE WHEN Content.exist('//p1:Map') = 1 THEN 1 ELSE 0 END ,
		CASE WHEN Content.exist('//p1:Subreport') = 1 THEN 1 ELSE 0 END ,
		CASE WHEN Content.exist('//p1:Drillthrough') = 1 THEN 1 ELSE 0 END ,
		CASE WHEN Content.exist('//p1:ToggleItem') = 1 THEN 1 ELSE 0 END 
          FROM      @ContentXML
WHERE CHARINDEX('http://schemas.microsoft.com/sqlserver/reporting/2010/01/reportdefinition', CAST(Content AS NVARCHAR(MAX))) > 0 

INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT DISTINCT
                2 ,
                50 ,
                'Performance' ,
                'Data Dumps in Disguise' ,
                'http://brentozar.com/BlitzRS/datadumps' ,
                'The "' + ReportName
                + '" report appears to be a data dump with no special SSRS features. If your server is under pressure, consider delivering this data through other means.'
        FROM    ExecutionLogStorage els WITH ( NOLOCK )
                JOIN Catalog C ON ( els.ReportID = C.ItemID )
                JOIN @ContentFeatures AS cf ON cf.ItemID = C.ItemID
        WHERE   [RowCount] >= 1000
                AND TimeDataRetrieval + TimeProcessing + TimeRendering > 5000
                AND ReportAction = 1
                AND RequestType IN ( 1, 2 )
                AND HasCharts = 0
                AND HasGauges = 0
                AND HasMaps = 0
                AND HasSubreports = 0
                AND HasDrillthroughs = 0
                AND HasToggles = 0;


/* 
-------------------------------------------------------------------
CHECK #3
Find reports needing a performance tune-up.
-------------------------------------------------------------------
*/

INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  3 AS CheckID ,
                60 AS [Priority] ,
                'Performance' AS FindingsGroup ,
                'Slow Datasets' AS Finding ,
                'http://brentozar.com/BlitzRS/slowdataset' AS URL ,
                'The "' + c.Path
                + '" report waits an average of 5+ seconds for query data to come back. There may be a query here that needs tuning. ' AS Details
        FROM    ExecutionLogStorage AS els
                JOIN Catalog AS c ON ( els.ReportID = c.ItemID )
        WHERE   ReportAction = 1
                AND RequestType IN ( 1, 2 )
        GROUP BY c.Path
        HAVING  AVG(TimeDataRetrieval) > 5000


/* 
-------------------------------------------------------------------
CHECK #4
Find reports that used to run but have failed from a certain point
forward.
-------------------------------------------------------------------
*/
;
WITH    cte
          AS ( SELECT   ReportID ,
                        c.Name AS ReportName ,
                        MAX(CASE WHEN [Status] = 'rsSuccess' THEN TimeStart
                            END) AS LastKnownSuccess
               FROM     dbo.ExecutionLogStorage AS els
                        JOIN dbo.Catalog AS c ON c.ItemID = els.ReportID
               GROUP BY ReportID ,
                        c.Name
             )
    INSERT  #BlitzRSResults
            ( CheckID ,
              [Priority] ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  4 AS CheckID ,
                    40 AS [Priority] ,
                    'Reliability' AS FindingsGroup ,
                    '"I''ve fallen and I can''t get up!"' AS Finding ,
                    'http://brentozar.com/BlitzRS/reportdown' AS URL ,
                    'The "' + cte.ReportName
                    + '" report last ran successfully at '
                    + CAST(cte.LastKnownSuccess AS VARCHAR(24))
                    + '. Since then, it''s had '
                    + CAST(COUNT([status]) AS VARCHAR(20))
                    + ' failed executions.' AS Details
            FROM    dbo.ExecutionLogStorage AS els
                    JOIN cte ON cte.ReportID = els.ReportID
                                AND els.TimeStart >= DATEADD(dd,
                                                             @DeadReportThreshold,
                                                             cte.LastKnownSuccess)
            WHERE   Status != 'rsSuccess'
            GROUP BY cte.ReportName ,
                    cte.LastKnownSuccess

/* 
-------------------------------------------------------------------
CHECK #5
Find altered role permissions and affected users.
-------------------------------------------------------------------
*/

IF OBJECT_ID('tempdb..#RoleDefault') IS NOT NULL
    DROP TABLE #RoleDefault;

CREATE TABLE #RoleDefault
    (
      RoleName NVARCHAR(32) ,
      RoleFlags TINYINT ,
      TaskMask NVARCHAR(32)
    )
INSERT  #RoleDefault
        ( RoleName, RoleFlags, TaskMask )
VALUES  ( N'Browser', 0, N'0010101001000100' )
		,
        ( N'Content Manager', 0, N'1111111111111111' )
		,
        ( N'Model Item Browser', 2, N'1' )
		,
        ( N'My Reports', 0, N'0111111111011000' )
		,
        ( N'Publisher', 0, N'0101010100001010' )
		,
        ( N'Report Builder', 0, N'0010101001000101' )
		,
        ( N'System Administrator', 1, N'110101011' )
		,
        ( N'System User', 1, N'001010001' )

IF OBJECT_ID('tempdb..#RoleDefaultTaskMask') IS NOT NULL
    DROP TABLE #RoleDefaultTaskMask;

CREATE TABLE #RoleDefaultTaskMask
    (
      RoleName NVARCHAR(32) ,
      RoleFlags TINYINT ,
      TaskMaskPos TINYINT ,
      TaskMaskValue TINYINT
    )
INSERT  #RoleDefaultTaskMask
        ( RoleName ,
          RoleFlags ,
          TaskMaskPos ,
          TaskMaskValue
        )
        SELECT  RoleName ,
                RoleFlags ,
                upvt.TaskMaskPos ,
                upvt.TaskMask AS TaskMaskValue
        FROM    ( SELECT    RoleName ,
                            RoleFlags ,
                            CAST(SUBSTRING(TaskMask, 1, 1) AS CHAR(1)) AS [1] ,
                            CAST(SUBSTRING(TaskMask, 2, 1) AS CHAR(1)) AS [2] ,
                            CAST(SUBSTRING(TaskMask, 3, 1) AS CHAR(1)) AS [3] ,
                            CAST(SUBSTRING(TaskMask, 4, 1) AS CHAR(1)) AS [4] ,
                            CAST(SUBSTRING(TaskMask, 5, 1) AS CHAR(1)) AS [5] ,
                            CAST(SUBSTRING(TaskMask, 6, 1) AS CHAR(1)) AS [6] ,
                            CAST(SUBSTRING(TaskMask, 7, 1) AS CHAR(1)) AS [7] ,
                            CAST(SUBSTRING(TaskMask, 8, 1) AS CHAR(1)) AS [8] ,
                            CAST(SUBSTRING(TaskMask, 9, 1) AS CHAR(1)) AS [9] ,
                            CAST(SUBSTRING(TaskMask, 10, 1) AS CHAR(1)) AS [10] ,
                            CAST(SUBSTRING(TaskMask, 11, 1) AS CHAR(1)) AS [11] ,
                            CAST(SUBSTRING(TaskMask, 12, 1) AS CHAR(1)) AS [12] ,
                            CAST(SUBSTRING(TaskMask, 13, 1) AS CHAR(1)) AS [13] ,
                            CAST(SUBSTRING(TaskMask, 14, 1) AS CHAR(1)) AS [14] ,
                            CAST(SUBSTRING(TaskMask, 15, 1) AS CHAR(1)) AS [15] ,
                            CAST(SUBSTRING(TaskMask, 16, 1) AS CHAR(1)) AS [16]
                  FROM      #RoleDefault
                ) AS r UNPIVOT
( TaskMask FOR TaskMaskPos IN ( [1], [2], [3], [4], [5], [6], [7], [8], [9],
                                [10], [11], [12], [13], [14], [15], [16] ) ) AS upvt
        WHERE   TaskMask != ' '

IF OBJECT_ID('tempdb..#RolePermissions') IS NOT NULL
    DROP TABLE #RolePermissions;

CREATE TABLE #RolePermissions
    (
      RoleFlag TINYINT ,
      TaskMaskPos TINYINT ,
      Permission NVARCHAR(80)
    )
INSERT  #RolePermissions
        ( RoleFlag, TaskMaskPos, Permission )
VALUES  ( 0, 1, N'Set security for individual items' )
		,
        ( 0, 2, N'Create linked reports' )
		,
        ( 0, 3, N'View reports' )
		,
        ( 0, 4, N'Manage reports' )
		,
        ( 0, 5, N'View resources' )
		,
        ( 0, 6, N'Manage resources' )
		,
        ( 0, 7, N'View folders' )
		,
        ( 0, 8, N'Manage folders' )
		,
        ( 0, 9, N'Manage report history' )
		,
        ( 0, 10, N'Manage individual subscriptions' )
		,
        ( 0, 11, N'Manage all subscriptions' )
		,
        ( 0, 12, N'View data sources' )
		,
        ( 0, 13, N'Manage data sources' )
		,
        ( 0, 14, N'View models' )
		,
        ( 0, 15, N'Manage models' )
		,
        ( 0, 16, N'Consume reports' )
		,
        ( 1, 1, N'Manage roles' )
		,
        ( 1, 2, N'Manage report server security' )
		,
        ( 1, 3, N'View report server properties' )
		,
        ( 1, 4, N'Manage report server properties' )
		,
        ( 1, 5, N'View shared schedules' )
		,
        ( 1, 6, N'Manage shared schedules' )
		,
        ( 1, 7, N'Generate events' )
		,
        ( 1, 8, N'Manage jobs' )
		,
        ( 1, 9, N'Execute Report Definitions' )

IF OBJECT_ID('tempdb..#RoleTaskMask') IS NOT NULL
    DROP TABLE #RoleTaskMask;

CREATE TABLE #RoleTaskMask
    (
      RoleName NVARCHAR(32) ,
      RoleFlags TINYINT ,
      TaskMaskPos TINYINT ,
      TaskMaskValue TINYINT
    )

INSERT  #RoleTaskMask
        ( RoleName ,
          RoleFlags ,
          TaskMaskPos ,
          TaskMaskValue
        )
        SELECT  RoleName ,
                RoleFlags ,
                upvt.TaskMaskPos ,
                upvt.TaskMask AS TaskMaskValue
        FROM    ( SELECT    RoleName ,
                            RoleFlags ,
                            SUBSTRING(TaskMask, 1, 1) AS [1] ,
                            SUBSTRING(TaskMask, 2, 1) AS [2] ,
                            SUBSTRING(TaskMask, 3, 1) AS [3] ,
                            SUBSTRING(TaskMask, 4, 1) AS [4] ,
                            SUBSTRING(TaskMask, 5, 1) AS [5] ,
                            SUBSTRING(TaskMask, 6, 1) AS [6] ,
                            SUBSTRING(TaskMask, 7, 1) AS [7] ,
                            SUBSTRING(TaskMask, 8, 1) AS [8] ,
                            SUBSTRING(TaskMask, 9, 1) AS [9] ,
                            SUBSTRING(TaskMask, 10, 1) AS [10] ,
                            SUBSTRING(TaskMask, 11, 1) AS [11] ,
                            SUBSTRING(TaskMask, 12, 1) AS [12] ,
                            SUBSTRING(TaskMask, 13, 1) AS [13] ,
                            SUBSTRING(TaskMask, 14, 1) AS [14] ,
                            SUBSTRING(TaskMask, 15, 1) AS [15] ,
                            SUBSTRING(TaskMask, 16, 1) AS [16]
                  FROM      dbo.Roles
                ) AS r UNPIVOT
( TaskMask FOR TaskMaskPos IN ( [1], [2], [3], [4], [5], [6], [7], [8], [9],
                                [10], [11], [12], [13], [14], [15], [16] ) ) AS upvt
        WHERE   TaskMask != ' '

INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  4 AS CheckID ,
                20 AS [Priority] ,
                'Security' AS FindingsGroup ,
                'Non-Default Role Permissions' AS Finding ,
                'http://brentozar.com/BlitzRS/roledefaults' AS URL ,
                'The user ' + u.UserName + ' has '
                + CASE WHEN r.TaskMaskValue = 0 THEN 'been denied '
                       ELSE ''
                  END + 'permission to "' + rp.Permission + '" because the '
                + r.RoleName
                + ' role''s default security settings have been changed.' AS Details
        FROM    #RoleTaskMask AS r
                JOIN #RoleDefaultTaskMask AS rd ON rd.RoleName = r.RoleName
                                                   AND rd.RoleFlags = r.RoleFlags
                                                   AND rd.TaskMaskPos = r.TaskMaskPos
                JOIN #RolePermissions AS rp ON rp.RoleFlag = r.RoleFlags
                                               AND rp.TaskMaskPos = r.TaskMaskPos
                JOIN dbo.Roles AS ro ON ro.RoleName COLLATE DATABASE_DEFAULT = r.RoleName
                LEFT JOIN dbo.PolicyUserRole AS pur ON ro.RoleID = pur.RoleID
                LEFT JOIN dbo.Users AS u ON pur.UserID = u.UserID
        WHERE   r.TaskMaskValue != rd.TaskMaskValue

/* 
-------------------------------------------------------------------
CHECK #6
Find accounts with administrator-like privileges.
-------------------------------------------------------------------
*/

INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT DISTINCT
                5 AS CheckID ,
                30 AS [Priority] ,
                'Security' AS FindingsGroup ,
                'Accounts with admin privileges' AS Finding ,
                'http://brentozar.com/BlitzRS/admins' AS URL ,
                'The user ' + u.UserName + ' belongs to the ' + ro.RoleName
                + ' role. This role has some administrator-like privileges related to '
                + CASE WHEN ro.RoleName = 'Content Manager'
                       THEN 'reports, subscriptions, and datasets.'
                       WHEN ro.RoleName = 'System Administrator'
                       THEN 'schedules and security.'
                       ELSE 'something, but I'' not sure what.'
                  END AS Details
        FROM    dbo.Roles AS ro
                LEFT JOIN dbo.PolicyUserRole AS pur ON ro.RoleID = pur.RoleID
                LEFT JOIN dbo.Users AS u ON pur.UserID = u.UserID
        WHERE   ro.RoleName IN ( 'Content Manager', 'System Administrator' )

/* ------------------------------------------------------- */
/* SECTION IS FOR LATER USE */


--DECLARE @CatalogParams TABLE
--    (
--      ItemID UNIQUEIDENTIFIER ,
--	  ParamName NVARCHAR(80),
--	  DefaultValue NVARCHAR(80)
--    );
--DECLARE @Catalog TABLE
--    (
--      ItemID UNIQUEIDENTIFIER ,
--      ParamField XML
--    );

--INSERT  @Catalog
--        ( ItemID ,
--		  ParamField 
--        )
--        SELECT  ItemID ,
--                CAST([Parameter] AS XML)
--        FROM    dbo.Catalog
--		WHERE [Type] = 2;
--		;
--INSERT @CatalogParams
--        ( ItemID, ParamName, DefaultValue )
--SELECT    ItemID ,
--					AOP.Params.value('Name[1]', 'Varchar(800)') AS 'PName',
--					AOP.Params.value('DefaultValues[1]/Value[1]', 'Varchar(800)') AS 'PVal'
--          FROM      @Catalog
--          CROSS APPLY ParamField.nodes('//Parameters/Parameter') AS AOP ( Params )

--SELECT * FROM @CatalogParams

/* END SECTION */
/*---------------------------------------------------------*/

/* 
-------------------------------------------------------------------
CHECK #7
Find reports that may do better with snapshots.
-------------------------------------------------------------------
*/
;
WITH    cte
          AS ( SELECT   COUNT(LogEntryId) AS RenderCount ,
                        ReportID 
               FROM     dbo.ExecutionLogStorage AS els
               WHERE    TimeDataRetrieval + TimeProcessing + TimeDataRetrieval >= 5000
			   			AND RequestType IN (0, 1)
						AND els.[Format] IS NOT NULL
               GROUP BY ReportID 
             )
    INSERT  #BlitzRSResults
            ( CheckID ,
              [Priority] ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT
			  7 AS CheckID ,
                    70 AS [Priority] ,
                    'Performance' AS FindingsGroup ,
                    'Candidates for snapshots' AS Finding ,
                    'http://brentozar.com/BlitzRS/snapshots' AS URL ,
                    'When the ' + c.[Name] + ' report runs over 5 seconds, '
                    + CAST(CAST(( COUNT(els.LogEntryId) * 100.0 )
                    / ( cte.RenderCount * 1.0 ) AS DECIMAL(9, 0)) AS NVARCHAR(3))
                    + '% of the time it uses a distinct format and set of parameters. This makes it a candidate for rendering from a snapshot.'
            FROM    dbo.ExecutionLogStorage AS els
                    JOIN cte ON cte.ReportID = els.ReportID
                    JOIN dbo.Catalog AS c ON c.itemid = els.ReportID
            WHERE   TimeDataRetrieval + TimeProcessing + TimeDataRetrieval >= 5000
                    AND cte.RenderCount >= 20
					AND els.RequestType IN (0, 1)
					AND els.[Format] IS NOT NULL
            GROUP BY c.[Name] ,
                    CAST(els.[Parameters] AS NVARCHAR(2000)) ,
                    cte.RenderCount,
					els.[Format]
            HAVING  CAST(( COUNT(els.LogEntryId) * 100.0 ) / ( cte.RenderCount
                                                              * 1.0 ) AS DECIMAL(9,
                                                              0)) > 20
            ORDER BY COUNT(LogEntryId) DESC

/* 
-------------------------------------------------------------------
CHECK #8
Find reports that may be worth caching.
-------------------------------------------------------------------
*/

;
WITH    cte
          AS ( SELECT   ReportID ,
                        COUNT(LogEntryId) AS RenderCount
               FROM     dbo.ExecutionLogStorage
               WHERE    RequestType IN ( 1, 2 )
               GROUP BY ReportID
             )
    INSERT  #BlitzRSResults
            ( CheckID ,
              [Priority] ,
              FindingsGroup ,
              Finding ,
              URL ,
              Details
            )
            SELECT  8 AS CheckID ,
                    80 AS [Priority] ,
                    'Performance' AS FindingsGroup ,
                    'Candidates for caching' AS Finding ,
                    'http://brentozar.com/BlitzRS/caching' AS URL ,
                    'When the ' + c.[Name] + ' report runs, '
                    + CAST(CAST(( COUNT(els.LogEntryId) * 100.0 )
                    / ( cte.RenderCount * 1.0 ) AS DECIMAL(9, 0)) AS NVARCHAR(3))
                    + '% of the time it starts during the '
                    + CAST(DATEPART(hh, TimeStart) AS NVARCHAR(12))
                    + ':00 hour. Caching these reports can improve performance during this hour.'
            FROM    dbo.ExecutionLogStorage AS els
                    JOIN cte ON cte.ReportID = els.ReportID
                    JOIN dbo.Catalog AS c ON c.itemid = els.ReportID
            WHERE   RequestType IN ( 1, 2 )
                    AND cte.RenderCount >= 20
            GROUP BY c.[Name] ,
                    DATEPART(hh, TimeStart) ,
                    cte.RenderCount
            HAVING  CAST(( COUNT(els.LogEntryId) * 100.0 ) / ( cte.RenderCount
                                                              * 1.0 ) AS DECIMAL(9,
                                                              0)) >= 10

/* 
-------------------------------------------------------------------
CHECK #9
See when the ReportServer database was last backed up.
-------------------------------------------------------------------
*/

INSERT  INTO #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  10 AS CheckID ,
                10 AS [PRIORITY] ,
                'Backup' AS FindingsGroup ,
                'Backups Not Performed Recently' AS Finding ,
                'http://brentozar.com/BlitzRS/nobackup' AS URL ,
                'Database ' + d.Name + ' last backed up: '
                + CAST(COALESCE(MAX(b.backup_finish_date), ' never ') AS VARCHAR(200)) AS Details
        FROM    master.sys.databases d
                LEFT OUTER JOIN msdb.dbo.backupset b ON d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
                                                        AND b.type = 'D'
                                                        AND b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server */
        WHERE   d.database_id <> 2  /* Bonus points if you know what that means */
                AND d.state <> 1 /* Not currently restoring, like log shipping databases */
                AND d.is_in_standby = 0 /* Not a log shipping target database */
                AND d.source_database_id IS NULL /* Excludes database snapshots */
                AND d.name = 'ReportServer'
		/*
		The above NOT IN filters out the databases we're not supposed to check.
		*/
        GROUP BY d.name
        HAVING  MAX(b.backup_finish_date) <= DATEADD(dd, -7, GETDATE());

/* 
-------------------------------------------------------------------
CHECK #10
Find who has a report subscription where the recipient is explicitly
stated (not in a data-driven result set).
-------------------------------------------------------------------
*/

IF OBJECT_ID('tempdb..#numbers') IS NOT NULL
    DROP TABLE #numbers;
IF OBJECT_ID('tempdb..#parms') IS NOT NULL
    DROP TABLE #parms;

/* Create a numbers table to cycle through number positions */

CREATE TABLE #numbers ( n SMALLINT )

DECLARE @x SMALLINT = 0
DECLARE @sql NVARCHAR(MAX)

SET @sql = N'INSERT #numbers VALUES '
WHILE @x < 800
    BEGIN
        SET @sql = @sql + '(' + CAST(@x AS VARCHAR(3)) + '),';
        SET @x = @x + 1
    END

SET @sql = @sql + '(' + CAST(@x AS VARCHAR(3)) + ')';

/* Populate the numbers table up to 800 */
EXEC sp_executesql @sql;

/* Fill a parameters table by changing ExtensionSettings to XML,
then parsing it, taking only the {to, cc, bcc} set.

*/

CREATE TABLE #parms
    (
      SubscriptionID UNIQUEIDENTIFIER ,
      parm NVARCHAR(3200) ,
      pos SMALLINT
    );
WITH    cte
          AS ( SELECT   SubscriptionID ,
                        CASE WHEN RIGHT(s.value('(Value/text())[1]',
                                                'nvarchar(800)'), 1) = ';'
                             THEN LEFT(s.value('(Value/text())[1]',
                                               'nvarchar(800)'),
                                       LEN(s.value('(Value/text())[1]',
                                                   'nvarchar(800)')) - 1)
                             ELSE s.value('(Value/text())[1]', 'nvarchar(800)')
                        END AS parm
               FROM     ( SELECT    SubscriptionID ,
                                    CAST(ExtensionSettings AS XML) AS ExtSettings
                          FROM      dbo.Subscriptions
                          WHERE     DeliveryExtension = 'Report Server Email'
                        ) AS subs
                        CROSS APPLY subs.ExtSettings.nodes('/ParameterValues/ParameterValue')
                        AS SubsX ( s )
               WHERE    s.value('(Name/text())[1]', 'nvarchar(800)') IN ( 'TO',
                                                              'CC', 'BCC' )
             )
    INSERT  #parms
            ( SubscriptionID ,
              parm ,
              pos
            )
            SELECT DISTINCT
                    cte.SubscriptionID ,
                    parm ,
                    n AS pos
            FROM    cte
                    CROSS JOIN #numbers
            WHERE   ( CHARINDEX(';', parm, n) = n
                      OR ( CHARINDEX(';', parm, n) = 0
                           AND n = 0
                         )
                      OR n = 0
                    )
                    AND LEN(parm) != n 

INSERT  #BlitzRSResults
        ( CheckID ,
          [Priority] ,
          FindingsGroup ,
          Finding ,
          URL ,
          Details
        )
        SELECT  10 AS CheckID ,
                150 AS [Priority] ,
                'Informational' AS FindingsGroup ,
                'E-mail accounts with subscriptions' AS Finding ,
                'http://brentozar.com/BlitzRS/emailsubs' AS URL ,
                'The e-mail address "'
                + CASE WHEN pos = 0
                            AND CHARINDEX(';', parm, pos) > 0
                       THEN SUBSTRING(parm, pos + 1,
                                      CHARINDEX(';', parm, pos) - 1)
                       WHEN pos = 0
                            AND CHARINDEX(';', parm, pos) = 0 THEN parm
                       ELSE SUBSTRING(parm, pos + 1, CHARINDEX(';', parm, pos))
                  END + '" is included in '
                + CAST(COUNT(p.SubscriptionID) AS NVARCHAR(8))
                + ' subscriptions'
                + CASE WHEN SUM(CASE WHEN s.DataSettings IS NULL THEN 0
                                     ELSE 1
                                END) > 0
                       THEN ', including at least one data-driven subscription'
                       ELSE ''
                  END + '.'
        FROM    #parms AS p
                JOIN dbo.Subscriptions AS s ON s.SubscriptionID = p.SubscriptionID
                JOIN dbo.Catalog AS c ON c.ItemID = s.Report_OID
                JOIN dbo.ReportSchedule AS rs ON rs.ReportID = s.Report_OID
                                                 AND rs.SubscriptionID = s.SubscriptionID
                JOIN dbo.Schedule AS sh ON sh.ScheduleID = rs.ScheduleID
        GROUP BY CASE WHEN pos = 0
                           AND CHARINDEX(';', parm, pos) > 0
                      THEN SUBSTRING(parm, pos + 1,
                                     CHARINDEX(';', parm, pos) - 1)
                      WHEN pos = 0
                           AND CHARINDEX(';', parm, pos) = 0 THEN parm
                      ELSE SUBSTRING(parm, pos + 1, CHARINDEX(';', parm, pos))
                 END
        ORDER BY SUM(CASE WHEN s.DataSettings IS NULL THEN 0
                          ELSE 1
                     END) DESC ,
                CASE WHEN pos = 0
                          AND CHARINDEX(';', parm, pos) > 0
                     THEN SUBSTRING(parm, pos + 1,
                                    CHARINDEX(';', parm, pos) - 1)
                     WHEN pos = 0
                          AND CHARINDEX(';', parm, pos) = 0 THEN parm
                     ELSE SUBSTRING(parm, pos + 1, CHARINDEX(';', parm, pos))
                END


				/* Add credits for the nice folks who put so much time into building and maintaining this for free: */
				INSERT  INTO #BlitzRSResults
						( CheckID ,
						  Priority ,
						  FindingsGroup ,
						  Finding ,
						  URL ,
						  Details
						)
				VALUES  ( -1 ,
						  255 ,
						  'Thanks!' ,
						  'From Brent Ozar Unlimited' ,
						  'http://www.BrentOzar.com/blitzrs/' ,
						  'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
						);



SELECT  [Priority] ,
        FindingsGroup ,
        Finding ,
        URL ,
        Details,
		CheckID
FROM    #BlitzRSResults
ORDER BY [Priority] ,
        FindingsGroup ,
        Finding


IF @WhoGetsWhat = 1
	SELECT 
	p.SubscriptionID
	, s.[Description] as SubscriptionName
	, c.[Name] as ReportName
	, sh.Name as ScheduleName
	, sh.LastRunTime
	, s.LastStatus
	, sh.NextRunTime
	, CASE WHEN pos = 0 AND CHARINDEX(';', parm, pos) > 0 THEN SUBSTRING(parm, pos + 1, CHARINDEX(';', parm, pos) -1)
		 WHEN pos = 0 AND CHARINDEX(';', parm, pos) = 0 THEN parm
		 ELSE SUBSTRING(parm, pos + 1, CHARINDEX(';', parm, pos)) 
		 END AS EmailAddress
	, CASE WHEN s.DataSettings IS NULL THEN 0 ELSE 1 END AS IsDataDrivenSubscription
	FROM #parms as p
	JOIN dbo.Subscriptions as s on s.SubscriptionID = p.SubscriptionID
	join dbo.Catalog as c on c.ItemID = s.Report_OID
	JOIN dbo.ReportSchedule as rs on rs.ReportID = s.Report_OID AND rs.SubscriptionID = s.SubscriptionID
	join dbo.Schedule as sh on sh.ScheduleID = rs.ScheduleID
	ORDER BY EmailAddress, p.SubscriptionID  

END

