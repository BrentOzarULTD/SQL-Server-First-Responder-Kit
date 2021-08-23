SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

IF OBJECT_ID('dbo.sp_ClusterInfo') IS NULL
  EXEC ('CREATE PROCEDURE dbo.sp_ClusterInfo AS RETURN 0;');
GO

ALTER PROCEDURE dbo.sp_ClusterInfo
    @Help TINYINT = 0
       --@Debug BIT = 0,
--   @Version     VARCHAR(30) = NULL OUTPUT,
       --@VersionDate DATETIME = NULL OUTPUT,
--   @VersionCheckMode BIT = 0
WITH RECOMPILE
AS
SET NOCOUNT ON;
SET STATISTICS XML OFF;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

--SELECT @Version = '8.05', @VersionDate = '20210725';
--SET @OutputType  = UPPER(@OutputType);

--IF(@VersionCheckMode = 1)
--BEGIN
--     RETURN;
--END;

IF @Help = 1 
BEGIN
PRINT '
/*
sp_ClusterInfo from http://FirstResponderKit.org
       
This script collect and show the cluster information of the database server.
This has been tested to work on SQL Server 2008 R2 and higher.

Displays the following information.
1. Instance Name
2. Server (Cluster Node) Name/s
3. Is it Standalone or Clustered
4. If Clustered, which node is Active/Primary and Passive/Secondary
4. If Clustered, is it using Always On Availability Group (AG), Failover Cluster Instance (FCI) or combination of FCI and AG
5. If using Availability Group, it shows Availability Group Name and Listener Name.
6. SQL Server version and patch level
Shared by RATG.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
- Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
- Supports SQL Server 2008 R2 and higher

Unknown limitations of this version:
- We knew them once, but we forgot.


MIT License

Copyright (c) 2021 Brent Ozar Unlimited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
';
RETURN;
END;    /* @Help = 1 */

DECLARE @SQLServerMajorBuildVersion AS INT 
SELECT @SQLServerMajorBuildVersion = LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR), CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR))-1)

IF @SQLServerMajorBuildVersion < 11
BEGIN
       IF (SELECT SERVERPROPERTY('IsClustered')) = 1
       BEGIN
              SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName  
              ,UPPER(NodeName) AS ServerName
              ,SERVERPROPERTY('IsClustered') AS IsClustered
              ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
              ,NULL AS AvailabilityGroupName
              ,NULL AS AvailabilityGroupListenerName
              ,CASE UPPER(NodeName)
                     WHEN UPPER(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR)) THEN 'PRIMARY'
                     ELSE 'SECONDARY'
              END AS HighAvailabilityRoleDesc
              ,'FCI' AS SQLServerClusteringMethod
              ,@@VERSION AS SQLServerVersion  
              FROM sys.dm_os_cluster_nodes
              ORDER BY NodeName;  
       END
       ELSE
       BEGIN
              SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName  
              ,UPPER(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR)) AS ServerName
              ,SERVERPROPERTY('IsClustered') AS IsClustered
              ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
              ,NULL AS AvailabilityGroupName
              ,NULL AS AvailabilityGroupListenerName
              ,NULL AS HighAvailabilityRoleDesc
              ,'Standalone' AS SQLServerClusteringMethod  
              ,@@VERSION AS SQLServerVersion  
       END
END
ELSE
BEGIN
       IF (SELECT SERVERPROPERTY('IsHadrEnabled')) = 1 AND (SELECT SERVERPROPERTY('IsClustered')) = 1
       BEGIN
              SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName
              ,UPPER(ar.replica_server_name) AS ServerName
              ,SERVERPROPERTY('IsClustered') AS IsClustered
              ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
              ,ag.name AS AvailabilityGroupName
              ,agl.dns_name AS AvailabilityGroupListenerName
                       ,CASE ar.replica_server_name WHEN dhags.primary_replica THEN 'PRIMARY'
                       ELSE 'SECONDARY'
              END AS HighAvailabilityRoleDesc
              ,'FCI/AG' AS SQLServerClusteringMethod
              ,@@VERSION AS SQLServerVersion  
              FROM sys.availability_groups AS ag
              LEFT OUTER JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
              LEFT OUTER JOIN sys.availability_group_listeners AS agl ON ag.group_id = agl.group_id
                       LEFT OUTER JOIN sys.dm_hadr_availability_group_states as dhags ON ag.group_id = dhags.group_id
              WHERE UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) <> ar.replica_server_name
              UNION ALL
              SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName  
              ,UPPER(NodeName) AS ServerName
              ,SERVERPROPERTY('IsClustered') AS IsClustered
              ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
              ,NULL AS AvailabilityGroupName
              ,NULL AS AvailabilityGroupListenerName
              ,CASE is_current_owner 
                     WHEN 1 THEN 'PRIMARY'
                     ELSE 'SECONDARY'
              END AS HighAvailabilityRoleDesc
              ,'FCI/AG' AS SQLServerClusteringMethod
              ,@@VERSION AS SQLServerVersion  
              FROM sys.dm_os_cluster_nodes
              ORDER BY 5,2;  
       END
       ELSE
       BEGIN
              IF (SELECT SERVERPROPERTY('IsHadrEnabled')) = 1 
              BEGIN
				   SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName
				   ,UPPER(ar.replica_server_name) AS ServerName
				   ,SERVERPROPERTY('IsClustered') AS IsClustered
				   ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
				   ,ag.name AS AvailabilityGroupName
				   ,agl.dns_name AS AvailabilityGroupListenerName
				   ,CASE ar.replica_server_name WHEN dhags.primary_replica THEN 'PRIMARY'
				   ELSE 'SECONDARY'
				   END AS HighAvailabilityRoleDesc
				   ,'AG' AS SQLServerClusteringMethod
				   ,@@VERSION AS SQLServerVersion
				   FROM sys.availability_groups AS ag
				   LEFT OUTER JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
				   LEFT OUTER JOIN sys.availability_group_listeners AS agl ON ag.group_id = agl.group_id
				   LEFT OUTER JOIN sys.dm_hadr_availability_group_states as dhags ON ag.group_id = dhags.group_id
				   ORDER BY 5,2;  
              END
              ELSE 
              BEGIN
                     IF (SELECT SERVERPROPERTY('IsClustered')) = 1
                     BEGIN
                           SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName  
                           ,UPPER(NodeName) AS ServerName
                           ,SERVERPROPERTY('IsClustered') AS IsClustered
                           ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
                           ,NULL AS AvailabilityGroupName
                           ,NULL AS AvailabilityGroupListenerName
                           ,CASE is_current_owner 
                                  WHEN 1 THEN 'PRIMARY'
                                  ELSE 'SECONDARY'
                           END AS HighAvailabilityRoleDesc
                           ,'FCI' AS SQLServerClusteringMethod
                           ,@@VERSION AS SQLServerVersion  
                           FROM sys.dm_os_cluster_nodes
                           ORDER BY NodeName;  
                     END
                     ELSE
                     BEGIN
                           SELECT UPPER(CAST(SERVERPROPERTY('ServerName') AS VARCHAR)) AS InstanceName  
                           ,UPPER(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR)) AS ServerName
                           ,SERVERPROPERTY('IsClustered') AS IsClustered
                           ,SERVERPROPERTY ('IsHadrEnabled') AS IsAvailabilityGroupEnabled
                           ,NULL AS AvailabilityGroupName
                           ,NULL AS AvailabilityGroupListenerName
                           ,NULL AS HighAvailabilityRoleDesc
                           ,'Standalone' AS SQLServerClusteringMethod
                           ,@@VERSION AS SQLServerVersion;  
                     END
              END
       END
END;

