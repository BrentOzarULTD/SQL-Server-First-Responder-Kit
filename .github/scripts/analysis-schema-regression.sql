USE FRKSmokeTest;
SET NOCOUNT ON;
IF SCHEMA_ID(N'FRKAnalysisSmoke') IS NULL EXEC(N'CREATE SCHEMA FRKAnalysisSmoke');
DROP TABLE IF EXISTS dbo.FRKSchemaHistory;
DROP TABLE IF EXISTS FRKAnalysisSmoke.FRKSchemaHistory;
CREATE TABLE dbo.FRKSchemaHistory
(ServerName nvarchar(128), CheckDate datetimeoffset, CheckID int, Priority int,
 FindingsGroup nvarchar(128), Finding nvarchar(128), URL nvarchar(256),
 Details nvarchar(max), HowToStopIt nvarchar(max), QueryPlan xml, QueryText nvarchar(max));
INSERT dbo.FRKSchemaHistory VALUES
(@@SERVERNAME, SYSDATETIMEOFFSET(), 1, 10, N'Test', N'FRK_DBO_SCHEMA_SENTINEL', NULL, NULL, NULL, NULL, NULL);
SELECT * INTO FRKAnalysisSmoke.FRKSchemaHistory FROM dbo.FRKSchemaHistory;
UPDATE FRKAnalysisSmoke.FRKSchemaHistory SET Finding = N'FRK_CUSTOM_SCHEMA_SENTINEL';
BEGIN TRY
    EXEC master.dbo.sp_BlitzAnalysis @OutputDatabaseName = N'FRKSmokeTest',
         @OutputSchemaName = $(SchemaArgument), @OutputTableNameBlitzFirst = N'FRKSchemaHistory',
         @OutputTableNameFileStats = NULL, @OutputTableNamePerfmonStats = NULL,
         @OutputTableNameWaitStats = NULL, @OutputTableNameBlitzCache = NULL,
         @OutputTableNameBlitzWho = NULL;
    DROP TABLE dbo.FRKSchemaHistory;
    DROP TABLE FRKAnalysisSmoke.FRKSchemaHistory;
    DROP SCHEMA FRKAnalysisSmoke;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS dbo.FRKSchemaHistory;
    DROP TABLE IF EXISTS FRKAnalysisSmoke.FRKSchemaHistory;
    DROP SCHEMA FRKAnalysisSmoke;
    THROW;
END CATCH;
