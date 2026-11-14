/* Run after installing sp_kill in a disposable Azure SQL test database.
   This check never executes KILL. A connected user workload should remain visible. */
IF CONVERT(int, SERVERPROPERTY('EngineEdition')) <> 5
    THROW 51000, 'This regression requires Azure SQL Database.', 1;
IF OBJECT_ID(N'dbo.FRK_KillScopeRegression') IS NOT NULL
    THROW 51000, 'FRK_KillScopeRegression already exists; refusing to overwrite it.', 1;
DECLARE @OutputDatabase sysname = DB_NAME();
BEGIN TRY
    EXEC dbo.sp_kill @ExecuteKills = 'N', @OutputDatabaseName = @OutputDatabase,
        @OutputSchemaName = N'dbo', @OutputTableName = N'FRK_KillScopeRegression';
    IF OBJECT_ID(N'dbo.FRK_KillScopeRegression', N'U') IS NULL
        THROW 51000, 'sp_kill did not persist its captured sessions.', 1;
    EXEC(N'IF EXISTS (SELECT 1 FROM dbo.FRK_KillScopeRegression
                     WHERE database_name IS NULL OR database_name <> DB_NAME() OR session_id = @@SPID)
        THROW 51000, ''sp_kill captured a session outside the current database or its own session.'', 1;');
    DROP TABLE dbo.FRK_KillScopeRegression;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS dbo.FRK_KillScopeRegression;
    THROW;
END CATCH;
PRINT 'PASS: Azure sp_kill candidates are scoped to the current database.';
