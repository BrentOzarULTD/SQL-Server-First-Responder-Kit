--First Responder Kit Uninstaller Script

--Configuration Parameters

DECLARE @allDatabases bit = 0;  --Flip this bit to 1 if you want to uninstall the scripts from all the databases, not only the current one
DECLARE @printOnly bit = 0; --Flip this bit to 1 if you want to print the drop commands only without executing

--End Configuration
--Variables

SET NOCOUNT ON;
DECLARE @SQL nvarchar(max) = N'';

IF OBJECT_ID('tempdb.dbo.#ToDelete') IS NOT NULL
    DROP TABLE #ToDelete;

SELECT 'sp_AllNightLog' as ProcedureName INTO #ToDelete UNION
SELECT 'sp_AllNightLog_Setup' as ProcedureName UNION
SELECT 'sp_Blitz' as ProcedureName UNION
SELECT 'sp_BlitzAnalysis' as ProcedureName UNION
SELECT 'sp_BlitzBackups' as ProcedureName UNION
SELECT 'sp_BlitzCache' as ProcedureName UNION
SELECT 'sp_BlitzFirst' as ProcedureName UNION
SELECT 'sp_BlitzInMemoryOLTP' as ProcedureName UNION
SELECT 'sp_BlitzIndex' as ProcedureName UNION
SELECT 'sp_BlitzLock' as ProcedureName UNION
SELECT 'sp_BlitzQueryStore' as ProcedureName UNION
SELECT 'sp_BlitzWho' as ProcedureName UNION
SELECT 'sp_DatabaseRestore' as ProcedureName UNION
SELECT 'sp_foreachdb' as ProcedureName UNION
SELECT 'sp_ineachdb' as ProcedureName UNION
SELECT 'sp_kill' as ProcedureName

--End Variables

IF (@allDatabases = 0)
BEGIN

    SELECT @SQL += N'DROP PROCEDURE ' + QUOTENAME(SCHEMA_NAME(P.schema_id)) + N'.' + QUOTENAME(P.name) + ';' + CHAR(10)
    FROM sys.procedures P
    JOIN #ToDelete D ON D.ProcedureName = P.name COLLATE DATABASE_DEFAULT
    WHERE P.schema_id = 1;

    SELECT @SQL += N'DROP TABLE dbo.SqlServerVersions;' + CHAR(10)
    FROM sys.tables 
    WHERE schema_id = 1 AND name = 'SqlServerVersions';

END
ELSE
BEGIN

    DECLARE @dbname NVARCHAR(258);
    DECLARE @databaseName SYSNAME;
    DECLARE @innerSQL NVARCHAR(max);

    DECLARE c CURSOR LOCAL FAST_FORWARD
    FOR SELECT QUOTENAME([name]), [name]
    FROM sys.databases
    WHERE [state] = 0;

    OPEN c;

    FETCH NEXT FROM c INTO @dbname, @databaseName;

    WHILE(@@FETCH_STATUS = 0)
    BEGIN

        SET @innerSQL = N'USE ' + @dbname + N'; SELECT @SQL += N''USE '' + QUOTENAME(@databaseName) + N'';'' + NCHAR(10) + N''DROP PROCEDURE '' + QUOTENAME(S.name) + N''.'' + QUOTENAME(P.name) + N'';'' + NCHAR(10)
        FROM ' + @dbname + N'.sys.procedures P
        JOIN ' + @dbname + N'.sys.schemas S ON S.schema_id = P.schema_id
        JOIN #ToDelete D ON D.ProcedureName = P.name COLLATE DATABASE_DEFAULT
        WHERE P.schema_id = 1';

        EXEC sp_executesql @innerSQL, N'@SQL nvarchar(max) OUTPUT, @databaseName sysname', @SQL = @SQL OUTPUT, @databaseName = @databaseName;

        SET @innerSQL = N'USE ' + @dbname + N'; SELECT @SQL += N''USE '' + QUOTENAME(@databaseName) + N'';'' + NCHAR(10) + N''DROP TABLE dbo.SqlServerVersions;'' + NCHAR(10)
        FROM ' + @dbname + N'.sys.tables
        WHERE schema_id = 1 AND name = ''SqlServerVersions''';

        EXEC sp_executesql @innerSQL, N'@SQL nvarchar(max) OUTPUT, @databaseName sysname', @SQL = @SQL OUTPUT, @databaseName = @databaseName;

        FETCH NEXT FROM c INTO @dbname, @databaseName;
    
    END

    CLOSE c;
    DEALLOCATE c;

END

PRINT @SQL;

IF(@printOnly = 0)
    EXEC sp_executesql @SQL
