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
SELECT 'sp_ineachdb' as ProcedureName

--End Variables

IF (@allDatabases = 0)
BEGIN

    SELECT @SQL += N'DROP PROCEDURE dbo.' + D.ProcedureName + ';' + CHAR(10)
    FROM sys.procedures P
    JOIN #ToDelete D ON D.ProcedureName = P.name COLLATE DATABASE_DEFAULT;

    SELECT @SQL += N'DROP TABLE dbo.SqlServerVersions;' + CHAR(10)
    FROM sys.tables 
    WHERE schema_id = 1 AND name = 'SqlServerVersions';

END
ELSE
BEGIN

    DECLARE @dbname SYSNAME;
    DECLARE @innerSQL NVARCHAR(max);

    DECLARE c CURSOR LOCAL FAST_FORWARD
    FOR SELECT QUOTENAME([name])
    FROM sys.databases
    WHERE [state] = 0;

    OPEN c;

    FETCH NEXT FROM c INTO @dbname;

    WHILE(@@FETCH_STATUS = 0)
    BEGIN

        SET @innerSQL = N'    SELECT @SQL += N''USE  ' + @dbname + N';' + NCHAR(10) + N'DROP PROCEDURE dbo.'' + D.ProcedureName + '';'' + NCHAR(10)
        FROM ' + @dbname + N'.sys.procedures P
        JOIN #ToDelete D ON D.ProcedureName = P.name COLLATE DATABASE_DEFAULT';

        EXEC sp_executesql @innerSQL, N'@SQL nvarchar(max) OUTPUT', @SQL = @SQL OUTPUT;

        SET @innerSQL = N'    SELECT @SQL += N''USE  ' + @dbname + N';' + NCHAR(10) + N'DROP TABLE dbo.SqlServerVersions;'' + NCHAR(10)
        FROM ' + @dbname + N'.sys.tables
        WHERE schema_id = 1 AND name = ''SqlServerVersions''';

        EXEC sp_executesql @innerSQL, N'@SQL nvarchar(max) OUTPUT', @SQL = @SQL OUTPUT;

        FETCH NEXT FROM c INTO @dbname;
    
    END

    CLOSE c;
    DEALLOCATE c;

END

PRINT @SQL;

IF(@printOnly = 0)
    EXEC sp_executesql @SQL
