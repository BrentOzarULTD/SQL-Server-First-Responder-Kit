IF OBJECT_ID('dbo.sp_ineachdb') IS NULL
    EXEC ('CREATE PROCEDURE dbo.sp_ineachdb AS RETURN 0')
GO

ALTER PROCEDURE [dbo].[sp_ineachdb]
  -- mssqltips.com/sqlservertip/5694/execute-a-command-in-the-context-of-each-database-in-sql-server--part-2/
  @command             nvarchar(max) = NULL,
  @replace_character   nchar(1) = N'?',
  @print_dbname        bit = 0,
  @select_dbname       bit = 0, 
  @print_command       bit = 0, 
  @print_command_only  bit = 0,
  @suppress_quotename  bit = 0, -- use with caution
  @system_only         bit = 0,
  @user_only           bit = 0,
  @name_pattern        nvarchar(300)  = N'%', 
  @database_list       nvarchar(max)  = NULL,
  @exclude_pattern     nvarchar(300)  = NULL,
  @exclude_list        nvarchar(max)  = NULL,
  @recovery_model_desc nvarchar(120)  = NULL,
  @compatibility_level tinyint        = NULL,
  @state_desc          nvarchar(120)  = N'ONLINE',
  @is_read_only        bit = 0,
  @is_auto_close_on    bit = NULL,
  @is_auto_shrink_on   bit = NULL,
  @is_broker_enabled   bit = NULL,
  @user_access         nvarchar(128)  = NULL, 
  @Help                BIT = 0,
  @Version             VARCHAR(30)    = NULL OUTPUT,
  @VersionDate         DATETIME       = NULL OUTPUT,
  @VersionCheckMode    BIT            = 0
-- WITH EXECUTE AS OWNER – maybe not a great idea, depending on the security of your system
AS
BEGIN
  SET NOCOUNT ON;
  SET STATISTICS XML OFF;

  SELECT @Version = '8.10', @VersionDate = '20220718';
  
  IF(@VersionCheckMode = 1)
  BEGIN
	RETURN;
  END;
  
  IF @Help = 1

	BEGIN
	
		PRINT '
		/*
			sp_ineachdb from http://FirstResponderKit.org
			
			This script will execute a command against multiple databases.
		
			To learn more, visit http://FirstResponderKit.org where you can download new
			versions for free, watch training videos on how it works, get more info on
			the findings, contribute your own code, and more.
		
			Known limitations of this version:
			 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
			 - Tastes awful with marmite.
		
			Unknown limitations of this version:
			 - None.  (If we knew them, they would be known. Duh.)
		
		     Changes - for the full list of improvements and fixes in this version, see:
		     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
		
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
		
		*/
		';
		
		RETURN -1;
	END

  DECLARE @exec   nvarchar(150),
          @sx     nvarchar(18) = N'.sys.sp_executesql',
          @db     sysname,
          @dbq    sysname,
          @cmd    nvarchar(max),
          @thisdb sysname,
          @cr     char(2) = CHAR(13) + CHAR(10),
		  @SQLVersion	AS tinyint = (@@microsoftversion / 0x1000000) & 0xff,	     -- Stores the SQL Server Version Number(8(2000),9(2005),10(2008 & 2008R2),11(2012),12(2014),13(2016),14(2017),15(2019)
		  @ServerName	AS sysname = CONVERT(sysname, SERVERPROPERTY('ServerName')), -- Stores the SQL Server Instance name.
		  @NoSpaces nvarchar(20) = N'%[^' + CHAR(9) + CHAR(32) + CHAR(10) + CHAR(13) + N']%'; --Pattern for PATINDEX


  CREATE TABLE #ineachdb(id int, name nvarchar(512), is_distributor bit);


/*
  -- first, let's limit to only DBs the caller is interested in
  IF @database_list > N''
  -- comma-separated list of potentially valid/invalid/quoted/unquoted names
  BEGIN
    ;WITH n(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM n WHERE n <= LEN(@database_list)),
    names AS
    (
      SELECT name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@database_list, n, 
        CHARINDEX(N',', @database_list + N',', n) - n), 1)))
      FROM n 
      WHERE SUBSTRING(N',' + @database_list, n, 1) = N','
    ) 
    INSERT #ineachdb(id,name,is_distributor) 
    SELECT d.database_id, d.name, d.is_distributor
      FROM sys.databases AS d
      WHERE EXISTS (SELECT 1 FROM names WHERE name = d.name)
      OPTION (MAXRECURSION 0);
  END
  ELSE
  BEGIN
    INSERT #ineachdb(id,name,is_distributor) SELECT database_id, name, is_distributor FROM sys.databases;
  END

  -- now delete any that have been explicitly excluded - exclude trumps include
  IF @exclude_list > N'' 
  -- comma-separated list of potentially valid/invalid/quoted/unquoted names
  BEGIN
    ;WITH n(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM n WHERE n <= LEN(@exclude_list)),
    names AS
    (
      SELECT name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@exclude_list, n, 
        CHARINDEX(N',', @exclude_list + N',', n) - n), 1)))
      FROM n 
      WHERE SUBSTRING(N',' + @exclude_list, n, 1) = N','
    )
    DELETE d 
      FROM #ineachdb AS d
      INNER JOIN names
      ON names.name = d.name
      OPTION (MAXRECURSION 0);
  END
*/

/*
@database_list and @exclude_list are are processed at the same time
1)Read the list searching for a comma or [
2)If we find a comma, save the name
3)If we find a [, we begin to accumulate the result until we reach closing ], (jumping over escaped ]]).
4)Finally, tabs, line breaks and spaces are removed from unquoted names
*/
WITH C
AS (SELECT V.SrcList
         , CAST('' AS nvarchar(MAX)) AS Name
         , V.DBList
         , 0 AS InBracket
         , 0 AS Quoted
    FROM (VALUES ('In', @database_list + ','), ('Out', @exclude_list + ',')) AS V (SrcList, DBList)
    UNION ALL
	SELECT C.SrcList									
--       , IIF(V.Found = '[', '', SUBSTRING(C.DBList, 1, V.Place - 1))/*remove initial [*/
         , CASE WHEN V.Found = '[' THEN '' ELSE SUBSTRING(C.DBList, 1, V.Place - 1)  END  /*remove initial [*/
         , STUFF(C.DBList, 1, V.Place, '')
--       , IIF(V.Found = '[', 1, 0)
		 ,Case WHEN V.Found = '[' THEN 1 ELSE 0 END
         , 0
    FROM C
        CROSS APPLY
    (   VALUES (PATINDEX('%[,[]%', C.DBList), SUBSTRING(C.DBList, PATINDEX('%[,[]%', C.DBList), 1))) AS V (Place, Found)
    WHERE C.DBList > ''
          AND C.InBracket = 0
    UNION ALL
    SELECT C.SrcList
--       , CONCAT(C.Name, SUBSTRING(C.DBList, 1, V.Place + W.DoubleBracket - 1)) /*Accumulates only one ] if escaped]] or none if end]*/ 
	     , ISNULL(C.Name,'') + ISNULL(SUBSTRING(C.DBList, 1, V.Place + W.DoubleBracket - 1),'') /*Accumulates only one ] if escaped]] or none if end]*/ 
         , STUFF(C.DBList, 1, V.Place + W.DoubleBracket, '')
         , W.DoubleBracket
         , 1
    FROM C
        CROSS APPLY (VALUES (CHARINDEX(']', C.DBList))) AS V (Place)
	--  CROSS APPLY (VALUES (IIF(SUBSTRING(C.DBList, V.Place + 1, 1) = ']', 1, 0))) AS W (DoubleBracket)
        CROSS APPLY (VALUES (CASE WHEN SUBSTRING(C.DBList, V.Place + 1, 1) = ']' THEN 1 ELSE 0 END)) AS W (DoubleBracket)							 				 
    WHERE C.DBList > ''
          AND C.InBracket = 1)
   , F
AS (SELECT C.SrcList
         , CASE WHEN C.Quoted = 0 THEN 
                SUBSTRING(C.Name, PATINDEX(@NoSpaces, Name), DATALENGTH (Name)/2 - PATINDEX(@NoSpaces, Name) - PATINDEX(@NoSpaces, REVERSE(Name))+2)
             ELSE C.Name END						   
 AS name
    FROM C
    WHERE C.InBracket = 0
          AND C.Name > '')																	 
INSERT #ineachdb(id,name,is_distributor)
SELECT d.database_id
     , d.name
     , d.is_distributor
FROM sys.databases AS d
WHERE (   EXISTS (SELECT NULL FROM F WHERE F.name = d.name AND F.SrcList = 'In')
          OR @database_list IS NULL)
      AND NOT EXISTS (SELECT NULL FROM F WHERE F.name = d.name AND F.SrcList = 'Out')
OPTION (MAXRECURSION 0);
;
  -- next, let's delete any that *don't* match various criteria passed in
  DELETE dbs FROM #ineachdb AS dbs
  WHERE (@system_only = 1 AND (id NOT IN (1,2,3,4) AND is_distributor <> 1))
     OR (@user_only   = 1 AND (id     IN (1,2,3,4) OR  is_distributor =  1))
     OR name NOT LIKE @name_pattern
     OR name LIKE @exclude_pattern
     OR EXISTS
     (
       SELECT 1 
         FROM sys.databases AS d
         WHERE d.database_id = dbs.id
         AND NOT
         (
           recovery_model_desc     = COALESCE(@recovery_model_desc, recovery_model_desc)
           AND compatibility_level = COALESCE(@compatibility_level, compatibility_level)
           AND is_read_only        = COALESCE(@is_read_only,        is_read_only)
           AND is_auto_close_on    = COALESCE(@is_auto_close_on,    is_auto_close_on)
           AND is_auto_shrink_on   = COALESCE(@is_auto_shrink_on,   is_auto_shrink_on)
           AND is_broker_enabled   = COALESCE(@is_broker_enabled,   is_broker_enabled)
         )
     );

  -- if a user access is specified, remove any that are NOT in that state
  IF @user_access IN (N'SINGLE_USER', N'MULTI_USER', N'RESTRICTED_USER')
  BEGIN
    DELETE #ineachdb WHERE 
      CONVERT(nvarchar(128), DATABASEPROPERTYEX(name, 'UserAccess')) <> @user_access;
  END  

  -- finally, remove any that are not *fully* online or we can't access
  DELETE dbs FROM #ineachdb AS dbs
  WHERE EXISTS
  (
    SELECT 1 FROM sys.databases
      WHERE database_id = dbs.id
      AND
      ( 
        @state_desc = N'ONLINE' AND
        (
          [state] & 992 <> 0         -- inaccessible
          OR state_desc <> N'ONLINE' -- not online
          OR HAS_DBACCESS(name) = 0  -- don't have access
          OR DATABASEPROPERTYEX(name, 'Collation') IS NULL -- not fully online. See "status" here:
          -- https://docs.microsoft.com/en-us/sql/t-sql/functions/databasepropertyex-transact-sql
        )
        OR (@state_desc <> N'ONLINE' AND state_desc <> @state_desc)
      )
  );

  -- from Andy Mallon / First Responders Kit. Make sure that if we're an 
  -- AG secondary, we skip any database where allow connections is off
  IF @SQLVersion >= 11 AND 3 = (SELECT COUNT(*) FROM sys.all_objects WHERE name IN('availability_replicas','dm_hadr_availability_group_states','dm_hadr_database_replica_states'))
  BEGIN
    DELETE dbs FROM #ineachdb AS dbs
    WHERE EXISTS
          (
            SELECT 1 FROM sys.dm_hadr_database_replica_states AS drs 
              INNER JOIN sys.availability_replicas AS ar
              ON ar.replica_id = drs.replica_id
              INNER JOIN sys.dm_hadr_availability_group_states ags 
              ON ags.group_id = ar.group_id
              WHERE drs.database_id = dbs.id
              AND ar.secondary_role_allow_connections = 0
              AND ags.primary_replica <> @ServerName
          );
  END

  -- Well, if we deleted them all...
  IF NOT EXISTS (SELECT 1 FROM #ineachdb)
  BEGIN
    RAISERROR(N'No databases to process.', 1, 0);
    RETURN;
  END

  -- ok, now, let's go through what we have left
  DECLARE dbs CURSOR LOCAL FAST_FORWARD
    FOR SELECT DB_NAME(id), QUOTENAME(DB_NAME(id))
    FROM #ineachdb;

  OPEN dbs;

  FETCH NEXT FROM dbs INTO @db, @dbq;

  DECLARE @msg1 nvarchar(512) = N'Could not run against %s : %s.',
          @msg2 nvarchar(max);

  WHILE @@FETCH_STATUS <> -1
  BEGIN
    SET @thisdb = CASE WHEN @suppress_quotename = 1 THEN @db ELSE @dbq END;
    SET @cmd = REPLACE(@command, @replace_character, REPLACE(@thisdb,'''',''''''));

    BEGIN TRY
      IF @print_dbname = 1
      BEGIN
        PRINT N'/* ' + @thisdb + N' */';
      END

      IF @select_dbname = 1
      BEGIN
        SELECT [ineachdb current database] = @thisdb;
      END

      IF 1 IN (@print_command, @print_command_only)
      BEGIN
        PRINT N'/* For ' + @thisdb + ': */' + @cr + @cr + @cmd + @cr + @cr;
      END

      IF COALESCE(@print_command_only,0) = 0
      BEGIN
        SET @exec = @dbq + @sx;
        EXEC @exec @cmd;
      END
    END TRY

    BEGIN CATCH
      SET @msg2 = ERROR_MESSAGE();
      RAISERROR(@msg1, 1, 0, @db, @msg2);
    END CATCH

    FETCH NEXT FROM dbs INTO @db, @dbq;
  END

  CLOSE dbs; 
  DEALLOCATE dbs;
END
GO
