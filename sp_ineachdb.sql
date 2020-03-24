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
-- WITH EXECUTE AS OWNER – maybe not a great idea, depending on the security your system
AS
BEGIN
  SET NOCOUNT ON;

  SELECT @Version = '2.94', @VersionDate = '20200324';
  
IF(@VersionCheckMode = 1)
BEGIN
	RETURN;
END;
IF @Help = 1

	BEGIN
	
		PRINT '
		/*
			sp_ineachdb from http://FirstResponderKit.org
			
			This script will restore a database from a given file path.
		
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
			
			Copyright (c) 2019 Brent Ozar Unlimited
		
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
          @cr     char(2) = CHAR(13) + CHAR(10);

DECLARE @SQLVersion	AS tinyint = (@@microsoftversion / 0x1000000) & 0xff		-- Stores the SQL Server Version Number(8(2000),9(2005),10(2008 & 2008R2),11(2012),12(2014),13(2016),14(2017))
DECLARE @ServerName	AS sysname = CONVERT(sysname, SERVERPROPERTY('ServerName')) -- Stores the SQL Server Instance name.

  CREATE TABLE #ineachdb(id int, name nvarchar(512));

  IF @database_list > N''
  -- comma-separated list of potentially valid/invalid/quoted/unquoted names
  BEGIN
    ;WITH n(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM n WHERE n < 4000),
    names AS
    (
      SELECT name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@database_list, n, 
        CHARINDEX(N',', @database_list + N',', n) - n), 1)))
      FROM n 
      WHERE n <= LEN(@database_list)
        AND SUBSTRING(N',' + @database_list, n, 1) = N','
    ) 
    INSERT #ineachdb(id,name) 
    SELECT d.database_id, d.name
      FROM sys.databases AS d
      WHERE EXISTS (SELECT 1 FROM names WHERE name = d.name)
      OPTION (MAXRECURSION 0);
  END
  ELSE
  BEGIN
    INSERT #ineachdb(id,name) SELECT database_id, name FROM sys.databases;
  END

  -- first, let's delete any that have been explicitly excluded
  IF @exclude_list > N'' 
  -- comma-separated list of potentially valid/invalid/quoted/unquoted names
  -- exclude trumps include
  BEGIN
    ;WITH n(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM n WHERE n < 4000),
    names AS
    (
      SELECT name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@exclude_list, n, 
        CHARINDEX(N',', @exclude_list + N',', n) - n), 1)))
      FROM n 
      WHERE n <= LEN(@exclude_list)
        AND SUBSTRING(N',' + @exclude_list, n, 1) = N','
    )
    DELETE d 
      FROM #ineachdb AS d
      INNER JOIN names
      ON names.name = d.name
      OPTION (MAXRECURSION 0);
  END

  -- next, let's delete any that *don't* match various criteria passed in
  DELETE dbs FROM #ineachdb AS dbs
  WHERE (@system_only = 1 AND id NOT IN (1,2,3,4))
     OR (@user_only   = 1 AND id     IN (1,2,3,4))
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
if @SQLVersion >= 11
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
