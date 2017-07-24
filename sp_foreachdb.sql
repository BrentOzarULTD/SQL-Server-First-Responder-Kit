USE [master];
GO

IF OBJECT_ID('dbo.sp_foreachdb') IS  NULL
    EXEC ('CREATE PROCEDURE dbo.sp_foreachdb AS RETURN 0');
GO

ALTER PROCEDURE dbo.sp_foreachdb
    @command NVARCHAR(MAX) ,
    @replace_character NCHAR(1) = N'?' ,
    @print_dbname BIT = 0 ,
    @print_command_only BIT = 0 ,
    @suppress_quotename BIT = 0 ,
    @system_only BIT = NULL ,
    @user_only BIT = NULL ,
    @name_pattern NVARCHAR(300) = N'%' ,
    @database_list NVARCHAR(MAX) = NULL ,
    @exclude_list NVARCHAR(MAX) = NULL ,
    @recovery_model_desc NVARCHAR(120) = NULL ,
    @compatibility_level TINYINT = NULL ,
    @state_desc NVARCHAR(120) = N'ONLINE' ,
    @is_read_only BIT = 0 ,
    @is_auto_close_on BIT = NULL ,
    @is_auto_shrink_on BIT = NULL ,
    @is_broker_enabled BIT = NULL
AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @sql NVARCHAR(MAX) ,
            @dblist NVARCHAR(MAX) ,
            @exlist NVARCHAR(MAX) ,
            @db NVARCHAR(300) ,
            @i INT;

        IF @database_list > N''
            BEGIN
       ;
                WITH    n ( n )
                          AS ( SELECT   ROW_NUMBER() OVER ( ORDER BY s1.name )
                                        - 1
                               FROM     sys.objects AS s1
                                        CROSS JOIN sys.objects AS s2
                             )
                    SELECT  @dblist = REPLACE(REPLACE(REPLACE(x, '</x><x>',
                                                              ','), '</x>', ''),
                                              '<x>', '')
                    FROM    ( SELECT DISTINCT
                                        x = 'N'''
                                        + LTRIM(RTRIM(SUBSTRING(@database_list,
                                                              n,
                                                              CHARINDEX(',',
                                                              @database_list
                                                              + ',', n) - n)))
                                        + ''''
                              FROM      n
                              WHERE     n <= LEN(@database_list)
                                        AND SUBSTRING(',' + @database_list, n,
                                                      1) = ','
                            FOR
                              XML PATH('')
                            ) AS y ( x );
            END
-- Added for @exclude_list
        IF @exclude_list > N''
            BEGIN
       ;
                WITH    n ( n )
                          AS ( SELECT   ROW_NUMBER() OVER ( ORDER BY s1.name )
                                        - 1
                               FROM     sys.objects AS s1
                                        CROSS JOIN sys.objects AS s2
                             )
                    SELECT  @exlist = REPLACE(REPLACE(REPLACE(x, '</x><x>',
                                                              ','), '</x>', ''),
                                              '<x>', '')
                    FROM    ( SELECT DISTINCT
                                        x = 'N'''
                                        + LTRIM(RTRIM(SUBSTRING(@exclude_list,
                                                              n,
                                                              CHARINDEX(',',
                                                              @exclude_list
                                                              + ',', n) - n)))
                                        + ''''
                              FROM      n
                              WHERE     n <= LEN(@exclude_list)
                                        AND SUBSTRING(',' + @exclude_list, n,
                                                      1) = ','
                            FOR
                              XML PATH('')
                            ) AS y ( x );
            END

        CREATE TABLE #x ( db NVARCHAR(300) );

        SET @sql = N'SELECT name FROM sys.databases WHERE 1=1'
            + CASE WHEN @system_only = 1 THEN ' AND database_id IN (1,2,3,4)'
                   ELSE ''
              END
            + CASE WHEN @user_only = 1
                   THEN ' AND database_id NOT IN (1,2,3,4)'
                   ELSE ''
              END
-- To exclude databases from changes	
            + CASE WHEN @exlist IS NOT NULL
                   THEN ' AND name NOT IN (' + @exlist + ')'
                   ELSE ''
              END + CASE WHEN @name_pattern <> N'%'
                         THEN ' AND name LIKE N''%' + REPLACE(@name_pattern,
                                                              '''', '''''')
                              + '%'''
                         ELSE ''
                    END + CASE WHEN @dblist IS NOT NULL
                               THEN ' AND name IN (' + @dblist + ')'
                               ELSE ''
                          END
            + CASE WHEN @recovery_model_desc IS NOT NULL
                   THEN ' AND recovery_model_desc = N'''
                        + @recovery_model_desc + ''''
                   ELSE ''
              END
            + CASE WHEN @compatibility_level IS NOT NULL
                   THEN ' AND compatibility_level = '
                        + RTRIM(@compatibility_level)
                   ELSE ''
              END
            + CASE WHEN @state_desc IS NOT NULL
                   THEN ' AND state_desc = N''' + @state_desc + ''''
                   ELSE ''
              END
            + CASE WHEN @is_read_only IS NOT NULL
                   THEN ' AND is_read_only = ' + RTRIM(@is_read_only)
                   ELSE ''
              END
            + CASE WHEN @is_auto_close_on IS NOT NULL
                   THEN ' AND is_auto_close_on = ' + RTRIM(@is_auto_close_on)
                   ELSE ''
              END
            + CASE WHEN @is_auto_shrink_on IS NOT NULL
                   THEN ' AND is_auto_shrink_on = ' + RTRIM(@is_auto_shrink_on)
                   ELSE ''
              END
            + CASE WHEN @is_broker_enabled IS NOT NULL
                   THEN ' AND is_broker_enabled = ' + RTRIM(@is_broker_enabled)
                   ELSE ''
              END;

        INSERT  #x
                EXEC sp_executesql @sql;

        DECLARE c CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY
        FOR
            SELECT  CASE WHEN @suppress_quotename = 1 THEN db
                         ELSE QUOTENAME(db)
                    END
            FROM    #x
            ORDER BY db;

        OPEN c;

        FETCH NEXT FROM c INTO @db;

        WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql = REPLACE(@command, @replace_character, @db);

                IF @print_command_only = 1
                    BEGIN
                        PRINT '/* For ' + @db + ': */' + CHAR(13) + CHAR(10)
                            + CHAR(13) + CHAR(10) + @sql + CHAR(13) + CHAR(10)
                            + CHAR(13) + CHAR(10);
                    END
                ELSE
                    BEGIN
                        IF @print_dbname = 1
                            BEGIN
                                PRINT '/* ' + @db + ' */';
                            END

                        EXEC sp_executesql @sql;
                    END

                FETCH NEXT FROM c INTO @db;
            END

        CLOSE c;
        DEALLOCATE c;
    END
GO
