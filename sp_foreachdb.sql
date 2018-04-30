USE [master];
GO

IF OBJECT_ID('dbo.sp_foreachdb') IS NULL
    EXEC ('CREATE PROCEDURE dbo.sp_foreachdb AS RETURN 0');
GO

ALTER PROCEDURE dbo.sp_foreachdb
    -- Original fields from sp_MSforeachdb...
    @command1 NVARCHAR(MAX) = NULL,
    @replacechar NCHAR(1) = N'?' ,
    @command2 NVARCHAR(MAX) = NULL ,
    @command3 NVARCHAR(MAX) = NULL ,
    @precommand NVARCHAR(MAX) = NULL ,
    @postcommand NVARCHAR(MAX) = NULL ,
    -- Additional fields for our sp_foreachdb!
    @command NVARCHAR(MAX) = NULL,  --For backwards compatibility
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
    @is_broker_enabled BIT = NULL , 
	@VersionDate DATETIME = NULL OUTPUT
AS
    BEGIN
        SET NOCOUNT ON;
		DECLARE @Version VARCHAR(30);
		SET @Version = '2.5';
		SET @VersionDate = '20180501';

        IF ( (@command1 IS NOT NULL AND @command IS NOT NULL)
            OR (@command1 IS NULL AND @command IS NULL) )
        BEGIN
            RAISERROR('You must supply either @command1 or @command, but not both.',16,1);
            RETURN -1;
        END;

        SET @command1 = COALESCE(@command1,@command);

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

        SET @sql = N'SELECT name FROM sys.databases d WHERE 1=1'
            + CASE WHEN @system_only = 1 THEN ' AND d.database_id IN (1,2,3,4)'
                   ELSE ''
              END
            + CASE WHEN @user_only = 1
                   THEN ' AND d.database_id NOT IN (1,2,3,4)'
                   ELSE ''
              END
-- To exclude databases from changes	
            + CASE WHEN @exlist IS NOT NULL
                   THEN ' AND d.name NOT IN (' + @exlist + ')'
                   ELSE ''
              END + CASE WHEN @name_pattern <> N'%'
                         THEN ' AND d.name LIKE N''' + REPLACE(@name_pattern,
                                                              '''', '''''')
                              + ''''
                         ELSE ''
                    END + CASE WHEN @dblist IS NOT NULL
                               THEN ' AND d.name IN (' + @dblist + ')'
                               ELSE ''
                          END
            + CASE WHEN @recovery_model_desc IS NOT NULL
                   THEN ' AND d.recovery_model_desc = N'''
                        + @recovery_model_desc + ''''
                   ELSE ''
              END
            + CASE WHEN @compatibility_level IS NOT NULL
                   THEN ' AND d.compatibility_level = '
                        + RTRIM(@compatibility_level)
                   ELSE ''
              END
            + CASE WHEN @state_desc IS NOT NULL
                   THEN ' AND d.state_desc = N''' + @state_desc + ''''
                   ELSE ''
              END
            + CASE WHEN @state_desc = 'ONLINE' AND SERVERPROPERTY('IsHadrEnabled') = 1
                   THEN ' AND NOT EXISTS (SELECT 1 
                                          FROM sys.dm_hadr_database_replica_states drs 
                                          JOIN sys.availability_replicas ar
                                                ON ar.replica_id = drs.replica_id
                                          JOIN sys.dm_hadr_availability_group_states ags 
                                                ON ags.group_id = ar.group_id
                                          WHERE drs.database_id = d.database_id
                                          AND ar.secondary_role_allow_connections = 0
                                          AND ags.primary_replica <> @@SERVERNAME)'
                    ELSE ''
              END
            + CASE WHEN @is_read_only IS NOT NULL
                   THEN ' AND d.is_read_only = ' + RTRIM(@is_read_only)
                   ELSE ''
              END
            + CASE WHEN @is_auto_close_on IS NOT NULL
                   THEN ' AND d.is_auto_close_on = ' + RTRIM(@is_auto_close_on)
                   ELSE ''
              END
            + CASE WHEN @is_auto_shrink_on IS NOT NULL
                   THEN ' AND d.is_auto_shrink_on = ' + RTRIM(@is_auto_shrink_on)
                   ELSE ''
              END
            + CASE WHEN @is_broker_enabled IS NOT NULL
                   THEN ' AND d.is_broker_enabled = ' + RTRIM(@is_broker_enabled)
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
                SET @sql = REPLACE(@command1, @replacechar, @db);

                IF @suppress_quotename = 0 SET @sql = REPLACE(REPLACE(@sql,'[[','['),']]',']');

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
