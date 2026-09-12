/* Dedicated test schema belongs to this serialized Azure CI workflow. */
SET NOCOUNT ON;
IF SCHEMA_ID(N'FRKIndexSmoke') IS NULL
    EXEC(N'CREATE SCHEMA FRKIndexSmoke AUTHORIZATION dbo');
DROP TABLE IF EXISTS FRKIndexSmoke.Mode0;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode1;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode3;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode4;
DROP TABLE IF EXISTS FRKIndexSmoke.Inventory;
DROP TABLE IF EXISTS FRKIndexSmoke.SourceData;
CREATE TABLE FRKIndexSmoke.SourceData (ID int NOT NULL PRIMARY KEY, Payload char(100));
INSERT FRKIndexSmoke.SourceData VALUES (1, 'a'), (2, 'b');
DECLARE @Database sysname = DB_NAME();
BEGIN TRY
    EXEC dbo.sp_BlitzIndex @Mode = 2, @DatabaseName = @Database,
         @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
         @OutputTableName = N'Inventory';
    IF OBJECT_ID(N'FRKIndexSmoke.Inventory') IS NULL
        THROW 51000, 'Azure index output table was not created.', 1;
    IF NOT EXISTS (SELECT 1 FROM FRKIndexSmoke.Inventory)
        THROW 51000, 'Azure index inventory is empty.', 1;
    DECLARE @Before bigint = (SELECT COUNT_BIG(*) FROM FRKIndexSmoke.Inventory);
    EXEC dbo.sp_BlitzIndex @Mode = 2, @DatabaseName = @Database,
         @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
         @OutputTableName = N'Inventory';
    IF (SELECT COUNT_BIG(*) FROM FRKIndexSmoke.Inventory) <= @Before
        THROW 51000, 'Azure index output did not append.', 1;

    DECLARE @Mode tinyint = 0, @Table sysname, @ObjectID int;
    WHILE @Mode <= 4
    BEGIN
        IF @Mode <> 2
        BEGIN
            SET @Table = CONCAT(N'Mode', @Mode);
            EXEC dbo.sp_BlitzIndex @Mode = @Mode, @DatabaseName = @Database,
                 @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
                 @OutputTableName = @Table;
            SET @ObjectID = OBJECT_ID(N'FRKIndexSmoke.' + QUOTENAME(@Table));
            IF @ObjectID IS NULL THROW 51000, 'Azure output mode did not create a table.', 1;
            EXEC dbo.sp_BlitzIndex @Mode = @Mode, @DatabaseName = @Database,
                 @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
                 @OutputTableName = @Table;
            IF OBJECT_ID(N'FRKIndexSmoke.' + QUOTENAME(@Table)) <> @ObjectID
               OR OBJECT_ID(N'FRKIndexSmoke.' + QUOTENAME(@Table)) IS NULL
                THROW 51000, 'Azure output mode did not reuse its table.', 1;
        END;
        SET @Mode += 1;
    END;

    ALTER TABLE FRKIndexSmoke.Inventory DROP COLUMN total_forwarded_fetch_count;
    SET @Before = (SELECT COUNT_BIG(*) FROM FRKIndexSmoke.Inventory);
    /* Omitted output database must default locally, including upgrades. */
    IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
        EXEC dbo.sp_BlitzIndex @Mode = 2, @DatabaseName = @Database,
             @OutputTableName = N'FRKIndexSmoke.Inventory';
    ELSE
        EXEC dbo.sp_BlitzIndex @Mode = 2, @DatabaseName = @Database,
             @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
             @OutputTableName = N'Inventory';
    IF COL_LENGTH(N'FRKIndexSmoke.Inventory', N'total_forwarded_fetch_count') IS NULL
        THROW 51000, 'Azure legacy output table was not upgraded.', 1;
    IF (SELECT COUNT_BIG(*) FROM FRKIndexSmoke.Inventory) <= @Before
        THROW 51000, 'Azure index upgrade did not append rows.', 1;

    IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
    BEGIN
        BEGIN TRY
            EXEC dbo.sp_BlitzIndex @Mode = 2, @OutputServerName = N'FRKInvalidServer',
                 @OutputDatabaseName = @Database, @OutputSchemaName = N'FRKIndexSmoke',
                 @OutputTableName = N'Inventory';
            THROW 51000, 'Azure remote output was accepted.', 1;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() <> 50000 OR ERROR_MESSAGE() NOT LIKE 'Azure SQL Database does not support @OutputServerName.%'
                THROW;
        END CATCH;
    END;
    DROP TABLE IF EXISTS FRKIndexSmoke.Mode0;
    DROP TABLE IF EXISTS FRKIndexSmoke.Mode1;
    DROP TABLE IF EXISTS FRKIndexSmoke.Mode3;
    DROP TABLE IF EXISTS FRKIndexSmoke.Mode4;
    DROP TABLE FRKIndexSmoke.Inventory;
    DROP TABLE FRKIndexSmoke.SourceData;
    DROP SCHEMA FRKIndexSmoke;
END TRY
BEGIN CATCH
    DROP TABLE IF EXISTS FRKIndexSmoke.Mode0;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode1;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode3;
DROP TABLE IF EXISTS FRKIndexSmoke.Mode4;
DROP TABLE IF EXISTS FRKIndexSmoke.Inventory;
    DROP TABLE IF EXISTS FRKIndexSmoke.SourceData;
    DROP SCHEMA FRKIndexSmoke;
    THROW;
END CATCH;
PRINT 'Azure index create/append/upgrade/rejection assertions passed.';
