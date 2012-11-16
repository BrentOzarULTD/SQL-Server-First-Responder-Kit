--Capture for a given table:
--Number of read operations
--Number OF WRITE operations (INSERT + UPDATE + DELETE commands-- not the number of rows impacted)
--Number of rows in the table
--Size of the table
--Be able to run from a central database and collect to it
--Be able to run against a db in 80 compat mode
--Does not have to support table partitioning

USE admin;
SET NOCOUNT ON;
GO

---------------------------------------
--1. CREATE TABLES
---------------------------------------
BEGIN
--DROP TABLE dbo.index_info;
--DROP TABLE dbo.index_info_runs;

    IF OBJECT_ID('dbo.index_info_runs') IS NULL 
        CREATE TABLE dbo.index_info_runs (
            [index_info_run_id] BIGINT IDENTITY
                                       NOT NULL ,
            [collection_time] DATETIME2(0) DEFAULT SYSDATETIME()
                                           NOT NULL );

    IF OBJECT_ID('pk_index_info_runs_index_info_run_id') IS NULL 
        BEGIN
            ALTER TABLE dbo.index_info_runs ADD CONSTRAINT pk_index_info_runs_index_info_run_id PRIMARY KEY CLUSTERED ( index_info_run_id );
        END

    IF OBJECT_ID('dbo.index_info') IS NULL 
        BEGIN
            CREATE TABLE dbo.index_info (
                [index_info_id] BIGINT IDENTITY
                                       NOT NULL ,
                [index_info_run_id] BIGINT NOT NULL ,
                [database_id] SMALLINT NOT NULL ,
                [object_id] INT NOT NULL ,
                [index_id] INT NOT NULL ,
                [database_name] NVARCHAR(256) NOT NULL ,
                [schema_name] NVARCHAR(256) NOT NULL ,
                [object_name] NVARCHAR(256) NOT NULL ,
                index_name NVARCHAR(256) NULL ,
                is_unique BIT NOT NULL ,
                is_primary_key BIT NOT NULL ,
                is_disabled BIT NOT NULL ,
                is_hypothetical BIT NOT NULL ,
                is_padded BIT NOT NULL ,
                fill_factor SMALLINT NOT NULL ,
                user_seeks BIGINT NOT NULL ,
                user_scans BIGINT NOT NULL ,
                user_lookups BIGINT NOT  NULL ,
                user_updates BIGINT NULL ,
                last_user_seek DATETIME NULL ,
                last_user_scan DATETIME NULL ,
                last_user_lookup DATETIME NULL ,
                last_user_update DATETIME NULL ,
                row_count BIGINT NOT NULL ,
                reserved_MB NUMERIC(10, 2) NOT NULL ,
                reserved_LOB_MB NUMERIC(10, 2) NOT NULL ,
                reserved_row_overflow_MB NUMERIC(10, 2) NOT NULL ,
                leaf_insert_count BIGINT NULL ,
                leaf_delete_count BIGINT NULL ,
                leaf_update_count BIGINT NULL ,
                forwarded_fetch_count BIGINT NULL ,
                lob_fetch_in_pages BIGINT NULL ,
                lob_fetch_in_bytes BIGINT NULL ,
                row_overflow_fetch_in_pages BIGINT NULL ,
                row_overflow_fetch_in_bytes BIGINT NULL ,
                row_lock_count BIGINT NULL ,
                row_lock_wait_count BIGINT NULL ,
                row_lock_wait_in_ms BIGINT NULL ,
                page_lock_count BIGINT NULL ,
                page_lock_wait_count BIGINT NULL ,
                page_lock_wait_in_ms BIGINT NULL ,
                index_lock_promotion_attempt_count BIGINT NULL ,
                index_lock_promotion_count BIGINT NULL
                 );	
        END

    IF OBJECT_ID('pk_index_info_index_info_id') IS NULL 
        BEGIN
            ALTER TABLE dbo.index_info ADD CONSTRAINT pk_index_info_index_info_id PRIMARY KEY CLUSTERED ( index_info_id );
        END
END

GO
