SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

USE master;
GO

IF OBJECT_ID('dbo.sp_BlitzIndex') IS NULL 
	EXEC ('CREATE PROCEDURE dbo.sp_BlitzIndex AS RETURN 0;')
GO
EXEC sys.sp_MS_marksystemobject 'dbo.sp_BlitzIndex';
GO

ALTER PROCEDURE dbo.sp_BlitzIndex
	@database_name NVARCHAR(256) = null,
	@mode tinyint=0, /*0=diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail*/
	@schema_name NVARCHAR(256) = NULL, /*Requires table_name as well.*/
	@table_name NVARCHAR(256) = NULL,  /*Requires schema_name as well.*/
		/*Note:@mode doesn't matter if you're specifying schema_name and @table_name.*/
	@filter tinyint = 0 /* 0=no filter (default). 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB */
		/*Note:@filter doesn't do anything unless @mode=0*/
/*
sp_BlitzIndex (TM) v2.0 - April 8, 2013

(C) 2013, Brent Ozar Unlimited. 
See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

For help and how-to info, visit http://www.BrentOzar.com/BlitzIndex

Usage examples:
	Diagnose:
		EXEC dbo.sp_BlitzIndex @database_name='AdventureWorks';
	Return detail for a specific table:
		EXEC dbo.sp_BlitzIndex @database_name='AdventureWorks', @schema_name='Person', @table_name='Person';

Known limitations of this version:
 - Does not include FULLTEXT indexes. (A possibility in the future, let us know if you're interested.)
 - Index create statements are just to give you a rough idea of the syntax.
 --		Example: they do not include all the options the index may have been created with (padding, etc.)
 - Doesn't advise you about data modeling for clustered indexes and primary keys (primarily looks for signs of insanity.)
 - Found something? Let us know at help@brentozar.com.

CHANGE LOG (last five versions):
	May 14, 2013 (v2.0) - Added data types and max length to all columns (keys, includes, secret columns)
		Set sp_blitz to default to current DB if database_name is not specified when called
		Added @filter:  
			0=no filter (default)
			1=Don't throw low-usage warnings for objects with 0 reads (helpful for dev/non-production environments)
			2=Only report on objects >= 250MB (helps focus on larger indexes). Still runs a few database-wide checks as well.
		Added list of all columns and types in table for runs using: @database_name, @schema_name, @table_name
		Added count of total number of indexes a column is part of.
		Added check_id 25: Addicted to nullable columns. (All or all but one column is nullable.)
		Added check_id 66 and 67 to flag tables/indexes created within 1 week or modified within 48 hours.
		Added check_id 26: Wide tables (35+ cols or > 2000 non-LOB bytes).
		Added check_id 27: Addicted to strings. Looks for tables with 4 or more columns, of which all or all but one are string or LOB types.
		Added check_id 68: Identity columns within 30% of the end of range (tinyint, smallint, int) AND
			Negative identity seeds or identity increments <> 1
		Added check_id 69: Column collation does not match database collation
		Added check_id 70: Replicated columns. This identifies which columns are in at least one replication publication.
		Added check_id 71: Cascading updates or cascading deletes.
		Split check_id 40 into two checks: fillfactor on nonclustered indexes < 80%, fillfactor on clustered indexes < 90%
		Added check_id 33: Potential filtered indexes based on column names.
		Fixed bug where you couldn't see detailed view for indexed views. 
			(Ex: EXEC dbo.sp_BlitzIndex @database_name='AdventureWorks', @schema_name='Production', @table_name='vProductAndDescription';)
		Added four index usage columns to table detail output: last_user_seek, last_user_scan, last_user_lookup, last_user_update
		Modified check_id 24. This now looks for wide clustered indexes (> 3 columns OR > 16 bytes).
			Previously just simplistically looked for multiple column CX.
		Removed extra spacing (non-breaking) in more_info column.
		Fixed bug where create t-sql didn't include filter (for filtered indexes)
		Fixed formatting bug where "magic number" in table detail view didn't have commas
		Neatened up column names in result sets.
	April 8, 2013 (v1.5) - Fixed breaking bug for partitioned tables with > 10(ish) partitions
		Added schema_name to suggested create statement for PKs
		Handled "magic_benefit_number" values for missing indexes >= 922,337,203,685,477
		Added count of NC indexes to Index Hoarder: Multi-column clustered index finding
		Added link to EULA
		Simplified aggressive index checks (blocking). Multiple checks confused people more than it helped.
			Left only "Total lock wait time > 5 minutes (row + page)".
		Added CheckId 25 for non-unique clustered indexes. 
		The "Create TSQL" column now shows a commented out drop command for disabled non-clustered indexes
		Updated query which joins to sys.dm_operational_stats DMV when running against 2012 for performance reasons
	December 20, 2012 (v1.4) - Fixed bugs for instances using a case-sensitive collation
		Added support to identify compressed indexes
		Added basic support for columnstore, XML, and spatial indexes
		Added "Abnormal Psychology" diagnosis to alert you to special index types in a database
		Removed hypothetical indexes and disabled indexes from "multiple personality disorders"
		Fixed bug where hypothetical indexes weren't showing up in "self-loathing indexes"
		Fixed bug where the partitioning key column was displayed in the key of aligned nonclustered indexes on partitioned tables
		Added set options to the script so procedure is created with required settings for its use of computed columns
	November 20, 2012 - @mode=2 now only returns index definition and usage. Added @mode=3 to return
		missing index data detail only.
	November 13, 2012 - Added secret_columns. This column shows key and included columns in 
		non-clustered indexes that are based on whether the NC index is unique AND whether the base table is 
		a heap, a unique clustered index, or a non-unique clustered index.
		Changed parameter order so @database_name is first. Some people were confused.
*/
AS 

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


DECLARE	@database_id INT;
DECLARE @object_id INT;
DECLARE	@dsql NVARCHAR(MAX);
DECLARE @params NVARCHAR(MAX);
DECLARE	@msg NVARCHAR(4000);
DECLARE	@ErrorSeverity INT;
DECLARE	@ErrorState INT;
DECLARE	@Rowcount BIGINT;
DECLARE @SQLServerProductVersion NVARCHAR(128);
DECLARE @SQLServerEdition INT;
DECLARE @filterMB INT;
DECLARE @collation NVARCHAR(256);

SELECT @SQLServerProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @SQLServerEdition =CAST(SERVERPROPERTY('EngineEdition') AS INT); /* We default to online index creates where EngineEdition=3*/
SET @filterMB=250;

IF @database_name is null 
	SET @database_name=DB_NAME();

SELECT	@database_id = database_id
FROM	sys.databases
WHERE	[name] = @database_name
	AND user_access_desc='MULTI_USER'
	AND state_desc = 'ONLINE';

----------------------------------------
--STEP 1: OBSERVE THE PATIENT
--This step puts index information into temp tables.
----------------------------------------
BEGIN TRY
	BEGIN

		--Validate SQL Server Verson

		IF (SELECT LEFT(@SQLServerProductVersion,
			  CHARINDEX('.',@SQLServerProductVersion,0)-1
			  )) <= 8
		BEGIN
			SET @msg=N'sp_BlitzIndex is only supported on SQL Server 2005 and higher. The version of this instance is: ' + @SQLServerProductVersion;
			RAISERROR(@msg,16,1);
		END

		--Short circuit here if database name does not exist.
		IF @database_name IS NULL OR @database_id IS NULL
		BEGIN
			SET @msg='Database does not exist or is not online/multi-user: cannot proceed.'
			RAISERROR(@msg,16,1);
		END    

		--Validate parameters.
		IF (@mode NOT IN (0,1,2,3))
		BEGIN
			SET @msg=N'Invalid @mode parameter. 0=diagnose, 1=summarize, 2=index detail, 3=missing index detail';
			RAISERROR(@msg,16,1);
		END

		IF (@mode <> 0 AND @table_name IS NOT NULL)
		BEGIN
			SET @msg=N'Setting the @mode doesn''t change behavior if you supply @table_name. Use default @mode=0 to see table detail.';
			RAISERROR(@msg,16,1);
		END

		IF ((@mode <> 0 OR @table_name IS NOT NULL) and @filter <> 0)
		BEGIN
			SET @msg=N'@filter only appies when @mode=0 and @table_name is not specified. Please try again.';
			RAISERROR(@msg,16,1);
		END

		IF (@schema_name IS NOT NULL AND @table_name IS NULL) OR (@table_name IS NOT NULL AND @schema_name IS NULL)
		BEGIN
			SET @msg='You must specify both @schema_name and @table_name, or leave both NULL for summary info.'
			RAISERROR(@msg,16,1);
		END

		--If a table is specified, grab the object id.
		--Short circuit if it doesn't exist.
		IF @table_name IS NOT NULL
		BEGIN
			SET @dsql = N'
					SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
					SELECT	@object_id= OBJECT_ID
					FROM	' + QUOTENAME(@database_name) + N'.sys.objects AS so
					JOIN	' + QUOTENAME(@database_name) + N'.sys.schemas AS sc on 
						so.schema_id=sc.schema_id
					where so.type in (''U'', ''V'')
					and so.name=' + QUOTENAME(@table_name,'''')+ N'
					and sc.name=' + QUOTENAME(@schema_name,'''')+ N'
					/*Has a row in sys.indexes. This lets us get indexed views.*/
					and exists (
						SELECT si.name
						FROM ' + QUOTENAME(@database_name) + '.sys.indexes AS si 
						WHERE so.object_id=si.object_id)
					OPTION (RECOMPILE);';

			SET @params='@object_id INT OUTPUT'				

			IF @dsql IS NULL 
				RAISERROR('@dsql is null',16,1);

			EXEC sp_executesql @dsql, @params, @object_id=@object_id OUTPUT;
			
			IF @object_id IS NULL
					BEGIN
						SET @msg='Table or indexed view does not exist in specified database, please check parameters.'
						RAISERROR(@msg,16,1);
					END
		END

		RAISERROR(N'Starting run. sp_BlitzIndex version 2.0 (May 15, 2013)', 0,1) WITH NOWAIT;

		IF OBJECT_ID('tempdb..#index_sanity') IS NOT NULL 
			DROP TABLE #index_sanity;

		IF OBJECT_ID('tempdb..#index_partition_sanity') IS NOT NULL 
			DROP TABLE #index_partition_sanity;

		IF OBJECT_ID('tempdb..#index_sanity_size') IS NOT NULL 
			DROP TABLE #index_sanity_size;

		IF OBJECT_ID('tempdb..#index_columns') IS NOT NULL 
			DROP TABLE #index_columns;

		IF OBJECT_ID('tempdb..#missing_indexes') IS NOT NULL 
			DROP TABLE #missing_indexes;

		IF OBJECT_ID('tempdb..#foreign_keys') IS NOT NULL 
			DROP TABLE #foreign_keys;

		IF OBJECT_ID('tempdb..#blitz_index_results') IS NOT NULL 
			DROP TABLE #blitz_index_results;
		
		IF OBJECT_ID('tempdb..#index_create_tsql') IS NOT NULL	
			DROP TABLE #index_create_tsql;

		RAISERROR (N'Create temp tables.',0,1) WITH NOWAIT;
		CREATE TABLE #blitz_index_results
			(
			  blitz_result_id INT IDENTITY PRIMARY KEY,
			  check_id INT NOT NULL,
			  index_sanity_id INT NULL,
			  findings_group VARCHAR(50) NOT NULL,
			  finding VARCHAR(200) NOT NULL,
			  URL VARCHAR(200) NOT NULL,
			  details NVARCHAR(4000) NOT NULL,
			  index_definition NVARCHAR(MAX) NOT NULL,
			  secret_columns NVARCHAR(MAX) NULL,
			  index_usage_summary NVARCHAR(MAX) NULL,
			  index_size_summary NVARCHAR(MAX) NULL,
			  create_tsql NVARCHAR(MAX) NULL,
			  more_info NVARCHAR(MAX)NULL
			);

		CREATE TABLE #index_sanity
			(
			  [index_sanity_id] INT IDENTITY PRIMARY KEY,
			  [database_id] SMALLINT NOT NULL ,
			  [object_id] INT NOT NULL ,
			  [index_id] INT NOT NULL ,
			  [index_type] TINYINT NOT NULL,
			  [database_name] NVARCHAR(256) NOT NULL ,
			  [schema_name] NVARCHAR(256) NOT NULL ,
			  [object_name] NVARCHAR(256) NOT NULL ,
			  index_name NVARCHAR(256) NULL ,
			  key_column_names NVARCHAR(MAX) NULL ,
			  key_column_names_with_sort_order NVARCHAR(MAX) NULL ,
			  key_column_names_with_sort_order_no_types NVARCHAR(MAX) NULL ,
			  count_key_columns INT NULL ,
			  include_column_names NVARCHAR(MAX) NULL ,
			  include_column_names_no_types NVARCHAR(MAX) NULL ,
			  count_included_columns INT NULL ,
			  partition_key_column_name NVARCHAR(MAX) NULL,
			  filter_definition NVARCHAR(MAX) NOT NULL ,
			  is_indexed_view BIT NOT NULL ,
			  is_unique BIT NOT NULL ,
			  is_primary_key BIT NOT NULL ,
			  is_XML BIT NOT NULL,
			  is_spatial BIT NOT NULL,
			  is_NC_columnstore BIT NOT NULL,
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
			  is_referenced_by_foreign_key BIT DEFAULT(0),
			  secret_columns NVARCHAR(MAX) NULL,
			  count_secret_columns INT NULL,
			  create_date DATETIME NOT NULL,
			  modify_date DATETIME NOT NULL
			);	

		CREATE TABLE #index_partition_sanity
			(
			  [index_partition_sanity_id] INT IDENTITY PRIMARY KEY ,
			  [index_sanity_id] INT NULL ,
			  [object_id] INT NOT NULL ,
			  [index_id] INT NOT NULL ,
			  [partition_number] INT NOT NULL ,
			  row_count BIGINT NOT NULL ,
			  reserved_MB NUMERIC(29,2) NOT NULL ,
			  reserved_LOB_MB NUMERIC(29,2) NOT NULL ,
			  reserved_row_overflow_MB NUMERIC(29,2) NOT NULL ,
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
			  index_lock_promotion_count BIGINT NULL,
  			  data_compression_desc VARCHAR(60) NULL
			);

		CREATE TABLE #index_sanity_size
			(
			  [index_sanity_size_id] INT IDENTITY NOT NULL ,
			  [index_sanity_id] INT NOT NULL ,
			  partition_count INT NOT NULL ,
			  total_rows BIGINT NOT NULL ,
			  total_reserved_MB NUMERIC(29,2) NOT NULL ,
			  total_reserved_LOB_MB NUMERIC(29,2) NOT NULL ,
			  total_reserved_row_overflow_MB NUMERIC(29,2) NOT NULL ,
			  total_row_lock_count BIGINT NULL ,
			  total_row_lock_wait_count BIGINT NULL ,
			  total_row_lock_wait_in_ms BIGINT NULL ,
			  avg_row_lock_wait_in_ms BIGINT NULL ,
			  total_page_lock_count BIGINT NULL ,
			  total_page_lock_wait_count BIGINT NULL ,
			  total_page_lock_wait_in_ms BIGINT NULL ,
			  avg_page_lock_wait_in_ms BIGINT NULL ,
 			  total_index_lock_promotion_attempt_count BIGINT NULL ,
			  total_index_lock_promotion_count BIGINT NULL ,
			  data_compression_desc VARCHAR(8000) NULL
			);

		CREATE TABLE #index_columns
			(
			  [object_id] INT NOT NULL ,
			  [index_id] INT NOT NULL ,
			  [key_ordinal] INT NULL ,
			  is_included_column BIT NULL ,
			  is_descending_key BIT NULL ,
			  [partition_ordinal] INT NULL ,
			  column_name NVARCHAR(256) NOT NULL ,
			  system_type_name NVARCHAR(256) NOT NULL,
			  max_length SMALLINT NOT NULL,
			  [precision] TINYINT NOT NULL,
			  [scale] TINYINT NOT NULL,
			  collation_name NVARCHAR(256) NULL,
			  is_nullable bit NULL,
			  is_identity bit NULL,
			  is_computed bit NULL,
			  is_replicated bit NULL,
			  is_sparse bit NULL,
			  is_filestream bit NULL,
			  seed_value BIGINT NULL,
			  increment_value INT NULL ,
			  last_value BIGINT NULL,
			  is_not_for_replication BIT NULL
			);

		CREATE TABLE #missing_indexes
			([object_id] INT NOT NULL,
			[database_name] NVARCHAR(256) NOT NULL ,
			[schema_name] NVARCHAR(256) NOT NULL ,
			[table_name] NVARCHAR(256),
			[statement] NVARCHAR(512) NOT NULL,
			magic_benefit_number AS (( user_seeks + user_scans ) * avg_total_user_cost * avg_user_impact),
			avg_total_user_cost NUMERIC(29,1) NOT NULL,
			avg_user_impact NUMERIC(29,1) NOT NULL,
			user_seeks BIGINT NOT NULL,
			user_scans BIGINT NOT NULL,
			unique_compiles BIGINT NULL,
			equality_columns NVARCHAR(4000), 
			inequality_columns NVARCHAR(4000),
			included_columns NVARCHAR(4000)
			);

		CREATE TABLE #foreign_keys (
			foreign_key_name NVARCHAR(256),
			parent_object_id INT,
			parent_object_name NVARCHAR(256),
			referenced_object_id INT,
			referenced_object_name NVARCHAR(256),
			is_disabled BIT,
			is_not_trusted BIT,
			is_not_for_replication BIT,
			parent_fk_columns NVARCHAR(MAX),
			referenced_fk_columns NVARCHAR(MAX),
			update_referential_action_desc NVARCHAR(16),
			delete_referential_action_desc NVARCHAR(60)
		)
		
		CREATE TABLE #index_create_tsql (
			index_sanity_id INT NOT NULL,
			create_tsql NVARCHAR(MAX) NOT NULL
		)

		--set @collation
		SELECT @collation=collation_name
		FROM sys.databases
		where database_id=@database_id;

		--insert columns for clustered indexes and heaps
		--collect info on identity columns for this one
		SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	
					si.object_id, 
					si.index_id, 
					sc.key_ordinal, 
					sc.is_included_column, 
					sc.is_descending_key,
					sc.partition_ordinal,
					c.name as column_name, 
					st.name as system_type_name,
					c.max_length,
					c.[precision],
					c.[scale],
					c.collation_name,
					c.is_nullable,
					c.is_identity,
					c.is_computed,
					c.is_replicated,
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_sparse' else N'NULL as is_sparse' END + N',
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_filestream' else N'NULL as is_filestream' END + N',
					CAST(ic.seed_value AS BIGINT),
					CAST(ic.increment_value AS INT),
					CAST(ic.last_value AS BIGINT),
					ic.is_not_for_replication
				FROM	' + QUOTENAME(@database_name) + N'.sys.indexes si
				JOIN	' + QUOTENAME(@database_name) + N'.sys.columns c ON
					si.object_id=c.object_id
				LEFT JOIN ' + QUOTENAME(@database_name) + N'.sys.index_columns sc ON 
					sc.object_id = si.object_id
					and sc.index_id=si.index_id
					AND sc.column_id=c.column_id
				LEFT JOIN sys.identity_columns ic ON
					c.object_id=ic.object_id and
					c.column_id=ic.column_id
				JOIN ' + QUOTENAME(@database_name) + N'.sys.types st ON 
					c.system_type_id=st.system_type_id
					AND c.user_type_id=st.user_type_id
				WHERE si.index_id in (0,1) ' 
					+ CASE WHEN @object_id IS NOT NULL 
						THEN N' AND si.object_id=' + CAST(@object_id AS NVARCHAR(30)) 
					ELSE N'' END 
				+ N';';

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #index_columns for clustered indexes and heaps',0,1) WITH NOWAIT;
		INSERT	#index_columns ( object_id, index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
			column_name, system_type_name, max_length, precision, scale, collation_name, is_nullable, is_identity, is_computed,
			is_replicated, is_sparse, is_filestream, seed_value, increment_value, last_value, is_not_for_replication )
				EXEC sp_executesql @dsql;

		--insert columns for nonclustered indexes
		--this uses a full join to sys.index_columns
		--We don't collect info on identity columns here. They may be in NC indexes, but we just analyze identities in the base table.
		SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	
					si.object_id, 
					si.index_id, 
					sc.key_ordinal, 
					sc.is_included_column, 
					sc.is_descending_key,
					sc.partition_ordinal,
					c.name as column_name, 
					st.name as system_type_name,
					c.max_length,
					c.[precision],
					c.[scale],
					c.collation_name,
					c.is_nullable,
					c.is_identity,
					c.is_computed,
					c.is_replicated,
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_sparse' else N'NULL as is_sparse' END + N',
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_filestream' else N'NULL as is_filestream' END + N'				
				FROM	' + QUOTENAME(@database_name) + N'.sys.indexes si
				JOIN	' + QUOTENAME(@database_name) + N'.sys.columns c ON
					si.object_id=c.object_id
				JOIN ' + QUOTENAME(@database_name) + N'.sys.index_columns sc ON 
					sc.object_id = si.object_id
					and sc.index_id=si.index_id
					AND sc.column_id=c.column_id
				JOIN ' + QUOTENAME(@database_name) + N'.sys.types st ON 
					c.system_type_id=st.system_type_id
					AND c.user_type_id=st.user_type_id
				WHERE si.index_id not in (0,1) ' 
					+ CASE WHEN @object_id IS NOT NULL 
						THEN N' AND si.object_id=' + CAST(@object_id AS NVARCHAR(30)) 
					ELSE N'' END 
				+ N';';

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #index_columns for nonclustered indexes',0,1) WITH NOWAIT;
		INSERT	#index_columns ( object_id, index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
			column_name, system_type_name, max_length, precision, scale, collation_name, is_nullable, is_identity, is_computed,
			is_replicated, is_sparse, is_filestream )
				EXEC sp_executesql @dsql;
					
		SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	' + CAST(@database_id AS NVARCHAR(10)) + ' AS database_id, 
						so.object_id, 
						si.index_id, 
						si.type,
						' + QUOTENAME(@database_name, '''') + ' AS database_name, 
						sc.NAME AS [schema_name],
						so.name AS [object_name], 
						si.name AS [index_name],
						CASE	WHEN so.[type] = CAST(''V'' AS CHAR(2)) THEN 1 ELSE 0 END, 
						si.is_unique, 
						si.is_primary_key, 
						CASE when si.type = 3 THEN 1 ELSE 0 END AS is_XML,
						CASE when si.type = 4 THEN 1 ELSE 0 END AS is_spatial,
						CASE when si.type = 6 THEN 1 ELSE 0 END AS is_NC_columnstore,
						si.is_disabled,
						si.is_hypothetical, 
						si.is_padded, 
						si.fill_factor,'
						+ case when @SQLServerProductVersion not like '9%' THEN '
						CASE WHEN si.filter_definition IS NOT NULL THEN si.filter_definition
							 ELSE ''''
						END AS filter_definition' ELSE ''''' AS filter_definition' END + '
						, ISNULL(us.user_seeks, 0), ISNULL(us.user_scans, 0),
						ISNULL(us.user_lookups, 0), ISNULL(us.user_updates, 0), us.last_user_seek, us.last_user_scan,
						us.last_user_lookup, us.last_user_update,
						so.create_date, so.modify_date
				FROM	' + QUOTENAME(@database_name) + '.sys.indexes AS si WITH (NOLOCK)
						JOIN ' + QUOTENAME(@database_name) + '.sys.objects AS so WITH (NOLOCK) ON si.object_id = so.object_id
											   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
											   AND so.type <> ''TF'' /*Exclude table valued functions*/
						JOIN ' + QUOTENAME(@database_name) + '.sys.schemas sc ON so.schema_id = sc.schema_id
						LEFT JOIN sys.dm_db_index_usage_stats AS us WITH (NOLOCK) ON si.[object_id] = us.[object_id]
																	   AND si.index_id = us.index_id
																	   AND us.database_id = '+ CAST(@database_id AS NVARCHAR(10)) + '
				WHERE	si.[type] IN ( 0, 1, 2, 3, 4, 6 ) /* Heaps, clustered, nonclustered, XML, spatial, NC Columnstore */ ' +
				CASE WHEN @table_name IS NOT NULL THEN ' and so.name=' + QUOTENAME(@table_name,'''') + ' ' ELSE '' END + 
		'OPTION	( RECOMPILE );
		';
		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #index_sanity',0,1) WITH NOWAIT;
		INSERT	#index_sanity ( [database_id], [object_id], [index_id], [index_type], [database_name], [schema_name], [object_name],
								index_name, is_indexed_view, is_unique, is_primary_key, is_XML, is_spatial, is_NC_columnstore, 
								is_disabled, is_hypothetical, is_padded, fill_factor, filter_definition, user_seeks, user_scans, 
								user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update,
								create_date, modify_date )
				EXEC sp_executesql @dsql;

		RAISERROR (N'Updating #index_sanity.key_column_names',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		key_column_names = D1.key_column_names
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name 
									+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
										AS col_definition
									FROM	#index_columns c
									WHERE	c.object_id = si.object_id
											AND c.index_id = si.index_id
											AND c.is_included_column = 0 /*Just Keys*/
											AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
									ORDER BY c.object_id, c.index_id, c.key_ordinal	
							FOR	  XML PATH('') ,TYPE).value('.', 'varchar(max)'), 1, 1, ''))
										) D1 ( key_column_names )

		RAISERROR (N'Updating #index_sanity.partition_key_column_name',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		partition_key_column_name = D1.partition_key_column_name
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name AS col_definition
									FROM	#index_columns c
									WHERE	c.object_id = si.object_id
											AND c.index_id = si.index_id
											AND c.partition_ordinal <> 0 /*Just Partitioned Keys*/
									ORDER BY c.object_id, c.index_id, c.key_ordinal	
							FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1,''))) D1 
										( partition_key_column_name )

		RAISERROR (N'Updating #index_sanity.key_column_names_with_sort_order',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		key_column_names_with_sort_order = D2.key_column_names_with_sort_order
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name + CASE c.is_descending_key
									WHEN 1 THEN N' DESC'
									ELSE N''
								+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
								END AS col_definition
							FROM	#index_columns c
							WHERE	c.object_id = si.object_id
									AND c.index_id = si.index_id
									AND c.is_included_column = 0 /*Just Keys*/
									AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
							ORDER BY c.object_id, c.index_id, c.key_ordinal	
					FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1, ''))
					) D2 ( key_column_names_with_sort_order )

		RAISERROR (N'Updating #index_sanity.key_column_names_with_sort_order_no_types (for create tsql)',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		key_column_names_with_sort_order_no_types = D2.key_column_names_with_sort_order_no_types
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + QUOTENAME(c.column_name) + CASE c.is_descending_key
									WHEN 1 THEN N' [DESC]'
									ELSE N''
								END AS col_definition
							FROM	#index_columns c
							WHERE	c.object_id = si.object_id
									AND c.index_id = si.index_id
									AND c.is_included_column = 0 /*Just Keys*/
									AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
							ORDER BY c.object_id, c.index_id, c.key_ordinal	
					FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1, ''))
					) D2 ( key_column_names_with_sort_order_no_types )

		RAISERROR (N'Updating #index_sanity.include_column_names',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		include_column_names = D3.include_column_names
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name
								+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
								FROM	#index_columns c
								WHERE	c.object_id = si.object_id
										AND c.index_id = si.index_id
										AND c.is_included_column = 1 /*Just includes*/
								ORDER BY c.column_name /*Order doesn't matter in includes, 
										this is here to make rows easy to compare.*/ 
						FOR	  XML PATH('') ,  TYPE).value('.', 'varchar(max)'), 1, 1, ''))
						) D3 ( include_column_names );

		RAISERROR (N'Updating #index_sanity.include_column_names_no_types (for create tsql)',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		include_column_names_no_types = D3.include_column_names_no_types
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + QUOTENAME(c.column_name)
								FROM	#index_columns c
								WHERE	c.object_id = si.object_id
										AND c.index_id = si.index_id
										AND c.is_included_column = 1 /*Just includes*/
								ORDER BY c.column_name /*Order doesn't matter in includes, 
										this is here to make rows easy to compare.*/ 
						FOR	  XML PATH('') ,  TYPE).value('.', 'varchar(max)'), 1, 1, ''))
						) D3 ( include_column_names_no_types );

		RAISERROR (N'Updating #index_sanity.count_key_columns and count_include_columns',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		count_included_columns = D4.count_included_columns,
				count_key_columns = D4.count_key_columns
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	SUM(CASE WHEN is_included_column = 'true' THEN 1
												 ELSE 0
											END) AS count_included_columns,
										SUM(CASE WHEN is_included_column = 'false' AND c.key_ordinal > 0 THEN 1
												 ELSE 0
											END) AS count_key_columns
							  FROM		#index_columns c
							  WHERE		c.object_id = si.object_id
										AND c.index_id = si.index_id 
										) AS D4 ( count_included_columns, count_key_columns );

		IF (SELECT LEFT(@SQLServerProductVersion,
			  CHARINDEX('.',@SQLServerProductVersion,0)-1
			  )) < 11 --Anything prior to 2012
		BEGIN
			--NOTE: we're joining to sys.dm_db_index_operational_stats differently than you might think (not using a cross apply)
			--This is because of quirks prior to SQL Server 2012 with this DMV.
			SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
					SELECT	ps.object_id, 
							ps.index_id, 
							ps.partition_number, 
							ps.row_count,
							ps.reserved_page_count * 8. / 1024. AS reserved_MB,
							ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
							ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
							os.leaf_insert_count, 
							os.leaf_delete_count, 
							os.leaf_update_count, 
							os.forwarded_fetch_count,
							os.lob_fetch_in_pages, 
							os.lob_fetch_in_bytes, 
							os.row_overflow_fetch_in_pages,
							os.row_overflow_fetch_in_bytes, 
							os.row_lock_count, 
							os.row_lock_wait_count,
							os.row_lock_wait_in_ms, 
							os.page_lock_count, 
							os.page_lock_wait_count, 
							os.page_lock_wait_in_ms,
							os.index_lock_promotion_attempt_count, 
							os.index_lock_promotion_count, 
							' + case when @SQLServerProductVersion not like '9%' THEN 'par.data_compression_desc ' ELSE 'null as data_compression_desc' END + '
					FROM	' + QUOTENAME(@database_name) + '.sys.dm_db_partition_stats AS ps  
					JOIN ' + QUOTENAME(@database_name) + '.sys.partitions AS par on ps.partition_id=par.partition_id
					JOIN ' + QUOTENAME(@database_name) + '.sys.objects AS so ON ps.object_id = so.object_id
							   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
							   AND so.type <> ''TF'' /*Exclude table valued functions*/
					LEFT JOIN ' + QUOTENAME(@database_name) + '.sys.dm_db_index_operational_stats('
				+ CAST(@database_id AS NVARCHAR(10)) + ', NULL, NULL,NULL) AS os ON
					ps.object_id=os.object_id and ps.index_id=os.index_id and ps.partition_number=os.partition_number 
					WHERE 1=1 
					' + CASE WHEN @object_id IS NOT NULL THEN N'AND so.object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' ' ELSE N' ' END + '
					' + CASE WHEN @filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@filterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + '
			ORDER BY ps.object_id,  ps.index_id, ps.partition_number
			OPTION	( RECOMPILE );
			';
		END
		ELSE /* Otherwise use this syntax which takes advantage of OUTER APPLY on the os_partitions DMV. 
		This performs much better on 2012 tables using 1000+ partitions. */
		BEGIN
 		SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
						SELECT	ps.object_id, 
								ps.index_id, 
								ps.partition_number, 
								ps.row_count,
								ps.reserved_page_count * 8. / 1024. AS reserved_MB,
								ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
								ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
								os.leaf_insert_count, 
								os.leaf_delete_count, 
								os.leaf_update_count, 
								os.forwarded_fetch_count,
								os.lob_fetch_in_pages, 
								os.lob_fetch_in_bytes, 
								os.row_overflow_fetch_in_pages,
								os.row_overflow_fetch_in_bytes, 
								os.row_lock_count, 
								os.row_lock_wait_count,
								os.row_lock_wait_in_ms, 
								os.page_lock_count, 
								os.page_lock_wait_count, 
								os.page_lock_wait_in_ms,
								os.index_lock_promotion_attempt_count, 
								os.index_lock_promotion_count, 
								' + case when @SQLServerProductVersion not like '9%' THEN N'par.data_compression_desc ' ELSE N'null as data_compression_desc' END + N'
						FROM	' + QUOTENAME(@database_name) + N'.sys.dm_db_partition_stats AS ps  
						JOIN ' + QUOTENAME(@database_name) + N'.sys.partitions AS par on ps.partition_id=par.partition_id
						JOIN ' + QUOTENAME(@database_name) + N'.sys.objects AS so ON ps.object_id = so.object_id
								   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
								   AND so.type <> ''TF'' /*Exclude table valued functions*/
						OUTER APPLY ' + QUOTENAME(@database_name) + N'.sys.dm_db_index_operational_stats('
					+ CAST(@database_id AS NVARCHAR(10)) + N', ps.object_id, ps.index_id,ps.partition_number) AS os
						WHERE 1=1 
						' + CASE WHEN @object_id IS NOT NULL THEN N'AND so.object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' ' ELSE N' ' END + N'
						' + CASE WHEN @filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@filterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + '
				ORDER BY ps.object_id,  ps.index_id, ps.partition_number
				OPTION	( RECOMPILE );
				';
 
		END       

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #index_partition_sanity',0,1) WITH NOWAIT;
		INSERT	#index_partition_sanity ( [object_id], index_id, partition_number, row_count, reserved_MB,
										  reserved_LOB_MB, reserved_row_overflow_MB, leaf_insert_count,
										  leaf_delete_count, leaf_update_count, forwarded_fetch_count,
										  lob_fetch_in_pages, lob_fetch_in_bytes, row_overflow_fetch_in_pages,
										  row_overflow_fetch_in_bytes, row_lock_count, row_lock_wait_count,
										  row_lock_wait_in_ms, page_lock_count, page_lock_wait_count,
										  page_lock_wait_in_ms, index_lock_promotion_attempt_count,
										  index_lock_promotion_count, data_compression_desc )
				EXEC sp_executesql @dsql;

		RAISERROR (N'Updating index_sanity_id on #index_partition_sanity',0,1) WITH NOWAIT;
		UPDATE	#index_partition_sanity
		SET		index_sanity_id = i.index_sanity_id
		FROM	#index_partition_sanity ps
				JOIN #index_sanity i ON ps.[object_id] = i.[object_id]
										AND ps.index_id = i.index_id

		RAISERROR (N'Inserting data into #index_sanity_size',0,1) WITH NOWAIT;
		INSERT	#index_sanity_size ( [index_sanity_id], partition_count, total_rows, total_reserved_MB,
									 total_reserved_LOB_MB, total_reserved_row_overflow_MB, total_row_lock_count,
									 total_row_lock_wait_count, total_row_lock_wait_in_ms, avg_row_lock_wait_in_ms,
									 total_page_lock_count, total_page_lock_wait_count, total_page_lock_wait_in_ms,
									 avg_page_lock_wait_in_ms, total_index_lock_promotion_attempt_count, 
									 total_index_lock_promotion_count, data_compression_desc )
				SELECT	index_sanity_id, COUNT(*), SUM(row_count), SUM(reserved_MB), SUM(reserved_LOB_MB),
						SUM(reserved_row_overflow_MB), 
						SUM(row_lock_count), 
						SUM(row_lock_wait_count),
						SUM(row_lock_wait_in_ms), 
						CASE WHEN SUM(row_lock_wait_in_ms) > 0 THEN
							SUM(row_lock_wait_in_ms)/(1.*SUM(row_lock_wait_count))
						ELSE 0 END AS avg_row_lock_wait_in_ms,           
						SUM(page_lock_count), 
						SUM(page_lock_wait_count),
						SUM(page_lock_wait_in_ms), 
						CASE WHEN SUM(page_lock_wait_in_ms) > 0 THEN
							SUM(page_lock_wait_in_ms)/(1.*SUM(page_lock_wait_count))
						ELSE 0 END AS avg_page_lock_wait_in_ms,           
						SUM(index_lock_promotion_attempt_count),
						SUM(index_lock_promotion_count),
						LEFT(MAX(data_compression_info.data_compression_rollup),8000)
				FROM	#index_partition_sanity ipp
				/* individual partitions can have distinct compression settings, just roll them into a list here*/
				OUTER APPLY (SELECT STUFF((
					SELECT	N', ' + data_compression_desc
					FROM	#index_partition_sanity ipp2
					WHERE ipp.[object_id]=ipp2.[object_id]
						AND ipp.[index_id]=ipp2.[index_id]
					ORDER BY ipp2.partition_number
					FOR	  XML PATH(''),TYPE).value('.', 'varchar(max)'), 1, 1, '')) 
						data_compression_info(data_compression_rollup)
				GROUP BY index_sanity_id
				ORDER BY index_sanity_id 
		OPTION	( RECOMPILE );

		RAISERROR (N'Adding UQ index on #index_sanity (object_id,index_id)',0,1) WITH NOWAIT;
		CREATE UNIQUE INDEX uq_object_id_index_id ON #index_sanity (object_id,index_id);

		RAISERROR (N'Inserting data into #missing_indexes',0,1) WITH NOWAIT;
		SET @dsql=N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	id.object_id, ' + QUOTENAME(@database_name,'''') + N', sc.[name], so.[name], id.statement , gs.avg_total_user_cost, 
						gs.avg_user_impact, gs.user_seeks, gs.user_scans, gs.unique_compiles,id.equality_columns, 
						id.inequality_columns,id.included_columns
				FROM	sys.dm_db_missing_index_groups ig
						JOIN sys.dm_db_missing_index_details id ON ig.index_handle = id.index_handle
						JOIN sys.dm_db_missing_index_group_stats gs ON ig.index_group_handle = gs.group_handle
						JOIN ' + QUOTENAME(@database_name) + N'.sys.objects so on 
							id.object_id=so.object_id
						JOIN ' + QUOTENAME(@database_name) + N'.sys.schemas sc on 
							so.schema_id=sc.schema_id
				WHERE	id.database_id = ' + CAST(@database_id AS NVARCHAR(30)) + '
				' + CASE WHEN @object_id IS NULL THEN N'' 
					ELSE N'and id.object_id=' + CAST(@object_id AS NVARCHAR(30)) 
				END +
		N';'

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);
		INSERT	#missing_indexes ( [object_id], [database_name], [schema_name], [table_name], [statement], avg_total_user_cost, 
									avg_user_impact, user_seeks, user_scans, unique_compiles, equality_columns, 
									inequality_columns,included_columns)
		EXEC sp_executesql @dsql;

		SET @dsql = N'
			SELECT 
				fk_object.name AS foreign_key_name,
				parent_object.[object_id] AS parent_object_id,
				parent_object.name AS parent_object_name,
				referenced_object.[object_id] AS referenced_object_id,
				referenced_object.name AS referenced_object_name,
				fk.is_disabled,
				fk.is_not_trusted,
				fk.is_not_for_replication,
				parent.fk_columns,
				referenced.fk_columns,
				[update_referential_action_desc],
				[delete_referential_action_desc]
			FROM ' + QUOTENAME(@database_name) + N'.sys.foreign_keys fk
			JOIN ' + QUOTENAME(@database_name) + N'.sys.objects fk_object ON fk.object_id=fk_object.object_id
			JOIN ' + QUOTENAME(@database_name) + N'.sys.objects parent_object ON fk.parent_object_id=parent_object.object_id
			JOIN ' + QUOTENAME(@database_name) + N'.sys.objects referenced_object ON fk.referenced_object_id=referenced_object.object_id
			CROSS APPLY ( SELECT	STUFF( (SELECT	N'', '' + c_parent.name AS fk_columns
											FROM	' + QUOTENAME(@database_name) + N'.sys.foreign_key_columns fkc 
											JOIN ' + QUOTENAME(@database_name) + N'.sys.columns c_parent ON fkc.parent_object_id=c_parent.[object_id]
												AND fkc.parent_column_id=c_parent.column_id
											WHERE	fk.parent_object_id=fkc.parent_object_id
												AND fk.[object_id]=fkc.constraint_object_id
											ORDER BY fkc.constraint_column_id 
									FOR	  XML PATH('''') ,
											  TYPE).value(''.'', ''varchar(max)''), 1, 1, '''')/*This is how we remove the first comma*/ ) parent ( fk_columns )
			CROSS APPLY ( SELECT	STUFF( (SELECT	N'', '' + c_referenced.name AS fk_columns
											FROM	' + QUOTENAME(@database_name) + N'.sys.	foreign_key_columns fkc 
											JOIN ' + QUOTENAME(@database_name) + N'.sys.columns c_referenced ON fkc.referenced_object_id=c_referenced.[object_id]
												AND fkc.referenced_column_id=c_referenced.column_id
											WHERE	fk.referenced_object_id=fkc.referenced_object_id
												and fk.[object_id]=fkc.constraint_object_id
											ORDER BY fkc.constraint_column_id  /*order by col name, we don''t have anything better*/
									FOR	  XML PATH('''') ,
											  TYPE).value(''.'', ''varchar(max)''), 1, 1, '''') ) referenced ( fk_columns )
			' + CASE WHEN @object_id IS NOT NULL THEN 
					'WHERE fk.parent_object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' OR fk.referenced_object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' ' 
					ELSE N' ' END + '
			ORDER BY parent_object_name, foreign_key_name;
		';
		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #foreign_keys',0,1) WITH NOWAIT;
        INSERT  #foreign_keys ( foreign_key_name, parent_object_id,parent_object_name, referenced_object_id, referenced_object_name,
                                is_disabled, is_not_trusted, is_not_for_replication, parent_fk_columns, referenced_fk_columns,
								[update_referential_action_desc], [delete_referential_action_desc] )
                EXEC sp_executesql @dsql;

        RAISERROR (N'Updating #index_sanity.referenced_by_foreign_key',0,1) WITH NOWAIT;
		UPDATE #index_sanity
			SET is_referenced_by_foreign_key=1
		FROM #index_sanity s
		JOIN #foreign_keys fk ON 
			s.object_id=fk.referenced_object_id
			AND LEFT(s.key_column_names,LEN(fk.referenced_fk_columns)) = fk.referenced_fk_columns

		RAISERROR (N'Add computed columns to #index_sanity to simplify queries.',0,1) WITH NOWAIT;
		ALTER TABLE #index_sanity ADD 
		[schema_object_name] AS [schema_name] + '.' + [object_name]  ,
		[schema_object_indexid] AS [schema_name] + '.' + [object_name]
			+ CASE WHEN [index_name] IS NOT NULL THEN '.' + index_name
			ELSE ''
			END + ' (' + CAST(index_id AS NVARCHAR(20)) + ')' ,
		first_key_column_name AS CASE	WHEN count_key_columns > 1
			THEN LEFT(key_column_names, CHARINDEX(',', key_column_names, 0) - 1)
			ELSE key_column_names
			END ,
		index_definition AS 
		CASE WHEN partition_key_column_name IS NOT NULL 
			THEN N'[PARTITIONED BY:' + partition_key_column_name +  N']' 
			ELSE '' 
			END +
			CASE index_id
				WHEN 0 THEN N'[HEAP] '
				WHEN 1 THEN N'[CX] '
				ELSE N'' END + CASE WHEN is_indexed_view = 1 THEN '[VIEW] '
				ELSE N'' END + CASE WHEN is_primary_key = 1 THEN N'[PK] '
				ELSE N'' END + CASE WHEN is_XML = 1 THEN N'[XML] '
				ELSE N'' END + CASE WHEN is_spatial = 1 THEN N'[SPATIAL] '
				ELSE N'' END + CASE WHEN is_NC_columnstore = 1 THEN N'[COLUMNSTORE] '
				ELSE N'' END + CASE WHEN is_disabled = 1 THEN N'[DISABLED] '
				ELSE N'' END + CASE WHEN is_hypothetical = 1 THEN N'[HYPOTHETICAL] '
				ELSE N'' END + CASE WHEN is_unique = 1 AND is_primary_key = 0 THEN N'[UNIQUE] '
				ELSE N'' END + CASE WHEN count_key_columns > 0 THEN 
					N'[' + CAST(count_key_columns AS VARCHAR(10)) + N' KEY' 
						+ CASE WHEN count_key_columns > 1 then  N'S' ELSE N'' END
						+ N'] ' + LTRIM(key_column_names_with_sort_order)
				ELSE N'' END + CASE WHEN count_included_columns > 0 THEN 
					N' [' + CAST(count_included_columns AS VARCHAR(10))  + N' INCLUDE' + 
						+ CASE WHEN count_included_columns > 1 then  N'S' ELSE N'' END					
						+ N'] ' + include_column_names
				ELSE N'' END + CASE WHEN filter_definition <> N'' THEN N' [FILTER] ' + filter_definition
				ELSE N'' END ,
		[total_reads] AS user_seeks + user_scans + user_lookups,
		[reads_per_write] AS CAST(CASE WHEN user_updates > 0
			THEN ( user_seeks + user_scans + user_lookups )  / (1.0 * user_updates)
			ELSE 0 END AS MONEY) ,
		[index_usage_summary] AS N'Reads: ' + 
			REPLACE(CONVERT(NVARCHAR(30),CAST((user_seeks + user_scans + user_lookups) AS money), 1), '.00', '')
			+ N'; Writes:' + 
			REPLACE(CONVERT(NVARCHAR(30),CAST(user_updates AS money), 1), '.00', ''),
		[more_info] AS N'EXEC dbo.sp_BlitzIndex @database_name=' + QUOTENAME([database_name],'''') + 
			N', @schema_name=' + QUOTENAME([schema_name],'''') + N', @table_name=' + QUOTENAME([object_name],'''') + N';'

		RAISERROR (N'Update index_secret on #index_sanity for NC indexes.',0,1) WITH NOWAIT;
		UPDATE nc 
		SET secret_columns=
			N'[' + 
			CASE tb.count_key_columns WHEN 0 THEN '1' ELSE CAST(tb.count_key_columns AS VARCHAR(10)) END +
			CASE nc.is_unique WHEN 1 THEN N' INCLUDE' ELSE N' KEY' END +
			CASE WHEN tb.count_key_columns > 1 then  N'S] ' ELSE N'] ' END +
			CASE tb.index_id WHEN 0 THEN '[RID]' ELSE LTRIM(tb.key_column_names) +
				/* Uniquifiers only needed on non-unique clustereds-- not heaps */
				CASE tb.is_unique WHEN 0 THEN ' [UNIQUIFIER]' ELSE N'' END
			END
			, count_secret_columns=
			CASE tb.index_id WHEN 0 THEN 1 ELSE 
				tb.count_key_columns +
					CASE tb.is_unique WHEN 0 THEN 1 ELSE 0 END
			END
		FROM #index_sanity AS nc
		JOIN #index_sanity AS tb ON nc.object_id=tb.object_id
			and tb.index_id in (0,1) 
		WHERE nc.index_id > 1;

		RAISERROR (N'Update index_secret on #index_sanity for heaps and non-unique clustered.',0,1) WITH NOWAIT;
		UPDATE tb
		SET secret_columns=	CASE tb.index_id WHEN 0 THEN '[RID]' ELSE '[UNIQUIFIER]' END
			, count_secret_columns = 1
		FROM #index_sanity AS tb
		WHERE tb.index_id = 0 /*Heaps-- these have the RID */
			or (tb.index_id=1 and tb.is_unique=0); /* Non-unique CX: has uniquifer (when needed) */

		RAISERROR (N'Add computed column to #index_sanity_size to simplify queries.',0,1) WITH NOWAIT;
		ALTER TABLE #index_sanity_size ADD 
			  index_size_summary AS ISNULL(
				CASE WHEN partition_count > 1
						THEN N'[' + CAST(partition_count AS NVARCHAR(10)) + N' PARTITIONS] '
						ELSE N''
				END + REPLACE(CONVERT(NVARCHAR(30),CAST([total_rows] AS money), 1), N'.00', N'') + N' rows; '
				+ CASE WHEN total_reserved_MB > 1024 THEN 
					CAST(CAST(total_reserved_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB'
				ELSE 
					CAST(CAST(total_reserved_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB'
				END
				+ CASE WHEN total_reserved_LOB_MB > 1024 THEN 
					N'; ' + CAST(CAST(total_reserved_LOB_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB LOB'
				WHEN total_reserved_LOB_MB > 0 THEN
					N'; ' + CAST(CAST(total_reserved_LOB_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB LOB'
				ELSE ''
				END
				 + CASE WHEN total_reserved_row_overflow_MB > 1024 THEN
					N'; ' + CAST(CAST(total_reserved_row_overflow_MB/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB Row Overflow'
				WHEN total_reserved_row_overflow_MB > 0 THEN
					N'; ' + CAST(CAST(total_reserved_row_overflow_MB AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'MB Row Overflow'
				ELSE ''
				END ,
					'Error- NULL in computed column'),
			index_lock_wait_summary AS ISNULL(
				CASE WHEN total_row_lock_wait_count = 0 and  total_page_lock_wait_count = 0 and
					total_index_lock_promotion_attempt_count = 0 THEN N'0 lock waits.'
				ELSE
					CASE WHEN total_row_lock_wait_count > 0 THEN
						N'Row lock waits: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_row_lock_wait_count AS money), 1), N'.00', N'')
						+ N'; total duration: ' + 
							CASE WHEN total_row_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
								REPLACE(CONVERT(NVARCHAR(30),CAST((total_row_lock_wait_in_ms/60000) AS money), 1), N'.00', N'') + N' minutes; '
							ELSE                         
								REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_row_lock_wait_in_ms/1000,0) AS money), 1), N'.00', N'') + N' seconds; '
							END
						+ N'avg duration: ' + 
							CASE WHEN avg_row_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
								REPLACE(CONVERT(NVARCHAR(30),CAST((avg_row_lock_wait_in_ms/60000) AS money), 1), N'.00', N'') + N' minutes; '
							ELSE                         
								REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(avg_row_lock_wait_in_ms/1000,0) AS money), 1), N'.00', N'') + N' seconds; '
							END
					ELSE N''
					END +
					CASE WHEN total_page_lock_wait_count > 0 THEN
						N'Page lock waits: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_page_lock_wait_count AS money), 1), N'.00', N'')
						+ N'; total duration: ' + 
							CASE WHEN total_page_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
								REPLACE(CONVERT(NVARCHAR(30),CAST((total_page_lock_wait_in_ms/60000) AS money), 1), N'.00', N'') + N' minutes; '
							ELSE                         
								REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_page_lock_wait_in_ms/1000,0) AS money), 1), N'.00', N'') + N' seconds; '
							END
						+ N'avg duration: ' + 
							CASE WHEN avg_page_lock_wait_in_ms >= 60000 THEN /*More than 1 min*/
								REPLACE(CONVERT(NVARCHAR(30),CAST((avg_page_lock_wait_in_ms/60000) AS money), 1), N'.00', N'') + N' minutes; '
							ELSE                         
								REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(avg_page_lock_wait_in_ms/1000,0) AS money), 1), N'.00', N'') + N' seconds; '
							END
					ELSE N''
					END +
					CASE WHEN total_index_lock_promotion_attempt_count > 0 THEN
						N'Lock escalation attempts: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(total_index_lock_promotion_attempt_count AS money), 1), N'.00', N'')
						+ N'; Actual Escalations: ' + REPLACE(CONVERT(NVARCHAR(30),CAST(ISNULL(total_index_lock_promotion_count,0) AS money), 1), N'.00', N'') + N'.'
					ELSE N''
					END
				END                  
					,'Error- NULL in computed column')


		RAISERROR (N'Add computed columns to #missing_index to simplify queries.',0,1) WITH NOWAIT;
		ALTER TABLE #missing_indexes ADD 
				[index_estimated_impact] AS 
					CAST(user_seeks + user_scans AS NVARCHAR(30)) + N' use' 
						+ CASE WHEN (user_seeks + user_scans) > 1 THEN N's' ELSE N'' END
						 +N'; Impact: ' + CAST(avg_user_impact AS NVARCHAR(30))
						+ N'%; Avg query cost: '
						+ CAST(avg_total_user_cost AS NVARCHAR(30)),
				[missing_index_details] AS
					CASE WHEN equality_columns IS NOT NULL THEN N'EQUALITY: ' + equality_columns + N' '
						 ELSE N''
					END + CASE WHEN inequality_columns IS NOT NULL THEN N'INEQUALITY: ' + inequality_columns + N' '
					   ELSE N''
					END + CASE WHEN included_columns IS NOT NULL THEN N'INCLUDES: ' + included_columns + N' '
						ELSE N''
					END,
				[create_tsql] AS N'CREATE INDEX [ix_' + table_name + N'_' 
					+ REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(equality_columns,N'') 
					+ ISNULL(inequality_columns,''),',',''),'[',''),']',''),' ','_') +
					CASE WHEN included_columns IS NOT NULL THEN N'_includes' ELSE N'' END + N'] ON ' + 
					[statement] + N' (' + ISNULL(equality_columns,N'')+
					CASE WHEN equality_columns IS NOT NULL AND inequality_columns IS NOT NULL THEN N', ' ELSE N'' END + 
					CASE WHEN inequality_columns IS NOT NULL THEN inequality_columns ELSE N'' END + 
					') ' + CASE WHEN included_columns IS NOT NULL THEN N' INCLUDE (' + included_columns + N')' ELSE N'' END,
				[more_info] AS N'EXEC dbo.sp_BlitzIndex @database_name=' + QUOTENAME([database_name],'''') + 
					N', @schema_name=	' + QUOTENAME([schema_name],'''') + N', @table_name=' + QUOTENAME([table_name],'''') + N';'
				;


		RAISERROR (N'Populate #index_create_tsql.',0,1) WITH NOWAIT;
		INSERT #index_create_tsql (index_sanity_id, create_tsql)
		SELECT
			index_sanity_id,
			ISNULL (
			/* Script drops for disabled non-clustered indexes*/
			CASE WHEN is_disabled = 1 AND index_id <> 1
				THEN N'--DROP INDEX ' + QUOTENAME([index_name]) + N' ON '
				 + QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name]) 
			ELSE
				CASE index_id WHEN 0 THEN N'(HEAP)' 
				ELSE 
					CASE WHEN is_XML = 1 OR is_spatial=1 THEN N'' /* Not even trying for these just yet...*/
					ELSE 
						CASE WHEN is_primary_key=1 THEN
							N'ALTER TABLE ' + QUOTENAME([schema_name]) +
								N'.' + QUOTENAME([object_name]) + 
								N' ADD CONSTRAINT [' +
								index_name + 
								N'] PRIMARY KEY ' + 
								CASE WHEN index_id=1 THEN N'CLUSTERED (' ELSE N'(' END +
								key_column_names_with_sort_order_no_types + N' )' 
						ELSE /*Else not a PK */ 
							N'CREATE ' + 
							CASE WHEN is_unique=1 THEN N'UNIQUE ' ELSE N'' END +
							CASE WHEN index_id=1 THEN N'CLUSTERED ' ELSE N'' END +
							CASE WHEN is_NC_columnstore=1 THEN N'NONCLUSTERED COLUMNSTORE ' ELSE N'' END +
							N'INDEX ['
								 + index_name + N'] ON ' + 
								QUOTENAME([schema_name]) + '.' + QUOTENAME([object_name]) + 
									CASE WHEN is_NC_columnstore=1 THEN 
										N' (' + ISNULL(include_column_names_no_types,'') +  N' )' 
									ELSE /*Else not colunnstore */ 
										N' (' + ISNULL(key_column_names_with_sort_order_no_types,'') +  N' )' 
										+ CASE WHEN include_column_names_no_types IS NOT NULL THEN 
											N' INCLUDE (' + include_column_names_no_types + N')' 
											ELSE N'' 
										END
									END /*End non-colunnstore case */ 
								+ CASE WHEN filter_definition <> N'' THEN N' WHERE ' + filter_definition ELSE N'' END
							END /*End Non-PK index CASE */ +
						CASE WHEN (@SQLServerEdition =  3  AND is_NC_columnstore=0 ) THEN + N' WITH (ONLINE=ON);' ELSE N';' END
  					END /*End non-spatial and non-xml CASE */ 
				END
			END, '[Unknown Error]')
				AS create_tsql
		FROM #index_sanity;
					
	END
END TRY
BEGIN CATCH
		RAISERROR (N'Failure populating temp tables.', 0,1) WITH NOWAIT;

		IF @dsql IS NOT NULL
		BEGIN
			SET @msg= 'Last @dsql: ' + @dsql;
			RAISERROR(@msg, 0, 1) WITH NOWAIT;
		END

		SELECT	@msg = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@msg,@ErrorSeverity, @ErrorState )WITH NOWAIT;
		
		
		WHILE @@trancount > 0 
			ROLLBACK;

		RETURN;
END CATCH;

----------------------------------------
--STEP 2: DIAGNOSE THE PATIENT
--EVERY QUERY AFTER THIS GOES AGAINST TEMP TABLES ONLY.
----------------------------------------
BEGIN TRY
----------------------------------------
--If @table_name is specified, just return information for that table.
--The @mode parameter doesn't matter if you're looking at a specific table.
----------------------------------------
IF @table_name IS NOT NULL
BEGIN
	RAISERROR(N'@table_name specified, giving detail only on that table.', 0,1) WITH NOWAIT;

	--We do a left join here in case this is a disabled NC.
	--In that case, it won't have any size info/pages allocated.
	WITH table_mode_cte AS (
		SELECT 
			s.schema_object_indexid, 
			s.key_column_names,
			s.index_definition, 
			ISNULL(s.secret_columns,N'') AS secret_columns,
			s.index_usage_summary, 
			ISNULL(sz.index_size_summary,'') /*disabled NCs will be null*/ AS index_size_summary,
			ISNULL(sz.index_lock_wait_summary,'') AS index_lock_wait_summary,
			s.is_referenced_by_foreign_key,
			(SELECT COUNT(*)
				FROM #foreign_keys fk WHERE fk.parent_object_id=s.object_id
				AND PATINDEX (fk.parent_fk_columns, s.key_column_names)=1) AS FKs_covered_by_index,
			s.last_user_seek,
			s.last_user_scan,
			s.last_user_lookup,
			s.last_user_update,
			s.create_date,
			s.modify_date,
			ct.create_tsql,
			1 as display_order
		FROM #index_sanity s
		LEFT JOIN #index_sanity_size sz ON 
			s.index_sanity_id=sz.index_sanity_id
		LEFT JOIN #index_create_tsql ct ON 
			s.index_sanity_id=ct.index_sanity_id
		WHERE s.[object_id]=@object_id
		UNION ALL
		SELECT 				
				N'sp_BlitzIndex version 2.0 (May 15, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				0 as display_order
	)
	SELECT 
			schema_object_indexid AS [Details: schema.table.index(indexid)], 
			index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
			secret_columns AS [Secret Columns],
			index_usage_summary AS [Usage], 
			index_size_summary AS [Size],
			index_lock_wait_summary AS [Lock Waits],
			is_referenced_by_foreign_key AS [Referenced by FK?],
			FKs_covered_by_index AS [FK Covered by Index?],
			last_user_seek AS [Last User Seek],
			last_user_scan AS [Last User Scan],
			last_user_lookup AS [Last User Lookup],
			last_user_update as [Last User Write],
			create_date AS [Created],
			modify_date AS [Last Modified],
			create_tsql AS [Create TSQL]
	FROM table_mode_cte
	ORDER BY display_order ASC, key_column_names ASC
	OPTION	( RECOMPILE );						

	IF (SELECT TOP 1 [object_id] FROM    #missing_indexes mi) IS NOT NULL
	BEGIN  
		SELECT  N'Missing index.' AS Finding ,
				N'http://BrentOzar.com/go/Indexaphobia' AS URL ,
				mi.[statement] + ' Est Benefit: '
					+ CASE WHEN magic_benefit_number >= 922337203685477 THEN '>= 922,337,203,685,477'
					ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(magic_benefit_number AS BIGINT) AS money), 1), '.00', '')
					END AS [Estimated Benefit],
				missing_index_details AS [Missing Index Request] ,
				index_estimated_impact AS [Estimated Impact],
				create_tsql AS [Create TSQL]
		FROM    #missing_indexes mi
		WHERE   [object_id] = @object_id
		ORDER BY magic_benefit_number DESC
		OPTION	( RECOMPILE );
	END       
	ELSE     
	SELECT 'No missing indexes.' AS finding;

	SELECT 	
		column_name AS [Column Name],
		(SELECT COUNT(*)  
			FROM #index_columns c2 
			WHERE c2.column_name=c.column_name
			and c2.key_ordinal is not null)
		+ CASE WHEN c.index_id = 1 and c.key_ordinal is not null THEN
			-1+ (SELECT COUNT(DISTINCT index_id)
			from #index_columns c3
			where c3.index_id not in (0,1))
			ELSE 0 END
				AS [Found In],
		system_type_name + 
			CASE max_length WHEN -1 THEN N' (max)' ELSE
				CASE  
					WHEN system_type_name in (N'char',N'nchar',N'binary',N'varbinary') THEN N' (' + CAST(max_length as NVARCHAR(20)) + N')' 
					WHEN system_type_name in (N'varchar',N'nvarchar') THEN N' (' + CAST(max_length/2 as NVARCHAR(20)) + N')' 
					ELSE '' 
				END
			END
			AS [Type],
		CASE is_computed WHEN 1 THEN 'yes' ELSE '' END AS [Computed?],
		max_length AS [Length (max bytes)],
		[precision] AS [Prec],
		[scale] AS [Scale],
		CASE is_nullable WHEN 1 THEN 'yes' ELSE '' END AS [Nullable?],
		CASE is_identity WHEN 1 THEN 'yes' ELSE '' END AS [Identity?],
		CASE is_replicated WHEN 1 THEN 'yes' ELSE '' END AS [Replicated?],
		CASE is_sparse WHEN 1 THEN 'yes' ELSE '' END AS [Sparse?],
		CASE is_filestream WHEN 1 THEN 'yes' ELSE '' END AS [Filestream?],
		collation_name AS [Collation]
	FROM #index_columns AS c
	where index_id in (0,1);

	IF (SELECT TOP 1 parent_object_id FROM #foreign_keys) IS NOT NULL
	BEGIN
		SELECT parent_object_name + N': ' + foreign_key_name AS [Foreign Key],
			parent_fk_columns AS [Foreign Key Columns],
			referenced_object_name AS [Referenced Table],
			referenced_fk_columns AS [Referenced Table Columns],
			is_disabled AS [Is Disabled?],
			is_not_trusted as [Not Trusted?],
			is_not_for_replication [Not for Replication?],
			[update_referential_action_desc] as [Cascading Updates?],
			[delete_referential_action_desc] as [Cascading Deletes?]
		FROM #foreign_keys
		ORDER BY [Foreign Key]
		OPTION	( RECOMPILE );
	END
	ELSE
	SELECT 'No foreign keys.' AS finding;
END 

--If @table_name is NOT specified...
--Act based on the @mode and @filter. (@filter applies only when @mode=0 "diagnose")
ELSE
BEGIN;
	IF @mode=0 /* DIAGNOSE*/
	BEGIN;
		RAISERROR(N'@mode=0, we are diagnosing.', 0,1) WITH NOWAIT;

		RAISERROR(N'Insert a row to help people find help', 0,1) WITH NOWAIT;
		INSERT	#blitz_index_results ( check_id, findings_group, finding, URL, details, index_definition,
										index_usage_summary, index_size_summary )
		VALUES  ( 0 , N'Database=' + @database_name, N'sp_BlitzIndex version 2.0 (May 15, 2013)' ,
				N'From Brent Ozar Unlimited' ,   N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
				, N'',N''
				);

		----------------------------------------
		--Multiple Index Personalities: Check_id 0-10
		----------------------------------------
		BEGIN;
		RAISERROR('check_id 1: Duplicate keys', 0,1) WITH NOWAIT;
			WITH	duplicate_indexes
					  AS ( SELECT	[object_id], key_column_names
						   FROM		#index_sanity
						   WHERE  index_type IN (1,2) /* Clustered, NC only*/
								AND is_hypothetical = 0
								AND is_disabled = 0
						   GROUP BY	[object_id], key_column_names
						   HAVING	COUNT(*) > 1)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	1 AS check_id, 
								ip.index_sanity_id,
								'Multiple Index Personalities' AS findings_group,
								'Duplicate keys' AS finding,
								N'http://BrentOzar.com/go/duplicateindex' AS URL,
								ip.schema_object_indexid AS details,
								ip.index_definition, 
								ip.secret_columns, 
								ip.index_usage_summary,
								ips.index_size_summary
						FROM	duplicate_indexes di
								JOIN #index_sanity ip ON di.[object_id] = ip.[object_id]
														 AND ip.key_column_names = di.key_column_names
								JOIN #index_sanity_size ips ON ip.index_sanity_id = ips.index_sanity_id
						ORDER BY ip.object_id, ip.key_column_names_with_sort_order	
				OPTION	( RECOMPILE );

		RAISERROR('check_id 2: Keys w/ identical leading columns.', 0,1) WITH NOWAIT;
			WITH	borderline_duplicate_indexes
					  AS ( SELECT DISTINCT [object_id], first_key_column_name, key_column_names,
									COUNT([object_id]) OVER ( PARTITION BY [object_id], first_key_column_name ) AS number_dupes
						   FROM		#index_sanity
						   WHERE index_type IN (1,2) /* Clustered, NC only*/
							AND is_hypothetical=0
							AND is_disabled=0)
				INSERT	#blitz_index_results ( check_id, index_sanity_id,  findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	2 AS check_id, 
								ip.index_sanity_id,
								'Multiple Index Personalities' AS findings_group,
								'Borderline duplicate keys' AS finding,
								N'http://BrentOzar.com/go/duplicateindex' AS URL,
								ip.schema_object_indexid AS details, 
								ip.index_definition, 
								ip.secret_columns,
								ip.index_usage_summary,
								ips.index_size_summary
						FROM	#index_sanity AS ip 
						JOIN #index_sanity_size ips ON ip.index_sanity_id = ips.index_sanity_id
						WHERE EXISTS (
							SELECT di.[object_id]
							FROM borderline_duplicate_indexes AS di
							WHERE di.[object_id] = ip.[object_id] AND
								di.first_key_column_name = ip.first_key_column_name AND
								di.key_column_names <> ip.key_column_names AND
								di.number_dupes > 1	
						)
						ORDER BY ip.[schema_name], ip.[object_name], ip.key_column_names, ip.include_column_names
			OPTION	( RECOMPILE );

		END
		----------------------------------------
		--Aggressive Indexes: Check_id 10-19
		----------------------------------------
		BEGIN;

		RAISERROR(N'check_id 11: Total lock wait time > 5 minutes (row + page)', 0,1) WITH NOWAIT;
		INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										secret_columns, index_usage_summary, index_size_summary )
				SELECT	11 AS check_id, 
						i.index_sanity_id,
						N'Aggressive Indexes' AS findings_group,
						N'Total lock wait time > 5 minutes (row + page)' AS finding, 
						N'http://BrentOzar.com/go/AggressiveIndexes' AS URL,
						i.schema_object_indexid + N': ' +
							sz.index_lock_wait_summary AS details, 
						i.index_definition,
						i.secret_columns,
						i.index_usage_summary,
						sz.index_size_summary
				FROM	#index_sanity AS i
				JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
				WHERE	(total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) > 300000
				OPTION	( RECOMPILE );
		END

		---------------------------------------- 
		--Index Hoarder: Check_id 20-29
		----------------------------------------
		BEGIN
			RAISERROR(N'check_id 20: >=7 NC indexes on any given table. Yes, 7 is an arbitrary number.', 0,1) WITH NOWAIT;
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	20 AS check_id, 
								MAX(i.index_sanity_id) AS index_sanity_id, 
								'Index Hoarder' AS findings_group,
								'Many NC indexes on a single table' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								CAST (COUNT(*) AS NVARCHAR(30)) + ' NC indexes on ' + i.schema_object_name AS details,
								i.schema_object_name + ' (' + CAST (COUNT(*) AS NVARCHAR(30)) + ' indexes)' AS index_definition,
								'' AS secret_columns,
								REPLACE(CONVERT(NVARCHAR(30),CAST(SUM(total_reads) AS money), 1), N'.00', N'') + N' reads (ALL); '
									+ REPLACE(CONVERT(NVARCHAR(30),CAST(SUM(user_updates) AS money), 1), N'.00', N'') + N' writes (ALL); ',
								REPLACE(CONVERT(NVARCHAR(30),CAST(MAX(total_rows) AS money), 1), N'.00', N'') + N' rows (MAX)'
									+ CASE WHEN SUM(total_reserved_MB) > 1024 THEN 
										N'; ' + CAST(CAST(SUM(total_reserved_MB)/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'GB (ALL)'
									WHEN SUM(total_reserved_MB) > 0 THEN
										N'; ' + CAST(CAST(SUM(total_reserved_MB) AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'MB (ALL)'
									ELSE ''
									END AS index_size_summary
						FROM	#index_sanity i
						JOIN #index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	index_id NOT IN ( 0, 1 )
						GROUP BY schema_object_name
						HAVING	COUNT(*) >= 7
						ORDER BY i.schema_object_name DESC  OPTION	( RECOMPILE );

			if @filter = 1 /*@filter=1 is "ignore unusued" */
			BEGIN
				RAISERROR(N'Skipping checks on unused indexes (21 and 22) because @filter=1', 0,1) WITH NOWAIT;
			END
			ELSE /*Otherwise, go ahead and do the checks*/
			BEGIN
				RAISERROR(N'check_id 21: >=5 percent of indexes are unused. Yes, 5 is an arbitrary number.', 0,1) WITH NOWAIT;
					DECLARE @percent_NC_indexes_unused NUMERIC(29,1);
					DECLARE @NC_indexes_unused_reserved_MB NUMERIC(29,1);

					SELECT	@percent_NC_indexes_unused =( 100.00 * SUM(CASE	WHEN total_reads = 0 THEN 1
												ELSE 0
										   END) ) / COUNT(*) ,
							@NC_indexes_unused_reserved_MB = SUM(CASE WHEN total_reads = 0 THEN sz.total_reserved_MB
									 ELSE 0
								END) 
					FROM	#index_sanity i
					JOIN	#index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id NOT IN ( 0, 1 ) 
					OPTION	( RECOMPILE );

				IF @percent_NC_indexes_unused >= 5 
					INSERT	#blitz_index_results ( check_id, index_sanity_id,  findings_group, finding, URL, details, index_definition,
												   secret_columns, index_usage_summary, index_size_summary )
							SELECT	21 AS check_id, 
									MAX(i.index_sanity_id) AS index_sanity_id, 
									N'Index Hoarder' AS findings_group,
									N'More than 5% of NC indexes are unused' AS finding,
									N'http://BrentOzar.com/go/IndexHoarder' AS URL,
									CAST (@percent_NC_indexes_unused AS NVARCHAR(30)) + N'% of NC indexes (' + CAST(COUNT(*) AS NVARCHAR(10)) + N') are unused. ' +
									N'These take up ' + CAST (@NC_indexes_unused_reserved_MB AS NVARCHAR(30)) + N'MB of space.' AS details,
									i.database_name + ' (' + CAST (COUNT(*) AS NVARCHAR(30)) + N' indexes)' AS index_definition,
									'' AS secret_columns, 
									CAST(SUM(total_reads) AS NVARCHAR(256)) + N' reads (ALL); '
										+ CAST(SUM([user_updates]) AS NVARCHAR(256)) + N' writes (ALL)' AS index_usage_summary,
								
									REPLACE(CONVERT(NVARCHAR(30),CAST(MAX([total_rows]) AS money), 1), '.00', '') + N' rows (MAX)'
										+ CASE WHEN SUM(total_reserved_MB) > 1024 THEN 
											N'; ' + CAST(CAST(SUM(total_reserved_MB)/1024. AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'GB (ALL)'
										WHEN SUM(total_reserved_MB) > 0 THEN
											N'; ' + CAST(CAST(SUM(total_reserved_MB) AS NUMERIC(29,1)) AS NVARCHAR(30)) + 'MB (ALL)'
										ELSE ''
										END AS index_size_summary
							FROM	#index_sanity i
							JOIN	#index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
							WHERE	index_id NOT IN ( 0, 1 )
									AND total_reads = 0
							GROUP BY i.database_name 
					OPTION	( RECOMPILE );

				RAISERROR(N'check_id 22: NC indexes with 0 reads. (Borderline)', 0,1) WITH NOWAIT;
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	22 AS check_id, 
								i.index_sanity_id,
								N'Index Hoarder' AS findings_group,
								N'Unused NC index' AS finding, 
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								N'0 reads: ' + i.schema_object_indexid AS details, 
								i.index_definition, 
								i.secret_columns, 
								i.index_usage_summary,
								sz.index_size_summary
						FROM	#index_sanity AS i
						JOIN	#index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
						WHERE	i.total_reads=0
								AND i.index_id NOT IN (0,1) /*NCs only*/
						ORDER BY i.schema_object_indexid
						OPTION	( RECOMPILE );
			END /*end checks only run when @filter <> 1*/

			RAISERROR(N'check_id 23: Indexes with 7 or more columns. (Borderline)', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	23 AS check_id, 
							i.index_sanity_id, 
							N'Index Hoarder' AS findings_group,
							N'Borderline: Wide indexes (7 or more columns)' AS finding, 
							N'http://BrentOzar.com/go/IndexHoarder' AS URL,
							CAST(count_key_columns + count_included_columns AS NVARCHAR(10)) + ' columns on '
							+ i.schema_object_indexid AS details, i.index_definition, 
							i.secret_columns, 
							i.index_usage_summary,
							sz.index_size_summary
					FROM	#index_sanity AS i
					JOIN	#index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	( count_key_columns + count_included_columns ) >= 7
					OPTION	( RECOMPILE );

			RAISERROR(N'check_id 24: Wide clustered indexes (> 3 columns or > 16 bytes).', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								SUM(CASE max_length when -1 THEN 0 ELSE max_length END) AS sum_max_length
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							and key_ordinal > 0
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	24 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Wide clustered index' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								CAST (i.count_key_columns AS NVARCHAR(10)) + N' columns with potential size of '
									+ CAST(cc.sum_max_length AS NVARCHAR(10))
									+ N' bytes in clustered index:' + i.schema_object_name 
									+ N'. ' + 
										(SELECT CAST(COUNT(*) AS NVARCHAR(23)) FROM #index_sanity i2 
										WHERE i2.[object_id]=i.[object_id] AND i2.index_id <> 1
										AND i2.is_disabled=0 AND i2.is_hypothetical=0)
										+ N' NC indexes on the table.'
									AS details,
								i.index_definition,
								secret_columns, 
								i.index_usage_summary,
								ip.index_size_summary
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]	
						WHERE	index_id = 1 /* clustered only */
								AND 
									(count_key_columns > 3 /*More than three key columns.*/
									OR cc.sum_max_length > 15 /*More than 16 bytes in key */)
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 25: Addicted to nullable columns.', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								SUM(CASE is_nullable WHEN 1 THEN 0 ELSE 1 END) as non_nullable_columns,
								COUNT(*) as total_columns
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	25 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Addicted to nulls' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								i.schema_object_name 
									+ N' allows null in ' + CAST((total_columns-non_nullable_columns) as NVARCHAR(10))
									+ N' of ' + CAST(total_columns as NVARCHAR(10))
									+ N' columns.' AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
							AND cc.non_nullable_columns < 2
							and cc.total_columns > 3
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 26: Wide tables (35+ cols or > 2000 non-LOB bytes).', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								SUM(CASE max_length when -1 THEN 1 ELSE 0 END) AS count_lob_columns,
								SUM(CASE max_length when -1 THEN 0 ELSE max_length END) AS sum_max_length,
								COUNT(*) as total_columns
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	26 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Wide tables: 35+ cols or > 2000 non-LOB bytes' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								i.schema_object_name 
									+ N' has ' + CAST((total_columns) as NVARCHAR(10))
									+ N' total columns with a max possible width of ' + CAST(sum_max_length as NVARCHAR(10))
									+ N' bytes.' +
									CASE WHEN count_lob_columns > 0 THEN CAST((count_lob_columns) as NVARCHAR(10))
										+ ' columns are LOB types.' ELSE ''
									END
										AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
							and 
							(cc.total_columns >= 35 OR
							cc.sum_max_length >= 2000)
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );
					
			RAISERROR(N'check_id 27: Addicted to strings.', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								SUM(CASE WHEN system_type_name in ('varchar','nvarchar','char') or max_length=-1 THEN 1 ELSE 0 END) as string_or_LOB_columns,
								COUNT(*) as total_columns
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	27 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Addicted to strings' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								i.schema_object_name 
									+ N' uses string or LOB types for ' + CAST((string_or_LOB_columns) as NVARCHAR(10))
									+ N' of ' + CAST(total_columns as NVARCHAR(10))
									+ N' columns. Check if data types are valid.' AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						CROSS APPLY (SELECT cc.total_columns - string_or_LOB_columns AS non_string_or_lob_columns) AS calc1
						WHERE	i.index_id in (1,0)
							AND calc1.non_string_or_lob_columns <= 1
							AND cc.total_columns > 3
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

		END
		 ----------------------------------------
		--Feature-Phobic Indexes: Check_id 30-39
		---------------------------------------- 
		BEGIN
			RAISERROR(N'check_id 30: No indexes with includes', 0,1) WITH NOWAIT;

			DECLARE	@number_indexes_with_includes INT;
			DECLARE	@percent_indexes_with_includes NUMERIC(10, 1);

			SELECT	@number_indexes_with_includes = SUM(CASE WHEN count_included_columns > 0 THEN 1 ELSE 0	END),
					@percent_indexes_with_includes = 100.* 
						SUM(CASE WHEN count_included_columns > 0 THEN 1 ELSE 0 END) / ( 1.0 * COUNT(*) )
			FROM	#index_sanity;

			IF @number_indexes_with_includes = 0 
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	30 AS check_id, 
								NULL AS index_sanity_id, 
								N'Feature-Phobic Indexes' AS findings_group,
								N'No indexes use includes' AS finding, 'http://BrentOzar.com/go/IndexFeatures' AS URL,
								N'No indexes use includes' AS details,
								N'Entire database' AS index_definition, 
								N'' AS secret_columns, 
								N'N/A' AS index_usage_summary, 
								N'N/A' AS index_size_summary OPTION	( RECOMPILE );

			RAISERROR(N'check_id 31: < 3 percent of indexes have includes', 0,1) WITH NOWAIT;
			IF @percent_indexes_with_includes <= 3 AND @number_indexes_with_includes > 0 
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	31 AS check_id,
								NULL AS index_sanity_id, 
								N'Feature-Phobic Indexes' AS findings_group,
								N'Borderline: Includes are used in < 3% of indexes' AS findings,
								N'http://BrentOzar.com/go/IndexFeatures' AS URL,
								N'Only ' + CAST(@percent_indexes_with_includes AS NVARCHAR(10)) + '% of indexes have includes' AS details, 
								N'Entire database' AS index_definition, 
								N'' AS secret_columns,
								N'N/A' AS index_usage_summary, 
								N'N/A' AS index_size_summary OPTION	( RECOMPILE );

			RAISERROR(N'check_id 32: filtered indexes and indexed views', 0,1) WITH NOWAIT;
			DECLARE @count_filtered_indexes INT;
			DECLARE @count_indexed_views INT;

				SELECT	@count_filtered_indexes=COUNT(*)
				FROM	#index_sanity
				WHERE	filter_definition <> '' OPTION	( RECOMPILE );

				SELECT	@count_indexed_views=COUNT(*)
				FROM	#index_sanity AS i
						JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
				WHERE	is_indexed_view = 1 OPTION	( RECOMPILE );

			IF @count_filtered_indexes = 0 AND @count_indexed_views=0
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	32 AS check_id, 
								NULL AS index_sanity_id,
								N'Feature-Phobic Indexes' AS findings_group,
								N'Borderline: No filtered indexes or indexed views exist' AS finding, 
								N'http://BrentOzar.com/go/IndexFeatures' AS URL,
								N'These are NOT always needed-- but do you know when you would use them?' AS details,
								N'Entire database' AS index_definition, 
								N'' AS secret_columns,
								N'N/A' AS index_usage_summary, 
								N'N/A' AS index_size_summary OPTION	( RECOMPILE );
		END;

		RAISERROR(N'check_id 33: Potential filtered indexes based on column names.', 0,1) WITH NOWAIT;

		INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										secret_columns, index_usage_summary, index_size_summary )
		SELECT	33 AS check_id, 
				i.index_sanity_id AS index_sanity_id,
				N'Feature-Phobic Indexes' AS findings_group,
				N'Potential filtered index (based on column name)' AS finding, 
				N'http://BrentOzar.com/go/IndexFeatures' AS URL,
				N'A column name in this index suggests it might be a candidate for filtering (is%, %archive%, %active%, %flag%)' AS details,
				i.index_definition, 
				i.secret_columns,
				i.index_usage_summary, 
				sz.index_size_summary
		FROM #index_columns ic 
		join #index_sanity i on 
			ic.[object_id]=i.[object_id] and
			ic.[index_id]=i.[index_id] and
			i.[index_id] > 1 /* non-clustered index */
		JOIN	#index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
		WHERE column_name like 'is%'
			or column_name like '%archive%'
			or column_name like '%active%'
			or column_name like '%flag%'
		OPTION	( RECOMPILE );

		 ----------------------------------------
		--Self Loathing Indexes : Check_id 40-49
		----------------------------------------
		BEGIN

			RAISERROR(N'check_id 40: Fillfactor in nonclustered 80 percent or less', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	40 AS check_id, 
							i.index_sanity_id,
							N'Self Loathing Indexes' AS findings_group,
							N'Low Fill Factor: nonclustered index' AS finding, 
							N'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Fill factor on ' + schema_object_indexid + N' is ' + CAST(fill_factor AS NVARCHAR(10)) + N'%. '+
								CASE WHEN (last_user_update is null OR user_updates < 1)
								THEN N'No writes have been made.'
								ELSE
									N'Last write was ' +  CONVERT(NVARCHAR(16),last_user_update,121) + N' and ' + 
									CAST(user_updates as NVARCHAR(25)) + N' updates have been made.'
								END
								AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							sz.index_size_summary
					FROM	#index_sanity AS i
					JOIN	#index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id > 1
					and	fill_factor BETWEEN 1 AND 80 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 40: Fillfactor in clustered 90 percent or less', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	40 AS check_id, 
							i.index_sanity_id,
							N'Self Loathing Indexes' AS findings_group,
							N'Low Fill Factor: clustered index' AS finding, 
							N'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Fill factor on ' + schema_object_indexid + N' is ' + CAST(fill_factor AS NVARCHAR(10)) + N'%. '+
								CASE WHEN (last_user_update is null OR user_updates < 1)
								THEN N'No writes have been made.'
								ELSE
									N'Last write was ' +  CONVERT(NVARCHAR(16),last_user_update,121) + N' and ' + 
									CAST(user_updates as NVARCHAR(25)) + N' updates have been made.'
								END
								AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							sz.index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id = 1
					and fill_factor BETWEEN 1 AND 90 OPTION	( RECOMPILE );


			RAISERROR(N'check_id 41: Hypothetical indexes ', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	41 AS check_id, 
							N'Self Loathing Indexes' AS findings_group,
							N'Hypothetical Index' AS finding, 'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Hypothetical Index: ' + schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							N'' AS index_usage_summary, 
							N'' AS index_size_summary
					FROM	#index_sanity AS i
					WHERE	is_hypothetical = 1 OPTION	( RECOMPILE );


			RAISERROR(N'check_id 42: Disabled indexes', 0,1) WITH NOWAIT;
			--Note: disabled NC indexes will have O rows in #index_sanity_size!
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	42 AS check_id, 
							index_sanity_id,
							N'Self Loathing Indexes' AS findings_group,
							N'Disabled Index' AS finding, 
							N'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Disabled Index:' + schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							'DISABLED' AS index_size_summary
					FROM	#index_sanity AS i
					WHERE	is_disabled = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 43: Heaps with forwarded records or deletes', 0,1) WITH NOWAIT;
			WITH	heaps_cte
					  AS ( SELECT	[object_id], SUM(forwarded_fetch_count) AS forwarded_fetch_count,
									SUM(leaf_delete_count) AS leaf_delete_count
						   FROM		#index_partition_sanity
						   GROUP BY	[object_id]
						   HAVING	SUM(forwarded_fetch_count) > 0
									OR SUM(leaf_delete_count) > 0)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	43 AS check_id, 
								i.index_sanity_id,
								N'Self Loathing Indexes' AS findings_group,
								N'Heaps with forwarded records or deletes' AS finding, 
								N'http://BrentOzar.com/go/SelfLoathing' AS URL,
								CAST(h.forwarded_fetch_count AS NVARCHAR(256)) + ' forwarded fetches, '
								+ CAST(h.leaf_delete_count AS NVARCHAR(256)) + ' deletes against heap:'
								+ schema_object_indexid AS details, 
								i.index_definition, 
								i.secret_columns,
								i.index_usage_summary,
								sz.index_size_summary
						FROM	#index_sanity i
						JOIN heaps_cte h ON i.[object_id] = h.[object_id]
						JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
						WHERE	i.index_id = 0 
				OPTION	( RECOMPILE );

			RAISERROR(N'check_id 44: Heaps with reads or writes.', 0,1) WITH NOWAIT;
			WITH	heaps_cte
					  AS ( SELECT	[object_id], SUM(forwarded_fetch_count) AS forwarded_fetch_count,
									SUM(leaf_delete_count) AS leaf_delete_count
						   FROM		#index_partition_sanity
						   GROUP BY	[object_id]
						   HAVING	SUM(forwarded_fetch_count) > 0
									OR SUM(leaf_delete_count) > 0)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	44 AS check_id, 
								i.index_sanity_id,
								N'Self Loathing Indexes' AS findings_group,
								N'Active heap' AS finding, 
								N'http://BrentOzar.com/go/SelfLoathing' AS URL,
								N'Should this table be a heap? ' + schema_object_indexid AS details, 
								i.index_definition, 
								'N/A' AS secret_columns,
								i.index_usage_summary,
								sz.index_size_summary
						FROM	#index_sanity i
						LEFT JOIN heaps_cte h ON i.[object_id] = h.[object_id]
						JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
						WHERE	i.index_id = 0 
								AND 
									(i.total_reads > 0 OR i.user_updates > 0)
								AND h.[object_id] IS NULL /*don't duplicate the prior check.*/
				OPTION	( RECOMPILE );


			END;
		----------------------------------------
		--Indexaphobia
		--Missing indexes with value >= 5 million: : Check_id 50-59
		----------------------------------------
		BEGIN
			RAISERROR(N'check_id 50: Indexaphobia.', 0,1) WITH NOWAIT;
			WITH	index_size_cte
					  AS ( SELECT	i.[object_id], 
									MAX(i.index_sanity_id) AS index_sanity_id,
								ISNULL (
									CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN 1 ELSE 0 END)
										 AS NVARCHAR(30))+ N' NC indexes exist (' + 
									CASE WHEN SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) > 1024
										THEN CAST(CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END )/1024. 
											AS NUMERIC(29,1)) AS NVARCHAR(30)) + N'GB); ' 
										ELSE CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) 
											AS NVARCHAR(30)) + N'MB); '
									END + 
										CASE WHEN MAX(sz.[total_rows]) >= 922337203685477 THEN '>= 922,337,203,685,477'
										ELSE REPLACE(CONVERT(NVARCHAR(30),CAST(MAX(sz.[total_rows]) AS money), 1), '.00', '') 
										END +
									+ N' Estimated Rows;' 
								,N'') AS index_size_summary
							FROM	#index_sanity AS i
							LEFT	JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
						   GROUP BY	i.[object_id])
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   index_usage_summary, index_size_summary, create_tsql, more_info )
						SELECT	50 AS check_id, 
								sz.index_sanity_id,
								N'Indexaphobia' AS findings_group,
								N'High value missing index' AS finding, 
								N'http://BrentOzar.com/go/Indexaphobia' AS URL,
								mi.[statement] + ' estimated benefit: ' + 
									CASE WHEN magic_benefit_number >= 922337203685477 THEN '>= 922,337,203,685,477'
									ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(magic_benefit_number AS BIGINT) AS money), 1), '.00', '') 
									END AS details,
								missing_index_details AS [definition],
								index_estimated_impact,
								sz.index_size_summary,
								mi.create_tsql,
								mi.more_info
				FROM	#missing_indexes mi
						LEFT JOIN index_size_cte sz ON mi.[object_id] = sz.object_id
				WHERE magic_benefit_number > 500000
				ORDER BY magic_benefit_number DESC;

	END
		 ----------------------------------------
		--Abnormal Psychology : Check_id 60-69
		----------------------------------------
	BEGIN
			RAISERROR(N'check_id 60: XML indexes', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	60 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'XML Indexes' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							N'' AS index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_XML = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 61: NC Columnstore indexes', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	61 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'NC Columnstore indexes' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_NC_columnstore = 1 OPTION	( RECOMPILE );


			RAISERROR(N'check_id 62: Spatial indexes', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	62 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Spatial indexes' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_spatial = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 63: Compressed indexes', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	63 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Compressed indexes' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid  + N'. COMPRESSION: ' + sz.data_compression_desc AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE sz.data_compression_desc LIKE '%PAGE%' OR sz.data_compression_desc LIKE '%ROW%' OPTION	( RECOMPILE );

			RAISERROR(N'check_id 64: Partitioned', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	64 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Partitioned indexes' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NOT NULL OPTION	( RECOMPILE );

			RAISERROR(N'check_id 65: Non-Aligned Partitioned', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	65 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Non-Aligned index on a partitioned table' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity AS iParent ON
						i.[object_id]=iParent.[object_id]
						AND iParent.index_id IN (0,1) /* could be a partitioned heap or clustered table */
						AND iParent.partition_key_column_name IS NOT NULL /* parent is partitioned*/         
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NULL 
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 66: Recently created tables/indexes (1 week)', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	66 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Recently created tables/indexes (1 week)' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid + N' was created on ' + 
								CONVERT(NVARCHAR(16),i.create_date,121) + 
								N'. Tables/indexes which are dropped/created regularly require special methods for index tuning.'
									 AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.create_date >= DATEADD(dd,-7,GETDATE()) 
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 67: Recently modified tables/indexes (2 days)', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	67 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							N'Recently modified tables/indexes (2 days)' AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid + N' was modified on ' + 
								CONVERT(NVARCHAR(16),i.modify_date,121) + 
								N'. A large amount of recently modified indexes may mean a lot of rebuilds are occurring each night.'
									 AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#index_sanity AS i
					JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.modify_date > DATEADD(dd,-2,GETDATE()) 
					and /*Exclude recently created tables unless they've been modified after being created.*/
					(i.create_date < DATEADD(dd,-7,GETDATE()) or i.create_date <> i.modify_date)
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 68: Identity columns within 30% of the end of range', 0,1) WITH NOWAIT;
			-- Allowed Ranges: 
				--int -2,147,483,648 to 2,147,483,647
				--smallint -32,768 to 32,768
				--tinyint 0 to 255
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	68 AS check_id, 
								i.index_sanity_id, 
								N'Abnormal Psychology' AS findings_group,
								N'Identity column within ' + 									
									CAST (calc1.percent_remaining as nvarchar(256))
									+ N'% of end of range' AS finding,
								N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
								i.schema_object_name + N'.' +  QUOTENAME(ic.column_name)
									+ N' is an identity with type ' + ic.system_type_name 
									+ N', last value of ' 
										+ ISNULL(REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(ic.last_value AS BIGINT) AS money), 1), '.00', ''),N'NULL')
									+ N', seed of '
										+ ISNULL(REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(ic.seed_value AS BIGINT) AS money), 1), '.00', ''),N'NULL')
									+ N', increment of ' + CAST(ic.increment_value AS NVARCHAR(256)) 
									+ N', and range of ' +
										CASE ic.system_type_name WHEN 'int' THEN N'+/- 2,147,483,647'
											WHEN 'smallint' THEN N'+/- 32,768'
											WHEN 'tinyint' THEN N'0 to 255'
										END
										AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_columns ic on
							i.object_id=ic.object_id
							and ic.is_identity=1
							and ic.system_type_name in ('tinyint', 'smallint', 'int')
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						CROSS APPLY (
							SELECT CAST(CASE WHEN ic.increment_value >= 0
									THEN
										CASE ic.system_type_name 
											WHEN 'int' then (2147483647 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 2147483647.*100
											WHEN 'smallint' then (32768 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 32768.*100
											WHEN 'tinyint' then ( 255 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 255.*100
											ELSE 999
										END
								ELSE --ic.increment_value is negative
										CASE ic.system_type_name 
											WHEN 'int' then ABS(-2147483647 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 2147483647.*100
											WHEN 'smallint' then ABS(-32768 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 32768.*100
											WHEN 'tinyint' then ABS( 0 - (ISNULL(ic.last_value,ic.seed_value) + ic.increment_value)) / 255.*100
											ELSE -1
										END 
								END AS NUMERIC(4,1)) AS percent_remaining
								) as calc1
						WHERE	i.index_id in (1,0)
							and calc1.percent_remaining <= 30
						UNION ALL
						SELECT	68 AS check_id, 
								i.index_sanity_id, 
								N'Abnormal Psychology' AS findings_group,
								N'Identity column using a negative seed or increment other than 1' AS finding,
								N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
								i.schema_object_name + N'.' +  QUOTENAME(ic.column_name)
									+ N' is an identity with type ' + ic.system_type_name 
									+ N', last value of ' 
										+ ISNULL(REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(ic.last_value AS BIGINT) AS money), 1), '.00', ''),N'NULL')
									+ N', seed of '
										+ ISNULL(REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(ic.seed_value AS BIGINT) AS money), 1), '.00', ''),N'NULL')
									+ N', increment of ' + CAST(ic.increment_value AS NVARCHAR(256)) 
									+ N', and range of ' +
										CASE ic.system_type_name WHEN 'int' THEN N'+/- 2,147,483,647'
											WHEN 'smallint' THEN N'+/- 32,768'
											WHEN 'tinyint' THEN N'0 to 255'
										END
										AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_columns ic on
							i.object_id=ic.object_id
							and ic.is_identity=1
							and ic.system_type_name in ('tinyint', 'smallint', 'int')
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	i.index_id in (1,0)
							and (ic.seed_value < 0 or ic.increment_value <> 1)
						ORDER BY finding, details DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 69: Column collation does not match database collation', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								COUNT(*) as column_count
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
								and collation_name <> @collation
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	69 AS check_id, 
								i.index_sanity_id, 
								N'Abnormal Psychology' AS findings_group,
								N'Column collation does not match database collation' AS finding,
								N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
								i.schema_object_name 
									+ N' has ' + CAST(column_count AS NVARCHAR(20))
									+ N' column' + CASE WHEN column_count > 1 THEN 's' ELSE '' END
									+ N' with a different collation than the db collation of '
									+ @collation	AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 70: Replicated columns', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								COUNT(*) as column_count,
								SUM(CASE is_replicated WHEN 1 THEN 1 ELSE 0 END) as replicated_column_count
							FROM #index_columns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	70 AS check_id, 
								i.index_sanity_id, 
								N'Abnormal Psychology' AS findings_group,
								N'Replicated columns' AS finding,
								N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
								i.schema_object_name 
									+ N' has ' + CAST(replicated_column_count AS NVARCHAR(20))
									+ N' out of ' + CAST(column_count AS NVARCHAR(20))
									+ N' column' + CASE WHEN column_count > 1 THEN 's' ELSE '' END
									+ N' in one or more publications.'
										AS details,
								i.index_definition,
								secret_columns, 
								ISNULL(i.index_usage_summary,''),
								ISNULL(ip.index_size_summary,'')
						FROM	#index_sanity i
						JOIN	#index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
							and replicated_column_count > 0
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 71: Cascading updates or cascading deletes.', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
								   secret_columns, index_usage_summary, index_size_summary, more_info )
			SELECT	71 AS check_id, 
					null as index_sanity_id,
					N'Abnormal Psychology' AS findings_group,
					N'Cascading Updates or Deletes' AS finding, 
					N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
					N'Foreign Key ' + foreign_key_name +
					N' on ' + QUOTENAME(parent_object_name)  + N'(' + LTRIM(parent_fk_columns) + N')'
						+ N' referencing ' + QUOTENAME(referenced_object_name) + N'(' + LTRIM(referenced_fk_columns) + N')'
						+ N' has settings:'
						+ CASE [delete_referential_action_desc] WHEN N'NO_ACTION' THEN N'' ELSE N' ON DELETE ' +[delete_referential_action_desc] END
						+ CASE [update_referential_action_desc] WHEN N'NO_ACTION' THEN N'' ELSE N' ON UPDATE ' + [update_referential_action_desc] END
							AS details, 
					N'N/A' 
							AS index_definition, 
					N'N/A' AS secret_columns,
					N'N/A' AS index_usage_summary,
					N'N/A' AS index_size_summary,
					(SELECT TOP 1 more_info from #index_sanity i where i.object_id=fk.parent_object_id)
						AS more_info
			from #foreign_keys fk
			where [delete_referential_action_desc] <> N'NO_ACTION'
			OR [update_referential_action_desc] <> N'NO_ACTION'

	END
		 ----------------------------------------
		--FINISHING UP
		----------------------------------------
	BEGIN
				INSERT	#blitz_index_results ( check_id, findings_group, finding, URL, details, index_definition,secret_columns,
											   index_usage_summary, index_size_summary )
				VALUES  ( 1000 , N'Database=' + @database_name,
						N' Learn how to use this script at:' ,   N'http://www.BrentOzar.com/BlitzIndex' ,
						N'Thanks from the Brent Ozar Unlimited, LLC team.',
						N'We hope you found this tool useful.',
						N'If you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
						, N'',N''
						);


	END
		RAISERROR(N'Returning results.', 0,1) WITH NOWAIT;
			
		/*Return results.*/
		SELECT br.findings_group + 
			N': ' + br.finding AS [Finding], 
			br.URL, 
			br.details AS [Details: schema.table.index(indexid)], 
			br.index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
			ISNULL(br.secret_columns,'') AS [Secret Columns],          
			br.index_usage_summary AS [Usage], 
			br.index_size_summary AS [Size],
			COALESCE(br.more_info,sn.more_info,'') AS [More Info],
			COALESCE(br.create_tsql,ts.create_tsql,'') AS [Create TSQL]
		FROM #blitz_index_results br
		LEFT JOIN #index_sanity sn ON 
			br.index_sanity_id=sn.index_sanity_id
		LEFT JOIN #index_create_tsql ts ON 
			br.index_sanity_id=ts.index_sanity_id
		ORDER BY [check_id] ASC, blitz_result_id ASC, findings_group;

	END; /* End @mode=0 (diagnose)*/
	ELSE IF @mode=1 /*Summarize*/
	BEGIN
	--This mode is to give some overall stats on the database.
		RAISERROR(N'@mode=1, we are summarizing.', 0,1) WITH NOWAIT;

		SELECT 
			CAST((COUNT(*)) AS NVARCHAR(256)) AS [Number Objects],
			CAST(CAST(SUM(sz.total_reserved_MB)/
				1024. AS numeric(29,1)) AS NVARCHAR(500)) AS [All GB],
			CAST(CAST(SUM(sz.total_reserved_LOB_MB)/
				1024. AS numeric(29,1)) AS NVARCHAR(500)) AS [LOB GB],
			CAST(CAST(SUM(sz.total_reserved_row_overflow_MB)/
				1024. AS numeric(29,1)) AS NVARCHAR(500)) AS [Row Overflow GB],
			CAST(SUM(CASE WHEN index_id=1 THEN 1 ELSE 0 END)AS NVARCHAR(50)) AS [Clustered Tables],
			CAST(SUM(CASE WHEN index_id=1 THEN sz.total_reserved_MB ELSE 0 END)
				/1024. AS numeric(29,1)) AS [Clustered Tables GB],
			SUM(CASE WHEN index_id NOT IN (0,1) THEN 1 ELSE 0 END) AS [NC Indexes],
			CAST(SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
				/1024. AS numeric(29,1)) AS [NC Indexes GB],
			CASE WHEN SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)  > 0 THEN
				CAST(SUM(CASE WHEN index_id IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
					/ SUM(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END) AS NUMERIC(29,1)) 
				ELSE 0 END AS [ratio table: NC Indexes],
			SUM(CASE WHEN index_id=0 THEN 1 ELSE 0 END) AS [Heaps],
			CAST(SUM(CASE WHEN index_id=0 THEN sz.total_reserved_MB ELSE 0 END)
				/1024. AS numeric(29,1)) AS [Heaps GB],
			SUM(CASE WHEN index_id IN (0,1) AND partition_key_column_name IS NOT NULL THEN 1 ELSE 0 END) AS [Partitioned Tables],
			SUM(CASE WHEN index_id NOT IN (0,1) AND  partition_key_column_name IS NOT NULL THEN 1 ELSE 0 END) AS [Partitioned NCs],
			CAST(SUM(CASE WHEN partition_key_column_name IS NOT NULL THEN sz.total_reserved_MB ELSE 0 END)/1024. AS numeric(29,1)) AS [Partitioned GB],
			SUM(CASE WHEN filter_definition <> '' THEN 1 ELSE 0 END) AS [Filtered Indexes],
			SUM(CASE WHEN is_indexed_view=1 THEN 1 ELSE 0 END) AS [Indexed Views],
			MAX(total_rows) AS [Max Row Count],
			CAST(MAX(CASE WHEN index_id IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
				/1024. AS numeric(29,1)) AS [Max Table GB],
			CAST(MAX(CASE WHEN index_id NOT IN (0,1) THEN sz.total_reserved_MB ELSE 0 END)
				/1024. AS numeric(29,1)) AS [Max NC Index GB],
			SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 1024 THEN 1 ELSE 0 END) AS [Count Tables > 1GB],
			SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 10240 THEN 1 ELSE 0 END) AS [Count Tables > 10GB],
			SUM(CASE WHEN index_id IN (0,1) AND sz.total_reserved_MB > 102400 THEN 1 ELSE 0 END) AS [Count Tables > 100GB],	
			SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 1024 THEN 1 ELSE 0 END) AS [Count NCs > 1GB],
			SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 10240 THEN 1 ELSE 0 END) AS [Count NCs > 10GB],
			SUM(CASE WHEN index_id NOT IN (0,1) AND sz.total_reserved_MB > 102400 THEN 1 ELSE 0 END) AS [Count NCs > 100GB],
			MIN(create_date) AS [Oldest Create Date],
			MAX(create_date) AS [Most Recent Create Date],
			MAX(modify_date) as [Most Recent Modify Date],
			1 as [Display Order]
		FROM #index_sanity AS i
		--left join here so we don't lose disabled nc indexes
		LEFT JOIN #index_sanity_size AS sz 
			ON i.index_sanity_id=sz.index_sanity_id 
		UNION ALL
		SELECT	N'Database='+ @database_name,		
				N'sp_BlitzIndex version 2.0 (May 15, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,0 as display_order
		ORDER BY [Display Order] ASC
		OPTION (RECOMPILE);
	   	
	END /* End @mode=1 (summarize)*/
	ELSE IF @mode=2 /*Index Detail*/
	BEGIN
		--This mode just spits out all the detail without filters.
		--This supports slicing AND dicing in Excel
		RAISERROR(N'@mode=2, here''s the details on existing indexes.', 0,1) WITH NOWAIT;

		SELECT	database_name AS [Database Name], 
				[schema_name] AS [Schema Name], 
				[object_name] AS [Object Name], 
				ISNULL(index_name, '') AS [Index Name], 
				cast(index_id as VARCHAR(10))AS [Index ID],
				schema_object_indexid AS [Details: schema.table.index(indexid)], 
				CASE	WHEN index_id IN ( 1, 0 ) THEN 'TABLE'
					ELSE 'NonClustered'
					END AS [Object Type], 
				index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}],
				ISNULL(LTRIM(key_column_names_with_sort_order), '') AS [Key Column Names With Sort],
				ISNULL(count_key_columns, 0) AS [Count Key Columns],
				ISNULL(include_column_names, '') AS [Include Column Names], 
				ISNULL(count_included_columns,0) AS [Count Included Columns],
				ISNULL(secret_columns,'') AS [Secret Column Names], 
				ISNULL(count_secret_columns,0) AS [Count Secret Columns],
				ISNULL(partition_key_column_name, '') AS [Partition Key Column Name],
				ISNULL(filter_definition, '') AS [Filter Definition], 
				is_indexed_view AS [Is Indexed View], 
				is_primary_key AS [Is Primary Key],
				is_XML AS [Is XML],
				is_spatial AS [Is Spatial],
				is_NC_columnstore AS [Is NC Columnstore],
				is_disabled AS [Is Disabled], 
				is_hypothetical AS [Is Hypothetical],
				is_padded AS [Is Padded], 
				fill_factor AS [Fill Factor], 
				is_referenced_by_foreign_key AS [Is Reference by Foreign Key], 
				last_user_seek AS [Last User Seek], 
				last_user_scan AS [Last User Scan], 
				last_user_lookup AS [Last User Lookup],
				last_user_update AS [Last User Update], 
				total_reads AS [Total Reads], 
				user_updates AS [User Updates], 
				reads_per_write AS [Reads Per Write], 
				index_usage_summary AS [Index Usage], 
				sz.partition_count AS [Partition Count],
				sz.total_rows AS [Rows], 
				sz.total_reserved_MB AS [Reserved MB], 
				sz.total_reserved_LOB_MB AS [Reserved LOB MB], 
				sz.total_reserved_row_overflow_MB AS [Reserved Row Overflow MB],
				sz.index_size_summary AS [Index Size], 
				sz.total_row_lock_count AS [Row Lock Count],
				sz.total_row_lock_wait_count AS [Row Lock Wait Count],
				sz.total_row_lock_wait_in_ms AS [Row Lock Wait ms],
				sz.avg_row_lock_wait_in_ms AS [Avg Row Lock Wait ms],
				sz.total_page_lock_count AS [Page Lock Count],
				sz.total_page_lock_wait_count AS [Page Lock Wait Count],
				sz.total_page_lock_wait_in_ms AS [Page Lock Wait ms],
				sz.avg_page_lock_wait_in_ms AS [Avg Page Lock Wait ms],
				sz.total_index_lock_promotion_attempt_count AS [Lock Escalation Attempts],
				sz.total_index_lock_promotion_count AS [Lock Escalations],
				sz.data_compression_desc AS [Data Compression],
				i.create_date AS [Create Date],
				i.modify_date as [Modify Date],
				more_info AS [More Info],
				1 as [Display Order]
		FROM	#index_sanity AS i --left join here so we don't lose disabled nc indexes
				LEFT JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
		UNION ALL
		SELECT 	N'Database=' + @database_name,			
				N'sp_BlitzIndex version 2.0 (May 15, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL, NULL,NULL, NULL, NULL, NULL, NULL,
				0 as [Display Order]
		ORDER BY [Display Order] ASC, [Reserved MB] DESC
		OPTION (RECOMPILE);

	END /* End @mode=2 (index detail)*/
	ELSE IF @mode=3 /*Missing index Detail*/
	BEGIN
		SELECT 
			database_name AS [Database], 
			[schema_name] AS [Schema], 
			table_name AS [Table], 
			CAST(magic_benefit_number AS BIGINT)
				AS [Magic Benefit Number], 
			missing_index_details AS [Missing Index Details], 
			avg_total_user_cost AS [Avg Query Cost], 
			avg_user_impact AS [Est Index Improvement], 
			user_seeks AS [Seeks], 
			user_scans AS [Scans],
			unique_compiles AS [Compiles], 
			equality_columns AS [Equality Columns], 
			inequality_columns AS [Inequality Columns], 
			included_columns AS [Included Columns], 
			index_estimated_impact AS [Estimated Impact], 
			create_tsql AS [Create TSQL], 
			more_info AS [More Info],
			1 as [Display Order]
		FROM #missing_indexes
		UNION ALL
		SELECT 				
			N'sp_BlitzIndex version 2.0 (May 15, 2013)' ,   
			N'From Brent Ozar Unlimited' ,   
			N'http://BrentOzar.com/BlitzIndex' ,
			100000000000,
			N'Thanks from the Brent Ozar Unlimited team. We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
			NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
			NULL, 0 as display_order
		ORDER BY [Display Order] ASC, [Magic Benefit Number] DESC

	END /* End @mode=3 (index detail)*/
END
END TRY
BEGIN CATCH
		RAISERROR (N'Failure analyzing temp tables.', 0,1) WITH NOWAIT;

		SELECT	@msg = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();

		RAISERROR (@msg, 
               @ErrorSeverity, 
               @ErrorState 
               );
		
		WHILE @@trancount > 0 
			ROLLBACK;

		RETURN;
	END CATCH;
GO

