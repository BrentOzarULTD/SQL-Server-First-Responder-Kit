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
	@database_name NVARCHAR(256),
	@mode tinyint=0, /*0=diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail*/
	@schema_name NVARCHAR(256) = NULL /*Requires table_name as well.*/,
	@table_name NVARCHAR(256) = NULL  /*Requires schema_name as well. @mode doesn't matter if you're specifying a table.*/
/*
sp_BlitzIndex (TM) v1.5 - April 8, 2013

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
 - Index create statements are just to give you a rough idea-- they do not include all the options the index may have been created with (padding, etc.)
 - Doesn't advise you about data modeling for clustered indexes and primary keys (primarily looks for signs of insanity.)
 - Found something? Let us know at help@brentozar.com.

CHANGE LOG (last four versions):
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

SELECT @SQLServerProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @SQLServerEdition =CAST(SERVERPROPERTY('EngineEdition') AS INT); /* We default to online index creates were EngineEdition=3*/

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
    
		--Validate parameters.
		IF (@mode NOT IN (0,1,2,3))
		BEGIN
			SET @msg=N'Invalid @mode parameter. 0=diagnose, 1=summarize, 2=index detail, 3=missing index detail';
			RAISERROR(@msg,16,1);
		END

		IF (@schema_name IS NOT NULL AND @table_name IS NULL) OR (@table_name IS NOT NULL AND @schema_name IS NULL)
		BEGIN
			SET @msg='You must specify both @schema_name and @table_name, or leave both NULL for summary info.'
			RAISERROR(@msg,16,1);
		END

		--Short circuit here if database name does not exist.
		IF @database_name IS NULL OR @database_id IS NULL
		BEGIN
			SET @msg='Database does not exist or is not online/multi-user: cannot proceed.'
			RAISERROR(@msg,16,1);
		END

		--If a table is specified, grab the object id.
		--Short circuit if it doesn't exist.
		IF @table_name IS NOT NULL
		BEGIN
			SET @dsql = N'
					SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
					SELECT	@object_id= OBJECT_ID
					FROM	' + QUOTENAME(@database_name) + '.sys.objects AS so
					JOIN	' + QUOTENAME(@database_name) + '.sys.schemas AS sc on 
						so.schema_id=sc.schema_id
					where so.type=''U''
					and so.name=' + QUOTENAME(@table_name,'''')+ '
					and sc.name=' + QUOTENAME(@schema_name,'''')+ '
					OPTION (RECOMPILE);';

			SET @params='@object_id INT OUTPUT'				

			IF @dsql IS NULL 
				RAISERROR('@dsql is null',16,1);

			EXEC sp_executesql @dsql, @params, @object_id=@object_id OUTPUT;
			
			IF @object_id IS NULL
					BEGIN
						SET @msg='Table does not exist in specified database, please check parameters.'
						RAISERROR(@msg,16,1);
					END
		END

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
			  count_key_columns INT NULL ,
			  include_column_names NVARCHAR(MAX) NULL ,
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
			  count_secret_columns INT NULL
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
			  [key_ordinal] INT NOT NULL ,
			  [partition_ordinal] INT NOT NULL ,
			  column_name NVARCHAR(256) NOT NULL ,
			  is_included_column BIT NOT NULL ,
			  is_descending_key BIT NOT NULL
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
			referenced_fk_columns NVARCHAR(MAX)
		)
		
		CREATE TABLE #index_create_tsql (
			index_sanity_id INT NOT NULL,
			create_tsql NVARCHAR(MAX) NOT NULL
		)

		SET @dsql = N'
				SELECT	sc.object_id, sc.index_id, sc.key_ordinal, sc.partition_ordinal,
					c.name, sc.is_included_column, sc.is_descending_key
				FROM	' + QUOTENAME(@database_name) + '.sys.index_columns sc
				LEFT JOIN ' + QUOTENAME(@database_name) + '.sys.columns c ON sc.object_id = c.object_id
							   AND sc.column_id = c.column_id
				' + CASE WHEN @object_id IS NOT NULL THEN 'WHERE sc.object_id=' + CAST(@object_id AS NVARCHAR(30)) + N'; ' ELSE N';' END ;

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #index_columns',0,1) WITH NOWAIT;
		INSERT	#index_columns ( object_id, index_id, key_ordinal, partition_ordinal,
			column_name, is_included_column, is_descending_key )
				EXEC sp_executesql @dsql;

		SET @dsql = N'
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
						us.last_user_lookup, us.last_user_update
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
								user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update )
				EXEC sp_executesql @dsql;

		RAISERROR (N'Updating #index_sanity.key_column_names',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		key_column_names = D1.key_column_names
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name AS col_definition
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
																				END AS col_definition
												FROM	#index_columns c
												WHERE	c.object_id = si.object_id
														AND c.index_id = si.index_id
														AND c.is_included_column = 0 /*Just Keys*/
														AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
												ORDER BY c.object_id, c.index_id, c.key_ordinal	
										FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1, ''))
										) D2 ( key_column_names_with_sort_order )


		RAISERROR (N'Updating #index_sanity.include_column_names',0,1) WITH NOWAIT;
		UPDATE	#index_sanity
		SET		include_column_names = D3.include_column_names
		FROM	#index_sanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name
												FROM	#index_columns c
												WHERE	c.object_id = si.object_id
														AND c.index_id = si.index_id
														AND c.is_included_column = 1 /*Just includes*/
												ORDER BY c.column_name /*Order doesn't matter in includes, 
														this is here to make rows easy to compare.*/ 
										FOR	  XML PATH('') ,  TYPE).value('.', 'varchar(max)'), 1, 1, ''))
										) D3 ( include_column_names );

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
			' + CASE WHEN @object_id IS NOT NULL THEN N'WHERE so.object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' ' ELSE N' ' END + '
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
								' + case when @SQLServerProductVersion not like '9%' THEN 'par.data_compression_desc ' ELSE 'null as data_compression_desc' END + '
						FROM	' + QUOTENAME(@database_name) + '.sys.dm_db_partition_stats AS ps  
						JOIN ' + QUOTENAME(@database_name) + '.sys.partitions AS par on ps.partition_id=par.partition_id
						JOIN ' + QUOTENAME(@database_name) + '.sys.objects AS so ON ps.object_id = so.object_id
								   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
								   AND so.type <> ''TF'' /*Exclude table valued functions*/
						OUTER APPLY ' + QUOTENAME(@database_name) + '.sys.dm_db_index_operational_stats('
					+ CAST(@database_id AS NVARCHAR(10)) + ', ps.object_id, ps.index_id,ps.partition_number) AS os
				' + CASE WHEN @object_id IS NOT NULL THEN N'WHERE so.object_id=' + CAST(@object_id AS NVARCHAR(30)) + N' ' ELSE N' ' END + '
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
				referenced.fk_columns
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
                                is_disabled, is_not_trusted, is_not_for_replication, parent_fk_columns, referenced_fk_columns )
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
				ELSE N'' END + CASE WHEN count_key_columns > 0 THEN N'[KEYS] '	+ LTRIM(key_column_names_with_sort_order)
				ELSE N'' END + CASE WHEN count_included_columns > 0 THEN N' [INCLUDES] ' + include_column_names
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
			N', @schema_name=	' + QUOTENAME([schema_name],'''') + N', @table_name=' + QUOTENAME([object_name],'''') + N';'

		RAISERROR (N'Update index_secret on #index_sanity for NC indexes.',0,1) WITH NOWAIT;
		UPDATE nc 
		SET secret_columns=
			CASE nc.is_unique WHEN 1 THEN N'[INCLUDES] ' ELSE N'[KEYS] ' END +
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
								key_column_names_with_sort_order + N' )' 
						ELSE /*End PK index CASE */ 
							N'CREATE ' + 
							CASE WHEN is_unique=1 THEN N'UNIQUE ' ELSE N'' END +
							CASE WHEN index_id=1 THEN N'CLUSTERED ' ELSE N'' END +
							CASE WHEN is_NC_columnstore=1 THEN N'NONCLUSTERED COLUMNSTORE ' ELSE N'' END +
							N' INDEX ['
								 + index_name + N'] ON ' + 
								QUOTENAME([schema_name]) + '.' + QUOTENAME([object_name]) + 
									CASE WHEN is_NC_columnstore=1 THEN 
										N' (' + ISNULL(include_column_names,'') +  N' )' 
									ELSE /*End non-colunnstore case */ 
										N' (' + ISNULL(key_column_names_with_sort_order,'') +  N' )' 
										+ CASE WHEN include_column_names IS NOT NULL THEN 
											N' INCLUDE (' + include_column_names + N')' 
											ELSE N'' 
										END
									END /*End non-colunnstore case */ 
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
				N'sp_BlitzIndex version 1.5 (Mar 8, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,
				0 as display_order
	)
	SELECT 
			schema_object_indexid, 
			index_definition, 
			secret_columns,
			index_usage_summary, 
			index_size_summary,
			index_lock_wait_summary,
			is_referenced_by_foreign_key,
			FKs_covered_by_index,
			create_tsql
	FROM table_mode_cte
	ORDER BY display_order ASC, key_column_names ASC
	OPTION	( RECOMPILE );

	IF (SELECT TOP 1 [object_id] FROM    #missing_indexes mi) IS NOT NULL
	BEGIN  
		SELECT  N'Missing index.' AS finding ,
				N'http://BrentOzar.com/go/Indexaphobia' AS URL ,
				mi.[statement] + ' Est Benefit: '
					+ CASE WHEN magic_benefit_number >= 922337203685477 THEN '>= 922,337,203,685,477'
					ELSE REPLACE(CONVERT(NVARCHAR(256),CAST(CAST(magic_benefit_number AS BIGINT) AS money), 1), '.00', '')
					END AS details,
				missing_index_details AS [index_definition] ,
				index_estimated_impact,
				create_tsql
		FROM    #missing_indexes mi
		WHERE   [object_id] = @object_id
		ORDER BY magic_benefit_number DESC
		OPTION	( RECOMPILE );
	END       
	ELSE     
	SELECT 'No missing indexes.' AS finding;

	IF (SELECT TOP 1 parent_object_id FROM #foreign_keys) IS NOT NULL
	BEGIN
		SELECT parent_object_name + N': ' + foreign_key_name AS foreign_key,
			parent_fk_columns AS foreign_key_columns,
			referenced_object_name AS referenced_table,
			referenced_fk_columns AS referenced_table_columns,
			is_disabled,
			is_not_trusted,
			is_not_for_replication
		FROM #foreign_keys
		ORDER BY foreign_key
		OPTION	( RECOMPILE );
	END
	ELSE
	SELECT 'No foreign keys.' AS finding;
END 

--If @table_name is NOT specified...
--Act based on the mode!
ELSE
BEGIN;
	IF @mode=0 /* DIAGNOSE*/
	BEGIN;
		RAISERROR(N'@mode=0, we are diagnosing.', 0,1) WITH NOWAIT;

		RAISERROR(N'Insert a row to help people find help', 0,1) WITH NOWAIT;
		INSERT	#blitz_index_results ( check_id, findings_group, finding, URL, details, index_definition,
										index_usage_summary, index_size_summary )
		VALUES  ( 0 , N'sp_BlitzIndex version 1.5 (Mar 8, 2013)' ,   N'From Brent Ozar Unlimited' ,   N'http://BrentOzar.com/BlitzIndex' ,
					N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
					, N'',N'',N''
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
								ISNULL(ips.index_size_summary,'N/A') /*Disabled*/
						FROM	duplicate_indexes di
								JOIN #index_sanity ip ON di.[object_id] = ip.[object_id]
														 AND ip.key_column_names = di.key_column_names
								LEFT JOIN #index_sanity_size ips ON ip.index_sanity_id = ips.index_sanity_id
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
								ISNULL(ips.index_size_summary,'N/A') /*Disabled*/
						FROM	#index_sanity AS ip 
						LEFT JOIN #index_sanity_size ips ON ip.index_sanity_id = ips.index_sanity_id
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
						JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
				WHERE	index_id NOT IN ( 0, 1 ) 
				OPTION	( RECOMPILE );

		--REPLACE(CONVERT(NVARCHAR(30),CAST(MAX([total_rows]) AS money), 1), '.00', '')

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
								JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
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
							JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	i.total_reads=0
							AND i.index_id NOT IN (0,1) /*NCs only*/
					ORDER BY i.schema_object_indexid
					OPTION	( RECOMPILE );

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
							JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	( count_key_columns + count_included_columns ) >= 7
					OPTION	( RECOMPILE );

			RAISERROR(N'check_id 24: Clustered indexes with > 1 column.', 0,1) WITH NOWAIT;
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	24 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Multi-column clustered index' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								CAST (i.count_key_columns AS NVARCHAR(10)) + N' columns in clustered index:' + i.schema_object_name 
									+ N'. ' + (SELECT CAST(COUNT(*) AS NVARCHAR(23)) FROM #index_sanity i2 
										WHERE i2.[object_id]=i.[object_id] AND i2.index_id <> 1
										AND i2.is_disabled=0 AND i2.is_hypothetical=0)
										+ N' NC indexes on the table.'
									AS details,
								i.index_definition,
								secret_columns, 
								i.index_usage_summary,
								ip.index_size_summary
						FROM	#index_sanity i
								JOIN #index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	index_id =1 /* clustered only */
								AND count_key_columns > 1 /*More than one key column.*/
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

		END


			RAISERROR(N'check_id 25: Non-Unique Clustered Indexes.', 0,1) WITH NOWAIT;
				INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	24 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Non-Unique clustered index' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								N'The clustered index on ' + i.schema_object_name 
									+ N' may incur overhead from uniquifiers.' AS details,
								i.index_definition,
								secret_columns, 
								i.index_usage_summary,
								ip.index_size_summary
						FROM	#index_sanity i
								JOIN #index_sanity_size ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	index_id =1 /* clustered only */
								AND is_unique = 0 /*is not unique*/
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

			RAISERROR(N'check_id 31: < 3% of indexes have includes', 0,1) WITH NOWAIT;
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
		 ----------------------------------------
		--Self Loathing Indexes : Check_id 40-49
		----------------------------------------
		BEGIN

			RAISERROR(N'check_id 40: Fillfactor less than or equal to 80 percent', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	40 AS check_id, 
							i.index_sanity_id,
							N'Self Loathing Indexes' AS findings_group,
							N'Low Fill Factor' AS finding, 
							N'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Fill factor on ' + schema_object_indexid + N' is ' + CAST(fill_factor AS NVARCHAR(10)) + N'%' AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							sz.index_size_summary
					FROM	#index_sanity AS i
							JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	fill_factor BETWEEN 1 AND 80 OPTION	( RECOMPILE );

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


		 ----------------------------------------
		--Abnormal Psychology : Check_id 60-69
		----------------------------------------
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NOT NULL OPTION	( RECOMPILE );

			RAISERROR(N'check_id 64: Non-Aligned Partitioned', 0,1) WITH NOWAIT;
			INSERT	#blitz_index_results ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	64 AS check_id, 
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
					LEFT JOIN #index_sanity_size sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NULL 
						OPTION	( RECOMPILE );


		 ----------------------------------------
		--FINISHING UP
		----------------------------------------
				INSERT	#blitz_index_results ( check_id, findings_group, finding, URL, details, index_definition,
											   index_usage_summary, index_size_summary )
				VALUES  ( 1000 , N'All done!' ,   N' Learn how to use this script at:' ,   N'http://www.BrentOzar.com/BlitzIndex' ,
						  N'Thanks from the Brent Ozar Unlimited, LLC team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
						  , N'',N'',N''
						);


		END
	
		/*Return results.*/
		SELECT br.findings_group + 
			CASE WHEN findings_group NOT LIKE N'All done!' THEN N': '  ELSE N'' END + br.finding AS [Finding], 
			br.URL, 
			br.details AS [details: schema.table.index(indexid)], 
			br.index_definition, 
			ISNULL(br.secret_columns,'') AS secret_columns,          
			br.index_usage_summary, 
			br.index_size_summary,
			COALESCE(br.more_info,sn.more_info,'') AS more_info,
			COALESCE(br.create_tsql,ts.create_tsql,'') AS create_tsql
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
			SUM(CASE WHEN index_id=1 THEN 1 ELSE 0 END) AS [Clustered Tables],
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
			1 as display_order
		FROM #index_sanity AS i
		--left join here so we don't lose disabled nc indexes
		LEFT JOIN #index_sanity_size AS sz 
			ON i.index_sanity_id=sz.index_sanity_id 
		UNION ALL
		SELECT 				
				N'sp_BlitzIndex version 1.5 (Mar 8, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,0 as display_order
		ORDER BY display_order ASC
		OPTION (RECOMPILE);
	   	
	END /* End @mode=1 (summarize)*/
	ELSE IF @mode=2 /*Index Detail*/
	BEGIN
		--This mode just spits out all the detail without filters.
		--This supports slicing AND dicing in Excel
		RAISERROR(N'@mode=2, here''s the details on existing indexes.', 0,1) WITH NOWAIT;

		SELECT	database_name, 
				[schema_name], 
				[object_name], 
				ISNULL(index_name, '') AS index_name, 
				index_id,
				schema_object_indexid, 
				CASE	WHEN index_id IN ( 1, 0 ) THEN 'TABLE'
					ELSE 'NonClustered'
					END AS object_type, 
				index_definition,
				ISNULL(LTRIM(key_column_names_with_sort_order), '') AS key_column_names_with_sort_order,
				ISNULL(count_key_columns, 0) AS count_key_columns,
				ISNULL(include_column_names, '') AS include_column_names, 
				ISNULL(count_included_columns,0) AS count_included_columns,
				ISNULL(secret_columns,'') AS secret_column_names, 
				ISNULL(count_secret_columns,0) AS count_secret_columns,
				ISNULL(partition_key_column_name, '') AS partition_key_column_name,
				ISNULL(filter_definition, '') AS filter_definition, 
				is_indexed_view, 
				is_primary_key,
				is_XML,
				is_spatial,
				is_NC_columnstore,
				is_disabled, 
				is_hypothetical,
				is_padded, 
				fill_factor, 
				is_referenced_by_foreign_key, 
				last_user_seek, 
				last_user_scan, 
				last_user_lookup,
				last_user_update, 
				total_reads, 
				user_updates, 
				reads_per_write, 
				index_usage_summary, 
				sz.partition_count,
				sz.total_rows, 
				sz.total_reserved_MB, 
				sz.total_reserved_LOB_MB, 
				sz.total_reserved_row_overflow_MB,
				sz.index_size_summary, 
				sz.total_row_lock_count ,
				sz.total_row_lock_wait_count ,
				sz.total_row_lock_wait_in_ms ,
				sz.avg_row_lock_wait_in_ms ,
				sz.total_page_lock_count ,
				sz.total_page_lock_wait_count ,
				sz.total_page_lock_wait_in_ms ,
				sz.avg_page_lock_wait_in_ms ,
				sz.total_index_lock_promotion_attempt_count ,
				sz.total_index_lock_promotion_count ,
				sz.data_compression_desc,
				more_info,
				1 as display_order
		FROM	#index_sanity AS i --left join here so we don't lose disabled nc indexes
				LEFT JOIN #index_sanity_size AS sz ON i.index_sanity_id = sz.index_sanity_id
		UNION ALL
		SELECT 				
				N'sp_BlitzIndex version 1.5 (Mar 8, 2013)' ,   
				N'From Brent Ozar Unlimited' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL, NULL,NULL, NULL, NULL, NULL, 0 as display_order
		ORDER BY display_order ASC, total_reserved_MB DESC
		OPTION (RECOMPILE);

	END /* End @mode=2 (index detail)*/
	ELSE IF @mode=3 /*Missing index Detail*/
	BEGIN
		SELECT 
			database_name, 
			[schema_name], 
			table_name, 
			CAST(magic_benefit_number AS BIGINT) AS magic_benefit_number, 
			missing_index_details, 
			avg_total_user_cost, 
			avg_user_impact, 
			user_seeks, 
			user_scans,
			unique_compiles, 
			equality_columns, 
			inequality_columns, 
			included_columns, 
			index_estimated_impact, 
			create_tsql, 
			more_info,
			1 as display_order
		FROM #missing_indexes
		UNION ALL
		SELECT 				
			N'sp_BlitzIndex version 1.5 (Mar 8, 2013)' ,   
			N'From Brent Ozar Unlimited' ,   
			N'http://BrentOzar.com/BlitzIndex' ,
			100000000000,
			N'Thanks from the Brent Ozar Unlimited team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
			NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
			NULL, 0 as display_order
		ORDER BY display_order ASC, magic_benefit_number DESC

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

