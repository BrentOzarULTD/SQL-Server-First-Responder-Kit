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

IF OBJECT_ID('dbo.sp_BlitzIndex') IS NOT NULL 
	DROP PROCEDURE dbo.sp_BlitzIndex;
GO

CREATE PROCEDURE dbo.sp_BlitzIndex
	@DatabaseName NVARCHAR(128) = null, /*Defaults to current DB if not specified*/
	@Mode tinyint=0, /*0=diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail*/
	@SchemaName NVARCHAR(128) = NULL, /*Requires table_name as well.*/
	@TableName NVARCHAR(128) = NULL,  /*Requires schema_name as well.*/
		/*Note:@Mode doesn't matter if you're specifying schema_name and @TableName.*/
	@Filter tinyint = 0 /* 0=no filter (default). 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB */
		/*Note:@Filter doesn't do anything unless @Mode=0*/
/*
sp_BlitzIndex(TM) v2.02 - Jan 30, 2014

(C) 2014, Brent Ozar Unlimited(TM). 
See http://BrentOzar.com/go/eula for the End User Licensing Agreement.

For help and how-to info, visit http://www.BrentOzar.com/BlitzIndex

How to use:
--	Diagnose:
		EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks';
--	Return detail for a specific table:
		EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks', @SchemaName='Person', @TableName='Person';

Known limitations of this version:
 - Does not include FULLTEXT indexes. (A possibility in the future, let us know if you're interested.)
 - Index create statements are just to give you a rough idea of the syntax. It includes filters and fillfactor.
 --		Example 1: index creates use ONLINE=? instead of ONLINE=ON / ONLINE=OFF. This is because it's important for the user to understand if it's going to be offline and not just run a script.
 --		Example 2: they do not include all the options the index may have been created with (padding, compression filegroup/partition scheme etc.)
 --		(The compression and filegroup index create syntax isn't trivial because it's set at the partition level and isn't trivial to code. Two people have voted for wanting it so far.)
 - Doesn't advise you about data modeling for clustered indexes and primary keys (primarily looks for signs of insanity.)
 - Found something? Let us know at help@brentozar.com.

 Thanks for using sp_BlitzIndex(TM)!
 Sincerely,
 The Humans of Brent Ozar Unlimited(TM)

CHANGE LOG (last five versions):
	Jan 30, 2014 (v2.02)
		Standardized calling parameters with sp_AskBrent(TM) and sp_BlitzIndex(TM). (@DatabaseName instead of @database_name, etc)
		Added check_id 80 and 81-- what appear to be the most frequently used indexes (workaholics)
		Added index_operational_stats info to table level output -- recent scans vs lookups
		Broke index_usage_stats output into two categories, scans and lookups (also in table level output)
		Changed db name, table name, index name to 128 length
		Fixed findings_group column length in #BlitzIndexResults (fixed issues for users w/ longer db names)
		Fixed issue where identities nearing end of range were only detected if the check was run with a specific db context
			Fixed extra tab in @SchemaName= that made pasting into Excel awkward/wrong
		Added abnormal psychology check for clustered columnstore indexes (and general support for detecting them)
		Standardized underscores in create TSQL for missing indexes
		Better error message when running in table mode and the table isn't found.
		Added current timestamp to the header based on user request. (Didn't add startup time-- sorry! Too many things reset usage info, don't want to mislead anyone.)
		Added fillfactor to index create statements.
		Changed all index create statements to ONLINE=?, SORT_IN_TEMPDB=?. The user should decide at index create time what's right for them.
	May 26, 2013 (v2.01)
		Added check_id 28: Non-unqiue clustered indexes. (This should have been checked in for an earlier version, it slipped by).
	May 14, 2013 (v2.0) - Added data types and max length to all columns (keys, includes, secret columns)
		Set sp_blitz to default to current DB if database_name is not specified when called
		Added @Filter:  
			0=no filter (default)
			1=Don't throw low-usage warnings for objects with 0 reads (helpful for dev/non-production environments)
			2=Only report on objects >= 250MB (helps focus on larger indexes). Still runs a few database-wide checks as well.
		Added list of all columns and types in table for runs using: @DatabaseName, @SchemaName, @TableName
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
			(Ex: EXEC dbo.sp_BlitzIndex @DatabaseName='AdventureWorks', @SchemaName='Production', @TableName='vProductAndDescription';)
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

*/
AS 

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


DECLARE	@DatabaseID INT;
DECLARE @ObjectID INT;
DECLARE	@dsql NVARCHAR(MAX);
DECLARE @params NVARCHAR(MAX);
DECLARE	@msg NVARCHAR(4000);
DECLARE	@ErrorSeverity INT;
DECLARE	@ErrorState INT;
DECLARE	@Rowcount BIGINT;
DECLARE @SQLServerProductVersion NVARCHAR(128);
DECLARE @SQLServerEdition INT;
DECLARE @FilterMB INT;
DECLARE @collation NVARCHAR(256);


SELECT @SQLServerProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
SELECT @SQLServerEdition =CAST(SERVERPROPERTY('EngineEdition') AS INT); /* We default to online index creates where EngineEdition=3*/
SET @FilterMB=250;

IF @DatabaseName is null 
	SET @DatabaseName=DB_NAME();

SELECT	@DatabaseID = database_id
FROM	sys.databases
WHERE	[name] = @DatabaseName
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
		IF @DatabaseName IS NULL OR @DatabaseID IS NULL
		BEGIN
			SET @msg='Database does not exist or is not online/multi-user: cannot proceed.'
			RAISERROR(@msg,16,1);
		END    

		--Validate parameters.
		IF (@Mode NOT IN (0,1,2,3))
		BEGIN
			SET @msg=N'Invalid @Mode parameter. 0=diagnose, 1=summarize, 2=index detail, 3=missing index detail';
			RAISERROR(@msg,16,1);
		END

		IF (@Mode <> 0 AND @TableName IS NOT NULL)
		BEGIN
			SET @msg=N'Setting the @Mode doesn''t change behavior if you supply @TableName. Use default @Mode=0 to see table detail.';
			RAISERROR(@msg,16,1);
		END

		IF ((@Mode <> 0 OR @TableName IS NOT NULL) and @Filter <> 0)
		BEGIN
			SET @msg=N'@Filter only appies when @Mode=0 and @TableName is not specified. Please try again.';
			RAISERROR(@msg,16,1);
		END

		IF (@SchemaName IS NOT NULL AND @TableName IS NULL) 
		BEGIN
			SET @msg='We can''t run against a whole schema! Specify a @TableName, or leave both NULL for diagnosis.'
			RAISERROR(@msg,16,1);
		END


		IF  (@TableName IS NOT NULL AND @SchemaName IS NULL)
		BEGIN
			SET @SchemaName=N'dbo'
			SET @msg='@SchemaName wasn''t specified-- assuming schema=dbo.'
			RAISERROR(@msg,1,1) WITH NOWAIT;
		END

		--If a table is specified, grab the object id.
		--Short circuit if it doesn't exist.
		IF @TableName IS NOT NULL
		BEGIN
			SET @dsql = N'
					SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
					SELECT	@ObjectID= OBJECT_ID
					FROM	' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so
					JOIN	' + QUOTENAME(@DatabaseName) + N'.sys.schemas AS sc on 
						so.schema_id=sc.schema_id
					where so.type in (''U'', ''V'')
					and so.name=' + QUOTENAME(@TableName,'''')+ N'
					and sc.name=' + QUOTENAME(@SchemaName,'''')+ N'
					/*Has a row in sys.indexes. This lets us get indexed views.*/
					and exists (
						SELECT si.name
						FROM ' + QUOTENAME(@DatabaseName) + '.sys.indexes AS si 
						WHERE so.object_id=si.object_id)
					OPTION (RECOMPILE);';

			SET @params='@ObjectID INT OUTPUT'				

			IF @dsql IS NULL 
				RAISERROR('@dsql is null',16,1);

			EXEC sp_executesql @dsql, @params, @ObjectID=@ObjectID OUTPUT;
			
			IF @ObjectID IS NULL
					BEGIN
						SET @msg=N'Oh, this is awkward. I can''t find the table or indexed view you''re looking for in that database.' + CHAR(10) +
							N'Please check your parameters.'
						RAISERROR(@msg,1,1);
						RETURN;
					END
		END

		RAISERROR(N'Starting run. sp_BlitzIndex(TM) v2.02 - Jan 30, 2014', 0,1) WITH NOWAIT;

		IF OBJECT_ID('tempdb..#IndexSanity') IS NOT NULL 
			DROP TABLE #IndexSanity;

		IF OBJECT_ID('tempdb..#IndexPartitionSanity') IS NOT NULL 
			DROP TABLE #IndexPartitionSanity;

		IF OBJECT_ID('tempdb..#IndexSanitySize') IS NOT NULL 
			DROP TABLE #IndexSanitySize;

		IF OBJECT_ID('tempdb..#IndexColumns') IS NOT NULL 
			DROP TABLE #IndexColumns;

		IF OBJECT_ID('tempdb..#MissingIndexes') IS NOT NULL 
			DROP TABLE #MissingIndexes;

		IF OBJECT_ID('tempdb..#ForeignKeys') IS NOT NULL 
			DROP TABLE #ForeignKeys;

		IF OBJECT_ID('tempdb..#BlitzIndexResults') IS NOT NULL 
			DROP TABLE #BlitzIndexResults;
		
		IF OBJECT_ID('tempdb..#IndexCreateTsql') IS NOT NULL	
			DROP TABLE #IndexCreateTsql;

		RAISERROR (N'Create temp tables.',0,1) WITH NOWAIT;
		CREATE TABLE #BlitzIndexResults
			(
			  blitz_result_id INT IDENTITY PRIMARY KEY,
			  check_id INT NOT NULL,
			  index_sanity_id INT NULL,
			  findings_group VARCHAR(4000) NOT NULL,
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

		CREATE TABLE #IndexSanity
			(
			  [index_sanity_id] INT IDENTITY PRIMARY KEY,
			  [database_id] SMALLINT NOT NULL ,
			  [object_id] INT NOT NULL ,
			  [index_id] INT NOT NULL ,
			  [index_type] TINYINT NOT NULL,
			  [database_name] NVARCHAR(128) NOT NULL ,
			  [schema_name] NVARCHAR(128) NOT NULL ,
			  [object_name] NVARCHAR(128) NOT NULL ,
			  index_name NVARCHAR(128) NULL ,
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
			  is_CX_columnstore BIT NOT NULL,
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

		CREATE TABLE #IndexPartitionSanity
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
			  range_scan_count BIGINT NULL ,
			  singleton_lookup_count BIGINT NULL , 
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

		CREATE TABLE #IndexSanitySize
			(
			  [index_sanity_size_id] INT IDENTITY NOT NULL ,
			  [index_sanity_id] INT NOT NULL ,
			  partition_count INT NOT NULL ,
			  total_rows BIGINT NOT NULL ,
			  total_reserved_MB NUMERIC(29,2) NOT NULL ,
			  total_reserved_LOB_MB NUMERIC(29,2) NOT NULL ,
			  total_reserved_row_overflow_MB NUMERIC(29,2) NOT NULL ,
			  total_leaf_delete_count BIGINT NULL,
			  total_leaf_update_count BIGINT NULL,
			  total_range_scan_count BIGINT NULL,
			  total_singleton_lookup_count BIGINT NULL,
			  total_forwarded_fetch_count BIGINT NULL,
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

		CREATE TABLE #IndexColumns
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

		CREATE TABLE #MissingIndexes
			([object_id] INT NOT NULL,
			[database_name] NVARCHAR(128) NOT NULL ,
			[schema_name] NVARCHAR(128) NOT NULL ,
			[table_name] NVARCHAR(128),
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

		CREATE TABLE #ForeignKeys (
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
		
		CREATE TABLE #IndexCreateTsql (
			index_sanity_id INT NOT NULL,
			create_tsql NVARCHAR(MAX) NOT NULL
		)

		--set @collation
		SELECT @collation=collation_name
		FROM sys.databases
		where database_id=@DatabaseID;

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
				FROM	' + QUOTENAME(@DatabaseName) + N'.sys.indexes si
				JOIN	' + QUOTENAME(@DatabaseName) + N'.sys.columns c ON
					si.object_id=c.object_id
				LEFT JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns sc ON 
					sc.object_id = si.object_id
					and sc.index_id=si.index_id
					AND sc.column_id=c.column_id
				LEFT JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.identity_columns ic ON
					c.object_id=ic.object_id and
					c.column_id=ic.column_id
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.types st ON 
					c.system_type_id=st.system_type_id
					AND c.user_type_id=st.user_type_id
				WHERE si.index_id in (0,1) ' 
					+ CASE WHEN @ObjectID IS NOT NULL 
						THEN N' AND si.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
					ELSE N'' END 
				+ N';';

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #IndexColumns for clustered indexes and heaps',0,1) WITH NOWAIT;
		INSERT	#IndexColumns ( object_id, index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
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
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_sparse' else N'NULL AS is_sparse' END + N',
					' + case when @SQLServerProductVersion not like '9%' THEN N'c.is_filestream' else N'NULL AS is_filestream' END + N'				
				FROM	' + QUOTENAME(@DatabaseName) + N'.sys.indexes AS si
				JOIN	' + QUOTENAME(@DatabaseName) + N'.sys.columns AS c ON
					si.object_id=c.object_id
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.index_columns AS sc ON 
					sc.object_id = si.object_id
					and sc.index_id=si.index_id
					AND sc.column_id=c.column_id
				JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.types AS st ON 
					c.system_type_id=st.system_type_id
					AND c.user_type_id=st.user_type_id
				WHERE si.index_id not in (0,1) ' 
					+ CASE WHEN @ObjectID IS NOT NULL 
						THEN N' AND si.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
					ELSE N'' END 
				+ N';';

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #IndexColumns for nonclustered indexes',0,1) WITH NOWAIT;
		INSERT	#IndexColumns ( object_id, index_id, key_ordinal, is_included_column, is_descending_key, partition_ordinal,
			column_name, system_type_name, max_length, precision, scale, collation_name, is_nullable, is_identity, is_computed,
			is_replicated, is_sparse, is_filestream )
				EXEC sp_executesql @dsql;
					
		SET @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	' + CAST(@DatabaseID AS NVARCHAR(10)) + ' AS database_id, 
						so.object_id, 
						si.index_id, 
						si.type,
						' + QUOTENAME(@DatabaseName, '''') + ' AS database_name, 
						sc.NAME AS [schema_name],
						so.name AS [object_name], 
						si.name AS [index_name],
						CASE	WHEN so.[type] = CAST(''V'' AS CHAR(2)) THEN 1 ELSE 0 END, 
						si.is_unique, 
						si.is_primary_key, 
						CASE when si.type = 3 THEN 1 ELSE 0 END AS is_XML,
						CASE when si.type = 4 THEN 1 ELSE 0 END AS is_spatial,
						CASE when si.type = 6 THEN 1 ELSE 0 END AS is_NC_columnstore,
						CASE when si.type = 5 then 1 else 0 end as is_CX_columnstore,
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
				FROM	' + QUOTENAME(@DatabaseName) + '.sys.indexes AS si WITH (NOLOCK)
						JOIN ' + QUOTENAME(@DatabaseName) + '.sys.objects AS so WITH (NOLOCK) ON si.object_id = so.object_id
											   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
											   AND so.type <> ''TF'' /*Exclude table valued functions*/
						JOIN ' + QUOTENAME(@DatabaseName) + '.sys.schemas sc ON so.schema_id = sc.schema_id
						LEFT JOIN sys.dm_db_index_usage_stats AS us WITH (NOLOCK) ON si.[object_id] = us.[object_id]
																	   AND si.index_id = us.index_id
																	   AND us.database_id = '+ CAST(@DatabaseID AS NVARCHAR(10)) + '
				WHERE	si.[type] IN ( 0, 1, 2, 3, 4, 5, 6 ) 
				/* Heaps, clustered, nonclustered, XML, spatial, Cluster Columnstore, NC Columnstore */ ' +
				CASE WHEN @TableName IS NOT NULL THEN ' and so.name=' + QUOTENAME(@TableName,'''') + ' ' ELSE '' END + 
		'OPTION	( RECOMPILE );
		';
		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #IndexSanity',0,1) WITH NOWAIT;
		INSERT	#IndexSanity ( [database_id], [object_id], [index_id], [index_type], [database_name], [schema_name], [object_name],
								index_name, is_indexed_view, is_unique, is_primary_key, is_XML, is_spatial, is_NC_columnstore, is_CX_columnstore,
								is_disabled, is_hypothetical, is_padded, fill_factor, filter_definition, user_seeks, user_scans, 
								user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, last_user_update,
								create_date, modify_date )
				EXEC sp_executesql @dsql;

		RAISERROR (N'Updating #IndexSanity.key_column_names',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		key_column_names = D1.key_column_names
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name 
									+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
										AS col_definition
									FROM	#IndexColumns c
									WHERE	c.object_id = si.object_id
											AND c.index_id = si.index_id
											AND c.is_included_column = 0 /*Just Keys*/
											AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
									ORDER BY c.object_id, c.index_id, c.key_ordinal	
							FOR	  XML PATH('') ,TYPE).value('.', 'varchar(max)'), 1, 1, ''))
										) D1 ( key_column_names )

		RAISERROR (N'Updating #IndexSanity.partition_key_column_name',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		partition_key_column_name = D1.partition_key_column_name
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name AS col_definition
									FROM	#IndexColumns c
									WHERE	c.object_id = si.object_id
											AND c.index_id = si.index_id
											AND c.partition_ordinal <> 0 /*Just Partitioned Keys*/
									ORDER BY c.object_id, c.index_id, c.key_ordinal	
							FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1,''))) D1 
										( partition_key_column_name )

		RAISERROR (N'Updating #IndexSanity.key_column_names_with_sort_order',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		key_column_names_with_sort_order = D2.key_column_names_with_sort_order
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name + CASE c.is_descending_key
									WHEN 1 THEN N' DESC'
									ELSE N''
								+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
								END AS col_definition
							FROM	#IndexColumns c
							WHERE	c.object_id = si.object_id
									AND c.index_id = si.index_id
									AND c.is_included_column = 0 /*Just Keys*/
									AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
							ORDER BY c.object_id, c.index_id, c.key_ordinal	
					FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1, ''))
					) D2 ( key_column_names_with_sort_order )

		RAISERROR (N'Updating #IndexSanity.key_column_names_with_sort_order_no_types (for create tsql)',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		key_column_names_with_sort_order_no_types = D2.key_column_names_with_sort_order_no_types
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + QUOTENAME(c.column_name) + CASE c.is_descending_key
									WHEN 1 THEN N' [DESC]'
									ELSE N''
								END AS col_definition
							FROM	#IndexColumns c
							WHERE	c.object_id = si.object_id
									AND c.index_id = si.index_id
									AND c.is_included_column = 0 /*Just Keys*/
									AND c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
							ORDER BY c.object_id, c.index_id, c.key_ordinal	
					FOR	  XML PATH('') , TYPE).value('.', 'varchar(max)'), 1, 1, ''))
					) D2 ( key_column_names_with_sort_order_no_types )

		RAISERROR (N'Updating #IndexSanity.include_column_names',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		include_column_names = D3.include_column_names
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + c.column_name
								+ N' {' + system_type_name + N' ' + CAST(max_length AS NVARCHAR(50)) +  N'}'
								FROM	#IndexColumns c
								WHERE	c.object_id = si.object_id
										AND c.index_id = si.index_id
										AND c.is_included_column = 1 /*Just includes*/
								ORDER BY c.column_name /*Order doesn't matter in includes, 
										this is here to make rows easy to compare.*/ 
						FOR	  XML PATH('') ,  TYPE).value('.', 'varchar(max)'), 1, 1, ''))
						) D3 ( include_column_names );

		RAISERROR (N'Updating #IndexSanity.include_column_names_no_types (for create tsql)',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		include_column_names_no_types = D3.include_column_names_no_types
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	RTRIM(STUFF( (SELECT	N', ' + QUOTENAME(c.column_name)
								FROM	#IndexColumns c
								WHERE	c.object_id = si.object_id
										AND c.index_id = si.index_id
										AND c.is_included_column = 1 /*Just includes*/
								ORDER BY c.column_name /*Order doesn't matter in includes, 
										this is here to make rows easy to compare.*/ 
						FOR	  XML PATH('') ,  TYPE).value('.', 'varchar(max)'), 1, 1, ''))
						) D3 ( include_column_names_no_types );

		RAISERROR (N'Updating #IndexSanity.count_key_columns and count_include_columns',0,1) WITH NOWAIT;
		UPDATE	#IndexSanity
		SET		count_included_columns = D4.count_included_columns,
				count_key_columns = D4.count_key_columns
		FROM	#IndexSanity si
				CROSS APPLY ( SELECT	SUM(CASE WHEN is_included_column = 'true' THEN 1
												 ELSE 0
											END) AS count_included_columns,
										SUM(CASE WHEN is_included_column = 'false' AND c.key_ordinal > 0 THEN 1
												 ELSE 0
											END) AS count_key_columns
							  FROM		#IndexColumns c
							  WHERE		c.object_id = si.object_id
										AND c.index_id = si.index_id 
										) AS D4 ( count_included_columns, count_key_columns );

		IF (SELECT LEFT(@SQLServerProductVersion,
			  CHARINDEX('.',@SQLServerProductVersion,0)-1
			  )) <> 11 --Anything other than 2012
		BEGIN

			RAISERROR (N'Using non-2012 syntax to query sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;

			--NOTE: we're joining to sys.dm_db_index_operational_stats differently than you might think (not using a cross apply)
			--This is because of quirks prior to SQL Server 2012 and in 2014 with this DMV.
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
								os.range_scan_count, 
								os.singleton_lookup_count,  
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
					FROM	' + QUOTENAME(@DatabaseName) + '.sys.dm_db_partition_stats AS ps  
					JOIN ' + QUOTENAME(@DatabaseName) + '.sys.partitions AS par on ps.partition_id=par.partition_id
					JOIN ' + QUOTENAME(@DatabaseName) + '.sys.objects AS so ON ps.object_id = so.object_id
							   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
							   AND so.type <> ''TF'' /*Exclude table valued functions*/
					LEFT JOIN ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_index_operational_stats('
				+ CAST(@DatabaseID AS NVARCHAR(10)) + ', NULL, NULL,NULL) AS os ON
					ps.object_id=os.object_id and ps.index_id=os.index_id and ps.partition_number=os.partition_number 
					WHERE 1=1 
					' + CASE WHEN @ObjectID IS NOT NULL THEN N'AND so.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' ELSE N' ' END + '
					' + CASE WHEN @Filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@FilterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + '
			ORDER BY ps.object_id,  ps.index_id, ps.partition_number
			OPTION	( RECOMPILE );
			';
		END
		ELSE /* Otherwise use this syntax which takes advantage of OUTER APPLY on the os_partitions DMV. 
		This performs better on 2012 tables using 1000+ partitions. */
		BEGIN
		RAISERROR (N'Using 2012 syntax to query sys.dm_db_index_operational_stats',0,1) WITH NOWAIT;

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
								os.range_scan_count, 
								os.singleton_lookup_count,  
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
						FROM	' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_partition_stats AS ps  
						JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.partitions AS par on ps.partition_id=par.partition_id
						JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects AS so ON ps.object_id = so.object_id
								   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
								   AND so.type <> ''TF'' /*Exclude table valued functions*/
						OUTER APPLY ' + QUOTENAME(@DatabaseName) + N'.sys.dm_db_index_operational_stats('
					+ CAST(@DatabaseID AS NVARCHAR(10)) + N', ps.object_id, ps.index_id,ps.partition_number) AS os
						WHERE 1=1 
						' + CASE WHEN @ObjectID IS NOT NULL THEN N'AND so.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' ELSE N' ' END + N'
						' + CASE WHEN @Filter = 2 THEN N'AND ps.reserved_page_count * 8./1024. > ' + CAST(@FilterMB AS NVARCHAR(5)) + N' ' ELSE N' ' END + '
				ORDER BY ps.object_id,  ps.index_id, ps.partition_number
				OPTION	( RECOMPILE );
				';
 
		END       

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

		RAISERROR (N'Inserting data into #IndexPartitionSanity',0,1) WITH NOWAIT;
		insert	#IndexPartitionSanity ( 
											[object_id], 
											index_id, 
											partition_number, 
											row_count, 
											reserved_MB,
										  reserved_LOB_MB, 
										  reserved_row_overflow_MB, 
										  leaf_insert_count,
										  leaf_delete_count, 
										  leaf_update_count, 
										  range_scan_count,
										  singleton_lookup_count,
										  forwarded_fetch_count, 
										  lob_fetch_in_pages, 
										  lob_fetch_in_bytes, 
										  row_overflow_fetch_in_pages,
										  row_overflow_fetch_in_bytes, 
										  row_lock_count, 
										  row_lock_wait_count,
										  row_lock_wait_in_ms, 
										  page_lock_count, 
										  page_lock_wait_count,
										  page_lock_wait_in_ms, 
										  index_lock_promotion_attempt_count,
										  index_lock_promotion_count, 
										  data_compression_desc )
				EXEC sp_executesql @dsql;


		RAISERROR (N'Updating index_sanity_id on #IndexPartitionSanity',0,1) WITH NOWAIT;
		UPDATE	#IndexPartitionSanity
		SET		index_sanity_id = i.index_sanity_id
		FROM #IndexPartitionSanity ps
				JOIN #IndexSanity i ON ps.[object_id] = i.[object_id]
										AND ps.index_id = i.index_id


		RAISERROR (N'Inserting data into #IndexSanitySize',0,1) WITH NOWAIT;
		INSERT	#IndexSanitySize ( [index_sanity_id], partition_count, total_rows, total_reserved_MB,
									 total_reserved_LOB_MB, total_reserved_row_overflow_MB, total_range_scan_count,
									 total_singleton_lookup_count, total_leaf_delete_count, total_leaf_update_count, 
									 total_forwarded_fetch_count,total_row_lock_count,
									 total_row_lock_wait_count, total_row_lock_wait_in_ms, avg_row_lock_wait_in_ms,
									 total_page_lock_count, total_page_lock_wait_count, total_page_lock_wait_in_ms,
									 avg_page_lock_wait_in_ms, total_index_lock_promotion_attempt_count, 
									 total_index_lock_promotion_count, data_compression_desc )
				SELECT	index_sanity_id, COUNT(*), SUM(row_count), SUM(reserved_MB), SUM(reserved_LOB_MB),
						SUM(reserved_row_overflow_MB), 
						SUM(range_scan_count),
						SUM(singleton_lookup_count),
						SUM(leaf_delete_count), 
						SUM(leaf_update_count),
						SUM(forwarded_fetch_count),
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
				FROM #IndexPartitionSanity ipp
				/* individual partitions can have distinct compression settings, just roll them into a list here*/
				OUTER APPLY (SELECT STUFF((
					SELECT	N', ' + data_compression_desc
					FROM #IndexPartitionSanity ipp2
					WHERE ipp.[object_id]=ipp2.[object_id]
						AND ipp.[index_id]=ipp2.[index_id]
					ORDER BY ipp2.partition_number
					FOR	  XML PATH(''),TYPE).value('.', 'varchar(max)'), 1, 1, '')) 
						data_compression_info(data_compression_rollup)
				GROUP BY index_sanity_id
				ORDER BY index_sanity_id 
		OPTION	( RECOMPILE );

		RAISERROR (N'Adding UQ index on #IndexSanity (object_id,index_id)',0,1) WITH NOWAIT;
		CREATE UNIQUE INDEX uq_object_id_index_id ON #IndexSanity (object_id,index_id);

		RAISERROR (N'Inserting data into #MissingIndexes',0,1) WITH NOWAIT;
		SET @dsql=N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT	id.object_id, ' + QUOTENAME(@DatabaseName,'''') + N', sc.[name], so.[name], id.statement , gs.avg_total_user_cost, 
						gs.avg_user_impact, gs.user_seeks, gs.user_scans, gs.unique_compiles,id.equality_columns, 
						id.inequality_columns,id.included_columns
				FROM	sys.dm_db_missing_index_groups ig
						JOIN sys.dm_db_missing_index_details id ON ig.index_handle = id.index_handle
						JOIN sys.dm_db_missing_index_group_stats gs ON ig.index_group_handle = gs.group_handle
						JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects so on 
							id.object_id=so.object_id
						JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.schemas sc on 
							so.schema_id=sc.schema_id
				WHERE	id.database_id = ' + CAST(@DatabaseID AS NVARCHAR(30)) + '
				' + CASE WHEN @ObjectID IS NULL THEN N'' 
					ELSE N'and id.object_id=' + CAST(@ObjectID AS NVARCHAR(30)) 
				END +
		N';'

		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);
		INSERT	#MissingIndexes ( [object_id], [database_name], [schema_name], [table_name], [statement], avg_total_user_cost, 
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
			FROM ' + QUOTENAME(@DatabaseName) + N'.sys.foreign_keys fk
			JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects fk_object ON fk.object_id=fk_object.object_id
			JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects parent_object ON fk.parent_object_id=parent_object.object_id
			JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.objects referenced_object ON fk.referenced_object_id=referenced_object.object_id
			CROSS APPLY ( SELECT	STUFF( (SELECT	N'', '' + c_parent.name AS fk_columns
											FROM	' + QUOTENAME(@DatabaseName) + N'.sys.foreign_key_columns fkc 
											JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c_parent ON fkc.parent_object_id=c_parent.[object_id]
												AND fkc.parent_column_id=c_parent.column_id
											WHERE	fk.parent_object_id=fkc.parent_object_id
												AND fk.[object_id]=fkc.constraint_object_id
											ORDER BY fkc.constraint_column_id 
									FOR	  XML PATH('''') ,
											  TYPE).value(''.'', ''varchar(max)''), 1, 1, '''')/*This is how we remove the first comma*/ ) parent ( fk_columns )
			CROSS APPLY ( SELECT	STUFF( (SELECT	N'', '' + c_referenced.name AS fk_columns
											FROM	' + QUOTENAME(@DatabaseName) + N'.sys.	foreign_key_columns fkc 
											JOIN ' + QUOTENAME(@DatabaseName) + N'.sys.columns c_referenced ON fkc.referenced_object_id=c_referenced.[object_id]
												AND fkc.referenced_column_id=c_referenced.column_id
											WHERE	fk.referenced_object_id=fkc.referenced_object_id
												and fk.[object_id]=fkc.constraint_object_id
											ORDER BY fkc.constraint_column_id  /*order by col name, we don''t have anything better*/
									FOR	  XML PATH('''') ,
											  TYPE).value(''.'', ''varchar(max)''), 1, 1, '''') ) referenced ( fk_columns )
			' + CASE WHEN @ObjectID IS NOT NULL THEN 
					'WHERE fk.parent_object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' OR fk.referenced_object_id=' + CAST(@ObjectID AS NVARCHAR(30)) + N' ' 
					ELSE N' ' END + '
			ORDER BY parent_object_name, foreign_key_name;
		';
		IF @dsql IS NULL 
			RAISERROR('@dsql is null',16,1);

        RAISERROR (N'Inserting data into #ForeignKeys',0,1) WITH NOWAIT;
        INSERT  #ForeignKeys ( foreign_key_name, parent_object_id,parent_object_name, referenced_object_id, referenced_object_name,
                                is_disabled, is_not_trusted, is_not_for_replication, parent_fk_columns, referenced_fk_columns,
								[update_referential_action_desc], [delete_referential_action_desc] )
                EXEC sp_executesql @dsql;

        RAISERROR (N'Updating #IndexSanity.referenced_by_foreign_key',0,1) WITH NOWAIT;
		UPDATE #IndexSanity
			SET is_referenced_by_foreign_key=1
		FROM #IndexSanity s
		JOIN #ForeignKeys fk ON 
			s.object_id=fk.referenced_object_id
			AND LEFT(s.key_column_names,LEN(fk.referenced_fk_columns)) = fk.referenced_fk_columns

		RAISERROR (N'Add computed columns to #IndexSanity to simplify queries.',0,1) WITH NOWAIT;
		ALTER TABLE #IndexSanity ADD 
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
			+ case when user_seeks + user_scans + user_lookups > 0 then
				N' (' 
					+ RTRIM(
					CASE WHEN user_seeks > 0 then REPLACE(CONVERT(NVARCHAR(30),CAST((user_seeks) AS money), 1), '.00', '') + N' seek ' ELSE N'' END
					+ CASE WHEN user_scans > 0 then REPLACE(CONVERT(NVARCHAR(30),CAST((user_scans) AS money), 1), '.00', '') + N' scan '  ELSE N'' END
					+ CASE WHEN user_lookups > 0 then  REPLACE(CONVERT(NVARCHAR(30),CAST((user_lookups) AS money), 1), '.00', '') + N' lookup' ELSE N'' END
					)
					+ N') '
				else N' ' end 
			+ N'Writes:' + 
			REPLACE(CONVERT(NVARCHAR(30),CAST(user_updates AS money), 1), '.00', ''),
		[more_info] AS N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name],'''') + 
			N', @SchemaName=' + QUOTENAME([schema_name],'''') + N', @TableName=' + QUOTENAME([object_name],'''') + N';'

		RAISERROR (N'Update index_secret on #IndexSanity for NC indexes.',0,1) WITH NOWAIT;
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
		FROM #IndexSanity AS nc
		JOIN #IndexSanity AS tb ON nc.object_id=tb.object_id
			and tb.index_id in (0,1) 
		WHERE nc.index_id > 1;

		RAISERROR (N'Update index_secret on #IndexSanity for heaps and non-unique clustered.',0,1) WITH NOWAIT;
		UPDATE tb
		SET secret_columns=	CASE tb.index_id WHEN 0 THEN '[RID]' ELSE '[UNIQUIFIER]' END
			, count_secret_columns = 1
		FROM #IndexSanity AS tb
		WHERE tb.index_id = 0 /*Heaps-- these have the RID */
			or (tb.index_id=1 and tb.is_unique=0); /* Non-unique CX: has uniquifer (when needed) */

		RAISERROR (N'Add computed columns to #IndexSanitySize to simplify queries.',0,1) WITH NOWAIT;
		ALTER TABLE #IndexSanitySize ADD 
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
					N'Error- NULL in computed column'),
			index_op_stats AS ISNULL(
				(
					REPLACE(CONVERT(NVARCHAR(30),CAST(total_singleton_lookup_count AS MONEY), 1),N'.00',N'') + N' singleton lookups; '
					+ REPLACE(CONVERT(NVARCHAR(30),CAST(total_range_scan_count AS MONEY), 1),N'.00',N'') + N' scans/seeks; '
					+ REPLACE(CONVERT(NVARCHAR(30),CAST(total_leaf_delete_count AS MONEY), 1),N'.00',N'') + N' deletes; '
					+ REPLACE(CONVERT(NVARCHAR(30),CAST(total_leaf_update_count AS MONEY), 1),N'.00',N'') + N' updates; '
					+ CASE WHEN ISNULL(total_forwarded_fetch_count,0) >0 THEN
						REPLACE(CONVERT(NVARCHAR(30),CAST(total_forwarded_fetch_count AS MONEY), 1),N'.00',N'') + N' forward records fetched; '
					ELSE N'' END

					/* rows will only be in this dmv when data is in memory for the table */
				), N'Table metadata not in memory'),
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
		ALTER TABLE #MissingIndexes ADD 
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
					+ REPLACE(REPLACE(REPLACE(REPLACE(
						ISNULL(equality_columns,N'')+ 
						CASE when equality_columns is not null and inequality_columns is not null then N'_' else N'' END
						+ ISNULL(inequality_columns,''),',','')
						,'[',''),']',''),' ','_') 
					+ CASE WHEN included_columns IS NOT NULL THEN N'_includes' ELSE N'' END + N'] ON ' 
					+ [statement] + N' (' + ISNULL(equality_columns,N'')
					+ CASE WHEN equality_columns IS NOT NULL AND inequality_columns IS NOT NULL THEN N', ' ELSE N'' END
					+ CASE WHEN inequality_columns IS NOT NULL THEN inequality_columns ELSE N'' END + 
					') ' + CASE WHEN included_columns IS NOT NULL THEN N' INCLUDE (' + included_columns + N')' ELSE N'' END
					+ N' WITH (' 
						+ N'FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?' 
					+ N')'
					+ N';'
					,
				[more_info] AS N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name],'''') + 
					N', @SchemaName=' + QUOTENAME([schema_name],'''') + N', @TableName=' + QUOTENAME([table_name],'''') + N';'
				;


		RAISERROR (N'Populate #IndexCreateTsql.',0,1) WITH NOWAIT;
		INSERT #IndexCreateTsql (index_sanity_id, create_tsql)
		SELECT
			index_sanity_id,
			ISNULL (
			/* Script drops for disabled non-clustered indexes*/
			CASE WHEN is_disabled = 1 AND index_id <> 1
				THEN N'--DROP INDEX ' + QUOTENAME([index_name]) + N' ON '
				 + QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name]) 
			ELSE
				CASE index_id WHEN 0 THEN N'--I''m a Heap!' 
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
							WHEN is_CX_columnstore= 1 THEN
								 N'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(index_name) + N' on ' + QUOTENAME([schema_name]) + '.' + QUOTENAME([object_name])
						ELSE /*Else not a PK or cx columnstore */ 
							N'CREATE ' + 
							CASE WHEN is_unique=1 THEN N'UNIQUE ' ELSE N'' END +
							CASE WHEN index_id=1 THEN N'CLUSTERED ' ELSE N'' END +
							CASE WHEN is_NC_columnstore=1 THEN N'NONCLUSTERED COLUMNSTORE ' 
							ELSE N'' END +
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
							END /*End Non-PK index CASE */ 
						+ CASE WHEN is_NC_columnstore=0 and is_CX_columnstore=0 then
							N' WITH (' 
								+ N'FILLFACTOR=' + CASE fill_factor when 0 then N'100' else CAST(fill_factor AS NVARCHAR(5)) END + ', '
								+ N'ONLINE=?, SORT_IN_TEMPDB=?'
							+ N')'
						else N'' end
						+ N';'
  					END /*End non-spatial and non-xml CASE */ 
				END
			END, '[Unknown Error]')
				AS create_tsql
		FROM #IndexSanity;
					
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
--If @TableName is specified, just return information for that table.
--The @Mode parameter doesn't matter if you're looking at a specific table.
----------------------------------------
IF @TableName IS NOT NULL
BEGIN
	RAISERROR(N'@TableName specified, giving detail only on that table.', 0,1) WITH NOWAIT;

	--We do a left join here in case this is a disabled NC.
	--In that case, it won't have any size info/pages allocated.
	WITH table_mode_cte AS (
		SELECT 
			s.schema_object_indexid, 
			s.key_column_names,
			s.index_definition, 
			ISNULL(s.secret_columns,N'') AS secret_columns,
			s.fill_factor,
			s.index_usage_summary, 
			sz.index_op_stats,
			ISNULL(sz.index_size_summary,'') /*disabled NCs will be null*/ AS index_size_summary,
			ISNULL(sz.index_lock_wait_summary,'') AS index_lock_wait_summary,
			s.is_referenced_by_foreign_key,
			(SELECT COUNT(*)
				FROM #ForeignKeys fk WHERE fk.parent_object_id=s.object_id
				AND PATINDEX (fk.parent_fk_columns, s.key_column_names)=1) AS FKs_covered_by_index,
			s.last_user_seek,
			s.last_user_scan,
			s.last_user_lookup,
			s.last_user_update,
			s.create_date,
			s.modify_date,
			ct.create_tsql,
			1 as display_order
		FROM #IndexSanity s
		LEFT JOIN #IndexSanitySize sz ON 
			s.index_sanity_id=sz.index_sanity_id
		LEFT JOIN #IndexCreateTsql ct ON 
			s.index_sanity_id=ct.index_sanity_id
		WHERE s.[object_id]=@ObjectID
		UNION ALL
		SELECT 	N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + convert(nvarchar(16),getdate(),121) + 			
				N' (sp_BlitzIndex(TM) v2.02 - Jan 30, 2014)' ,   
				N'From Brent Ozar Unlimited(TM)' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited(TM) team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				0 as display_order
	)
	SELECT 
			schema_object_indexid AS [Details: schema.table.index(indexid)], 
			index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
			secret_columns AS [Secret Columns],
			fill_factor AS [Fillfactor],
			index_usage_summary AS [Usage Stats], 
			index_op_stats as [Op Stats],
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

	IF (SELECT TOP 1 [object_id] FROM    #MissingIndexes mi) IS NOT NULL
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
		FROM    #MissingIndexes mi
		WHERE   [object_id] = @ObjectID
		ORDER BY magic_benefit_number DESC
		OPTION	( RECOMPILE );
	END       
	ELSE     
	SELECT 'No missing indexes.' AS finding;

	SELECT 	
		column_name AS [Column Name],
		(SELECT COUNT(*)  
			FROM #IndexColumns c2 
			WHERE c2.column_name=c.column_name
			and c2.key_ordinal is not null)
		+ CASE WHEN c.index_id = 1 and c.key_ordinal is not null THEN
			-1+ (SELECT COUNT(DISTINCT index_id)
			from #IndexColumns c3
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
	FROM #IndexColumns AS c
	where index_id in (0,1);

	IF (SELECT TOP 1 parent_object_id FROM #ForeignKeys) IS NOT NULL
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
		FROM #ForeignKeys
		ORDER BY [Foreign Key]
		OPTION	( RECOMPILE );
	END
	ELSE
	SELECT 'No foreign keys.' AS finding;
END 

--If @TableName is NOT specified...
--Act based on the @Mode and @Filter. (@Filter applies only when @Mode=0 "diagnose")
ELSE
BEGIN;
	IF @Mode=0 /* DIAGNOSE*/
	BEGIN;
		RAISERROR(N'@Mode=0, we are diagnosing.', 0,1) WITH NOWAIT;

		RAISERROR(N'Insert a row to help people find help', 0,1) WITH NOWAIT;
		INSERT	#BlitzIndexResults ( check_id, findings_group, finding, URL, details, index_definition,
										index_usage_summary, index_size_summary )
		VALUES  ( 0 , 
				N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + convert(nvarchar(16),getdate(),121), 
				N'sp_BlitzIndex(TM) v2.02 - Jan 30, 2014' ,
				N'From Brent Ozar Unlimited(TM)' ,   N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited(TM) team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
				, N'',N''
				);

		----------------------------------------
		--Multiple Index Personalities: Check_id 0-10
		----------------------------------------
		BEGIN;
		RAISERROR('check_id 1: Duplicate keys', 0,1) WITH NOWAIT;
			WITH	duplicate_indexes
					  AS ( SELECT	[object_id], key_column_names
						   FROM		#IndexSanity
						   WHERE  index_type IN (1,2) /* Clustered, NC only*/
								AND is_hypothetical = 0
								AND is_disabled = 0
						   GROUP BY	[object_id], key_column_names
						   HAVING	COUNT(*) > 1)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
								JOIN #IndexSanity ip ON di.[object_id] = ip.[object_id]
														 AND ip.key_column_names = di.key_column_names
								JOIN #IndexSanitySize ips ON ip.index_sanity_id = ips.index_sanity_id
						ORDER BY ip.object_id, ip.key_column_names_with_sort_order	
				OPTION	( RECOMPILE );

		RAISERROR('check_id 2: Keys w/ identical leading columns.', 0,1) WITH NOWAIT;
			WITH	borderline_duplicate_indexes
					  AS ( SELECT DISTINCT [object_id], first_key_column_name, key_column_names,
									COUNT([object_id]) OVER ( PARTITION BY [object_id], first_key_column_name ) AS number_dupes
						   FROM		#IndexSanity
						   WHERE index_type IN (1,2) /* Clustered, NC only*/
							AND is_hypothetical=0
							AND is_disabled=0)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id,  findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity AS ip 
						JOIN #IndexSanitySize ips ON ip.index_sanity_id = ips.index_sanity_id
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
		INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
				FROM	#IndexSanity AS i
				JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
				WHERE	(total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) > 300000
				OPTION	( RECOMPILE );
		END

		---------------------------------------- 
		--Index Hoarder: Check_id 20-29
		----------------------------------------
		BEGIN
			RAISERROR(N'check_id 20: >=7 NC indexes on any given table. Yes, 7 is an arbitrary number.', 0,1) WITH NOWAIT;
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN #IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	index_id NOT IN ( 0, 1 )
						GROUP BY schema_object_name
						HAVING	COUNT(*) >= 7
						ORDER BY i.schema_object_name DESC  OPTION	( RECOMPILE );

			if @Filter = 1 /*@Filter=1 is "ignore unusued" */
			BEGIN
				RAISERROR(N'Skipping checks on unused indexes (21 and 22) because @Filter=1', 0,1) WITH NOWAIT;
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
					FROM	#IndexSanity i
					JOIN	#IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id NOT IN ( 0, 1 ) 
							and i.is_unique = 0
					OPTION	( RECOMPILE );

				IF @percent_NC_indexes_unused >= 5 
					INSERT	#BlitzIndexResults ( check_id, index_sanity_id,  findings_group, finding, URL, details, index_definition,
												   secret_columns, index_usage_summary, index_size_summary )
							SELECT	21 AS check_id, 
									MAX(i.index_sanity_id) AS index_sanity_id, 
									N'Index Hoarder' AS findings_group,
									N'More than 5 percent NC indexes are unused' AS finding,
									N'http://BrentOzar.com/go/IndexHoarder' AS URL,
									CAST (@percent_NC_indexes_unused AS NVARCHAR(30)) + N' percent NC indexes (' + CAST(COUNT(*) AS NVARCHAR(10)) + N') unused. ' +
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
							FROM	#IndexSanity i
							JOIN	#IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
							WHERE	index_id NOT IN ( 0, 1 )
									AND i.is_unique = 0
									AND total_reads = 0
							GROUP BY i.database_name 
					OPTION	( RECOMPILE );

				RAISERROR(N'check_id 22: NC indexes with 0 reads. (Borderline)', 0,1) WITH NOWAIT;
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity AS i
						JOIN	#IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
						WHERE	i.total_reads=0
								AND i.index_id NOT IN (0,1) /*NCs only*/
								and i.is_unique = 0
						ORDER BY i.schema_object_indexid
						OPTION	( RECOMPILE );
			END /*end checks only run when @Filter <> 1*/

			RAISERROR(N'check_id 23: Indexes with 7 or more columns. (Borderline)', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN	#IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	( count_key_columns + count_included_columns ) >= 7
					OPTION	( RECOMPILE );

			RAISERROR(N'check_id 24: Wide clustered indexes (> 3 columns or > 16 bytes).', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								SUM(CASE max_length when -1 THEN 0 ELSE max_length END) AS sum_max_length
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							and key_ordinal > 0
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	24 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Wide clustered index (> 3 columns OR > 16 bytes)' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								CAST (i.count_key_columns AS NVARCHAR(10)) + N' columns with potential size of '
									+ CAST(cc.sum_max_length AS NVARCHAR(10))
									+ N' bytes in clustered index:' + i.schema_object_name 
									+ N'. ' + 
										(SELECT CAST(COUNT(*) AS NVARCHAR(23)) FROM #IndexSanity i2 
										WHERE i2.[object_id]=i.[object_id] AND i2.index_id <> 1
										AND i2.is_disabled=0 AND i2.is_hypothetical=0)
										+ N' NC indexes on the table.'
									AS details,
								i.index_definition,
								secret_columns, 
								i.index_usage_summary,
								ip.index_size_summary
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
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
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
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
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
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
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						CROSS APPLY (SELECT cc.total_columns - string_or_LOB_columns AS non_string_or_lob_columns) AS calc1
						WHERE	i.index_id in (1,0)
							AND calc1.non_string_or_lob_columns <= 1
							AND cc.total_columns > 3
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 28: Non-unique clustered index.', 0,1) WITH NOWAIT;
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	28 AS check_id, 
								i.index_sanity_id, 
								N'Index Hoarder' AS findings_group,
								N'Non-Unique clustered index' AS finding,
								N'http://BrentOzar.com/go/IndexHoarder' AS URL,
								N'Uniquifiers will be required! Clustered index: ' + i.schema_object_name 
									+ N' and all NC indexes. ' + 
										(SELECT CAST(COUNT(*) AS NVARCHAR(23)) FROM #IndexSanity i2 
										WHERE i2.[object_id]=i.[object_id] AND i2.index_id <> 1
										AND i2.is_disabled=0 AND i2.is_hypothetical=0)
										+ N' NC indexes on the table.'
									AS details,
								i.index_definition,
								secret_columns, 
								i.index_usage_summary,
								ip.index_size_summary
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	index_id = 1 /* clustered only */
								AND is_unique=0 /* not unique */
								AND is_CX_columnstore=0 /* not a clustered columnstore-- no unique option on those */
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
			FROM	#IndexSanity;

			IF @number_indexes_with_includes = 0 
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
				FROM	#IndexSanity
				WHERE	filter_definition <> '' OPTION	( RECOMPILE );

				SELECT	@count_indexed_views=COUNT(*)
				FROM	#IndexSanity AS i
						JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
				WHERE	is_indexed_view = 1 OPTION	( RECOMPILE );

			IF @count_filtered_indexes = 0 AND @count_indexed_views=0
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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

		INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
		FROM #IndexColumns ic 
		join #IndexSanity i on 
			ic.[object_id]=i.[object_id] and
			ic.[index_id]=i.[index_id] and
			i.[index_id] > 1 /* non-clustered index */
		JOIN	#IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
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
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN	#IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id > 1
					and	fill_factor BETWEEN 1 AND 80 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 40: Fillfactor in clustered 90 percent or less', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE	index_id = 1
					and fill_factor BETWEEN 1 AND 90 OPTION	( RECOMPILE );


			RAISERROR(N'check_id 41: Hypothetical indexes ', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	41 AS check_id, 
							N'Self Loathing Indexes' AS findings_group,
							N'Hypothetical Index' AS finding, 'http://BrentOzar.com/go/SelfLoathing' AS URL,
							N'Hypothetical Index: ' + schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							N'' AS index_usage_summary, 
							N'' AS index_size_summary
					FROM	#IndexSanity AS i
					WHERE	is_hypothetical = 1 OPTION	( RECOMPILE );


			RAISERROR(N'check_id 42: Disabled indexes', 0,1) WITH NOWAIT;
			--Note: disabled NC indexes will have O rows in #IndexSanitySize!
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					WHERE	is_disabled = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 43: Heaps with forwarded records or deletes', 0,1) WITH NOWAIT;
			WITH	heaps_cte
					  AS ( SELECT	[object_id], 
									SUM(forwarded_fetch_count) AS forwarded_fetch_count,
									SUM(leaf_delete_count) AS leaf_delete_count
						   FROM		#IndexPartitionSanity
						   GROUP BY	[object_id]
						   HAVING	SUM(forwarded_fetch_count) > 0
									OR SUM(leaf_delete_count) > 0)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN heaps_cte h ON i.[object_id] = h.[object_id]
						JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
						WHERE	i.index_id = 0 
				OPTION	( RECOMPILE );

			RAISERROR(N'check_id 44: Heaps with reads or writes.', 0,1) WITH NOWAIT;
			WITH	heaps_cte
					  AS ( SELECT	[object_id], SUM(forwarded_fetch_count) AS forwarded_fetch_count,
									SUM(leaf_delete_count) AS leaf_delete_count
						   FROM		#IndexPartitionSanity
						   GROUP BY	[object_id]
						   HAVING	SUM(forwarded_fetch_count) > 0
									OR SUM(leaf_delete_count) > 0)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						LEFT JOIN heaps_cte h ON i.[object_id] = h.[object_id]
						JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
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
							FROM	#IndexSanity AS i
							LEFT	JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
						   GROUP BY	i.[object_id])
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
				FROM	#MissingIndexes mi
						LEFT JOIN index_size_cte sz ON mi.[object_id] = sz.object_id
				WHERE magic_benefit_number > 500000
				ORDER BY magic_benefit_number DESC;

	END
		 ----------------------------------------
		--Abnormal Psychology : Check_id 60-79
		----------------------------------------
	BEGIN
			RAISERROR(N'check_id 60: XML indexes', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_XML = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 61: Columnstore indexes', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
										   secret_columns, index_usage_summary, index_size_summary )
					SELECT	61 AS check_id, 
							i.index_sanity_id,
							N'Abnormal Psychology' AS findings_group,
							CASE WHEN i.is_NC_columnstore=1
								THEN N'NC Columnstore Index' 
								ELSE N'Clustered Columnstore Index' 
								END AS finding, 
							N'http://BrentOzar.com/go/AbnormalPsychology' AS URL,
							i.schema_object_indexid AS details, 
							i.index_definition,
							i.secret_columns,
							i.index_usage_summary,
							ISNULL(sz.index_size_summary,'') AS index_size_summary
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_NC_columnstore = 1 OR i.is_CX_columnstore=1
					OPTION	( RECOMPILE );


			RAISERROR(N'check_id 62: Spatial indexes', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.is_spatial = 1 OPTION	( RECOMPILE );

			RAISERROR(N'check_id 63: Compressed indexes', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE sz.data_compression_desc LIKE '%PAGE%' OR sz.data_compression_desc LIKE '%ROW%' OPTION	( RECOMPILE );

			RAISERROR(N'check_id 64: Partitioned', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NOT NULL OPTION	( RECOMPILE );

			RAISERROR(N'check_id 65: Non-Aligned Partitioned', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanity AS iParent ON
						i.[object_id]=iParent.[object_id]
						AND iParent.index_id IN (0,1) /* could be a partitioned heap or clustered table */
						AND iParent.partition_key_column_name IS NOT NULL /* parent is partitioned*/         
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.partition_key_column_name IS NULL 
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 66: Recently created tables/indexes (1 week)', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.create_date >= DATEADD(dd,-7,GETDATE()) 
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 67: Recently modified tables/indexes (2 days)', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					FROM	#IndexSanity AS i
					JOIN #IndexSanitySize sz ON i.index_sanity_id = sz.index_sanity_id
					WHERE i.modify_date > DATEADD(dd,-2,GETDATE()) 
					and /*Exclude recently created tables unless they've been modified after being created.*/
					(i.create_date < DATEADD(dd,-7,GETDATE()) or i.create_date <> i.modify_date)
						OPTION	( RECOMPILE );

			RAISERROR(N'check_id 68: Identity columns within 30 percent of the end of range', 0,1) WITH NOWAIT;
			-- Allowed Ranges: 
				--int -2,147,483,648 to 2,147,483,647
				--smallint -32,768 to 32,768
				--tinyint 0 to 255

				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
											   secret_columns, index_usage_summary, index_size_summary )
						SELECT	68 AS check_id, 
								i.index_sanity_id, 
								N'Abnormal Psychology' AS findings_group,
								N'Identity column within ' + 									
									CAST (calc1.percent_remaining as nvarchar(256))
									+ N' percent  end of range' AS finding,
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
						FROM	#IndexSanity i
						JOIN	#IndexColumns ic on
							i.object_id=ic.object_id
							and i.index_id in (0,1) /* heaps and cx only */
							and ic.is_identity=1
							and ic.system_type_name in ('tinyint', 'smallint', 'int')
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
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
								END AS NUMERIC(5,1)) AS percent_remaining
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
						FROM	#IndexSanity i
						JOIN	#IndexColumns ic on
							i.object_id=ic.object_id
							and i.index_id in (0,1) /* heaps and cx only */
							and ic.is_identity=1
							and ic.system_type_name in ('tinyint', 'smallint', 'int')
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						WHERE	i.index_id in (1,0)
							and (ic.seed_value < 0 or ic.increment_value <> 1)
						ORDER BY finding, details DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 69: Column collation does not match database collation', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								COUNT(*) as column_count
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
								and collation_name <> @collation
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 70: Replicated columns', 0,1) WITH NOWAIT;
				WITH count_columns AS (
							SELECT [object_id],
								COUNT(*) as column_count,
								SUM(CASE is_replicated WHEN 1 THEN 1 ELSE 0 END) as replicated_column_count
							FROM #IndexColumns ic
							WHERE index_id in (1,0) /*Heap or clustered only*/
							GROUP BY object_id
							)
				INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
						FROM	#IndexSanity i
						JOIN	#IndexSanitySize ip ON i.index_sanity_id = ip.index_sanity_id
						JOIN	count_columns AS cc ON i.[object_id]=cc.[object_id]
						WHERE	i.index_id in (1,0)
							and replicated_column_count > 0
						ORDER BY i.schema_object_name DESC OPTION	( RECOMPILE );

			RAISERROR(N'check_id 71: Cascading updates or cascading deletes.', 0,1) WITH NOWAIT;
			INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
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
					(SELECT TOP 1 more_info from #IndexSanity i where i.object_id=fk.parent_object_id)
						AS more_info
			from #ForeignKeys fk
			where [delete_referential_action_desc] <> N'NO_ACTION'
			OR [update_referential_action_desc] <> N'NO_ACTION'

	END

		 ----------------------------------------
		--Workaholics: Check_id 80-89
		----------------------------------------
	BEGIN

		RAISERROR(N'check_id 80: Most scanned indexes (index_usage_stats)', 0,1) WITH NOWAIT;
		INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
							   secret_columns, index_usage_summary, index_size_summary )

		--Workaholics according to index_usage_stats
		--This isn't perfect: it mentions the number of scans present in a plan
		--A "scan" isn't necessarily a full scan, but hey, we gotta do the best with what we've got.
		--in the case of things like indexed views, the operator might be in the plan but never executed
		SELECT TOP 5 
			80 AS check_id,
			i.index_sanity_id as index_sanity_id,
			N'Workaholics' as findings_group,
			N'Scan-a-lots (index_usage_stats)' as finding,
			N'http://BrentOzar.com/go/Workaholics' AS URL,
			REPLACE(CONVERT( NVARCHAR(50),CAST(i.user_scans AS MONEY),1),'.00','')
				+ N' scans against ' + i.schema_object_indexid
				+ N'. Latest scan: ' + ISNULL(cast(i.last_user_scan as nvarchar(128)),'?') + N'. ' 
				+ N'ScanFactor=' + cast(((i.user_scans * iss.total_reserved_MB)/1000000.) as NVARCHAR(256)) as details,
			isnull(i.key_column_names_with_sort_order,'N/A') as index_definition,
			isnull(i.secret_columns,'') as secret_columns,
			i.index_usage_summary as index_usage_summary,
			iss.index_size_summary as index_size_summary
		FROM #IndexSanity i
		JOIN #IndexSanitySize iss on i.index_sanity_id=iss.index_sanity_id
		WHERE isnull(i.user_scans,0) > 0
		ORDER BY  i.user_scans * iss.total_reserved_MB DESC;

		RAISERROR(N'check_id 81: Top recent accesses (op stats)', 0,1) WITH NOWAIT;
		INSERT	#BlitzIndexResults ( check_id, index_sanity_id, findings_group, finding, URL, details, index_definition,
							   secret_columns, index_usage_summary, index_size_summary )
		--Workaholics according to index_operational_stats
		--This isn't perfect either: range_scan_count contains full scans, partial scans, even seeks in nested loop ops
		--But this can help bubble up some most-accessed tables 
		SELECT TOP 5 
			81 as check_id,
			i.index_sanity_id as index_sanity_id,
			N'Workaholics' as findings_group,
			N'Top recent accesses (index_op_stats)' as finding,
			N'http://BrentOzar.com/go/Workaholics' AS URL,
			ISNULL(REPLACE(
					CONVERT(NVARCHAR(50),cast((iss.total_range_scan_count + iss.total_singleton_lookup_count) AS MONEY),1),
					N'.00',N'') 
				+ N' uses of ' + i.schema_object_indexid + N'. '
				+ REPLACE(CONVERT(NVARCHAR(50), CAST(iss.total_range_scan_count AS MONEY),1),N'.00',N'') + N' scans or seeks. '
				+ REPLACE(CONVERT(NVARCHAR(50), CAST(iss.total_singleton_lookup_count AS MONEY), 1),N'.00',N'') + N' singleton lookups. '
				+ N'OpStatsFactor=' + cast(((((iss.total_range_scan_count + iss.total_singleton_lookup_count) * iss.total_reserved_MB))/1000000.) as varchar(256)),'') as details,
			isnull(i.key_column_names_with_sort_order,'N/A') as index_definition,
			isnull(i.secret_columns,'') as secret_columns,
			i.index_usage_summary as index_usage_summary,
			iss.index_size_summary as index_size_summary
		FROM #IndexSanity i
		JOIN #IndexSanitySize iss on i.index_sanity_id=iss.index_sanity_id
		WHERE isnull(iss.total_range_scan_count,0)  > 0 or isnull(iss.total_singleton_lookup_count,0) > 0
		ORDER BY ((iss.total_range_scan_count + iss.total_singleton_lookup_count) * iss.total_reserved_MB) DESC;


	END

		 ----------------------------------------
		--FINISHING UP
		----------------------------------------
	BEGIN
				INSERT	#BlitzIndexResults ( check_id, findings_group, finding, URL, details, index_definition,secret_columns,
											   index_usage_summary, index_size_summary )
				VALUES  ( 1000 , N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + convert(nvarchar(16),getdate(),121)	,
						N'' ,   N'http://www.BrentOzar.com/BlitzIndex' ,
						N'Thanks from the Brent Ozar Unlimited(TM), LLC team.',
						N'We hope you found this tool useful.',
						N'If you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.'
						, N'',N''
						);


	END
		RAISERROR(N'Returning results.', 0,1) WITH NOWAIT;
			
		/*Return results.*/
		SELECT isnull(br.findings_group,N'') + 
				CASE WHEN ISNULL(br.finding,N'') <> N'' THEN N': ' ELSE N'' END
				+ br.finding AS [Finding], 
			br.URL, 
			br.details AS [Details: schema.table.index(indexid)], 
			br.index_definition AS [Definition: [Property]] ColumnName {datatype maxbytes}], 
			ISNULL(br.secret_columns,'') AS [Secret Columns],          
			br.index_usage_summary AS [Usage], 
			br.index_size_summary AS [Size],
			COALESCE(br.more_info,sn.more_info,'') AS [More Info],
			COALESCE(br.create_tsql,ts.create_tsql,'') AS [Create TSQL]
		FROM #BlitzIndexResults br
		LEFT JOIN #IndexSanity sn ON 
			br.index_sanity_id=sn.index_sanity_id
		LEFT JOIN #IndexCreateTsql ts ON 
			br.index_sanity_id=ts.index_sanity_id
		ORDER BY [check_id] ASC, blitz_result_id ASC, findings_group;

	END; /* End @Mode=0 (diagnose)*/
	ELSE IF @Mode=1 /*Summarize*/
	BEGIN
	--This mode is to give some overall stats on the database.
		RAISERROR(N'@Mode=1, we are summarizing.', 0,1) WITH NOWAIT;

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
		FROM #IndexSanity AS i
		--left join here so we don't lose disabled nc indexes
		LEFT JOIN #IndexSanitySize AS sz 
			ON i.index_sanity_id=sz.index_sanity_id 
		UNION ALL
		SELECT	N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + convert(nvarchar(16),getdate(),121)	,		
				N'sp_BlitzIndex(TM) v2.02 - Jan 30, 2014' ,   
				N'From Brent Ozar Unlimited(TM)' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited(TM) team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,0 as display_order
		ORDER BY [Display Order] ASC
		OPTION (RECOMPILE);
	   	
	END /* End @Mode=1 (summarize)*/
	ELSE IF @Mode=2 /*Index Detail*/
	BEGIN
		--This mode just spits out all the detail without filters.
		--This supports slicing AND dicing in Excel
		RAISERROR(N'@Mode=2, here''s the details on existing indexes.', 0,1) WITH NOWAIT;

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
				is_CX_columnstore AS [Is CX Columnstore],
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
		FROM	#IndexSanity AS i --left join here so we don't lose disabled nc indexes
				LEFT JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
		UNION ALL
		SELECT 	N'Database ' + QUOTENAME(@DatabaseName) + N' as of ' + convert(nvarchar(16),getdate(),121)			
				N'sp_BlitzIndex(TM) v2.02 - Jan 30, 2014' ,   
				N'From Brent Ozar Unlimited(TM)' ,   
				N'http://BrentOzar.com/BlitzIndex' ,
				N'Thanks from the Brent Ozar Unlimited(TM) team.  We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL, NULL,NULL, NULL, NULL, NULL, NULL,NULL,NULL,
				0 as [Display Order]
		ORDER BY [Display Order] ASC, [Reserved MB] DESC
		OPTION (RECOMPILE);

	END /* End @Mode=2 (index detail)*/
	ELSE IF @Mode=3 /*Missing index Detail*/
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
		FROM #MissingIndexes
		UNION ALL
		SELECT 				
			N'sp_BlitzIndex(TM) v2.02 - Jan 30, 2014' ,   
			N'From Brent Ozar Unlimited(TM)' ,   
			N'http://BrentOzar.com/BlitzIndex' ,
			100000000000,
			N'Thanks from the Brent Ozar Unlimited(TM) team. We hope you found this tool useful, and if you need help relieving your SQL Server pains, email us at Help@BrentOzar.com.',
			NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
			NULL, 0 as display_order
		ORDER BY [Display Order] ASC, [Magic Benefit Number] DESC

	END /* End @Mode=3 (index detail)*/
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

