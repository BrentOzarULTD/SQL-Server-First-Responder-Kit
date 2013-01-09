USE tpchscale1;
GO

--------------------------------
--CASE 1:
--NON-UNIQUE CLUSTERED INDEX
--------------------------------

EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='lineitem';
GO

--CLUSTERED INDEX: dbo.lineitem.l_shipdate_ind (1)	
--Index definition: [CX] [KEYS] l_shipdate	
--Secret columns: [UNIQUIFIER]


--Look at some sample data.
--We do have duplicate values in the key of shipdate!
--If SQL Server has to uniquify a row, it will-- and it costs 4 bytes for that row.

SELECT TOP 10 *
FROM dbo.lineitem;


--Find the root index page for the clustered index:

SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'),OBJECT_ID('lineitem'),1,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC

--Look at the root index page.
--sp_BlitzIndex said we'd see:
--Index definition: [CX] [KEYS] l_shipdate	
--Secret columns: [UNIQUIFIER]

DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,242242,3);
GO


--Look at a data page in the leaf level of the clustered index.

SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'),OBJECT_ID('lineitem'),1,NULL,'detailed')
WHERE page_type=1 /* data page*/
	AND page_level=0 /*leaf*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO

--Index definition: [CX] [KEYS] l_shipdate	
--Secret columns: [UNIQUIFIER]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,248446,3);
GO


--------------------------------
--CASE 2
--NON-UNIQUE NONCLUSTERED INDEX
--ON TABLE WITH A NON-UNIQUE CLUSTERED INDEX
--------------------------------

--Nonclustered Index: dbo.lineitem.l_orderkey_ind (2)

--Find the root index page for the non-clustered index
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('lineitem'),2,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--Look at the root index page.
--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [KEYS] l_orderkey	
--SECRET COLUMNS: [KEYS] l_shipdate [UNIQUIFIER]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,22586,3);
GO




--Now look at a page from the leaf (level=0)
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('lineitem'),2,NULL,'detailed')
WHERE page_level=0 /* leaf */
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [KEYS] l_orderkey	
--SECRET COLUMNS: [KEYS] l_shipdate [UNIQUIFIER]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,242142,3);
GO

--This is: dbo.lineitem.l_orderkey_ind


--What if I move l_shipdate?
--Can I return the value of the secret column?
SELECT l_shipdate
FROM dbo.lineitem
WHERE l_orderkey=5486433;
GO


--Can I use the secret key column for a seek?
SELECT COUNT(*)
FROM dbo.lineitem
WHERE l_orderkey=5486433
	AND l_shipdate='1998-09-24';
GO



--------------------------------
--CASE 3
--UNIQUE NONCLUSTERED INDEX
--WITH INCLUDE SPECIFIED
--ON TABLE WITH A NON-UNIQUE CLUSTERED INDEX
--------------------------------

CREATE UNIQUE INDEX [kl_test_orderkey_linenumber_ind] 
	ON [dbo].[lineitem] ( l_orderkey, l_linenumber ) 
		INCLUDE (l_quantity)
	WITH (ONLINE=ON, MAXDOP=2);
GO


--First, let's prove that the 'included' column in the index doesn't count toward uniqueness.
--Check out this lineitem.

SELECT l_linenumber, l_quantity
FROM dbo.lineitem
WHERE l_orderkey= 2511555

--We are enforcing uniqueness on the two keys:
--[UNIQUE] [KEYS] l_orderkey, l_linenumber 
--So we can't do this.
BEGIN TRAN
	UPDATE dbo.lineitem
	SET l_linenumber=2
	WHERE l_orderkey= 2511555
		AND l_linenumber=3;

ROLLBACK;

--But we aren't enforcing uniqueness on the includes.
--We specified the included column l_quantity.
--We can still make that duplicate values.
BEGIN TRAN
	UPDATE dbo.lineitem
	SET l_quantity=1
	WHERE l_orderkey= 2511555;

ROLLBACK;


--How is a UNIQUE nonclustered index different?
EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='lineitem';
GO



--Nonclustered Index: 
--dbo.lineitem.kl_test_orderkey_linenumber_ind (14)



--Find the root index page for the non-clustered index.
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('lineitem'),14,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO




--Look at the root index page.
--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [UNIQUE] [KEYS] l_orderkey, l_linenumber [INCLUDES]  l_quantity		
--SECRET COLUMNS: [INCLUDES] l_shipdate [UNIQUIFIER]

DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,72337,3);
GO




--Now look at a page from the leaf (level=0)
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('lineitem'),14,NULL,'detailed')
WHERE page_level=0 /* leaf */
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [UNIQUE] [KEYS] l_orderkey, l_linenumber [INCLUDES]  l_quantity		
--SECRET COLUMNS: [INCLUDES] l_shipdate [UNIQUIFIER]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,101959,3);
GO



--Can we use the secret column in this index?
SELECT l_linenumber, l_shipdate, l_quantity
FROM dbo.lineitem
WHERE l_orderkey=5486433
ORDER BY l_linenumber;
GO




--What if I want all the columns?
--You can see how it has to use the uniquifier!
SELECT *
FROM dbo.lineitem
WHERE l_orderkey=5486433
ORDER BY l_linenumber;
GO



--Clean up
DROP INDEX [kl_test_orderkey_linenumber_ind] 
	ON [dbo].[lineitem];
GO




--------------------------------
--CASE 4:
--NON-UNIQUE NON-CLUSTERED INDEX
--ON A TABLE WITH A UNIQUE CLUSTERED INDEX
--------------------------------

--This is a very common configuration.

EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='supplier';
GO



--Nonclustered Index: 
--dbo.supplier.s_nationkey_ind (2)



--Find the root index page for the non-clustered index.
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('supplier'),2,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO




--Look at the root index page.
--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [KEYS] s_nationkey	
--SECRET COLUMNS: [KEYS] s_suppkey
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,3484,3);
GO




--Now look at a page from the leaf (level=0)
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('supplier'),2,NULL,'detailed')
WHERE page_level=0 /* leaf */
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [KEYS] s_nationkey	
--SECRET COLUMNS: [KEYS] s_suppkey
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,52065,3);
GO



--Can we use the secret column in this index?
--This is: dbo.supplier.s_nationkey_ind (2)
SELECT s_suppkey, s_nationkey
FROM dbo.supplier
WHERE  s_nationkey=23
ORDER BY s_suppkey;
GO



--------------------------------
--CASE 5:
--HEAP WITH A UNIQUE NONCLUSTERED INDEX
--------------------------------

--We don't have a heap in this database by default
--So let's make one!
--An 'un-makeover'

--The Orders table: BEFORE
EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='orders';
GO


DROP INDEX o_orderdate_ind ON dbo.orders;
GO

--The Orders table: AFTER
EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='orders';
GO

--We now have a fairly common finding: 
--A heap with a secret column: [RID]
--A nonclustered PK: dbo.orders.orders_pk (2)
--This NC PK includes the [RID]




--Find the root index page for the non-clustered index.
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('orders'),2,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO




--Look at the root index page.
--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [PK] [KEYS] o_orderkey
--SECRET COLUMNS: [INCLUDES] [RID]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,84882,3);
GO




--Now look at a page from the leaf (level=0)
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('orders'),2,NULL,'detailed')
WHERE page_level=0 /* leaf */
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--sp_BlitzIndex said we'd see:
--INDEX DEFINITION: [PK] [KEYS] o_orderkey
--SECRET COLUMNS: [INCLUDES] [RID]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,89247,3);
GO

--Each RID is 8 bytes


--Can we use the secret column in this index?
--This is: dbo.supplier.s_nationkey_ind (2)
SELECT *
FROM dbo.orders
WHERE  o_orderkey = 5998114;
GO



--------------------------------
--CASE 5:
--HEAP WITH A NON-UNIQUE NONCLUSTERED INDEX
--------------------------------

--This is not uncommon!

CREATE INDEX [kl_test_orderkey_linenumber_ind] 
	ON [dbo].[orders] ( o_orderkey) 
		INCLUDE (o_orderdate, o_orderpriority, o_orderstatus, o_totalprice)
	WITH (ONLINE=ON, MAXDOP=2);
GO

--The Orders table: NOW WITH MORE MADNESS
EXEC dbo.sp_BlitzIndex @database_name='tpchscale1', @schema_name='dbo', @table_name='orders';
GO



--Our new index: dbo.orders.kl_test_orderkey_linenumber_ind (7)
--We specified: [KEYS] o_orderkey [INCLUDES]  o_orderdate, o_orderpriority, o_orderstatus, o_totalprice
--Secret columns: [KEYS] [RID]



--Find the root index page for the non-clustered index.
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('orders'),7,NULL,'detailed')
WHERE page_type=2 /* index page*/
ORDER BY page_level DESC, next_page_page_id DESC;
GO




--Look at the root index page.
--sp_BlitzIndex said we'd see:
--We specified: [KEYS] o_orderkey [INCLUDES]  o_orderdate, o_orderpriority, o_orderstatus, o_totalprice
--Secret columns: [KEYS] [RID]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,306,3);
GO




--Now look at a page from the leaf (level=0)
SELECT TOP 10 allocated_page_page_id, page_type_desc, page_level, next_page_page_id, previous_page_page_id
FROM sys.dm_db_database_page_allocations(DB_ID('tpchscale1'), OBJECT_ID('orders'),7,NULL,'detailed')
WHERE page_level=0 /* leaf */
ORDER BY page_level DESC, next_page_page_id DESC;
GO


--sp_BlitzIndex said we'd see:
--We specified: [KEYS] o_orderkey [INCLUDES]  o_orderdate, o_orderpriority, o_orderstatus, o_totalprice
--Secret columns: [KEYS] [RID]
DBCC TRACEON (3604);
DBCC PAGE('tpchscale1', 1,100345,3);
GO

--What about this query?
SELECT o_comment
FROM orders 
WHERE o_orderkey > 2998848;
GO


--------------------------------
--TLDR;
--------------------------------
--Columns hide in your indexes!
--Make indexes unique when possible
--Make the column that connects your indexes as useful as possible.