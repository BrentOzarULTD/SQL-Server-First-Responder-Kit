EXEC sp_BlitzCache @results = 'narrow'
EXEC sp_BlitzCache @results = 'simple'
EXEC sp_BlitzCache @results = 'expert'

EXEC sp_BlitzCache @sort_order = 'cpu'
EXEC sp_BlitzCache @sort_order = 'reads'
EXEC sp_BlitzCache @sort_order = 'writes'
EXEC sp_BlitzCache @sort_order = 'duration'
EXEC sp_BlitzCache @sort_order = 'execution'

EXEC sp_BlitzCache @sort_order = 'cpu', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'reads', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'writes', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'duration', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'execution', @export_to_excel = 1

EXEC sp_BlitzCache @sort_order = 'average cpu', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'avg cpu', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'average reads', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'avg reads', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'average writes', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'avg writes', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'average duration', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'avg duration', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'average execution', @export_to_excel = 1
EXEC sp_BlitzCache @sort_order = 'avg execution', @export_to_excel = 1


IF OBJECT_ID('tempdb.dbo.blitzcache') IS NOT NULL
    DROP TABLE tempdb.dbo.blitzcache

EXEC sp_BlitzCache @output_database_name = 'tempdb', @output_schema_name = 'dbo', @output_table_name = 'blitzcache'

