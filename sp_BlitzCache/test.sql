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



/* Testing output to table mode */
IF OBJECT_ID('tempdb.dbo.blitzcache') IS NOT NULL
    DROP TABLE tempdb.dbo.blitzcache

EXEC sp_BlitzCache @output_database_name = 'tempdb',
                   @output_schema_name = 'dbo',
                   @output_table_name = 'blitzcache' ;



/* Test sp_BlitzCache with a configuration table */
IF OBJECT_ID('tempdb.dbo.blitzcache_config') IS NOT NULL
   DROP TABLE tempdb.dbo.blitzcache_config;

CREATE TABLE tempdb.dbo.blitzcache_config
(
       parameter_name VARCHAR(100) ,
       value DECIMAL(38, 0)
);

INSERT INTO tempdb.dbo.blitzcache_config VALUES ('frequent execution threshold', 1);
INSERT INTO tempdb.dbo.blitzcache_config VALUES ('parameter sniffing variance percent', 1);
INSERT INTO tempdb.dbo.blitzcache_config VALUES ('parameter sniffing io threshold', 1);
INSERT INTO tempdb.dbo.blitzcache_config VALUES ('cost threshold for parallelism warning', 1);
INSERT INTO tempdb.dbo.blitzcache_config VALUES ('long running query warning (seconds)', 0);

EXEC sp_BlitzCache @configuration_database_name = 'tempdb',
                   @configuration_schema_name = 'dbo',
                   @configuration_table_name = 'blitzcache_config' ;
