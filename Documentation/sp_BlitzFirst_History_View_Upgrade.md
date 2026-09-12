# Upgrading sp_BlitzFirst history delta views

The server-scoped history fix upgrades the file, Perfmon, and wait-stat `_Deltas`
views in place. Existing object IDs and grants are preserved. A marker in each
view definition prevents subsequent collections from altering it again.

Before resuming a scheduled collection after this upgrade, have the history
database owner run sp_BlitzFirst once with the same output database, schema,
and file/Perfmon/wait table names used by the job. For example, replacing these
names with the job's actual settings:

```sql
EXEC master.dbo.sp_BlitzFirst
    @Seconds = 1,
    @OutputDatabaseName = N'DBAHistory',
    @OutputSchemaName = N'dbo',
    @OutputTableNameFileStats = N'BlitzFirst_FileStats',
    @OutputTableNamePerfmonStats = N'BlitzFirst_PerfmonStats',
    @OutputTableNameWaitStats = N'BlitzFirst_WaitStats';
```

That first call needs permission to alter the existing views. A collector that
can only insert history rows cannot perform this migration. Run it as the
history database owner instead of permanently granting the scheduled collector
DDL permissions.

The scheduled collector must also be able to read the definitions of the delta views its job creates
to recognize that migration is complete. In the output database, grant this to
the collector's database user if its existing permissions do not already allow
it. Grant only on views created by the job: an output-table parameter set to
NULL does not create its delta view. Omit the corresponding GRANT below and
substitute the actual names:

```sql
USE DBAHistory;
GRANT VIEW DEFINITION ON dbo.BlitzFirst_FileStats_Deltas TO HistoryCollector;
GRANT VIEW DEFINITION ON dbo.BlitzFirst_PerfmonStats_Deltas TO HistoryCollector;
GRANT VIEW DEFINITION ON dbo.BlitzFirst_WaitStats_Deltas TO HistoryCollector;
```

These grants provide metadata visibility, not permission to alter the views.
The collector still needs its existing procedure, DMV, and output-table
permissions. Without metadata visibility, the procedure cannot recognize the
marker and will attempt the migration again. Do not remove or change the
`FRK_ServerScopedDeltas_v1` marker in migrated view definitions.
