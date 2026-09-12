"""Generate a real deadlock and require both parsed participants in system_health.

Runs only on the disposable boxed SQL Server smoke-test instance.
"""
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import uuid

# sqlcmd reads SQLCMDPASSWORD from the environment; keep secrets off argv.
client_env = os.environ.copy()
if not client_env.get('SQLCMDPASSWORD'):
    raise RuntimeError('SQLCMDPASSWORD must be set for the SQL-authenticated test.')

suffix = uuid.uuid4().hex[:12]
schema = 'FRKRing_' + suffix
table = 'Fixture_' + suffix
args = [os.environ.get('SQLCMD', 'sqlcmd'), '-S', os.environ['SQLCMDSERVER'],
        '-U', os.environ['SQLCMDUSER'], '-C', '-I', '-b', '-l', '60', '-t', '90']

def run(sql, database='FRKSmokeTest', check=True):
    result = subprocess.run(args + ['-d', database, '-Q', sql],
                            capture_output=True, text=True, timeout=120, env=client_env)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result

# This instance is disposable. Reset stale/truncated ring contents and shorten
# dispatch while generating the fixture; restore the configured latency below.
latency_result = subprocess.run(args + ['-d', 'master', '-h', '-1', '-W', '-Q',
    "SET NOCOUNT ON; SELECT max_dispatch_latency FROM sys.server_event_sessions WHERE name=N'system_health';"],
    capture_output=True, text=True, timeout=120, env=client_env, check=True)
latency = int(latency_result.stdout.strip())
original_latency = "INFINITE" if latency == 0 else f"{latency // 1000} SECONDS"
run(f"CREATE SCHEMA [{schema}];")
try:
    run("ALTER EVENT SESSION system_health ON SERVER STATE=STOP; "
        "ALTER EVENT SESSION system_health ON SERVER WITH (MAX_DISPATCH_LATENCY=1 SECONDS); "
        "ALTER EVENT SESSION system_health ON SERVER STATE=START;", database='master')
    run(f"CREATE TABLE [{schema}].[{table}](ID int PRIMARY KEY, V int); "
        f"INSERT [{schema}].[{table}] VALUES(1,0),(2,0);")
    def worker(own):
        other = 3 - own
        return run(f"""SET LOCK_TIMEOUT 30000;
BEGIN TRAN;
UPDATE [{schema}].[{table}] WITH (ROWLOCK) SET V = 1 WHERE ID = {own};
DECLARE @Deadline datetime2 = DATEADD(second,30,SYSUTCDATETIME());
WHILE NOT EXISTS(SELECT 1 FROM [{schema}].[{table}] WITH (READUNCOMMITTED) WHERE ID={other} AND V=1)
BEGIN
    IF SYSUTCDATETIME() > @Deadline THROW 51000,'Deadlock worker barrier timed out.',1;
    WAITFOR DELAY '00:00:00.100';
END;
UPDATE [{schema}].[{table}] WITH (ROWLOCK) SET V = V + 1 WHERE ID={other};
COMMIT;""", check=False)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, [1, 2]))
    failures = [r for r in results if r.returncode]
    if len(failures) != 1 or '1205' not in failures[0].stdout + failures[0].stderr:
        raise RuntimeError('Expected exactly one deadlock victim.\n' +
                           '\n'.join(r.stdout + r.stderr for r in results))

    # Wait for the actual fixture event to reach the ring buffer; old events
    # from earlier tests cannot satisfy the unique table-name assertion.
    run(f"""DECLARE @Deadline datetime2 = DATEADD(second,20,SYSUTCDATETIME());
WHILE NOT EXISTS
(SELECT 1 FROM sys.dm_xe_session_targets t JOIN sys.dm_xe_sessions s
 ON s.address=t.event_session_address
 WHERE s.name=N'system_health' AND t.target_name=N'ring_buffer'
 AND CONVERT(nvarchar(max),t.target_data) LIKE N'%{table}%')
BEGIN
 IF SYSUTCDATETIME()>@Deadline THROW 51000,'Fixture deadlock did not reach system_health ring buffer.',1;
 WAITFOR DELAY '00:00:00.100';
END;
EXEC master.dbo.sp_BlitzLock @EventSessionName=N'system_health', @TargetSessionType=N'ring_buffer',
 @DatabaseName=N'FRKSmokeTest', @SkipExecutionPlans=1,
 @OutputDatabaseName=N'FRKSmokeTest', @OutputSchemaName=N'{schema}', @OutputTableName=N'Deadlocks';
IF (SELECT COUNT(DISTINCT spid) FROM [{schema}].Deadlocks
 WHERE CONVERT(nvarchar(max),deadlock_graph) LIKE N'%{table}%') <> 2
 THROW 51000,'sp_BlitzLock did not parse both fixture deadlock participants.',1;
""")
    print('PASS system_health ring buffer contains and parses both real deadlock participants')
finally:
    run("ALTER EVENT SESSION system_health ON SERVER STATE=STOP; "
        f"ALTER EVENT SESSION system_health ON SERVER WITH (MAX_DISPATCH_LATENCY={original_latency}); "
        "ALTER EVENT SESSION system_health ON SERVER STATE=START;", database='master')
    # The procedure normally removes its synonyms; clean owned leftovers if it
    # returned early or a test failed. Never remove another target's synonym.
    run(f"""IF EXISTS(SELECT 1 FROM sys.synonyms WHERE name=N'DeadLockTbl' AND base_object_name LIKE N'%{schema}%')
 DROP SYNONYM dbo.DeadLockTbl;
IF EXISTS(SELECT 1 FROM sys.synonyms WHERE name=N'DeadlockFindings' AND base_object_name LIKE N'%{schema}%')
 DROP SYNONYM dbo.DeadlockFindings;""", database='master')
    run(f"DROP TABLE IF EXISTS [{schema}].Deadlocks; DROP TABLE IF EXISTS [{schema}].BlitzLockFindings; "
        f"DROP TABLE IF EXISTS [{schema}].[{table}]; DROP SCHEMA [{schema}];")
