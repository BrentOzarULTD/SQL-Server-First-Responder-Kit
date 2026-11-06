"""Two disposable SQL Server instances; same connection variables as the linked-server regression."""
from plancompare_test_helpers import *
from contextlib import ExitStack
import os

local_server=os.environ.get('FRK_TEST_SERVER','127.0.0.1:11433')
with ExitStack() as stack:
    local=stack.enter_context(Database('ScopeLocal'))
    os.environ['FRK_TEST_SERVER']=os.environ.get('FRK_TEST_REMOTE_SERVER','127.0.0.1:11434')
    try:
        remote=stack.enter_context(Database('ScopeRemote'))
    finally:
        os.environ['FRK_TEST_SERVER']=local_server
    install(local.c)
    for d in (local,remote):
        execute(d.c,'ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS=ON; CREATE TABLE dbo.T(id int); INSERT dbo.T VALUES(1),(2);')
        execute(d.c,'CREATE PROC dbo.P AS SELECT SUM(id) total FROM dbo.T;');execute(d.c,'EXEC dbo.P;')
    assert hashes(local.c,'dbo.P')[0]['query_hash']==hashes(remote.c,'dbo.P')[0]['query_hash']
    execute(local.admin,"EXEC sys.sp_addlinkedserver @server=N'FRKPlanCompareScope',@srvproduct=N'',@provider=N'MSOLEDBSQL',@datasrc=%s,@provstr=N'TrustServerCertificate=yes';",
            (os.environ.get('FRK_TEST_REMOTE_DATASOURCE','frk-plancompare-remote'),))
    try:
        execute(local.admin,"EXEC sys.sp_addlinkedsrvlogin @rmtsrvname=N'FRKPlanCompareScope',@useself=N'False',@rmtuser=N'sa',@rmtpassword=%s",(os.environ['FRK_TEST_PASSWORD'],))
        execute(local.admin,"EXEC sys.sp_serveroption N'FRKPlanCompareScope',N'rpc out',N'true'; EXEC sys.sp_serveroption N'FRKPlanCompareScope',N'remote proc transaction promotion',N'false';")
        for proc in ('P','dbo.P',local.name+'.dbo.P'):
            result=execute(local.c,"EXEC dbo.sp_BlitzPlanCompare @StoredProcName=%s,@LinkedServer=N'FRKPlanCompareScope';",(proc,))[0]
            assert result and not any(r['Category']=='Setup' for r in result),result
            assert any(r['Category']=='Meta' and r['Setting']=='SourceServer' and r['LocalValue']!=r['RemoteValue'] for r in result),result
        # An explicit caller filter still constrains both sides; the remote has no local-named database.
        try:
            result=execute(local.c,"EXEC dbo.sp_BlitzPlanCompare @StoredProcName=N'dbo.P',@DatabaseName=%s,@LinkedServer=N'FRKPlanCompareScope';",(local.name,))[0]
            assert any(r['Category']=='Setup' for r in result),result
        except pymssql.DatabaseError as e:
            assert 'No cached plan matched' in str(e) and local.name in str(e),str(e)
    finally:
        execute(local.admin,"EXEC sys.sp_dropserver N'FRKPlanCompareScope',N'droplogins';")
print('PASS: inferred local database names do not constrain remote lookup; explicit caller filters are retained.')
