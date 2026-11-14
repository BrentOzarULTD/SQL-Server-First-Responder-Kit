"""Two disposable instances required. FRK_TEST_SERVER defaults to 127.0.0.1:11433;
FRK_TEST_REMOTE_SERVER defaults to 127.0.0.1:11434, and FRK_TEST_REMOTE_DATASOURCE
is the remote instance's address as seen by the first instance (Docker name by default).
Uses FRK_TEST_USER/FRK_TEST_PASSWORD for both. Set FRK_TEST_LINKED_SERVER to reuse
an existing link without changing it; otherwise owns and removes a temporary link.
"""
from plancompare_test_helpers import *
import os
from contextlib import ExitStack

local_server=os.environ.get('FRK_TEST_SERVER','127.0.0.1:11433')
remote_server=os.environ.get('FRK_TEST_REMOTE_SERVER','127.0.0.1:11434')
with ExitStack() as stack:
    local=stack.enter_context(Database('Linked'))
    os.environ['FRK_TEST_SERVER']=remote_server
    try:
        remote=stack.enter_context(Database('Linked'))
    finally:
        os.environ['FRK_TEST_SERVER']=local_server
    for d in (local,remote):
        execute(d.c,'ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS=ON; CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T SELECT TOP(100) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects;')
        execute(d.c,'CREATE PROC dbo.P @p int AS SELECT SUM(n) n FROM dbo.T WHERE id>=@p OPTION(MAXDOP 1);')
        execute(d.c,'SET ARITHABORT ON; EXEC dbo.P 1; SET ARITHABORT OFF; EXEC dbo.P 99; SET ARITHABORT ON;')
    with linked_server(local.admin,'FRKPlanCompareRemote') as link:
        def candidates(d):
            result={}
            for h in hashes(d.c,'dbo.P'):
                snap=snapshot(d.c,h['query_plan_hash'],d.name,',@PlanHandle=%s,@StatementStartOffset=%s',
                              (h['plan_handle'],h['statement_start_offset']))
                result[rows(snap,'Parameter','CompiledValue')[0]['ValueText'].split(' = ',1)[-1]]=h
            assert set(result)=={'(1)','(99)'},result
            return result
        lh,rh=candidates(local),candidates(remote)
        left,right=lh['(1)'],rh['(99)']
        sets=execute(local.c,"""EXEC master.dbo.sp_BlitzPlanCompare @QueryPlanHash=%s,@DatabaseName=%s,
          @PlanHandle=%s,@StatementStartOffset=%s,@LinkedServer=%s,
          @RemotePlanHandle=%s,@RemoteStatementStartOffset=%s;""",
          (left['query_plan_hash'],local.name,left['plan_handle'],left['statement_start_offset'],link,
           right['plan_handle'],right['statement_start_offset']))
        diffs=sets[0]
        assert not any(r['Category']=='Setup' for r in diffs),diffs
        parameters=[r for r in diffs if r['Category']=='Parameter' and r['Setting']=='CompiledValue']
        assert len(parameters)==1 and parameters[0]['LocalValue'].endswith(' = (1)') and parameters[0]['RemoteValue'].endswith(' = (99)'),parameters
        # With the reproducer fixes installed, execute the complete emitted script
        # in the disposable database and verify its two parameter variants.
        repro=[r for r in diffs if r['Category']=='Parameter' and r['Setting']=='Reproducer']
        assert len(repro)==1,repro
        script=ET.fromstring(repro[0]['CallStack']).text
        results=[]
        for batch in re.split(r'^GO\s*$',script,flags=re.M|re.I):
            if batch.strip(): results.extend(execute(local.c,batch))
        assert [r[0]['n'] for r in results if r]==[100,2],results

print('PASS: linked-server comparison selects local (1) and remote (99), and the emitted reproducer executes both.')
