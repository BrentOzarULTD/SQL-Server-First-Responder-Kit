"""Check runtime labels against the actual plan, including asymmetric CPU/elapsed values."""
from plancompare_test_helpers import *
import threading
import time
with Database(4165) as d:
    execute(d.c, 'ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS=ON; CREATE TABLE dbo.T(id int,n int); INSERT dbo.T SELECT TOP(50000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;')
    execute(d.c, 'CREATE PROC dbo.P @p int AS SELECT SUM(n) FROM dbo.T WHERE id>=@p OPTION(MAXDOP 1);')
    execute(d.c, 'EXEC dbo.P @p=1;')
    blocker=connect(d.name)
    execute(blocker, 'BEGIN TRAN; UPDATE dbo.T WITH(TABLOCKX) SET n=n WHERE id=1;')
    worker=connect(d.name)
    errors=[]
    def run():
        try: execute(worker,'EXEC dbo.P @p=1;')
        except Exception as e: errors.append(e)
    t=threading.Thread(target=run);t.start();time.sleep(2)
    execute(blocker,'COMMIT;');t.join();worker.close();blocker.close()
    assert not errors,errors
    h=hashes(d.c,'dbo.P')[0]
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    timing=snap.find('.//p:QueryTimeStats',NS)
    assert timing is not None
    assert int(timing.get('ElapsedTime'))>1000 and int(timing.get('ElapsedTime'))>int(timing.get('CpuTime'))
    for setting,attribute in [('ElapsedTimeMs','ElapsedTime'),('CpuTimeMs','CpuTime')]:
        assert int(rows(snap,'PlanRuntime',setting)[0]['ValueText'])==int(timing.get(attribute))
    # Exercise UDF fields with known asymmetric values using the same production extraction block.
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    block=source[source.index('CREATE TABLE #PlanRuntime'):source.index('/* Per-wait-type runtime waits.')]
    result=execute(d.c,"DECLARE @PlanXmlForEmit xml=N'<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><QueryTimeStats CpuTime=\"7\" ElapsedTime=\"2000\" UdfCpuTime=\"3\" UdfElapsedTime=\"1000\"/></ShowPlanXML>';"+block+'SELECT Setting,ValueNumeric FROM #PlanRuntime;')[-1]
    values={r['Setting']:int(r['ValueNumeric']) for r in result}
    assert values['UdfCpuTimeMs']==3 and values['UdfElapsedTimeMs']==1000,values
print('PASS: #4165 runtime time labels')
