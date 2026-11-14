"""Compare timing with independent DMV counters, including blocked and selected statements."""
from plancompare_test_helpers import *
from decimal import Decimal
import threading
import time

with Database(4165) as d:
    execute(d.c, 'ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS=ON; CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T SELECT TOP(50000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;')
    execute(d.c, 'CREATE PROC dbo.P @p int AS SELECT COUNT_BIG(*) FROM sys.objects; SELECT SUM(n) FROM dbo.T WHERE id>=@p OPTION(MAXDOP 1);')
    execute(d.c, 'EXEC dbo.P @p=1;')
    blocker=connect(d.name)
    worker=connect(d.name)
    worker_id=execute(worker,'SELECT @@SPID id;')[0][0]['id']
    errors=[]
    def run():
        try: execute(worker,'EXEC dbo.P @p=1;')
        except Exception as e: errors.append(e)
    t=threading.Thread(target=run)
    try:
        execute(blocker, 'BEGIN TRAN; UPDATE dbo.T WITH(ROWLOCK) SET n=n+1 WHERE id=50000;')
        t.start()
        deadline=time.monotonic()+15
        while True:
            waiting=execute(d.c,'SELECT wait_type FROM sys.dm_exec_requests WHERE session_id=%s',(worker_id,))[0]
            if waiting and (waiting[0]['wait_type'] or '').startswith('LCK_M_'):
                break
            assert t.is_alive() and time.monotonic()<deadline,('worker did not block',errors,waiting)
            time.sleep(.05)
        time.sleep(1.25)
    finally:
        execute(blocker,'IF @@TRANCOUNT>0 ROLLBACK;')
        if t.ident is not None: t.join(70)
        worker.close();blocker.close()
    assert not t.is_alive() and not errors,errors

    def check_snapshot(h, actual):
        metrics=execute(d.c,'''SELECT last_elapsed_time,last_worker_time FROM sys.dm_exec_query_stats
            WHERE plan_handle=%s AND statement_start_offset=%s AND statement_end_offset=%s''',
            (h['plan_handle'],h['statement_start_offset'],h['statement_end_offset']))[0][0]
        snap=snapshot(d.c,h['query_plan_hash'],d.name,',@PlanHandle=%s,@StatementStartOffset=%s',
                      (h['plan_handle'],h['statement_start_offset']))
        for setting,column in [('ElapsedTimeMs','last_elapsed_time'),('CpuTimeMs','last_worker_time')]:
            value=rows(snap,'PlanRuntime',setting)
            assert len(value)==1,(setting,value)
            expected=Decimal(metrics[column])/1000
            assert Decimal(value[0]['ValueNumeric'])==expected,(setting,value,metrics)
            assert Decimal(value[0]['ValueText'])==expected,(setting,value,metrics)
        if not actual:
            assert rows(snap,'PlanAttribute','PlanSource')[0]['ValueText']=='Cached estimated'
        return metrics

    hs=hashes(d.c,'dbo.P')
    assert len(hs)==2,hs
    blocked=next(h for h in hs if 'SUM(n)' in h['statement_text'])
    metrics=check_snapshot(blocked,True)
    assert metrics['last_elapsed_time']>=1000000 and metrics['last_elapsed_time']>metrics['last_worker_time'],metrics
    for h in hs: check_snapshot(h,True)
    # The other compile context and first statement must retain their own timings.
    execute(d.c,'SET ARITHABORT OFF; EXEC dbo.P @p=49000; SET ARITHABORT ON;')
    hs=hashes(d.c,'dbo.P')
    assert len({h['plan_handle'] for h in hs})==2,hs
    for h in hs: check_snapshot(h,True)
    # Force the estimated-only lookup branch without changing a server-wide trace
    # flag (2451 can keep actual capture enabled despite the database option).
    original=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    estimated=re.sub(r'DECLARE @HasPlanStatsDmv BIT =.*?;',
                     'DECLARE @HasPlanStatsDmv BIT = 0;',original,count=1,flags=re.S)
    assert estimated!=original
    try:
        install(d.admin,estimated)
        for h in hashes(d.c,'dbo.P'): check_snapshot(h,False)
    finally:
        install(d.admin,original)

    # Exercise the production collector with known DMV values and misleading XML.
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    block=source[source.index('CREATE TABLE #PlanRuntime'):source.index('/* Per-wait-type runtime waits.')]
    fixture="""CREATE TABLE #PlanMatches(last_elapsed_time bigint,last_worker_time bigint);
    INSERT #PlanMatches VALUES(2500250,9000);
    DECLARE @PlanXmlForEmit xml=N'<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><QueryTimeStats CpuTime="2500" ElapsedTime="9" UdfCpuTime="3" UdfElapsedTime="1000"/></ShowPlanXML>';"""
    result=execute(d.c,fixture+block+'SELECT Setting,ValueNumeric FROM #PlanRuntime;')[-1]
    values={r['Setting']:r['ValueNumeric'] for r in result}
    assert values['CpuTimeMs']==9 and values['ElapsedTimeMs']==Decimal('2500.25'),values
    assert values['UdfCpuTimeMs']==3 and values['UdfElapsedTimeMs']==1000,values
print('PASS: #4165/#4192 timing follows exact statement DMV counters for blocked, multi-statement, multiple-copy, and estimated-plan cases')
