"""Force real Query Store plans and apply a real plan guide, then check selected-query scope."""
from plancompare_test_helpers import *
with Database(4172) as d:
    execute(d.c,"ALTER DATABASE ["+d.name+"] SET QUERY_STORE=ON (OPERATION_MODE=READ_WRITE,QUERY_CAPTURE_MODE=ALL)")
    execute(d.c,"CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T VALUES(1,1),(2,2);")
    execute(d.c,"CREATE PROC dbo.P AS SELECT SUM(n) n FROM dbo.T;")
    execute(d.c,"CREATE PROC dbo.Q AS SELECT MAX(n) n FROM dbo.T;")
    execute(d.c,"EXEC dbo.P; EXEC dbo.Q; EXEC sys.sp_query_store_flush_db;")
    h=hashes(d.c,'dbo.P')[0]
    for proc in ('dbo.P','dbo.Q'):
        qp=execute(d.c,"SELECT TOP(1) q.query_id,p.plan_id FROM sys.query_store_query q JOIN sys.query_store_plan p ON p.query_id=q.query_id WHERE q.object_id=OBJECT_ID(%s)",(proc,))[0][0]
        execute(d.c,'EXEC sys.sp_query_store_force_plan @query_id=%s,@plan_id=%s',(qp['query_id'],qp['plan_id']))
    execute(d.c,'EXEC dbo.P; EXEC dbo.Q;')
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    forced=rows(snap,'ForcedPlan','QueryStore')
    assert len(forced)==1 and 'plan_id=' not in forced[0]['ValueText'],forced
    assert h['query_hash'].hex().upper() in forced[0]['SubjectKey'],forced
    execute(d.c,"EXEC sys.sp_create_plan_guide @name=N'Relevant',@stmt=N'SELECT SUM(n) n FROM dbo.T;',@type=N'OBJECT',@module_or_batch=N'dbo.P',@params=NULL,@hints=N'OPTION(MAXDOP 1)'")
    execute(d.c,"EXEC sys.sp_create_plan_guide @name=N'Unrelated',@stmt=N'SELECT MAX(n) n FROM dbo.T;',@type=N'OBJECT',@module_or_batch=N'dbo.Q',@params=NULL,@hints=N'OPTION(MAXDOP 1)'")
    # Unforce P so its guide, rather than a forced Query Store plan, controls compilation.
    qp=execute(d.c,"SELECT q.query_id,p.plan_id FROM sys.query_store_query q JOIN sys.query_store_plan p ON p.query_id=q.query_id WHERE q.object_id=OBJECT_ID('dbo.P') AND p.is_forced_plan=1")[0][0]
    execute(d.c,'EXEC sys.sp_query_store_unforce_plan @query_id=%s,@plan_id=%s',(qp['query_id'],qp['plan_id']))
    execute(d.c,"EXEC sys.sp_recompile N'dbo.P'; EXEC dbo.P; EXEC dbo.Q;")
    h=hashes(d.c,'dbo.P')[0]
    guides=rows(snapshot(d.c,h['query_plan_hash'],d.name),'ForcedPlan','PlanGuide')
    assert len(guides)==1 and guides[0]['SubjectKey'].endswith('.Relevant'),guides
    execute(d.c,"EXEC sys.sp_control_plan_guide N'DISABLE',N'Relevant'; EXEC dbo.P;")
    h=hashes(d.c,'dbo.P')[0]
    assert not rows(snapshot(d.c,h['query_plan_hash'],d.name),'ForcedPlan','PlanGuide')
print('PASS: unrelated forced plans and guides excluded; selected forced plan and applied guide captured; disabled guide omitted.')
