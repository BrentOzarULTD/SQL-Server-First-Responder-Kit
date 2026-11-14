from plancompare_test_helpers import *
with Database(4166) as d:
    execute(d.c,'CREATE TABLE dbo.FirstTable(id int); CREATE TABLE dbo.SecondTable(id int,n int); INSERT dbo.FirstTable VALUES(1); INSERT dbo.SecondTable SELECT TOP(10000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;')
    execute(d.c,'CREATE PROC dbo.P AS SELECT COUNT_BIG(*) FROM dbo.FirstTable; SELECT SUM(n) FROM dbo.SecondTable;')
    for actual in [False,True]:
        execute(d.c,'ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS='+('ON' if actual else 'OFF')+';')
        execute(d.c,"EXEC sys.sp_recompile N'dbo.P'; EXEC dbo.P;");hs=hashes(d.c,'dbo.P');h=[h for h in hs if 'SUM(n)' in h['statement_text']][0]
        snap=snapshot(d.c,h['query_plan_hash'],d.name)
        stmts=snap.findall('.//p:StmtSimple',NS)
        assert len(stmts)==1,[s.attrib for s in stmts]
        assert 'SecondTable' in stmts[0].get('StatementText'),stmts[0].attrib
        objects=rows(snap,'Object','Present')
        assert objects and all('FirstTable' not in r['SubjectKey'] for r in objects),objects
        plan=snap.find('./Plan')
        ids=[n.get('NodeId') for n in plan.findall('.//p:RelOp',NS)]
        assert len(ids)==len(set(ids)),ids
        if actual:
            assert rows(snap,'PlanAttribute','PlanSource')[0]['ValueText']=='Actual (LAST_QUERY_PLAN_STATS)'
            assert snap.find('.//p:QueryTimeStats',NS) is not None
print('PASS: #4166 selected statement scope for estimated and actual batch plans')
