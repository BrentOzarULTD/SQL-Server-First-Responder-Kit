from plancompare_test_helpers import *
with Database(4169) as d:
    execute(d.c,'CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T SELECT TOP(100) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects;')
    execute(d.c,'CREATE PROC dbo.P @p int AS SELECT SUM(n) FROM dbo.T WHERE id>=@p OPTION(MAXDOP 1);')
    execute(d.c,'SET ARITHABORT ON; EXEC dbo.P @p=1; SET ARITHABORT OFF; EXEC dbo.P @p=99; SET ARITHABORT ON;')
    hs=hashes(d.c,'dbo.P');assert len(hs)==2,hs
    assert hs[0]['query_plan_hash']==hs[1]['query_plan_hash']
    values=set()
    for h in hs:
        snap=snapshot(d.c,h['query_plan_hash'],d.name,',@PlanHandle=%s,@StatementStartOffset=%s',(h['plan_handle'],h['statement_start_offset']))
        values.add(rows(snap,'Parameter','CompiledValue')[0]['ValueText'])
        assert compare(d.c,snap,h['query_plan_hash'],d.name,',@PlanHandle=%s,@StatementStartOffset=%s',(h['plan_handle'],h['statement_start_offset']))[0]
        r=execute(d.c,'EXEC master.dbo.sp_BlitzPlanCompare @PlanHandle=%s,@StatementStartOffset=%s',(h['plan_handle'],h['statement_start_offset']))
        assert 'CallStack' in r[0][0]
    assert len(values)==2,values
    try:
        snapshot(d.c,hs[0]['query_plan_hash'],d.name)
        raise AssertionError('Ambiguous copies were silently selected')
    except pymssql.DatabaseError as e: assert '@PlanHandle' in str(e),str(e)
print('PASS: #4169 explicit cache selectors preserve distinct sniffed values')
