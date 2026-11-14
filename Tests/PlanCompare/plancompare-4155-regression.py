from plancompare_test_helpers import *
with Database(4155) as d:
    execute(d.c,'CREATE TABLE dbo.T(id int); INSERT dbo.T VALUES(1),(2);')
    execute(d.c,'CREATE PROC dbo.P AS SELECT SUM(id) AS total FROM dbo.T;');execute(d.c,'EXEC dbo.P;')
    h=hashes(d.c,'dbo.P')[0];snap=snapshot(d.c,h['query_plan_hash'],d.name)
    for extra,args in [('',()),(',@QueryHash=%s',(h['query_hash'],)),(',@StoredProcName=%s',('dbo.P',))]:
        assert compare(d.c,snap,None,d.name,extra,args)[0]
    assert compare(d.c,snap,h['query_plan_hash'],d.name)[0]
    assert compare(d.c,snap)[0]
    try:
        compare(d.c,snap,None,d.name,',@QueryHash=0x0000000000000001')
        raise AssertionError('Mismatched query hash accepted')
    except pymssql.DatabaseError as e:
        assert 'does not match' in str(e),str(e)
print('PASS: #4155 XML-only and explicit lookup selectors')
