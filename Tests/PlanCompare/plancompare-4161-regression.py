from plancompare_test_helpers import *
with Database(4161) as d:
    # Install in the user database so a bare name resolves there.
    install(d.c)
    execute(d.c,'CREATE TABLE dbo.T(id int); INSERT dbo.T VALUES(1),(2);')
    execute(d.c,'CREATE PROC dbo.P AS SELECT SUM(id) AS total FROM dbo.T;');execute(d.c,'EXEC dbo.P;')
    for name,extra in [('dbo.P',''),('P',''),('['+d.name+'].[dbo].[P]','')]:
        r=execute(d.c,'EXEC dbo.sp_BlitzPlanCompare @StoredProcName=%s',(name,))
        assert 'CallStack' in r[0][0],r
    execute(d.c,'CREATE PROC dbo.Multi AS SELECT SUM(id) FROM dbo.T; SELECT COUNT(*) FROM dbo.T;');execute(d.c,'EXEC dbo.Multi;')
    # Ambiguity remains explicit rather than dropping all but one statement.
    try:
        execute(d.c,"EXEC dbo.sp_BlitzPlanCompare @StoredProcName=N'dbo.Multi';")
        raise AssertionError('Expected statement ambiguity')
    except pymssql.DatabaseError as e:
        assert 'Multiple cached plans' in str(e),str(e)
print('PASS: #4161 bare/qualified procedure lookup and statement preservation')
