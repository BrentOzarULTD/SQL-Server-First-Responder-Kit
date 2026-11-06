from plancompare_test_helpers import *
with Database(4167) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    marker="CAST(SERVERPROPERTY('EngineEdition')  AS INT)"
    try:
        for edition in (5,6,11,8,3):
            # Only substitute the detected engine code in the test installation.
            install(d.admin,source.replace(marker,str(edition)))
            try:
                execute(d.c,"EXEC master.dbo.sp_BlitzPlanCompare @QueryPlanHash=0x0123456789abcdef,@LinkedServer=N'FRK_Missing_Link';")
                raise AssertionError('Expected validation error')
            except pymssql.DatabaseError as e:
                text=str(e)
                if edition in (5,6,11): assert 'not supported' in text,text
                else: assert 'not found in sys.servers' in text,text
        install(d.admin,source.replace(marker,'8'))
        execute(d.c,'CREATE TABLE dbo.T(id int); INSERT dbo.T VALUES(1);')
        execute(d.c,'CREATE PROC dbo.P AS SELECT SUM(id) FROM dbo.T;');execute(d.c,'EXEC dbo.P;')
        result=execute(d.c,'EXEC master.dbo.sp_BlitzPlanCompare @StoredProcName=%s,@DatabaseName=%s',('dbo.P',d.name))
        assert result[0][0]['CallStack'].startswith('EXEC ')
        snap=snapshot(d.c,hashes(d.c,'dbo.P')[0]['query_plan_hash'],d.name)
        assert rows(snap,'Hardware','PhysicalMemoryKB')
        assert not rows(snap,'Database','_unreadable_')
    finally:
        install(d.admin,source)
print('PASS: #4167 engine dispatch (injected engine codes on SQL Server, not a live MI test)')
