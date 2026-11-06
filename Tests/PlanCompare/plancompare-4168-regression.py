"""Execute both generated calls with numeric, quoted, Unicode, date, binary and NULL values."""
from plancompare_test_helpers import *
import re

source = (ROOT / 'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
block = source[source.index('    /* Assign expressions to typed variables first.'):source.index('    /* Build the clickable XML.')]
with Database(4168) as d:
    execute(d.c, """CREATE PROC dbo.P @i int, @d decimal(9,2), @s nvarchar(4000),
      @t datetime2, @b varbinary(20), @n int AS
      SELECT @i i,@d d,@s s,@t t,@b b,@n n;""")
    setup = """CREATE TABLE #PlanParameters(ParameterName sysname, DataType nvarchar(128),CompiledValue nvarchar(4000));
    CREATE TABLE #RemotePlanParameters(ParameterName sysname, DataType nvarchar(128),CompiledValue nvarchar(4000));"""
    execute(d.c,setup)
    values = [('@i','int','(1)'),('@d','decimal(9,2)','(12.34)'),
              ('@s','nvarchar(4000)',"N'O''Brien (日本語)'"),('@t','datetime2',"'2026-04-07T12:34:56'"),
              ('@b','varbinary(20)','(0x00FF)'),('@n','int','NULL')]
    for row in values:
        execute(d.c,'INSERT #PlanParameters VALUES (%s,%s,%s); INSERT #RemotePlanParameters VALUES (%s,%s,%s)',row+row)
    execute(d.c,"UPDATE #RemotePlanParameters SET CompiledValue='(99)' WHERE ParameterName='@i'")
    wrapper = """DECLARE @LocalExec nvarchar(max),@RemoteExec nvarchar(max),@ParamDecls nvarchar(max),
      @ProcDbId int=DB_ID(),@ProcObjectId int=OBJECT_ID(@proc),
      @UseDb nvarchar(400)=N'',@ProcName nvarchar(600)=@proc,@SqlText nvarchar(max)=@text;""" + block + """
      SELECT @LocalExec LocalCall,@RemoteExec RemoteCall;"""
    for proc in ('dbo.P',None):
        calls = execute(d.c, "EXEC sys.sp_executesql %s,N'@proc nvarchar(600),@text nvarchar(max)',%s,%s",
                        (wrapper,proc,'SELECT @i i,@d d,@s s,@t t,@b b,@n n;'))[0][0]
        for key, expected in [('LocalCall',1),('RemoteCall',99)]:
            result = execute(d.c,calls[key])[0][0]
            assert result['i']==expected and float(result['d'])==12.34 and result['s']=="O'Brien (日本語)"
            assert result['b']==b'\x00\xff' and result['n'] is None and result['t'].year==2026,result
        assert (proc is not None)==('WITH RECOMPILE' in calls['LocalCall'])
    # Either side missing a procedure parameter must suppress both calls.
    execute(d.c,"DELETE #RemotePlanParameters WHERE ParameterName='@n'")
    calls=execute(d.c,"EXEC sys.sp_executesql %s,N'@proc nvarchar(600),@text nvarchar(max)',%s,%s",
                  (wrapper,'dbo.P',None))[0][0]
    assert calls=={'LocalCall':None,'RemoteCall':None},calls
    execute(d.c,"INSERT #RemotePlanParameters SELECT * FROM #PlanParameters WHERE ParameterName='@n'")
    long_value="N'"+('x'*3975)+"'"
    execute(d.c,"UPDATE #PlanParameters SET CompiledValue=%s WHERE ParameterName='@s'",(long_value,))
    calls=execute(d.c,"EXEC sys.sp_executesql %s,N'@proc nvarchar(600),@text nvarchar(max)',%s,%s",
                  (wrapper,'dbo.P',None))[0][0]
    assert len(execute(d.c,calls['LocalCall'])[0][0]['s'])==3975
    execute(d.c,"UPDATE #PlanParameters SET DataType=NULL WHERE ParameterName='@i'")
    calls = execute(d.c,"EXEC sys.sp_executesql %s,N'@proc nvarchar(600),@text nvarchar(max)',%s,%s",
                    (wrapper,'dbo.P',None))[0][0]
    assert calls=={'LocalCall':None,'RemoteCall':None},calls
    execute(d.c,"DROP TABLE #PlanParameters; DROP TABLE #RemotePlanParameters;")
    # A real two-statement procedure requires @b, which is absent from the selected plan.
    execute(d.c,"ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS=ON; CREATE TABLE dbo.T(id int,n int); INSERT dbo.T SELECT TOP(50000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;")
    execute(d.c,"CREATE PROC dbo.Multi @a int,@b int AS SELECT SUM(n) FROM dbo.T WHERE id>=@a; SELECT MAX(n) FROM dbo.T WHERE id<=@b;")
    execute(d.c,"EXEC dbo.Multi @a=1,@b=2;")
    h=hashes(d.c,'dbo.Multi')[0]
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    parameters=snap.findall('.//p:ParameterList/p:ColumnReference',NS)
    assert {p.get('Column') for p in parameters}=={'@a'},[p.attrib for p in parameters]
    for p in parameters: p.set('ParameterCompiledValue','(99)')
    for r in snap.findall('./Snapshot/Row'):
        if r.get('Category')=='Parameter' and r.get('Setting')=='CompiledValue':
            r.set('ValueText','int = (99)')
    diff=compare(d.c,snap,h['query_plan_hash'],d.name)[0]
    assert any(r['Category']=='Parameter' and r['Setting']=='CompiledValue' for r in diff),diff
    assert not any(r['Category']=='Parameter' and r['Setting']=='Reproducer' for r in diff),diff
print('PASS: generated procedure and sp_executesql calls execute all value types; unknown type is skipped.')
