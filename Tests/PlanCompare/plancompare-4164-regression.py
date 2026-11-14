"""Use real cache offsets for auto-parameterized and multi-statement ad-hoc reproducers."""
from plancompare_test_helpers import *

source = (ROOT / 'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
lookup = source[source.index('    /* Cache offsets exclude'):source.index('\n    IF @ProcObjectId IS NOT NULL',source.index('    /* Cache offsets exclude'))]
generator = source[source.index('    /* Assign expressions to typed variables first.'):source.index('    /* Build the clickable XML.')]
with Database(4164) as d:
    execute(d.c,"CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T VALUES(1,11),(99,99); CREATE TABLE dbo.Audit(n int);")
    execute(d.c,"ALTER DATABASE ["+d.name+"] SET PARAMETERIZATION FORCED")
    execute(d.c,"SELECT n FROM dbo.T WHERE id=1")
    auto = execute(d.c,"""SELECT qs.sql_handle,qs.statement_start_offset,qs.statement_end_offset,st.text
      FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)st
      WHERE EXISTS (SELECT 1 FROM sys.dm_exec_plan_attributes(qs.plan_handle) a WHERE a.attribute='dbid' AND CONVERT(int,a.value)=DB_ID()) AND st.text LIKE '(@%'""")[0]
    assert len(auto)==1,auto
    execute(d.c,"EXEC sp_executesql N'INSERT dbo.Audit VALUES(123); SELECT n FROM dbo.T WHERE id=@p;',N'@p int',@p=1")
    multi = execute(d.c,"""SELECT qs.sql_handle,qs.statement_start_offset,qs.statement_end_offset,st.text
      FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)st
      WHERE EXISTS (SELECT 1 FROM sys.dm_exec_plan_attributes(qs.plan_handle) a WHERE a.attribute='dbid' AND CONVERT(int,a.value)=DB_ID()) AND st.text LIKE '%INSERT dbo.Audit VALUES(123); SELECT n FROM dbo.T%'
      AND qs.statement_start_offset>50""")[0]
    assert len(multi)==1,multi
    execute(d.c,"CREATE TABLE #PlanParameters(ParameterName sysname,DataType nvarchar(128),CompiledValue nvarchar(4000)); SELECT * INTO #RemotePlanParameters FROM #PlanParameters;")
    for entry,param in [(auto[0],'@0'),(multi[0],'@p')]:
        execute(d.c,"TRUNCATE TABLE #PlanParameters; TRUNCATE TABLE #RemotePlanParameters; INSERT #PlanParameters VALUES(%s,'int','(1)'); INSERT #RemotePlanParameters VALUES(%s,'int','(99)')",(param,param))
        script = """DECLARE @ProcObjectId int,@ProcDbId int,@SqlText nvarchar(max),
          @LocalExec nvarchar(max),@RemoteExec nvarchar(max),@ParamDecls nvarchar(max),
          @UseDb nvarchar(400)=N'',@ProcName nvarchar(600)=NULL;""" + lookup + generator + """
          SELECT @SqlText StatementText,@LocalExec LocalCall,@RemoteExec RemoteCall;"""
        result = execute(d.c,"EXEC sys.sp_executesql %s,N'@SqlHandle varbinary(64),@StmtStartOffset int,@StmtEndOffset int',%s,%s,%s",
                         (script,entry['sql_handle'],entry['statement_start_offset'],entry['statement_end_offset']))[0][0]
        assert not result['StatementText'].startswith('(@') and 'INSERT' not in result['StatementText'],result
        assert execute(d.c,result['LocalCall'])[0][0]['n']==11
        assert execute(d.c,result['RemoteCall'])[0][0]['n']==99
    assert execute(d.c,'SELECT COUNT(*) n FROM dbo.Audit')[0][0]['n']==1
print('PASS: both cache formats produce executable selected-statement reproducers without repeating the INSERT.')
