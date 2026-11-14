"""Check real optimizer stats usage and the production comparison priorities."""
from plancompare_test_helpers import *
with Database(4160) as d:
    execute(d.c,"CREATE TABLE dbo.T(id int PRIMARY KEY,n int,x int); INSERT dbo.T SELECT TOP(1000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1,2 FROM sys.all_objects; CREATE INDEX IX_n ON dbo.T(n); CREATE INDEX IX_unused ON dbo.T(x); CREATE STATISTICS UnusedStats ON dbo.T(x);")
    execute(d.c,"CREATE PROC dbo.P @p int AS SELECT id FROM dbo.T WITH(INDEX(IX_n)) WHERE n=@p;")
    execute(d.c,"EXEC dbo.P 1")
    h=hashes(d.c,'dbo.P')[0]
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    used=[r for r in rows(snap,'Statistics','UsedByPlan') if r['ValueText']=='1']
    assert used and not any('UnusedStats' in r['SubjectKey'] for r in used),used
    indexes=rows(snap,'Index','UsedByPlan')
    assert any(r['SubjectKey'].endswith('.IX_n') for r in indexes),indexes
    # A different query prompts an auto statistic, whose key must use its column.
    execute(d.c,"CREATE TABLE dbo.AutoT(id int,n int); INSERT dbo.AutoT VALUES(1,1),(2,2);")
    execute(d.c,"CREATE PROC dbo.AutoP AS SELECT id FROM dbo.AutoT WHERE n=1;")
    execute(d.c,"EXEC dbo.AutoP;")
    ah=hashes(d.c,'dbo.AutoP')[0]
    auto=rows(snapshot(d.c,ah['query_plan_hash'],d.name),'Statistics','UsedByPlan')
    assert any(r['SubjectKey'].endswith('.auto:n') and r['ValueText']=='1' for r in auto),auto
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    schema=source[source.index('CREATE TABLE #Diff ('):source.index('DECLARE @RemoteXml XML;')]
    block=source[source.index("IF OBJECT_ID('tempdb..#Rubric')"):source.index('/* Echo header info')]
    table='(Category varchar(50),Setting varchar(200),SubjectKey nvarchar(776),ValueText nvarchar(max),ValueNumeric decimal(38,4));'
    fixture="""INSERT #LocalSnapshot VALUES
      ('Statistics','UsedByPlan','used','1',1),('Statistics','LastUpdated','used','today',NULL),
      ('Statistics','Rows','used','1000',1000),('Statistics','Rows','unused','1000',1000),
      ('Index','UsedByPlan','usedindex','1',1),('Index','Present','usedindex','yes',NULL),
      ('Index','Present','unusedindex','yes',NULL);
      INSERT #RemoteSnapshot VALUES
      ('Statistics','UsedByPlan','used','0',0),('Statistics','LastUpdated','used','yesterday',NULL),
      ('Statistics','Rows','used','900',900),('Statistics','Rows','unused','1',1);"""
    script=schema+'CREATE TABLE #LocalSnapshot'+table+'CREATE TABLE #RemoteSnapshot'+table+fixture+block+'SELECT * FROM #Diff;'
    diff=execute(d.c,'EXEC sys.sp_executesql %s',(script,))[0]
    assert {(r['Category'],r['Setting'],r['Object'],r['Priority']) for r in diff}=={
      ('Statistics','LastUpdated','used',100),('Index','Present','usedindex',1),('Index','Present','unusedindex',25)},diff
    diff=execute(d.c,'EXEC sys.sp_executesql %s',(script.replace("'900',900","'100',100"),))[0]
    assert any(r['Category']=='Statistics' and r['Setting']=='Rows' and r['Priority']==15 for r in diff),diff
print('PASS: actual optimizer stats usage, stable auto-stat keys, noise filtering, and used/unused index priorities.')
