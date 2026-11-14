"""Exercise the production diff join with repeated keys, and grant collection across session IDs."""
from plancompare_test_helpers import *
with Database(4158) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    schema=source[source.index('CREATE TABLE #Diff ('):source.index('DECLARE @RemoteXml XML;')]
    block=source[source.index("IF OBJECT_ID('tempdb..#Rubric')"):source.index('/* Echo header info')]
    table='(Category varchar(50),Setting varchar(200),SubjectKey nvarchar(776),ValueText nvarchar(max),ValueNumeric decimal(38,4));'
    seed="""INSERT #LocalSnapshot VALUES ('PlanWarning','PlanAffectingConvert',NULL,'A',NULL),
      ('PlanWarning','PlanAffectingConvert',NULL,'B',NULL),('PlanWarning','PlanAffectingConvert',NULL,'B',NULL),
      ('ForcedPlan','QueryStore','Db','A',NULL),('ForcedPlan','QueryStore','Db','B',NULL);
      INSERT #RemoteSnapshot SELECT * FROM #LocalSnapshot;"""
    for edit,count in [('',0),("DELETE FROM #RemoteSnapshot WHERE Category='PlanWarning' AND ValueText='A';",1),("UPDATE #RemoteSnapshot SET ValueText='Z' WHERE Category='ForcedPlan' AND ValueText='B';",1),
                       ("DELETE TOP(1) FROM #RemoteSnapshot WHERE Category='PlanWarning' AND ValueText='B';",1)]:
        result=execute(d.c,'EXEC sys.sp_executesql %s',(schema+'CREATE TABLE #LocalSnapshot'+table+
            'CREATE TABLE #RemoteSnapshot'+table+seed+edit+block+'SELECT * FROM #Diff;',))[0]
        assert len(result)==count,result
    collector=source[source.index(';WITH grants AS'):source.index('/* =====================================================================',source.index(';WITH grants AS'))]
    collector=collector.replace('sys.dm_exec_query_memory_grants','#TestGrants')
    fixture="""CREATE TABLE #PlanMatches(plan_handle varbinary(64)); INSERT #PlanMatches VALUES(0x01);
      CREATE TABLE #TestGrants(session_id int,plan_handle varbinary(64),granted_memory_kb bigint,ideal_memory_kb bigint,used_memory_kb bigint);
      INSERT #TestGrants VALUES (10,0x01,100,200,50),(20,0x01,100,200,50),(30,0x02,999,999,999);"""
    script='CREATE TABLE #LocalSnapshot'+table+fixture+collector+'SELECT * FROM #LocalSnapshot ORDER BY Setting;'
    first=execute(d.c,'EXEC sys.sp_executesql %s',(script,))[0]
    second=execute(d.c,'EXEC sys.sp_executesql %s',(script.replace('(10,0x01','(110,0x01').replace('(20,0x01','(220,0x01'),))[0]
    assert first==second and all(r['SubjectKey'] is None for r in first)
    assert {r['Setting']:int(r['ValueNumeric']) for r in first}=={
      'ActiveMemoryGrantCount':2,'ActiveMemoryGrantKB':200,'ActiveIdealMemoryKB':400,'ActiveUsedMemoryKB':100},first
    idle=execute(d.c,'EXEC sys.sp_executesql %s',(script.replace('INSERT #TestGrants VALUES','INSERT #TestGrants VALUES').replace(collector,'DELETE #TestGrants;'+collector),))[0]
    assert all(r['ValueNumeric']==0 for r in idle)
    normalizer=source[source.index('/* Normalize legacy per-session grants.'):source.index("IF OBJECT_ID('tempdb..#Rubric')")]
    for legacy in (True,False):
        seed="INSERT #LocalSnapshot VALUES ('LiveState','ActiveMemoryGrantCount',NULL,'1',1),('LiveState','ActiveMemoryGrantKB',NULL,'100',100),('LiveState','ActiveIdealMemoryKB',NULL,'200',200),('LiveState','ActiveUsedMemoryKB',NULL,'50',50);"
        if legacy: seed+="INSERT #RemoteSnapshot VALUES('LiveState','ActiveMemoryGrantKB','500','100/200 (used 50)',100);"
        else: seed=seed.replace("'1',1","'0',0").replace("'100',100","'0',0")
        result=execute(d.c,'EXEC sys.sp_executesql %s',(schema+'CREATE TABLE #LocalSnapshot'+table+'CREATE TABLE #RemoteSnapshot'+table+seed+normalizer+block+'SELECT * FROM #Diff;',))[0]
        assert not result,result

print('PASS: repeated snapshots compare cleanly, differences are bounded, grants ignore server-local session IDs.')
