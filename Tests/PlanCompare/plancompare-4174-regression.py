from plancompare_test_helpers import *
with Database(4174) as d:
    execute(d.c,'CREATE TABLE dbo.T(id int); INSERT dbo.T VALUES(1);')
    execute(d.c,'CREATE PROC dbo.P AS SELECT SUM(id) FROM dbo.T;')
    execute(d.c,'EXEC dbo.P;')
    h=hashes(d.c,'dbo.P')[0];snap=snapshot(d.c,h['query_plan_hash'],d.name)
    assert not rows(snap,'LiveState','RunnableSchedulers')
    metric=rows(snap,'LiveState','RunnableTasks');assert len(metric)==1
    old=float(metric[0]['ValueText'])
    for r in snap.findall('./Snapshot/Row'):
        if r.get('Category')=='LiveState' and r.get('Setting')=='RunnableTasks':
            r.set('ValueText',str(int(old)+10000));r.set('ValueNumeric',str(int(old)+10000))
    diffs=compare(d.c,snap,h['query_plan_hash'],d.name)[0]
    hit=[r for r in diffs if r['Category']=='LiveState' and r['Setting']=='RunnableTasks']
    assert len(hit)==1 and hit[0]['Priority']==35,hit
    # Execute the production collector against deterministic scheduler rows.
    start=source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    collector=source[source.index('SELECT @RunnableTasks ='):source.index('SELECT @WorkerCount =')].replace('sys.dm_os_schedulers','#Schedulers')
    r=execute(d.c,"CREATE TABLE #Schedulers(status nvarchar(60),runnable_tasks_count int); INSERT #Schedulers VALUES('VISIBLE ONLINE',3),('VISIBLE ONLINE',7),('HIDDEN ONLINE',99); DECLARE @RunnableTasks int;"+collector+'SELECT @RunnableTasks tasks;')[0]
    assert r[0]['tasks']==10,r
print('PASS: #4174 runnable queue and priority')
