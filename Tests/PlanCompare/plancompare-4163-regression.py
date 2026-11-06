from plancompare_test_helpers import *
with Database(4163) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    schema=source[source.index('CREATE TABLE #Diff ('):source.index('DECLARE @RemoteXml XML;')]
    block=source[source.index("IF OBJECT_ID('tempdb..#Rubric')"):source.index('/* Echo header info')]
    table='(Category varchar(50),Setting varchar(200),SubjectKey nvarchar(776),ValueText nvarchar(max),ValueNumeric decimal(38,4));'
    for category,setting,local,remote,expected in [('PlanAttribute','DegreeOfParallelism',8,6,True),('PlanAttribute','DegreeOfParallelism',4,3,True),('Hardware','CommittedTargetKB',1000,950,False),('Hardware','CommittedTargetKB',1000,500,True),('Hardware','CPUCount',64,48,True),('Hardware','PhysicalMemoryKB',128,96,True)]:
        script=schema+'CREATE TABLE #LocalSnapshot'+table+'CREATE TABLE #RemoteSnapshot'+table
        script+="INSERT #LocalSnapshot VALUES('"+category+"','"+setting+"',NULL,'"+str(local)+"',"+str(local)+");"
        script+="INSERT #RemoteSnapshot VALUES('"+category+"','"+setting+"',NULL,'"+str(remote)+"',"+str(remote)+");"
        script+=block+'SELECT * FROM #Diff;'
        result=execute(d.c,'EXEC sys.sp_executesql %s',(script,))[0]
        assert bool(result)==expected,(setting,result)
print('PASS: #4163 DOP exactness and targeted memory tolerance')
