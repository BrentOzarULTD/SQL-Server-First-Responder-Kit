from plancompare_test_helpers import *
with Database(4173) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    schema=source[source.index('CREATE TABLE #Diff ('):source.index('DECLARE @RemoteXml XML;')]
    analysis=source[source.index("IF OBJECT_ID('tempdb..#OpsLocal')"):source.index('/* Final result set. CallStack')]
    def plan(rows):
        return '<ShowPlanXML xmlns="'+NS['p']+'"><RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimateRows="'+str(rows)+'" /></ShowPlanXML>'
    for local,remote,expected in [(0.1,0.9,True),(0.9,1.1,False),(1,1,False),(0,1,True),(1234567,1,True),(0.123456789,1,True)]:
        result=execute(d.c,"EXEC sys.sp_executesql %s,N'@PlanXmlForEmit xml,@RemotePlanXml xml',%s,%s",('DECLARE @Debug bit=0,@ErrMessage nvarchar(4000);'+schema+analysis+"SELECT * FROM #Diff WHERE Category='OperatorVariance';",plan(local),plan(remote)))[0]
        assert bool(result)==expected,(local,remote,result)
        if result: assert abs(float(result[0]['LocalValue'])-local)<1e-10,result
print('PASS: #4173 fractional estimate thresholds')
