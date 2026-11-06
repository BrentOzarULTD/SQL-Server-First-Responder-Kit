from plancompare_test_helpers import *
with Database(4159) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    schema=source[source.index('CREATE TABLE #Diff ('):source.index('DECLARE @RemoteXml XML;')]
    analysis=source[source.index("IF OBJECT_ID('tempdb..#OpsLocal')"):source.index('/* Final result set. CallStack')]
    for count in [5,30]:
        objects=''.join('<Object Database="[db]" Schema="[dbo]" Table="[table]" Index="['+str(i)+'_'+('x'*120)+']"/>' for i in range(count))
        def plan(rows): return '<ShowPlanXML xmlns="'+NS['p']+'"><RelOp NodeId="0" PhysicalOp="Index Update" LogicalOp="Update" EstimateRows="'+str(rows)+'"><Update>'+objects+'</Update></RelOp></ShowPlanXML>'
        r=execute(d.c,"EXEC sys.sp_executesql %s,N'@PlanXmlForEmit xml,@RemotePlanXml xml',%s,%s",('DECLARE @Debug bit=0,@ErrMessage nvarchar(4000);'+schema+analysis+"SELECT * FROM #Diff WHERE Category='OperatorVariance';",plan(10),plan(100)))[0]
        assert len(r)==1,r
        assert str(count-1)+'_'+('x'*120) in r[0]['Object'],r
        if count==30: assert len(r[0]['Object'])>4000 and len(r[0]['Details'])>4000
print('PASS: #4159 full operator signatures beyond 776 and 4000 characters')
