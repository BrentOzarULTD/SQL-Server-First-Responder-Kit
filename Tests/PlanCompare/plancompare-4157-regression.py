from plancompare_test_helpers import *
with Database(4157) as d:
    source=(ROOT/'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    start=source.index("SELECT 'SpillCount'");end=source.index('/* Per-wait-type',start)
    block=source[start:end]
    # The count is evaluated by the production XPath, not a copied test formula.
    xpath=block.split(".value('")[1].split("', 'INT')")[0]
    for warnings,expected in [('',0),('<Warnings><SpillOccurred Detail="0"/></Warnings>',1),('<Warnings><SpillToTempDb/><SortSpillDetails/></Warnings>',1),('<Warnings><SpillOccurred/><SpillToTempDb/><HashSpillDetails/></Warnings>',1),('<Warnings><ExchangeSpillDetails/></Warnings><Warnings><SpillOccurred/></Warnings>',2)]:
        xml='<ShowPlanXML xmlns="'+NS['p']+'">'+warnings+'</ShowPlanXML>'
        r=execute(d.c,"DECLARE @x xml=%s; WITH XMLNAMESPACES('"+NS['p']+"' AS p) SELECT @x.value('"+xpath+"','int') spills;",(xml,))[0]
        assert r[0]['spills']==expected,(warnings,r)
print('PASS: #4157 spilling operators counted once across warning representations')
