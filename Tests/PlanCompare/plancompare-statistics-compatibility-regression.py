"""Old Statistics.Header snapshots must not produce false missing-statistics differences."""
from plancompare_test_helpers import *
from copy import deepcopy

with Database('StatisticsCompatibility') as d:
    execute(d.c,"CREATE TABLE dbo.T(id int PRIMARY KEY,n int); INSERT dbo.T SELECT TOP(1000) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),1 FROM sys.all_objects; CREATE INDEX IX_n ON dbo.T(n);")
    execute(d.c,"CREATE PROC dbo.P @p int AS SELECT id FROM dbo.T WHERE n=@p;")
    execute(d.c,"EXEC dbo.P 1")
    h=hashes(d.c,'dbo.P')[0]
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    assert snap.get('StatisticsFormatVersion')=='2'
    assert any(r['ValueText']=='1' for r in rows(snap,'Statistics','UsedByPlan'))
    for old_header in (True,False):
        old=deepcopy(snap);old.attrib.pop('StatisticsFormatVersion')
        body=old.find('./Snapshot')
        for row in list(body):
            if row.get('Category')=='Statistics':body.remove(row)
        if old_header:
            ET.SubElement(body,'Row',Category='Statistics',Setting='Header',
                          SubjectKey=d.name+'.dbo.T.IX_n',
                          ValueText='lastUpdated=2026-01-01|rows=1000|sampled=1000|modCounter=0|autoCreated=N|hasFilter=N',
                          ValueNumeric='0')
        # Other categories still report real differences.
        for row in body:
            if row.get('Category')=='RowCount' and row.get('Setting')=='Rows':
                row.set('ValueText','100');row.set('ValueNumeric','100')
        diff=compare(d.c,old,h['query_plan_hash'],d.name)[0]
        assert not any(r['Category']=='Statistics' for r in diff),diff
        assert sum(r['Setting']=='StatisticsNotCompared' and r['Priority']==100 for r in diff)==1,diff
        assert any(r['Category']=='RowCount' and r['Setting']=='Rows' for r in diff),diff
    # Snapshots already using the new rows remain compatible without the marker.
    transition=deepcopy(snap);transition.attrib.pop('StatisticsFormatVersion')
    diff=compare(d.c,transition,h['query_plan_hash'],d.name)[0]
    assert not any(r['Setting']=='StatisticsNotCompared' or r['Category']=='Statistics' for r in diff),diff
    # Explicit format 2 with no statistics means no stats, not an unknown version.
    empty=deepcopy(snap)
    body=empty.find('./Snapshot')
    for row in list(body):
        if row.get('Category')=='Statistics':body.remove(row)
    diff=compare(d.c,empty,h['query_plan_hash'],d.name)[0]
    assert not any(r['Setting']=='StatisticsNotCompared' for r in diff),diff
    assert any(r['Category']=='Statistics' for r in diff),diff
print('PASS: old and empty legacy statistics formats are disclosed and skipped; current formats retain comparisons.')
