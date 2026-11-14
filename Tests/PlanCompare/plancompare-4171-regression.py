from plancompare_test_helpers import *
with Database(4171,'Latin1_General_100_CS_AS') as d:
    execute(d.c,'CREATE TABLE dbo.[CaseTable](id int PRIMARY KEY); CREATE TABLE dbo.[casetable](id int PRIMARY KEY); CREATE TABLE dbo.[日本語](id int PRIMARY KEY); INSERT dbo.[CaseTable] VALUES(1); INSERT dbo.[casetable] VALUES(2),(4),(5); INSERT dbo.[日本語] VALUES(3);')
    execute(d.c,'CREATE PROC dbo.P AS SELECT a.id,b.id,c.id FROM dbo.[CaseTable] a CROSS JOIN dbo.[casetable] b CROSS JOIN dbo.[日本語] c;')
    execute(d.c,'EXEC dbo.P;');h=hashes(d.c,'dbo.P')[0]
    snap=snapshot(d.c,h['query_plan_hash'],d.name)
    assert not rows(snap,'Database','_unreadable_'),rows(snap,'Database','_unreadable_')
    found={r['SubjectKey'] for r in rows(snap,'RowCount','Rows')}
    assert found=={d.name+'.dbo.CaseTable',d.name+'.dbo.casetable',d.name+'.dbo.日本語'},found
    assert len(rows(snap,'Index','Present'))==3,rows(snap,'Index','Present')
    diffs=compare(d.c,snap,h['query_plan_hash'],d.name)[0]
    assert not [r for r in diffs if r['Category'] in ('Setup','Object','Index','Statistics','RowCount')],diffs
    # Force a real mismatch to exercise the Details concatenation, not only empty diffs.
    for r in snap.findall('./Snapshot/Row'):
        if r.get('Category')=='RowCount' and r.get('Setting')=='Rows' and r.get('SubjectKey').endswith('.CaseTable'):
            r.set('ValueText','999');r.set('ValueNumeric','999')
    diffs=compare(d.c,snap,h['query_plan_hash'],d.name)[0]
    changed=[r for r in diffs if r['Category']=='RowCount' and r['Setting']=='Rows']
    assert len(changed)==1 and changed[0]['Details']=='Mismatch: RowCount.Rows ('+d.name+'.dbo.CaseTable)',changed
print('PASS: #4171 cross-collation catalog reads, case-distinct and Unicode tables')
