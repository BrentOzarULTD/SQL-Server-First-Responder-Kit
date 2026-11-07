"""SQL Server regression helpers. Use only a disposable test instance.

Install pymssql, set FRK_TEST_PASSWORD (and optionally FRK_TEST_SERVER, FRK_TEST_USER),
then run a plancompare regression from the repository root. Default server is
127.0.0.1:11433. Each test owns and removes its FRKPlanCompare_<issue> database.
"""
import os
import re
from pathlib import Path
from contextlib import contextmanager
from xml.etree import ElementTree as ET
import pymssql

NS = {'p': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
ROOT = Path(__file__).resolve().parents[2]

def connect(database='master'):
    server = os.environ.get('FRK_TEST_SERVER', '127.0.0.1:11433').removeprefix('tcp:').replace(',', ':')
    c = pymssql.connect(server=server,
                        user=os.environ.get('FRK_TEST_USER', 'sa'),
                        password=os.environ['FRK_TEST_PASSWORD'], database=database,
                        autocommit=True, timeout=60, login_timeout=15)
    execute(c, 'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; SET NOCOUNT ON;')
    return c

def execute(c, sql, args=()):
    cur = c.cursor()
    cur.execute(sql, args) if args else cur.execute(sql)
    sets = []
    while True:
        if cur.description:
            names = [col[0] or 'column_'+str(i) for i,col in enumerate(cur.description)]
            sets.append([dict(zip(names,row)) for row in cur.fetchall()])
        if not cur.nextset():
            break
    cur.close()
    return sets

def install(c, source=None):
    source = source or (ROOT / 'OptionalScripts/sp_BlitzPlanCompare.sql').read_text()
    for batch in re.split(r'^GO\s*$', source, flags=re.M | re.I):
        if batch.strip():
            execute(c, batch)

def hashes(c, proc):
    return execute(c, '''SELECT qs.plan_handle, qs.query_hash, qs.query_plan_hash,
        qs.statement_start_offset, qs.statement_end_offset,
        SUBSTRING(st.text,qs.statement_start_offset/2+1,
          (CASE WHEN qs.statement_end_offset=-1 THEN DATALENGTH(st.text)
                ELSE qs.statement_end_offset END-qs.statement_start_offset)/2+1) statement_text
        FROM sys.dm_exec_query_stats qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE st.dbid=DB_ID() AND st.objectid=OBJECT_ID(%s)
        ORDER BY qs.statement_start_offset''', (proc,))[0]

def snapshot(c, qph, db, extra='', args=()):
    sets = execute(c, 'EXEC master.dbo.sp_BlitzPlanCompare @QueryPlanHash=%s, @DatabaseName=%s'+extra,
                   (qph, db)+args)
    command = sets[0][0]['CallStack']
    prefix = "EXEC dbo.sp_BlitzPlanCompare @CompareToXML = N'"
    assert command.startswith(prefix) and command.endswith("';"), command[:300]
    return ET.fromstring(command[len(prefix):-2].replace("''", "'"))

def compare(c, snap, qph=None, db=None, extra='', args=()):
    return execute(c, 'EXEC master.dbo.sp_BlitzPlanCompare @CompareToXML=%s, @QueryPlanHash=%s, @DatabaseName=%s'+extra,
                   (ET.tostring(snap, encoding='unicode'), qph, db)+args)

def rows(snap, category, setting=None):
    return [r.attrib for r in snap.findall('./Snapshot/Row') if r.get('Category')==category and
            (setting is None or r.get('Setting')==setting)]

class Database:
    def __init__(self, issue, collation=None):
        self.name = 'FRKPlanCompare_'+str(issue)
        self.collation = collation
    def __enter__(self):
        self.admin = connect()
        self.c = None
        self.created = False
        try:
            install(self.admin)
            execute(self.admin, 'CREATE DATABASE ['+self.name+']'+
                    (' COLLATE '+self.collation if self.collation else ''))
            self.created = True
            self.c = connect(self.name)
            return self
        except BaseException:
            self.__exit__()
            raise
    def __exit__(self, *exc):
        try:
            if self.c is not None:
                self.c.close()
            if self.created:
                execute(self.admin, 'ALTER DATABASE ['+self.name+'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ['+self.name+'];')
        finally:
            self.admin.close()


@contextmanager
def linked_server(admin, temporary_name):
    """Reuse FRK_TEST_LINKED_SERVER or own a temporary link for two-instance tests."""
    existing = os.environ.get('FRK_TEST_LINKED_SERVER')
    if existing:
        found = execute(admin, 'SELECT is_rpc_out_enabled FROM sys.servers WHERE name=%s', (existing,))[0]
        assert len(found) == 1 and found[0]['is_rpc_out_enabled'], 'Existing test link must have RPC OUT enabled'
        yield existing
        return
    execute(admin, "EXEC sys.sp_addlinkedserver @server=%s,@srvproduct=N'',@provider=N'MSOLEDBSQL',@datasrc=%s,@provstr=N'TrustServerCertificate=yes';",
            (temporary_name, os.environ.get('FRK_TEST_REMOTE_DATASOURCE', 'frk-plancompare-remote')))
    try:
        execute(admin, "EXEC sys.sp_addlinkedsrvlogin @rmtsrvname=%s,@useself=N'False',@rmtuser=%s,@rmtpassword=%s",
                (temporary_name, os.environ.get('FRK_TEST_USER', 'sa'), os.environ['FRK_TEST_PASSWORD']))
        execute(admin, "EXEC sys.sp_serveroption @server=%s,@optname=N'rpc out',@optvalue=N'true';", (temporary_name,))
        execute(admin, "EXEC sys.sp_serveroption @server=%s,@optname=N'remote proc transaction promotion',@optvalue=N'false';", (temporary_name,))
        yield temporary_name
    finally:
        execute(admin, "EXEC sys.sp_dropserver @server=%s,@droplogins=N'droplogins';", (temporary_name,))
