"""Integration regression for #4194. Run only against a disposable SQL Server.

Requires pymssql and FRK_TEST_PASSWORD. Optional FRK_TEST_SERVER (127.0.0.1:11433)
and FRK_TEST_USER (sa). Installs sp_Blitz and sp_ineachdb in master, creates a
restricted SQL login, a case-sensitive database, and inert Agent fixtures, then
removes its fixtures. --source PATH tests an alternate sp_Blitz.sql (e.g. baseline).
"""
import argparse
from contextlib import closing
import os
from pathlib import Path
import re
import uuid

import pymssql

ROOT = Path(__file__).resolve().parents[2]
TARGETS = (6, 13, 28, 29, 30, 31, 57, 59, 61, 79, 94, 96, 98, 123, 180, 181, 219)


def connect(user=None, password=None):
    return pymssql.connect(
        server=os.environ.get('FRK_TEST_SERVER', '127.0.0.1:11433')
        .removeprefix('tcp:').replace(',', ':'),
        user=user or os.environ.get('FRK_TEST_USER', 'sa'),
        password=password or os.environ['FRK_TEST_PASSWORD'], database='master',
        autocommit=True, login_timeout=15, timeout=120, charset='UTF-8')


def execute(c, sql, args=()):
    with closing(c.cursor(as_dict=True)) as cur:
        cur.execute(sql, args) if args else cur.execute(sql)
        rows = []
        while True:
            if cur.description:
                rows.extend(cur.fetchall())
            if not cur.nextset():
                break
        return rows


def install(c, path):
    for batch in re.split(r'^GO\s*$', path.read_text(), flags=re.M | re.I):
        if batch.strip():
            execute(c, batch)


def qi(name):
    return '[' + name.replace(']', ']]') + ']'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ROOT / 'sp_Blitz.sql')
    args = parser.parse_args()
    suffix = uuid.uuid4().hex[:12]
    login = 'FRKBlitz4194_' + suffix
    password = uuid.uuid4().hex + '!aA1'
    # Unicode and an apostrophe exercise the named-database permission lookup.
    db = "FRKBlitz4194_é'" + suffix
    job = 'FRKBlitz4194_job_' + suffix
    alert = 'FRKBlitz4194_alert_' + suffix
    qdb, qlogin = qi(db), qi(login)
    created_login = created_db = created_job = created_alert = False
    with closing(connect()) as admin:
        install(admin, args.source)
        install(admin, ROOT / 'sp_ineachdb.sql')
        try:
            execute(admin, f'CREATE LOGIN {qlogin} WITH PASSWORD=%s, CHECK_POLICY=OFF;', (password,))
            created_login = True
            execute(admin, f'CREATE DATABASE {qdb} COLLATE Latin1_General_100_CS_AS;')
            created_db = True
            execute(admin, f'ALTER DATABASE {qdb} SET AUTO_SHRINK ON;')
            execute(admin, f'''CREATE USER {qlogin} FOR LOGIN {qlogin};
                GRANT VIEW SERVER STATE TO {qlogin};
                GRANT EXECUTE ON dbo.sp_Blitz TO {qlogin};
                GRANT EXECUTE ON dbo.sp_ineachdb TO {qlogin};
                USE msdb; CREATE USER {qlogin} FOR LOGIN {qlogin};
                USE {qdb}; CREATE USER {qlogin} FOR LOGIN {qlogin};
                CREATE TABLE dbo.ChecksToSkip(DatabaseName nvarchar(128), CheckID int, ServerName nvarchar(128));
                WITH n AS (SELECT TOP (3001) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))-1 AS id
                           FROM sys.all_objects a CROSS JOIN sys.all_objects b)
                INSERT dbo.ChecksToSkip(CheckID) SELECT id FROM n
                WHERE id NOT IN ({','.join(map(str, TARGETS))});
                GRANT SELECT ON dbo.ChecksToSkip TO {qlogin};''')
            execute(admin, 'EXEC msdb.dbo.sp_add_job @job_name=%s, @owner_login_name=%s;', (job, login))
            created_job = True
            execute(admin, '''EXEC msdb.dbo.sp_add_jobstep @job_name=%s, @step_name=N'Inert fixture',
                @subsystem=N'TSQL', @command=N'SELECT 1;';
                EXEC msdb.dbo.sp_add_jobschedule @job_name=%s, @name=%s, @freq_type=64;''', (job, job, job))
            execute(admin, '''EXEC msdb.dbo.sp_add_alert @name=%s, @severity=16,
                @enabled=0, @database_name=%s;''', (alert, db))
            created_alert = True

            def run(label, expected, absent, database_select=False, expected_checks=(), absent_checks=()):
                # Use a new real SQL-authenticated connection for each grant state.
                with closing(connect(login, password)) as limited:
                    identity = execute(limited, '''SELECT IS_SRVROLEMEMBER('sysadmin') AS sa,
                        HAS_PERMS_BY_NAME('master','DATABASE','SELECT') AS master_read,
                        HAS_PERMS_BY_NAME('msdb','DATABASE','SELECT') AS msdb_read;''')[0]
                    assert identity == {'sa': 0, 'master_read': 0, 'msdb_read': int(database_select)}, identity
                    results = execute(limited, '''EXEC dbo.sp_Blitz @CheckUserDatabaseObjects=1,
                        @CheckServerInfo=1, @CheckProcedureCache=0, @UsualOwnerOfJobs=N'NotTheFixtureOwner',
                        @SkipChecksDatabase=%s, @SkipChecksSchema=N'dbo', @SkipChecksTable=N'ChecksToSkip';''', (db,))
                fixtures = {r['CheckID'] for r in results if
                            (r['CheckID'] == 13 and r['DatabaseName'] == db) or
                            (r['CheckID'] in (6, 57, 94) and job in (r['Details'] or '')) or
                            (r['CheckID'] == 98 and alert in (r['Details'] or ''))}
                assert expected <= fixtures and not (absent & fixtures), (label, fixtures, expected, absent)
                findings = {r['CheckID'] for r in results}
                assert set(expected_checks) <= findings and not (set(absent_checks) & findings), (label, findings)
                print('PASS:', label, sorted(fixtures), flush=True)

            # Database-level SELECT outside master must not create database skip entries.
            execute(admin, f'USE msdb; GRANT SELECT TO {qlogin}; USE {qdb}; GRANT SELECT TO {qlogin};')
            with closing(connect(login, password)) as limited:
                perms = execute(limited, '''SELECT HAS_PERMS_BY_NAME(%s,'DATABASE','SELECT') AS db_read,
                    HAS_PERMS_BY_NAME('msdb','DATABASE','SELECT') AS msdb_read;''', (db,))[0]
                assert perms == {'db_read': 1, 'msdb_read': 1}, perms
            run('database SELECT outside master', {6, 13, 57, 94, 98}, set(), database_select=True)
            execute(admin, f'USE msdb; REVOKE SELECT FROM {qlogin}; USE {qdb}; REVOKE SELECT FROM {qlogin};')
            run('no Agent grants', set(), {6, 13, 57, 94, 98}, absent_checks=(31,))

            # sysjobs alone enables check 6, but not checks needing schedules/history/alerts.
            execute(admin, f'USE msdb; GRANT SELECT ON dbo.sysjobs TO {qlogin};')
            run('sysjobs only', {6}, {13, 57, 94, 98})
            execute(admin, f'''USE msdb; GRANT SELECT ON dbo.sysjobschedules TO {qlogin};
                GRANT SELECT ON dbo.sysschedules TO {qlogin};
                GRANT SELECT ON dbo.sysjobhistory TO {qlogin};''')
            run('job dependencies granted', {6, 57, 94}, {13, 98})
            # A column DENY must disable check 6 without disabling checks 57/94,
            # which read the same sysjobs table but never use owner_sid.
            execute(admin, f'USE msdb; DENY SELECT ON dbo.sysjobs (owner_sid) TO {qlogin};')
            with closing(connect(login, password)) as limited:
                try:
                    execute(limited, 'SELECT TOP (0) owner_sid FROM msdb.dbo.sysjobs;')
                except pymssql.DatabaseError as exc:
                    assert exc.args[0] == 230, exc
                else:
                    raise AssertionError('Fixture column DENY did not reject owner_sid access')
            run('job owner column denied', {57, 94}, {6, 13, 98})
            execute(admin, f'USE msdb; REVOKE SELECT ON dbo.sysjobs (owner_sid) FROM {qlogin};')

            # The converse: grant only check 6's columns, with no table-wide SELECT.
            execute(admin, f'''USE msdb; REVOKE SELECT ON dbo.sysjobs FROM {qlogin};
                GRANT SELECT ON dbo.sysjobs (name, enabled, owner_sid) TO {qlogin};''')
            run('job owner columns only', {6}, {13, 57, 94, 98})
            execute(admin, f'''USE msdb; REVOKE SELECT ON dbo.sysjobs (name, enabled, owner_sid) FROM {qlogin};
                GRANT SELECT ON dbo.sysjobs TO {qlogin};''')

            execute(admin, f'USE msdb; DENY SELECT ON dbo.sysjobhistory TO {qlogin};')
            run('job history denied', {6, 57}, {13, 94, 98})
            execute(admin, f'USE msdb; REVOKE SELECT ON dbo.sysjobs FROM {qlogin}; GRANT SELECT ON dbo.sysalerts TO {qlogin};')
            run('alerts without sysjobs', {98}, {6, 13, 57, 94})

            # Alert checks also have independent column requirements: severity is
            # needed by checks 30/61, while the seeded disabled-alert finding needs
            # only name and enabled. A severity DENY must not suppress check 98.
            execute(admin, f'USE msdb; DENY SELECT ON dbo.sysalerts (severity) TO {qlogin};')
            run('alert severity column denied', {98}, {6, 13, 57, 94}, absent_checks=(30, 61))

            # Operators have their own permission gate, independent of jobs and alerts.
            no_enabled_operators = not execute(admin, 'SELECT TOP (1) 1 AS enabled_operator FROM msdb.dbo.sysoperators WHERE enabled=1;')
            execute(admin, f'USE msdb; GRANT SELECT ON dbo.sysoperators TO {qlogin};')
            run('operators granted independently', {98}, {6, 13, 57, 94},
                expected_checks=(31,) if no_enabled_operators else (),
                absent_checks=() if no_enabled_operators else (31,))
            # Keep a sysadmin smoke run to cover the bypass of permission probes.
            results = execute(admin, '''EXEC dbo.sp_Blitz @CheckUserDatabaseObjects=1,
                @CheckServerInfo=1, @UsualOwnerOfJobs=N'NotTheFixtureOwner',
                @SkipChecksDatabase=%s, @SkipChecksSchema=N'dbo', @SkipChecksTable=N'ChecksToSkip';''', (db,))
            assert any(r['CheckID'] == 13 and r['DatabaseName'] == db for r in results)
            print('PASS: sysadmin checks still run', flush=True)
        finally:
            if created_alert:
                execute(admin, 'EXEC msdb.dbo.sp_delete_alert @name=%s;', (alert,))
            if created_job:
                execute(admin, 'EXEC msdb.dbo.sp_delete_job @job_name=%s, @delete_unused_schedule=1;', (job,))
            if created_db:
                execute(admin, f'USE master; ALTER DATABASE {qdb} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {qdb};')
            if created_login:
                execute(admin, f'''USE msdb; DROP USER IF EXISTS {qlogin};
                    USE master; DROP USER IF EXISTS {qlogin}; DROP LOGIN {qlogin};''')


if __name__ == '__main__':
    main()
