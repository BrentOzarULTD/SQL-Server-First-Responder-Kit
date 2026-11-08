# sp_Blitz permission regressions

Run `blitz-4194-regression.py` only against a disposable SQL Server with SQL Server
Agent metadata available. It requires Python 3.10+ and `pymssql`.

Set `FRK_TEST_PASSWORD`, plus `FRK_TEST_SERVER` (default `127.0.0.1:11433`) and
`FRK_TEST_USER` (default `sa`), then run from the repository root:

```sh
python Tests/Blitz/blitz-4194-regression.py
```

The administrative connection installs `sp_Blitz` and `sp_ineachdb` in master.
The test creates and removes a restricted login, a case-sensitive database with
a Unicode name, a harmless Agent job and startup schedule, and a disabled alert.
Do not run this concurrently with other tests that install these procedures.

Each permission scenario opens a real connection as the restricted SQL login.
Assertions require the seeded database, job, and alert findings, covering
SELECT outside master, individual object grants, missing grants, explicit
DENY, and checks with multiple table dependencies. Unrelated checks are skipped
to keep this focused on #4194. A sysadmin run covers the permission-probe bypass.

To verify the regression against an older procedure, pass `--source /path/to/sp_Blitz.sql`.
These are manual integration tests; the GitHub smoke workflow does not invoke them.
