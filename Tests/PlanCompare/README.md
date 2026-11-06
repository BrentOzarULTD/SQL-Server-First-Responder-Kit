# sp_BlitzPlanCompare manual regressions

These scripts are manual SQL Server 2022 integration tests; the existing GitHub
smoke workflows do not invoke them. Run only against a disposable instance:
the helper installs the procedure in master, and tests create their own databases.

Install Python 3.10+ and pymssql, set FRK_TEST_PASSWORD, and optionally
FRK_TEST_SERVER (default 127.0.0.1:11433) and FRK_TEST_USER (default sa).
Run an individual plancompare-<issue>-regression.py from the repository root.
The separate FRK_TEST_* variables deliberately avoid inheriting an ambient
SQLCMD connection to a non-test server.

Tests include fixtures exercising extracted production SQL as well as real cached
query plans. EngineEdition checks inject engine IDs on SQL Server; they are not
live Azure SQL Managed Instance tests.

Run all single-instance tests with:

~~~sh
for test in Tests/PlanCompare/plancompare-[0-9]*-regression.py; do
    python "$test" || exit 1
done
~~~

The separate linked-server test requires two disposable SQL Server 2022 instances;
its module docstring lists the remote connection variables.
