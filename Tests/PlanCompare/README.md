# sp_BlitzPlanCompare manual regressions

These scripts are manual SQL Server 2022/2025 integration tests; the existing GitHub
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
for test in Tests/PlanCompare/plancompare-[0-9]*-regression.py Tests/PlanCompare/plancompare-statistics-compatibility-regression.py; do
    python "$test" || exit 1
done
~~~

The linked-server and database-scope regressions require two disposable SQL Server
instances. Their module docstrings list the remote connection variables. Both
connections use FRK_TEST_USER and FRK_TEST_PASSWORD. Set FRK_TEST_LINKED_SERVER
to an existing link with RPC OUT to test that configuration without changing or
dropping the link. Otherwise the tests create and remove their own temporary link.
Do not run multiple test scripts concurrently against the same server: they install
(and some temporarily replace) the procedure in master.
