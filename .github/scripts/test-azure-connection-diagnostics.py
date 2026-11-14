"""Exercise the real retry loop with a fake client; no database connection."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / ".github/scripts/run-azure-smoke-test.sh"


class ConnectionDiagnostics(unittest.TestCase):
    def run_retry(self, succeed_after=0):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            client = temp / "sqlcmd"
            client.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
counter = pathlib.Path(os.environ['TEST_COUNTER'])
n = int(counter.read_text()) + 1 if counter.exists() else 1
counter.write_text(str(n))
if n == int(os.environ['TEST_SUCCEED_AFTER']):
    print('1')
    sys.exit(0)
print('Sqlcmd: Error: Login failed for ' + os.environ['AZURE_SQL_USER'])
for key in ('SERVER', 'DATABASE', 'PASSWORD'):
    print(os.environ['AZURE_SQL_' + key])
print('::warning::untrusted client text')
print('prefix ##[warning]legacy client text')
sys.exit(42)
''')
            client.chmod(0o755)
            sleeper = temp / "sleep"
            sleeper.write_text("#!/bin/sh\nexit 0\n")
            sleeper.chmod(0o755)
            env = dict(os.environ, AZURE_SQL_SERVER="private.example",
                       AZURE_SQL_DATABASE="private-db", AZURE_SQL_USER="private-login",
                       AZURE_SQL_PASSWORD="secret.*[x]\r\nsecond-line", SQLCMD=str(client),
                       TEST_COUNTER=str(temp / "count"), TEST_SUCCEED_AFTER=str(succeed_after),
                       PATH=str(temp) + os.pathsep + os.environ["PATH"])
            # Run the actual runner setup and wake function, stopping before DDL.
            source = RUNNER.read_text().split("\nwake_database\n", 1)[0]
            # bash -c has no source path, so supply the repository root explicitly.
            source = source.replace('REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"',
                                    'REPO_ROOT="$TEST_REPO_ROOT"')
            env["TEST_REPO_ROOT"] = str(ROOT)
            result = subprocess.run(["bash", "-c", source + "\nwake_database\n"],
                                    env=env, text=True, capture_output=True)
            output = result.stdout + result.stderr
            for key in ("SERVER", "DATABASE", "USER", "PASSWORD"):
                self.assertNotIn(env["AZURE_SQL_" + key], output)
            self.assertNotIn("second-line", output)
            self.assertNotIn("secret.*[x]", output)
            self.assertIn("    : :warning: :untrusted client text", output)
            self.assertIn("prefix # #[warning]legacy client text", output)
            self.assertNotIn("::warning::", output)
            self.assertNotIn("##[warning]", output)
            self.assertIn("Sqlcmd: Error: Login failed for [redacted]", output)
            return result, int((temp / "count").read_text()), output

    def test_exhaustion_retains_error_and_exit_code(self):
        result, count, output = self.run_retry()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(count, 30)
        self.assertIn("attempt 30: sqlcmd exited 42", output)
        self.assertEqual(output.count("Login failed"), 2)
        self.assertIn("installation and procedure tests have not run", output)

    def test_transient_failure_still_retries_successfully(self):
        result, count, output = self.run_retry(2)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(count, 2)
        self.assertIn("Connected on attempt 2.", output)


if __name__ == "__main__":
    unittest.main()
