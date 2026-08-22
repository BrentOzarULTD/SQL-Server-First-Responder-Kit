#!/usr/bin/env bash
#
# Installs and runs sp_Blitz against a real Azure SQL Database.
#
# Azure SQL DB refuses to compile a module that names another database, which is
# the whole subject of issue #4040: sp_Blitz's ALTER PROCEDURE aborted on the
# first of 88 cross-database references and the procedure was never created.
# Worse, the CREATE stub in the preceding batch succeeded, so users were left
# with an sp_Blitz that ran, returned nothing, and raised no error.
#
# That failure mode is invisible to a boxed SQL Server test, and invisible to
# "did it install" -- the install genuinely succeeds. So this checks three
# things: the procedure installs without error, it runs without error, and it
# actually returns findings.

set -Eeuo pipefail

: "${AZURE_SQL_SERVER:?AZURE_SQL_SERVER must be set}"
: "${AZURE_SQL_DATABASE:?AZURE_SQL_DATABASE must be set}"
: "${AZURE_SQL_USER:?AZURE_SQL_USER must be set}"
: "${AZURE_SQL_PASSWORD:?AZURE_SQL_PASSWORD must be set}"
: "${SQLCMD:=sqlcmd}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# -I for QUOTED_IDENTIFIER ON, matching every real client. No -C: Azure presents
# a valid certificate, so the connection is verified rather than trusted blindly.
SQLCMD_ARGS=(
  -S "tcp:$AZURE_SQL_SERVER,1433"
  -d "$AZURE_SQL_DATABASE"
  -U "$AZURE_SQL_USER"
  -P "$AZURE_SQL_PASSWORD"
  -N
  -b
  -I
  -l 60
  -t 600
)

run_query() { "$SQLCMD" "${SQLCMD_ARGS[@]}" -Q "$1"; }

# ---------------------------------------------------------------------------
# Serverless Azure SQL DB auto-pauses when idle, and the connection that wakes it
# is itself rejected while it resumes. A first failure means nothing; only a
# sustained one does.
# ---------------------------------------------------------------------------
wake_database() {
  echo "Connecting to Azure SQL Database (it auto-pauses; early failures are expected)..."
  for attempt in {1..30}; do
    if run_query "SET NOCOUNT ON; SELECT 1;" >/dev/null 2>&1; then
      echo "Connected on attempt $attempt."
      return 0
    fi
    echo "  attempt $attempt: not up yet, waiting..."
    sleep 20
  done

  echo "::error::Could not connect to Azure SQL Database after 30 attempts (10 minutes)." >&2
  echo "::error::Check that the server is reachable and that its firewall admits GitHub runner IPs." >&2
  return 1
}

wake_database

echo
echo "=== Engine confirms this really is Azure SQL Database ==="
# EngineEdition 5 is Azure SQL Database. If this is anything else, the test is
# passing for the wrong reasons and should say so rather than look green.
edition="$("$SQLCMD" "${SQLCMD_ARGS[@]}" -h -1 -W -Q \
  "SET NOCOUNT ON; SELECT CONVERT(INT, SERVERPROPERTY('EngineEdition'));" \
  | sed '/^$/d;/rows affected/d' | head -1 | tr -d '[:space:]')"

echo "EngineEdition: $edition"
if [[ "$edition" != "5" ]]; then
  echo "::error::Expected EngineEdition 5 (Azure SQL Database), got '$edition'. Refusing to report success." >&2
  exit 1
fi

echo
echo "=== Installing ==="
# sp_ineachdb first: sp_Blitz calls it to iterate databases.
for script in sp_ineachdb sp_Blitz; do
  echo "  installing $script"
  if ! "$SQLCMD" "${SQLCMD_ARGS[@]}" -i "$REPO_ROOT/$script.sql" > "$WORK_DIR/$script.log" 2>&1; then
    echo "::error::$script failed to install on Azure SQL Database"
    grep -E -A2 '^(Msg [0-9]+,|Sqlcmd: Error)' "$WORK_DIR/$script.log" | head -30 || tail -30 "$WORK_DIR/$script.log"
    exit 1
  fi
done

# The install "succeeding" is exactly what made #4040 invisible: the CREATE stub
# is its own batch and always works, while only the ALTER carrying the real body
# fails. So confirm the procedure has a body, not merely a name.
echo
echo "=== Confirming sp_Blitz is more than the stub ==="
body_lines="$("$SQLCMD" "${SQLCMD_ARGS[@]}" -h -1 -W -Q \
  "SET NOCOUNT ON;
   SELECT LEN(OBJECT_DEFINITION(OBJECT_ID('dbo.sp_Blitz')));" \
  | sed '/^$/d;/rows affected/d' | head -1 | tr -d '[:space:]')"

echo "sp_Blitz definition length: $body_lines characters"
if ! [[ "$body_lines" =~ ^[0-9]+$ ]] || (( body_lines < 10000 )); then
  echo "::error::sp_Blitz exists but its body is $body_lines characters -- that is the RETURN 0 stub, not the real procedure." >&2
  echo "::error::This is the #4040 failure: the CREATE stub succeeded and the ALTER carrying the body did not." >&2
  exit 1
fi

echo
echo "=== Running sp_Blitz ==="
if ! run_query "EXEC dbo.sp_Blitz;" > "$WORK_DIR/run.log" 2>&1; then
  echo "::error::sp_Blitz raised an error on Azure SQL Database"
  grep -E -A2 '^(Msg [0-9]+,|Sqlcmd: Error)' "$WORK_DIR/run.log" | head -40 || tail -40 "$WORK_DIR/run.log"
  exit 1
fi

echo "  ran without error"

# A silent clean bill of health is the bug, not the goal. Any real server has
# something to say, so zero findings means the checks are not running.
echo
echo "=== Counting findings ==="
findings="$("$SQLCMD" "${SQLCMD_ARGS[@]}" -h -1 -W -Q \
  "SET NOCOUNT ON; EXEC dbo.sp_Blitz @OutputType = 'COUNT';" \
  | sed '/^$/d;/rows affected/d' | head -1 | tr -d '[:space:]')"

echo "sp_Blitz returned $findings finding(s)"
if ! [[ "$findings" =~ ^[0-9]+$ ]] || (( findings < 1 )); then
  echo "::error::sp_Blitz returned no findings. Running clean with nothing to say is the #4040 symptom, not a pass." >&2
  exit 1
fi

{
  echo "### Azure SQL Database"
  echo
  echo "- EngineEdition \`5\` confirmed"
  echo "- \`sp_Blitz\` installed, definition $body_lines characters (not the stub)"
  echo "- ran with no errors"
  echo "- returned **$findings findings**"
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

echo
echo "Azure SQL Database smoke test passed."
