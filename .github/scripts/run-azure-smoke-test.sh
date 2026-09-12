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
SQLCMD_BASE=(
  -S "tcp:$AZURE_SQL_SERVER,1433"
  -d "$AZURE_SQL_DATABASE"
  -U "$AZURE_SQL_USER"
  -P "$AZURE_SQL_PASSWORD"
  -N
  -b
  -I
  -t 600
)

SQLCMD_ARGS=("${SQLCMD_BASE[@]}" -l 60)

# Wake-up attempts get their own, much shorter login timeout, and the retry
# budget is sized to fit inside the job's timeout-minutes with room to spare.
# With the 60-second -l used for real work, a run where every attempt times out
# would spend 30 x (60s + 20s) = 40 minutes -- longer than the 30-minute job
# timeout, so the runner would kill the job before the loop could report why it
# gave up. At 15s + 15s the whole loop is 15 minutes at worst, leaving the rest
# of the budget for the install and the run.
WAKE_LOGIN_TIMEOUT=15
WAKE_SLEEP_SECONDS=15
WAKE_ATTEMPTS=30
WAKE_ARGS=("${SQLCMD_BASE[@]}" -l "$WAKE_LOGIN_TIMEOUT")

run_query() { "$SQLCMD" "${SQLCMD_ARGS[@]}" -Q "$1"; }

# ---------------------------------------------------------------------------
# Read a single integer out of a batch.
#
# Not "the first line of output": sqlcmd prints every result set the batch
# produced, and sp_Blitz emits a version-check result set ("Component ... is
# outdated") ahead of its real output. Taking the first line picked up that
# warning text and reported it as the finding count. So take the last line that
# is nothing but digits, which the warning text can never be.
#
# `|| true` because grep exits 1 when it matches nothing, and pipefail would
# turn that into an abort instead of letting the caller report a clear error.
# ---------------------------------------------------------------------------
run_scalar_int() {
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -h -1 -W -Q "$1" \
    | sed 's/[[:space:]]//g' \
    | grep -E '^[0-9]+$' \
    | tail -1 || true
}

# ---------------------------------------------------------------------------
# Serverless Azure SQL DB auto-pauses when idle, and the connection that wakes it
# is itself rejected while it resumes. A first failure means nothing; only a
# sustained one does.
# ---------------------------------------------------------------------------
wake_database() {
  local worst_case_minutes=$(( WAKE_ATTEMPTS * (WAKE_LOGIN_TIMEOUT + WAKE_SLEEP_SECONDS) / 60 ))

  echo "Connecting to Azure SQL Database (it auto-pauses; early failures are expected)..."
  echo "Up to $WAKE_ATTEMPTS attempts, at most ~${worst_case_minutes} minutes."
  for attempt in $(seq 1 "$WAKE_ATTEMPTS"); do
    if "$SQLCMD" "${WAKE_ARGS[@]}" -Q "SET NOCOUNT ON; SELECT 1;" >/dev/null 2>&1; then
      echo "Connected on attempt $attempt."
      return 0
    fi
    echo "  attempt $attempt: not up yet, waiting..."
    sleep "$WAKE_SLEEP_SECONDS"
  done

  echo "::error::Could not connect to Azure SQL Database after $WAKE_ATTEMPTS attempts (~${worst_case_minutes} minutes)." >&2
  echo "::error::Check that the server is reachable and that its firewall admits GitHub runner IPs." >&2
  return 1
}

wake_database

echo
echo "=== Engine confirms this really is Azure SQL Database ==="
# EngineEdition 5 is Azure SQL Database. If this is anything else, the test is
# passing for the wrong reasons and should say so rather than look green.
edition="$(run_scalar_int "SET NOCOUNT ON; SELECT CONVERT(INT, SERVERPROPERTY('EngineEdition'));")"

echo "EngineEdition: $edition"
if [[ "$edition" != "5" ]]; then
  echo "::error::Expected EngineEdition 5 (Azure SQL Database), got '$edition'. Refusing to report success." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Clear anything an earlier run left behind, before installing.
#
# Unlike the boxed jobs, which get a fresh container every time, this target is
# one long-lived database. A previous run's sp_Blitz would satisfy every
# assertion below -- it exists, its body is well over 10,000 characters, it runs,
# it returns findings -- even if this checkout installed nothing at all. The job
# would go green having measured yesterday's code.
#
# That is the same shape as the bug this job exists to catch: #4040 was invisible
# precisely because something called sp_Blitz was present and answered. So drop
# first and confirm the drop, which makes the definition check downstream prove
# that *this* checkout created what it is measuring.
# ---------------------------------------------------------------------------
echo
echo "=== Clearing procedures left by earlier runs ==="
run_query "DROP PROCEDURE IF EXISTS dbo.sp_Blitz;
           DROP PROCEDURE IF EXISTS dbo.sp_ineachdb;
           DROP PROCEDURE IF EXISTS dbo.sp_BlitzIndex;" > /dev/null

remaining="$(run_scalar_int "SET NOCOUNT ON;
SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID(N'dbo')
AND name IN ('sp_Blitz', 'sp_ineachdb', 'sp_BlitzIndex');")"

# Anything but a definite zero -- including an empty result -- means we cannot
# prove the database is clean, and a stale procedure could carry the run.
if [[ "$remaining" != "0" ]]; then
  echo "::error::Could not confirm the existing procedures were dropped (got '$remaining')." >&2
  echo "::error::A leftover sp_Blitz would let this run pass without testing this checkout." >&2
  exit 1
fi
echo "  database is clean"

echo
echo "=== Installing ==="
# sp_ineachdb first: sp_Blitz calls it to iterate databases.
for script in sp_ineachdb sp_Blitz sp_BlitzIndex; do
  echo "  installing $script"
  if ! "$SQLCMD" "${SQLCMD_ARGS[@]}" -i "$REPO_ROOT/$script.sql" > "$WORK_DIR/$script.log" 2>&1; then
    echo "::error::$script failed to install on Azure SQL Database"
    grep -E -A2 '^(Msg [0-9]+,|Sqlcmd: Error)' "$WORK_DIR/$script.log" | head -30 || tail -30 "$WORK_DIR/$script.log"
    exit 1
  fi
done

echo "=== Verifying Azure index output ==="
"$SQLCMD" "${SQLCMD_ARGS[@]}" -i "$REPO_ROOT/.github/scripts/azure-index-output-regression.sql"
echo "=== Verifying Azure database include/exclude lists ==="
"$SQLCMD" "${SQLCMD_ARGS[@]}" -i "$REPO_ROOT/.github/scripts/azure-ineachdb-regression.sql"

# The install "succeeding" is exactly what made #4040 invisible: the CREATE stub
# is its own batch and always works, while only the ALTER carrying the real body
# fails. So confirm the procedure has a body, not merely a name.
echo
echo "=== Confirming sp_Blitz is more than the stub ==="
body_length="$(run_scalar_int "SET NOCOUNT ON;
SELECT LEN(OBJECT_DEFINITION(OBJECT_ID('dbo.sp_Blitz')));")"

echo "sp_Blitz definition length: $body_length characters"
if ! [[ "$body_length" =~ ^[0-9]+$ ]] || (( body_length < 10000 )); then
  echo "::error::sp_Blitz exists but its body is '$body_length' characters -- that is the RETURN 0 stub, not the real procedure." >&2
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

# ---------------------------------------------------------------------------
# What the run actually found.
#
# Deliberately not a raw row count. Every successful run of sp_Blitz inserts rows
# that say nothing about whether a single check did any work:
#
#   -1   two credit / version rows, unconditionally
#   156  the rundate row, unconditionally
#   223  "Some Checks Skipped", whenever the login is not sysadmin -- so always,
#        here
#
# @OutputType = 'COUNT' counts those too, which made the previous "returned at
# least one finding" assertion unfailable: it stayed green even if every
# substantive check were skipped or silently returned nothing. That is the same
# shape as #4040 itself -- output that looks like proof of life but is not. So
# assert on CheckIDs that are not sentinels.
#
# CSV mode emits "Priority,CheckID,FindingsGroup,Finding,...", so CheckID is the
# second field, ahead of any free text; a comma inside a finding cannot shift it,
# and the -1 rows drop out because the field test admits digits only.
# ---------------------------------------------------------------------------
SENTINEL_CHECK_IDS="156 223"

echo
echo "=== What sp_Blitz found ==="
if ! "$SQLCMD" "${SQLCMD_ARGS[@]}" -h -1 -y 8000 -w 8000 -Q \
      "SET NOCOUNT ON; EXEC dbo.sp_Blitz @OutputType = 'CSV';" \
      > "$WORK_DIR/findings.csv" 2>&1; then
  echo "::error::Could not read sp_Blitz's findings on Azure SQL Database." >&2
  tail -20 "$WORK_DIR/findings.csv" >&2
  exit 1
fi

all_ids="$(awk -F, '$2 ~ /^[0-9]+$/ { print $2 }' "$WORK_DIR/findings.csv" \
           | sort -n -u | tr '\n' ' ')"

substantive_ids=""
for id in $all_ids; do
  case " $SENTINEL_CHECK_IDS " in
    *" $id "*) ;;
    *)         substantive_ids+="$id " ;;
  esac
done
substantive_count="$(wc -w <<< "$substantive_ids" | tr -d ' ')"

echo "CheckIDs returned:  ${all_ids:-(none)}"
echo "Sentinels ignored:  $SENTINEL_CHECK_IDS"
echo "Substantive checks: $substantive_count"

if (( substantive_count < 1 )); then
  echo "::error::sp_Blitz produced only sentinel rows -- no actual check returned a finding." >&2
  echo "::error::That is the #4040 symptom: the procedure answers, but none of its checks ran." >&2
  exit 1
fi

{
  echo "### Azure SQL Database"
  echo
  echo "- EngineEdition \`5\` confirmed"
  echo "- installed from a cleared database, definition $body_length characters (not the stub)"
  echo "- ran with no errors"
  echo "- **$substantive_count substantive checks** returned findings"
  echo "- CheckIDs: \`${all_ids:-none}\` (sentinels $SENTINEL_CHECK_IDS not counted)"
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

echo
echo "Azure SQL Database smoke test passed."
