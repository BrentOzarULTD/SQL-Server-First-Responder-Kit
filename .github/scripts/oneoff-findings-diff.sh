#!/usr/bin/env bash
#
# TEMPORARY -- delete this file and its workflow once PR #4045 is verified.
#
# Answers the one question code review cannot: does the rewritten sp_Blitz still
# find the same things as the released one?
#
# Installs dev's sp_Blitz, runs it, records every (CheckID, DatabaseName,
# Finding). Installs this branch's, runs the same thing, records again. Diffs.
# The Azure rewrite is supposed to change how the code is written, not what it
# reports, so on boxed SQL Server the two lists should be identical.
#
# This is deliberately NOT part of the permanent smoke tests. Run on every PR it
# was noisy and never decided anything (see #4047). Run once against a change
# that rewrote 51 cross-database blocks, it is exactly the right tool.

set -Eeuo pipefail

: "${SQLCMDSERVER:=tcp:127.0.0.1,1433}"
: "${SQLCMDUSER:=sa}"
: "${SQLCMDPASSWORD:?SQLCMDPASSWORD must be set}"
: "${SQLCMD:=sqlcmd}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

SQLCMD_ARGS=(-S "$SQLCMDSERVER" -U "$SQLCMDUSER" -P "$SQLCMDPASSWORD" -C -b -I -r 1 -l 60 -t 600)

run_query() { "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -Q "$1"; }

# CheckIDs whose text embeds a timestamp or a live counter, so they differ
# between two runs minutes apart no matter what the code says:
#   156 -- puts GETDATE() straight into Finding
#   185 -- "Wait Stats Have Been Cleared", compares uptime against live counters
VOLATILE="156, 185"

wait_for_sql_server() {
  echo "Waiting for SQL Server..."
  for attempt in {1..90}; do
    run_query "SET NOCOUNT ON; SELECT 1;" >/dev/null 2>&1 && { echo "ready."; return 0; }
    [[ "$attempt" -eq 90 ]] && { echo "::error::SQL Server never became ready." >&2; return 1; }
    sleep 2
  done
}

# sp_Blitz aborts with a divide-by-zero inside the instance's first minute
# (issue #4048), which would poison whichever capture ran first.
wait_for_uptime() {
  local minutes
  for attempt in {1..40}; do
    minutes="$("$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -h -1 -W -Q \
      "SET NOCOUNT ON; SELECT DATEDIFF(MINUTE, create_date, CURRENT_TIMESTAMP) FROM sys.databases WHERE name='tempdb';" \
      2>/dev/null | sed '/^$/d;/rows affected/d' | head -1 | tr -d '[:space:]' || true)"
    [[ "$minutes" =~ ^[0-9]+$ ]] && (( minutes > 0 )) && { echo "Uptime window cleared."; return 0; }
    sleep 5
  done
  echo "::warning::Uptime window not confirmed; continuing."
}

capture() {
  local revision="$1" destination="$2"

  echo "  installing sp_Blitz from $revision"
  git -C "$REPO_ROOT" show "$revision:sp_Blitz.sql" > "$WORK_DIR/sp_Blitz.sql"
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -i "$WORK_DIR/sp_Blitz.sql" > "$WORK_DIR/install.log" 2>&1 || {
    echo "::error::sp_Blitz from $revision failed to install"; cat "$WORK_DIR/install.log"; return 1; }

  run_query "SET NOCOUNT ON;
IF OBJECT_ID('FRKSmokeTest.dbo.OneOffFindings') IS NOT NULL DROP TABLE FRKSmokeTest.dbo.OneOffFindings;" >/dev/null

  echo "  running sp_Blitz from $revision"
  run_query "
EXEC dbo.sp_Blitz
     @CheckUserDatabaseObjects = 1,
     @CheckServerInfo          = 1,
     @OutputDatabaseName       = 'FRKSmokeTest',
     @OutputSchemaName         = 'dbo',
     @OutputTableName          = 'OneOffFindings',
     @SkipChecksDatabase       = 'FRKSmokeTest',
     @SkipChecksSchema         = 'dbo',
     @SkipChecksTable          = 'BlitzChecksToSkip';" >/dev/null

  # -y/-w so long findings are not clipped; -y and -W are mutually exclusive, so
  # trailing padding is stripped here instead.
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d FRKSmokeTest -h -1 -y 8000 -w 8000 -Q "
SET NOCOUNT ON;
SELECT CONVERT(VARCHAR(10), CheckID)
       + ' | ' + ISNULL(DatabaseName, '(server)')
       + ' | ' + ISNULL(Finding, '')
FROM dbo.OneOffFindings
WHERE CheckID NOT IN ($VOLATILE)
ORDER BY CheckID, DatabaseName, Finding;" \
    | sed -e 's/[[:space:]]*$//' -e '/^$/d' -e '/rows affected/d' \
    | sort -u > "$destination"

  echo "  $(wc -l < "$destination" | tr -d ' ') findings recorded"
}

wait_for_sql_server
wait_for_uptime

echo
echo "=== Seeding ==="
"$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -i "$REPO_ROOT/.github/scripts/smoke-test-seed.sql"

BASE_REVISION="${GITHUB_BASE_SHA:-origin/dev}"

echo
echo "=== Baseline: sp_Blitz as released ($BASE_REVISION) ==="
capture "$BASE_REVISION" "$WORK_DIR/base.txt"

echo
echo "=== This branch: sp_Blitz with the Azure rewrite ==="
capture "HEAD" "$WORK_DIR/head.txt"

echo
echo "=== Result ==="
added="$(comm -13 "$WORK_DIR/base.txt" "$WORK_DIR/head.txt" || true)"
removed="$(comm -23 "$WORK_DIR/base.txt" "$WORK_DIR/head.txt" || true)"

{
  echo "## sp_Blitz findings, released vs rewritten -- \`${MSSQL_IMAGE:-this image}\`"
  echo
  echo "Baseline \`$BASE_REVISION\`: $(wc -l < "$WORK_DIR/base.txt" | tr -d ' ') findings."
  echo "This branch: $(wc -l < "$WORK_DIR/head.txt" | tr -d ' ') findings."
  echo
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

if [[ -z "$added" && -z "$removed" ]]; then
  echo "IDENTICAL. $(wc -l < "$WORK_DIR/head.txt" | tr -d ' ') findings, unchanged by the rewrite."
  echo "**Identical.** The rewrite changes how the code is written, not what it reports." \
    >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 0
fi

echo "DIFFERENT -- the rewrite changes what sp_Blitz reports:"
if [[ -n "$removed" ]]; then
  echo
  echo "  No longer reported (checks that went silent):"
  sed 's/^/    - /' <<< "$removed"
fi
if [[ -n "$added" ]]; then
  echo
  echo "  Newly reported:"
  sed 's/^/    + /' <<< "$added"
fi

{
  echo "**Differences found.**"
  [[ -n "$removed" ]] && { echo; echo "No longer reported:"; echo '```'; echo "$removed"; echo '```'; }
  [[ -n "$added" ]] && { echo; echo "Newly reported:"; echo '```'; echo "$added"; echo '```'; }
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

echo
echo "::error::sp_Blitz reports different findings before and after the rewrite."
exit 1
