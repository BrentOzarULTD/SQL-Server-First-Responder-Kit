#!/usr/bin/env bash
#
# Installs every non-deprecated First Responder Kit script onto a throwaway SQL
# Server and runs it. Any SQL error fails the build.
#
# Before issue #4046 this script installed only the changed sp_*.sql and ran it
# with @VersionCheckMode = 1 -- an unconditional early RETURN -- so no check body
# ever executed and runtime breakage passed green. PR #4045 had two such bugs
# reach human review; executing the scripts at all is what catches that class.
#
# Deliberately simple. An earlier draft also installed the base branch's copies,
# ran everything twice, and classified each failure as new or pre-existing using
# error signatures, step-body comparison and replay-on-mismatch. That machinery
# only earns its keep when the baseline is dirty, and the baseline is clean --
# every step passes on both engine versions. It was ~155 lines that never once
# took a decision, and it accounted for nearly every defect found while reviewing
# this harness. If a dirty baseline ever makes "is this failure ours?" a real
# question, bring it back then; git history has a working implementation.

set -Eeuo pipefail

: "${SQLCMDSERVER:=tcp:127.0.0.1,1433}"
: "${SQLCMDUSER:=sa}"
: "${SQLCMDPASSWORD:?SQLCMDPASSWORD must be set}"
: "${SQLCMD:=sqlcmd}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

SEED_SQL="$SCRIPT_DIR/smoke-test-seed.sql"
MATRIX_SQL="$SCRIPT_DIR/smoke-test-matrix.sql"

# Every non-deprecated script in the kit. sp_BlitzUpdate is deliberately absent:
# it overwrites the procs mid-run, invalidating every step after it
# (issue #4046, decision 4).
KIT_SCRIPTS=(
  "sp_Blitz.sql"
  "sp_BlitzAnalysis.sql"
  "sp_BlitzBackups.sql"
  "sp_BlitzCache.sql"
  "sp_BlitzFirst.sql"
  "sp_BlitzIndex.sql"
  "sp_BlitzLock.sql"
  "sp_BlitzWho.sql"
  "sp_DatabaseRestore.sql"
  "sp_ineachdb.sql"
  "sp_kill.sql"
  "OptionalScripts/sp_BlitzPlanCompare.sql"
)

# -I turns QUOTED_IDENTIFIER ON. sqlcmd defaults it OFF, and a procedure captures
# the setting in force when it is created, so without this the sp_BlitzFirst and
# sp_BlitzLock code paths that build indexed/XML expressions die at runtime with
# Msg 1934. Every real client (SSMS, .NET, ODBC) has it ON, so OFF was testing a
# configuration no user actually runs.
SQLCMD_ARGS=(
  -S "$SQLCMDSERVER"
  -U "$SQLCMDUSER"
  -P "$SQLCMDPASSWORD"
  -C
  -b
  -I
  -r 1
  -l 60
  -t 600
)

run_query() {
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -Q "$1"
}

run_file() {
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -i "$1"
}

# Single bare value, no headers or row counts. run_query cannot do this: it only
# forwards "$1", so any extra sqlcmd flags handed to it are silently dropped.
run_scalar() {
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -h -1 -W -Q "$1" 2>/dev/null \
    | sed '/^$/d;/rows affected/d' \
    | head -1 \
    | tr -d '[:space:]'
}

wait_for_sql_server() {
  echo "Waiting for SQL Server to accept connections..."
  for attempt in {1..90}; do
    if run_query "SET NOCOUNT ON; SELECT 1 AS ready;" >/dev/null 2>&1; then
      echo "SQL Server is ready."
      run_query "SET NOCOUNT ON; SELECT @@VERSION;"
      return 0
    fi

    if [[ "$attempt" -eq 90 ]]; then
      echo "::error::SQL Server did not become ready in time." >&2
      return 1
    fi

    sleep 2
  done
}

# ---------------------------------------------------------------------------
# Hold off until the instance has been up longer than a minute.
#
# sp_Blitz CheckID 152 divides by @MsSinceWaitsCleared, which is
# DATEDIFF(MINUTE, create_date, CURRENT_TIMESTAMP) * 60000.0 and is therefore 0
# for the instance's first minute. Its zero-guard sits inside a branch that
# cannot be taken while the value is 0, so sp_Blitz aborts with a divide-by-zero.
# Tracked in issue #4048; remove this once that is fixed.
#
# The value must parse as a number greater than zero. Anything else -- an empty
# result, a header, an error -- means we have not confirmed the window has passed,
# so keep waiting rather than assuming the best. `|| true` matters: under set -e a
# failed command substitution would abort instead of retrying.
# ---------------------------------------------------------------------------
wait_for_wait_stats() {
  local uptime_minutes

  echo "Waiting for the instance to pass one minute of uptime (issue #4048)..."
  for attempt in {1..40}; do
    uptime_minutes="$(run_scalar "SET NOCOUNT ON;
SELECT DATEDIFF(MINUTE, create_date, CURRENT_TIMESTAMP)
FROM sys.databases WHERE name = 'tempdb';" || true)"

    if [[ "$uptime_minutes" =~ ^[0-9]+$ ]] && (( uptime_minutes > 0 )); then
      echo "Uptime window cleared after ${attempt} check(s) (${uptime_minutes} minute(s) up)."
      return 0
    fi

    sleep 5
  done

  echo "::warning::Instance still reports under a minute of uptime; continuing anyway."
}

kit_proc_name() {
  basename "$1" .sql
}

# Installing without error is not proof the procedure exists: a script reduced to
# an empty file still exits sqlcmd 0.
verify_kit_installed() {
  local script proc missing=""

  for script in "${KIT_SCRIPTS[@]}"; do
    proc="$(kit_proc_name "$script")"
    if [[ "$(run_scalar "SET NOCOUNT ON;
SELECT CASE WHEN OBJECT_ID('dbo.$proc', 'P') IS NULL THEN 'MISSING' ELSE 'OK' END;" || true)" != "OK" ]]; then
      missing+="$proc "
    fi
  done

  if [[ -n "$missing" ]]; then
    echo "::error::Installed without error but these procedures do not exist: $missing"
    return 1
  fi
}

install_kit() {
  local script

  for script in "${KIT_SCRIPTS[@]}"; do
    if [[ ! -f "$REPO_ROOT/$script" ]]; then
      echo "::error::Expected kit script is missing: $script"
      return 1
    fi

    echo "  installing $script"
    if ! run_file "$REPO_ROOT/$script" > "$WORK_DIR/install.log" 2>&1; then
      echo "::error::Failed to install $script"
      cat "$WORK_DIR/install.log"
      return 1
    fi
  done

  verify_kit_installed
}

split_matrix() {
  local steps_dir="$1"

  rm -rf "$steps_dir"
  mkdir -p "$steps_dir"

  python3 - "$MATRIX_SQL" "$steps_dir" <<'PYTHON'
import os
import sys

matrix_path, steps_dir = sys.argv[1], sys.argv[2]

steps, label, buf = [], None, []
with open(matrix_path, encoding="utf-8") as handle:
    for line in handle:
        if line.startswith("--#STEP:"):
            if label is not None:
                steps.append((label, "".join(buf)))
            label = line.split(":", 1)[1].strip()
            buf = []
        elif label is not None:
            buf.append(line)
if label is not None:
    steps.append((label, "".join(buf)))

duplicates = {label for label, _ in steps if [l for l, _ in steps].count(label) > 1}
if duplicates:
    sys.exit("Duplicate --#STEP: labels in the matrix: " + ", ".join(sorted(duplicates)))

for index, (step_label, body) in enumerate(steps):
    with open(os.path.join(steps_dir, f"{index:03d}.sql"), "w", encoding="utf-8") as handle:
        handle.write(body)
    with open(os.path.join(steps_dir, f"{index:03d}.label"), "w", encoding="utf-8") as handle:
        handle.write(step_label)
PYTHON
}

# ---------------------------------------------------------------------------
# Run every step on its own, so a failure is attributed to one labelled step and
# the rest still run. One CI round should surface every problem, not just the
# first one to blow up.
# ---------------------------------------------------------------------------
run_matrix() {
  local steps_dir="$WORK_DIR/steps"
  local step_file current_label failures=0 total=0

  for step_file in "$steps_dir"/*.sql; do
    current_label="$(cat "${step_file%.sql}.label")"
    total=$((total + 1))

    if "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -i "$step_file" \
         > "$WORK_DIR/step.log" 2>&1; then
      echo "  PASS  $current_label"
    else
      echo "  FAIL  $current_label"
      # Surface the diagnostics themselves, not a blind tail: these procs print
      # result sets, so the error scrolls out of view long before the end.
      {
        grep -E -A2 '^(Msg [0-9]+,|Sqlcmd: Error)' "$WORK_DIR/step.log" | head -24 || true
        echo "--- last lines ---"
        tail -6 "$WORK_DIR/step.log"
      } | sed 's/^/        /'

      {
        echo "### Failed: \`$current_label\` on \`${MSSQL_IMAGE:-this image}\`"
        echo '```'
        grep -E -A2 '^(Msg [0-9]+,|Sqlcmd: Error)' "$WORK_DIR/step.log" | head -12 || true
        echo '```'
      } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

      failures=$((failures + 1))
    fi
  done

  echo
  if [[ "$failures" -gt 0 ]]; then
    echo "::error::$failures of $total step(s) failed on ${MSSQL_IMAGE:-this image}."
    return 1
  fi

  echo "All $total steps passed."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
wait_for_sql_server
wait_for_wait_stats

echo
echo "=== Seeding test data ==="
run_file "$SEED_SQL"

echo
echo "=== Installing the kit ==="
install_kit

echo
echo "=== Running the matrix ==="
split_matrix "$WORK_DIR/steps"
run_matrix

echo
echo "Smoke tests passed."
