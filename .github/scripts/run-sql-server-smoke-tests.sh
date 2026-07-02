#!/usr/bin/env bash
set -Eeuo pipefail

: "${SQLCMDSERVER:=tcp:127.0.0.1,1433}"
: "${SQLCMDUSER:=sa}"
: "${SQLCMDPASSWORD:?SQLCMDPASSWORD must be set}"
: "${SQLCMD:=sqlcmd}"

SQLCMD_ARGS=(
  -S "$SQLCMDSERVER"
  -U "$SQLCMDUSER"
  -P "$SQLCMDPASSWORD"
  -C
  -b
  -r 1
  -l 30
)

run_query() {
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -Q "$1"
}

is_root_sp_script() {
  local path="$1"

  [[ "$path" =~ ^sp_[A-Za-z0-9_]+\.sql$ && -f "$path" ]]
}

discover_changed_sp_scripts() {
  local changed_path
  local diff_base
  local diff_head
  local -a changed_paths=()
  local -a scripts=()

  if [[ -n "${CHANGED_SQL_FILES:-}" ]]; then
    while IFS= read -r changed_path; do
      [[ -n "$changed_path" ]] && changed_paths+=("$changed_path")
    done <<< "$CHANGED_SQL_FILES"
  elif [[ -n "${GITHUB_BASE_SHA:-}" && -n "${GITHUB_HEAD_SHA:-}" ]]; then
    diff_base="$GITHUB_BASE_SHA"
    diff_head="$GITHUB_HEAD_SHA"
    while IFS= read -r changed_path; do
      [[ -n "$changed_path" ]] && changed_paths+=("$changed_path")
    done < <(git diff --name-only --diff-filter=ACMR "$diff_base" "$diff_head" -- ':(top)sp_*.sql')
  elif [[ -n "${GITHUB_BASE_REF:-}" ]]; then
    diff_base="origin/${GITHUB_BASE_REF}"
    diff_head="HEAD"
    while IFS= read -r changed_path; do
      [[ -n "$changed_path" ]] && changed_paths+=("$changed_path")
    done < <(git diff --name-only --diff-filter=ACMR "$diff_base" "$diff_head" -- ':(top)sp_*.sql')
  else
    echo "Unable to determine changed PR files. Set CHANGED_SQL_FILES for a manual run." >&2
    exit 1
  fi

  for changed_path in "${changed_paths[@]}"; do
    if is_root_sp_script "$changed_path"; then
      scripts+=("$changed_path")
    fi
  done

  if [[ "${#scripts[@]}" -gt 0 ]]; then
    printf '%s\n' "${scripts[@]}" | sort -u
  fi
}

verify_proc_installed_and_version_check() {
  local proc_name="$1"

  run_query "
SET NOCOUNT ON;

DECLARE @ProcName sysname = N'$proc_name',
        @ObjectId int,
        @Version varchar(30),
        @VersionDate datetime,
        @Sql nvarchar(max);

SET @ObjectId = OBJECT_ID(N'dbo.' + QUOTENAME(@ProcName), N'P');

IF @ObjectId IS NULL
BEGIN
    THROW 51000, 'Expected stored procedure was not installed.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.parameters
    WHERE object_id = @ObjectId
      AND name = N'@VersionCheckMode'
)
BEGIN
    THROW 51001, 'Stored procedure does not expose @VersionCheckMode.', 1;
END;

SET @Sql = N'EXEC dbo.' + QUOTENAME(@ProcName) + N'
    @Version = @Version OUTPUT,
    @VersionDate = @VersionDate OUTPUT,
    @VersionCheckMode = 1;';

EXEC sys.sp_executesql
    @Sql,
    N'@Version varchar(30) OUTPUT, @VersionDate datetime OUTPUT',
    @Version = @Version OUTPUT,
    @VersionDate = @VersionDate OUTPUT;

IF @Version IS NULL OR @VersionDate IS NULL
BEGIN
    THROW 51002, 'Stored procedure version check failed.', 1;
END;
"
}

echo "Waiting for SQL Server to accept connections..."
for attempt in {1..60}; do
  if run_query "SET NOCOUNT ON; SELECT 1 AS ready;" >/dev/null 2>&1; then
    echo "SQL Server is ready."
    break
  fi

  if [[ "$attempt" -eq 60 ]]; then
    echo "SQL Server did not become ready in time." >&2
    exit 1
  fi

  sleep 2
done

changed_scripts=()
while IFS= read -r changed_script; do
  [[ -n "$changed_script" ]] && changed_scripts+=("$changed_script")
done < <(discover_changed_sp_scripts)

if [[ "${#changed_scripts[@]}" -eq 0 ]]; then
  echo "No changed root-level sp_*.sql scripts found; skipping SQL Server smoke tests."
  exit 0
fi

echo "Changed root-level stored procedure scripts:"
printf ' - %s\n' "${changed_scripts[@]}"

for script in "${changed_scripts[@]}"; do
  proc_name="$(basename "$script" .sql)"

  echo "Installing $script..."
  "$SQLCMD" "${SQLCMD_ARGS[@]}" -d master -i "$script"

  echo "Running version check for dbo.$proc_name..."
  verify_proc_installed_and_version_check "$proc_name"
done

echo "Changed stored procedure smoke tests passed."
