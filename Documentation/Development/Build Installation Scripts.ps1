<#
.SYNOPSIS
    Automates the First Responder Kit release process: bumps version numbers,
    builds the Install-*.sql merged scripts, runs them on a local SQL Server,
    executes test calls, and (if clean) drafts a pull request into dev.

.PARAMETER ServerInstance
    SQL Server instance to test against. Default: localhost

.PARAMETER TestDatabase
    Database containing dbo.Users for the table-specific sp_BlitzIndex tests.
    Must already exist and be online. For the FRK VM, use StackOverflow2010.

.PARAMETER Database
    Database where the procs are installed. Default: master

.EXAMPLE
    & "Documentation\Development\Build Installation Scripts.ps1"
#>
param(
    [string]$ServerInstance = "localhost",
    [string]$Database       = "master",
    [ValidateNotNullOrEmpty()]
    [ValidateLength(1, 128)]
    [string]$TestDatabase   = "StackOverflow"
)

$ErrorActionPreference = "Stop"

# ── Paths ────────────────────────────────────────────────────────────────────
$ScriptDir  = $PSScriptRoot
$RepoRoot   = (Resolve-Path "$ScriptDir\..\..\").Path
$ErrorLog   = Join-Path $ScriptDir "release_errors_$(Get-Date -Format 'yyyyMMdd').log"
$Today      = Get-Date -Format "yyyyMMdd"
$BranchName = "${Today}_release_prep"
$Utf8NoBom  = New-Object System.Text.UTF8Encoding $false

# ── Helper: run a SQL string via sqlcmd, return output ───────────────────────
function Invoke-SqlCmd-String {
    param([string]$Sql, [string]$Label)

    $tmpFile = [System.IO.Path]::GetTempFileName() + ".sql"
    [System.IO.File]::WriteAllText($tmpFile, $Sql, $Utf8NoBom)

    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & sqlcmd -S $ServerInstance -E -d $Database -C -i $tmpFile -b -r 1 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $prevPref
    Remove-Item $tmpFile -ErrorAction SilentlyContinue

    return @{
        Output = ($output -join "`n")
        ExitCode = $exitCode
        Label = $Label
        Command = "sqlcmd -S `"$ServerInstance`" -E -d `"$Database`" -C -i `"$tmpFile`" -b -r 1"
    }
}

# ── Helper: run a .sql file via sqlcmd, return output ────────────────────────
function Invoke-SqlCmd-File {
    param([string]$FilePath, [string]$Label)

    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & sqlcmd -S $ServerInstance -E -d $Database -C -i $FilePath -b -r 1 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $prevPref

    return @{
        Output = ($output -join "`n")
        ExitCode = $exitCode
        Label = $Label
        FilePath = $FilePath
        Command = "sqlcmd -S `"$ServerInstance`" -E -d `"$Database`" -C -i `"$FilePath`" -b -r 1"
    }
}

# ── Helper: pull out the most useful sqlcmd error lines ──────────────────────
function Get-SqlCmdErrorSummary {
    param([string]$Output)

    if ([string]::IsNullOrWhiteSpace($Output)) {
        return "(sqlcmd did not return any output.)"
    }

    $lines = $Output -split "`r?`n"
    $errorLines = $lines | Where-Object {
        $_ -match "^(Msg\s+\d+|Sqlcmd:\s+Error:|Sqlcmd:\s+Warning:)" -or
        $_ -match "\b(Level|State|Line)\b" -or
        $_ -match "(Incorrect syntax|Invalid object|Cannot|Could not|failed|error)"
    } | Select-Object -First 20

    if ($errorLines.Count -eq 0) {
        $errorLines = $lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 20
    }

    return ($errorLines -join "`n")
}

# ── Helper: write actionable failure details to console and log ──────────────
function Report-SqlCmdFailure {
    param([hashtable]$Result, [string]$ExtraContext = "")

    $details = @"
Command:
$($Result.Command)

Exit code: $($Result.ExitCode)
$ExtraContext
Output:
$($Result.Output)
"@

    Log-Error -Label $Result.Label -Details $details

    Write-Host "  sqlcmd exit code: $($Result.ExitCode)" -ForegroundColor Red
    if ($Result.FilePath) {
        Write-Host "  SQL file: $($Result.FilePath)" -ForegroundColor Yellow
    }
    Write-Host "  Target: $ServerInstance / $Database" -ForegroundColor Yellow
    Write-Host "  Most relevant output:" -ForegroundColor Yellow
    foreach ($line in (Get-SqlCmdErrorSummary -Output $Result.Output) -split "`n") {
        Write-Host "    $line" -ForegroundColor DarkYellow
    }
    Write-Host "  Full command, exit code, and output were written to: $ErrorLog" -ForegroundColor Yellow
}

# ── Helper: log an error ─────────────────────────────────────────────────────
$script:HasErrors = $false
function Log-Error {
    param([string]$Label, [string]$Details)
    $script:HasErrors = $true
    $entry = @"
================================================================================
TEST: $Label
$(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
--------------------------------------------------------------------------------
$Details
"@
    Add-Content -Path $ErrorLog -Value $entry -Encoding UTF8
    Write-Host "  FAIL: $Label" -ForegroundColor Red
}

# Validate the fixture before changing versions, rebuilding, or dropping test tables.
$TestDatabaseSql = $TestDatabase.Replace("'", "''")
$fixtureSql = @"
DECLARE @Fixture sysname = N'$TestDatabaseSql';
IF DB_ID(@Fixture) IS NULL OR ISNULL(CONVERT(nvarchar(60), DATABASEPROPERTYEX(@Fixture, 'Status')), N'') <> N'ONLINE'
    THROW 51000, 'The test database must exist and be online. Specify -TestDatabase.', 1;
IF OBJECT_ID(QUOTENAME(@Fixture) + N'.dbo.Users', N'U') IS NULL
    THROW 51000, 'The test database must contain dbo.Users.', 1;
"@
$fixtureResult = Invoke-SqlCmd-String -Sql $fixtureSql -Label "Test database prerequisite"
if ($fixtureResult.ExitCode -ne 0) {
    Report-SqlCmdFailure -Result $fixtureResult
    throw "Test database prerequisite failed; no release files were changed."
}

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Determine current version and compute new version
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== Step 1: Determine version ===" -ForegroundColor Cyan

$sampleFile = Get-ChildItem -Path $RepoRoot -Filter "sp_Blitz.sql" | Select-Object -First 1
$sampleContent = Get-Content $sampleFile.FullName -Raw -Encoding UTF8

if ($sampleContent -match "SELECT\s+@Version\s*=\s*'([^']+)',\s*@VersionDate\s*=\s*'([^']+)'") {
    $currentVersion = $Matches[1]
    $currentDate    = $Matches[2]
} else {
    throw "Could not parse current version from $($sampleFile.Name)"
}

# Bump minor version by 0.01
$newVersion = [string]([math]::Round([double]$currentVersion + 0.01, 2))
# Ensure two decimal places (e.g. 8.30 not 8.3)
if ($newVersion -notmatch "\.\d{2}$") { $newVersion = "{0:F2}" -f [double]$newVersion }

Write-Host "  Current: v$currentVersion ($currentDate)"
Write-Host "  New:     v$newVersion ($Today)"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Update version numbers and dates in all sp_*.sql files
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== Step 2: Update version numbers ===" -ForegroundColor Cyan

$spFiles = Get-ChildItem -Path $RepoRoot -Filter "sp_*.sql"
foreach ($file in $spFiles) {
    $content = [System.IO.File]::ReadAllText($file.FullName)
    $content = $content -replace "SELECT\s+@Version\s*=\s*'[^']+',\s*@VersionDate\s*=\s*'[^']+';", "SELECT @Version = '$newVersion', @VersionDate = '$Today';"
    [System.IO.File]::WriteAllText($file.FullName, $content, $Utf8NoBom)
    Write-Host "  Updated: $($file.Name)"
}

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Build installation scripts
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== Step 3: Build Install-*.sql ===" -ForegroundColor Cyan

$SqlVersionsPath  = Join-Path $RepoRoot "SqlServerVersions.sql"
$BlitzFirstPath   = Join-Path $RepoRoot "sp_BlitzFirst.sql"
$InstallAllPath   = Join-Path $RepoRoot "Install-All-Scripts.sql"
$InstallAzurePath = Join-Path $RepoRoot "Install-Azure.sql"

# ── Install-Azure.sql ────────────────────────────────────────────────────────
# sp_ineachdb.sql must come first because sp_Blitz calls it at runtime.
# All sp_Blitz*.sql except sp_BlitzBackups.sql, sp_DatabaseRestore.sql, sp_BlitzFirst.sql.
# sp_BlitzBackups and sp_DatabaseRestore depend on xp_cmdshell / msdb backup history.
# sp_BlitzFirst is appended last (same as Install-All-Scripts.sql).
$IneachdbPath = Join-Path $RepoRoot "sp_ineachdb.sql"
$azureContent = @()
if (Test-Path $IneachdbPath) {
    $azureContent += Get-Content -Path $IneachdbPath -Raw -Encoding UTF8
}
$azureContent += Get-ChildItem -Path $RepoRoot -Filter "sp_Blitz*.sql" |
    Where-Object { $_.Name -notlike "*BlitzBackups*" -and $_.Name -notlike "*DatabaseRestore*" -and $_.Name -notlike "*BlitzFirst*" } |
    ForEach-Object { Get-Content $_.FullName -Raw -Encoding UTF8 }

if (Test-Path $BlitzFirstPath) {
    $azureContent += Get-Content -Path $BlitzFirstPath -Raw -Encoding UTF8
}

[System.IO.File]::WriteAllText($InstallAzurePath, ($azureContent -join "`r`n"), $Utf8NoBom)
Write-Host "  Built: Install-Azure.sql"

# ── Install-All-Scripts.sql ──────────────────────────────────────────────────
# All sp_*.sql except sp_BlitzInMemoryOLTP.sql and sp_BlitzFirst.sql.
# sp_ineachdb.sql must come first because sp_Blitz (and others) call it at
# runtime — putting it ahead of sp_Blitz means the dependency exists by the
# time later procs execute per-database checks.
$IneachdbPath = Join-Path $RepoRoot "sp_ineachdb.sql"
$allContent = @()
if (Test-Path $IneachdbPath) {
    $allContent += Get-Content -Path $IneachdbPath -Raw -Encoding UTF8
}
$allContent += Get-ChildItem -Path $RepoRoot -Filter "sp_*.sql" |
    Where-Object { $_.Name -notlike "*BlitzInMemoryOLTP*" -and $_.Name -notlike "*BlitzFirst*" -and $_.Name -ne "sp_ineachdb.sql" } |
    ForEach-Object { Get-Content $_.FullName -Raw -Encoding UTF8 }

# Append SqlServerVersions.sql
if (Test-Path $SqlVersionsPath) {
    $allContent += Get-Content -Path $SqlVersionsPath -Raw -Encoding UTF8
}

# Append sp_BlitzFirst.sql last
if (Test-Path $BlitzFirstPath) {
    $allContent += Get-Content -Path $BlitzFirstPath -Raw -Encoding UTF8
}

[System.IO.File]::WriteAllText($InstallAllPath, ($allContent -join "`r`n"), $Utf8NoBom)
Write-Host "  Built: Install-All-Scripts.sql"

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Install procs on local SQL Server
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== Step 4: Install procs via Install-All-Scripts.sql ===" -ForegroundColor Cyan

$installResult = Invoke-SqlCmd-File -FilePath $InstallAllPath -Label "Install-All-Scripts.sql"
if ($installResult.ExitCode -ne 0) {
    $installContext = @"

Install script path: $InstallAllPath
Install script size: $((Get-Item $InstallAllPath).Length) bytes
Working directory: $(Get-Location)
"@
    Report-SqlCmdFailure -Result $installResult -ExtraContext $installContext
    Write-Host "`n  Installation failed. See $ErrorLog for details." -ForegroundColor Red
    Write-Host "  Skipping test calls." -ForegroundColor Yellow
} else {
    Write-Host "  Installation succeeded."

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 5 — Run test calls
    # ══════════════════════════════════════════════════════════════════════════
    Write-Host "`n=== Step 5: Run test calls ===" -ForegroundColor Cyan

    $testCalls = @(
        @{
            Label = "Drop test tables"
            Sql   = @"
DROP TABLE IF EXISTS DBAtools.dbo.Blitz;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzWho_Results;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_FileStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_PerfmonStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzFirst_WaitStats;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzCache;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzWho;
DROP TABLE IF EXISTS DBAtools.dbo.BlitzLock;
"@
        },
        @{
            Label = "sp_Blitz"
            Sql   = "EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1;"
        },
        @{
            Label = "sp_Blitz (output to table)"
            Sql   = "EXEC dbo.sp_Blitz @CheckUserDatabaseObjects = 1, @CheckServerInfo = 1, @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'Blitz';"
        },
        @{
            Label = "sp_BlitzBackups"
            Sql   = "EXEC dbo.sp_BlitzBackups @HoursBack = 1000000;"
        },
        @{
            Label = "sp_BlitzCache"
            Sql   = "EXEC dbo.sp_BlitzCache;"
        },
        @{
            Label = "sp_BlitzCache @AI = 2"
            Sql   = "EXEC dbo.sp_BlitzCache @AI = 2;"
        },
        @{
            Label = "sp_BlitzCache @SortOrder = 'all'"
            Sql   = "EXEC dbo.sp_BlitzCache @SortOrder = 'all';"
        },
        @{
            Label = "sp_BlitzCache (output to table)"
            Sql   = "EXEC dbo.sp_BlitzCache @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzCache';"
        },
        @{
            Label = "sp_BlitzFirst"
            Sql   = "EXEC dbo.sp_BlitzFirst;"
        },
        @{
            Label = "sp_BlitzFirst @Seconds = 5, @ExpertMode = 1"
            Sql   = "EXEC dbo.sp_BlitzFirst @Seconds = 5, @ExpertMode = 1;"
        },
        @{
            Label = "sp_BlitzFirst @SinceStartup = 1"
            Sql   = "EXEC dbo.sp_BlitzFirst @SinceStartup = 1;"
        },
        @{
            Label = "sp_BlitzFirst (output to tables)"
            Sql   = @"
EXEC dbo.sp_BlitzFirst @OutputDatabaseName = 'DBAtools',
                       @OutputSchemaName = 'dbo',
                       @OutputTableName = 'BlitzFirst',
                       @OutputTableNameFileStats = 'BlitzFirst_FileStats',
                       @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats',
                       @OutputTableNameWaitStats = 'BlitzFirst_WaitStats',
                       @OutputTableNameBlitzCache = 'BlitzCache',
                       @OutputTableNameBlitzWho = 'BlitzWho';
"@
        },
        @{
            Label = "sp_BlitzIndex @Mode = 0"
            Sql   = "EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 0;"
        },
        @{
            Label = "sp_BlitzIndex @Mode = 1"
            Sql   = "EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 1;"
        },
        @{
            Label = "sp_BlitzIndex @Mode = 2"
            Sql   = "EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 2;"
        },
        @{
            Label = "sp_BlitzIndex @Mode = 3"
            Sql   = "EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 3;"
        },
        @{
            Label = "sp_BlitzIndex @Mode = 4"
            Sql   = "EXEC dbo.sp_BlitzIndex @GetAllDatabases = 1, @Mode = 4;"
        },
        @{
            Label = "sp_BlitzIndex @DatabaseName = '$TestDatabase', @TableName = 'Users'"
            Sql   = "EXEC dbo.sp_BlitzIndex @DatabaseName = N'$TestDatabaseSql', @TableName = 'Users';"
        },
        @{
            Label = "sp_BlitzIndex @DatabaseName = '$TestDatabase', @TableName = 'Users', @AI = 2"
            Sql   = "EXEC dbo.sp_BlitzIndex @DatabaseName = N'$TestDatabaseSql', @TableName = 'Users', @AI = 2;"
        },
        @{
            Label = "sp_BlitzLock"
            Sql   = "EXEC dbo.sp_BlitzLock;"
        },
        @{
            Label = "sp_BlitzLock (output to table)"
            Sql   = "EXEC dbo.sp_BlitzLock @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzLock';"
        },
        @{
            Label = "sp_BlitzWho"
            Sql   = "EXEC dbo.sp_BlitzWho;"
        },
        @{
            Label = "sp_BlitzWho @ExpertMode = 1"
            Sql   = "EXEC dbo.sp_BlitzWho @ExpertMode = 1;"
        },
        @{
            Label = "sp_BlitzWho (output to table)"
            Sql   = "EXEC dbo.sp_BlitzWho @OutputDatabaseName = 'DBAtools', @OutputSchemaName = 'dbo', @OutputTableName = 'BlitzWho_Results';"
        }
    )

    $testNumber = 0
    foreach ($test in $testCalls) {
        $testNumber++
        Write-Host "  [$testNumber/$($testCalls.Count)] $($test.Label)..." -NoNewline
        $result = Invoke-SqlCmd-String -Sql $test.Sql -Label $test.Label
        if ($result.ExitCode -ne 0) {
            Report-SqlCmdFailure -Result $result
        } else {
            Write-Host "  OK" -ForegroundColor Green
        }
    }
}

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Report results
# ══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== Step 6: Results ===" -ForegroundColor Cyan

if ($script:HasErrors) {
    Write-Host "`n  Errors were found. See log: $ErrorLog" -ForegroundColor Red
    Write-Host "  PR was NOT created. Fix the errors and re-run." -ForegroundColor Yellow
} else {
    Write-Host "  All tests passed!" -ForegroundColor Green

    # Create the release branch only after the generated scripts have passed
    # installation and test calls. This keeps repeated troubleshooting runs
    # from failing early on an already-existing branch.
    Write-Host "`n=== Step 7: Create branch $BranchName ===" -ForegroundColor Cyan

    Push-Location $RepoRoot
    try {
        $ErrorActionPreference = "Continue"
        $null = git checkout -b $BranchName 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create branch $BranchName. Confirm you are on the intended base branch and that this branch name does not already exist."
        }
        Write-Host "  Branch created: $BranchName"

        # Stage and commit changes
        $null = git add -A 2>&1
        $null = git commit -m "$Today release prep" 2>&1
        $null = git push -u origin $BranchName 2>&1
        $ErrorActionPreference = "Stop"

        # Create PR
        $prBody = "## Summary`n- Version bumped to $newVersion`n- Version date set to $Today`n- Install scripts rebuilt`n- All test calls passed on local SQL Server ($ServerInstance)"
        $prUrl = gh pr create --base dev --title "$Today release prep" --body $prBody 2>&1
        Write-Host "  PR created: $prUrl" -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

Write-Host "`nDone.`n"
