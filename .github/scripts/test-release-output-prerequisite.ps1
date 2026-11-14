$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$source = Get-Content (Join-Path $repo 'Documentation/Development/Build Installation Scripts.ps1') -Raw
$tokens = $null; $errors = $null
[void][System.Management.Automation.Language.Parser]::ParseInput($source,[ref]$tokens,[ref]$errors)
if ($errors) { throw ($errors | Out-String) }
# Execute the real prerequisite path, stopping before version changes and publishing.
$prefix = $source.Substring(0,$source.LastIndexOf('# ═',$source.IndexOf('# STEP 1')))
$temp = Join-Path ([IO.Path]::GetTempPath()) ('frk-prerequisite-' + [guid]::NewGuid())
$dir = New-Item (Join-Path $temp 'Documentation/Development') -ItemType Directory -Force
$scriptPath = Join-Path $dir 'Build.ps1'
Set-Content $scriptPath $prefix
$global:CapturedSql = ''
$global:NativeExit = 1
function sqlcmd {
    $fileIndex = [array]::IndexOf($args, '-i') + 1
    $global:CapturedSql = Get-Content $args[$fileIndex] -Raw
    $global:LASTEXITCODE = $global:NativeExit
    if ($global:NativeExit) { 'Msg 51000, Level 16, State 1'; 'Fixture prerequisite missing.' }
}
try {
    $failed = $false
    try { & $scriptPath  } catch { $failed = $_.Exception.Message -like '*prerequisite failed*' }
    if (-not $failed) { throw 'Missing prerequisite did not stop release preparation.' }
    $global:NativeExit = 0
    & $scriptPath 
    if ($global:CapturedSql -notlike "*DB_ID(N'DBAtools')*" -or $global:CapturedSql -notlike '*READ_WRITE*' -or
         $global:CapturedSql -notlike '*CREATE TABLE*' -or $global:CapturedSql -notlike '*SCHEMA*') {
        throw 'DBAtools state or permissions were not validated.'
    }
    Write-Host 'PASS: missing prerequisite stops release preparation; valid prerequisite continues.'
} finally {
    Remove-Item $temp -Recurse -Force
    Remove-Variable CapturedSql, NativeExit -Scope Global
}
