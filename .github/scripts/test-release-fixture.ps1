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
    $encodingIndex = [array]::IndexOf($args, '-f') + 1
    if ($encodingIndex -eq 0 -or $args[$encodingIndex] -ne 'i:65001') { throw 'sqlcmd input encoding must be UTF-8.' }
    $fileIndex = [array]::IndexOf($args, '-i') + 1
    $global:CapturedSql = Get-Content $args[$fileIndex] -Raw
    $global:LASTEXITCODE = $global:NativeExit
    if ($global:NativeExit) { 'Msg 51000, Level 16, State 1'; 'Fixture prerequisite missing.' }
}
try {
    $failed = $false
    try { & $scriptPath -TestDatabase "StackOverflow'雪]" } catch { $failed = $_.Exception.Message -like '*prerequisite failed*' }
    if (-not $failed) { throw 'Missing prerequisite did not stop release preparation.' }
    $global:NativeExit = 0
    & $scriptPath -TestDatabase "StackOverflow'雪]"
    if ($global:CapturedSql -notlike "*N'StackOverflow''雪]'*" -or $global:CapturedSql -notlike '*dbo.Users*') {
        throw 'Fixture identifier was not escaped or dbo.Users was not validated.'
    }
    . $scriptPath -TestDatabase "StackOverflow'雪]"
    $fileResult = Invoke-SqlCmd-File -FilePath $scriptPath -Label 'File encoding'
    if ($fileResult.Command -notlike '*-f i:65001*') { throw 'Logged file command omits encoding.' }
    Write-Host 'PASS: missing prerequisite stops release preparation; valid prerequisite continues.'
} finally {
    Remove-Item $temp -Recurse -Force
    Remove-Variable CapturedSql, NativeExit -Scope Global
}
