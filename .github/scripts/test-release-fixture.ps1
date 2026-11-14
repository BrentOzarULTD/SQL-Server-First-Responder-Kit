param(
    [string]$BuildScriptPath = (Join-Path $PSScriptRoot '../../Documentation/Development/Build Installation Scripts.ps1')
)
$ErrorActionPreference = 'Stop'
$source = Get-Content $BuildScriptPath -Raw -Encoding UTF8
$tokens = $null; $errors = $null
[void][System.Management.Automation.Language.Parser]::ParseInput($source,[ref]$tokens,[ref]$errors)
if ($errors) { throw ($errors | Out-String) }
# Execute the real prerequisite path, stopping before version changes and publishing.
$prefix = $source.Substring(0,$source.LastIndexOf('#', $source.IndexOf('# STEP 1') - 2))
$temp = Join-Path ([IO.Path]::GetTempPath()) ('frk-prerequisite-' + [guid]::NewGuid())
$dir = New-Item (Join-Path $temp 'Documentation/Development') -ItemType Directory -Force
$scriptPath = Join-Path $dir 'Build.ps1'
Set-Content $scriptPath $prefix -Encoding UTF8
$fixture = "[StackOverflow'" + [char]38634 + '$(FRK_UNDEFINED)]'
$fixtureSqlLiteral = "N'" + $fixture.Replace("'", "''") + "'"
$global:CapturedSql = ''
$global:NativeExit = 1
function sqlcmd {
    if ($args -notcontains '-x') { throw 'sqlcmd substitution must be disabled.' }
    $encodingIndex = [array]::IndexOf($args, '-f') + 1
    if ($encodingIndex -eq 0 -or $args[$encodingIndex] -ne 'i:65001') { throw 'sqlcmd input encoding must be UTF-8.' }
    $fileIndex = [array]::IndexOf($args, '-i') + 1
    $global:CapturedSql = Get-Content $args[$fileIndex] -Raw -Encoding UTF8
    $global:LASTEXITCODE = $global:NativeExit
    if ($global:NativeExit) { 'Msg 51000, Level 16, State 1'; 'Fixture prerequisite missing.' }
}
try {
    $failed = $false
    try { & $scriptPath -TestDatabase $fixture } catch { $failed = $_.Exception.Message -like '*prerequisite failed*' }
    if (-not $failed) { throw 'Missing prerequisite did not stop release preparation.' }
    $global:NativeExit = 0
    & $scriptPath -TestDatabase $fixture
    if (-not $global:CapturedSql.Contains($fixtureSqlLiteral) -or $global:CapturedSql -notlike '*dbo.Users*') {
        throw 'Fixture identifier was not escaped or dbo.Users was not validated.'
    }
    if ($global:CapturedSql -notlike "*'UserAccess'*'MULTI_USER'*") { throw 'Fixture access mode was not validated.' }
    . $scriptPath -TestDatabase $fixture
    $fileResult = Invoke-SqlCmd-File -FilePath $scriptPath -Label 'File encoding'
    if ($fileResult.Command -notlike '*-f i:65001*') { throw 'Logged file command omits encoding.' }
    if ($fileResult.Command -notlike '*-x*') { throw 'Logged file command omits -x.' }
    $stringResult = Invoke-SqlCmd-String -Sql 'SELECT 1' -Label 'String options'
    if ($stringResult.Command -notlike '*-x*') { throw 'Logged string command omits -x.' }
    $ast = [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$errors)
    $calls = $ast.Find({ param($node)
        $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq '$testCalls'
    }, $true)
    Invoke-Expression $calls.Extent.Text
    $fixtureCalls = @($testCalls | Where-Object { $_.Sql -like '*sp_BlitzIndex @DatabaseName*' })
    if ($fixtureCalls.Count -ne 2) { throw 'Expected two fixture index tests.' }
    foreach ($call in $fixtureCalls) {
        if (-not $call.Sql.Contains($fixtureSqlLiteral) -or $call.Sql -notlike "*@ObjectName = N'dbo.Users'*") {
            throw 'An index test does not preserve the literal fixture name.'
        }
    }
    Write-Host 'PASS: missing prerequisite stops release preparation; valid prerequisite continues.'
} finally {
    Remove-Item $temp -Recurse -Force
    Remove-Variable CapturedSql, NativeExit -Scope Global
}
