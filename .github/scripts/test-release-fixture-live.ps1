param(
    [Parameter(Mandatory)][string]$ServerInstance,
    [string]$BuildScriptPath = (Join-Path $PSScriptRoot '../../Documentation/Development/Build Installation Scripts.ps1')
)
$ErrorActionPreference = 'Stop'
$source = Get-Content $BuildScriptPath -Raw -Encoding UTF8
$prefix = $source.Substring(0, $source.LastIndexOf('#', $source.IndexOf('# STEP 1') - 2))
$temp = Join-Path ([IO.Path]::GetTempPath()) ('frk-live-' + [guid]::NewGuid())
$dir = New-Item (Join-Path $temp 'Documentation/Development') -ItemType Directory -Force
$scriptPath = Join-Path $dir 'Prerequisite.ps1'
Set-Content $scriptPath $prefix -Encoding UTF8
$fixture = '[FRK_' + [guid]::NewGuid().ToString('N') + '_' + [char]38634 + '$(FRK_UNDEFINED)' + "']"
$fixtureIdentifier = '[' + $fixture.Replace(']', ']]') + ']'
$unicodePosition = $fixture.IndexOf([char]38634) + 1
$created = $false
try {
    # Load the real helpers without invoking the fixture prerequisite yet.
    $helpers = $prefix.Substring(0, $prefix.IndexOf('# Validate the fixture'))
    Set-Content $scriptPath $helpers -Encoding UTF8
    . $scriptPath -ServerInstance $ServerInstance
    $result = Invoke-SqlCmd-String -Sql "CREATE DATABASE $fixtureIdentifier;" -Label 'Create literal-name fixture'
    if ($result.ExitCode) { throw $result.Output }
    $created = $true
    $result = Invoke-SqlCmd-String -Sql "USE $fixtureIdentifier; IF UNICODE(SUBSTRING(DB_NAME(), $unicodePosition, 1)) <> 38634 THROW 51000, 'Fixture name lost its Unicode character.', 1; CREATE TABLE dbo.Users(id int);" -Label 'Create fixture table'
    if ($result.ExitCode) { throw $result.Output }
    Set-Content $scriptPath $prefix -Encoding UTF8
    . $scriptPath -ServerInstance $ServerInstance -TestDatabase $fixture
    # Exercise the actual table-specific calls, including the AI variant.
    $tokens = $null; $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$errors)
    $calls = $ast.Find({ param($node)
        $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq '$testCalls'
    }, $true)
    Invoke-Expression $calls.Extent.Text
    $fixtureCalls = @($testCalls | Where-Object { $_.Sql -like '*sp_BlitzIndex @DatabaseName*' })
    if ($fixtureCalls.Count -ne 2) { throw 'Expected two fixture index tests.' }
    foreach ($call in $fixtureCalls) {
        $result = Invoke-SqlCmd-String -Sql $call.Sql -Label $call.Label
        if ($result.ExitCode) { throw $result.Output }
    }
    Write-Host 'PASS: real sqlcmd prerequisite and both index calls accepted the literal Unicode, bracket, apostrophe, and dollar-sign fixture name.'
    $result = Invoke-SqlCmd-String -Sql "ALTER DATABASE $fixtureIdentifier SET RESTRICTED_USER;" -Label 'Restrict disposable fixture'
    if ($result.ExitCode) { throw $result.Output }
    $failed = $false
    try { & $scriptPath -ServerInstance $ServerInstance -TestDatabase $fixture }
    catch { $failed = $_.Exception.Message -like '*prerequisite failed*' }
    if (-not $failed) { throw 'Restricted fixture did not stop release preparation.' }
    Write-Host 'PASS: RESTRICTED_USER fixture rejected before release preparation.'
} finally {
    if ($created) {
        $result = Invoke-SqlCmd-String -Sql "ALTER DATABASE $fixtureIdentifier SET MULTI_USER; DROP DATABASE $fixtureIdentifier;" -Label 'Remove disposable fixture'
        if ($result.ExitCode) { throw $result.Output }
    }
    Remove-Item $temp -Recurse -Force
}
