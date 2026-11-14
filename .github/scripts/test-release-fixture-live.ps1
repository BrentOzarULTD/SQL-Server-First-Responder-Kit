param(
    [Parameter(Mandatory)][string]$ServerInstance,
    [string]$BuildScriptPath = (Join-Path $PSScriptRoot '../../Documentation/Development/Build Installation Scripts.ps1')
)
$ErrorActionPreference = 'Stop'
$source = Get-Content $BuildScriptPath -Raw -Encoding UTF8
$prefix = $source.Substring(0, $source.LastIndexOf('# ═', $source.IndexOf('# STEP 1')))
$temp = Join-Path ([IO.Path]::GetTempPath()) ('frk-live-' + [guid]::NewGuid())
$dir = New-Item (Join-Path $temp 'Documentation/Development') -ItemType Directory -Force
$scriptPath = Join-Path $dir 'Prerequisite.ps1'
Set-Content $scriptPath $prefix -Encoding UTF8
$fixture = 'FRK_' + [guid]::NewGuid().ToString('N') + '_' + [char]38634
$created = $false
try {
    # Load the real helpers without invoking the fixture prerequisite yet.
    $helpers = $prefix.Substring(0, $prefix.IndexOf('# Validate the fixture'))
    Set-Content $scriptPath $helpers -Encoding UTF8
    . $scriptPath -ServerInstance $ServerInstance
    $result = Invoke-SqlCmd-String -Sql "CREATE DATABASE [$fixture];" -Label 'Create Unicode fixture'
    if ($result.ExitCode) { throw $result.Output }
    $created = $true
    $result = Invoke-SqlCmd-String -Sql "USE [$fixture]; CREATE TABLE dbo.Users(id int);" -Label 'Create fixture table'
    if ($result.ExitCode) { throw $result.Output }
    Set-Content $scriptPath $prefix -Encoding UTF8
    . $scriptPath -ServerInstance $ServerInstance -TestDatabase $fixture
    Write-Host 'PASS: real Windows sqlcmd accepted the Unicode database fixture.'
} finally {
    if ($created) {
        $result = Invoke-SqlCmd-String -Sql "DROP DATABASE [$fixture];" -Label 'Remove Unicode fixture'
        if ($result.ExitCode) { throw $result.Output }
    }
    Remove-Item $temp -Recurse -Force
}
