$ErrorActionPreference = 'Stop'
$path = Join-Path $PSScriptRoot '../../Documentation/Development/Build Installation Scripts.ps1'
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
if ($errors) { throw ($errors | Out-String) }
$function = $ast.Find({ param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-SqlCmdErrorSummary'
}, $true)
Invoke-Expression $function.Extent.Text

$noise = ((1..25 | ForEach-Object { 'Result row with error and Line in a query plan ' + ('x' * 2000) }) -join "`n")
$header = 'Msg 50000, Level 16, State 1, Procedure dbo.sp_BlitzCache, Line 7947'
$message = 'The specified output database was not found on this server'
$result = Get-SqlCmdErrorSummary "$noise`n$header`n$message`n`nFinished"
if ($result -notlike "*$header*$message*" -or $result -like '*Result row*') {
    throw 'SQL error context was lost or tabular noise was included.'
}
$result = Get-SqlCmdErrorSummary "$header`nFirst message`nMsg 208, Level 16, State 1, Line 2`nSecond message"
if ($result -notlike '*First message*Second message*') { throw 'Consecutive error context was lost.' }
$result = Get-SqlCmdErrorSummary 'Sqlcmd: Error: Login failed.'
if ($result -notlike '*Login failed*') { throw 'Native client error was lost.' }
$result = Get-SqlCmdErrorSummary ('x' * 5000)
if ($result.Length -gt 550 -or $result -notlike '*see full log*') { throw 'Wide fallback output was not bounded.' }
if ((Get-SqlCmdErrorSummary '') -ne '(sqlcmd did not return any output.)') { throw 'Empty output regression.' }
$boundaryInput = "$header`nFirst message`nAdditional context`n" + ((1..9 | ForEach-Object {
    "Msg $($_), Level 16, State 1, Line 1`nMessage $($_)"
}) -join "`n")
$result = Get-SqlCmdErrorSummary $boundaryInput
if (($result -split "`n").Count -gt 20 -or $result -notlike '*Message 8*' -or $result -like '*Msg 9,*') {
    throw 'The summary limit admitted an error header without its message.'
}
Write-Host 'PASS: SQL headers retain messages, result noise is excluded, fallback is bounded.'
