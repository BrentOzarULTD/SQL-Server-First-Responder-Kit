#Set your file path
$RepoRootPath = $PSScriptRoot
$RepoRootPath = Join-Path -Path $RepoRootPath -ChildPath ".."
$RepoRootPath = Join-Path -Path $RepoRootPath -ChildPath ".."
$RepoRootPath = Resolve-Path -Path $RepoRootPath

$SqlVersionsPath = "$RepoRootPath/SqlServerVersions.sql"
$BlitzFirstPath = "$RepoRootPath/sp_BlitzFirst.sql"

#Azure - skip sp_Blitz, sp_BlitzBackups, sp_DatabaseRestore, sp_ineachdb
Get-ChildItem -Path "$RepoRootPath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_Blitz.sql*" -and $_.FullName -notlike "*sp_BlitzBackups*" -and $_.FullName -notlike "*sp_DatabaseRestore*"} |
ForEach-Object { Get-Content $_.FullName } |
Set-Content -Path "$RepoRootPath/Install-Azure.sql" -Force
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$RepoRootPath/Install-All-Scripts.sql" -Value (Get-Content -Path "$BlitzFirstPath")}


#All Scripts
Get-ChildItem -Path "$RepoRootPath" -Filter "sp_*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*" -and $_.FullName -notlike "*sp_BlitzFirst*"} |
ForEach-Object { Get-Content $_.FullName } |
Set-Content -Path "$RepoRootPath/Install-All-Scripts.sql" -Force
#append script to (re-)create SqlServerVersions Table (https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2429)
if ( test-path "$SqlVersionsPath")
	{ Add-Content -Path "$RepoRootPath/Install-All-Scripts.sql" -Value (Get-Content -Path "$SqlVersionsPath")}
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$RepoRootPath/Install-All-Scripts.sql" -Value (Get-Content -Path "$BlitzFirstPath")}
