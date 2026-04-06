#Set your file path
$FilePath = "/Users/brentozar/LocalOnly/Github/SQL-Server-First-Responder-Kit"
$SqlVersionsPath = "$FilePath/SqlServerVersions.sql"
$BlitzFirstPath = "$FilePath/sp_BlitzFirst.sql"

#Azure - skip sp_Blitz, sp_BlitzBackups, sp_DatabaseRestore, sp_ineachdb
Get-ChildItem -Path "$FilePath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_Blitz.sql*" -and $_.FullName -notlike "*sp_BlitzBackups*" -and $_.FullName -notlike "*sp_DatabaseRestore*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath/Install-Azure.sql" -Force
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$FilePath/Install-All-Scripts.sql" -Value (Get-Content -Path "$BlitzFirstPath")}
	

#All Scripts
Get-ChildItem -Path "$FilePath" -Filter "sp_*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*" -and $_.FullName -notlike "*sp_BlitzFirst*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath/Install-All-Scripts.sql" -Force
#append script to (re-)create SqlServerVersions Table (https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2429)
if ( test-path "$SqlVersionsPath")
	{ Add-Content -Path "$FilePath/Install-All-Scripts.sql" -Value (Get-Content -Path "$SqlVersionsPath")}
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$FilePath/Install-All-Scripts.sql" -Value (Get-Content -Path "$BlitzFirstPath")}
