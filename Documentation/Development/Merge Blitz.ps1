#Set your file path
$FilePath = "C:\temp\SQL-Server-First-Responder-Kit"
$SqlVersionsPath = "$FilePath\SqlServerVersions.sql"
$BlitzFirstPath = "$FilePath\sp_BlitzFirst.sql"

#All Core Blitz Without sp_BlitzQueryStore
Get-ChildItem -Path "$FilePath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzQueryStore.sql*" -and $_.FullName -notlike "*sp_BlitzInMemoryOLTP*" -and $_.FullName -notlike "*sp_BlitzFirst*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-Core-Blitz-No-Query-Store.sql" -Force
#append script to (re-)create SqlServerVersions Table (https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2429)
if ( test-path "$SqlVersionsPath")
	{ Add-Content -Path "$FilePath\Install-Core-Blitz-No-Query-Store.sql" -Value (Get-Content -Path "$SqlVersionsPath")}
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$FilePath\Install-Core-Blitz-No-Query-Store.sql" -Value (Get-Content -Path "$BlitzFirstPath")}


#All Core Blitz With sp_BlitzQueryStore
Get-ChildItem -Path "$FilePath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*" -and $_.FullName -notlike "*sp_BlitzFirst*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-Core-Blitz-With-Query-Store.sql" -Force
#append script to (re-)create SqlServerVersions Table (https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2429)
if ( test-path "$SqlVersionsPath")
	{ Add-Content -Path "$FilePath\Install-Core-Blitz-With-Query-Store.sql" -Value (Get-Content -Path "$SqlVersionsPath")}
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$FilePath\Install-Core-Blitz-With-Query-Store.sql" -Value (Get-Content -Path "$BlitzFirstPath")}


#All Scripts
Get-ChildItem -Path "$FilePath" -Filter "sp_*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*" -and $_.FullName -notlike "*sp_BlitzFirst*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-All-Scripts.sql" -Force
#append script to (re-)create SqlServerVersions Table (https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2429)
if ( test-path "$SqlVersionsPath")
	{ Add-Content -Path "$FilePath\Install-All-Scripts.sql" -Value (Get-Content -Path "$SqlVersionsPath")}
if ( test-path "$BlitzFirstPath")
	{ Add-Content -Path "$FilePath\Install-All-Scripts.sql" -Value (Get-Content -Path "$BlitzFirstPath")}
