#Set your file path
$FilePath = "C:\Users\edarl\OneDrive\Documents\GitHub\SQL-Server-First-Responder-Kit"


#All Core Blitz Without sp_BlitzQueryStore
Get-ChildItem -Path "$FilePath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzQueryStore.sql*" -and $_.FullName -notlike "*sp_BlitzInMemoryOLTP*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-Core-Blitz-No-Query-Store.sql" -Force

#All Core Blitz With sp_BlitzQueryStore
Get-ChildItem -Path "$FilePath" -Filter "sp_Blitz*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-Core-Blitz-With-Query-Store.sql" -Force

#All Scripts
Get-ChildItem -Path "$FilePath" -Filter "sp_*.sql" |
Where-Object { $_.FullName -notlike "*sp_BlitzInMemoryOLTP*"} |
ForEach-Object { Get-Content $_.FullName } | 
Set-Content -Path "$FilePath\Install-All-Scripts.sql" -Force