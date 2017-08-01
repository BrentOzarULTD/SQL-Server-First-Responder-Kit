#All Core Blitz Without sp_BlitzQueryStore
Get-ChildItem -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\*" -Include sp_Blitz*.sql -Exclude sp_BlitzQueryStore.sql | ForEach-Object { Get-Content $_.FullName } | Set-Content -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\Install-Core-Blitz-No-Query-Store.sql" -Force

#All Core Blitz With sp_BlitzQueryStore
Get-ChildItem -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\*" -Include sp_Blitz*.sql | ForEach-Object { Get-Content $_.FullName } | Set-Content -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\Install-Core-Blitz-With-Query-Store.sql" -Force

#All Scripts
Get-ChildItem -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\*" -Include sp_*.sql | ForEach-Object { Get-Content $_.FullName } | Set-Content -Path "C:\Users\$env:username\Documents\GitHub\PublicTools\Install-All-Scripts.sql" -Force