Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_AskBrent.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2014' -U 'sa' -P 'Password12!' -Database 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}

Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_AskBrent.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2012SP1' -U 'sa' -P 'Password12!' -Database 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}

Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_AskBrent.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2008R2SP2' -U 'sa' -P 'Password12!' -Database 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}
