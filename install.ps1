sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzFirst.sql -d "master" -b
sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = sqlcmd -S '(local)\SQL2014' -U 'sa' -P 'Password12!' -d 'master' -Q "SELECT name FROM sys.databases where name = 'ReportServer'" -b -W
if ($result -eq 'ReportServer')
{
  sqlcmd -S "(local)\SQL2014" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}

sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzFirst.sql -d "master" -b
sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = sqlcmd -S '(local)\SQL2012SP1' -U 'sa' -P 'Password12!' -d 'master' -Q "SELECT name FROM sys.databases where name = 'ReportServer'" -b -W
if ($result -eq 'ReportServer')
{
  sqlcmd -S "(local)\SQL2012SP1" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}

sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzFirst.sql -d "master" -b
sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_Blitz.sql -d "master" -b
sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzCache.sql -d "master" -b
sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzIndex.sql -d "master" -b
sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzTrace.sql -d "master" -b
$result = sqlcmd -S '(local)\SQL2008R2SP2' -U 'sa' -P 'Password12!' -d 'master' -Q "SELECT name FROM sys.databases where name = 'ReportServer'" -b -W
if ($result -eq 'ReportServer')
{
  sqlcmd -S "(local)\SQL2008R2SP2" -U "sa" -P "Password12!" -i sp_BlitzRS.sql -d "master" -b
}
