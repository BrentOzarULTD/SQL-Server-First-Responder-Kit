Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_AskBrent.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_Blitz.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_BlitzCache.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_BlitzIndex.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_BlitzTrace.sql -Database "master" -AbortOnError
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2014' -Username 'sa' -Password 'Password12!' -Databaseatabase 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -ServerInstance "(local)\SQL2014" -Username "sa" -Password "Password12!" -InputFile sp_BlitzRS.sql -Database "master" -AbortOnError
}

Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_AskBrent.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_Blitz.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_BlitzCache.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_BlitzIndex.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_BlitzTrace.sql -Database "master" -AbortOnError
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2012SP1' -Username 'sa' -Password 'Password12!' -Databaseatabase 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -ServerInstance "(local)\SQL2012SP1" -Username "sa" -Password "Password12!" -InputFile sp_BlitzRS.sql -Database "master" -AbortOnError
}

Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_AskBrent.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_Blitz.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_BlitzCache.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_BlitzIndex.sql -Database "master" -AbortOnError
Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_BlitzTrace.sql -Database "master" -AbortOnError
$result = Invoke-Sqlcmd -ServerInstance '(local)\SQL2008R2SP2' -Username 'sa' -Password 'Password12!' -Databaseatabase 'master' -Query "SELECT name FROM sys.databases where name = 'ReportServer'"
if ($result -eq 'ReportServer')
{
  Invoke-Sqlcmd -ServerInstance "(local)\SQL2008R2SP2" -Username "sa" -Password "Password12!" -InputFile sp_BlitzRS.sql -Database "master" -AbortOnError
}
